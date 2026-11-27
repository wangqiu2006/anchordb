//! Ergonomic structured query API with automatic primary-key / secondary-index selection,
//! explainable execution plans, sorting/projection/limit, and per-query memory budgets.

use core::cmp::Ordering;
use core::fmt;
use core::ops::Bound;

use kv_core::KvTransaction;
use table_codec::{CodecError, SortOrder, SystemCatalog, TableSchema, TableStore, Value};

use crate::QueryConfig;

/// Errors returned during structured query planning or execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    Codec(CodecError),
    ColumnNotFound(String),
    TableNotFound(String),
    MemoryBudgetExceeded { limit: usize, used: usize },
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(err) => write!(f, "codec error: {err}"),
            Self::ColumnNotFound(col) => write!(f, "column '{col}' not found"),
            Self::TableNotFound(tbl) => write!(f, "table '{tbl}' not found"),
            Self::MemoryBudgetExceeded { limit, used } => {
                write!(
                    f,
                    "query memory budget exceeded: used {used} bytes (limit {limit} bytes)"
                )
            }
        }
    }
}

impl core::error::Error for QueryError {}

impl From<CodecError> for QueryError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

/// Helper trait allowing Rust primitives (`i64`, `&str`, `String`, `bool`, `f64`, `Vec<u8>`)
/// to be passed ergonomically into [`ColExpr`] builders.
pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::Int64(self)
    }
}

impl IntoValue for i32 {
    fn into_value(self) -> Value {
        Value::Int64(i64::from(self))
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::Float64(self)
    }
}

impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::Text(self.to_owned())
    }
}

impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::Text(self)
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Blob(self)
    }
}

/// Comparison operator inside a filter predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// Composable filter expression tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Compare {
        column: String,
        op: CmpOp,
        value: Value,
    },
    IsNull(String),
    IsNotNull(String),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    #[must_use]
    pub fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }

    fn eval(&self, schema: &TableSchema, row: &[Value]) -> Result<bool, QueryError> {
        match self {
            Self::Compare { column, op, value } => {
                let idx = schema
                    .column_index_by_name(column)
                    .ok_or_else(|| QueryError::ColumnNotFound(column.clone()))?;
                let cell = &row[idx];
                if cell.is_null() || value.is_null() {
                    // SQL three-valued logic: comparisons against NULL evaluate to false in WHERE.
                    return Ok(false);
                }
                let ord = cell.cmp(value);
                Ok(match op {
                    CmpOp::Eq => ord == Ordering::Equal,
                    CmpOp::Ne => ord != Ordering::Equal,
                    CmpOp::Lt => ord == Ordering::Less,
                    CmpOp::Lte => ord != Ordering::Greater,
                    CmpOp::Gt => ord == Ordering::Greater,
                    CmpOp::Gte => ord != Ordering::Less,
                })
            }
            Self::IsNull(column) => {
                let idx = schema
                    .column_index_by_name(column)
                    .ok_or_else(|| QueryError::ColumnNotFound(column.clone()))?;
                Ok(row[idx].is_null())
            }
            Self::IsNotNull(column) => {
                let idx = schema
                    .column_index_by_name(column)
                    .ok_or_else(|| QueryError::ColumnNotFound(column.clone()))?;
                Ok(!row[idx].is_null())
            }
            Self::And(a, b) => Ok(a.eval(schema, row)? && b.eval(schema, row)?),
            Self::Or(a, b) => Ok(a.eval(schema, row)? || b.eval(schema, row)?),
            Self::Not(inner) => Ok(!inner.eval(schema, row)?),
        }
    }
}

/// Column expression builder entry point: `col("rarity").gte(4)`.
#[derive(Debug, Clone)]
pub struct ColExpr {
    name: String,
}

#[must_use]
pub fn col(name: impl Into<String>) -> ColExpr {
    ColExpr { name: name.into() }
}

impl ColExpr {
    #[must_use]
    pub fn eq(self, val: impl IntoValue) -> Expr {
        Expr::Compare {
            column: self.name,
            op: CmpOp::Eq,
            value: val.into_value(),
        }
    }

    #[must_use]
    pub fn ne(self, val: impl IntoValue) -> Expr {
        Expr::Compare {
            column: self.name,
            op: CmpOp::Ne,
            value: val.into_value(),
        }
    }

    #[must_use]
    pub fn lt(self, val: impl IntoValue) -> Expr {
        Expr::Compare {
            column: self.name,
            op: CmpOp::Lt,
            value: val.into_value(),
        }
    }

    #[must_use]
    pub fn lte(self, val: impl IntoValue) -> Expr {
        Expr::Compare {
            column: self.name,
            op: CmpOp::Lte,
            value: val.into_value(),
        }
    }

    #[must_use]
    pub fn gt(self, val: impl IntoValue) -> Expr {
        Expr::Compare {
            column: self.name,
            op: CmpOp::Gt,
            value: val.into_value(),
        }
    }

    #[must_use]
    pub fn gte(self, val: impl IntoValue) -> Expr {
        Expr::Compare {
            column: self.name,
            op: CmpOp::Gte,
            value: val.into_value(),
        }
    }

    #[must_use]
    pub fn is_null(self) -> Expr {
        Expr::IsNull(self.name)
    }

    #[must_use]
    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(self.name)
    }
}

/// Named query output row with typed column accessors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRow {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl QueryRow {
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    #[must_use]
    pub fn get(&self, col_name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == col_name)
            .and_then(|idx| self.values.get(idx))
    }

    #[must_use]
    pub fn get_i64(&self, col_name: &str) -> Option<i64> {
        match self.get(col_name) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn get_f64(&self, col_name: &str) -> Option<f64> {
        match self.get(col_name) {
            Some(Value::Float64(v)) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn get_bool(&self, col_name: &str) -> Option<bool> {
        match self.get(col_name) {
            Some(Value::Bool(v)) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn get_str(&self, col_name: &str) -> Option<&str> {
        match self.get(col_name) {
            Some(Value::Text(s)) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// Physical access path selected by the rule-based index chooser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessPath {
    PrimaryKeyLookup {
        pk_values: Vec<Value>,
    },
    IndexPrefixScan {
        index_name: String,
        prefix_values: Vec<Value>,
    },
    IndexRangeScan {
        index_name: String,
        start: Bound<Vec<Value>>,
        end: Bound<Vec<Value>>,
    },
    SeqScan,
}

/// Inspectable execution plan returned by [`TableQuery::explain`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub table_name: String,
    pub access_path: AccessPath,
    pub has_post_filter: bool,
    pub order_by: Vec<(String, SortOrder)>,
    pub limit: Option<usize>,
    pub offset: usize,
}

/// Chainable structured query builder over a named table.
#[derive(Debug, Clone)]
pub struct TableQuery {
    table_name: String,
    projection: Option<Vec<String>>,
    filter: Option<Expr>,
    order_by: Vec<(String, SortOrder)>,
    limit: Option<usize>,
    offset: usize,
}

impl TableQuery {
    #[must_use]
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            projection: None,
            filter: None,
            order_by: Vec::new(),
            limit: None,
            offset: 0,
        }
    }

    #[must_use]
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.projection = Some(columns.iter().map(|&c| c.to_owned()).collect());
        self
    }

    #[must_use]
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter = Some(match self.filter {
            Some(existing) => existing.and(expr),
            None => expr,
        });
        self
    }

    #[must_use]
    pub fn order_by(mut self, column: impl Into<String>, direction: SortOrder) -> Self {
        self.order_by.push((column.into(), direction));
        self
    }

    #[must_use]
    pub const fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count);
        self
    }

    #[must_use]
    pub const fn offset(mut self, skip: usize) -> Self {
        self.offset = skip;
        self
    }

    /// Computes the rule-based physical execution plan against `schema`.
    #[must_use]
    pub fn plan_for_schema(&self, schema: &TableSchema) -> QueryPlan {
        let access_path = choose_access_path(schema, self.filter.as_ref());
        QueryPlan {
            table_name: schema.name.clone(),
            access_path,
            has_post_filter: self.filter.is_some(),
            order_by: self.order_by.clone(),
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Resolves table metadata in `txn` and returns the chosen [`QueryPlan`].
    pub fn explain<T: KvTransaction>(&self, txn: &T) -> Result<QueryPlan, QueryError> {
        let schema = SystemCatalog::get_table_by_name(txn, &self.table_name)?
            .ok_or_else(|| QueryError::TableNotFound(self.table_name.clone()))?;
        Ok(self.plan_for_schema(&schema))
    }

    /// Executes the query inside `txn` respecting `config.memory_budget_bytes`.
    pub fn execute<T: KvTransaction>(
        &self,
        txn: &T,
        config: &QueryConfig,
    ) -> Result<Vec<QueryRow>, QueryError> {
        let schema = SystemCatalog::get_table_by_name(txn, &self.table_name)?
            .ok_or_else(|| QueryError::TableNotFound(self.table_name.clone()))?;

        // Validate projection and sort columns up front.
        let proj_indices: Vec<(String, usize)> = match &self.projection {
            Some(cols) => {
                let mut out = Vec::with_capacity(cols.len());
                for name in cols {
                    let idx = schema
                        .column_index_by_name(name)
                        .ok_or_else(|| QueryError::ColumnNotFound(name.clone()))?;
                    out.push((name.clone(), idx));
                }
                out
            }
            None => schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.clone(), i))
                .collect(),
        };

        let mut order_indices = Vec::with_capacity(self.order_by.len());
        for (col_name, dir) in &self.order_by {
            let idx = schema
                .column_index_by_name(col_name)
                .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
            order_indices.push((idx, *dir));
        }

        let plan = self.plan_for_schema(&schema);
        let raw_rows = match &plan.access_path {
            AccessPath::PrimaryKeyLookup { pk_values } => {
                match TableStore::get_row_by_pk(txn, &schema, pk_values)? {
                    Some(row) => vec![row],
                    None => Vec::new(),
                }
            }
            AccessPath::IndexPrefixScan {
                index_name,
                prefix_values,
            } => TableStore::scan_index_prefix(txn, &schema, index_name, prefix_values)?,
            AccessPath::IndexRangeScan {
                index_name,
                start,
                end,
            } => TableStore::scan_index_range(
                txn,
                &schema,
                index_name,
                bound_vec_as_slice(start),
                bound_vec_as_slice(end),
            )?,
            AccessPath::SeqScan => TableStore::scan_table(txn, &schema)?,
        };

        // Filter and enforce per-query memory budget.
        let mut matched = Vec::new();
        let mut used_bytes = 0usize;
        for row in raw_rows {
            if let Some(expr) = &self.filter
                && !expr.eval(&schema, &row)?
            {
                continue;
            }
            used_bytes = used_bytes.saturating_add(estimate_row_bytes(&row));
            if used_bytes > config.memory_budget_bytes {
                return Err(QueryError::MemoryBudgetExceeded {
                    limit: config.memory_budget_bytes,
                    used: used_bytes,
                });
            }
            matched.push(row);
        }

        // Apply ORDER BY.
        if !order_indices.is_empty() {
            matched.sort_by(|a, b| {
                for &(idx, dir) in &order_indices {
                    let ord = a[idx].cmp(&b[idx]);
                    let directed = match dir {
                        SortOrder::Asc => ord,
                        SortOrder::Desc => ord.reverse(),
                    };
                    if directed != Ordering::Equal {
                        return directed;
                    }
                }
                Ordering::Equal
            });
        }

        // Apply OFFSET + LIMIT + Projection.
        let col_names: Vec<String> = proj_indices.iter().map(|(n, _)| n.clone()).collect();
        let iter = matched.into_iter().skip(self.offset);
        let limited: Vec<Vec<Value>> = match self.limit {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        };

        let result = limited
            .into_iter()
            .map(|row| {
                let values = proj_indices.iter().map(|(_, i)| row[*i].clone()).collect();
                QueryRow {
                    columns: col_names.clone(),
                    values,
                }
            })
            .collect();

        Ok(result)
    }
}

fn bound_vec_as_slice(b: &Bound<Vec<Value>>) -> Bound<&[Value]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn estimate_row_bytes(row: &[Value]) -> usize {
    let mut bytes = 32usize;
    for v in row {
        bytes += match v {
            Value::Null | Value::Bool(_) => 8,
            Value::Int64(_) | Value::Float64(_) => 16,
            Value::Text(s) => 24 + s.len(),
            Value::Blob(b) => 24 + b.len(),
        };
    }
    bytes
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::And(a, b) => {
            collect_conjuncts(a, out);
            collect_conjuncts(b, out);
        }
        other => out.push(other),
    }
}

fn find_eq_value(conjuncts: &[&Expr], col_name: &str) -> Option<Value> {
    for expr in conjuncts {
        if let Expr::Compare {
            column,
            op: CmpOp::Eq,
            value,
        } = expr
            && column == col_name
            && !value.is_null()
        {
            return Some(value.clone());
        }
    }
    None
}

type ValueRangeBounds = (Bound<Vec<Value>>, Bound<Vec<Value>>);

fn choose_access_path(schema: &TableSchema, filter: Option<&Expr>) -> AccessPath {
    let Some(expr) = filter else {
        return AccessPath::SeqScan;
    };

    let mut conjuncts = Vec::new();
    collect_conjuncts(expr, &mut conjuncts);

    // 1. Check full Primary Key equality match first (O(log N) direct lookup).
    let mut pk_vals = Vec::with_capacity(schema.primary_key_cols.len());
    let mut all_pk_matched = !schema.primary_key_cols.is_empty();
    for &pk_cid in &schema.primary_key_cols {
        let Some(pos) = schema.column_index_by_id(pk_cid) else {
            all_pk_matched = false;
            break;
        };
        let col_name = &schema.columns[pos].name;
        if let Some(v) = find_eq_value(&conjuncts, col_name) {
            pk_vals.push(v);
        } else {
            all_pk_matched = false;
            break;
        }
    }
    if all_pk_matched {
        return AccessPath::PrimaryKeyLookup { pk_values: pk_vals };
    }

    // 2. Match best secondary index prefix (longest equality prefix, or single-column range).
    let mut best_prefix: Option<(usize, String, Vec<Value>)> = None;
    let mut best_range: Option<(String, ValueRangeBounds)> = None;

    for idx in &schema.indexes {
        // Count consecutive leading equality predicates.
        let mut prefix_vals = Vec::new();
        for &cid in &idx.column_ids {
            let Some(pos) = schema.column_index_by_id(cid) else {
                break;
            };
            let col_name = &schema.columns[pos].name;
            if let Some(v) = find_eq_value(&conjuncts, col_name) {
                prefix_vals.push(v);
            } else {
                break;
            }
        }
        if !prefix_vals.is_empty() {
            let score = prefix_vals.len();
            if best_prefix
                .as_ref()
                .is_none_or(|(best_score, _, _)| score > *best_score)
            {
                best_prefix = Some((score, idx.name.clone(), prefix_vals));
            }
            continue;
        }

        // If no equality on col 0, check range predicates on the index's leading column.
        if let Some(&first_cid) = idx.column_ids.first()
            && let Some(pos) = schema.column_index_by_id(first_cid)
        {
            let col_name = &schema.columns[pos].name;
            let dir = idx.directions.first().copied().unwrap_or(SortOrder::Asc);
            if let Some(bounds) = extract_leading_range_bounds(&conjuncts, col_name, dir)
                && best_range.is_none()
            {
                best_range = Some((idx.name.clone(), bounds));
            }
        }
    }

    if let Some((_score, index_name, prefix_values)) = best_prefix {
        return AccessPath::IndexPrefixScan {
            index_name,
            prefix_values,
        };
    }

    if let Some((index_name, (start, end))) = best_range {
        return AccessPath::IndexRangeScan {
            index_name,
            start,
            end,
        };
    }

    AccessPath::SeqScan
}

/// Extracts logical lower/upper bounds on `col_name` and maps them to bytewise index bounds
/// accounting for `SortOrder::Asc` vs `SortOrder::Desc`.
fn extract_leading_range_bounds(
    conjuncts: &[&Expr],
    col_name: &str,
    dir: SortOrder,
) -> Option<ValueRangeBounds> {
    let mut logical_low: Bound<Value> = Bound::Unbounded;
    let mut logical_high: Bound<Value> = Bound::Unbounded;

    for expr in conjuncts {
        let Expr::Compare { column, op, value } = expr else {
            continue;
        };
        if column != col_name || value.is_null() {
            continue;
        }
        match op {
            CmpOp::Gt => logical_low = tighten_lower(logical_low, Bound::Excluded(value.clone())),
            CmpOp::Gte => logical_low = tighten_lower(logical_low, Bound::Included(value.clone())),
            CmpOp::Lt => logical_high = tighten_upper(logical_high, Bound::Excluded(value.clone())),
            CmpOp::Lte => {
                logical_high = tighten_upper(logical_high, Bound::Included(value.clone()))
            }
            CmpOp::Eq | CmpOp::Ne => {}
        }
    }

    if matches!(logical_low, Bound::Unbounded) && matches!(logical_high, Bound::Unbounded) {
        return None;
    }

    let wrap = |b: Bound<Value>| match b {
        Bound::Included(v) => Bound::Included(vec![v]),
        Bound::Excluded(v) => Bound::Excluded(vec![v]),
        Bound::Unbounded => Bound::Unbounded,
    };

    // For DESC indexes, larger logical values have smaller encoded bytes, so low/high swap.
    match dir {
        SortOrder::Asc => Some((wrap(logical_low), wrap(logical_high))),
        SortOrder::Desc => Some((wrap(logical_high), wrap(logical_low))),
    }
}

fn tighten_lower(cur: Bound<Value>, cand: Bound<Value>) -> Bound<Value> {
    match (&cur, &cand) {
        (Bound::Unbounded, _) => cand,
        (_, Bound::Unbounded) => cur,
        (Bound::Included(a), Bound::Included(b)) if b > a => cand,
        (Bound::Included(a), Bound::Excluded(b)) if b >= a => cand,
        (Bound::Excluded(a), Bound::Included(b)) if b > a => cand,
        (Bound::Excluded(a), Bound::Excluded(b)) if b > a => cand,
        _ => cur,
    }
}

fn tighten_upper(cur: Bound<Value>, cand: Bound<Value>) -> Bound<Value> {
    match (&cur, &cand) {
        (Bound::Unbounded, _) => cand,
        (_, Bound::Unbounded) => cur,
        (Bound::Included(a), Bound::Included(b)) if b < a => cand,
        (Bound::Included(a), Bound::Excluded(b)) if b <= a => cand,
        (Bound::Excluded(a), Bound::Included(b)) if b < a => cand,
        (Bound::Excluded(a), Bound::Excluded(b)) if b < a => cand,
        _ => cur,
    }
}
