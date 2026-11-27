//! Columnar `DataChunk` batch representation and pull-based relational execution operators.
//!
//! Processing rows in `DataChunk` batches (default 256 rows) lays the interface foundation
//! for future SIMD vectorized execution while keeping per-query memory strictly bounded.

use core::cmp::Ordering;
use core::fmt;
use std::collections::{BTreeMap, BTreeSet};

use kv_core::KvError;
use table_codec::{CodecError, ColumnType, SortOrder, Value};

use crate::QueryConfig;

/// Unified SQL parse, bind, plan, and execution error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlError {
    Parse(String),
    Bind(String),
    Unsupported(String),
    Execution(String),
    Transaction(String),
    MemoryBudgetExceeded { limit: usize, used: usize },
    Codec(CodecError),
    Kv(KvError),
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(msg) => write!(f, "SQL parse error: {msg}"),
            Self::Bind(msg) => write!(f, "SQL bind error: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported SQL feature: {msg}"),
            Self::Execution(msg) => write!(f, "SQL execution error: {msg}"),
            Self::Transaction(msg) => write!(f, "SQL transaction error: {msg}"),
            Self::MemoryBudgetExceeded { limit, used } => {
                write!(
                    f,
                    "query memory budget exceeded: used {used} bytes (limit {limit} bytes)"
                )
            }
            Self::Codec(err) => write!(f, "{err}"),
            Self::Kv(err) => write!(f, "{err}"),
        }
    }
}

impl core::error::Error for SqlError {}

impl From<CodecError> for SqlError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<KvError> for SqlError {
    fn from(value: KvError) -> Self {
        Self::Kv(value)
    }
}

/// Output column metadata carried across `DataChunk` operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub table: Option<String>,
    pub name: String,
    pub data_type: Option<ColumnType>,
}

impl ColumnMeta {
    #[must_use]
    pub fn new(
        table: Option<String>,
        name: impl Into<String>,
        data_type: Option<ColumnType>,
    ) -> Self {
        Self {
            table,
            name: name.into(),
            data_type,
        }
    }
}

/// Columnar batch of rows passed between pull-based [`Executor`] operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataChunk {
    columns: Vec<Vec<Value>>,
    num_rows: usize,
}

impl DataChunk {
    #[must_use]
    pub fn empty(num_cols: usize) -> Self {
        Self {
            columns: vec![Vec::new(); num_cols],
            num_rows: 0,
        }
    }

    #[must_use]
    pub fn from_columns(columns: Vec<Vec<Value>>) -> Self {
        let num_rows = columns.first().map_or(0, Vec::len);
        Self { columns, num_rows }
    }

    #[must_use]
    pub fn from_rows(rows: &[Vec<Value>], num_cols: usize) -> Self {
        let mut columns: Vec<Vec<Value>> = (0..num_cols)
            .map(|_| Vec::with_capacity(rows.len()))
            .collect();
        for row in rows {
            for (c_idx, col_vec) in columns.iter_mut().enumerate() {
                let val = row.get(c_idx).cloned().unwrap_or(Value::Null);
                col_vec.push(val);
            }
        }
        Self {
            columns,
            num_rows: rows.len(),
        }
    }

    #[must_use]
    pub const fn num_rows(&self) -> usize {
        self.num_rows
    }

    #[must_use]
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    #[must_use]
    pub fn columns(&self) -> &[Vec<Value>] {
        &self.columns
    }

    #[must_use]
    pub fn cell(&self, col_idx: usize, row_idx: usize) -> Option<&Value> {
        self.columns.get(col_idx).and_then(|c| c.get(row_idx))
    }

    #[must_use]
    pub fn row_values(&self, row_idx: usize) -> Vec<Value> {
        self.columns
            .iter()
            .map(|col| col.get(row_idx).cloned().unwrap_or(Value::Null))
            .collect()
    }

    #[must_use]
    pub fn to_rows(&self) -> Vec<Vec<Value>> {
        (0..self.num_rows).map(|r| self.row_values(r)).collect()
    }
}

/// Pull-based vectorized/batch execution operator interface.
pub trait Executor {
    fn schema(&self) -> &[ColumnMeta];
    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError>;
}

/// Binary operator in a [`BoundExpr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Like,
}

/// Unary operator in a [`BoundExpr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    IsNull,
    IsNotNull,
}

/// Column-index-bound scalar expression evaluated directly over [`DataChunk`] rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundExpr {
    Column(usize),
    Literal(Value),
    BinaryOp {
        left: Box<BoundExpr>,
        op: BinOp,
        right: Box<BoundExpr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<BoundExpr>,
    },
    InList {
        expr: Box<BoundExpr>,
        list: Vec<BoundExpr>,
        negated: bool,
    },
    ScalarFunc {
        name: String,
        args: Vec<BoundExpr>,
    },
}

impl BoundExpr {
    pub fn eval_row(&self, chunk: &DataChunk, row_idx: usize) -> Result<Value, SqlError> {
        match self {
            Self::Column(idx) => chunk
                .cell(*idx, row_idx)
                .cloned()
                .ok_or_else(|| SqlError::Execution(format!("column index {idx} out of bounds"))),
            Self::Literal(v) => Ok(v.clone()),
            Self::UnaryOp { op, expr } => {
                let v = expr.eval_row(chunk, row_idx)?;
                eval_unary(*op, v)
            }
            Self::BinaryOp { left, op, right } => {
                let lv = left.eval_row(chunk, row_idx)?;
                let rv = right.eval_row(chunk, row_idx)?;
                eval_binary(*op, lv, rv)
            }
            Self::InList {
                expr,
                list,
                negated,
            } => {
                let target = expr.eval_row(chunk, row_idx)?;
                if target.is_null() {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in list {
                    let v = item.eval_row(chunk, row_idx)?;
                    if v.is_null() {
                        saw_null = true;
                    } else if values_equal_sql(&target, &v)? {
                        return Ok(Value::Bool(!*negated));
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Self::ScalarFunc { name, args } => {
                let mut evaluated = Vec::with_capacity(args.len());
                for a in args {
                    evaluated.push(a.eval_row(chunk, row_idx)?);
                }
                eval_scalar_func(name, &evaluated)
            }
        }
    }

    /// Evaluates expression for a full [`DataChunk`] column in one pass.
    pub fn eval_chunk(&self, chunk: &DataChunk) -> Result<Vec<Value>, SqlError> {
        let mut out = Vec::with_capacity(chunk.num_rows());
        for r in 0..chunk.num_rows() {
            out.push(self.eval_row(chunk, r)?);
        }
        Ok(out)
    }
}

fn eval_unary(op: UnaryOp, val: Value) -> Result<Value, SqlError> {
    match op {
        UnaryOp::IsNull => Ok(Value::Bool(val.is_null())),
        UnaryOp::IsNotNull => Ok(Value::Bool(!val.is_null())),
        UnaryOp::Not => match val {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            _ => Err(SqlError::Execution("NOT expects boolean operand".into())),
        },
        UnaryOp::Neg => match val {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => n
                .checked_neg()
                .map(Value::Int64)
                .ok_or_else(|| SqlError::Execution("integer overflow in unary negation".into())),
            Value::Float64(f) => Ok(Value::Float64(-f)),
            _ => Err(SqlError::Execution(
                "unary '-' expects numeric operand".into(),
            )),
        },
    }
}

fn eval_binary(op: BinOp, left: Value, right: Value) -> Result<Value, SqlError> {
    match op {
        BinOp::And => {
            return Ok(match (left, right) {
                (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
                (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                (Value::Null, _) | (_, Value::Null) => Value::Null,
                _ => {
                    return Err(SqlError::Execution("AND expects boolean operands".into()));
                }
            });
        }
        BinOp::Or => {
            return Ok(match (left, right) {
                (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
                (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
                (Value::Null, _) | (_, Value::Null) => Value::Null,
                _ => {
                    return Err(SqlError::Execution("OR expects boolean operands".into()));
                }
            });
        }
        _ => {}
    }

    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    match op {
        BinOp::Add => eval_arith(left, right, |a, b| a.checked_add(b), |a, b| a + b),
        BinOp::Sub => eval_arith(left, right, |a, b| a.checked_sub(b), |a, b| a - b),
        BinOp::Mul => eval_arith(left, right, |a, b| a.checked_mul(b), |a, b| a * b),
        BinOp::Div => match (&left, &right) {
            (Value::Int64(_), Value::Int64(0)) => {
                Err(SqlError::Execution("division by zero".into()))
            }
            (Value::Float64(_), Value::Float64(r)) if *r == 0.0 => {
                Err(SqlError::Execution("division by zero".into()))
            }
            (Value::Int64(_), Value::Float64(r)) if *r == 0.0 => {
                Err(SqlError::Execution("division by zero".into()))
            }
            (Value::Float64(_), Value::Int64(0)) => {
                Err(SqlError::Execution("division by zero".into()))
            }
            _ => eval_arith(left, right, |a, b| a.checked_div(b), |a, b| a / b),
        },
        BinOp::Eq => Ok(Value::Bool(values_equal_sql(&left, &right)?)),
        BinOp::Ne => Ok(Value::Bool(!values_equal_sql(&left, &right)?)),
        BinOp::Lt => Ok(Value::Bool(compare_sql(&left, &right)? == Ordering::Less)),
        BinOp::Lte => Ok(Value::Bool(
            compare_sql(&left, &right)? != Ordering::Greater,
        )),
        BinOp::Gt => Ok(Value::Bool(
            compare_sql(&left, &right)? == Ordering::Greater,
        )),
        BinOp::Gte => Ok(Value::Bool(compare_sql(&left, &right)? != Ordering::Less)),
        BinOp::Like => match (left, right) {
            (Value::Text(s), Value::Text(pat)) => Ok(Value::Bool(sql_like(&s, &pat))),
            _ => Err(SqlError::Execution("LIKE expects text operands".into())),
        },
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

fn eval_arith(
    left: Value,
    right: Value,
    int_op: fn(i64, i64) -> Option<i64>,
    float_op: fn(f64, f64) -> f64,
) -> Result<Value, SqlError> {
    match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => int_op(a, b)
            .map(Value::Int64)
            .ok_or_else(|| SqlError::Execution("integer arithmetic overflow".into())),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(float_op(a, b))),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(float_op(a as f64, b))),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(float_op(a, b as f64))),
        _ => Err(SqlError::Execution(
            "arithmetic operators require numeric operands".into(),
        )),
    }
}

fn compare_sql(left: &Value, right: &Value) -> Result<Ordering, SqlError> {
    match (left, right) {
        (Value::Int64(a), Value::Float64(b)) => Ok((*a as f64).total_cmp(b)),
        (Value::Float64(a), Value::Int64(b)) => Ok(a.total_cmp(&(*b as f64))),
        (a, b) if a.data_type() == b.data_type() => Ok(a.cmp(b)),
        _ => Err(SqlError::Execution(format!(
            "cannot compare incompatible types {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn values_equal_sql(left: &Value, right: &Value) -> Result<bool, SqlError> {
    Ok(compare_sql(left, right)? == Ordering::Equal)
}

fn sql_like(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star_pi = Some(pi);
            pi += 1;
            star_ti = ti;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn eval_scalar_func(name: &str, args: &[Value]) -> Result<Value, SqlError> {
    match name {
        "version" => Ok(Value::Text(format!(
            "AnchorDB {} (PostgreSQL-compatible)",
            env!("CARGO_PKG_VERSION")
        ))),
        "lower" => match args {
            [Value::Null] => Ok(Value::Null),
            [Value::Text(s)] => Ok(Value::Text(s.to_lowercase())),
            _ => Err(SqlError::Execution("LOWER expects 1 text argument".into())),
        },
        "upper" => match args {
            [Value::Null] => Ok(Value::Null),
            [Value::Text(s)] => Ok(Value::Text(s.to_uppercase())),
            _ => Err(SqlError::Execution("UPPER expects 1 text argument".into())),
        },
        "length" => match args {
            [Value::Null] => Ok(Value::Null),
            [Value::Text(s)] => Ok(Value::Int64(s.chars().count() as i64)),
            [Value::Blob(b)] => Ok(Value::Int64(b.len() as i64)),
            _ => Err(SqlError::Execution(
                "LENGTH expects 1 text/blob argument".into(),
            )),
        },
        "coalesce" => {
            for v in args {
                if !v.is_null() {
                    return Ok(v.clone());
                }
            }
            Ok(Value::Null)
        }
        other => Err(SqlError::Unsupported(format!(
            "unknown scalar function '{other}'"
        ))),
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

// ============================================================================
// Pull-based DataChunk Executors
// ============================================================================

/// Yields rows from a materialized scan (`SeqScan` / `PkLookup` / `IndexScan` / `VALUES`)
/// sliced into `chunk_rows` [`DataChunk`] batches.
pub struct ChunkSourceExec {
    schema: Vec<ColumnMeta>,
    rows: Vec<Vec<Value>>,
    cursor: usize,
    chunk_rows: usize,
}

impl ChunkSourceExec {
    #[must_use]
    pub fn new(schema: Vec<ColumnMeta>, rows: Vec<Vec<Value>>, chunk_rows: usize) -> Self {
        Self {
            schema,
            rows,
            cursor: 0,
            chunk_rows: chunk_rows.max(1),
        }
    }
}

impl Executor for ChunkSourceExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        if self.cursor >= self.rows.len() {
            return Ok(None);
        }
        let end = (self.cursor + self.chunk_rows).min(self.rows.len());
        let chunk = DataChunk::from_rows(&self.rows[self.cursor..end], self.schema.len());
        self.cursor = end;
        Ok(Some(chunk))
    }
}

/// Pulls [`DataChunk`] batches and retains rows where `predicate` evaluates to `Bool(true)`.
pub struct FilterExec {
    input: Box<dyn Executor>,
    predicate: BoundExpr,
    schema: Vec<ColumnMeta>,
}

impl FilterExec {
    #[must_use]
    pub fn new(input: Box<dyn Executor>, predicate: BoundExpr) -> Self {
        let schema = input.schema().to_vec();
        Self {
            input,
            predicate,
            schema,
        }
    }
}

impl Executor for FilterExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        while let Some(chunk) = self.input.next_chunk()? {
            let mut kept_rows = Vec::new();
            for r in 0..chunk.num_rows() {
                if matches!(self.predicate.eval_row(&chunk, r)?, Value::Bool(true)) {
                    kept_rows.push(chunk.row_values(r));
                }
            }
            if !kept_rows.is_empty() {
                return Ok(Some(DataChunk::from_rows(&kept_rows, self.schema.len())));
            }
        }
        Ok(None)
    }
}

/// Evaluates projection expressions column-by-column over incoming [`DataChunk`]s.
pub struct ProjectionExec {
    input: Box<dyn Executor>,
    exprs: Vec<BoundExpr>,
    schema: Vec<ColumnMeta>,
}

impl ProjectionExec {
    #[must_use]
    pub fn new(input: Box<dyn Executor>, exprs: Vec<BoundExpr>, schema: Vec<ColumnMeta>) -> Self {
        Self {
            input,
            exprs,
            schema,
        }
    }
}

impl Executor for ProjectionExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        let Some(chunk) = self.input.next_chunk()? else {
            return Ok(None);
        };
        let mut out_cols = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            out_cols.push(expr.eval_chunk(&chunk)?);
        }
        Ok(Some(DataChunk::from_columns(out_cols)))
    }
}

/// Join type supported by [`HashJoinExec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

/// Equi-join executor building a bounded in-memory hash table over the right side
/// and streaming left-side [`DataChunk`]s through it.
pub struct HashJoinExec {
    left: Box<dyn Executor>,
    right: Option<Box<dyn Executor>>,
    join_kind: JoinKind,
    left_keys: Vec<BoundExpr>,
    right_keys: Vec<BoundExpr>,
    extra_filter: Option<BoundExpr>,
    schema: Vec<ColumnMeta>,
    hash_table: BTreeMap<Vec<Value>, Vec<Vec<Value>>>,
    built: bool,
    config: QueryConfig,
}

impl HashJoinExec {
    #[must_use]
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_kind: JoinKind,
        left_keys: Vec<BoundExpr>,
        right_keys: Vec<BoundExpr>,
        extra_filter: Option<BoundExpr>,
        config: QueryConfig,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend_from_slice(right.schema());
        Self {
            left,
            right: Some(right),
            join_kind,
            left_keys,
            right_keys,
            extra_filter,
            schema,
            hash_table: BTreeMap::new(),
            built: false,
            config,
        }
    }

    fn build_right(&mut self) -> Result<(), SqlError> {
        if self.built {
            return Ok(());
        }
        let Some(mut right_exec) = self.right.take() else {
            self.built = true;
            return Ok(());
        };
        let mut used_bytes = 0usize;
        while let Some(chunk) = right_exec.next_chunk()? {
            for r in 0..chunk.num_rows() {
                let mut key = Vec::with_capacity(self.right_keys.len());
                let mut has_null = false;
                for ke in &self.right_keys {
                    let v = ke.eval_row(&chunk, r)?;
                    if v.is_null() {
                        has_null = true;
                    }
                    key.push(v);
                }
                // In SQL equi-joins, NULL never equals NULL.
                if has_null && !self.right_keys.is_empty() {
                    continue;
                }
                let row = chunk.row_values(r);
                used_bytes = used_bytes.saturating_add(estimate_row_bytes(&row));
                if used_bytes > self.config.memory_budget_bytes {
                    return Err(SqlError::MemoryBudgetExceeded {
                        limit: self.config.memory_budget_bytes,
                        used: used_bytes,
                    });
                }
                self.hash_table.entry(key).or_default().push(row);
            }
        }
        self.built = true;
        Ok(())
    }
}

impl Executor for HashJoinExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        self.build_right()?;
        while let Some(left_chunk) = self.left.next_chunk()? {
            let mut out_rows = Vec::new();
            for r in 0..left_chunk.num_rows() {
                let left_row = left_chunk.row_values(r);
                let mut key = Vec::with_capacity(self.left_keys.len());
                let mut has_null = false;
                for ke in &self.left_keys {
                    let v = ke.eval_row(&left_chunk, r)?;
                    if v.is_null() {
                        has_null = true;
                    }
                    key.push(v);
                }

                let mut matched_any = false;
                if !(has_null && !self.left_keys.is_empty())
                    && let Some(candidates) = self.hash_table.get(&key)
                {
                    for right_row in candidates {
                        let mut combined = Vec::with_capacity(self.schema.len());
                        combined.extend_from_slice(&left_row);
                        combined.extend_from_slice(right_row);

                        let passes = if let Some(filter) = &self.extra_filter {
                            let single = DataChunk::from_rows(
                                core::slice::from_ref(&combined),
                                self.schema.len(),
                            );
                            matches!(filter.eval_row(&single, 0)?, Value::Bool(true))
                        } else {
                            true
                        };
                        if passes {
                            out_rows.push(combined);
                            matched_any = true;
                        }
                    }
                }

                if !matched_any && self.join_kind == JoinKind::Left {
                    let mut combined = Vec::with_capacity(self.schema.len());
                    combined.extend_from_slice(&left_row);
                    combined.resize(self.schema.len(), Value::Null);
                    out_rows.push(combined);
                }
            }
            if !out_rows.is_empty() {
                return Ok(Some(DataChunk::from_rows(&out_rows, self.schema.len())));
            }
        }
        Ok(None)
    }
}

/// Supported aggregate functions in [`HashAggregateExec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateExpr {
    CountStar,
    Count(BoundExpr),
    Sum(BoundExpr),
    Min(BoundExpr),
    Max(BoundExpr),
    Avg(BoundExpr),
}

#[derive(Debug, Clone)]
enum AggState {
    Count(i64),
    Sum {
        has_val: bool,
        is_float: bool,
        int_sum: i64,
        float_sum: f64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        count: i64,
        sum: f64,
    },
}

impl AggState {
    fn new(expr: &AggregateExpr) -> Self {
        match expr {
            AggregateExpr::CountStar | AggregateExpr::Count(_) => Self::Count(0),
            AggregateExpr::Sum(_) => Self::Sum {
                has_val: false,
                is_float: false,
                int_sum: 0,
                float_sum: 0.0,
            },
            AggregateExpr::Min(_) => Self::Min(None),
            AggregateExpr::Max(_) => Self::Max(None),
            AggregateExpr::Avg(_) => Self::Avg { count: 0, sum: 0.0 },
        }
    }

    fn update(
        &mut self,
        expr: &AggregateExpr,
        chunk: &DataChunk,
        row_idx: usize,
    ) -> Result<(), SqlError> {
        match (self, expr) {
            (Self::Count(c), AggregateExpr::CountStar) => {
                *c = c.saturating_add(1);
            }
            (Self::Count(c), AggregateExpr::Count(e)) => {
                if !e.eval_row(chunk, row_idx)?.is_null() {
                    *c = c.saturating_add(1);
                }
            }
            (
                Self::Sum {
                    has_val,
                    is_float,
                    int_sum,
                    float_sum,
                },
                AggregateExpr::Sum(e),
            ) => match e.eval_row(chunk, row_idx)? {
                Value::Null => {}
                Value::Int64(n) => {
                    *has_val = true;
                    if *is_float {
                        *float_sum += n as f64;
                    } else {
                        *int_sum = int_sum
                            .checked_add(n)
                            .ok_or_else(|| SqlError::Execution("SUM overflow".into()))?;
                    }
                }
                Value::Float64(f) => {
                    *has_val = true;
                    if !*is_float {
                        *is_float = true;
                        *float_sum = (*int_sum as f64) + f;
                    } else {
                        *float_sum += f;
                    }
                }
                _ => return Err(SqlError::Execution("SUM expects numeric column".into())),
            },
            (Self::Min(cur), AggregateExpr::Min(e)) => {
                let v = e.eval_row(chunk, row_idx)?;
                if !v.is_null() && cur.as_ref().is_none_or(|c| v < *c) {
                    *cur = Some(v);
                }
            }
            (Self::Max(cur), AggregateExpr::Max(e)) => {
                let v = e.eval_row(chunk, row_idx)?;
                if !v.is_null() && cur.as_ref().is_none_or(|c| v > *c) {
                    *cur = Some(v);
                }
            }
            (Self::Avg { count, sum }, AggregateExpr::Avg(e)) => {
                match e.eval_row(chunk, row_idx)? {
                    Value::Null => {}
                    Value::Int64(n) => {
                        *count += 1;
                        *sum += n as f64;
                    }
                    Value::Float64(f) => {
                        *count += 1;
                        *sum += f;
                    }
                    _ => return Err(SqlError::Execution("AVG expects numeric column".into())),
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn finalize(self) -> Value {
        match self {
            Self::Count(c) => Value::Int64(c),
            Self::Sum {
                has_val,
                is_float,
                int_sum,
                float_sum,
            } => {
                if !has_val {
                    Value::Null
                } else if is_float {
                    Value::Float64(float_sum)
                } else {
                    Value::Int64(int_sum)
                }
            }
            Self::Min(v) | Self::Max(v) => v.unwrap_or(Value::Null),
            Self::Avg { count, sum } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Float64(sum / (count as f64))
                }
            }
        }
    }
}

/// Hash aggregation operator producing `[group_by_cols..., agg_results...]`.
pub struct HashAggregateExec {
    input: Box<dyn Executor>,
    group_by: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: Vec<ColumnMeta>,
    config: QueryConfig,
    output_source: Option<ChunkSourceExec>,
}

impl HashAggregateExec {
    #[must_use]
    pub fn new(
        input: Box<dyn Executor>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        schema: Vec<ColumnMeta>,
        config: QueryConfig,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            schema,
            config,
            output_source: None,
        }
    }
}

impl Executor for HashAggregateExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        if self.output_source.is_none() {
            let mut groups: BTreeMap<Vec<Value>, Vec<AggState>> = BTreeMap::new();
            let mut used_bytes = 0usize;

            while let Some(chunk) = self.input.next_chunk()? {
                for r in 0..chunk.num_rows() {
                    let mut gkey = Vec::with_capacity(self.group_by.len());
                    for ge in &self.group_by {
                        gkey.push(ge.eval_row(&chunk, r)?);
                    }
                    let states = groups.entry(gkey.clone()).or_insert_with(|| {
                        used_bytes = used_bytes
                            .saturating_add(estimate_row_bytes(&gkey) + self.aggregates.len() * 32);
                        self.aggregates.iter().map(AggState::new).collect()
                    });
                    if used_bytes > self.config.memory_budget_bytes {
                        return Err(SqlError::MemoryBudgetExceeded {
                            limit: self.config.memory_budget_bytes,
                            used: used_bytes,
                        });
                    }
                    for (state, agg) in states.iter_mut().zip(self.aggregates.iter()) {
                        state.update(agg, &chunk, r)?;
                    }
                }
            }

            // Global aggregate without GROUP BY over an empty table still yields 1 row (e.g. COUNT(*)=0).
            if groups.is_empty() && self.group_by.is_empty() {
                groups.insert(
                    Vec::new(),
                    self.aggregates.iter().map(AggState::new).collect(),
                );
            }

            let mut out_rows = Vec::with_capacity(groups.len());
            for (mut gkey, states) in groups {
                for s in states {
                    gkey.push(s.finalize());
                }
                out_rows.push(gkey);
            }
            self.output_source = Some(ChunkSourceExec::new(
                self.schema.clone(),
                out_rows,
                self.config.chunk_rows,
            ));
        }

        match &mut self.output_source {
            Some(src) => src.next_chunk(),
            None => Ok(None),
        }
    }
}

/// Deduplicates output rows under `memory_budget_bytes`.
pub struct DistinctExec {
    input: Box<dyn Executor>,
    schema: Vec<ColumnMeta>,
    seen: BTreeSet<Vec<Value>>,
    used_bytes: usize,
    config: QueryConfig,
}

impl DistinctExec {
    #[must_use]
    pub fn new(input: Box<dyn Executor>, config: QueryConfig) -> Self {
        let schema = input.schema().to_vec();
        Self {
            input,
            schema,
            seen: BTreeSet::new(),
            used_bytes: 0,
            config,
        }
    }
}

impl Executor for DistinctExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        while let Some(chunk) = self.input.next_chunk()? {
            let mut out_rows = Vec::new();
            for r in 0..chunk.num_rows() {
                let row = chunk.row_values(r);
                if self.seen.insert(row.clone()) {
                    self.used_bytes = self.used_bytes.saturating_add(estimate_row_bytes(&row));
                    if self.used_bytes > self.config.memory_budget_bytes {
                        return Err(SqlError::MemoryBudgetExceeded {
                            limit: self.config.memory_budget_bytes,
                            used: self.used_bytes,
                        });
                    }
                    out_rows.push(row);
                }
            }
            if !out_rows.is_empty() {
                return Ok(Some(DataChunk::from_rows(&out_rows, self.schema.len())));
            }
        }
        Ok(None)
    }
}

/// Multi-key sort + OFFSET + LIMIT operator with memory quota protection.
pub struct TopNSortExec {
    input: Box<dyn Executor>,
    order_by: Vec<(BoundExpr, SortOrder)>,
    offset: usize,
    limit: Option<usize>,
    schema: Vec<ColumnMeta>,
    config: QueryConfig,
    output_source: Option<ChunkSourceExec>,
}

impl TopNSortExec {
    #[must_use]
    pub fn new(
        input: Box<dyn Executor>,
        order_by: Vec<(BoundExpr, SortOrder)>,
        offset: usize,
        limit: Option<usize>,
        config: QueryConfig,
    ) -> Self {
        let schema = input.schema().to_vec();
        Self {
            input,
            order_by,
            offset,
            limit,
            schema,
            config,
            output_source: None,
        }
    }
}

impl Executor for TopNSortExec {
    fn schema(&self) -> &[ColumnMeta] {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>, SqlError> {
        if self.output_source.is_none() {
            let mut keyed_rows: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
            let mut used_bytes = 0usize;

            while let Some(chunk) = self.input.next_chunk()? {
                for r in 0..chunk.num_rows() {
                    let row = chunk.row_values(r);
                    let mut sort_keys = Vec::with_capacity(self.order_by.len());
                    for (expr, _) in &self.order_by {
                        sort_keys.push(expr.eval_row(&chunk, r)?);
                    }
                    used_bytes = used_bytes
                        .saturating_add(estimate_row_bytes(&row) + estimate_row_bytes(&sort_keys));
                    if used_bytes > self.config.memory_budget_bytes {
                        return Err(SqlError::MemoryBudgetExceeded {
                            limit: self.config.memory_budget_bytes,
                            used: used_bytes,
                        });
                    }
                    keyed_rows.push((sort_keys, row));
                }
            }

            if !self.order_by.is_empty() {
                keyed_rows.sort_by(|(ka, _), (kb, _)| {
                    for (idx, (_, dir)) in self.order_by.iter().enumerate() {
                        let ord = ka[idx].cmp(&kb[idx]);
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

            let iter = keyed_rows.into_iter().map(|(_, row)| row).skip(self.offset);
            let final_rows: Vec<Vec<Value>> = match self.limit {
                Some(n) => iter.take(n).collect(),
                None => iter.collect(),
            };

            self.output_source = Some(ChunkSourceExec::new(
                self.schema.clone(),
                final_rows,
                self.config.chunk_rows,
            ));
        }

        match &mut self.output_source {
            Some(src) => src.next_chunk(),
            None => Ok(None),
        }
    }
}

/// Drains an entire [`Executor`] pipeline into `(column_names, rows)`.
pub fn collect_executor(
    mut exec: Box<dyn Executor>,
) -> Result<(Vec<String>, Vec<Vec<Value>>), SqlError> {
    let col_names: Vec<String> = exec.schema().iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();
    while let Some(chunk) = exec.next_chunk()? {
        rows.extend(chunk.to_rows());
    }
    Ok((col_names, rows))
}
