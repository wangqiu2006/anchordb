//! `SELECT` binder, RBO index selector, `EXPLAIN` plan formatter, and `DataChunk` executor assembly.

use core::ops::Bound;

use sqlparser::ast::{
    BinaryOperator, Distinct, Expr as AstExpr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, InTarget, JoinConstraint, JoinOperator, LimitClause,
    OrderByKind, OrderBySort, Query, Select, SelectItem, SetExpr, TableFactor, UnaryOperator,
};

use kv_core::KvTransaction;
use table_codec::{CodecError, SortOrder, SystemCatalog, TableSchema, TableStore, Value};

use crate::QueryConfig;
use crate::api::{AccessPath, Expr as ApiExpr, TableQuery as ApiTableQuery, col as api_col};
use crate::chunk::{
    AggregateExpr, BinOp, BoundExpr, ChunkSourceExec, ColumnMeta, DistinctExec, Executor,
    FilterExec, HashAggregateExec, HashJoinExec, JoinKind, ProjectionExec, SqlError, TopNSortExec,
    UnaryOp, collect_executor,
};
use crate::sql::{SqlResult, eval_ast_literal, extract_table_factor_name};

pub(crate) fn table_column_metas(schema: &TableSchema, alias: Option<&str>) -> Vec<ColumnMeta> {
    let qualifier = alias.unwrap_or(&schema.name).to_owned();
    schema
        .columns
        .iter()
        .map(|c| ColumnMeta::new(Some(qualifier.clone()), c.name.clone(), Some(c.data_type)))
        .collect()
}

pub(crate) fn exec_query<T: KvTransaction>(
    txn: &T,
    query: &Query,
    config: &QueryConfig,
) -> Result<SqlResult, SqlError> {
    let select = extract_select(query)?;
    let (exec, _plan_lines) = build_select_pipeline(txn, query, select, config)?;
    let (columns, rows) = collect_executor(exec)?;
    Ok(SqlResult::Rows { columns, rows })
}

pub(crate) fn explain_query<T: KvTransaction>(
    txn: &T,
    query: &Query,
) -> Result<SqlResult, SqlError> {
    let select = extract_select(query)?;
    let (_exec, plan_lines) = build_select_pipeline(txn, query, select, &QueryConfig::default())?;
    let rows = plan_lines
        .into_iter()
        .map(|line| vec![Value::Text(line)])
        .collect();
    Ok(SqlResult::Rows {
        columns: vec!["QUERY PLAN".into()],
        rows,
    })
}

fn extract_select(query: &Query) -> Result<&Select, SqlError> {
    match query.body.as_ref() {
        SetExpr::Select(sel) => Ok(sel),
        _ => Err(SqlError::Unsupported(
            "only standard SELECT queries are currently supported".into(),
        )),
    }
}

fn build_select_pipeline<T: KvTransaction>(
    txn: &T,
    query: &Query,
    select: &Select,
    config: &QueryConfig,
) -> Result<(Box<dyn Executor>, Vec<String>), SqlError> {
    let mut plan_lines = Vec::new();

    // 1. FROM + JOIN (with RBO index selection on single-table queries).
    let mut exec: Box<dyn Executor> = if select.from.is_empty() {
        plan_lines.push("DummyRowScan (1 row)".to_owned());
        Box::new(ChunkSourceExec::new(Vec::new(), vec![Vec::new()], 1))
    } else {
        if select.from.len() > 1 {
            return Err(SqlError::Unsupported(
                "implicit comma joins are not supported; use explicit INNER/LEFT JOIN".into(),
            ));
        }
        let twj = &select.from[0];
        let (base_name, base_alias) = extract_table_and_alias(&twj.relation)?;
        let base_schema = SystemCatalog::get_table_by_name(txn, &base_name)?
            .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(base_name.clone())))?;
        let base_metas = table_column_metas(&base_schema, base_alias.as_deref());

        // Single-table RBO index selection when there are no joins.
        let (base_rows, scan_desc) = if twj.joins.is_empty() {
            plan_and_scan_table(txn, &base_schema, select.selection.as_ref())?
        } else {
            (
                TableStore::scan_table(txn, &base_schema)?,
                format!("SeqScan on {}", base_schema.name),
            )
        };
        plan_lines.push(scan_desc);

        let mut left_exec: Box<dyn Executor> = Box::new(ChunkSourceExec::new(
            base_metas,
            base_rows,
            config.chunk_rows,
        ));

        // Attach explicit INNER / LEFT joins.
        for join in &twj.joins {
            let (join_kind, on_expr) = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(e))
                | JoinOperator::Join(JoinConstraint::On(e)) => (JoinKind::Inner, e),
                JoinOperator::LeftOuter(JoinConstraint::On(e))
                | JoinOperator::Left(JoinConstraint::On(e)) => (JoinKind::Left, e),
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported JOIN type or constraint: {other:?}"
                    )));
                }
            };
            let (r_name, r_alias) = extract_table_and_alias(&join.relation)?;
            let r_schema = SystemCatalog::get_table_by_name(txn, &r_name)?
                .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(r_name.clone())))?;
            let r_metas = table_column_metas(&r_schema, r_alias.as_deref());
            let r_rows = TableStore::scan_table(txn, &r_schema)?;

            let left_metas = left_exec.schema().to_vec();
            let mut combined_metas = left_metas.clone();
            combined_metas.extend_from_slice(&r_metas);

            let (left_keys, right_keys, extra_filter) =
                extract_join_keys(on_expr, &left_metas, &r_metas, &combined_metas)?;

            let right_exec: Box<dyn Executor> =
                Box::new(ChunkSourceExec::new(r_metas, r_rows, config.chunk_rows));
            plan_lines.push(format!(
                "HashJoin ({join_kind:?}) with SeqScan on {}",
                r_schema.name
            ));
            left_exec = Box::new(HashJoinExec::new(
                left_exec,
                right_exec,
                join_kind,
                left_keys,
                right_keys,
                extra_filter,
                *config,
            ));
        }

        left_exec
    };

    // 2. WHERE Filter.
    if let Some(where_expr) = &select.selection {
        let bound_pred = bind_expr(where_expr, exec.schema())?;
        plan_lines.push(format!("Filter ({where_expr})"));
        exec = Box::new(FilterExec::new(exec, bound_pred));
    }

    // 3. GROUP BY / Aggregates / Projection / HAVING.
    let group_by_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs.as_slice(),
        GroupByExpr::All(_) => {
            return Err(SqlError::Unsupported(
                "GROUP BY ALL is not supported".into(),
            ));
        }
    };

    let has_agg = !group_by_exprs.is_empty()
        || select.having.is_some()
        || select.projection.iter().any(projection_item_has_aggregate);

    // Keep track of output column names/exprs so ORDER BY can reference aliases or input columns.
    if has_agg {
        exec =
            build_aggregate_and_projection(exec, select, group_by_exprs, config, &mut plan_lines)?;
    } else {
        let (proj_exprs, proj_metas) = bind_projection_items(&select.projection, exec.schema())?;
        // Attach ORDER BY before projection if ORDER BY references columns not in SELECT,
        // or after projection if referencing output aliases. Here we bind ORDER BY against
        // projection output first, falling back to pre-projection columns.
        if let Some(order_by) = &query.order_by {
            let order_specs = extract_order_specs(order_by)?;
            let can_bind_post = order_specs
                .iter()
                .all(|(e, _)| bind_expr(e, &proj_metas).is_ok());

            if !can_bind_post {
                let bound_orders: Vec<(BoundExpr, SortOrder)> = order_specs
                    .iter()
                    .map(|(e, d)| Ok((bind_expr(e, exec.schema())?, *d)))
                    .collect::<Result<_, SqlError>>()?;
                let (limit, offset) = extract_limit_offset(query)?;
                let apply_limit_here = select.distinct.is_none();
                plan_lines.push(format!("TopNSort (keys={})", bound_orders.len()));
                exec = Box::new(TopNSortExec::new(
                    exec,
                    bound_orders,
                    if apply_limit_here { offset } else { 0 },
                    if apply_limit_here { limit } else { None },
                    *config,
                ));
                plan_lines.push(format!("Projection ({} cols)", proj_metas.len()));
                exec = Box::new(ProjectionExec::new(exec, proj_exprs, proj_metas));
                if matches!(select.distinct, Some(Distinct::Distinct)) {
                    plan_lines.push("Distinct".to_owned());
                    exec = Box::new(DistinctExec::new(exec, *config));
                    if limit.is_some() || offset > 0 {
                        exec =
                            Box::new(TopNSortExec::new(exec, Vec::new(), offset, limit, *config));
                    }
                }
                plan_lines.reverse();
                return Ok((exec, plan_lines));
            }
        }

        plan_lines.push(format!("Projection ({} cols)", proj_metas.len()));
        exec = Box::new(ProjectionExec::new(exec, proj_exprs, proj_metas));
    }

    // 4. DISTINCT.
    if matches!(select.distinct, Some(Distinct::Distinct)) {
        plan_lines.push("Distinct".to_owned());
        exec = Box::new(DistinctExec::new(exec, *config));
    }

    // 5. ORDER BY + LIMIT + OFFSET.
    let (limit, offset) = extract_limit_offset(query)?;
    let order_specs = match &query.order_by {
        Some(ob) => extract_order_specs(ob)?,
        None => Vec::new(),
    };
    if !order_specs.is_empty() || limit.is_some() || offset > 0 {
        let bound_orders: Vec<(BoundExpr, SortOrder)> = order_specs
            .iter()
            .map(|(e, d)| Ok((bind_expr(e, exec.schema())?, *d)))
            .collect::<Result<_, SqlError>>()?;
        plan_lines.push(format!(
            "TopNSort (order_keys={}, limit={limit:?}, offset={offset})",
            bound_orders.len()
        ));
        exec = Box::new(TopNSortExec::new(
            exec,
            bound_orders,
            offset,
            limit,
            *config,
        ));
    }

    // Present EXPLAIN top-down (root operator first).
    plan_lines.reverse();
    Ok((exec, plan_lines))
}

fn plan_and_scan_table<T: KvTransaction>(
    txn: &T,
    schema: &TableSchema,
    where_ast: Option<&AstExpr>,
) -> Result<(Vec<Vec<Value>>, String), SqlError> {
    let api_filter = where_ast.and_then(try_lower_to_api_expr);
    let mut api_q = ApiTableQuery::new(&schema.name);
    if let Some(f) = api_filter {
        api_q = api_q.filter(f);
    }
    let plan = api_q.plan_for_schema(schema);

    match plan.access_path {
        AccessPath::PrimaryKeyLookup { pk_values } => {
            let rows = match TableStore::get_row_by_pk(txn, schema, &pk_values)? {
                Some(r) => vec![r],
                None => Vec::new(),
            };
            Ok((rows, format!("PrimaryKeyLookup on {}", schema.name)))
        }
        AccessPath::IndexPrefixScan {
            index_name,
            prefix_values,
        } => {
            let rows = TableStore::scan_index_prefix(txn, schema, &index_name, &prefix_values)?;
            Ok((
                rows,
                format!("IndexPrefixScan on {} using {index_name}", schema.name),
            ))
        }
        AccessPath::IndexRangeScan {
            index_name,
            start,
            end,
        } => {
            let rows = TableStore::scan_index_range(
                txn,
                schema,
                &index_name,
                match &start {
                    Bound::Included(v) => Bound::Included(v.as_slice()),
                    Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                },
                match &end {
                    Bound::Included(v) => Bound::Included(v.as_slice()),
                    Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                },
            )?;
            Ok((
                rows,
                format!("IndexRangeScan on {} using {index_name}", schema.name),
            ))
        }
        AccessPath::SeqScan => {
            let rows = TableStore::scan_table(txn, schema)?;
            Ok((rows, format!("SeqScan on {}", schema.name)))
        }
    }
}

/// Best-effort lowering of SQL `WHERE` predicates to `query_engine::api::Expr` for RBO index selection.
fn try_lower_to_api_expr(expr: &AstExpr) -> Option<ApiExpr> {
    match expr {
        AstExpr::Nested(inner) => try_lower_to_api_expr(inner),
        AstExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (try_lower_to_api_expr(left), try_lower_to_api_expr(right)) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        },
        AstExpr::BinaryOp { left, op, right } => {
            if let (Some(col_name), Some(val)) = (extract_col_name(left), extract_const_lit(right))
            {
                return build_cmp_api_expr(&col_name, op, val);
            }
            if let (Some(val), Some(col_name)) = (extract_const_lit(left), extract_col_name(right))
            {
                let flipped = match op {
                    BinaryOperator::Eq => BinaryOperator::Eq,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    _ => return None,
                };
                return build_cmp_api_expr(&col_name, &flipped, val);
            }
            None
        }
        _ => None,
    }
}

fn build_cmp_api_expr(col_name: &str, op: &BinaryOperator, val: Value) -> Option<ApiExpr> {
    match op {
        BinaryOperator::Eq => Some(api_col(col_name).eq(val)),
        BinaryOperator::Lt => Some(api_col(col_name).lt(val)),
        BinaryOperator::LtEq => Some(api_col(col_name).lte(val)),
        BinaryOperator::Gt => Some(api_col(col_name).gt(val)),
        BinaryOperator::GtEq => Some(api_col(col_name).gte(val)),
        _ => None,
    }
}

fn extract_col_name(expr: &AstExpr) -> Option<String> {
    match expr {
        AstExpr::Identifier(id) => Some(id.value.clone()),
        AstExpr::CompoundIdentifier(ids) => ids.last().map(|id| id.value.clone()),
        _ => None,
    }
}

fn extract_table_and_col_name(expr: &AstExpr) -> (Option<String>, Option<String>) {
    match expr {
        AstExpr::Identifier(id) => (None, Some(id.value.clone())),
        AstExpr::CompoundIdentifier(ids) => match ids.as_slice() {
            [tbl, col] => (Some(tbl.value.clone()), Some(col.value.clone())),
            [col] => (None, Some(col.value.clone())),
            _ => (None, ids.last().map(|id| id.value.clone())),
        },
        _ => (None, None),
    }
}

fn extract_const_lit(expr: &AstExpr) -> Option<Value> {
    match expr {
        AstExpr::Value(v) => eval_ast_literal(&v.value).ok(),
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match extract_const_lit(expr)? {
            Value::Int64(n) => n.checked_neg().map(Value::Int64),
            Value::Float64(f) => Some(Value::Float64(-f)),
            _ => None,
        },
        _ => None,
    }
}

fn extract_table_and_alias(factor: &TableFactor) -> Result<(String, Option<String>), SqlError> {
    let name = extract_table_factor_name(factor)?;
    let alias = match factor {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    };
    Ok((name, alias))
}

type JoinKeyExtraction = (Vec<BoundExpr>, Vec<BoundExpr>, Option<BoundExpr>);

fn extract_join_keys(
    on_expr: &AstExpr,
    left_metas: &[ColumnMeta],
    right_metas: &[ColumnMeta],
    combined_metas: &[ColumnMeta],
) -> Result<JoinKeyExtraction, SqlError> {
    let mut conjuncts = Vec::new();
    split_ast_and(on_expr, &mut conjuncts);

    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut residual = Vec::new();

    for c in conjuncts {
        if let AstExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = c
        {
            if let (Ok(lk), Ok(rk)) = (bind_expr(left, left_metas), bind_expr(right, right_metas)) {
                left_keys.push(lk);
                right_keys.push(rk);
                continue;
            }
            if let (Ok(rk), Ok(lk)) = (bind_expr(left, right_metas), bind_expr(right, left_metas)) {
                left_keys.push(lk);
                right_keys.push(rk);
                continue;
            }
        }
        residual.push(bind_expr(c, combined_metas)?);
    }

    let extra_filter = residual.into_iter().reduce(|a, b| BoundExpr::BinaryOp {
        left: Box::new(a),
        op: BinOp::And,
        right: Box::new(b),
    });

    Ok((left_keys, right_keys, extra_filter))
}

fn split_ast_and<'a>(expr: &'a AstExpr, out: &mut Vec<&'a AstExpr>) {
    match expr {
        AstExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_ast_and(left, out);
            split_ast_and(right, out);
        }
        AstExpr::Nested(inner) => split_ast_and(inner, out),
        other => out.push(other),
    }
}

// ============================================================================
// Aggregate & Projection Binding
// ============================================================================

fn build_aggregate_and_projection(
    input: Box<dyn Executor>,
    select: &Select,
    group_by_ast: &[AstExpr],
    config: &QueryConfig,
    plan_lines: &mut Vec<String>,
) -> Result<Box<dyn Executor>, SqlError> {
    let input_schema = input.schema().to_vec();

    // Bind GROUP BY expressions.
    let mut group_by_bound = Vec::with_capacity(group_by_ast.len());
    let mut agg_output_metas = Vec::new();
    for g in group_by_ast {
        group_by_bound.push(bind_expr(g, &input_schema)?);
        let name = extract_col_name(g).unwrap_or_else(|| g.to_string());
        agg_output_metas.push(ColumnMeta::new(None, name, None));
    }

    // Collect aggregate functions referenced in SELECT projection and HAVING.
    let mut aggregates: Vec<AggregateExpr> = Vec::new();
    let mut agg_ast_keys: Vec<String> = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                collect_aggregates_in_expr(e, &input_schema, &mut aggregates, &mut agg_ast_keys)?;
            }
            SelectItem::Wildcard(_)
            | SelectItem::QualifiedWildcard(_, _)
            | SelectItem::ExprWithAliases { .. } => {
                return Err(SqlError::Bind(
                    "SELECT * or multi-alias expressions cannot be combined with GROUP BY or aggregates".into(),
                ));
            }
        }
    }
    if let Some(having) = &select.having {
        collect_aggregates_in_expr(having, &input_schema, &mut aggregates, &mut agg_ast_keys)?;
    }

    for k in &agg_ast_keys {
        agg_output_metas.push(ColumnMeta::new(None, k.clone(), None));
    }

    plan_lines.push(format!(
        "HashAggregate (groups={}, aggs={})",
        group_by_bound.len(),
        aggregates.len()
    ));
    let mut exec: Box<dyn Executor> = Box::new(HashAggregateExec::new(
        input,
        group_by_bound,
        aggregates,
        agg_output_metas.clone(),
        *config,
    ));

    // Bind HAVING against HashAggregate output schema.
    if let Some(having) = &select.having {
        let bound_having = bind_post_agg_expr(having, group_by_ast, &agg_ast_keys)?;
        plan_lines.push(format!("Filter/Having ({having})"));
        exec = Box::new(FilterExec::new(exec, bound_having));
    }

    // Bind final SELECT projection against HashAggregate output schema.
    let mut proj_exprs = Vec::with_capacity(select.projection.len());
    let mut proj_metas = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                proj_exprs.push(bind_post_agg_expr(e, group_by_ast, &agg_ast_keys)?);
                let (tbl, col_opt) = extract_table_and_col_name(e);
                let name = col_opt.unwrap_or_else(|| e.to_string());
                proj_metas.push(ColumnMeta::new(tbl, name, None));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                proj_exprs.push(bind_post_agg_expr(expr, group_by_ast, &agg_ast_keys)?);
                proj_metas.push(ColumnMeta::new(None, alias.value.clone(), None));
            }
            SelectItem::Wildcard(_)
            | SelectItem::QualifiedWildcard(_, _)
            | SelectItem::ExprWithAliases { .. } => {
                return Err(SqlError::Bind(
                    "wildcard or multi-alias not allowed in aggregate query".into(),
                ));
            }
        }
    }

    plan_lines.push(format!("Projection ({} cols)", proj_metas.len()));
    Ok(Box::new(ProjectionExec::new(exec, proj_exprs, proj_metas)))
}

fn is_agg_func_name(name: &str) -> bool {
    matches!(name, "count" | "sum" | "min" | "max" | "avg")
}

fn projection_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_aggregate(e)
        }
        _ => false,
    }
}

fn expr_has_aggregate(expr: &AstExpr) -> bool {
    match expr {
        AstExpr::Function(func) => {
            let fname = func.name.to_string().to_lowercase();
            if is_agg_func_name(&fname) {
                return true;
            }
            if let FunctionArguments::List(arg_list) = &func.args {
                return arg_list.args.iter().any(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => expr_has_aggregate(e),
                    _ => false,
                });
            }
            false
        }
        AstExpr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        AstExpr::UnaryOp { expr, .. }
        | AstExpr::Nested(expr)
        | AstExpr::IsNull(expr)
        | AstExpr::IsNotNull(expr) => expr_has_aggregate(expr),
        _ => false,
    }
}

fn collect_aggregates_in_expr(
    expr: &AstExpr,
    input_schema: &[ColumnMeta],
    out_aggs: &mut Vec<AggregateExpr>,
    out_keys: &mut Vec<String>,
) -> Result<(), SqlError> {
    match expr {
        AstExpr::Function(func) => {
            let fname = func.name.to_string().to_lowercase();
            if is_agg_func_name(&fname) {
                let key = expr.to_string().to_lowercase();
                if !out_keys.contains(&key) {
                    let agg = build_agg_expr(func, &fname, input_schema)?;
                    out_aggs.push(agg);
                    out_keys.push(key);
                }
                return Ok(());
            }
            if let FunctionArguments::List(arg_list) = &func.args {
                for a in &arg_list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = a
                    {
                        collect_aggregates_in_expr(e, input_schema, out_aggs, out_keys)?;
                    }
                }
            }
            Ok(())
        }
        AstExpr::BinaryOp { left, right, .. } => {
            collect_aggregates_in_expr(left, input_schema, out_aggs, out_keys)?;
            collect_aggregates_in_expr(right, input_schema, out_aggs, out_keys)
        }
        AstExpr::UnaryOp { expr, .. }
        | AstExpr::Nested(expr)
        | AstExpr::IsNull(expr)
        | AstExpr::IsNotNull(expr) => {
            collect_aggregates_in_expr(expr, input_schema, out_aggs, out_keys)
        }
        _ => Ok(()),
    }
}

fn build_agg_expr(
    func: &Function,
    fname: &str,
    input_schema: &[ColumnMeta],
) -> Result<AggregateExpr, SqlError> {
    let args = match &func.args {
        FunctionArguments::List(list) => &list.args,
        _ => {
            return Err(SqlError::Bind(format!(
                "aggregate function '{fname}' requires argument list"
            )));
        }
    };
    if fname == "count" {
        if args.len() != 1 {
            return Err(SqlError::Bind("COUNT expects exactly 1 argument".into()));
        }
        return match &args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(AggregateExpr::CountStar),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                Ok(AggregateExpr::Count(bind_expr(e, input_schema)?))
            }
            _ => Err(SqlError::Bind("invalid COUNT argument".into())),
        };
    }

    if args.len() != 1 {
        return Err(SqlError::Bind(format!(
            "aggregate '{fname}' expects exactly 1 argument"
        )));
    }
    let inner = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => bind_expr(e, input_schema)?,
        _ => {
            return Err(SqlError::Bind(format!(
                "aggregate '{fname}' expects an expression argument"
            )));
        }
    };
    match fname {
        "sum" => Ok(AggregateExpr::Sum(inner)),
        "min" => Ok(AggregateExpr::Min(inner)),
        "max" => Ok(AggregateExpr::Max(inner)),
        "avg" => Ok(AggregateExpr::Avg(inner)),
        _ => Err(SqlError::Unsupported(format!(
            "unsupported aggregate function '{fname}'"
        ))),
    }
}

fn bind_post_agg_expr(
    expr: &AstExpr,
    group_by_ast: &[AstExpr],
    agg_keys: &[String],
) -> Result<BoundExpr, SqlError> {
    // Check if `expr` matches a GROUP BY expression.
    let expr_norm = expr.to_string().to_lowercase();
    for (i, g) in group_by_ast.iter().enumerate() {
        if g.to_string().to_lowercase() == expr_norm {
            return Ok(BoundExpr::Column(i));
        }
        if let (Some(gc), Some(ec)) = (extract_col_name(g), extract_col_name(expr))
            && gc.eq_ignore_ascii_case(&ec)
        {
            return Ok(BoundExpr::Column(i));
        }
    }
    // Check if `expr` matches a computed aggregate column.
    for (i, ak) in agg_keys.iter().enumerate() {
        if *ak == expr_norm {
            return Ok(BoundExpr::Column(group_by_ast.len() + i));
        }
    }

    match expr {
        AstExpr::Value(v) => Ok(BoundExpr::Literal(eval_ast_literal(&v.value)?)),
        AstExpr::Nested(inner) => bind_post_agg_expr(inner, group_by_ast, agg_keys),
        AstExpr::BinaryOp { left, op, right } => Ok(BoundExpr::BinaryOp {
            left: Box::new(bind_post_agg_expr(left, group_by_ast, agg_keys)?),
            op: map_bin_op(op)?,
            right: Box::new(bind_post_agg_expr(right, group_by_ast, agg_keys)?),
        }),
        AstExpr::UnaryOp { op, expr } => Ok(BoundExpr::UnaryOp {
            op: map_unary_op(op)?,
            expr: Box::new(bind_post_agg_expr(expr, group_by_ast, agg_keys)?),
        }),
        AstExpr::IsNull(e) => Ok(BoundExpr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(bind_post_agg_expr(e, group_by_ast, agg_keys)?),
        }),
        AstExpr::IsNotNull(e) => Ok(BoundExpr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(bind_post_agg_expr(e, group_by_ast, agg_keys)?),
        }),
        other => Err(SqlError::Bind(format!(
            "expression '{other}' must appear in GROUP BY or be used inside an aggregate function"
        ))),
    }
}

fn bind_projection_items(
    items: &[SelectItem],
    input_schema: &[ColumnMeta],
) -> Result<(Vec<BoundExpr>, Vec<ColumnMeta>), SqlError> {
    let mut exprs = Vec::new();
    let mut metas = Vec::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                for (i, meta) in input_schema.iter().enumerate() {
                    exprs.push(BoundExpr::Column(i));
                    metas.push(meta.clone());
                }
            }
            SelectItem::QualifiedWildcard(obj, _) => {
                let prefix = obj.to_string();
                let mut matched = false;
                for (i, meta) in input_schema.iter().enumerate() {
                    if meta.table.as_deref() == Some(prefix.as_str()) {
                        exprs.push(BoundExpr::Column(i));
                        metas.push(meta.clone());
                        matched = true;
                    }
                }
                if !matched {
                    return Err(SqlError::Bind(format!(
                        "unknown table qualifier '{prefix}' in wildcard"
                    )));
                }
            }
            SelectItem::UnnamedExpr(e) => {
                exprs.push(bind_expr(e, input_schema)?);
                let (tbl, col_opt) = extract_table_and_col_name(e);
                let name = col_opt.unwrap_or_else(|| e.to_string());
                metas.push(ColumnMeta::new(tbl, name, None));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push(bind_expr(expr, input_schema)?);
                metas.push(ColumnMeta::new(None, alias.value.clone(), None));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(SqlError::Unsupported(
                    "multi-alias SELECT item is not supported".into(),
                ));
            }
        }
    }

    Ok((exprs, metas))
}

fn extract_order_specs(
    order_by: &sqlparser::ast::OrderBy,
) -> Result<Vec<(AstExpr, SortOrder)>, SqlError> {
    let exprs = match &order_by.kind {
        OrderByKind::Expressions(list) => list,
        OrderByKind::All(_) => {
            return Err(SqlError::Unsupported(
                "ORDER BY ALL is not supported".into(),
            ));
        }
    };
    let mut out = Vec::with_capacity(exprs.len());
    for oe in exprs {
        let dir = match oe.options.sort {
            Some(OrderBySort::Desc) => SortOrder::Desc,
            _ => SortOrder::Asc,
        };
        out.push((oe.expr.clone(), dir));
    }
    Ok(out)
}

fn extract_limit_offset(query: &Query) -> Result<(Option<usize>, usize), SqlError> {
    let Some(lc) = &query.limit_clause else {
        return Ok((None, 0));
    };
    match lc {
        LimitClause::LimitOffset { limit, offset, .. } => {
            let lim = limit.as_ref().map(eval_nonneg_usize).transpose()?;
            let off = match offset {
                Some(o) => eval_nonneg_usize(&o.value)?,
                None => 0,
            };
            Ok((lim, off))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            Ok((Some(eval_nonneg_usize(limit)?), eval_nonneg_usize(offset)?))
        }
    }
}

fn eval_nonneg_usize(expr: &AstExpr) -> Result<usize, SqlError> {
    match expr {
        AstExpr::Value(v) => match eval_ast_literal(&v.value)? {
            Value::Int64(n) if n >= 0 => {
                usize::try_from(n).map_err(|_| SqlError::Bind("LIMIT/OFFSET overflow".into()))
            }
            _ => Err(SqlError::Bind(
                "LIMIT/OFFSET must be a non-negative integer".into(),
            )),
        },
        _ => Err(SqlError::Bind(
            "LIMIT/OFFSET must be an integer literal".into(),
        )),
    }
}

// ============================================================================
// Scalar Expression Binder
// ============================================================================

pub(crate) fn bind_expr(expr: &AstExpr, schema: &[ColumnMeta]) -> Result<BoundExpr, SqlError> {
    match expr {
        AstExpr::Identifier(id) => resolve_column(None, &id.value, schema),
        AstExpr::CompoundIdentifier(ids) => match ids.as_slice() {
            [tbl, col] => resolve_column(Some(&tbl.value), &col.value, schema),
            [col] => resolve_column(None, &col.value, schema),
            _ => Err(SqlError::Bind(format!(
                "unsupported compound identifier: {expr}"
            ))),
        },
        AstExpr::Value(v) => Ok(BoundExpr::Literal(eval_ast_literal(&v.value)?)),
        AstExpr::Nested(inner) => bind_expr(inner, schema),
        AstExpr::BinaryOp { left, op, right } => Ok(BoundExpr::BinaryOp {
            left: Box::new(bind_expr(left, schema)?),
            op: map_bin_op(op)?,
            right: Box::new(bind_expr(right, schema)?),
        }),
        AstExpr::UnaryOp { op, expr } => Ok(BoundExpr::UnaryOp {
            op: map_unary_op(op)?,
            expr: Box::new(bind_expr(expr, schema)?),
        }),
        AstExpr::IsNull(e) => Ok(BoundExpr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(bind_expr(e, schema)?),
        }),
        AstExpr::IsNotNull(e) => Ok(BoundExpr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(bind_expr(e, schema)?),
        }),
        AstExpr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let like = BoundExpr::BinaryOp {
                left: Box::new(bind_expr(expr, schema)?),
                op: BinOp::Like,
                right: Box::new(bind_expr(pattern, schema)?),
            };
            if *negated {
                Ok(BoundExpr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(like),
                })
            } else {
                Ok(like)
            }
        }
        AstExpr::In {
            expr,
            target: InTarget::List(list),
            negated,
            ..
        } => {
            let target = bind_expr(expr, schema)?;
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                items.push(bind_expr(item, schema)?);
            }
            Ok(BoundExpr::InList {
                expr: Box::new(target),
                list: items,
                negated: *negated,
            })
        }
        AstExpr::Function(func) => {
            let fname = func.name.to_string().to_lowercase();
            let mut bound_args = Vec::new();
            if let FunctionArguments::List(list) = &func.args {
                for a in &list.args {
                    match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            bound_args.push(bind_expr(e, schema)?);
                        }
                        _ => {
                            return Err(SqlError::Unsupported(format!(
                                "unsupported argument in function '{fname}'"
                            )));
                        }
                    }
                }
            }
            Ok(BoundExpr::ScalarFunc {
                name: fname,
                args: bound_args,
            })
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported SQL expression: {other}"
        ))),
    }
}

fn resolve_column(
    table: Option<&str>,
    col_name: &str,
    schema: &[ColumnMeta],
) -> Result<BoundExpr, SqlError> {
    let mut matched: Option<usize> = None;
    for (idx, meta) in schema.iter().enumerate() {
        let name_ok = meta.name.eq_ignore_ascii_case(col_name);
        let table_ok = match table {
            Some(t) => meta
                .table
                .as_deref()
                .is_some_and(|mt| mt.eq_ignore_ascii_case(t)),
            None => true,
        };
        if name_ok && table_ok {
            if matched.is_some() {
                return Err(SqlError::Bind(format!(
                    "ambiguous column reference '{col_name}'"
                )));
            }
            matched = Some(idx);
        }
    }
    matched
        .map(BoundExpr::Column)
        .ok_or_else(|| SqlError::Bind(format!("column '{col_name}' not found")))
}

fn map_bin_op(op: &BinaryOperator) -> Result<BinOp, SqlError> {
    match op {
        BinaryOperator::Plus => Ok(BinOp::Add),
        BinaryOperator::Minus => Ok(BinOp::Sub),
        BinaryOperator::Multiply => Ok(BinOp::Mul),
        BinaryOperator::Divide => Ok(BinOp::Div),
        BinaryOperator::Eq => Ok(BinOp::Eq),
        BinaryOperator::NotEq => Ok(BinOp::Ne),
        BinaryOperator::Lt => Ok(BinOp::Lt),
        BinaryOperator::LtEq => Ok(BinOp::Lte),
        BinaryOperator::Gt => Ok(BinOp::Gt),
        BinaryOperator::GtEq => Ok(BinOp::Gte),
        BinaryOperator::And => Ok(BinOp::And),
        BinaryOperator::Or => Ok(BinOp::Or),
        other => Err(SqlError::Unsupported(format!(
            "unsupported binary operator: {other}"
        ))),
    }
}

fn map_unary_op(op: &UnaryOperator) -> Result<UnaryOp, SqlError> {
    match op {
        UnaryOperator::Minus => Ok(UnaryOp::Neg),
        UnaryOperator::Not => Ok(UnaryOp::Not),
        other => Err(SqlError::Unsupported(format!(
            "unsupported unary operator: {other}"
        ))),
    }
}
