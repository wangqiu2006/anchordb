//! Full SQL pipeline: Parser (`sqlparser`), Binder, RBO optimizer, and `DataChunk` executor.

mod select;

use sqlparser::ast::{
    AlterTable, AlterTableOperation, AssignmentTarget, ColumnOption, CreateIndex, CreateTable,
    DataType, Delete, Expr as AstExpr, FromTable, Insert, ObjectName, OrderBySort, Parens, Query,
    SetExpr, Statement, TableConstraint, TableFactor, TableObject, UnaryOperator, Update,
    Value as AstValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use kv_core::KvTransaction;
use table_codec::{
    CodecError, ColumnSpec, ColumnType, IndexSpec, SortOrder, SystemCatalog, TableSchema,
    TableStore, Value,
};

use crate::QueryConfig;
use crate::chunk::{DataChunk, SqlError};

/// Outcome of executing a single SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Affected {
        count: usize,
    },
    Message(String),
}

/// High-level transaction control signal recognized before dispatching a statement into a txn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnControl {
    Begin,
    Commit,
    Rollback,
}

/// Parses one or more semicolon-separated SQL statements using the PostgreSQL dialect.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, SqlError> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| SqlError::Parse(e.to_string()))
}

/// Detects whether a parsed statement is an explicit transaction control command.
#[must_use]
pub fn classify_txn_control(stmt: &Statement) -> Option<TxnControl> {
    match stmt {
        Statement::StartTransaction { .. } => Some(TxnControl::Begin),
        Statement::Commit { .. } => Some(TxnControl::Commit),
        Statement::Rollback { .. } => Some(TxnControl::Rollback),
        _ => None,
    }
}

/// Returns `true` if the statement mutates catalog or table data (requires a read-write txn).
#[must_use]
pub fn statement_is_write(stmt: &Statement) -> bool {
    !matches!(
        stmt,
        Statement::Query(_) | Statement::Explain { .. } | Statement::ShowTables { .. }
    )
}

/// Executes a single non-transaction-control SQL statement inside an existing [`KvTransaction`].
pub fn execute_statement_in_txn<T: KvTransaction>(
    txn: &mut T,
    stmt: &Statement,
    config: &QueryConfig,
) -> Result<SqlResult, SqlError> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(txn, ct),
        Statement::CreateIndex(ci) => exec_create_index(txn, ci),
        Statement::AlterTable(at) => exec_alter_table(txn, at),
        Statement::Insert(ins) => exec_insert(txn, ins),
        Statement::Update(upd) => exec_update(txn, upd),
        Statement::Delete(del) => exec_delete(txn, del),
        Statement::Query(query) => select::exec_query(txn, query, config),
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(query) => select::explain_query(txn, query),
            _ => Err(SqlError::Unsupported(
                "EXPLAIN is currently supported for SELECT queries".into(),
            )),
        },
        Statement::ShowTables { .. } => exec_show_tables(txn),
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => Err(SqlError::Transaction(
            "transaction control statements must be executed via a SQL session".into(),
        )),
        other => Err(SqlError::Unsupported(format!(
            "unsupported SQL statement: {other}"
        ))),
    }
}

/// Formats a [`SqlResult`] as an ASCII table or status line suitable for interactive CLI output.
#[must_use]
pub fn format_sql_result(result: &SqlResult) -> String {
    match result {
        SqlResult::Message(msg) => msg.clone(),
        SqlResult::Affected { count } => format!("OK ({count} row(s) affected)"),
        SqlResult::Rows { columns, rows } => {
            if columns.is_empty() {
                return "(0 columns)".to_owned();
            }
            let str_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|r| r.iter().map(format_value).collect())
                .collect();

            let mut widths: Vec<usize> = columns.iter().map(String::len).collect();
            for r in &str_rows {
                for (i, cell) in r.iter().enumerate() {
                    if let Some(w) = widths.get_mut(i) {
                        *w = (*w).max(cell.len());
                    }
                }
            }

            let sep: String = format!(
                "+-{}-+",
                widths
                    .iter()
                    .map(|&w| "-".repeat(w))
                    .collect::<Vec<_>>()
                    .join("-+-")
            );
            let header: String = format!(
                "| {} |",
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{c:<width$}", width = widths[i]))
                    .collect::<Vec<_>>()
                    .join(" | ")
            );

            let mut lines = Vec::with_capacity(str_rows.len() + 4);
            lines.push(sep.clone());
            lines.push(header);
            lines.push(sep.clone());
            for r in &str_rows {
                let line = format!(
                    "| {} |",
                    r.iter()
                        .enumerate()
                        .map(|(i, cell)| format!("{cell:<width$}", width = widths[i]))
                        .collect::<Vec<_>>()
                        .join(" | ")
                );
                lines.push(line);
            }
            lines.push(sep);
            lines.push(format!("({} row(s))", rows.len()));
            lines.join("\n")
        }
    }
}

/// Renders a single [`Value`] as human-readable text.
#[must_use]
pub fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(bytes) => {
            let mut s = String::from("0x");
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            s
        }
    }
}

// ============================================================================
// DDL Execution
// ============================================================================

fn exec_create_table<T: KvTransaction>(
    txn: &mut T,
    ct: &CreateTable,
) -> Result<SqlResult, SqlError> {
    let table_name = object_name_to_ident(&ct.name)?;
    if ct.if_not_exists && SystemCatalog::get_table_by_name(txn, &table_name)?.is_some() {
        return Ok(SqlResult::Message(format!(
            "Table '{table_name}' already exists (skipped)"
        )));
    }

    let mut col_specs = Vec::with_capacity(ct.columns.len());
    let mut pk_cols: Vec<String> = Vec::new();
    let mut inline_unique_cols: Vec<String> = Vec::new();

    for col_def in &ct.columns {
        let name = col_def.name.value.clone();
        let data_type = map_data_type(&col_def.data_type)?;
        let mut nullable = true;
        let mut default_value = None;
        let mut is_pk = false;
        let mut is_unique = false;

        for opt in &col_def.options {
            match &opt.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                ColumnOption::Default(expr) => {
                    default_value = Some(eval_const_expr(expr, Some(data_type))?);
                }
                ColumnOption::PrimaryKey(_) => {
                    is_pk = true;
                    nullable = false;
                }
                ColumnOption::Unique(_) => {
                    is_unique = true;
                }
                _ => {}
            }
        }

        if is_pk {
            pk_cols.push(name.clone());
        } else if is_unique {
            inline_unique_cols.push(name.clone());
        }

        col_specs.push(ColumnSpec {
            name,
            data_type,
            nullable,
            default_value,
        });
    }

    for constraint in &ct.constraints {
        if let TableConstraint::PrimaryKey(pk) = constraint {
            for col in &pk.columns {
                let col_name = extract_expr_ident(&col.column.expr)?;
                if !pk_cols.contains(&col_name) {
                    pk_cols.push(col_name.clone());
                }
                if let Some(spec) = col_specs.iter_mut().find(|c| c.name == col_name) {
                    spec.nullable = false;
                }
            }
        }
    }

    let pk_refs: Vec<&str> = pk_cols.iter().map(String::as_str).collect();
    let schema = SystemCatalog::create_table(txn, &table_name, &col_specs, &pk_refs)?;

    for ucol in inline_unique_cols {
        SystemCatalog::create_index(
            txn,
            schema.table_id,
            IndexSpec {
                name: format!("uidx_{table_name}_{ucol}"),
                unique: true,
                columns: vec![(ucol, SortOrder::Asc)],
            },
        )?;
    }

    Ok(SqlResult::Message(format!("CREATE TABLE {table_name}")))
}

fn exec_create_index<T: KvTransaction>(
    txn: &mut T,
    ci: &CreateIndex,
) -> Result<SqlResult, SqlError> {
    let table_name = object_name_to_ident(&ci.table_name)?;
    let schema = SystemCatalog::get_table_by_name(txn, &table_name)?
        .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(table_name.clone())))?;

    let index_name = match &ci.name {
        Some(n) => object_name_to_ident(n)?,
        None => format!("idx_{}_{}", table_name, schema.next_index_id),
    };
    if ci.if_not_exists && schema.index_by_name(&index_name).is_some() {
        return Ok(SqlResult::Message(format!(
            "Index '{index_name}' already exists (skipped)"
        )));
    }

    let mut cols = Vec::with_capacity(ci.columns.len());
    for ic in &ci.columns {
        let col_name = extract_expr_ident(&ic.column.expr)?;
        let dir = match ic.column.options.sort {
            Some(OrderBySort::Desc) => SortOrder::Desc,
            _ => SortOrder::Asc,
        };
        cols.push((col_name, dir));
    }

    SystemCatalog::create_index(
        txn,
        schema.table_id,
        IndexSpec {
            name: index_name.clone(),
            unique: ci.unique,
            columns: cols,
        },
    )?;

    Ok(SqlResult::Message(format!("CREATE INDEX {index_name}")))
}

fn exec_alter_table<T: KvTransaction>(txn: &mut T, at: &AlterTable) -> Result<SqlResult, SqlError> {
    let table_name = object_name_to_ident(&at.name)?;
    let schema = SystemCatalog::get_table_by_name(txn, &table_name)?
        .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(table_name.clone())))?;

    for op in &at.operations {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let data_type = map_data_type(&column_def.data_type)?;
                let mut nullable = true;
                let mut default_value = None;
                for opt in &column_def.options {
                    match &opt.option {
                        ColumnOption::NotNull => nullable = false,
                        ColumnOption::Null => nullable = true,
                        ColumnOption::Default(expr) => {
                            default_value = Some(eval_const_expr(expr, Some(data_type))?);
                        }
                        _ => {}
                    }
                }
                SystemCatalog::add_column(
                    txn,
                    schema.table_id,
                    ColumnSpec {
                        name: column_def.name.value.clone(),
                        data_type,
                        nullable,
                        default_value,
                    },
                )?;
            }
            AlterTableOperation::DropColumn { column_names, .. } => {
                for ident in column_names {
                    SystemCatalog::drop_column(txn, schema.table_id, &ident.value)?;
                }
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported ALTER TABLE operation: {other}"
                )));
            }
        }
    }

    Ok(SqlResult::Message(format!("ALTER TABLE {table_name}")))
}

fn exec_show_tables<T: KvTransaction>(txn: &T) -> Result<SqlResult, SqlError> {
    let tables = SystemCatalog::list_tables(txn)?;
    let rows = tables
        .into_iter()
        .map(|t| {
            vec![
                Value::Text(t.name),
                Value::Int64(i64::from(t.version)),
                Value::Int64(t.columns.len() as i64),
                Value::Int64(t.indexes.len() as i64),
            ]
        })
        .collect();
    Ok(SqlResult::Rows {
        columns: vec![
            "table_name".into(),
            "schema_version".into(),
            "columns".into(),
            "indexes".into(),
        ],
        rows,
    })
}

// ============================================================================
// DML Execution
// ============================================================================

fn exec_insert<T: KvTransaction>(txn: &mut T, ins: &Insert) -> Result<SqlResult, SqlError> {
    let table_obj = match &ins.table {
        TableObject::TableName(name) => name,
        _ => {
            return Err(SqlError::Unsupported(
                "INSERT target must be a table name".into(),
            ));
        }
    };
    let table_name = object_name_to_ident(table_obj)?;
    let schema = SystemCatalog::get_table_by_name(txn, &table_name)?
        .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(table_name.clone())))?;

    let Some(source) = &ins.source else {
        return Err(SqlError::Unsupported(
            "INSERT without VALUES source is not supported".into(),
        ));
    };

    let value_rows = extract_insert_values(source)?;

    // Resolve target column positions (either explicit column list or full table column order).
    let target_positions: Vec<usize> = if ins.columns.is_empty() {
        (0..schema.columns.len()).collect()
    } else {
        let mut pos_list = Vec::with_capacity(ins.columns.len());
        for col_obj in &ins.columns {
            let col_name = object_name_to_ident(col_obj)?;
            let pos = schema
                .column_index_by_name(&col_name)
                .ok_or(SqlError::Codec(CodecError::ColumnNotFound(col_name)))?;
            pos_list.push(pos);
        }
        pos_list
    };

    let mut inserted = 0usize;
    for row_parens in value_rows {
        let expr_row = &row_parens.content;
        if expr_row.len() != target_positions.len() {
            return Err(SqlError::Codec(CodecError::ColumnCountMismatch {
                expected: target_positions.len(),
                actual: expr_row.len(),
            }));
        }

        // Start with defaults / NULL for omitted columns.
        let mut full_row: Vec<Value> = schema
            .columns
            .iter()
            .map(|c| c.default_value.clone().unwrap_or(Value::Null))
            .collect();

        for (&col_pos, expr) in target_positions.iter().zip(expr_row.iter()) {
            let expected_type = schema.columns[col_pos].data_type;
            full_row[col_pos] = eval_const_expr(expr, Some(expected_type))?;
        }

        TableStore::insert_row(txn, &schema, &full_row)?;
        inserted += 1;
    }

    Ok(SqlResult::Affected { count: inserted })
}

fn extract_insert_values(source: &Query) -> Result<&[Parens<Vec<AstExpr>>], SqlError> {
    match source.body.as_ref() {
        SetExpr::Values(values) => Ok(&values.rows),
        _ => Err(SqlError::Unsupported(
            "INSERT currently supports VALUES (...) rows".into(),
        )),
    }
}

fn exec_update<T: KvTransaction>(txn: &mut T, upd: &Update) -> Result<SqlResult, SqlError> {
    let table_name = extract_table_factor_name(&upd.table.relation)?;
    let schema = SystemCatalog::get_table_by_name(txn, &table_name)?
        .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(table_name.clone())))?;

    let col_metas = select::table_column_metas(&schema, None);
    let predicate = upd
        .selection
        .as_ref()
        .map(|expr| select::bind_expr(expr, &col_metas))
        .transpose()?;

    let mut assignments = Vec::with_capacity(upd.assignments.len());
    for assign in &upd.assignments {
        let col_name = match &assign.target {
            AssignmentTarget::ColumnName(obj) => object_name_to_ident(obj)?,
            AssignmentTarget::Tuple(_) => {
                return Err(SqlError::Unsupported(
                    "tuple assignment in UPDATE is not supported".into(),
                ));
            }
        };
        let col_pos = schema
            .column_index_by_name(&col_name)
            .ok_or(SqlError::Codec(CodecError::ColumnNotFound(col_name)))?;
        let bound_val = select::bind_expr(&assign.value, &col_metas)?;
        assignments.push((col_pos, bound_val));
    }

    let rows = TableStore::scan_table(txn, &schema)?;
    let mut updated = 0usize;
    for row in rows {
        let chunk = DataChunk::from_rows(core::slice::from_ref(&row), schema.columns.len());
        if let Some(pred) = &predicate
            && !matches!(pred.eval_row(&chunk, 0)?, Value::Bool(true))
        {
            continue;
        }
        let old_pk = extract_pk_values(&schema, &row)?;
        let mut new_row = row.clone();
        for (col_pos, expr) in &assignments {
            let val = expr.eval_row(&chunk, 0)?;
            new_row[*col_pos] = coerce_value(val, schema.columns[*col_pos].data_type);
        }
        TableStore::update_row(txn, &schema, &old_pk, &new_row)?;
        updated += 1;
    }

    Ok(SqlResult::Affected { count: updated })
}

fn exec_delete<T: KvTransaction>(txn: &mut T, del: &Delete) -> Result<SqlResult, SqlError> {
    let from_tables = match &del.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let first = from_tables
        .first()
        .ok_or_else(|| SqlError::Bind("DELETE requires a target table".into()))?;
    let table_name = extract_table_factor_name(&first.relation)?;
    let schema = SystemCatalog::get_table_by_name(txn, &table_name)?
        .ok_or_else(|| SqlError::Codec(CodecError::TableNotFound(table_name.clone())))?;

    let col_metas = select::table_column_metas(&schema, None);
    let predicate = del
        .selection
        .as_ref()
        .map(|expr| select::bind_expr(expr, &col_metas))
        .transpose()?;

    let rows = TableStore::scan_table(txn, &schema)?;
    let mut deleted = 0usize;
    for row in rows {
        if let Some(pred) = &predicate {
            let chunk = DataChunk::from_rows(core::slice::from_ref(&row), schema.columns.len());
            if !matches!(pred.eval_row(&chunk, 0)?, Value::Bool(true)) {
                continue;
            }
        }
        let pk = extract_pk_values(&schema, &row)?;
        if TableStore::delete_row(txn, &schema, &pk)? {
            deleted += 1;
        }
    }

    Ok(SqlResult::Affected { count: deleted })
}

// ============================================================================
// Shared AST Helpers
// ============================================================================

pub(crate) fn object_name_to_ident(name: &ObjectName) -> Result<String, SqlError> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|id| id.value.clone())
        .ok_or_else(|| SqlError::Bind(format!("invalid identifier '{name}'")))
}

pub(crate) fn extract_table_factor_name(factor: &TableFactor) -> Result<String, SqlError> {
    match factor {
        TableFactor::Table { name, .. } => object_name_to_ident(name),
        other => Err(SqlError::Unsupported(format!(
            "unsupported table reference: {other}"
        ))),
    }
}

fn extract_expr_ident(expr: &AstExpr) -> Result<String, SqlError> {
    match expr {
        AstExpr::Identifier(id) => Ok(id.value.clone()),
        AstExpr::CompoundIdentifier(ids) => ids
            .last()
            .map(|id| id.value.clone())
            .ok_or_else(|| SqlError::Bind("empty compound identifier".into())),
        other => Err(SqlError::Unsupported(format!(
            "expected column identifier, got: {other}"
        ))),
    }
}

fn extract_pk_values(schema: &TableSchema, row: &[Value]) -> Result<Vec<Value>, SqlError> {
    let mut pk = Vec::with_capacity(schema.primary_key_cols.len());
    for &cid in &schema.primary_key_cols {
        let pos = schema
            .column_index_by_id(cid)
            .ok_or(SqlError::Codec(CodecError::InvalidEncoding))?;
        pk.push(row[pos].clone());
    }
    Ok(pk)
}

fn map_data_type(dt: &DataType) -> Result<ColumnType, SqlError> {
    match dt {
        DataType::Boolean | DataType::Bool => Ok(ColumnType::Bool),
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::Int2(_)
        | DataType::Int4(_)
        | DataType::Int8(_) => Ok(ColumnType::Int64),
        DataType::Float(_)
        | DataType::Float4
        | DataType::Float8
        | DataType::Double(_)
        | DataType::DoublePrecision
        | DataType::Real
        | DataType::Numeric(_)
        | DataType::Decimal(_) => Ok(ColumnType::Float64),
        DataType::Text
        | DataType::Varchar(_)
        | DataType::Char(_)
        | DataType::Character(_)
        | DataType::CharacterVarying(_)
        | DataType::String(_) => Ok(ColumnType::Text),
        DataType::Blob(_) | DataType::Bytea | DataType::Binary(_) | DataType::Varbinary(_) => {
            Ok(ColumnType::Blob)
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported column data type: {other}"
        ))),
    }
}

pub(crate) fn eval_ast_literal(v: &AstValue) -> Result<Value, SqlError> {
    match v {
        AstValue::Null => Ok(Value::Null),
        AstValue::Boolean(b) => Ok(Value::Bool(*b)),
        AstValue::Number(s, _) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(Value::Int64(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Value::Float64(f))
            } else {
                Err(SqlError::Parse(format!("invalid numeric literal '{s}'")))
            }
        }
        AstValue::SingleQuotedString(s) | AstValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported literal value: {other}"
        ))),
    }
}

fn eval_const_expr(expr: &AstExpr, expected_type: Option<ColumnType>) -> Result<Value, SqlError> {
    let raw = match expr {
        AstExpr::Value(v) => eval_ast_literal(&v.value)?,
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match eval_const_expr(expr, expected_type)? {
            Value::Int64(n) => n
                .checked_neg()
                .map(Value::Int64)
                .ok_or_else(|| SqlError::Execution("integer overflow".into()))?,
            Value::Float64(f) => Value::Float64(-f),
            _ => {
                return Err(SqlError::Execution(
                    "unary '-' only applies to numbers".into(),
                ));
            }
        },
        AstExpr::Nested(inner) => eval_const_expr(inner, expected_type)?,
        other => {
            // Fall back to full constant expression evaluator (supports e.g. `1 + 2`, `LOWER('A')`).
            let bound = select::bind_expr(other, &[])?;
            let empty_chunk = DataChunk::from_rows(&[Vec::new()], 0);
            bound.eval_row(&empty_chunk, 0)?
        }
    };
    Ok(match expected_type {
        Some(dt) => coerce_value(raw, dt),
        None => raw,
    })
}

fn coerce_value(val: Value, expected: ColumnType) -> Value {
    match (val, expected) {
        (Value::Int64(n), ColumnType::Float64) => Value::Float64(n as f64),
        (other, _) => other,
    }
}
