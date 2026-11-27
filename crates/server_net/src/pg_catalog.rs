//! PostgreSQL `pg_catalog` / `information_schema` and client-handshake compatibility layer.
//!
//! Enables standard tools (`psql`, DBeaver, Navicat, `psycopg2`) to connect, negotiate session
//! parameters, and introspect tables & columns cleanly without external dependencies.

use query_engine::{ColumnMeta, QueryConfig, SqlError, SqlResult, execute_sql_on_virtual_table};
use table_codec::{ColumnType, TableSchema, Value};

use crate::backend::ServerSession;

/// Standard PostgreSQL Type OIDs used in `RowDescription` and `pg_type`/`pg_attribute`.
pub const OID_BOOL: i32 = 16;
pub const OID_BYTEA: i32 = 17;
pub const OID_INT8: i32 = 20;
pub const OID_TEXT: i32 = 25;
pub const OID_FLOAT8: i32 = 701;

const PUBLIC_NAMESPACE_OID: i64 = 2200;
const PG_CATALOG_NAMESPACE_OID: i64 = 11;

#[must_use]
pub const fn column_type_to_pg_oid(dt: ColumnType) -> i32 {
    match dt {
        ColumnType::Bool => OID_BOOL,
        ColumnType::Int64 => OID_INT8,
        ColumnType::Float64 => OID_FLOAT8,
        ColumnType::Text => OID_TEXT,
        ColumnType::Blob => OID_BYTEA,
    }
}

#[must_use]
pub const fn value_to_pg_oid(val: &Value) -> i32 {
    match val {
        Value::Bool(_) => OID_BOOL,
        Value::Int64(_) => OID_INT8,
        Value::Float64(_) => OID_FLOAT8,
        Value::Blob(_) => OID_BYTEA,
        Value::Null | Value::Text(_) => OID_TEXT,
    }
}

/// Attempts to handle PostgreSQL client handshake, `SET`/`SHOW`, and `pg_catalog` /
/// `information_schema` metadata queries before delegating to the core SQL engine.
///
/// Returns `Ok(Some(results))` when intercepted, or `Ok(None)` to fall through to normal SQL.
pub fn try_handle_pg_compat<S: ServerSession>(
    session: &S,
    raw_sql: &str,
    database_name: &str,
    user_name: &str,
) -> Result<Option<Vec<SqlResult>>, SqlError> {
    let trimmed = raw_sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let lower = trimmed.to_lowercase();

    // 1. Session parameter assignments sent by psql / JDBC / Navicat / DBeaver on startup.
    if lower.starts_with("set ") || lower.starts_with("reset ") || lower.starts_with("discard ") {
        return Ok(Some(vec![SqlResult::Message("SET".to_owned())]));
    }
    if lower == "deallocate all" || lower.starts_with("deallocate ") {
        return Ok(Some(vec![SqlResult::Message("DEALLOCATE".to_owned())]));
    }

    // 2. Standard `SHOW <parameter>` probes (excluding `SHOW TABLES`, handled by core SQL).
    if lower.starts_with("show ") && !lower.starts_with("show tables") {
        let param = lower.trim_start_matches("show ").trim();
        let val = match param {
            "server_version" => format!("15.0 (AnchorDB {})", env!("CARGO_PKG_VERSION")),
            "server_version_num" => "150000".to_owned(),
            "server_encoding" | "client_encoding" => "UTF8".to_owned(),
            "standard_conforming_strings" => "on".to_owned(),
            "transaction_isolation" | "default_transaction_isolation" => {
                "repeatable read".to_owned()
            }
            "search_path" => "\"$user\", public".to_owned(),
            "timezone" => "UTC".to_owned(),
            "datestyle" => "ISO, MDY".to_owned(),
            "integer_datetimes" => "on".to_owned(),
            _ => String::new(),
        };
        return Ok(Some(vec![SqlResult::Rows {
            columns: vec![param.to_owned()],
            rows: vec![vec![Value::Text(val)]],
        }]));
    }

    // 3. Common scalar metadata probes (`SELECT current_schema()`, `current_database()`, etc.).
    if lower.starts_with("select ")
        && !lower.contains(" from ")
        && (lower.contains("current_schema")
            || lower.contains("current_database")
            || lower.contains("current_user")
            || lower.contains("current_setting")
            || lower.contains("pg_backend_pid"))
    {
        return Ok(Some(vec![handle_scalar_pg_probe(
            &lower,
            database_name,
            user_name,
        )]));
    }

    // 4. `pg_catalog` & `information_schema` virtual metadata tables.
    // Execute client SELECT/WHERE/ORDER BY/LIMIT over the virtual table via `DataChunk` operators
    // (falling back to full virtual rows if the client uses exotic PG-only cast syntax).
    if lower.contains("pg_namespace") {
        return Ok(Some(vec![run_virtual_query(
            trimmed,
            "pg_namespace",
            virtual_pg_namespace(),
        )]));
    }
    if lower.contains("pg_type") {
        return Ok(Some(vec![run_virtual_query(
            trimmed,
            "pg_type",
            virtual_pg_type(),
        )]));
    }
    if lower.contains("pg_class") {
        let tables = session.list_tables()?;
        return Ok(Some(vec![run_virtual_query(
            trimmed,
            "pg_class",
            virtual_pg_class(&tables),
        )]));
    }
    if lower.contains("pg_attribute") {
        let tables = session.list_tables()?;
        return Ok(Some(vec![run_virtual_query(
            trimmed,
            "pg_attribute",
            virtual_pg_attribute(&tables),
        )]));
    }
    if lower.contains("information_schema.tables") {
        let tables = session.list_tables()?;
        return Ok(Some(vec![run_virtual_query(
            trimmed,
            "tables",
            virtual_info_schema_tables(database_name, &tables),
        )]));
    }
    if lower.contains("information_schema.columns") {
        let tables = session.list_tables()?;
        return Ok(Some(vec![run_virtual_query(
            trimmed,
            "columns",
            virtual_info_schema_columns(database_name, &tables),
        )]));
    }

    Ok(None)
}

fn run_virtual_query(sql: &str, table_name: &str, full_table: SqlResult) -> SqlResult {
    let SqlResult::Rows { columns, rows } = full_table else {
        return full_table;
    };
    let metas: Vec<ColumnMeta> = columns
        .iter()
        .map(|c| ColumnMeta::new(Some(table_name.to_owned()), c.clone(), None))
        .collect();
    match execute_sql_on_virtual_table(sql, metas, rows.clone(), &QueryConfig::default()) {
        Ok(projected) => projected,
        Err(_) => SqlResult::Rows { columns, rows },
    }
}

fn handle_scalar_pg_probe(lower: &str, database_name: &str, user_name: &str) -> SqlResult {
    let body = lower.trim_start_matches("select ").trim();
    let mut columns = Vec::new();
    let mut row = Vec::new();

    for part in body.split(',') {
        let p = part.trim();
        if p.contains("current_schema") {
            columns.push("current_schema".to_owned());
            row.push(Value::Text("public".to_owned()));
        } else if p.contains("current_database") {
            columns.push("current_database".to_owned());
            row.push(Value::Text(database_name.to_owned()));
        } else if p.contains("current_user") {
            columns.push("current_user".to_owned());
            row.push(Value::Text(user_name.to_owned()));
        } else if p.contains("version") {
            columns.push("version".to_owned());
            row.push(Value::Text(format!(
                "PostgreSQL 15.0 (AnchorDB {})",
                env!("CARGO_PKG_VERSION")
            )));
        } else if p.contains("pg_backend_pid") {
            columns.push("pg_backend_pid".to_owned());
            row.push(Value::Int64(i64::from(std::process::id())));
        } else if p.contains("current_setting") {
            columns.push("current_setting".to_owned());
            let val = if p.contains("server_version_num") {
                "150000"
            } else if p.contains("server_version") {
                "15.0"
            } else {
                ""
            };
            row.push(Value::Text(val.to_owned()));
        } else {
            columns.push(p.to_owned());
            row.push(Value::Null);
        }
    }

    SqlResult::Rows {
        columns,
        rows: vec![row],
    }
}

fn virtual_pg_namespace() -> SqlResult {
    SqlResult::Rows {
        columns: vec!["oid".into(), "nspname".into()],
        rows: vec![
            vec![
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
                Value::Text("pg_catalog".into()),
            ],
            vec![
                Value::Int64(PUBLIC_NAMESPACE_OID),
                Value::Text("public".into()),
            ],
        ],
    }
}

fn virtual_pg_type() -> SqlResult {
    SqlResult::Rows {
        columns: vec!["oid".into(), "typname".into(), "typnamespace".into()],
        rows: vec![
            vec![
                Value::Int64(i64::from(OID_BOOL)),
                Value::Text("bool".into()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
            ],
            vec![
                Value::Int64(i64::from(OID_BYTEA)),
                Value::Text("bytea".into()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
            ],
            vec![
                Value::Int64(i64::from(OID_INT8)),
                Value::Text("int8".into()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
            ],
            vec![
                Value::Int64(i64::from(OID_TEXT)),
                Value::Text("text".into()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
            ],
            vec![
                Value::Int64(i64::from(OID_FLOAT8)),
                Value::Text("float8".into()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
            ],
        ],
    }
}

fn virtual_pg_class(tables: &[TableSchema]) -> SqlResult {
    let mut rows = Vec::new();
    for t in tables {
        let rel_oid = 16384 + i64::from(t.table_id);
        rows.push(vec![
            Value::Int64(rel_oid),
            Value::Text(t.name.clone()),
            Value::Int64(PUBLIC_NAMESPACE_OID),
            Value::Text("r".into()),
            Value::Bool(!t.indexes.is_empty()),
        ]);
        for idx in &t.indexes {
            let idx_oid = 100_000 + i64::from(t.table_id) * 1000 + i64::from(idx.id);
            rows.push(vec![
                Value::Int64(idx_oid),
                Value::Text(idx.name.clone()),
                Value::Int64(PUBLIC_NAMESPACE_OID),
                Value::Text("i".into()),
                Value::Bool(false),
            ]);
        }
    }
    SqlResult::Rows {
        columns: vec![
            "oid".into(),
            "relname".into(),
            "relnamespace".into(),
            "relkind".into(),
            "relhasindex".into(),
        ],
        rows,
    }
}

fn virtual_pg_attribute(tables: &[TableSchema]) -> SqlResult {
    let mut rows = Vec::new();
    for t in tables {
        let rel_oid = 16384 + i64::from(t.table_id);
        for (idx, col) in t.columns.iter().enumerate() {
            rows.push(vec![
                Value::Int64(rel_oid),
                Value::Text(col.name.clone()),
                Value::Int64(i64::from(column_type_to_pg_oid(col.data_type))),
                Value::Int64((idx + 1) as i64),
                Value::Bool(!col.nullable),
                Value::Bool(false),
            ]);
        }
    }
    SqlResult::Rows {
        columns: vec![
            "attrelid".into(),
            "attname".into(),
            "atttypid".into(),
            "attnum".into(),
            "attnotnull".into(),
            "attisdropped".into(),
        ],
        rows,
    }
}

fn virtual_info_schema_tables(database_name: &str, tables: &[TableSchema]) -> SqlResult {
    let rows = tables
        .iter()
        .map(|t| {
            vec![
                Value::Text(database_name.to_owned()),
                Value::Text("public".into()),
                Value::Text(t.name.clone()),
                Value::Text("BASE TABLE".into()),
            ]
        })
        .collect();
    SqlResult::Rows {
        columns: vec![
            "table_catalog".into(),
            "table_schema".into(),
            "table_name".into(),
            "table_type".into(),
        ],
        rows,
    }
}

fn virtual_info_schema_columns(database_name: &str, tables: &[TableSchema]) -> SqlResult {
    let mut rows = Vec::new();
    for t in tables {
        for (idx, col) in t.columns.iter().enumerate() {
            let pg_type_name = match col.data_type {
                ColumnType::Bool => "boolean",
                ColumnType::Int64 => "bigint",
                ColumnType::Float64 => "double precision",
                ColumnType::Text => "text",
                ColumnType::Blob => "bytea",
            };
            rows.push(vec![
                Value::Text(database_name.to_owned()),
                Value::Text("public".into()),
                Value::Text(t.name.clone()),
                Value::Text(col.name.clone()),
                Value::Int64((idx + 1) as i64),
                Value::Text(if col.nullable { "YES" } else { "NO" }.into()),
                Value::Text(pg_type_name.into()),
            ]);
        }
    }
    SqlResult::Rows {
        columns: vec![
            "table_catalog".into(),
            "table_schema".into(),
            "table_name".into(),
            "column_name".into(),
            "ordinal_position".into(),
            "is_nullable".into(),
            "data_type".into(),
        ],
        rows,
    }
}
