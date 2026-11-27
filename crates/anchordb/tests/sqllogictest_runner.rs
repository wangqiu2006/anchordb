//! `sqllogictest` regression runner for AnchorDB (Phase 4A).
//!
//! Runs every `.slt` script under `crates/anchordb/tests/slt/` against a fresh
//! `AnchorDb<LsmEngine<FaultSimFs>>` configured with aggressive MemTable flush & compaction
//! thresholds so SQL correctness, `SST2` Bloom Filters, MVCC Snapshot Isolation, and LSM
//! compaction are all exercised together.

use std::path::Path;

use anchordb::hal_vfs::fault_sim::FaultSimFs;
use anchordb::kv_core::{LsmConfig, LsmEngine};
use anchordb::{AnchorDb, DbError, QueryConfig, SqlResult, SqlSession, Value};
use sqllogictest::{DB, DBOutput, DefaultColumnType, Runner};

struct AnchorSltConn<'a> {
    session: SqlSession<'a, LsmEngine<FaultSimFs>>,
}

impl DB for AnchorSltConn<'_> {
    type Error = DbError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut results = self.session.execute(sql)?;
        let Some(last) = results.pop() else {
            return Ok(DBOutput::StatementComplete(0));
        };
        match last {
            SqlResult::Rows { columns, rows } => {
                let col_count = columns.len();
                let mut types = vec![DefaultColumnType::Any; col_count];
                for (col_idx, col_ty) in types.iter_mut().enumerate() {
                    for row in &rows {
                        if let Some(val) = row.get(col_idx)
                            && !matches!(val, Value::Null)
                        {
                            *col_ty = value_column_type(val);
                            break;
                        }
                    }
                    if matches!(col_ty, DefaultColumnType::Any) {
                        *col_ty = DefaultColumnType::Text;
                    }
                }

                let formatted_rows = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(format_slt_value).collect())
                    .collect();

                Ok(DBOutput::Rows {
                    types,
                    rows: formatted_rows,
                })
            }
            SqlResult::Affected { count } => Ok(DBOutput::StatementComplete(count as u64)),
            SqlResult::Message(_) => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn engine_name(&self) -> &'static str {
        "anchordb"
    }
}

const fn value_column_type(val: &Value) -> DefaultColumnType {
    match val {
        Value::Int64(_) => DefaultColumnType::Integer,
        Value::Float64(_) => DefaultColumnType::FloatingPoint,
        Value::Null | Value::Bool(_) | Value::Text(_) | Value::Blob(_) => DefaultColumnType::Text,
    }
}

fn format_slt_value(val: Value) -> String {
    match val {
        Value::Null => "NULL".to_owned(),
        Value::Bool(true) => "true".to_owned(),
        Value::Bool(false) => "false".to_owned(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => {
            if f.fract() == 0.0 {
                format!("{f:.1}")
            } else {
                format!("{f}")
            }
        }
        Value::Text(s) => {
            if s.is_empty() {
                "(empty)".to_owned()
            } else {
                s
            }
        }
        Value::Blob(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2 + 2);
            hex.push_str("0x");
            for b in bytes {
                use std::fmt::Write;
                let _ = write!(&mut hex, "{b:02x}");
            }
            hex
        }
    }
}

fn run_slt_script(script_name: &str, seed: u64) -> Result<(), String> {
    let fs = FaultSimFs::new(seed);
    let lsm = LsmConfig {
        memtable_flush_bytes: 512,
        block_size: 256,
        page_size: 1024,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    };
    let db = AnchorDb::open_fs(fs, lsm, QueryConfig::default())
        .map_err(|e| format!("failed to open test db: {e}"))?;

    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("slt")
        .join(script_name);

    let db_ref = &db;
    let mut runner = Runner::new(move || async move {
        Ok(AnchorSltConn {
            session: db_ref.sql_session(),
        })
    });
    runner
        .run_file(&path)
        .map_err(|err| format!("slt failure in {script_name}:\n{err}"))
}

#[test]
fn slt_01_basic_crud_types() {
    run_slt_script("01_basic_crud_types.slt", 4101).unwrap();
}

#[test]
fn slt_02_indexes_and_evolution() {
    run_slt_script("02_indexes_and_evolution.slt", 4102).unwrap();
}

#[test]
fn slt_03_joins_aggregates_txns() {
    run_slt_script("03_joins_aggregates_txns.slt", 4103).unwrap();
}
