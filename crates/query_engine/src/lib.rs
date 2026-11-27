//! Structured query API, SQL pipeline, and `DataChunk` pull-based execution engine.

#[cfg(feature = "structured-api")]
pub mod api;
#[cfg(feature = "sql")]
pub mod chunk;
#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "structured-api")]
pub use api::{
    AccessPath, CmpOp, ColExpr, Expr, IntoValue, QueryError, QueryPlan, QueryRow, TableQuery, col,
};
#[cfg(feature = "sql")]
pub use chunk::{
    AggregateExpr, BinOp, BoundExpr, ChunkSourceExec, ColumnMeta, DataChunk, DistinctExec,
    Executor, FilterExec, HashAggregateExec, HashJoinExec, JoinKind, ProjectionExec, SqlError,
    TopNSortExec, UnaryOp, collect_executor,
};
pub use kv_core::KvError;
#[cfg(feature = "sql")]
pub use sql::{
    SqlResult, TxnControl, classify_txn_control, execute_statement_in_txn, format_sql_result,
    format_value, parse_sql, statement_is_write,
};
pub use table_codec::{IndexId, SchemaVersion, SortOrder, TableId, Value};

/// Default per-query memory quota (64 MiB) to guard against OOM in hash/sort operators.
pub const DEFAULT_QUERY_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Default batch size for `DataChunk` execution.
pub const DEFAULT_CHUNK_ROWS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryConfig {
    pub memory_budget_bytes: usize,
    pub chunk_rows: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            memory_budget_bytes: DEFAULT_QUERY_MEMORY_BUDGET_BYTES,
            chunk_rows: DEFAULT_CHUNK_ROWS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryConfig;

    #[test]
    fn default_query_config_has_bounded_memory() {
        let cfg = QueryConfig::default();
        assert!(cfg.memory_budget_bytes > 0);
        assert!((64..=1024).contains(&cfg.chunk_rows));
    }
}
