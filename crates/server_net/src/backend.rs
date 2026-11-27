//! Decoupled backend boundary separating `server_net` from concrete storage implementations.

use query_engine::{SqlError, SqlResult};
use table_codec::TableSchema;

/// Stateful per-connection SQL session driven by [`crate::pg_server::PgServer`].
pub trait ServerSession: Send {
    fn in_transaction(&self) -> bool;
    fn execute(&mut self, sql: &str) -> Result<Vec<SqlResult>, SqlError>;
    fn list_tables(&self) -> Result<Vec<TableSchema>, SqlError>;
    fn rollback_if_active(&mut self);
}

/// Thread-safe factory creating isolated per-connection [`ServerSession`]s.
pub trait SqlBackend: Send + Sync + 'static {
    type Session<'a>: ServerSession
    where
        Self: 'a;

    fn new_session(&self) -> Self::Session<'_>;
}
