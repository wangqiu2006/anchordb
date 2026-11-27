//! Bridge connecting [`AnchorDb`] to [`server_net::PgServer`] (`mode-server`).

use std::io;
use std::sync::Arc;
use std::vec::Vec;

use hal_vfs::FileSystem;
use query_engine::{SqlError, SqlResult};
use server_net::{PgServer, ServerConfig, ServerSession, SqlBackend};
use table_codec::TableSchema;

use crate::embedded::{AnchorDb, DbError, SqlSession};

fn map_db_err(err: DbError) -> SqlError {
    match err {
        DbError::Sql(e) => e,
        DbError::Codec(e) => SqlError::Codec(e),
        DbError::Kv(e) => SqlError::Kv(e),
        DbError::Query(e) => SqlError::Execution(std::format!("{e}")),
    }
}

impl<F: FileSystem + Send + Sync + 'static> ServerSession for SqlSession<'_, F>
where
    F::File: Send + Sync,
    F::Lock: Send + Sync,
{
    fn in_transaction(&self) -> bool {
        SqlSession::in_transaction(self)
    }

    fn execute(&mut self, sql: &str) -> Result<Vec<SqlResult>, SqlError> {
        SqlSession::execute(self, sql).map_err(map_db_err)
    }

    fn list_tables(&self) -> Result<Vec<TableSchema>, SqlError> {
        SqlSession::list_tables(self).map_err(map_db_err)
    }

    fn rollback_if_active(&mut self) {
        SqlSession::rollback_if_active(self);
    }
}

impl<F: FileSystem + Send + Sync + 'static> SqlBackend for AnchorDb<F>
where
    F::File: Send + Sync,
    F::Lock: Send + Sync,
{
    type Session<'a>
        = SqlSession<'a, F>
    where
        Self: 'a;

    fn new_session(&self) -> Self::Session<'_> {
        self.sql_session()
    }
}

/// Starts a PostgreSQL v3.0 TCP server backed by `db`.
pub fn start_pg_server<F: FileSystem + Send + Sync + 'static>(
    db: Arc<AnchorDb<F>>,
    config: ServerConfig,
) -> io::Result<PgServer>
where
    F::File: Send + Sync,
    F::Lock: Send + Sync,
{
    PgServer::start(db, config)
}
