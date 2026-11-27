//! Bridge connecting [`AnchorDb`] to [`server_net::PgServer`] (`mode-server`).

use std::io;
use std::sync::Arc;
use std::vec::Vec;

use kv_core::KvEngine;
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

impl<'a, E: KvEngine + Send + Sync + 'static> ServerSession for SqlSession<'a, E>
where
    E::Txn<'a>: Send,
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

impl<E: KvEngine + Send + Sync + 'static> SqlBackend for AnchorDb<E>
where
    for<'a> E::Txn<'a>: Send,
{
    type Session<'a>
        = SqlSession<'a, E>
    where
        Self: 'a;

    fn new_session(&self) -> Self::Session<'_> {
        self.sql_session()
    }
}

/// Starts a PostgreSQL v3.0 TCP server backed by any [`KvEngine`]-powered [`AnchorDb`].
pub fn start_pg_server<E: KvEngine + Send + Sync + 'static>(
    db: Arc<AnchorDb<E>>,
    config: ServerConfig,
) -> io::Result<PgServer>
where
    for<'a> E::Txn<'a>: Send,
{
    PgServer::start(db, config)
}
