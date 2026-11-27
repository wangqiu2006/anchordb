//! Ergonomic embedded database facade (`mode-embedded`) for desktop apps and games.

use core::fmt;
use std::borrow::ToOwned;
use std::path::Path;
use std::string::String;
use std::vec::Vec;

use hal_vfs::FileSystem;
use hal_vfs::std_fs::StdFileSystem;
use kv_core::{
    DoctorReport, KvEngine, KvError, KvTransaction, LsmConfig, LsmEngine, LsmTxn, inspect_database,
};
use query_engine::{
    Expr, QueryConfig, QueryError, QueryPlan, QueryRow, SqlError, SqlResult, TableQuery,
    TxnControl, classify_txn_control, execute_statement_in_txn, parse_sql, statement_is_write,
};
use table_codec::{
    CodecError, ColumnSpec, IndexSpec, SortOrder, SystemCatalog, TableId, TableSchema, TableStore,
    Value,
};

/// Unified error type exposed by [`AnchorDb`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbError {
    Kv(KvError),
    Codec(CodecError),
    Query(QueryError),
    Sql(SqlError),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kv(err) => write!(f, "{err}"),
            Self::Codec(err) => write!(f, "{err}"),
            Self::Query(err) => write!(f, "{err}"),
            Self::Sql(err) => write!(f, "{err}"),
        }
    }
}

impl core::error::Error for DbError {}

impl From<KvError> for DbError {
    fn from(value: KvError) -> Self {
        Self::Kv(value)
    }
}

impl From<CodecError> for DbError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<QueryError> for DbError {
    fn from(value: QueryError) -> Self {
        Self::Query(value)
    }
}

impl From<SqlError> for DbError {
    fn from(value: SqlError) -> Self {
        Self::Sql(value)
    }
}

/// Embedded AnchorDB handle supporting transactional CRUD, schema evolution, structured queries,
/// and embedded SQL execution.
pub struct AnchorDb<F: FileSystem = StdFileSystem> {
    engine: LsmEngine<F>,
    query_config: QueryConfig,
}

impl AnchorDb<StdFileSystem> {
    /// Opens or creates a local embedded database at `path` using default settings.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_config(path, LsmConfig::default(), QueryConfig::default())
    }

    /// Opens or creates a local embedded database at `path` with custom engine and query configs.
    pub fn open_with_config(
        path: impl AsRef<Path>,
        lsm_config: LsmConfig,
        query_config: QueryConfig,
    ) -> Result<Self, DbError> {
        let fs = StdFileSystem::open(path.as_ref()).map_err(KvError::from)?;
        Self::open_fs(fs, lsm_config, query_config)
    }

    /// Runs full on-disk CRC32C and format-version inspection on `path` (`anchordb doctor`).
    pub fn doctor(path: impl AsRef<Path>) -> Result<DoctorReport, DbError> {
        let fs = StdFileSystem::open(path.as_ref()).map_err(KvError::from)?;
        Ok(inspect_database(&fs)?)
    }
}

impl<F: FileSystem> AnchorDb<F> {
    /// Opens an [`AnchorDb`] over any custom [`FileSystem`] (such as `FaultSimFs` for crash tests).
    pub fn open_fs(
        fs: F,
        lsm_config: LsmConfig,
        query_config: QueryConfig,
    ) -> Result<Self, DbError> {
        let engine = LsmEngine::open(fs, lsm_config)?;
        Ok(Self {
            engine,
            query_config,
        })
    }

    /// Begins an explicit multi-statement transaction with Snapshot Isolation.
    pub fn begin_txn(&self, read_only: bool) -> Result<DbTxn<'_, F>, DbError> {
        let txn = self.engine.begin_txn(read_only)?;
        Ok(DbTxn {
            txn,
            query_config: self.query_config,
        })
    }

    /// Opens a stateful SQL session supporting `BEGIN` / `COMMIT` / `ROLLBACK` across statements.
    #[must_use]
    pub fn sql_session(&self) -> SqlSession<'_, F> {
        SqlSession {
            db: self,
            active_txn: None,
        }
    }

    /// Executes one or more SQL statements in a temporary session (auto-commit per statement
    /// unless explicit `BEGIN`/`COMMIT` are used inside the script).
    pub fn execute_sql(&self, sql: &str) -> Result<Vec<SqlResult>, DbError> {
        let mut session = self.sql_session();
        session.execute(sql)
    }

    /// Starts a chainable structured query against `table_name`:
    /// `db.table("items").filter(col("rarity").gte(4)).order_by("level", SortOrder::Desc).limit(20).collect()`
    #[must_use]
    pub fn table(&self, table_name: impl Into<String>) -> TableQueryHandle<'_, F> {
        TableQueryHandle {
            db: self,
            query: TableQuery::new(table_name),
        }
    }

    pub fn create_table(
        &self,
        name: &str,
        columns: &[ColumnSpec],
        primary_key: &[&str],
    ) -> Result<TableSchema, DbError> {
        let mut txn = self.begin_txn(false)?;
        let schema = txn.create_table(name, columns, primary_key)?;
        txn.commit()?;
        Ok(schema)
    }

    pub fn add_column(&self, table_name: &str, spec: ColumnSpec) -> Result<TableSchema, DbError> {
        let mut txn = self.begin_txn(false)?;
        let schema = txn.add_column(table_name, spec)?;
        txn.commit()?;
        Ok(schema)
    }

    pub fn drop_column(&self, table_name: &str, column_name: &str) -> Result<TableSchema, DbError> {
        let mut txn = self.begin_txn(false)?;
        let schema = txn.drop_column(table_name, column_name)?;
        txn.commit()?;
        Ok(schema)
    }

    pub fn create_index(&self, table_name: &str, spec: IndexSpec) -> Result<TableSchema, DbError> {
        let mut txn = self.begin_txn(false)?;
        let schema = txn.create_index(table_name, spec)?;
        txn.commit()?;
        Ok(schema)
    }

    pub fn get_table(&self, name: &str) -> Result<Option<TableSchema>, DbError> {
        let txn = self.begin_txn(true)?;
        txn.get_table(name)
    }

    pub fn list_tables(&self) -> Result<Vec<TableSchema>, DbError> {
        let txn = self.begin_txn(true)?;
        txn.list_tables()
    }

    pub fn insert(&self, table_name: &str, values: &[Value]) -> Result<(), DbError> {
        let mut txn = self.begin_txn(false)?;
        txn.insert(table_name, values)?;
        txn.commit()
    }

    pub fn get_by_pk(
        &self,
        table_name: &str,
        pk_values: &[Value],
    ) -> Result<Option<Vec<Value>>, DbError> {
        let txn = self.begin_txn(true)?;
        txn.get_by_pk(table_name, pk_values)
    }

    pub fn update(
        &self,
        table_name: &str,
        pk_values: &[Value],
        new_values: &[Value],
    ) -> Result<(), DbError> {
        let mut txn = self.begin_txn(false)?;
        txn.update(table_name, pk_values, new_values)?;
        txn.commit()
    }

    pub fn delete(&self, table_name: &str, pk_values: &[Value]) -> Result<bool, DbError> {
        let mut txn = self.begin_txn(false)?;
        let deleted = txn.delete(table_name, pk_values)?;
        txn.commit()?;
        Ok(deleted)
    }

    pub fn checkpoint(&self) -> Result<(), DbError> {
        self.engine.checkpoint()?;
        Ok(())
    }

    pub fn compact_all(&self) -> Result<(), DbError> {
        self.engine.compact_all()?;
        Ok(())
    }
}

/// Stateful SQL session supporting explicit `BEGIN` / `COMMIT` / `ROLLBACK` transactions
/// as well as auto-commit statements.
pub struct SqlSession<'a, F: FileSystem> {
    db: &'a AnchorDb<F>,
    active_txn: Option<LsmTxn<'a, F>>,
}

impl<F: FileSystem> SqlSession<'_, F> {
    #[must_use]
    pub const fn in_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    pub fn list_tables(&self) -> Result<Vec<TableSchema>, DbError> {
        if let Some(txn) = self.active_txn.as_ref() {
            Ok(SystemCatalog::list_tables(txn)?)
        } else {
            self.db.list_tables()
        }
    }

    pub fn rollback_if_active(&mut self) {
        if let Some(txn) = self.active_txn.take() {
            txn.abort();
        }
    }

    /// Parses and executes one or more semicolon-separated SQL statements.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<SqlResult>, DbError> {
        let statements = parse_sql(sql)?;
        let mut results = Vec::with_capacity(statements.len());

        for stmt in &statements {
            if let Some(ctrl) = classify_txn_control(stmt) {
                match ctrl {
                    TxnControl::Begin => {
                        if self.active_txn.is_some() {
                            return Err(DbError::Sql(SqlError::Transaction(
                                "a transaction is already active".to_owned(),
                            )));
                        }
                        self.active_txn = Some(self.db.engine.begin_txn(false)?);
                        results.push(SqlResult::Message("BEGIN".to_owned()));
                    }
                    TxnControl::Commit => {
                        let Some(txn) = self.active_txn.take() else {
                            return Err(DbError::Sql(SqlError::Transaction(
                                "no active transaction to COMMIT".to_owned(),
                            )));
                        };
                        txn.commit()?;
                        results.push(SqlResult::Message("COMMIT".to_owned()));
                    }
                    TxnControl::Rollback => {
                        let Some(txn) = self.active_txn.take() else {
                            return Err(DbError::Sql(SqlError::Transaction(
                                "no active transaction to ROLLBACK".to_owned(),
                            )));
                        };
                        txn.abort();
                        results.push(SqlResult::Message("ROLLBACK".to_owned()));
                    }
                }
                continue;
            }

            if let Some(txn) = self.active_txn.as_mut() {
                let res = execute_statement_in_txn(txn, stmt, &self.db.query_config)?;
                results.push(res);
            } else {
                let read_only = !statement_is_write(stmt);
                let mut txn = self.db.engine.begin_txn(read_only)?;
                match execute_statement_in_txn(&mut txn, stmt, &self.db.query_config) {
                    Ok(res) => {
                        if !read_only {
                            txn.commit()?;
                        }
                        results.push(res);
                    }
                    Err(err) => {
                        txn.abort();
                        return Err(DbError::Sql(err));
                    }
                }
            }
        }

        Ok(results)
    }
}

/// Explicit multi-statement transaction handle over [`AnchorDb`].
pub struct DbTxn<'a, F: FileSystem> {
    txn: LsmTxn<'a, F>,
    query_config: QueryConfig,
}

impl<F: FileSystem> DbTxn<'_, F> {
    pub fn create_table(
        &mut self,
        name: &str,
        columns: &[ColumnSpec],
        primary_key: &[&str],
    ) -> Result<TableSchema, DbError> {
        Ok(SystemCatalog::create_table(
            &mut self.txn,
            name,
            columns,
            primary_key,
        )?)
    }

    pub fn add_column(
        &mut self,
        table_name: &str,
        spec: ColumnSpec,
    ) -> Result<TableSchema, DbError> {
        let table_id = self.resolve_table_id(table_name)?;
        Ok(SystemCatalog::add_column(&mut self.txn, table_id, spec)?)
    }

    pub fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
    ) -> Result<TableSchema, DbError> {
        let table_id = self.resolve_table_id(table_name)?;
        Ok(SystemCatalog::drop_column(
            &mut self.txn,
            table_id,
            column_name,
        )?)
    }

    pub fn create_index(
        &mut self,
        table_name: &str,
        spec: IndexSpec,
    ) -> Result<TableSchema, DbError> {
        let table_id = self.resolve_table_id(table_name)?;
        Ok(SystemCatalog::create_index(&mut self.txn, table_id, spec)?)
    }

    pub fn get_table(&self, name: &str) -> Result<Option<TableSchema>, DbError> {
        Ok(SystemCatalog::get_table_by_name(&self.txn, name)?)
    }

    pub fn list_tables(&self) -> Result<Vec<TableSchema>, DbError> {
        Ok(SystemCatalog::list_tables(&self.txn)?)
    }

    pub fn insert(&mut self, table_name: &str, values: &[Value]) -> Result<(), DbError> {
        let schema = self.require_schema(table_name)?;
        TableStore::insert_row(&mut self.txn, &schema, values)?;
        Ok(())
    }

    pub fn get_by_pk(
        &self,
        table_name: &str,
        pk_values: &[Value],
    ) -> Result<Option<Vec<Value>>, DbError> {
        let schema = self.require_schema(table_name)?;
        Ok(TableStore::get_row_by_pk(&self.txn, &schema, pk_values)?)
    }

    pub fn update(
        &mut self,
        table_name: &str,
        pk_values: &[Value],
        new_values: &[Value],
    ) -> Result<(), DbError> {
        let schema = self.require_schema(table_name)?;
        TableStore::update_row(&mut self.txn, &schema, pk_values, new_values)?;
        Ok(())
    }

    pub fn delete(&mut self, table_name: &str, pk_values: &[Value]) -> Result<bool, DbError> {
        let schema = self.require_schema(table_name)?;
        Ok(TableStore::delete_row(&mut self.txn, &schema, pk_values)?)
    }

    pub fn query(&self, query: &TableQuery) -> Result<Vec<QueryRow>, DbError> {
        Ok(query.execute(&self.txn, &self.query_config)?)
    }

    pub fn explain(&self, query: &TableQuery) -> Result<QueryPlan, DbError> {
        Ok(query.explain(&self.txn)?)
    }

    pub fn commit(self) -> Result<(), DbError> {
        self.txn.commit()?;
        Ok(())
    }

    pub fn abort(self) {
        self.txn.abort();
    }

    fn require_schema(&self, table_name: &str) -> Result<TableSchema, DbError> {
        SystemCatalog::get_table_by_name(&self.txn, table_name)?
            .ok_or_else(|| DbError::Codec(CodecError::TableNotFound(table_name.to_owned())))
    }

    fn resolve_table_id(&self, table_name: &str) -> Result<TableId, DbError> {
        Ok(self.require_schema(table_name)?.table_id)
    }
}

/// Fluent chainable query handle returned by [`AnchorDb::table`].
pub struct TableQueryHandle<'a, F: FileSystem> {
    db: &'a AnchorDb<F>,
    query: TableQuery,
}

impl<F: FileSystem> TableQueryHandle<'_, F> {
    #[must_use]
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.query = self.query.select(columns);
        self
    }

    #[must_use]
    pub fn filter(mut self, expr: Expr) -> Self {
        self.query = self.query.filter(expr);
        self
    }

    #[must_use]
    pub fn order_by(mut self, column: impl Into<String>, direction: SortOrder) -> Self {
        self.query = self.query.order_by(column, direction);
        self
    }

    #[must_use]
    pub fn limit(mut self, count: usize) -> Self {
        self.query = self.query.limit(count);
        self
    }

    #[must_use]
    pub fn offset(mut self, skip: usize) -> Self {
        self.query = self.query.offset(skip);
        self
    }

    pub fn explain(&self) -> Result<QueryPlan, DbError> {
        let txn = self.db.begin_txn(true)?;
        txn.explain(&self.query)
    }

    pub fn collect(&self) -> Result<Vec<QueryRow>, DbError> {
        let txn = self.db.begin_txn(true)?;
        txn.query(&self.query)
    }
}
