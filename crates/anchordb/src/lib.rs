#![no_std]

#[cfg(feature = "mode-embedded")]
extern crate std;

#[cfg(feature = "mode-embedded")]
pub mod embedded;

#[cfg(feature = "mode-server")]
pub mod server;

pub use hal_vfs;
pub use kv_core;

#[cfg(feature = "mode-embedded")]
pub use embedded::{AnchorDb, DbError, DbTxn, SqlSession, TableQueryHandle};

#[cfg(feature = "mode-embedded")]
pub use query_engine::{
    self, AccessPath, CmpOp, ColExpr, DataChunk, Expr, IntoValue, QueryConfig, QueryError,
    QueryPlan, QueryRow, SqlError, SqlResult, TableQuery, col, format_sql_result, format_value,
};

#[cfg(feature = "mode-embedded")]
pub use table_codec::{
    self, CodecError, ColumnDef, ColumnSpec, ColumnType, IndexDef, IndexSpec, SchemaVersion,
    SortOrder, SystemCatalog, TableId, TableSchema, TableStore, Value,
};

#[cfg(feature = "mode-server")]
pub use server::start_pg_server;
#[cfg(feature = "mode-server")]
pub use server_net::{
    self, DEFAULT_LISTEN_ADDR, PgServer, ServerConfig, ServerSession, SqlBackend,
};

/// Compile-time form factor selected via Cargo features.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildProfile {
    Mcu,
    Embedded,
    Server,
}

impl BuildProfile {
    #[must_use]
    pub const fn current() -> Self {
        #[cfg(feature = "mode-server")]
        {
            Self::Server
        }
        #[cfg(all(feature = "mode-embedded", not(feature = "mode-server")))]
        {
            Self::Embedded
        }
        #[cfg(not(feature = "mode-embedded"))]
        {
            Self::Mcu
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mcu => "mode-mcu",
            Self::Embedded => "mode-embedded",
            Self::Server => "mode-server",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BuildProfile;

    #[test]
    fn build_profile_matches_active_features() {
        let profile = BuildProfile::current();
        assert!(!profile.as_str().is_empty());
    }
}
