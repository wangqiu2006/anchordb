//! PostgreSQL wire protocol v3.0 server and `pg_catalog` / `information_schema` compatibility layer.

pub mod backend;
pub mod pg_catalog;
pub mod pg_server;

pub use backend::{ServerSession, SqlBackend};
pub use pg_server::PgServer;
use query_engine::QueryConfig;

/// Default loopback bind address to avoid accidental unauthenticated public exposure.
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:5432";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub password: Option<String>,
    pub query: QueryConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_LISTEN_ADDR.to_owned(),
            password: None,
            query: QueryConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_LISTEN_ADDR, ServerConfig};

    #[test]
    fn default_server_binds_loopback() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.listen_addr, DEFAULT_LISTEN_ADDR);
        assert!(cfg.listen_addr.starts_with("127.0.0.1:"));
        assert!(cfg.password.is_none());
    }
}
