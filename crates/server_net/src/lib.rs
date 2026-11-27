//! PostgreSQL wire protocol (`pgwire`) server and basic `pg_catalog` compatibility layer.

use query_engine::QueryConfig;

/// Default loopback bind address to avoid accidental unauthenticated public exposure.
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:5432";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub require_auth: bool,
    pub query: QueryConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_LISTEN_ADDR.to_owned(),
            require_auth: false,
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
    }
}
