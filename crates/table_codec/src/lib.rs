//! Table schema, versioned row encoding, and order-preserving Memcomparable key encoding.

pub use kv_core::KvError;

pub type TableId = u32;
pub type IndexId = u32;
pub type SchemaVersion = u32;

/// Key-space prefix reserved for internal System Catalog tables.
pub const SYS_CATALOG_PREFIX: u8 = 0x00;

/// Key-space prefix reserved for user tables and indexes.
pub const USER_TABLE_PREFIX: u8 = 0x01;

/// Sub-prefix tags within a table key namespace.
pub const ROW_TAG: u8 = b'r';
pub const INDEX_TAG: u8 = b'i';
pub const UNIQUE_INDEX_TAG: u8 = b'u';

#[cfg(test)]
mod tests {
    use super::{INDEX_TAG, ROW_TAG, SYS_CATALOG_PREFIX, UNIQUE_INDEX_TAG, USER_TABLE_PREFIX};

    #[test]
    fn keyspace_prefixes_are_distinct() {
        assert_ne!(SYS_CATALOG_PREFIX, USER_TABLE_PREFIX);
        assert_ne!(ROW_TAG, INDEX_TAG);
        assert_ne!(ROW_TAG, UNIQUE_INDEX_TAG);
        assert_ne!(INDEX_TAG, UNIQUE_INDEX_TAG);
    }
}
