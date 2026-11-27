//! Table schema, System Catalog, versioned row encoding, and order-preserving Memcomparable keys.

pub mod catalog;
pub mod key_encoding;
pub mod row_codec;
pub mod table_store;
pub mod types;

pub use catalog::{ColumnSpec, IndexSpec, SystemCatalog};
pub use key_encoding::{
    INDEX_TAG, ROW_TAG, SYS_CATALOG_PREFIX, UNIQUE_INDEX_TAG, USER_TABLE_PREFIX,
    encode_non_unique_index_key, encode_row_key, encode_tuple_comparable, encode_unique_index_key,
    encode_value_comparable, prefix_upper_bound, table_row_prefix,
};
pub use kv_core::KvError;
pub use row_codec::{decode_row, encode_row};
pub use table_store::TableStore;
pub use types::{
    CodecError, ColumnDef, ColumnId, ColumnType, IndexDef, IndexId, SchemaVersion, SortOrder,
    TableId, TableSchema, Value,
};
