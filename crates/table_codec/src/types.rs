//! Core relational types, values, schema definitions, and codec errors.

use core::cmp::Ordering;
use core::fmt;

use kv_core::KvError;

pub type TableId = u32;
pub type IndexId = u32;
pub type ColumnId = u32;
pub type SchemaVersion = u32;

/// Logical column data types supported by AnchorDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnType {
    Bool = 1,
    Int64 = 2,
    Float64 = 3,
    Text = 4,
    Blob = 5,
}

impl ColumnType {
    pub const fn from_u8(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Bool),
            2 => Some(Self::Int64),
            3 => Some(Self::Float64),
            4 => Some(Self::Text),
            5 => Some(Self::Blob),
            _ => None,
        }
    }
}

/// Sort direction for primary keys and secondary index columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SortOrder {
    Asc = 0,
    Desc = 1,
}

impl SortOrder {
    pub const fn from_u8(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Asc),
            1 => Some(Self::Desc),
            _ => None,
        }
    }
}

/// Canonicalizes `f64` bit patterns so `-0.0 == +0.0` and all NaNs share one canonical ordering.
#[must_use]
pub fn canonical_f64(v: f64) -> f64 {
    if v.is_nan() {
        f64::from_bits(0x7FF8_0000_0000_0000)
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

/// Runtime typed scalar value.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    #[must_use]
    pub const fn data_type(&self) -> Option<ColumnType> {
        match self {
            Self::Null => None,
            Self::Bool(_) => Some(ColumnType::Bool),
            Self::Int64(_) => Some(ColumnType::Int64),
            Self::Float64(_) => Some(ColumnType::Float64),
            Self::Text(_) => Some(ColumnType::Text),
            Self::Blob(_) => Some(ColumnType::Blob),
        }
    }

    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub fn matches_type(&self, expected: ColumnType) -> bool {
        match self.data_type() {
            None => true,
            Some(actual) => actual == expected,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => {
                canonical_f64(*a).to_bits() == canonical_f64(*b).to_bits()
            }
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Blob(a), Self::Blob(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        fn discriminant(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Int64(_) => 2,
                Value::Float64(_) => 3,
                Value::Text(_) => 4,
                Value::Blob(_) => 5,
            }
        }
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::Float64(a), Self::Float64(b)) => canonical_f64(*a).total_cmp(&canonical_f64(*b)),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Blob(a), Self::Blob(b)) => a.cmp(b),
            _ => discriminant(self).cmp(&discriminant(other)),
        }
    }
}

/// Column definition inside a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
    /// Constant default value frozen at DDL time (used for zero-rewrite schema evolution).
    pub default_value: Option<Value>,
}

/// Secondary index definition (supports composite, unique/non-unique, mixed ASC/DESC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub id: IndexId,
    pub name: String,
    pub unique: bool,
    pub column_ids: Vec<ColumnId>,
    pub directions: Vec<SortOrder>,
}

/// Full versioned table schema persisted in the System Catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table_id: TableId,
    pub name: String,
    pub version: SchemaVersion,
    pub columns: Vec<ColumnDef>,
    pub primary_key_cols: Vec<ColumnId>,
    pub indexes: Vec<IndexDef>,
    pub next_column_id: ColumnId,
    pub next_index_id: IndexId,
}

impl TableSchema {
    #[must_use]
    pub fn column_index_by_id(&self, col_id: ColumnId) -> Option<usize> {
        self.columns.iter().position(|c| c.id == col_id)
    }

    #[must_use]
    pub fn column_index_by_name(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    #[must_use]
    pub fn index_by_id(&self, index_id: IndexId) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.id == index_id)
    }

    #[must_use]
    pub fn index_by_name(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.name == name)
    }
}

/// Errors produced by `table_codec` encoding, catalog DDL, or row/index operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    Kv(KvError),
    InvalidEncoding,
    TypeMismatch {
        column: String,
        expected: ColumnType,
    },
    NullConstraintViolation {
        column: String,
    },
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    DuplicatePrimaryKey,
    DuplicateUniqueKey {
        index_name: String,
    },
    TableAlreadyExists(String),
    TableNotFound(String),
    ColumnAlreadyExists(String),
    ColumnNotFound(String),
    IndexAlreadyExists(String),
    IndexNotFound(String),
    InvalidSchema(String),
    RowNotFound,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kv(err) => write!(f, "kv error: {err}"),
            Self::InvalidEncoding => f.write_str("invalid binary key or row encoding"),
            Self::TypeMismatch { column, expected } => {
                write!(
                    f,
                    "type mismatch for column '{column}', expected {expected:?}"
                )
            }
            Self::NullConstraintViolation { column } => {
                write!(f, "NOT NULL constraint violated for column '{column}'")
            }
            Self::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "column count mismatch: expected {expected}, got {actual}"
                )
            }
            Self::DuplicatePrimaryKey => f.write_str("duplicate primary key"),
            Self::DuplicateUniqueKey { index_name } => {
                write!(f, "duplicate value violates unique index '{index_name}'")
            }
            Self::TableAlreadyExists(name) => write!(f, "table '{name}' already exists"),
            Self::TableNotFound(name) => write!(f, "table '{name}' not found"),
            Self::ColumnAlreadyExists(name) => write!(f, "column '{name}' already exists"),
            Self::ColumnNotFound(name) => write!(f, "column '{name}' not found"),
            Self::IndexAlreadyExists(name) => write!(f, "index '{name}' already exists"),
            Self::IndexNotFound(name) => write!(f, "index '{name}' not found"),
            Self::InvalidSchema(msg) => write!(f, "invalid schema: {msg}"),
            Self::RowNotFound => f.write_str("row not found"),
        }
    }
}

impl core::error::Error for CodecError {}

impl From<KvError> for CodecError {
    fn from(value: KvError) -> Self {
        Self::Kv(value)
    }
}
