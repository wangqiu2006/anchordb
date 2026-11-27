//! Self-hosted System Catalog persisted inside `kv_core` under prefix `0x00`.
//!
//! Persists:
//! - Monotonic `TableId` allocator (`[0x00][0x01]`)
//! - Table name -> `TableId` lookup (`[0x00][0x02][name]`)
//! - Latest `TableSchema` (`[0x00][0x03][table_id BE]`)
//! - Immutable historical schema snapshots (`[0x00][0x04][table_id BE][version BE]`)

use core::ops::Bound;

use kv_core::{KvIterator, KvTransaction, KvTransactionExt};

use crate::key_encoding::{
    SYS_CATALOG_PREFIX, encode_non_unique_index_key, encode_tuple_comparable,
    encode_unique_index_key, prefix_upper_bound, table_row_prefix,
};
use crate::row_codec::{decode_row, decode_value, encode_value};
use crate::types::{
    CodecError, ColumnDef, ColumnType, IndexDef, SchemaVersion, SortOrder, TableId, TableSchema,
    Value,
};

const CAT_NEXT_TABLE_ID: u8 = 0x01;
const CAT_NAME_TO_ID: u8 = 0x02;
const CAT_LATEST_SCHEMA: u8 = 0x03;
const CAT_SCHEMA_HISTORY: u8 = 0x04;
const SCHEMA_ENCODING_MAGIC: u8 = 0x53;

/// User-facing column specification when creating a table or adding a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
    /// Constant default value frozen at DDL execution time.
    pub default_value: Option<Value>,
}

/// User-facing secondary index specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
    pub unique: bool,
    pub columns: Vec<(String, SortOrder)>,
}

/// System Catalog operations executed transactionally on top of any [`KvTransaction`].
pub struct SystemCatalog;

impl SystemCatalog {
    /// Creates a new table with primary key columns and persists schema version 1.
    pub fn create_table<T: KvTransaction>(
        txn: &mut T,
        name: &str,
        column_specs: &[ColumnSpec],
        primary_key_names: &[&str],
    ) -> Result<TableSchema, CodecError> {
        if name.is_empty() {
            return Err(CodecError::InvalidSchema(
                "table name cannot be empty".into(),
            ));
        }
        if column_specs.is_empty() {
            return Err(CodecError::InvalidSchema(
                "table must have at least one column".into(),
            ));
        }
        if primary_key_names.is_empty() {
            return Err(CodecError::InvalidSchema(
                "table must define a primary key".into(),
            ));
        }
        if txn.get(&key_name_to_id(name))?.is_some() {
            return Err(CodecError::TableAlreadyExists(name.to_owned()));
        }

        let mut columns = Vec::with_capacity(column_specs.len());
        let mut next_col_id = 1u32;
        for spec in column_specs {
            validate_column_spec(spec)?;
            if columns.iter().any(|c: &ColumnDef| c.name == spec.name) {
                return Err(CodecError::ColumnAlreadyExists(spec.name.clone()));
            }
            columns.push(ColumnDef {
                id: next_col_id,
                name: spec.name.clone(),
                data_type: spec.data_type,
                nullable: spec.nullable,
                default_value: spec.default_value.clone(),
            });
            next_col_id = next_col_id.saturating_add(1);
        }

        let mut primary_key_cols = Vec::with_capacity(primary_key_names.len());
        for &pk_name in primary_key_names {
            let col = columns
                .iter()
                .find(|c| c.name == pk_name)
                .ok_or_else(|| CodecError::ColumnNotFound(pk_name.to_owned()))?;
            if col.nullable {
                return Err(CodecError::InvalidSchema(format!(
                    "primary key column '{pk_name}' must be NOT NULL"
                )));
            }
            if primary_key_cols.contains(&col.id) {
                return Err(CodecError::InvalidSchema(format!(
                    "duplicate primary key column '{pk_name}'"
                )));
            }
            primary_key_cols.push(col.id);
        }

        let table_id = alloc_next_table_id(txn)?;
        let schema = TableSchema {
            table_id,
            name: name.to_owned(),
            version: 1,
            columns,
            primary_key_cols,
            indexes: Vec::new(),
            next_column_id: next_col_id,
            next_index_id: 1,
        };

        txn.put(&key_name_to_id(name), &table_id.to_le_bytes())?;
        persist_schema(txn, &schema)?;
        Ok(schema)
    }

    pub fn get_table_by_name<T: KvTransaction>(
        txn: &T,
        name: &str,
    ) -> Result<Option<TableSchema>, CodecError> {
        let Some(raw_id) = txn.get(&key_name_to_id(name))? else {
            return Ok(None);
        };
        if raw_id.len() != 4 {
            return Err(CodecError::InvalidEncoding);
        }
        let table_id = u32::from_le_bytes([raw_id[0], raw_id[1], raw_id[2], raw_id[3]]);
        Self::get_table_by_id(txn, table_id)
    }

    pub fn get_table_by_id<T: KvTransaction>(
        txn: &T,
        table_id: TableId,
    ) -> Result<Option<TableSchema>, CodecError> {
        let Some(raw) = txn.get(&key_latest_schema(table_id))? else {
            return Ok(None);
        };
        Ok(Some(decode_schema(&raw)?))
    }

    /// Loads any immutable historical version of a table's schema.
    pub fn get_table_version<T: KvTransaction>(
        txn: &T,
        table_id: TableId,
        version: SchemaVersion,
    ) -> Result<Option<TableSchema>, CodecError> {
        let Some(raw) = txn.get(&key_schema_history(table_id, version))? else {
            return Ok(None);
        };
        Ok(Some(decode_schema(&raw)?))
    }

    /// Lists all tables currently registered in the System Catalog.
    pub fn list_tables<T: KvTransaction>(txn: &T) -> Result<Vec<TableSchema>, CodecError> {
        let prefix = [SYS_CATALOG_PREFIX, CAT_LATEST_SCHEMA];
        let end = prefix_upper_bound(&prefix);
        let mut iter = txn.scan(
            Bound::Included(&prefix),
            match &end {
                Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                Bound::Included(v) => Bound::Included(v.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            },
        )?;
        let mut tables = Vec::new();
        while let Some((_k, v)) = iter.next_entry()? {
            tables.push(decode_schema(v)?);
        }
        Ok(tables)
    }

    /// Online `ADD COLUMN` (O(1) metadata update; zero historical row rewrite).
    pub fn add_column<T: KvTransaction>(
        txn: &mut T,
        table_id: TableId,
        spec: ColumnSpec,
    ) -> Result<TableSchema, CodecError> {
        validate_column_spec(&spec)?;
        if !spec.nullable && spec.default_value.is_none() {
            return Err(CodecError::InvalidSchema(format!(
                "adding NOT NULL column '{}' requires a frozen default value",
                spec.name
            )));
        }

        let mut schema = Self::get_table_by_id(txn, table_id)?
            .ok_or_else(|| CodecError::TableNotFound(format!("id={table_id}")))?;
        if schema.column_index_by_name(&spec.name).is_some() {
            return Err(CodecError::ColumnAlreadyExists(spec.name));
        }

        let col_id = schema.next_column_id;
        schema.next_column_id = schema.next_column_id.saturating_add(1);
        schema.version = schema.version.saturating_add(1);
        schema.columns.push(ColumnDef {
            id: col_id,
            name: spec.name,
            data_type: spec.data_type,
            nullable: spec.nullable,
            default_value: spec.default_value,
        });

        persist_schema(txn, &schema)?;
        Ok(schema)
    }

    /// Online `DROP COLUMN` (preserves historical schema snapshot; rejects PK or indexed columns).
    pub fn drop_column<T: KvTransaction>(
        txn: &mut T,
        table_id: TableId,
        column_name: &str,
    ) -> Result<TableSchema, CodecError> {
        let mut schema = Self::get_table_by_id(txn, table_id)?
            .ok_or_else(|| CodecError::TableNotFound(format!("id={table_id}")))?;
        let col_pos = schema
            .column_index_by_name(column_name)
            .ok_or_else(|| CodecError::ColumnNotFound(column_name.to_owned()))?;
        let col_id = schema.columns[col_pos].id;

        if schema.primary_key_cols.contains(&col_id) {
            return Err(CodecError::InvalidSchema(format!(
                "cannot drop primary key column '{column_name}'"
            )));
        }
        if schema
            .indexes
            .iter()
            .any(|idx| idx.column_ids.contains(&col_id))
        {
            return Err(CodecError::InvalidSchema(format!(
                "cannot drop column '{column_name}' referenced by an active secondary index"
            )));
        }

        schema.columns.remove(col_pos);
        schema.version = schema.version.saturating_add(1);
        persist_schema(txn, &schema)?;
        Ok(schema)
    }

    /// Creates a secondary index and backfills all existing rows transactionally (validating UNIQUE).
    pub fn create_index<T: KvTransaction>(
        txn: &mut T,
        table_id: TableId,
        spec: IndexSpec,
    ) -> Result<TableSchema, CodecError> {
        if spec.name.is_empty() || spec.columns.is_empty() {
            return Err(CodecError::InvalidSchema(
                "index name and column list cannot be empty".into(),
            ));
        }

        let mut schema = Self::get_table_by_id(txn, table_id)?
            .ok_or_else(|| CodecError::TableNotFound(format!("id={table_id}")))?;
        if schema.index_by_name(&spec.name).is_some() {
            return Err(CodecError::IndexAlreadyExists(spec.name));
        }

        let mut column_ids = Vec::with_capacity(spec.columns.len());
        let mut directions = Vec::with_capacity(spec.columns.len());
        for (col_name, dir) in &spec.columns {
            let pos = schema
                .column_index_by_name(col_name)
                .ok_or_else(|| CodecError::ColumnNotFound(col_name.clone()))?;
            let cid = schema.columns[pos].id;
            if column_ids.contains(&cid) {
                return Err(CodecError::InvalidSchema(format!(
                    "duplicate column '{col_name}' in index '{}'",
                    spec.name
                )));
            }
            column_ids.push(cid);
            directions.push(*dir);
        }

        let index_id = schema.next_index_id;
        schema.next_index_id = schema.next_index_id.saturating_add(1);
        schema.version = schema.version.saturating_add(1);
        let index_def = IndexDef {
            id: index_id,
            name: spec.name,
            unique: spec.unique,
            column_ids,
            directions,
        };

        // Collect existing rows before mutating KV to avoid holding an iterator while writing.
        let row_prefix = table_row_prefix(table_id);
        let row_end = prefix_upper_bound(&row_prefix);
        let mut existing_rows = Vec::new();
        {
            let mut iter = txn.scan(
                Bound::Included(&row_prefix),
                match &row_end {
                    Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                    Bound::Included(v) => Bound::Included(v.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                },
            )?;
            while let Some((raw_key, raw_val)) = iter.next_entry()? {
                let pk_encoded = raw_key
                    .get(row_prefix.len()..)
                    .ok_or(CodecError::InvalidEncoding)?
                    .to_vec();
                let (_ver, row_vals) = decode_row(&schema, raw_val)?;
                existing_rows.push((pk_encoded, row_vals));
            }
        }

        // Backfill index entries (enforcing UNIQUE constraints immediately).
        for (pk_encoded, row_vals) in existing_rows {
            let mut idx_vals = Vec::with_capacity(index_def.column_ids.len());
            let mut has_null = false;
            for &cid in &index_def.column_ids {
                let pos = schema
                    .column_index_by_id(cid)
                    .ok_or(CodecError::InvalidEncoding)?;
                let v = &row_vals[pos];
                if v.is_null() {
                    has_null = true;
                }
                idx_vals.push(v.clone());
            }
            let idx_encoded = encode_tuple_comparable(&idx_vals, &index_def.directions);

            if index_def.unique {
                let idx_key = encode_unique_index_key(
                    table_id,
                    index_def.id,
                    &idx_encoded,
                    &pk_encoded,
                    has_null,
                );
                if !has_null && txn.get(&idx_key)?.is_some() {
                    return Err(CodecError::DuplicateUniqueKey {
                        index_name: index_def.name,
                    });
                }
                txn.put(&idx_key, &pk_encoded)?;
            } else {
                let idx_key =
                    encode_non_unique_index_key(table_id, index_def.id, &idx_encoded, &pk_encoded);
                txn.put(&idx_key, &pk_encoded)?;
            }
        }

        schema.indexes.push(index_def);
        persist_schema(txn, &schema)?;
        Ok(schema)
    }
}

fn validate_column_spec(spec: &ColumnSpec) -> Result<(), CodecError> {
    if spec.name.is_empty() {
        return Err(CodecError::InvalidSchema(
            "column name cannot be empty".into(),
        ));
    }
    if let Some(default_val) = &spec.default_value {
        if default_val.is_null() && !spec.nullable {
            return Err(CodecError::NullConstraintViolation {
                column: spec.name.clone(),
            });
        }
        if !default_val.matches_type(spec.data_type) {
            return Err(CodecError::TypeMismatch {
                column: spec.name.clone(),
                expected: spec.data_type,
            });
        }
    }
    Ok(())
}

fn alloc_next_table_id<T: KvTransaction>(txn: &mut T) -> Result<TableId, CodecError> {
    let key = [SYS_CATALOG_PREFIX, CAT_NEXT_TABLE_ID];
    let current = match txn.get(&key)? {
        Some(raw) => {
            if raw.len() != 4 {
                return Err(CodecError::InvalidEncoding);
            }
            u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]])
        }
        None => 1,
    };
    let next = current.saturating_add(1);
    txn.put(&key, &next.to_le_bytes())?;
    Ok(current)
}

fn persist_schema<T: KvTransaction>(txn: &mut T, schema: &TableSchema) -> Result<(), CodecError> {
    let encoded = encode_schema(schema)?;
    txn.put(&key_latest_schema(schema.table_id), &encoded)?;
    txn.put(
        &key_schema_history(schema.table_id, schema.version),
        &encoded,
    )?;
    Ok(())
}

fn key_name_to_id(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + name.len());
    key.push(SYS_CATALOG_PREFIX);
    key.push(CAT_NAME_TO_ID);
    key.extend_from_slice(name.as_bytes());
    key
}

fn key_latest_schema(table_id: TableId) -> [u8; 6] {
    let b = table_id.to_be_bytes();
    [
        SYS_CATALOG_PREFIX,
        CAT_LATEST_SCHEMA,
        b[0],
        b[1],
        b[2],
        b[3],
    ]
}

fn key_schema_history(table_id: TableId, version: SchemaVersion) -> [u8; 10] {
    let t = table_id.to_be_bytes();
    let v = version.to_be_bytes();
    [
        SYS_CATALOG_PREFIX,
        CAT_SCHEMA_HISTORY,
        t[0],
        t[1],
        t[2],
        t[3],
        v[0],
        v[1],
        v[2],
        v[3],
    ]
}

fn encode_schema(schema: &TableSchema) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    out.push(SCHEMA_ENCODING_MAGIC);
    out.extend_from_slice(&schema.table_id.to_le_bytes());
    out.extend_from_slice(&schema.version.to_le_bytes());
    out.extend_from_slice(&schema.next_column_id.to_le_bytes());
    out.extend_from_slice(&schema.next_index_id.to_le_bytes());
    encode_str(&schema.name, &mut out)?;

    let col_count = u32::try_from(schema.columns.len()).map_err(|_| CodecError::InvalidEncoding)?;
    out.extend_from_slice(&col_count.to_le_bytes());
    for col in &schema.columns {
        out.extend_from_slice(&col.id.to_le_bytes());
        encode_str(&col.name, &mut out)?;
        out.push(col.data_type as u8);
        out.push(u8::from(col.nullable));
        match &col.default_value {
            Some(v) => {
                out.push(1);
                encode_value(v, &mut out)?;
            }
            None => out.push(0),
        }
    }

    let pk_count =
        u32::try_from(schema.primary_key_cols.len()).map_err(|_| CodecError::InvalidEncoding)?;
    out.extend_from_slice(&pk_count.to_le_bytes());
    for &cid in &schema.primary_key_cols {
        out.extend_from_slice(&cid.to_le_bytes());
    }

    let idx_count = u32::try_from(schema.indexes.len()).map_err(|_| CodecError::InvalidEncoding)?;
    out.extend_from_slice(&idx_count.to_le_bytes());
    for idx in &schema.indexes {
        out.extend_from_slice(&idx.id.to_le_bytes());
        encode_str(&idx.name, &mut out)?;
        out.push(u8::from(idx.unique));
        let ic_count =
            u32::try_from(idx.column_ids.len()).map_err(|_| CodecError::InvalidEncoding)?;
        out.extend_from_slice(&ic_count.to_le_bytes());
        for (&cid, &dir) in idx.column_ids.iter().zip(idx.directions.iter()) {
            out.extend_from_slice(&cid.to_le_bytes());
            out.push(dir as u8);
        }
    }

    Ok(out)
}

fn decode_schema(raw: &[u8]) -> Result<TableSchema, CodecError> {
    if raw.first().copied() != Some(SCHEMA_ENCODING_MAGIC) {
        return Err(CodecError::InvalidEncoding);
    }
    let mut cursor = 1usize;
    let table_id = read_u32(raw, &mut cursor)?;
    let version = read_u32(raw, &mut cursor)?;
    let next_column_id = read_u32(raw, &mut cursor)?;
    let next_index_id = read_u32(raw, &mut cursor)?;
    let name = decode_str(raw, &mut cursor)?;

    let col_count = read_u32(raw, &mut cursor)? as usize;
    let mut columns = Vec::with_capacity(col_count.min(1024));
    for _ in 0..col_count {
        let id = read_u32(raw, &mut cursor)?;
        let col_name = decode_str(raw, &mut cursor)?;
        let dt_tag = read_u8(raw, &mut cursor)?;
        let data_type = ColumnType::from_u8(dt_tag).ok_or(CodecError::InvalidEncoding)?;
        let nullable = read_bool(raw, &mut cursor)?;
        let has_default = read_bool(raw, &mut cursor)?;
        let default_value = if has_default {
            Some(decode_value(raw, &mut cursor)?)
        } else {
            None
        };
        columns.push(ColumnDef {
            id,
            name: col_name,
            data_type,
            nullable,
            default_value,
        });
    }

    let pk_count = read_u32(raw, &mut cursor)? as usize;
    let mut primary_key_cols = Vec::with_capacity(pk_count.min(64));
    for _ in 0..pk_count {
        primary_key_cols.push(read_u32(raw, &mut cursor)?);
    }

    let idx_count = read_u32(raw, &mut cursor)? as usize;
    let mut indexes = Vec::with_capacity(idx_count.min(256));
    for _ in 0..idx_count {
        let id = read_u32(raw, &mut cursor)?;
        let idx_name = decode_str(raw, &mut cursor)?;
        let unique = read_bool(raw, &mut cursor)?;
        let ic_count = read_u32(raw, &mut cursor)? as usize;
        let mut column_ids = Vec::with_capacity(ic_count.min(64));
        let mut directions = Vec::with_capacity(ic_count.min(64));
        for _ in 0..ic_count {
            column_ids.push(read_u32(raw, &mut cursor)?);
            let dir_tag = read_u8(raw, &mut cursor)?;
            directions.push(SortOrder::from_u8(dir_tag).ok_or(CodecError::InvalidEncoding)?);
        }
        indexes.push(IndexDef {
            id,
            name: idx_name,
            unique,
            column_ids,
            directions,
        });
    }

    if cursor != raw.len() {
        return Err(CodecError::InvalidEncoding);
    }

    Ok(TableSchema {
        table_id,
        name,
        version,
        columns,
        primary_key_cols,
        indexes,
        next_column_id,
        next_index_id,
    })
}

fn encode_str(s: &str, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let len = u32::try_from(s.len()).map_err(|_| CodecError::InvalidEncoding)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

fn decode_str(raw: &[u8], cursor: &mut usize) -> Result<String, CodecError> {
    let len = read_u32(raw, cursor)? as usize;
    let end = cursor.checked_add(len).ok_or(CodecError::InvalidEncoding)?;
    let slice = raw.get(*cursor..end).ok_or(CodecError::InvalidEncoding)?;
    *cursor = end;
    core::str::from_utf8(slice)
        .map(str::to_owned)
        .map_err(|_| CodecError::InvalidEncoding)
}

fn read_u8(raw: &[u8], cursor: &mut usize) -> Result<u8, CodecError> {
    let b = *raw.get(*cursor).ok_or(CodecError::InvalidEncoding)?;
    *cursor += 1;
    Ok(b)
}

fn read_bool(raw: &[u8], cursor: &mut usize) -> Result<bool, CodecError> {
    match read_u8(raw, cursor)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::InvalidEncoding),
    }
}

fn read_u32(raw: &[u8], cursor: &mut usize) -> Result<u32, CodecError> {
    let end = cursor.checked_add(4).ok_or(CodecError::InvalidEncoding)?;
    let s = raw.get(*cursor..end).ok_or(CodecError::InvalidEncoding)?;
    *cursor = end;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}
