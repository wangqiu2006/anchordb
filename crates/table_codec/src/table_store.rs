//! Transactional table row CRUD, automatic secondary index maintenance, and index scans.

use core::ops::Bound;

use kv_core::{KvIterator, KvTransaction, KvTransactionExt};

use crate::key_encoding::{
    encode_non_unique_index_key, encode_row_key, encode_tuple_comparable, encode_unique_index_key,
    non_unique_index_prefix, prefix_upper_bound, table_row_prefix, unique_index_non_null_prefix,
    unique_index_prefix,
};
use crate::row_codec::{decode_row, encode_row};
use crate::types::{CodecError, IndexDef, SortOrder, TableSchema, Value};

/// High-level relational row & index operations executing inside a single [`KvTransaction`].
pub struct TableStore;

impl TableStore {
    /// Inserts a new row and all of its secondary index entries (rejecting PK / UNIQUE duplicates).
    pub fn insert_row<T: KvTransaction>(
        txn: &mut T,
        schema: &TableSchema,
        values: &[Value],
    ) -> Result<(), CodecError> {
        let encoded_row = encode_row(schema, values)?;
        let pk_encoded = encode_primary_key_from_row(schema, values)?;
        let row_key = encode_row_key(schema.table_id, &pk_encoded);

        if txn.get(&row_key)?.is_some() {
            return Err(CodecError::DuplicatePrimaryKey);
        }

        // Maintain all secondary indexes before committing the row (enforcing UNIQUE).
        for idx in &schema.indexes {
            insert_index_entry(txn, schema, idx, values, &pk_encoded)?;
        }

        txn.put(&row_key, &encoded_row)?;
        Ok(())
    }

    /// Point lookup by primary key values (decodes historical rows onto `schema`).
    pub fn get_row_by_pk<T: KvTransaction>(
        txn: &T,
        schema: &TableSchema,
        pk_values: &[Value],
    ) -> Result<Option<Vec<Value>>, CodecError> {
        let pk_encoded = encode_primary_key_values(schema, pk_values)?;
        let row_key = encode_row_key(schema.table_id, &pk_encoded);
        let Some(raw) = txn.get(&row_key)? else {
            return Ok(None);
        };
        let (_ver, row) = decode_row(schema, &raw)?;
        Ok(Some(row))
    }

    /// Updates an existing row identified by `pk_values` and atomically updates secondary indexes.
    pub fn update_row<T: KvTransaction>(
        txn: &mut T,
        schema: &TableSchema,
        pk_values: &[Value],
        new_values: &[Value],
    ) -> Result<(), CodecError> {
        let old_pk_encoded = encode_primary_key_values(schema, pk_values)?;
        let old_row_key = encode_row_key(schema.table_id, &old_pk_encoded);
        let Some(old_raw) = txn.get(&old_row_key)? else {
            return Err(CodecError::RowNotFound);
        };
        let (_old_ver, old_values) = decode_row(schema, &old_raw)?;

        let new_encoded_row = encode_row(schema, new_values)?;
        let new_pk_encoded = encode_primary_key_from_row(schema, new_values)?;
        let new_row_key = encode_row_key(schema.table_id, &new_pk_encoded);

        // If primary key changed, ensure the destination PK does not already exist.
        if new_row_key != old_row_key && txn.get(&new_row_key)?.is_some() {
            return Err(CodecError::DuplicatePrimaryKey);
        }

        // Remove old index entries first so updating a unique row with unchanged key never self-collides.
        for idx in &schema.indexes {
            delete_index_entry(txn, schema, idx, &old_values, &old_pk_encoded)?;
        }

        // Insert new index entries (validating UNIQUE against other rows).
        for idx in &schema.indexes {
            insert_index_entry(txn, schema, idx, new_values, &new_pk_encoded)?;
        }

        if new_row_key != old_row_key {
            txn.delete(&old_row_key)?;
        }
        txn.put(&new_row_key, &new_encoded_row)?;
        Ok(())
    }

    /// Deletes a row by primary key along with all of its secondary index entries.
    pub fn delete_row<T: KvTransaction>(
        txn: &mut T,
        schema: &TableSchema,
        pk_values: &[Value],
    ) -> Result<bool, CodecError> {
        let pk_encoded = encode_primary_key_values(schema, pk_values)?;
        let row_key = encode_row_key(schema.table_id, &pk_encoded);
        let Some(old_raw) = txn.get(&row_key)? else {
            return Ok(false);
        };
        let (_old_ver, old_values) = decode_row(schema, &old_raw)?;

        for idx in &schema.indexes {
            delete_index_entry(txn, schema, idx, &old_values, &pk_encoded)?;
        }
        txn.delete(&row_key)?;
        Ok(true)
    }

    /// Full ordered table scan in primary key order.
    pub fn scan_table<T: KvTransaction>(
        txn: &T,
        schema: &TableSchema,
    ) -> Result<Vec<Vec<Value>>, CodecError> {
        let prefix = table_row_prefix(schema.table_id);
        let end = prefix_upper_bound(&prefix);
        let mut iter = txn.scan(Bound::Included(&prefix), bound_as_slice(&end))?;
        let mut rows = Vec::new();
        while let Some((_k, raw)) = iter.next_entry()? {
            let (_ver, row) = decode_row(schema, raw)?;
            rows.push(row);
        }
        Ok(rows)
    }

    /// Scans rows matching an index prefix (supports composite index leftmost-prefix queries),
    /// returning full rows ordered by the index's configured `ASC`/`DESC` directions.
    pub fn scan_index_prefix<T: KvTransaction>(
        txn: &T,
        schema: &TableSchema,
        index_name: &str,
        prefix_values: &[Value],
    ) -> Result<Vec<Vec<Value>>, CodecError> {
        let idx = schema
            .index_by_name(index_name)
            .ok_or_else(|| CodecError::IndexNotFound(index_name.to_owned()))?;
        if prefix_values.len() > idx.column_ids.len() {
            return Err(CodecError::ColumnCountMismatch {
                expected: idx.column_ids.len(),
                actual: prefix_values.len(),
            });
        }

        let has_null = prefix_values.iter().any(Value::is_null);
        let encoded_prefix_vals = encode_tuple_comparable(prefix_values, &idx.directions);

        let base_prefix = if idx.unique {
            if has_null {
                let mut p = unique_index_prefix(schema.table_id, idx.id);
                p.push(0x00);
                p
            } else if prefix_values.is_empty() {
                unique_index_prefix(schema.table_id, idx.id)
            } else {
                unique_index_non_null_prefix(schema.table_id, idx.id)
            }
        } else {
            non_unique_index_prefix(schema.table_id, idx.id)
        };

        let mut scan_start = base_prefix;
        scan_start.extend_from_slice(&encoded_prefix_vals);
        let scan_end = prefix_upper_bound(&scan_start);

        scan_index_bounds_and_fetch_rows(
            txn,
            schema,
            Bound::Included(&scan_start),
            bound_as_slice(&scan_end),
        )
    }

    /// Range scan over a secondary index (`start`..`end` bounds on indexed column values).
    ///
    /// Note: For `UNIQUE` indexes this ranges over non-null index entries.
    pub fn scan_index_range<T: KvTransaction>(
        txn: &T,
        schema: &TableSchema,
        index_name: &str,
        start: Bound<&[Value]>,
        end: Bound<&[Value]>,
    ) -> Result<Vec<Vec<Value>>, CodecError> {
        let idx = schema
            .index_by_name(index_name)
            .ok_or_else(|| CodecError::IndexNotFound(index_name.to_owned()))?;

        let base_prefix = if idx.unique {
            unique_index_non_null_prefix(schema.table_id, idx.id)
        } else {
            non_unique_index_prefix(schema.table_id, idx.id)
        };

        let start_key: Bound<Vec<u8>> = match start {
            Bound::Unbounded => Bound::Included(base_prefix.clone()),
            Bound::Included(vals) => {
                let mut k = base_prefix.clone();
                k.extend_from_slice(&encode_tuple_comparable(vals, &idx.directions));
                Bound::Included(k)
            }
            Bound::Excluded(vals) => {
                // Skip every key sharing the exact prefix `base_prefix + encoded(vals)`.
                let mut k = base_prefix.clone();
                k.extend_from_slice(&encode_tuple_comparable(vals, &idx.directions));
                match prefix_upper_bound(&k) {
                    Bound::Excluded(next) => Bound::Included(next),
                    Bound::Included(next) => Bound::Included(next),
                    Bound::Unbounded => return Ok(Vec::new()),
                }
            }
        };

        let end_key: Bound<Vec<u8>> = match end {
            Bound::Unbounded => prefix_upper_bound(&base_prefix),
            Bound::Excluded(vals) => {
                let mut k = base_prefix.clone();
                k.extend_from_slice(&encode_tuple_comparable(vals, &idx.directions));
                Bound::Excluded(k)
            }
            Bound::Included(vals) => {
                // Include all primary keys sharing the exact indexed tuple `vals`.
                let mut k = base_prefix.clone();
                k.extend_from_slice(&encode_tuple_comparable(vals, &idx.directions));
                prefix_upper_bound(&k)
            }
        };

        scan_index_bounds_and_fetch_rows(
            txn,
            schema,
            bound_as_slice(&start_key),
            bound_as_slice(&end_key),
        )
    }
}

/// Scans index entries and performs point lookups back to the primary table **inside the same
/// transaction** (leveraging `KvTransaction`'s `&self` read methods with zero borrow conflict).
fn scan_index_bounds_and_fetch_rows<T: KvTransaction>(
    txn: &T,
    schema: &TableSchema,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Result<Vec<Vec<Value>>, CodecError> {
    let mut iter = txn.scan(start, end)?;
    let mut rows = Vec::new();
    while let Some((_idx_key, pk_encoded)) = iter.next_entry()? {
        let row_key = encode_row_key(schema.table_id, pk_encoded);
        let Some(raw_row) = txn.get(&row_key)? else {
            return Err(CodecError::InvalidEncoding);
        };
        let (_ver, row) = decode_row(schema, &raw_row)?;
        rows.push(row);
    }
    Ok(rows)
}

fn bound_as_slice(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn encode_primary_key_from_row(
    schema: &TableSchema,
    row_values: &[Value],
) -> Result<Vec<u8>, CodecError> {
    let mut pk_vals = Vec::with_capacity(schema.primary_key_cols.len());
    let dirs = vec![SortOrder::Asc; schema.primary_key_cols.len()];
    for &cid in &schema.primary_key_cols {
        let pos = schema
            .column_index_by_id(cid)
            .ok_or(CodecError::InvalidEncoding)?;
        pk_vals.push(row_values[pos].clone());
    }
    Ok(encode_tuple_comparable(&pk_vals, &dirs))
}

fn encode_primary_key_values(
    schema: &TableSchema,
    pk_values: &[Value],
) -> Result<Vec<u8>, CodecError> {
    if pk_values.len() != schema.primary_key_cols.len() {
        return Err(CodecError::ColumnCountMismatch {
            expected: schema.primary_key_cols.len(),
            actual: pk_values.len(),
        });
    }
    for (&cid, val) in schema.primary_key_cols.iter().zip(pk_values.iter()) {
        let pos = schema
            .column_index_by_id(cid)
            .ok_or(CodecError::InvalidEncoding)?;
        let col = &schema.columns[pos];
        if val.is_null() {
            return Err(CodecError::NullConstraintViolation {
                column: col.name.clone(),
            });
        }
        if !val.matches_type(col.data_type) {
            return Err(CodecError::TypeMismatch {
                column: col.name.clone(),
                expected: col.data_type,
            });
        }
    }
    let dirs = vec![SortOrder::Asc; pk_values.len()];
    Ok(encode_tuple_comparable(pk_values, &dirs))
}

fn extract_index_key(
    schema: &TableSchema,
    idx: &IndexDef,
    row_values: &[Value],
    pk_encoded: &[u8],
) -> Result<(Vec<u8>, bool), CodecError> {
    let mut idx_vals = Vec::with_capacity(idx.column_ids.len());
    let mut has_null = false;
    for &cid in &idx.column_ids {
        let pos = schema
            .column_index_by_id(cid)
            .ok_or(CodecError::InvalidEncoding)?;
        let val = &row_values[pos];
        if val.is_null() {
            has_null = true;
        }
        idx_vals.push(val.clone());
    }
    let idx_encoded = encode_tuple_comparable(&idx_vals, &idx.directions);
    let key = if idx.unique {
        encode_unique_index_key(schema.table_id, idx.id, &idx_encoded, pk_encoded, has_null)
    } else {
        encode_non_unique_index_key(schema.table_id, idx.id, &idx_encoded, pk_encoded)
    };
    Ok((key, has_null))
}

fn insert_index_entry<T: KvTransaction>(
    txn: &mut T,
    schema: &TableSchema,
    idx: &IndexDef,
    row_values: &[Value],
    pk_encoded: &[u8],
) -> Result<(), CodecError> {
    let (key, has_null) = extract_index_key(schema, idx, row_values, pk_encoded)?;
    if idx.unique && !has_null && txn.get(&key)?.is_some() {
        return Err(CodecError::DuplicateUniqueKey {
            index_name: idx.name.clone(),
        });
    }
    txn.put(&key, pk_encoded)?;
    Ok(())
}

fn delete_index_entry<T: KvTransaction>(
    txn: &mut T,
    schema: &TableSchema,
    idx: &IndexDef,
    row_values: &[Value],
    pk_encoded: &[u8],
) -> Result<(), CodecError> {
    let (key, _has_null) = extract_index_key(schema, idx, row_values, pk_encoded)?;
    txn.delete(&key)?;
    Ok(())
}
