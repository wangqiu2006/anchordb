//! Versioned row binary encoding supporting zero-rewrite online schema evolution.
//!
//! Row format (Little-Endian):
//! - `RowMagic: u8` (`0xA1`)
//! - `SchemaVersion: u32`
//! - `ColumnCount: u32`
//! - Repeated `ColumnCount` times: `[ColumnId: u32][ValueEncoded...]`
//!
//! Storing immutable `ColumnId` alongside `SchemaVersion` guarantees that:
//! 1. Newly added columns automatically materialize their frozen DDL default (or `NULL`).
//! 2. Dropped columns in historical rows are safely skipped.
//! 3. Drop-then-re-add of a same-named column receives a fresh `ColumnId`, so stale data
//!    never leaks into the newly added column.

use std::collections::BTreeMap;

use crate::types::{CodecError, ColumnId, SchemaVersion, TableSchema, Value, canonical_f64};

const ROW_MAGIC: u8 = 0xA1;

const VAL_NULL: u8 = 0;
const VAL_BOOL: u8 = 1;
const VAL_INT64: u8 = 2;
const VAL_FLOAT64: u8 = 3;
const VAL_TEXT: u8 = 4;
const VAL_BLOB: u8 = 5;

/// Validates `values` against `schema` (count, types, NOT NULL) and encodes a versioned row.
pub fn encode_row(schema: &TableSchema, values: &[Value]) -> Result<Vec<u8>, CodecError> {
    if values.len() != schema.columns.len() {
        return Err(CodecError::ColumnCountMismatch {
            expected: schema.columns.len(),
            actual: values.len(),
        });
    }

    let col_count = u32::try_from(schema.columns.len()).map_err(|_| CodecError::InvalidEncoding)?;
    let mut buf = Vec::with_capacity(16 + schema.columns.len() * 12);
    buf.push(ROW_MAGIC);
    buf.extend_from_slice(&schema.version.to_le_bytes());
    buf.extend_from_slice(&col_count.to_le_bytes());

    for (col, val) in schema.columns.iter().zip(values.iter()) {
        if val.is_null() {
            if !col.nullable {
                return Err(CodecError::NullConstraintViolation {
                    column: col.name.clone(),
                });
            }
        } else if !val.matches_type(col.data_type) {
            return Err(CodecError::TypeMismatch {
                column: col.name.clone(),
                expected: col.data_type,
            });
        }
        buf.extend_from_slice(&col.id.to_le_bytes());
        encode_value(val, &mut buf)?;
    }

    Ok(buf)
}

/// Decodes a persisted row (from any historical `SchemaVersion`) projected onto `current_schema`.
/// Returns `(row_schema_version, values_aligned_to_current_schema)`.
pub fn decode_row(
    current_schema: &TableSchema,
    raw: &[u8],
) -> Result<(SchemaVersion, Vec<Value>), CodecError> {
    if raw.len() < 9 || raw[0] != ROW_MAGIC {
        return Err(CodecError::InvalidEncoding);
    }
    let row_version = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]);
    let col_count = u32::from_le_bytes([raw[5], raw[6], raw[7], raw[8]]) as usize;

    let mut cursor = 9usize;
    let mut stored: BTreeMap<ColumnId, Value> = BTreeMap::new();
    for _ in 0..col_count {
        let id = read_u32(raw, &mut cursor)?;
        let val = decode_value(raw, &mut cursor)?;
        stored.insert(id, val);
    }
    if cursor != raw.len() {
        return Err(CodecError::InvalidEncoding);
    }

    let mut out = Vec::with_capacity(current_schema.columns.len());
    for col in &current_schema.columns {
        if let Some(val) = stored.remove(&col.id) {
            out.push(val);
        } else if let Some(default_val) = &col.default_value {
            out.push(default_val.clone());
        } else if col.nullable {
            out.push(Value::Null);
        } else {
            return Err(CodecError::NullConstraintViolation {
                column: col.name.clone(),
            });
        }
    }

    Ok((row_version, out))
}

/// Encodes a single typed [`Value`] in Little-Endian binary format.
pub fn encode_value(val: &Value, out: &mut Vec<u8>) -> Result<(), CodecError> {
    match val {
        Value::Null => out.push(VAL_NULL),
        Value::Bool(b) => {
            out.push(VAL_BOOL);
            out.push(u8::from(*b));
        }
        Value::Int64(n) => {
            out.push(VAL_INT64);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            out.push(VAL_FLOAT64);
            out.extend_from_slice(&canonical_f64(*f).to_bits().to_le_bytes());
        }
        Value::Text(s) => {
            let len = u32::try_from(s.len()).map_err(|_| CodecError::InvalidEncoding)?;
            out.push(VAL_TEXT);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Blob(bytes) => {
            let len = u32::try_from(bytes.len()).map_err(|_| CodecError::InvalidEncoding)?;
            out.push(VAL_BLOB);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
    }
    Ok(())
}

/// Decodes a single typed [`Value`] starting at `*cursor`, advancing `*cursor`.
pub fn decode_value(raw: &[u8], cursor: &mut usize) -> Result<Value, CodecError> {
    let tag = *raw.get(*cursor).ok_or(CodecError::InvalidEncoding)?;
    *cursor += 1;

    match tag {
        VAL_NULL => Ok(Value::Null),
        VAL_BOOL => {
            let b = *raw.get(*cursor).ok_or(CodecError::InvalidEncoding)?;
            *cursor += 1;
            match b {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(CodecError::InvalidEncoding),
            }
        }
        VAL_INT64 => {
            let bytes = read_slice(raw, cursor, 8)?;
            let n = i64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            Ok(Value::Int64(n))
        }
        VAL_FLOAT64 => {
            let bytes = read_slice(raw, cursor, 8)?;
            let bits = u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            Ok(Value::Float64(f64::from_bits(bits)))
        }
        VAL_TEXT => {
            let len = read_u32(raw, cursor)? as usize;
            let bytes = read_slice(raw, cursor, len)?;
            let s = core::str::from_utf8(bytes)
                .map_err(|_| CodecError::InvalidEncoding)?
                .to_owned();
            Ok(Value::Text(s))
        }
        VAL_BLOB => {
            let len = read_u32(raw, cursor)? as usize;
            let bytes = read_slice(raw, cursor, len)?;
            Ok(Value::Blob(bytes.to_vec()))
        }
        _ => Err(CodecError::InvalidEncoding),
    }
}

fn read_u32(raw: &[u8], cursor: &mut usize) -> Result<u32, CodecError> {
    let bytes = read_slice(raw, cursor, 4)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_slice<'a>(raw: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], CodecError> {
    let end = cursor.checked_add(len).ok_or(CodecError::InvalidEncoding)?;
    let slice = raw.get(*cursor..end).ok_or(CodecError::InvalidEncoding)?;
    *cursor = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::{decode_row, encode_row};
    use crate::types::{ColumnDef, ColumnType, TableSchema, Value};

    #[test]
    fn schema_evolution_add_drop_readd_same_name_never_leaks_stale_column() {
        // Version 1: (id=1, score=2)
        let v1 = TableSchema {
            table_id: 1,
            name: "players".into(),
            version: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    data_type: ColumnType::Int64,
                    nullable: false,
                    default_value: None,
                },
                ColumnDef {
                    id: 2,
                    name: "score".into(),
                    data_type: ColumnType::Int64,
                    nullable: true,
                    default_value: None,
                },
            ],
            primary_key_cols: vec![1],
            indexes: vec![],
            next_column_id: 3,
            next_index_id: 1,
        };

        let raw_v1 = encode_row(&v1, &[Value::Int64(7), Value::Int64(999)]).unwrap();

        // Version 3: dropped `score` (id=2), re-added `score` (id=3, default=0), added `tag` (id=4, default="rookie").
        let v3 = TableSchema {
            table_id: 1,
            name: "players".into(),
            version: 3,
            columns: vec![
                v1.columns[0].clone(),
                ColumnDef {
                    id: 3,
                    name: "score".into(),
                    data_type: ColumnType::Int64,
                    nullable: false,
                    default_value: Some(Value::Int64(0)),
                },
                ColumnDef {
                    id: 4,
                    name: "tag".into(),
                    data_type: ColumnType::Text,
                    nullable: false,
                    default_value: Some(Value::Text("rookie".into())),
                },
            ],
            primary_key_cols: vec![1],
            indexes: vec![],
            next_column_id: 5,
            next_index_id: 1,
        };

        let (row_ver, decoded) = decode_row(&v3, &raw_v1).unwrap();
        assert_eq!(row_ver, 1);
        assert_eq!(
            decoded,
            vec![
                Value::Int64(7),
                Value::Int64(0),
                Value::Text("rookie".into()),
            ]
        );
    }
}
