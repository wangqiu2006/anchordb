//! Order-preserving (Memcomparable) binary key encoding for tables, primary keys,
//! and unique/non-unique composite secondary indexes.

use core::ops::Bound;

use crate::types::{CodecError, IndexId, SortOrder, TableId, Value, canonical_f64};

/// Key-space prefix reserved for internal System Catalog tables.
pub const SYS_CATALOG_PREFIX: u8 = 0x00;

/// Key-space prefix reserved for user tables and indexes.
pub const USER_TABLE_PREFIX: u8 = 0x01;

/// Sub-prefix tags within a table key namespace.
pub const ROW_TAG: u8 = b'r';
pub const INDEX_TAG: u8 = b'i';
pub const UNIQUE_INDEX_TAG: u8 = b'u';

const NULL_MARKER: u8 = 0x00;
const NON_NULL_MARKER: u8 = 0x01;
const UNIQUE_HAS_NULL_FLAG: u8 = 0x00;
const UNIQUE_NO_NULL_FLAG: u8 = 0x01;
const CHUNK_SIZE: usize = 8;
const CHUNK_MORE_MARKER: u8 = 0xFF;

/// Encodes a single typed [`Value`] in Memcomparable order into `out`.
///
/// Properties:
/// - `NULL` sorts strictly before all non-null values in `Asc` order.
/// - Signed `i64` flips the sign bit so negative numbers sort before positive numbers.
/// - `f64` canonicalizes `-0.0` to `+0.0` and IEEE-754 bit-flips so total order matches byte order.
/// - `Text` / `Blob` use 8-byte chunked framing with length/continuation markers, eliminating
///   prefix ambiguity in composite keys (e.g., `("ab","c")` vs `("a","bc")`).
/// - `SortOrder::Desc` bitwise-inverts every emitted byte for that column.
pub fn encode_value_comparable(val: &Value, dir: SortOrder, out: &mut Vec<u8>) {
    let start_pos = out.len();
    match val {
        Value::Null => {
            out.push(NULL_MARKER);
        }
        Value::Bool(b) => {
            out.push(NON_NULL_MARKER);
            out.push(u8::from(*b));
        }
        Value::Int64(n) => {
            out.push(NON_NULL_MARKER);
            let ordered = (*n as u64) ^ (1u64 << 63);
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Float64(f) => {
            out.push(NON_NULL_MARKER);
            let bits = canonical_f64(*f).to_bits();
            let ordered = if (bits >> 63) == 0 {
                bits ^ (1u64 << 63)
            } else {
                !bits
            };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Text(s) => {
            out.push(NON_NULL_MARKER);
            encode_memcomparable_bytes(s.as_bytes(), out);
        }
        Value::Blob(bytes) => {
            out.push(NON_NULL_MARKER);
            encode_memcomparable_bytes(bytes, out);
        }
    }

    if dir == SortOrder::Desc {
        for b in &mut out[start_pos..] {
            *b = !*b;
        }
    }
}

/// Encodes a slice of `(Value, SortOrder)` pairs into a composite Memcomparable key segment.
#[must_use]
pub fn encode_tuple_comparable(values: &[Value], dirs: &[SortOrder]) -> Vec<u8> {
    let mut out = Vec::new();
    for (idx, val) in values.iter().enumerate() {
        let dir = dirs.get(idx).copied().unwrap_or(SortOrder::Asc);
        encode_value_comparable(val, dir, &mut out);
    }
    out
}

/// Decodes a single Memcomparable byte-chunked field (`Text` / `Blob`) starting at `input`.
/// Returns `(decoded_bytes, bytes_consumed)`.
pub fn decode_memcomparable_bytes(input: &[u8]) -> Result<(Vec<u8>, usize), CodecError> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    loop {
        let end = cursor
            .checked_add(CHUNK_SIZE + 1)
            .ok_or(CodecError::InvalidEncoding)?;
        let chunk = input.get(cursor..end).ok_or(CodecError::InvalidEncoding)?;
        let marker = chunk[CHUNK_SIZE];
        cursor = end;

        if marker == CHUNK_MORE_MARKER {
            out.extend_from_slice(&chunk[..CHUNK_SIZE]);
        } else if usize::from(marker) < CHUNK_SIZE {
            let valid_len = usize::from(marker);
            // Padding bytes must be zero.
            if chunk[valid_len..CHUNK_SIZE].iter().any(|&b| b != 0) {
                return Err(CodecError::InvalidEncoding);
            }
            out.extend_from_slice(&chunk[..valid_len]);
            return Ok((out, cursor));
        } else {
            return Err(CodecError::InvalidEncoding);
        }
    }
}

fn encode_memcomparable_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    let mut offset = 0usize;
    while bytes.len() - offset >= CHUNK_SIZE {
        out.extend_from_slice(&bytes[offset..offset + CHUNK_SIZE]);
        out.push(CHUNK_MORE_MARKER);
        offset += CHUNK_SIZE;
    }
    let rem = &bytes[offset..];
    out.extend_from_slice(rem);
    let pad = CHUNK_SIZE - rem.len();
    for _ in 0..pad {
        out.push(0x00);
    }
    out.push(rem.len() as u8);
}

/// Builds the common prefix for all primary rows of `table_id`: `[0x01][table_id BE]['r']`.
#[must_use]
pub fn table_row_prefix(table_id: TableId) -> Vec<u8> {
    let mut key = Vec::with_capacity(6);
    key.push(USER_TABLE_PREFIX);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(ROW_TAG);
    key
}

/// Builds a primary row KV key: `[0x01][table_id BE]['r'][pk_encoded...]`.
#[must_use]
pub fn encode_row_key(table_id: TableId, pk_encoded: &[u8]) -> Vec<u8> {
    let mut key = table_row_prefix(table_id);
    key.extend_from_slice(pk_encoded);
    key
}

/// Builds the prefix for a non-unique secondary index: `[0x01][table_id BE]['i'][index_id BE]`.
#[must_use]
pub fn non_unique_index_prefix(table_id: TableId, index_id: IndexId) -> Vec<u8> {
    let mut key = Vec::with_capacity(10);
    key.push(USER_TABLE_PREFIX);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(INDEX_TAG);
    key.extend_from_slice(&index_id.to_be_bytes());
    key
}

/// Builds a non-unique secondary index KV key:
/// `[0x01][table_id BE]['i'][index_id BE][indexed_cols_encoded...][pk_len:u32 BE? No -> pk stored in value!]`
///
/// Storing `pk_encoded` both at the key suffix (for uniqueness & tie-breaking) and in the KV value
/// allows O(1) primary key extraction during index scan without re-decoding indexed columns.
#[must_use]
pub fn encode_non_unique_index_key(
    table_id: TableId,
    index_id: IndexId,
    indexed_encoded: &[u8],
    pk_encoded: &[u8],
) -> Vec<u8> {
    let mut key = non_unique_index_prefix(table_id, index_id);
    key.extend_from_slice(indexed_encoded);
    key.extend_from_slice(pk_encoded);
    key
}

/// Builds the prefix for a unique secondary index: `[0x01][table_id BE]['u'][index_id BE]`.
#[must_use]
pub fn unique_index_prefix(table_id: TableId, index_id: IndexId) -> Vec<u8> {
    let mut key = Vec::with_capacity(10);
    key.push(USER_TABLE_PREFIX);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(UNIQUE_INDEX_TAG);
    key.extend_from_slice(&index_id.to_be_bytes());
    key
}

/// Builds a unique secondary index KV key.
///
/// Per SQL standard:
/// - If NO indexed column is `NULL`: key omits `pk_encoded` (using flag `0x01`), so duplicate
///   indexed values collide directly on the KV key.
/// - If ANY indexed column is `NULL`: key appends `pk_encoded` (using flag `0x00`), allowing
///   multiple rows with `NULL` to coexist while remaining index-scannable.
#[must_use]
pub fn encode_unique_index_key(
    table_id: TableId,
    index_id: IndexId,
    indexed_encoded: &[u8],
    pk_encoded: &[u8],
    has_null: bool,
) -> Vec<u8> {
    let mut key = unique_index_prefix(table_id, index_id);
    if has_null {
        key.push(UNIQUE_HAS_NULL_FLAG);
        key.extend_from_slice(indexed_encoded);
        key.extend_from_slice(pk_encoded);
    } else {
        key.push(UNIQUE_NO_NULL_FLAG);
        key.extend_from_slice(indexed_encoded);
    }
    key
}

/// Builds a unique-index scan prefix targeting non-null entries (`has_null = false`).
#[must_use]
pub fn unique_index_non_null_prefix(table_id: TableId, index_id: IndexId) -> Vec<u8> {
    let mut key = unique_index_prefix(table_id, index_id);
    key.push(UNIQUE_NO_NULL_FLAG);
    key
}

/// Computes the tightest exclusive upper bound for all keys starting with `prefix`.
/// Returns `Bound::Unbounded` when `prefix` consists solely of `0xFF` bytes.
#[must_use]
pub fn prefix_upper_bound(prefix: &[u8]) -> Bound<Vec<u8>> {
    for i in (0..prefix.len()).rev() {
        if prefix[i] < 0xFF {
            let mut upper = prefix[..=i].to_vec();
            upper[i] += 1;
            return Bound::Excluded(upper);
        }
    }
    Bound::Unbounded
}

#[cfg(test)]
mod tests {
    use core::ops::Bound;

    use super::{
        INDEX_TAG, ROW_TAG, SYS_CATALOG_PREFIX, UNIQUE_INDEX_TAG, USER_TABLE_PREFIX,
        decode_memcomparable_bytes, encode_tuple_comparable, encode_value_comparable,
        prefix_upper_bound,
    };
    use crate::types::{SortOrder, Value};

    #[test]
    fn keyspace_prefixes_are_distinct() {
        assert_ne!(SYS_CATALOG_PREFIX, USER_TABLE_PREFIX);
        assert_ne!(ROW_TAG, INDEX_TAG);
        assert_ne!(ROW_TAG, UNIQUE_INDEX_TAG);
        assert_ne!(INDEX_TAG, UNIQUE_INDEX_TAG);
    }

    #[test]
    fn int64_and_float64_preserve_mathematical_order_asc_and_desc() {
        let ints = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let enc_asc: Vec<Vec<u8>> = ints
            .iter()
            .map(|&n| encode_tuple_comparable(&[Value::Int64(n)], &[SortOrder::Asc]))
            .collect();
        for w in enc_asc.windows(2) {
            assert!(w[0] < w[1]);
        }

        let enc_desc: Vec<Vec<u8>> = ints
            .iter()
            .map(|&n| encode_tuple_comparable(&[Value::Int64(n)], &[SortOrder::Desc]))
            .collect();
        for w in enc_desc.windows(2) {
            assert!(w[0] > w[1]);
        }

        // Float64: -inf < -1.5 < -0.0 == +0.0 < 1.5 < +inf < NaN
        let floats = [
            f64::NEG_INFINITY,
            -100.25,
            -0.0001,
            0.0,
            0.0001,
            100.25,
            f64::INFINITY,
            f64::NAN,
        ];
        let f_asc: Vec<Vec<u8>> = floats
            .iter()
            .map(|&f| encode_tuple_comparable(&[Value::Float64(f)], &[SortOrder::Asc]))
            .collect();
        for w in f_asc.windows(2) {
            assert!(w[0] < w[1]);
        }
        assert_eq!(
            encode_tuple_comparable(&[Value::Float64(-0.0)], &[SortOrder::Asc]),
            encode_tuple_comparable(&[Value::Float64(0.0)], &[SortOrder::Asc])
        );
    }

    #[test]
    fn composite_variable_length_strings_have_no_concatenation_ambiguity() {
        let k1 = encode_tuple_comparable(
            &[Value::Text("ab".into()), Value::Text("c".into())],
            &[SortOrder::Asc, SortOrder::Asc],
        );
        let k2 = encode_tuple_comparable(
            &[Value::Text("a".into()), Value::Text("bc".into())],
            &[SortOrder::Asc, SortOrder::Asc],
        );
        assert_ne!(k1, k2);
        assert!(k2 < k1, "('a','bc') must sort strictly before ('ab','c')");

        // Test exact 8-byte boundary and round-trip decode.
        for sample in [
            b"".as_slice(),
            b"a",
            b"1234567",
            b"12345678",
            b"123456789",
            b"a\x00b\xffc\x00\x00\x00\x01",
        ] {
            let mut enc = Vec::new();
            encode_value_comparable(&Value::Blob(sample.to_vec()), SortOrder::Asc, &mut enc);
            assert_eq!(enc[0], 0x01);
            let (dec, consumed) = decode_memcomparable_bytes(&enc[1..]).unwrap();
            assert_eq!(dec, sample);
            assert_eq!(consumed, enc.len() - 1);
        }
    }

    #[test]
    fn prefix_upper_bound_handles_all_ff_edge_case() {
        assert_eq!(
            prefix_upper_bound(&[0x01, 0x02]),
            Bound::Excluded(vec![0x01, 0x03])
        );
        assert_eq!(
            prefix_upper_bound(&[0x01, 0xFF]),
            Bound::Excluded(vec![0x02])
        );
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), Bound::Unbounded);
    }
}
