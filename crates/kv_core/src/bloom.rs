//! Pure-Rust `#![no_std]` + `alloc` Bloom Filter for SSTable point-lookup acceleration.
//!
//! Uses Kirsch-Mitzenmacher double hashing over two independent 64-bit hashes derived from
//! FNV-1a + SplitMix64 mixing. Serialized with Little-Endian layout and a trailing CRC32C:
//!
//! `k(u8) | bitset_len_bytes(u32 LE) | bitset_bytes([u8; N]) | crc32c(u32 LE)`

use alloc::vec;
use alloc::vec::Vec;

use crate::engine::KvError;
use crate::format::{FormatError, crc32c};

/// Default Bloom Filter density: 10 bits per key (~0.82% false positive rate, `k = 7`).
pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;

const MIN_FILTER_BYTES: usize = 8;
const MAX_HASH_PROBES: u8 = 30;
const FILTER_HEADER_LEN: usize = 5; // k(1) + len(4)
const FILTER_TRAILER_LEN: usize = 4; // crc32c(4)

/// Compact immutable Bloom Filter stored inside an SSTable Filter Block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u8>,
    k: u8,
}

impl BloomFilter {
    /// Builds a Bloom Filter over `keys` using `bits_per_key` bits per entry.
    #[must_use]
    pub fn build<'a, I>(keys: I, bits_per_key: usize) -> Self
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: Clone,
    {
        let iter = keys.into_iter();
        let key_count = iter.clone().count();
        let effective_bpk = bits_per_key.max(2);

        // k = floor(bits_per_key * ln(2)) ≈ bits_per_key * 69 / 100
        let k_raw = (effective_bpk.saturating_mul(69) / 100).clamp(1, usize::from(MAX_HASH_PROBES));
        let k = u8::try_from(k_raw).unwrap_or(MAX_HASH_PROBES);

        let total_bits = key_count
            .saturating_mul(effective_bpk)
            .max(MIN_FILTER_BYTES * 8);
        let num_bytes = total_bits.div_ceil(8).max(MIN_FILTER_BYTES);
        let mut bits = vec![0u8; num_bytes];
        let num_bits = (num_bytes as u64).saturating_mul(8);

        for key in iter {
            let (h1, h2) = double_hash(key);
            for i in 0..u64::from(k) {
                let bit_idx = (h1.wrapping_add(i.wrapping_mul(h2)) % num_bits) as usize;
                bits[bit_idx / 8] |= 1u8 << (bit_idx % 8);
            }
        }

        Self { bits, k }
    }

    /// Returns `false` if `key` is guaranteed absent, or `true` if `key` may be present.
    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() || self.k == 0 {
            return true;
        }
        let num_bits = (self.bits.len() as u64).saturating_mul(8);
        let (h1, h2) = double_hash(key);
        for i in 0..u64::from(self.k) {
            let bit_idx = (h1.wrapping_add(i.wrapping_mul(h2)) % num_bits) as usize;
            if (self.bits[bit_idx / 8] & (1u8 << (bit_idx % 8))) == 0 {
                return false;
            }
        }
        true
    }

    /// Number of hash probe functions (`k`).
    #[must_use]
    pub const fn hash_count(&self) -> u8 {
        self.k
    }

    /// Total number of bits in the filter bitset.
    #[must_use]
    pub fn bit_len(&self) -> usize {
        self.bits.len().saturating_mul(8)
    }

    /// Serializes the filter into Little-Endian bytes with a trailing CRC32C checksum.
    pub fn encode(&self) -> Result<Vec<u8>, KvError> {
        let len_u32 = u32::try_from(self.bits.len()).map_err(|_| KvError::InvalidArgument)?;
        let mut out = Vec::with_capacity(FILTER_HEADER_LEN + self.bits.len() + FILTER_TRAILER_LEN);
        out.push(self.k);
        out.extend_from_slice(&len_u32.to_le_bytes());
        out.extend_from_slice(&self.bits);
        let checksum = crc32c(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        Ok(out)
    }

    /// Decodes and CRC32C-verifies a serialized filter block.
    pub fn decode(raw: &[u8]) -> Result<Self, FormatError> {
        if raw.len() < FILTER_HEADER_LEN + FILTER_TRAILER_LEN + 1 {
            return Err(FormatError::ChecksumMismatch);
        }
        let crc_pos = raw.len() - FILTER_TRAILER_LEN;
        let expected_crc = u32::from_le_bytes([
            raw[crc_pos],
            raw[crc_pos + 1],
            raw[crc_pos + 2],
            raw[crc_pos + 3],
        ]);
        if crc32c(&raw[..crc_pos]) != expected_crc {
            return Err(FormatError::ChecksumMismatch);
        }

        let k = raw[0];
        if k == 0 || k > MAX_HASH_PROBES {
            return Err(FormatError::ChecksumMismatch);
        }
        let declared_len = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
        let body = &raw[FILTER_HEADER_LEN..crc_pos];
        if declared_len == 0 || declared_len != body.len() {
            return Err(FormatError::ChecksumMismatch);
        }

        Ok(Self {
            bits: body.to_vec(),
            k,
        })
    }
}

/// Computes two well-mixed, independent 64-bit hashes for Kirsch-Mitzenmacher double hashing.
#[inline]
fn double_hash(key: &[u8]) -> (u64, u64) {
    let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
    let mut h2: u64 = 0x8422_2325_cbf2_9ce4;
    for &b in key {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(0x0000_0100_0000_01b3);

        h2 = h2.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h2 ^= u64::from(b).wrapping_add(0x517c_c1b7_2722_0a95);
    }
    let mixed1 = splitmix64(h1 ^ (key.len() as u64));
    // Ensure h2 is odd so successive probes cover the bitset evenly without degeneracy.
    let mixed2 = splitmix64(h2 ^ (key.len() as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1;
    (mixed1, mixed2)
}

#[inline]
const fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use alloc::format;
    use alloc::vec::Vec;

    use super::{BloomFilter, DEFAULT_BLOOM_BITS_PER_KEY};
    use crate::format::FormatError;

    #[test]
    fn bloom_filter_has_zero_false_negatives_and_low_false_positive_rate() {
        let present: Vec<Vec<u8>> = (0..2_000u32)
            .map(|i| format!("user:{i:06}").into_bytes())
            .collect();
        let filter = BloomFilter::build(
            present.iter().map(Vec::as_slice),
            DEFAULT_BLOOM_BITS_PER_KEY,
        );
        assert_eq!(filter.hash_count(), 6);

        // Zero false negatives invariant.
        for key in &present {
            assert!(filter.may_contain(key));
        }

        // Empirical false positive rate on 5,000 disjoint keys must stay well below 2.5%
        // (theoretical optimum at 10 bits/key is ~0.82%).
        let mut false_positives = 0usize;
        let absent_total = 5_000u32;
        for i in 100_000..(100_000 + absent_total) {
            let key = format!("user:{i:06}").into_bytes();
            if filter.may_contain(&key) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives * 1000 < (absent_total as usize) * 25,
            "unexpectedly high false positive rate: {false_positives}/{absent_total}"
        );
    }

    #[test]
    fn bloom_filter_encode_decode_roundtrip_and_crc_validation() {
        let keys = [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()];
        let filter = BloomFilter::build(keys, DEFAULT_BLOOM_BITS_PER_KEY);
        let encoded = filter.encode().unwrap();
        let decoded = BloomFilter::decode(&encoded).unwrap();
        assert_eq!(filter, decoded);
        for k in keys {
            assert!(decoded.may_contain(k));
        }

        // Flip a bit inside the bitset and verify CRC32C catches corruption.
        let mut corrupted = encoded;
        corrupted[6] ^= 0x04;
        assert_eq!(
            BloomFilter::decode(&corrupted),
            Err(FormatError::ChecksumMismatch)
        );
    }
}
