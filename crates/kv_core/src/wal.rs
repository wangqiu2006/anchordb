//! Commit-time `WriteBatch` WAL writer and crash-safe recovery reader.
//!
//! Recovery invariant (RFC 0001):
//! - An incomplete or CRC-invalid trailing record at the end of the WAL is treated as
//!   a power-loss torn write and safely truncated.
//! - A CRC-invalid record followed by subsequent valid WAL frames indicates mid-file
//!   bit rot and MUST fail with `FormatError::ChecksumMismatch` instead of silently
//!   dropping committed data.

use alloc::vec::Vec;

use hal_vfs::StorageFile;

use crate::engine::KvError;
use crate::format::{
    CURRENT_FORMAT_VERSION, FormatError, SUPERBLOCK_SIZE, SuperblockHeader, crc32c,
};

const WAL_RECORD_MAGIC: [u8; 4] = *b"WAL1";
const WAL_HEADER_LEN: usize = 28; // Magic(4) + CRC(4) + LSN(8) + TxnId(8) + PayloadLen(4)
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

/// Single mutation inside a [`WriteBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put(Vec<u8>),
    Delete,
}

/// Atomic group of mutations written together on transaction commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBatch {
    pub lsn: u64,
    pub txn_id: u64,
    pub entries: Vec<(Vec<u8>, WriteOp)>,
}

impl WriteBatch {
    /// Encodes this batch into a single framed WAL record (Little-Endian + CRC32C).
    pub fn encode(&self) -> Result<Vec<u8>, KvError> {
        let entry_count =
            u32::try_from(self.entries.len()).map_err(|_| KvError::InvalidArgument)?;

        let mut payload = Vec::new();
        payload.extend_from_slice(&entry_count.to_le_bytes());
        for (key, op) in &self.entries {
            let key_len = u32::try_from(key.len()).map_err(|_| KvError::InvalidArgument)?;
            match op {
                WriteOp::Put(val) => {
                    let val_len = u32::try_from(val.len()).map_err(|_| KvError::InvalidArgument)?;
                    payload.push(OP_PUT);
                    payload.extend_from_slice(&key_len.to_le_bytes());
                    payload.extend_from_slice(&val_len.to_le_bytes());
                    payload.extend_from_slice(key);
                    payload.extend_from_slice(val);
                }
                WriteOp::Delete => {
                    payload.push(OP_DELETE);
                    payload.extend_from_slice(&key_len.to_le_bytes());
                    payload.extend_from_slice(&0u32.to_le_bytes());
                    payload.extend_from_slice(key);
                }
            }
        }

        let payload_len = u32::try_from(payload.len()).map_err(|_| KvError::InvalidArgument)?;
        let mut frame = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
        frame.extend_from_slice(&WAL_RECORD_MAGIC);
        frame.extend_from_slice(&0u32.to_le_bytes()); // CRC placeholder
        frame.extend_from_slice(&self.lsn.to_le_bytes());
        frame.extend_from_slice(&self.txn_id.to_le_bytes());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&payload);

        // Compute CRC32C over bytes [8..] (LSN + TxnId + PayloadLen + Payload).
        let checksum = crc32c(&frame[8..]);
        frame[4..8].copy_from_slice(&checksum.to_le_bytes());
        Ok(frame)
    }
}

/// Append-only WAL file writer.
#[derive(Debug)]
pub struct WalWriter<F: StorageFile> {
    file: F,
    offset: u64,
}

impl<F: StorageFile> WalWriter<F> {
    /// Initializes a brand new WAL file with a 64-byte SuperblockHeader and syncs it.
    pub fn create(mut file: F, page_size: u16) -> Result<Self, KvError> {
        let header = SuperblockHeader::new(page_size).encode();
        file.write_all_at(0, &header)?;
        file.sync_data()?;
        Ok(Self {
            file,
            offset: SUPERBLOCK_SIZE as u64,
        })
    }

    /// Reopens an existing WAL file positioned at `valid_offset`.
    pub const fn resume(file: F, valid_offset: u64) -> Self {
        Self {
            file,
            offset: valid_offset,
        }
    }

    /// Appends a complete transaction `WriteBatch` and optionally flushes with `sync_data()`.
    pub fn append(&mut self, batch: &WriteBatch, sync: bool) -> Result<(), KvError> {
        let encoded = batch.encode()?;
        self.file.write_all_at(self.offset, &encoded)?;
        if sync {
            self.file.sync_data()?;
        }
        self.offset = self.offset.saturating_add(encoded.len() as u64);
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), KvError> {
        self.file.sync_data()?;
        Ok(())
    }

    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }
}

/// Result of recovering a WAL file.
#[derive(Debug)]
pub struct WalRecovery {
    pub batches: Vec<WriteBatch>,
    pub valid_offset: u64,
    pub truncated_tail: bool,
}

/// Scans a WAL file from its superblock, verifies every record, truncates any trailing
/// torn write, and rejects mid-file corruption.
pub fn recover_wal<F: StorageFile>(file: &mut F) -> Result<WalRecovery, KvError> {
    let file_len = file.len()?;
    if file_len == 0 {
        return Ok(WalRecovery {
            batches: Vec::new(),
            valid_offset: 0,
            truncated_tail: false,
        });
    }
    if file_len < SUPERBLOCK_SIZE as u64 {
        // Incomplete superblock from a crash during WAL creation: treat as torn empty WAL.
        file.truncate(0)?;
        file.sync_data()?;
        return Ok(WalRecovery {
            batches: Vec::new(),
            valid_offset: 0,
            truncated_tail: true,
        });
    }

    let total_len = usize::try_from(file_len).map_err(|_| KvError::InvalidArgument)?;
    let mut raw = alloc::vec![0u8; total_len];
    file.read_exact_at(0, &mut raw)?;

    let mut sb_bytes = [0u8; SUPERBLOCK_SIZE];
    sb_bytes.copy_from_slice(&raw[..SUPERBLOCK_SIZE]);
    let header = SuperblockHeader::decode(&sb_bytes)?;
    header.check_compatibility(CURRENT_FORMAT_VERSION, 0)?;

    let mut batches = Vec::new();
    let mut pos = SUPERBLOCK_SIZE;
    let mut truncated_tail = false;

    while pos < raw.len() {
        match try_decode_record(&raw[pos..]) {
            Ok((batch, consumed)) => {
                batches.push(batch);
                pos += consumed;
            }
            Err(RecordDecodeFailure::IncompleteTail) => {
                truncated_tail = true;
                break;
            }
            Err(RecordDecodeFailure::Corrupted { claimed_end }) => {
                // Distinguish mid-file bit rot from a torn tail write:
                // If another valid framed WAL record starts at or after the claimed boundary,
                // this record sits in the middle of the WAL and MUST NOT be silently truncated.
                let next_search_start = pos.saturating_add(claimed_end).min(raw.len());
                if contains_valid_later_record(&raw, pos + 1, next_search_start) {
                    return Err(KvError::Format(FormatError::ChecksumMismatch));
                }
                truncated_tail = true;
                break;
            }
        }
    }

    let valid_offset = pos as u64;
    if truncated_tail && valid_offset < file_len {
        file.truncate(valid_offset)?;
        file.sync_data()?;
    }

    Ok(WalRecovery {
        batches,
        valid_offset,
        truncated_tail,
    })
}

enum RecordDecodeFailure {
    IncompleteTail,
    Corrupted { claimed_end: usize },
}

fn try_decode_record(slice: &[u8]) -> Result<(WriteBatch, usize), RecordDecodeFailure> {
    if slice.len() < WAL_HEADER_LEN {
        return Err(RecordDecodeFailure::IncompleteTail);
    }
    if slice[0..4] != WAL_RECORD_MAGIC {
        return Err(RecordDecodeFailure::Corrupted {
            claimed_end: WAL_HEADER_LEN,
        });
    }

    let expected_crc = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]);
    let lsn = u64::from_le_bytes([
        slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
    ]);
    let txn_id = u64::from_le_bytes([
        slice[16], slice[17], slice[18], slice[19], slice[20], slice[21], slice[22], slice[23],
    ]);
    let payload_len = u32::from_le_bytes([slice[24], slice[25], slice[26], slice[27]]) as usize;

    let total_len = WAL_HEADER_LEN.saturating_add(payload_len);
    if total_len > slice.len() {
        return Err(RecordDecodeFailure::IncompleteTail);
    }

    let actual_crc = crc32c(&slice[8..total_len]);
    if actual_crc != expected_crc {
        return Err(RecordDecodeFailure::Corrupted {
            claimed_end: total_len,
        });
    }

    let payload = &slice[WAL_HEADER_LEN..total_len];
    let entries = decode_payload(payload).ok_or(RecordDecodeFailure::Corrupted {
        claimed_end: total_len,
    })?;

    Ok((
        WriteBatch {
            lsn,
            txn_id,
            entries,
        },
        total_len,
    ))
}

fn decode_payload(payload: &[u8]) -> Option<Vec<(Vec<u8>, WriteOp)>> {
    if payload.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let mut cursor = 4usize;
    let mut entries = Vec::with_capacity(count.min(1024));

    for _ in 0..count {
        if cursor.checked_add(9)? > payload.len() {
            return None;
        }
        let op_type = payload[cursor];
        let key_len = u32::from_le_bytes([
            payload[cursor + 1],
            payload[cursor + 2],
            payload[cursor + 3],
            payload[cursor + 4],
        ]) as usize;
        let val_len = u32::from_le_bytes([
            payload[cursor + 5],
            payload[cursor + 6],
            payload[cursor + 7],
            payload[cursor + 8],
        ]) as usize;
        cursor += 9;

        let key_end = cursor.checked_add(key_len)?;
        let val_end = key_end.checked_add(val_len)?;
        if val_end > payload.len() {
            return None;
        }
        let key = payload[cursor..key_end].to_vec();
        let op = match op_type {
            OP_PUT => WriteOp::Put(payload[key_end..val_end].to_vec()),
            OP_DELETE if val_len == 0 => WriteOp::Delete,
            _ => return None,
        };
        entries.push((key, op));
        cursor = val_end;
    }

    if cursor != payload.len() {
        return None;
    }
    Some(entries)
}

fn contains_valid_later_record(raw: &[u8], scan_from: usize, likely_next: usize) -> bool {
    // Fast path: check the exact claimed boundary first.
    if likely_next < raw.len() && try_decode_record(&raw[likely_next..]).is_ok() {
        return true;
    }
    // Fallback scan for any valid framed WAL record following the corrupted position.
    if scan_from + WAL_HEADER_LEN > raw.len() {
        return false;
    }
    let limit = raw.len() - WAL_HEADER_LEN;
    let mut idx = scan_from;
    while idx <= limit {
        if raw[idx..idx + 4] == WAL_RECORD_MAGIC && try_decode_record(&raw[idx..]).is_ok() {
            return true;
        }
        idx += 1;
    }
    false
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use alloc::vec;

    use hal_vfs::fault_sim::FaultSimFs;
    use hal_vfs::{FileSystem, StorageFile};

    use super::{WalWriter, WriteBatch, WriteOp, recover_wal};
    use crate::engine::KvError;
    use crate::format::FormatError;

    #[test]
    fn wal_recovers_committed_batches_and_truncates_torn_tail() {
        let fs = FaultSimFs::new(1);
        let file = fs.open_or_create("wal-000001.log").unwrap();
        let mut writer = WalWriter::create(file, 4096).unwrap();

        let b1 = WriteBatch {
            lsn: 1,
            txn_id: 1,
            entries: vec![(b"k1".to_vec(), WriteOp::Put(b"v1".to_vec()))],
        };
        let b2 = WriteBatch {
            lsn: 2,
            txn_id: 2,
            entries: vec![(b"k1".to_vec(), WriteOp::Delete)],
        };
        writer.append(&b1, true).unwrap();
        let good_len = writer.offset();
        writer.append(&b2, true).unwrap();
        drop(writer);
        fs.sync_dir().unwrap();

        // Corrupt the tail of b2 to mimic a torn write on power loss.
        let mut f = fs.open_or_create("wal-000001.log").unwrap();
        let full_len = f.len().unwrap();
        f.truncate(full_len - 3).unwrap();
        f.sync_data().unwrap();

        let rec = recover_wal(&mut f).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(rec.valid_offset, good_len);
        assert_eq!(rec.batches, vec![b1]);
        assert_eq!(f.len().unwrap(), good_len);
    }

    #[test]
    fn wal_rejects_mid_file_bit_rot_without_silent_truncation() {
        let fs = FaultSimFs::new(2);
        let file = fs.open_or_create("wal-000001.log").unwrap();
        let mut writer = WalWriter::create(file, 4096).unwrap();

        let b1 = WriteBatch {
            lsn: 1,
            txn_id: 1,
            entries: vec![(b"alpha".to_vec(), WriteOp::Put(b"11111".to_vec()))],
        };
        let b2 = WriteBatch {
            lsn: 2,
            txn_id: 2,
            entries: vec![(b"beta".to_vec(), WriteOp::Put(b"22222".to_vec()))],
        };
        writer.append(&b1, true).unwrap();
        writer.append(&b2, true).unwrap();
        drop(writer);
        fs.sync_dir().unwrap();

        // Flip 1 bit inside the first record's payload (mid-file bit rot).
        let mut f = fs.open_or_create("wal-000001.log").unwrap();
        let mut byte = [0u8; 1];
        f.read_exact_at(64 + 35, &mut byte).unwrap();
        byte[0] ^= 0x04;
        f.write_all_at(64 + 35, &byte).unwrap();
        f.sync_data().unwrap();

        let err = recover_wal(&mut f).unwrap_err();
        assert_eq!(err, KvError::Format(FormatError::ChecksumMismatch));
    }
}
