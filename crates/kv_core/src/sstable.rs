//! Immutable sorted SSTable builder and binary-search reader with Bloom Filter acceleration.
//!
//! File layout (`SST2`, Little-Endian):
//! - `0..64`:  `SuperblockHeader` (64B, CRC32C)
//! - Data Blocks `0..N`: each block contains sorted entries followed by 4B `CRC32C`
//! - Filter Block (optional): serialized [`BloomFilter`] followed by 4B `CRC32C`
//! - Index Block: `(last_key, block_offset, block_size)` entries followed by 4B `CRC32C`
//! - Footer (`SST2`, 44B fixed):
//!   `index_offset(u64) | index_size(u32) | filter_offset(u64) | filter_size(u32) | entry_count(u32) | max_seq(u64) | magic("SST2") | crc32c(u32)`
//!
//! Legacy `SST1` files (32B Footer without Filter Block) remain readable for backward compatibility.

use alloc::vec::Vec;
use core::ops::Bound;

use hal_vfs::StorageFile;

use crate::bloom::{BloomFilter, DEFAULT_BLOOM_BITS_PER_KEY};
use crate::engine::KvError;
use crate::format::{
    CURRENT_FORMAT_VERSION, FormatError, SUPERBLOCK_SIZE, SuperblockHeader, crc32c,
};
use crate::manifest::TableMeta;
use crate::wal::WriteOp;

const SST1_FOOTER_MAGIC: [u8; 4] = *b"SST1";
const SST1_FOOTER_SIZE: usize = 32;
const SST2_FOOTER_MAGIC: [u8; 4] = *b"SST2";
const SST2_FOOTER_SIZE: usize = 44;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

/// Single sorted entry stored inside an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub seq: u64,
    pub op: WriteOp,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    last_key: Vec<u8>,
    block_offset: u64,
    block_size: u32,
}

/// Builds a complete immutable SSTable from strictly ascending unique-key entries.
pub struct SstBuilder;

impl SstBuilder {
    /// Builds an `SST2` table with the default Bloom Filter (`10` bits/key).
    pub fn build<F: StorageFile>(
        file: F,
        file_id: u64,
        level: u8,
        entries: &[SstEntry],
        target_block_size: usize,
        page_size: u16,
    ) -> Result<TableMeta, KvError> {
        Self::build_with_bloom_bits(
            file,
            file_id,
            level,
            entries,
            target_block_size,
            page_size,
            DEFAULT_BLOOM_BITS_PER_KEY,
        )
    }

    /// Builds an `SST2` table with a configurable Bloom Filter density (`0` omits the filter block).
    pub fn build_with_bloom_bits<F: StorageFile>(
        mut file: F,
        file_id: u64,
        level: u8,
        entries: &[SstEntry],
        target_block_size: usize,
        page_size: u16,
        bloom_bits_per_key: usize,
    ) -> Result<TableMeta, KvError> {
        if entries.is_empty() {
            return Err(KvError::InvalidArgument);
        }
        let target_block_size = target_block_size.max(256);

        let mut raw = Vec::new();
        raw.extend_from_slice(&SuperblockHeader::new(page_size).encode());

        let mut index_entries: Vec<IndexEntry> = Vec::new();
        let mut block_buf = Vec::new();
        let mut block_last_key: Option<Vec<u8>> = None;
        let mut max_seq = 0u64;

        let min_key = entries[0].key.clone();
        let max_key = entries[entries.len() - 1].key.clone();

        for entry in entries {
            max_seq = max_seq.max(entry.seq);
            encode_entry(&mut block_buf, entry)?;
            block_last_key = Some(entry.key.clone());

            if block_buf.len() >= target_block_size {
                flush_data_block(
                    &mut raw,
                    &mut block_buf,
                    &mut block_last_key,
                    &mut index_entries,
                )?;
            }
        }
        if !block_buf.is_empty() {
            flush_data_block(
                &mut raw,
                &mut block_buf,
                &mut block_last_key,
                &mut index_entries,
            )?;
        }

        // Encode optional Bloom Filter Block + CRC32C.
        let (filter_offset, filter_size) = if bloom_bits_per_key > 0 {
            let offset = raw.len() as u64;
            let filter =
                BloomFilter::build(entries.iter().map(|e| e.key.as_slice()), bloom_bits_per_key);
            let encoded = filter.encode()?;
            let size = u32::try_from(encoded.len()).map_err(|_| KvError::InvalidArgument)?;
            raw.extend_from_slice(&encoded);
            (offset, size)
        } else {
            (0u64, 0u32)
        };

        // Encode Index Block + CRC32C.
        let index_offset = raw.len() as u64;
        let mut index_buf = Vec::new();
        let index_count =
            u32::try_from(index_entries.len()).map_err(|_| KvError::InvalidArgument)?;
        index_buf.extend_from_slice(&index_count.to_le_bytes());
        for idx in &index_entries {
            let klen = u32::try_from(idx.last_key.len()).map_err(|_| KvError::InvalidArgument)?;
            index_buf.extend_from_slice(&klen.to_le_bytes());
            index_buf.extend_from_slice(&idx.last_key);
            index_buf.extend_from_slice(&idx.block_offset.to_le_bytes());
            index_buf.extend_from_slice(&idx.block_size.to_le_bytes());
        }
        let index_crc = crc32c(&index_buf);
        index_buf.extend_from_slice(&index_crc.to_le_bytes());
        let index_size = u32::try_from(index_buf.len()).map_err(|_| KvError::InvalidArgument)?;
        raw.extend_from_slice(&index_buf);

        // Encode fixed 44-byte SST2 Footer.
        let entry_count = u32::try_from(entries.len()).map_err(|_| KvError::InvalidArgument)?;
        let mut footer = [0u8; SST2_FOOTER_SIZE];
        footer[0..8].copy_from_slice(&index_offset.to_le_bytes());
        footer[8..12].copy_from_slice(&index_size.to_le_bytes());
        footer[12..20].copy_from_slice(&filter_offset.to_le_bytes());
        footer[20..24].copy_from_slice(&filter_size.to_le_bytes());
        footer[24..28].copy_from_slice(&entry_count.to_le_bytes());
        footer[28..36].copy_from_slice(&max_seq.to_le_bytes());
        footer[36..40].copy_from_slice(&SST2_FOOTER_MAGIC);
        let footer_crc = crc32c(&footer[0..40]);
        footer[40..44].copy_from_slice(&footer_crc.to_le_bytes());
        raw.extend_from_slice(&footer);

        file.truncate(0)?;
        file.write_all_at(0, &raw)?;
        file.sync_data()?;

        Ok(TableMeta {
            file_id,
            level,
            max_seq,
            file_size: raw.len() as u64,
            min_key,
            max_key,
        })
    }
}

/// Verified reader for an immutable SSTable file (`SST1` or `SST2`).
#[derive(Debug)]
pub struct SstReader<F: StorageFile> {
    file: F,
    meta: TableMeta,
    index: Vec<IndexEntry>,
    filter: Option<BloomFilter>,
}

impl<F: StorageFile> SstReader<F> {
    pub fn open(file: F, meta: TableMeta) -> Result<Self, KvError> {
        let file_len = file.len()?;
        if file_len < (SUPERBLOCK_SIZE + SST1_FOOTER_SIZE) as u64 {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }

        // Verify SuperblockHeader.
        let mut sb_raw = [0u8; SUPERBLOCK_SIZE];
        file.read_exact_at(0, &mut sb_raw)?;
        let header = SuperblockHeader::decode(&sb_raw)?;
        header.check_compatibility(CURRENT_FORMAT_VERSION, 0)?;

        // Inspect trailing magic (last 8 bytes are always `[magic: 4B][crc32c: 4B]`).
        let mut tail = [0u8; 8];
        file.read_exact_at(file_len - 8, &mut tail)?;
        let magic = [tail[0], tail[1], tail[2], tail[3]];

        let (index_offset, index_size, filter, footer_size) = if magic == SST2_FOOTER_MAGIC {
            if file_len < (SUPERBLOCK_SIZE + SST2_FOOTER_SIZE) as u64 {
                return Err(KvError::Format(FormatError::ChecksumMismatch));
            }
            let mut footer = [0u8; SST2_FOOTER_SIZE];
            file.read_exact_at(file_len - SST2_FOOTER_SIZE as u64, &mut footer)?;
            let expected_footer_crc =
                u32::from_le_bytes([footer[40], footer[41], footer[42], footer[43]]);
            if crc32c(&footer[0..40]) != expected_footer_crc {
                return Err(KvError::Format(FormatError::ChecksumMismatch));
            }
            let index_offset = u64::from_le_bytes([
                footer[0], footer[1], footer[2], footer[3], footer[4], footer[5], footer[6],
                footer[7],
            ]);
            let index_size =
                u32::from_le_bytes([footer[8], footer[9], footer[10], footer[11]]) as usize;
            let filter_offset = u64::from_le_bytes([
                footer[12], footer[13], footer[14], footer[15], footer[16], footer[17], footer[18],
                footer[19],
            ]);
            let filter_size =
                u32::from_le_bytes([footer[20], footer[21], footer[22], footer[23]]) as usize;

            let filter = if filter_size > 0 {
                let filter_end = filter_offset
                    .checked_add(filter_size as u64)
                    .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
                if filter_offset < SUPERBLOCK_SIZE as u64
                    || filter_end > file_len - SST2_FOOTER_SIZE as u64
                {
                    return Err(KvError::Format(FormatError::ChecksumMismatch));
                }
                let mut filter_raw = alloc::vec![0u8; filter_size];
                file.read_exact_at(filter_offset, &mut filter_raw)?;
                Some(BloomFilter::decode(&filter_raw)?)
            } else {
                None
            };

            (index_offset, index_size, filter, SST2_FOOTER_SIZE)
        } else if magic == SST1_FOOTER_MAGIC {
            let mut footer = [0u8; SST1_FOOTER_SIZE];
            file.read_exact_at(file_len - SST1_FOOTER_SIZE as u64, &mut footer)?;
            let expected_footer_crc =
                u32::from_le_bytes([footer[28], footer[29], footer[30], footer[31]]);
            if crc32c(&footer[0..28]) != expected_footer_crc {
                return Err(KvError::Format(FormatError::ChecksumMismatch));
            }
            let index_offset = u64::from_le_bytes([
                footer[0], footer[1], footer[2], footer[3], footer[4], footer[5], footer[6],
                footer[7],
            ]);
            let index_size =
                u32::from_le_bytes([footer[8], footer[9], footer[10], footer[11]]) as usize;
            (index_offset, index_size, None, SST1_FOOTER_SIZE)
        } else {
            return Err(KvError::Format(FormatError::InvalidMagic));
        };

        let index_end = index_offset
            .checked_add(index_size as u64)
            .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
        if index_size < 8 || index_end > file_len - footer_size as u64 {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }

        // Read and verify Index Block.
        let mut index_raw = alloc::vec![0u8; index_size];
        file.read_exact_at(index_offset, &mut index_raw)?;
        let index = decode_index_block(&index_raw)?;

        Ok(Self {
            file,
            meta,
            index,
            filter,
        })
    }

    #[must_use]
    pub const fn meta(&self) -> &TableMeta {
        &self.meta
    }

    /// Returns `true` if this SSTable carries a verified Bloom Filter block.
    #[must_use]
    pub const fn has_bloom_filter(&self) -> bool {
        self.filter.is_some()
    }

    /// Binary-searches the index and single candidate data block for `key`,
    /// short-circuiting absent keys via the Bloom Filter before any Data Block I/O.
    pub fn get(&self, key: &[u8]) -> Result<Option<(u64, WriteOp)>, KvError> {
        if key < self.meta.min_key.as_slice() || key > self.meta.max_key.as_slice() {
            return Ok(None);
        }
        if let Some(filter) = &self.filter
            && !filter.may_contain(key)
        {
            return Ok(None);
        }
        let block_idx = self
            .index
            .partition_point(|entry| entry.last_key.as_slice() < key);
        let Some(idx_entry) = self.index.get(block_idx) else {
            return Ok(None);
        };
        let entries = self.read_block(idx_entry)?;
        match entries.binary_search_by(|e| e.key.as_slice().cmp(key)) {
            Ok(pos) => Ok(Some((entries[pos].seq, entries[pos].op.clone()))),
            Err(_) => Ok(None),
        }
    }

    /// Scans all entries falling within `(start, end)` bounds in ascending key order.
    pub fn scan_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<Vec<SstEntry>, KvError> {
        let first_block = match start {
            Bound::Unbounded => 0,
            Bound::Included(k) => self.index.partition_point(|e| e.last_key.as_slice() < k),
            Bound::Excluded(k) => self.index.partition_point(|e| e.last_key.as_slice() <= k),
        };

        let mut out = Vec::new();
        for idx_entry in self.index.iter().skip(first_block) {
            let entries = self.read_block(idx_entry)?;
            for entry in entries {
                if !key_meets_start(&entry.key, start) {
                    continue;
                }
                if !key_meets_end(&entry.key, end) {
                    return Ok(out);
                }
                out.push(entry);
            }
        }
        Ok(out)
    }

    /// Reads all entries from every block (used during Compaction).
    pub fn read_all(&self) -> Result<Vec<SstEntry>, KvError> {
        self.scan_range(Bound::Unbounded, Bound::Unbounded)
    }

    fn read_block(&self, idx: &IndexEntry) -> Result<Vec<SstEntry>, KvError> {
        let size = idx.block_size as usize;
        if size < 4 {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let mut raw = alloc::vec![0u8; size];
        self.file.read_exact_at(idx.block_offset, &mut raw)?;
        let crc_pos = size - 4;
        let expected_crc = u32::from_le_bytes([
            raw[crc_pos],
            raw[crc_pos + 1],
            raw[crc_pos + 2],
            raw[crc_pos + 3],
        ]);
        if crc32c(&raw[..crc_pos]) != expected_crc {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        decode_data_block(&raw[..crc_pos])
    }
}

fn key_meets_start(key: &[u8], start: Bound<&[u8]>) -> bool {
    match start {
        Bound::Unbounded => true,
        Bound::Included(s) => key >= s,
        Bound::Excluded(s) => key > s,
    }
}

fn key_meets_end(key: &[u8], end: Bound<&[u8]>) -> bool {
    match end {
        Bound::Unbounded => true,
        Bound::Included(e) => key <= e,
        Bound::Excluded(e) => key < e,
    }
}

fn encode_entry(buf: &mut Vec<u8>, entry: &SstEntry) -> Result<(), KvError> {
    let klen = u32::try_from(entry.key.len()).map_err(|_| KvError::InvalidArgument)?;
    match &entry.op {
        WriteOp::Put(val) => {
            let vlen = u32::try_from(val.len()).map_err(|_| KvError::InvalidArgument)?;
            buf.extend_from_slice(&klen.to_le_bytes());
            buf.extend_from_slice(&vlen.to_le_bytes());
            buf.push(OP_PUT);
            buf.extend_from_slice(&entry.seq.to_le_bytes());
            buf.extend_from_slice(&entry.key);
            buf.extend_from_slice(val);
        }
        WriteOp::Delete => {
            buf.extend_from_slice(&klen.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.push(OP_DELETE);
            buf.extend_from_slice(&entry.seq.to_le_bytes());
            buf.extend_from_slice(&entry.key);
        }
    }
    Ok(())
}

fn flush_data_block(
    raw: &mut Vec<u8>,
    block_buf: &mut Vec<u8>,
    last_key: &mut Option<Vec<u8>>,
    index_entries: &mut Vec<IndexEntry>,
) -> Result<(), KvError> {
    let offset = raw.len() as u64;
    let crc = crc32c(block_buf);
    block_buf.extend_from_slice(&crc.to_le_bytes());
    let size = u32::try_from(block_buf.len()).map_err(|_| KvError::InvalidArgument)?;
    raw.extend_from_slice(block_buf);
    block_buf.clear();

    let key = last_key.take().ok_or(KvError::InvalidArgument)?;
    index_entries.push(IndexEntry {
        last_key: key,
        block_offset: offset,
        block_size: size,
    });
    Ok(())
}

fn decode_data_block(payload: &[u8]) -> Result<Vec<SstEntry>, KvError> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;
    while cursor < payload.len() {
        if cursor + 17 > payload.len() {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let klen = u32::from_le_bytes([
            payload[cursor],
            payload[cursor + 1],
            payload[cursor + 2],
            payload[cursor + 3],
        ]) as usize;
        let vlen = u32::from_le_bytes([
            payload[cursor + 4],
            payload[cursor + 5],
            payload[cursor + 6],
            payload[cursor + 7],
        ]) as usize;
        let op_type = payload[cursor + 8];
        let seq = u64::from_le_bytes([
            payload[cursor + 9],
            payload[cursor + 10],
            payload[cursor + 11],
            payload[cursor + 12],
            payload[cursor + 13],
            payload[cursor + 14],
            payload[cursor + 15],
            payload[cursor + 16],
        ]);
        cursor += 17;

        let key_end = cursor
            .checked_add(klen)
            .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
        let val_end = key_end
            .checked_add(vlen)
            .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
        if val_end > payload.len() {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let key = payload[cursor..key_end].to_vec();
        let op = match op_type {
            OP_PUT => WriteOp::Put(payload[key_end..val_end].to_vec()),
            OP_DELETE if vlen == 0 => WriteOp::Delete,
            _ => return Err(KvError::Format(FormatError::ChecksumMismatch)),
        };
        entries.push(SstEntry { key, seq, op });
        cursor = val_end;
    }
    Ok(entries)
}

fn decode_index_block(raw: &[u8]) -> Result<Vec<IndexEntry>, KvError> {
    if raw.len() < 8 {
        return Err(KvError::Format(FormatError::ChecksumMismatch));
    }
    let crc_pos = raw.len() - 4;
    let expected_crc = u32::from_le_bytes([
        raw[crc_pos],
        raw[crc_pos + 1],
        raw[crc_pos + 2],
        raw[crc_pos + 3],
    ]);
    if crc32c(&raw[..crc_pos]) != expected_crc {
        return Err(KvError::Format(FormatError::ChecksumMismatch));
    }
    let body = &raw[..crc_pos];
    let count = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let mut cursor = 4usize;
    let mut entries = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if cursor + 4 > body.len() {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let klen = u32::from_le_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]) as usize;
        cursor += 4;
        let key_end = cursor
            .checked_add(klen)
            .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
        let rec_end = key_end
            .checked_add(12)
            .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
        if rec_end > body.len() {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let last_key = body[cursor..key_end].to_vec();
        let block_offset = u64::from_le_bytes([
            body[key_end],
            body[key_end + 1],
            body[key_end + 2],
            body[key_end + 3],
            body[key_end + 4],
            body[key_end + 5],
            body[key_end + 6],
            body[key_end + 7],
        ]);
        let block_size = u32::from_le_bytes([
            body[key_end + 8],
            body[key_end + 9],
            body[key_end + 10],
            body[key_end + 11],
        ]);
        entries.push(IndexEntry {
            last_key,
            block_offset,
            block_size,
        });
        cursor = rec_end;
    }
    if cursor != body.len() {
        return Err(KvError::Format(FormatError::ChecksumMismatch));
    }
    Ok(entries)
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use alloc::format;
    use alloc::vec::Vec;
    use core::ops::Bound;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use hal_vfs::fault_sim::FaultSimFs;
    use hal_vfs::{FileSystem, StorageFile, VfsError};

    use super::{SstBuilder, SstEntry, SstReader};
    use crate::wal::WriteOp;

    struct CountingFile<F> {
        inner: F,
        reads: Arc<AtomicUsize>,
    }

    impl<F: StorageFile> StorageFile for CountingFile<F> {
        fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), VfsError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read_exact_at(offset, buf)
        }

        fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), VfsError> {
            self.inner.write_all_at(offset, buf)
        }

        fn sync_data(&mut self) -> Result<(), VfsError> {
            self.inner.sync_data()
        }

        fn len(&self) -> Result<u64, VfsError> {
            self.inner.len()
        }

        fn truncate(&mut self, len: u64) -> Result<(), VfsError> {
            self.inner.truncate(len)
        }
    }

    #[test]
    fn sstable_multi_block_point_lookup_and_range_scan() {
        let fs = FaultSimFs::new(11);
        let mut entries = Vec::new();
        for i in 0..100u32 {
            let key = format!("key-{i:04}").into_bytes();
            let op = if i % 10 == 0 {
                WriteOp::Delete
            } else {
                WriteOp::Put(format!("val-{i:04}-payload-padding-bytes").into_bytes())
            };
            entries.push(SstEntry {
                key,
                seq: u64::from(i) + 1,
                op,
            });
        }

        let file = fs.open_or_create("sst-000001.sst").unwrap();
        let meta = SstBuilder::build(file, 1, 0, &entries, 256, 4096).unwrap();
        fs.sync_dir().unwrap();

        let ro = fs.open_read_only("sst-000001.sst").unwrap();
        let reader = SstReader::open(ro, meta).unwrap();
        assert!(reader.has_bloom_filter());

        assert_eq!(reader.get(b"key-0000").unwrap(), Some((1, WriteOp::Delete)));
        assert_eq!(
            reader.get(b"key-0042").unwrap(),
            Some((43, WriteOp::Put(b"val-0042-payload-padding-bytes".to_vec())))
        );
        assert_eq!(reader.get(b"key-9999").unwrap(), None);

        let range = reader
            .scan_range(Bound::Included(b"key-0020"), Bound::Excluded(b"key-0025"))
            .unwrap();
        assert_eq!(range.len(), 5);
        assert_eq!(range[0].key, b"key-0020");
        assert_eq!(range[4].key, b"key-0024");
    }

    #[test]
    fn sstable_bloom_filter_eliminates_data_block_reads_for_absent_keys() {
        let fs = FaultSimFs::new(77);
        let mut entries = Vec::new();
        // Insert only even keys so odd keys fall strictly inside [min_key, max_key].
        for i in 0..500u32 {
            let k = i * 2;
            entries.push(SstEntry {
                key: format!("k-{k:06}").into_bytes(),
                seq: u64::from(i) + 1,
                op: WriteOp::Put(format!("payload-{k:06}").into_bytes()),
            });
        }

        let file = fs.open_or_create("sst-bloom.sst").unwrap();
        let meta = SstBuilder::build(file, 2, 0, &entries, 256, 4096).unwrap();
        fs.sync_dir().unwrap();

        let reads = Arc::new(AtomicUsize::new(0));
        let counting = CountingFile {
            inner: fs.open_read_only("sst-bloom.sst").unwrap(),
            reads: Arc::clone(&reads),
        };
        let reader = SstReader::open(counting, meta).unwrap();
        assert!(reader.has_bloom_filter());

        // Reset read counter after open() so we measure only point-lookup Data Block I/Os.
        reads.store(0, Ordering::Relaxed);
        for i in 0..500u32 {
            let absent_key = format!("k-{:06}", i * 2 + 1).into_bytes();
            assert_eq!(reader.get(&absent_key).unwrap(), None);
        }
        let absent_disk_reads = reads.load(Ordering::Relaxed);
        assert!(
            absent_disk_reads <= 20,
            "Bloom filter should avoid >96% of Data Block reads on absent keys, got {absent_disk_reads}/500"
        );

        // Every present key must still be found (zero false negatives).
        for i in 0..500u32 {
            let present_key = format!("k-{:06}", i * 2).into_bytes();
            assert!(reader.get(&present_key).unwrap().is_some());
        }
    }

    #[test]
    fn sstable_reader_remains_backward_compatible_with_legacy_sst1() {
        use super::{SST1_FOOTER_MAGIC, SST1_FOOTER_SIZE, encode_entry, flush_data_block};
        use crate::format::{SuperblockHeader, crc32c};
        use crate::manifest::TableMeta;

        let fs = FaultSimFs::new(88);
        let entry = SstEntry {
            key: b"legacy-key".to_vec(),
            seq: 9,
            op: WriteOp::Put(b"legacy-val".to_vec()),
        };

        let mut raw = Vec::new();
        raw.extend_from_slice(&SuperblockHeader::new(4096).encode());
        let mut block_buf = Vec::new();
        let mut last_key = Some(entry.key.clone());
        let mut index_entries = Vec::new();
        encode_entry(&mut block_buf, &entry).unwrap();
        flush_data_block(&mut raw, &mut block_buf, &mut last_key, &mut index_entries).unwrap();

        let index_offset = raw.len() as u64;
        let mut index_buf = Vec::new();
        index_buf.extend_from_slice(&1u32.to_le_bytes());
        index_buf.extend_from_slice(&(index_entries[0].last_key.len() as u32).to_le_bytes());
        index_buf.extend_from_slice(&index_entries[0].last_key);
        index_buf.extend_from_slice(&index_entries[0].block_offset.to_le_bytes());
        index_buf.extend_from_slice(&index_entries[0].block_size.to_le_bytes());
        let index_crc = crc32c(&index_buf);
        index_buf.extend_from_slice(&index_crc.to_le_bytes());
        let index_size = index_buf.len() as u32;
        raw.extend_from_slice(&index_buf);

        let mut footer = [0u8; SST1_FOOTER_SIZE];
        footer[0..8].copy_from_slice(&index_offset.to_le_bytes());
        footer[8..12].copy_from_slice(&index_size.to_le_bytes());
        footer[12..16].copy_from_slice(&1u32.to_le_bytes());
        footer[16..24].copy_from_slice(&9u64.to_le_bytes());
        footer[24..28].copy_from_slice(&SST1_FOOTER_MAGIC);
        let fcrc = crc32c(&footer[0..28]);
        footer[28..32].copy_from_slice(&fcrc.to_le_bytes());
        raw.extend_from_slice(&footer);

        let mut f = fs.open_or_create("legacy.sst").unwrap();
        f.write_all_at(0, &raw).unwrap();
        f.sync_data().unwrap();
        drop(f);
        fs.sync_dir().unwrap();

        let meta = TableMeta {
            file_id: 99,
            level: 0,
            max_seq: 9,
            file_size: raw.len() as u64,
            min_key: b"legacy-key".to_vec(),
            max_key: b"legacy-key".to_vec(),
        };
        let reader = SstReader::open(fs.open_read_only("legacy.sst").unwrap(), meta).unwrap();
        assert!(!reader.has_bloom_filter());
        assert_eq!(
            reader.get(b"legacy-key").unwrap(),
            Some((9, WriteOp::Put(b"legacy-val".to_vec())))
        );
        assert_eq!(reader.get(b"missing").unwrap(), None);
    }
}
