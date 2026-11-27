//! Atomic LSM version metadata (`CURRENT` pointer + CRC-protected `MANIFEST` files).
//!
//! Atomic update protocol (safe on both POSIX and Windows):
//! 1. Write `manifest-<id>.bin` -> `sync_data()` -> close handle -> `sync_dir()`.
//! 2. Write `CURRENT.tmp` -> `sync_data()` -> close handle.
//! 3. `rename("CURRENT.tmp", "CURRENT")` -> `sync_dir()`.
//! 4. Only after step 3 completes may obsolete WAL/SSTable/Manifest files be reclaimed.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use hal_vfs::{FileSystem, StorageFile};

use crate::engine::KvError;
use crate::format::{
    CURRENT_FORMAT_VERSION, FormatError, SUPERBLOCK_SIZE, SuperblockHeader, crc32c,
};

pub const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TMP_FILE: &str = "CURRENT.tmp";
const CURRENT_MAGIC: [u8; 4] = *b"CUR1";
const MANIFEST_MAGIC: [u8; 4] = *b"MNF1";

/// Metadata describing one live immutable SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    pub file_id: u64,
    pub level: u8,
    pub max_seq: u64,
    pub file_size: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

/// Complete durable database version state tracked by the Manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    pub manifest_file_id: u64,
    pub checkpoint_lsn: u64,
    pub active_wal_id: u64,
    pub next_file_id: u64,
    pub tables: Vec<TableMeta>,
}

impl ManifestState {
    #[must_use]
    pub fn initial() -> Self {
        Self {
            manifest_file_id: 1,
            checkpoint_lsn: 0,
            active_wal_id: 2,
            next_file_id: 3,
            tables: Vec::new(),
        }
    }

    pub fn alloc_file_id(&mut self) -> u64 {
        let id = self.next_file_id;
        self.next_file_id = self.next_file_id.saturating_add(1);
        id
    }

    pub fn encode(&self, page_size: u16) -> Result<Vec<u8>, KvError> {
        let table_count = u32::try_from(self.tables.len()).map_err(|_| KvError::InvalidArgument)?;
        let header = SuperblockHeader::new(page_size).encode();

        let mut buf = Vec::with_capacity(SUPERBLOCK_SIZE + 64);
        buf.extend_from_slice(&header);
        buf.extend_from_slice(&MANIFEST_MAGIC);
        buf.extend_from_slice(&self.manifest_file_id.to_le_bytes());
        buf.extend_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf.extend_from_slice(&self.active_wal_id.to_le_bytes());
        buf.extend_from_slice(&self.next_file_id.to_le_bytes());
        buf.extend_from_slice(&table_count.to_le_bytes());

        for t in &self.tables {
            let min_len = u32::try_from(t.min_key.len()).map_err(|_| KvError::InvalidArgument)?;
            let max_len = u32::try_from(t.max_key.len()).map_err(|_| KvError::InvalidArgument)?;
            buf.extend_from_slice(&t.file_id.to_le_bytes());
            buf.push(t.level);
            buf.extend_from_slice(&t.max_seq.to_le_bytes());
            buf.extend_from_slice(&t.file_size.to_le_bytes());
            buf.extend_from_slice(&min_len.to_le_bytes());
            buf.extend_from_slice(&t.min_key);
            buf.extend_from_slice(&max_len.to_le_bytes());
            buf.extend_from_slice(&t.max_key);
        }

        let checksum = crc32c(&buf[SUPERBLOCK_SIZE..]);
        buf.extend_from_slice(&checksum.to_le_bytes());
        Ok(buf)
    }

    pub fn decode(raw: &[u8]) -> Result<Self, KvError> {
        if raw.len() < SUPERBLOCK_SIZE + 40 + 4 {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let mut sb = [0u8; SUPERBLOCK_SIZE];
        sb.copy_from_slice(&raw[..SUPERBLOCK_SIZE]);
        let header = SuperblockHeader::decode(&sb)?;
        header.check_compatibility(CURRENT_FORMAT_VERSION, 0)?;

        let crc_pos = raw.len() - 4;
        let expected_crc = u32::from_le_bytes([
            raw[crc_pos],
            raw[crc_pos + 1],
            raw[crc_pos + 2],
            raw[crc_pos + 3],
        ]);
        let actual_crc = crc32c(&raw[SUPERBLOCK_SIZE..crc_pos]);
        if actual_crc != expected_crc {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }

        let body = &raw[SUPERBLOCK_SIZE..crc_pos];
        if body.len() < 40 || body[0..4] != MANIFEST_MAGIC {
            return Err(KvError::Format(FormatError::InvalidMagic));
        }
        let manifest_file_id = read_u64(body, 4)?;
        let checkpoint_lsn = read_u64(body, 12)?;
        let active_wal_id = read_u64(body, 20)?;
        let next_file_id = read_u64(body, 28)?;
        let table_count = read_u32(body, 36)? as usize;

        let mut cursor = 40usize;
        let mut tables = Vec::with_capacity(table_count.min(1024));
        for _ in 0..table_count {
            let file_id = read_u64(body, cursor)?;
            cursor += 8;
            let level = *body
                .get(cursor)
                .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
            cursor += 1;
            let max_seq = read_u64(body, cursor)?;
            cursor += 8;
            let file_size = read_u64(body, cursor)?;
            cursor += 8;
            let min_len = read_u32(body, cursor)? as usize;
            cursor += 4;
            let min_end = cursor
                .checked_add(min_len)
                .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
            let min_key = body
                .get(cursor..min_end)
                .ok_or(KvError::Format(FormatError::ChecksumMismatch))?
                .to_vec();
            cursor = min_end;

            let max_len = read_u32(body, cursor)? as usize;
            cursor += 4;
            let max_end = cursor
                .checked_add(max_len)
                .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
            let max_key = body
                .get(cursor..max_end)
                .ok_or(KvError::Format(FormatError::ChecksumMismatch))?
                .to_vec();
            cursor = max_end;

            tables.push(TableMeta {
                file_id,
                level,
                max_seq,
                file_size,
                min_key,
                max_key,
            });
        }

        if cursor != body.len() {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }

        Ok(Self {
            manifest_file_id,
            checkpoint_lsn,
            active_wal_id,
            next_file_id,
            tables,
        })
    }
}

#[must_use]
pub fn manifest_file_name(file_id: u64) -> String {
    format!("manifest-{file_id:06}.bin")
}

#[must_use]
pub fn wal_file_name(file_id: u64) -> String {
    format!("wal-{file_id:06}.log")
}

#[must_use]
pub fn sst_file_name(file_id: u64) -> String {
    format!("sst-{file_id:06}.sst")
}

/// Atomically writes a new Manifest file and switches `CURRENT` to point to it.
pub fn persist_manifest<F: FileSystem>(
    fs: &F,
    state: &ManifestState,
    page_size: u16,
) -> Result<(), KvError> {
    let manifest_name = manifest_file_name(state.manifest_file_id);
    let encoded = state.encode(page_size)?;

    {
        let mut mf = fs.open_or_create(&manifest_name)?;
        mf.truncate(0)?;
        mf.write_all_at(0, &encoded)?;
        mf.sync_data()?;
    }
    fs.sync_dir()?;

    // Write CURRENT.tmp with Magic + NameLen + Name + CRC32C.
    let name_bytes = manifest_name.as_bytes();
    let name_len = u32::try_from(name_bytes.len()).map_err(|_| KvError::InvalidArgument)?;
    let mut cur_buf = Vec::with_capacity(12 + name_bytes.len());
    cur_buf.extend_from_slice(&CURRENT_MAGIC);
    cur_buf.extend_from_slice(&name_len.to_le_bytes());
    cur_buf.extend_from_slice(name_bytes);
    let cur_crc = crc32c(&cur_buf);
    cur_buf.extend_from_slice(&cur_crc.to_le_bytes());

    {
        let mut tmp = fs.open_or_create(CURRENT_TMP_FILE)?;
        tmp.truncate(0)?;
        tmp.write_all_at(0, &cur_buf)?;
        tmp.sync_data()?;
    }

    fs.rename(CURRENT_TMP_FILE, CURRENT_FILE)?;
    fs.sync_dir()?;
    Ok(())
}

/// Loads the active `ManifestState` pointed to by `CURRENT`, or returns `Ok(None)` for a fresh DB.
pub fn load_manifest<F: FileSystem>(fs: &F) -> Result<Option<ManifestState>, KvError> {
    if !fs.exists(CURRENT_FILE)? {
        return Ok(None);
    }

    let manifest_name = {
        let cur_file = fs.open_read_only(CURRENT_FILE)?;
        let len = usize::try_from(cur_file.len()?).map_err(|_| KvError::InvalidArgument)?;
        if len < 12 {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        let mut raw = alloc::vec![0u8; len];
        cur_file.read_exact_at(0, &mut raw)?;

        let crc_pos = raw.len() - 4;
        let expected_crc = read_u32(&raw, crc_pos)?;
        if crc32c(&raw[..crc_pos]) != expected_crc {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        if raw[0..4] != CURRENT_MAGIC {
            return Err(KvError::Format(FormatError::InvalidMagic));
        }
        let name_len = read_u32(&raw, 4)? as usize;
        if 8 + name_len != crc_pos {
            return Err(KvError::Format(FormatError::ChecksumMismatch));
        }
        core::str::from_utf8(&raw[8..8 + name_len])
            .map_err(|_| KvError::Format(FormatError::ChecksumMismatch))?
            .to_string()
    };

    let mf = fs.open_read_only(&manifest_name)?;
    let mf_len = usize::try_from(mf.len()?).map_err(|_| KvError::InvalidArgument)?;
    let mut mf_raw = alloc::vec![0u8; mf_len];
    mf.read_exact_at(0, &mut mf_raw)?;
    let state = ManifestState::decode(&mf_raw)?;
    Ok(Some(state))
}

fn read_u32(buf: &[u8], offset: usize) -> Result<u32, KvError> {
    let end = offset
        .checked_add(4)
        .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
    let s = buf
        .get(offset..end)
        .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_u64(buf: &[u8], offset: usize) -> Result<u64, KvError> {
    let end = offset
        .checked_add(8)
        .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
    let s = buf
        .get(offset..end)
        .ok_or(KvError::Format(FormatError::ChecksumMismatch))?;
    Ok(u64::from_le_bytes([
        s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
    ]))
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use hal_vfs::fault_sim::FaultSimFs;

    use super::{ManifestState, TableMeta, load_manifest, persist_manifest};

    #[test]
    fn manifest_atomic_persist_and_reload_survives_power_loss() {
        let fs = FaultSimFs::new(100);
        assert_eq!(load_manifest(&fs).unwrap(), None);

        let mut state = ManifestState::initial();
        state.checkpoint_lsn = 42;
        state.tables.push(TableMeta {
            file_id: 5,
            level: 0,
            max_seq: 42,
            file_size: 4096,
            min_key: b"aaa".to_vec(),
            max_key: b"zzz".to_vec(),
        });

        persist_manifest(&fs, &state, 4096).unwrap();
        fs.simulate_power_loss_and_reboot().unwrap();

        let loaded = load_manifest(&fs).unwrap().unwrap();
        assert_eq!(loaded, state);
    }
}
