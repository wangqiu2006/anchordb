//! Crash-safe LSM-Tree ordered KV engine built over [`hal_vfs::FileSystem`].
//!
//! Features implemented in Milestone 1:
//! - Exclusive directory lock (`LOCK`) held for the engine's lifetime.
//! - Commit-time `WriteBatch` WAL with single `sync_data()` per transaction.
//! - Copy-on-Write `Arc<BTreeMap>` MemTable snapshots (read transactions never block writers).
//! - Ordered SSTable flush, multi-way merge scan (`MergeIterator`), and Compaction with tombstone GC.
//! - Reference-counted deferred SSTable reclamation for strict Windows file-handle safety.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::string::String;
use std::sync::{Arc, Mutex, RwLock};
use std::vec::Vec;

use hal_vfs::{FileSystem, StorageFile, VfsError};

use crate::engine::{KvEngine, KvEntry, KvError, KvIterator, KvTransaction};
use crate::manifest::{
    ManifestState, load_manifest, manifest_file_name, persist_manifest, sst_file_name,
    wal_file_name,
};
use crate::sstable::{SstBuilder, SstEntry, SstReader};
use crate::wal::{WalWriter, WriteBatch, WriteOp, recover_wal};

const LOCK_FILE_NAME: &str = "LOCK";

/// Tuning options for [`LsmEngine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmConfig {
    /// Approximate MemTable byte size triggering an automatic SSTable flush.
    pub memtable_flush_bytes: usize,
    /// Target SSTable data block size in bytes (4096 for host, 512/1024 for `mode-mcu`).
    pub block_size: usize,
    /// Page size written to file superblock headers.
    pub page_size: u16,
    /// Number of active SSTables triggering a full compaction.
    pub compaction_trigger_tables: usize,
    /// Whether `commit()` calls `sync_data()` on the WAL before returning `Ok(())`.
    pub sync_on_commit: bool,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_flush_bytes: 1024 * 1024,
            block_size: 4096,
            page_size: 4096,
            compaction_trigger_tables: 4,
            sync_on_commit: true,
        }
    }
}

type MemMap = BTreeMap<Vec<u8>, (u64, WriteOp)>;

#[derive(Debug)]
struct PendingReclaimSst<F: StorageFile> {
    file_name: String,
    reader: Arc<SstReader<F>>,
}

struct EngineState<F: FileSystem> {
    manifest: ManifestState,
    memtable: Arc<MemMap>,
    memtable_bytes: usize,
    /// Active SSTables ordered from newest to oldest.
    tables: Vec<Arc<SstReader<F::File>>>,
    wal: WalWriter<F::File>,
    last_lsn: u64,
    next_txn_id: u64,
    obsolete_files: Vec<String>,
    pending_reclaim_ssts: Vec<PendingReclaimSst<F::File>>,
}

/// Persistent ordered KV engine backed by an LSM-Tree over any [`FileSystem`].
pub struct LsmEngine<F: FileSystem> {
    fs: F,
    config: LsmConfig,
    _lock: F::Lock,
    write_guard: Mutex<()>,
    state: RwLock<EngineState<F>>,
}

impl<F: FileSystem> Drop for LsmEngine<F> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.write() {
            reclaim_obsolete_locked(&self.fs, &mut state);
        }
    }
}

impl<F: FileSystem> LsmEngine<F> {
    /// Opens an existing AnchorDB directory (replaying Manifest + WAL) or initializes a fresh one.
    pub fn open(fs: F, config: LsmConfig) -> Result<Self, KvError> {
        let lock = fs.lock_exclusive(LOCK_FILE_NAME)?;

        let manifest = match load_manifest(&fs)? {
            Some(existing) => existing,
            None => {
                let initial = ManifestState::initial();
                let wal_name = wal_file_name(initial.active_wal_id);
                let wal_file = fs.open_or_create(&wal_name)?;
                let wal_writer = WalWriter::create(wal_file, config.page_size)?;
                drop(wal_writer);
                fs.sync_dir()?;
                persist_manifest(&fs, &initial, config.page_size)?;
                initial
            }
        };

        // Load and verify all live SSTables referenced by the Manifest (newest first).
        let mut sorted_metas = manifest.tables.clone();
        sorted_metas.sort_by_key(|meta| core::cmp::Reverse(meta.file_id));
        let mut tables = Vec::with_capacity(sorted_metas.len());
        for meta in sorted_metas {
            let name = sst_file_name(meta.file_id);
            let ro = fs.open_read_only(&name)?;
            let reader = SstReader::open(ro, meta)?;
            tables.push(Arc::new(reader));
        }

        // Open and recover the active WAL, replaying records with LSN > checkpoint_lsn.
        let wal_name = wal_file_name(manifest.active_wal_id);
        let mut wal_file = fs.open_or_create(&wal_name)?;
        let recovery = recover_wal(&mut wal_file)?;

        let mut mem = MemMap::new();
        let mut mem_bytes = 0usize;
        let mut last_lsn = manifest.checkpoint_lsn;
        let mut max_txn_id = 0u64;

        for batch in recovery.batches {
            max_txn_id = max_txn_id.max(batch.txn_id);
            if batch.lsn <= manifest.checkpoint_lsn {
                continue;
            }
            last_lsn = last_lsn.max(batch.lsn);
            for (key, op) in batch.entries {
                apply_mem_entry(&mut mem, &mut mem_bytes, key, batch.lsn, op);
            }
        }

        let wal = if recovery.valid_offset == 0 {
            WalWriter::create(wal_file, config.page_size)?
        } else {
            WalWriter::resume(wal_file, recovery.valid_offset)
        };

        let engine_state = EngineState {
            manifest,
            memtable: Arc::new(mem),
            memtable_bytes: mem_bytes,
            tables,
            wal,
            last_lsn,
            next_txn_id: max_txn_id.saturating_add(1),
            obsolete_files: Vec::new(),
            pending_reclaim_ssts: Vec::new(),
        };

        Ok(Self {
            fs,
            config,
            _lock: lock,
            write_guard: Mutex::new(()),
            state: RwLock::new(engine_state),
        })
    }

    /// Returns the number of currently active SSTables (useful for diagnostics/tests).
    pub fn active_table_count(&self) -> Result<usize, KvError> {
        let guard = self
            .state
            .read()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        Ok(guard.tables.len())
    }

    /// Returns how many obsolete SSTables are still awaiting deferred reclamation.
    pub fn pending_reclaim_count(&self) -> Result<usize, KvError> {
        let guard = self
            .state
            .read()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        Ok(guard.pending_reclaim_ssts.len())
    }

    /// Forces a full compaction across all active SSTables (reclaiming overwritten keys & tombstones).
    pub fn compact_all(&self) -> Result<(), KvError> {
        let _wguard = self
            .write_guard
            .lock()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        let mut state = self
            .state
            .write()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        self.compact_locked(&mut state)?;
        reclaim_obsolete_locked(&self.fs, &mut state);
        Ok(())
    }

    fn flush_memtable_locked(&self, state: &mut EngineState<F>) -> Result<(), KvError> {
        if state.memtable.is_empty() {
            return Ok(());
        }

        let mut next_manifest = state.manifest.clone();
        let sst_id = next_manifest.alloc_file_id();
        let new_wal_id = next_manifest.alloc_file_id();
        let new_manifest_id = next_manifest.alloc_file_id();
        let old_wal_name = wal_file_name(state.manifest.active_wal_id);
        let old_manifest_name = manifest_file_name(state.manifest.manifest_file_id);

        // 1. Write new L0 SSTable and sync it.
        let entries: Vec<SstEntry> = state
            .memtable
            .iter()
            .map(|(k, (seq, op))| SstEntry {
                key: k.clone(),
                seq: *seq,
                op: op.clone(),
            })
            .collect();
        let sst_name = sst_file_name(sst_id);
        let meta = {
            let sst_file = self.fs.open_or_create(&sst_name)?;
            SstBuilder::build(
                sst_file,
                sst_id,
                0,
                &entries,
                self.config.block_size,
                self.config.page_size,
            )?
        };

        // 2. Create and sync the next WAL file before switching Manifest.
        let new_wal_name = wal_file_name(new_wal_id);
        let new_wal_file = self.fs.open_or_create(&new_wal_name)?;
        let new_wal = WalWriter::create(new_wal_file, self.config.page_size)?;

        // 3. Persist directory entries for both new SSTable and new WAL.
        self.fs.sync_dir()?;

        // 4. Verify & open the newly created SSTable reader.
        let sst_ro = self.fs.open_read_only(&sst_name)?;
        let sst_reader = Arc::new(SstReader::open(sst_ro, meta.clone())?);

        // 5. Atomically persist Manifest + CURRENT.
        next_manifest.manifest_file_id = new_manifest_id;
        next_manifest.checkpoint_lsn = state.last_lsn;
        next_manifest.active_wal_id = new_wal_id;
        next_manifest.tables.push(meta);
        persist_manifest(&self.fs, &next_manifest, self.config.page_size)?;

        // 6. Commit in-memory state transition and schedule obsolete file cleanup.
        state.manifest = next_manifest;
        state.memtable = Arc::new(MemMap::new());
        state.memtable_bytes = 0;
        state.tables.insert(0, sst_reader);
        state.wal = new_wal; // Drops old WAL handle before removing old WAL file.
        state.obsolete_files.push(old_wal_name);
        state.obsolete_files.push(old_manifest_name);
        Ok(())
    }

    fn compact_locked(&self, state: &mut EngineState<F>) -> Result<(), KvError> {
        if state.tables.len() < 2 {
            return Ok(());
        }

        // Merge all SSTables newest -> oldest; drop tombstones since this covers the bottom level.
        let mut merged: BTreeMap<Vec<u8>, (u64, WriteOp)> = BTreeMap::new();
        for reader in &state.tables {
            for entry in reader.read_all()? {
                merged.entry(entry.key).or_insert((entry.seq, entry.op));
            }
        }

        let live_entries: Vec<SstEntry> = merged
            .into_iter()
            .filter_map(|(key, (seq, op))| match op {
                WriteOp::Put(val) => Some(SstEntry {
                    key,
                    seq,
                    op: WriteOp::Put(val),
                }),
                WriteOp::Delete => None,
            })
            .collect();

        let mut next_manifest = state.manifest.clone();
        let old_manifest_name = manifest_file_name(state.manifest.manifest_file_id);
        let new_manifest_id = next_manifest.alloc_file_id();

        let mut new_readers = Vec::new();
        let mut new_metas = Vec::new();

        if !live_entries.is_empty() {
            let new_sst_id = next_manifest.alloc_file_id();
            let new_sst_name = sst_file_name(new_sst_id);
            let meta = {
                let f = self.fs.open_or_create(&new_sst_name)?;
                SstBuilder::build(
                    f,
                    new_sst_id,
                    1,
                    &live_entries,
                    self.config.block_size,
                    self.config.page_size,
                )?
            };
            self.fs.sync_dir()?;
            let ro = self.fs.open_read_only(&new_sst_name)?;
            let reader = Arc::new(SstReader::open(ro, meta.clone())?);
            new_readers.push(reader);
            new_metas.push(meta);
        }

        next_manifest.manifest_file_id = new_manifest_id;
        next_manifest.tables = new_metas;
        persist_manifest(&self.fs, &next_manifest, self.config.page_size)?;

        // Move replaced SSTables to deferred reclamation queue (respects active readers on Windows).
        let old_readers = core::mem::replace(&mut state.tables, new_readers);
        for reader in old_readers {
            let file_name = sst_file_name(reader.meta().file_id);
            state
                .pending_reclaim_ssts
                .push(PendingReclaimSst { file_name, reader });
        }
        state.manifest = next_manifest;
        state.obsolete_files.push(old_manifest_name);
        Ok(())
    }
}

impl<F: FileSystem> KvEngine for LsmEngine<F> {
    type Txn<'a>
        = LsmTxn<'a, F>
    where
        Self: 'a;

    fn begin_txn(&self, read_only: bool) -> Result<Self::Txn<'_>, KvError> {
        let guard = self
            .state
            .read()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        Ok(LsmTxn {
            engine: self,
            read_only,
            snapshot_mem: Arc::clone(&guard.memtable),
            snapshot_tables: guard.tables.clone(),
            write_batch: BTreeMap::new(),
        })
    }

    fn checkpoint(&self) -> Result<(), KvError> {
        let _wguard = self
            .write_guard
            .lock()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        let mut state = self
            .state
            .write()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        self.flush_memtable_locked(&mut state)?;
        reclaim_obsolete_locked(&self.fs, &mut state);
        Ok(())
    }
}

/// Snapshot transaction over [`LsmEngine`].
///
/// Holds an immutable Copy-on-Write MemTable reference and ref-counted SSTable handles,
/// allowing concurrent `scan` + `get_into` point lookups via `&self`.
pub struct LsmTxn<'a, F: FileSystem> {
    engine: &'a LsmEngine<F>,
    read_only: bool,
    snapshot_mem: Arc<MemMap>,
    snapshot_tables: Vec<Arc<SstReader<F::File>>>,
    write_batch: BTreeMap<Vec<u8>, WriteOp>,
}

/// Ordered iterator yielded by [`LsmTxn::scan`].
pub struct LsmIter {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl KvIterator for LsmIter {
    fn next_entry(&mut self) -> Result<Option<KvEntry<'_>>, KvError> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }
        let pos = self.index;
        self.index += 1;
        let (k, v) = &self.entries[pos];
        Ok(Some((k.as_slice(), v.as_slice())))
    }
}

impl<F: FileSystem> KvTransaction for LsmTxn<'_, F> {
    type Iter<'b>
        = LsmIter
    where
        Self: 'b;

    fn get_into(&self, key: &[u8], out: &mut [u8]) -> Result<Option<usize>, KvError> {
        // 1. Check transaction-local WriteBatch first (Read-Your-Own-Writes).
        if let Some(op) = self.write_batch.get(key) {
            return copy_op_into(op, out);
        }

        // 2. Check MemTable snapshot.
        if let Some((_, op)) = self.snapshot_mem.get(key) {
            return copy_op_into(op, out);
        }

        // 3. Probe SSTables from newest to oldest.
        for reader in &self.snapshot_tables {
            if let Some((_, op)) = reader.get(key)? {
                return copy_op_into(&op, out);
            }
        }

        Ok(None)
    }

    fn scan<'b>(
        &'b self,
        start: Bound<&'b [u8]>,
        end: Bound<&'b [u8]>,
    ) -> Result<Self::Iter<'b>, KvError> {
        // Multi-way merge from highest priority to lowest:
        // WriteBatch > MemTable > SSTables (newest -> oldest).
        let mut merged: BTreeMap<Vec<u8>, WriteOp> = BTreeMap::new();

        for (k, op) in self.write_batch.iter() {
            if key_in_bounds(k, start, end) {
                merged.insert(k.clone(), op.clone());
            }
        }

        for (k, (_, op)) in self.snapshot_mem.iter() {
            if key_in_bounds(k, start, end) {
                merged.entry(k.clone()).or_insert_with(|| op.clone());
            }
        }

        for reader in &self.snapshot_tables {
            for entry in reader.scan_range(start, end)? {
                merged.entry(entry.key).or_insert(entry.op);
            }
        }

        let live: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(k, op)| match op {
                WriteOp::Put(v) => Some((k, v)),
                WriteOp::Delete => None,
            })
            .collect();

        Ok(LsmIter {
            entries: live,
            index: 0,
        })
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), KvError> {
        if self.read_only {
            return Err(KvError::ReadOnlyTransaction);
        }
        self.write_batch
            .insert(key.to_vec(), WriteOp::Put(val.to_vec()));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), KvError> {
        if self.read_only {
            return Err(KvError::ReadOnlyTransaction);
        }
        self.write_batch.insert(key.to_vec(), WriteOp::Delete);
        Ok(())
    }

    fn commit(self) -> Result<(), KvError> {
        if self.read_only || self.write_batch.is_empty() {
            return Ok(());
        }

        let engine = self.engine;
        let write_batch = self.write_batch;
        // Release this transaction's snapshot references before acquiring the write lock so
        // deferred SSTable reclamation and COW MemTable mutation can proceed cleanly.
        drop(self.snapshot_mem);
        drop(self.snapshot_tables);

        let _wguard = engine
            .write_guard
            .lock()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;
        let mut state = engine
            .state
            .write()
            .map_err(|_| KvError::Vfs(VfsError::IoFailure))?;

        let lsn = state.last_lsn.saturating_add(1);
        let txn_id = state.next_txn_id;
        let entries: Vec<(Vec<u8>, WriteOp)> = write_batch.into_iter().collect();
        let batch = WriteBatch {
            lsn,
            txn_id,
            entries,
        };

        // 1. WAL-First: append complete WriteBatch with single sync_data().
        state.wal.append(&batch, engine.config.sync_on_commit)?;

        // 2. Apply atomically to MemTable (Copy-on-Write if active read snapshots exist).
        state.last_lsn = lsn;
        state.next_txn_id = txn_id.saturating_add(1);
        let mut mem_bytes = state.memtable_bytes;
        let mem_mut = Arc::make_mut(&mut state.memtable);
        for (key, op) in batch.entries {
            apply_mem_entry(mem_mut, &mut mem_bytes, key, lsn, op);
        }
        state.memtable_bytes = mem_bytes;

        // 3. Trigger Flush & Compaction when thresholds are reached.
        if state.memtable_bytes >= engine.config.memtable_flush_bytes {
            engine.flush_memtable_locked(&mut state)?;
        }
        if state.tables.len() >= engine.config.compaction_trigger_tables {
            engine.compact_locked(&mut state)?;
        }

        // 4. Reclaim any obsolete files whose handles are no longer held by snapshots.
        reclaim_obsolete_locked(&engine.fs, &mut state);
        Ok(())
    }

    fn abort(self) {
        // Dropping `write_batch` discards all uncommitted changes with zero WAL/MemTable impact.
    }
}

fn copy_op_into(op: &WriteOp, out: &mut [u8]) -> Result<Option<usize>, KvError> {
    match op {
        WriteOp::Delete => Ok(None),
        WriteOp::Put(val) => {
            if val.len() > out.len() {
                return Err(KvError::BufferTooSmall {
                    required: val.len(),
                });
            }
            out[..val.len()].copy_from_slice(val);
            Ok(Some(val.len()))
        }
    }
}

fn key_in_bounds(key: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let ok_start = match start {
        Bound::Unbounded => true,
        Bound::Included(s) => key >= s,
        Bound::Excluded(s) => key > s,
    };
    let ok_end = match end {
        Bound::Unbounded => true,
        Bound::Included(e) => key <= e,
        Bound::Excluded(e) => key < e,
    };
    ok_start && ok_end
}

fn apply_mem_entry(mem: &mut MemMap, mem_bytes: &mut usize, key: Vec<u8>, lsn: u64, op: WriteOp) {
    let added = key.len()
        + match &op {
            WriteOp::Put(v) => v.len(),
            WriteOp::Delete => 0,
        };
    if let Some((_, prev_op)) = mem.insert(key, (lsn, op)) {
        let prev_val_len = match prev_op {
            WriteOp::Put(v) => v.len(),
            WriteOp::Delete => 0,
        };
        *mem_bytes = mem_bytes.saturating_sub(prev_val_len);
        *mem_bytes = mem_bytes.saturating_add(added);
    } else {
        *mem_bytes = mem_bytes.saturating_add(added);
    }
}

fn reclaim_obsolete_locked<F: FileSystem>(fs: &F, state: &mut EngineState<F>) {
    // Reclaim simple obsolete files (old WALs, old Manifests).
    let mut remaining_files = Vec::new();
    for name in state.obsolete_files.drain(..) {
        match fs.remove(&name) {
            Ok(()) | Err(VfsError::NotFound) => {}
            Err(_) => remaining_files.push(name),
        }
    }
    state.obsolete_files = remaining_files;

    // Reclaim obsolete SSTables only when no active snapshot holds an Arc clone.
    let mut still_pinned = Vec::new();
    for item in state.pending_reclaim_ssts.drain(..) {
        if Arc::strong_count(&item.reader) == 1 {
            let name = item.file_name;
            drop(item.reader); // Close underlying file handle before remove (required on Windows).
            let _ = fs.remove(&name);
        } else {
            still_pinned.push(item);
        }
    }
    state.pending_reclaim_ssts = still_pinned;
}
