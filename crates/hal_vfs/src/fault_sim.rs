//! Deterministic in-memory power-loss fault-injection filesystem.
//!
//! Simulates real-world crash hazards with 100% seed reproducibility:
//! 1. Write-budget exhaustion (`SimulatedPowerLoss`).
//! 2. Torn writes (partial prefix + corrupted tail byte).
//! 3. Unsynced `write_all_at` / `truncate` drop or out-of-order persistence.
//! 4. Unsynced directory entry loss (`open_or_create`, `rename`, `remove` without `sync_dir`).
//! 5. Optional strict Windows file-handle semantics (forbidding delete/overwrite of open files).

use std::borrow::ToOwned;
use std::collections::{BTreeMap, BTreeSet};
use std::string::String;
use std::sync::{Arc, Mutex, MutexGuard};
use std::vec::Vec;

use crate::{FileLock, FileSystem, StorageFile, VfsError, validate_file_name};

type InodeId = u64;

/// Zero-dependency SplitMix64 PRNG for 100% deterministic cross-platform fault injection.
#[derive(Debug, Clone)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_usize(&mut self, upper_exclusive: usize) -> usize {
        if upper_exclusive <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % upper_exclusive
    }
}

/// Policy applied to buffered writes that have not been flushed via `sync_data()` on power loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsyncedWritePolicy {
    /// Drop every write performed after the last `sync_data()`.
    DropAll,
    /// Deterministically drop, reorder a subset, or tear unsynced writes.
    RandomDropOrReorder,
}

/// Configuration for [`FaultSimFs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultConfig {
    pub seed: u64,
    pub allow_torn_writes: bool,
    pub drop_unsynced_dir_ops: bool,
    pub unsynced_write_policy: UnsyncedWritePolicy,
    /// Reject `remove` / `rename` targeting an inode with active open handles (mirrors Windows).
    pub enforce_windows_handle_semantics: bool,
}

impl FaultConfig {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            seed,
            allow_torn_writes: true,
            drop_unsynced_dir_ops: true,
            unsynced_write_policy: UnsyncedWritePolicy::RandomDropOrReorder,
            enforce_windows_handle_semantics: true,
        }
    }
}

/// Counters exposed for assertions and benchmark diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FaultSimStats {
    pub write_calls: u64,
    pub bytes_written: u64,
    pub sync_data_calls: u64,
    pub sync_dir_calls: u64,
    pub power_losses: u64,
}

#[derive(Debug, Clone)]
enum PendingOp {
    Write { offset: usize, data: Vec<u8> },
    Truncate { size: usize },
}

#[derive(Debug, Clone, Default)]
struct SimInode {
    durable_data: Vec<u8>,
    live_data: Vec<u8>,
    pending_ops: Vec<PendingOp>,
    open_handles: usize,
}

impl SimInode {
    fn apply_op(buf: &mut Vec<u8>, op: &PendingOp) {
        match op {
            PendingOp::Write { offset, data } => {
                let end = offset.saturating_add(data.len());
                if buf.len() < end {
                    buf.resize(end, 0);
                }
                buf[*offset..end].copy_from_slice(data);
            }
            PendingOp::Truncate { size } => {
                buf.resize(*size, 0);
            }
        }
    }
}

#[derive(Debug)]
struct SimState {
    config: FaultConfig,
    rng: SplitMix64,
    next_inode: InodeId,
    inodes: BTreeMap<InodeId, SimInode>,
    durable_dir: BTreeMap<String, InodeId>,
    live_dir: BTreeMap<String, InodeId>,
    active_locks: BTreeSet<String>,
    crashed: bool,
    remaining_write_budget: Option<usize>,
    stats: FaultSimStats,
}

impl SimState {
    fn new(config: FaultConfig) -> Self {
        Self {
            rng: SplitMix64::new(config.seed),
            config,
            next_inode: 1,
            inodes: BTreeMap::new(),
            durable_dir: BTreeMap::new(),
            live_dir: BTreeMap::new(),
            active_locks: BTreeSet::new(),
            crashed: false,
            remaining_write_budget: None,
            stats: FaultSimStats::default(),
        }
    }

    fn check_alive(&self) -> Result<(), VfsError> {
        if self.crashed {
            Err(VfsError::SimulatedPowerLoss)
        } else {
            Ok(())
        }
    }

    fn alloc_inode(&mut self) -> InodeId {
        let id = self.next_inode;
        self.next_inode = self.next_inode.saturating_add(1);
        self.inodes.insert(id, SimInode::default());
        id
    }

    fn gc_unreferenced_inodes(&mut self) {
        let referenced: BTreeSet<InodeId> = self
            .live_dir
            .values()
            .chain(self.durable_dir.values())
            .copied()
            .collect();
        self.inodes
            .retain(|id, inode| inode.open_handles > 0 || referenced.contains(id));
    }
}

/// Exclusive lock handle for [`FaultSimFs`].
#[derive(Debug)]
pub struct FaultSimLock {
    state: Arc<Mutex<SimState>>,
    name: String,
    epoch: u64,
}

impl FileLock for FaultSimLock {}

impl Drop for FaultSimLock {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.state.lock()
            && guard.stats.power_losses == self.epoch
        {
            guard.active_locks.remove(&self.name);
        }
    }
}

/// Positioned file handle backed by [`FaultSimFs`].
#[derive(Debug)]
pub struct FaultSimFile {
    state: Arc<Mutex<SimState>>,
    inode_id: InodeId,
    writable: bool,
}

impl Drop for FaultSimFile {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.state.lock() {
            if let Some(inode) = guard.inodes.get_mut(&self.inode_id) {
                inode.open_handles = inode.open_handles.saturating_sub(1);
            }
            guard.gc_unreferenced_inodes();
        }
    }
}

impl FaultSimFile {
    fn lock_state(&self) -> Result<MutexGuard<'_, SimState>, VfsError> {
        self.state.lock().map_err(|_| VfsError::IoFailure)
    }
}

impl StorageFile for FaultSimFile {
    fn len(&self) -> Result<u64, VfsError> {
        let state = self.lock_state()?;
        state.check_alive()?;
        let inode = state.inodes.get(&self.inode_id).ok_or(VfsError::NotFound)?;
        Ok(inode.live_data.len() as u64)
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), VfsError> {
        let state = self.lock_state()?;
        state.check_alive()?;
        let inode = state.inodes.get(&self.inode_id).ok_or(VfsError::NotFound)?;
        let start = usize::try_from(offset).map_err(|_| VfsError::IoFailure)?;
        let end = start.checked_add(buf.len()).ok_or(VfsError::IoFailure)?;
        if end > inode.live_data.len() {
            return Err(VfsError::IoFailure);
        }
        buf.copy_from_slice(&inode.live_data[start..end]);
        Ok(())
    }

    fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<(), VfsError> {
        if !self.writable {
            return Err(VfsError::IoFailure);
        }
        let mut state = self.lock_state()?;
        state.check_alive()?;
        if data.is_empty() {
            return Ok(());
        }
        let start = usize::try_from(offset).map_err(|_| VfsError::IoFailure)?;
        let _end = start.checked_add(data.len()).ok_or(VfsError::NoSpace)?;

        if let Some(budget) = state.remaining_write_budget {
            if data.len() > budget {
                // Budget exhausted: simulate sudden power loss with an optional torn prefix.
                let allow_torn = state.config.allow_torn_writes;
                let torn_bytes = if allow_torn {
                    let max_prefix = budget.min(data.len());
                    let prefix_len = if max_prefix == 0 {
                        // Even at zero remaining budget, allow a partial sector tear when data > 0.
                        state.rng.next_usize(data.len()).max(1)
                    } else {
                        max_prefix
                    };
                    let mut torn = data[..prefix_len].to_vec();
                    // Corrupt the trailing byte deterministically to exercise CRC checks.
                    if let Some(last) = torn.last_mut() {
                        let mask = (state.rng.next_u64() as u8) | 0x01;
                        *last ^= mask;
                    }
                    Some(torn)
                } else {
                    None
                };

                if let Some(torn) = torn_bytes
                    && let Some(inode) = state.inodes.get_mut(&self.inode_id)
                {
                    let op = PendingOp::Write {
                        offset: start,
                        data: torn,
                    };
                    SimInode::apply_op(&mut inode.durable_data, &op);
                    SimInode::apply_op(&mut inode.live_data, &op);
                }

                state.remaining_write_budget = Some(0);
                state.crashed = true;
                return Err(VfsError::SimulatedPowerLoss);
            }
            state.remaining_write_budget = Some(budget - data.len());
        }

        let op = PendingOp::Write {
            offset: start,
            data: data.to_vec(),
        };
        let inode = state
            .inodes
            .get_mut(&self.inode_id)
            .ok_or(VfsError::NotFound)?;
        SimInode::apply_op(&mut inode.live_data, &op);
        inode.pending_ops.push(op);

        state.stats.write_calls = state.stats.write_calls.saturating_add(1);
        state.stats.bytes_written = state.stats.bytes_written.saturating_add(data.len() as u64);
        Ok(())
    }

    fn sync_data(&mut self) -> Result<(), VfsError> {
        let mut state = self.lock_state()?;
        state.check_alive()?;
        let inode = state
            .inodes
            .get_mut(&self.inode_id)
            .ok_or(VfsError::NotFound)?;
        inode.durable_data = inode.live_data.clone();
        inode.pending_ops.clear();
        state.stats.sync_data_calls = state.stats.sync_data_calls.saturating_add(1);
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<(), VfsError> {
        if !self.writable {
            return Err(VfsError::IoFailure);
        }
        let mut state = self.lock_state()?;
        state.check_alive()?;
        let new_len = usize::try_from(size).map_err(|_| VfsError::NoSpace)?;
        let op = PendingOp::Truncate { size: new_len };
        let inode = state
            .inodes
            .get_mut(&self.inode_id)
            .ok_or(VfsError::NotFound)?;
        SimInode::apply_op(&mut inode.live_data, &op);
        inode.pending_ops.push(op);
        Ok(())
    }
}

/// Deterministic power-loss fault-injection [`FileSystem`].
#[derive(Debug, Clone)]
pub struct FaultSimFs {
    state: Arc<Mutex<SimState>>,
}

impl FaultSimFs {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self::with_config(FaultConfig::new(seed))
    }

    #[must_use]
    pub fn with_config(config: FaultConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(SimState::new(config))),
        }
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SimState>, VfsError> {
        self.state.lock().map_err(|_| VfsError::IoFailure)
    }

    /// Sets or clears the remaining byte write budget before triggering `SimulatedPowerLoss`.
    pub fn set_write_budget(&self, budget_bytes: Option<usize>) -> Result<(), VfsError> {
        let mut state = self.lock_state()?;
        state.remaining_write_budget = budget_bytes;
        Ok(())
    }

    /// Returns snapshot of diagnostic counters.
    pub fn stats(&self) -> Result<FaultSimStats, VfsError> {
        let state = self.lock_state()?;
        Ok(state.stats)
    }

    /// Returns whether the simulator is currently halted due to a simulated power loss.
    pub fn is_crashed(&self) -> Result<bool, VfsError> {
        let state = self.lock_state()?;
        Ok(state.crashed)
    }

    /// Simulates an abrupt power cut followed by a clean reboot:
    /// - Reverts unsynced directory entries (if enabled).
    /// - Drops, reorders, or tears unsynced writes according to [`UnsyncedWritePolicy`].
    /// - Releases all volatile exclusive locks and clears the crash latch.
    pub fn simulate_power_loss_and_reboot(&self) -> Result<(), VfsError> {
        let mut state = self.lock_state()?;
        state.stats.power_losses = state.stats.power_losses.saturating_add(1);
        state.crashed = false;
        state.remaining_write_budget = None;
        state.active_locks.clear();

        if state.config.drop_unsynced_dir_ops {
            state.live_dir = state.durable_dir.clone();
        } else {
            state.durable_dir = state.live_dir.clone();
        }

        let policy = state.config.unsynced_write_policy;
        let allow_torn = state.config.allow_torn_writes;
        let inode_ids: Vec<InodeId> = state.inodes.keys().copied().collect();

        for id in inode_ids {
            let pending = state
                .inodes
                .get_mut(&id)
                .map(|inode| core::mem::take(&mut inode.pending_ops))
                .unwrap_or_default();

            if !pending.is_empty() {
                match policy {
                    UnsyncedWritePolicy::DropAll => {}
                    UnsyncedWritePolicy::RandomDropOrReorder => {
                        let mode = state.rng.next_usize(3);
                        match mode {
                            0 => {
                                // Drop all unsynced writes.
                            }
                            1 => {
                                // Persist a random out-of-order subset of unsynced operations.
                                let mut indices: Vec<usize> = (0..pending.len()).collect();
                                for i in (1..indices.len()).rev() {
                                    let j = state.rng.next_usize(i + 1);
                                    indices.swap(i, j);
                                }
                                let keep_count = state.rng.next_usize(indices.len() + 1);
                                if let Some(inode) = state.inodes.get_mut(&id) {
                                    for &idx in indices.iter().take(keep_count) {
                                        SimInode::apply_op(&mut inode.durable_data, &pending[idx]);
                                    }
                                }
                            }
                            _ => {
                                // Persist a random prefix and optionally tear the final write.
                                let keep_count = state.rng.next_usize(pending.len()).max(1);
                                let last_idx = keep_count - 1;
                                for (idx, op) in pending.iter().take(keep_count).enumerate() {
                                    let mut applied = op.clone();
                                    if idx == last_idx
                                        && allow_torn
                                        && let PendingOp::Write { data, .. } = &mut applied
                                        && !data.is_empty()
                                    {
                                        let cut = state.rng.next_usize(data.len()).max(1);
                                        data.truncate(cut);
                                        if let Some(last) = data.last_mut() {
                                            let mask = (state.rng.next_u64() as u8) | 0x01;
                                            *last ^= mask;
                                        }
                                    }
                                    if let Some(inode) = state.inodes.get_mut(&id) {
                                        SimInode::apply_op(&mut inode.durable_data, &applied);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if let Some(inode) = state.inodes.get_mut(&id) {
                inode.live_data = inode.durable_data.clone();
            }
        }

        state.gc_unreferenced_inodes();
        Ok(())
    }
}

impl FileSystem for FaultSimFs {
    type File = FaultSimFile;
    type Lock = FaultSimLock;

    fn lock_exclusive(&self, name: &str) -> Result<Self::Lock, VfsError> {
        validate_file_name(name)?;
        let mut state = self.lock_state()?;
        state.check_alive()?;
        if !state.active_locks.insert(name.to_owned()) {
            return Err(VfsError::AlreadyLocked);
        }
        let epoch = state.stats.power_losses;
        Ok(FaultSimLock {
            state: Arc::clone(&self.state),
            name: name.to_owned(),
            epoch,
        })
    }

    fn exists(&self, name: &str) -> Result<bool, VfsError> {
        validate_file_name(name)?;
        let state = self.lock_state()?;
        state.check_alive()?;
        Ok(state.live_dir.contains_key(name))
    }

    fn open_or_create(&self, name: &str) -> Result<Self::File, VfsError> {
        validate_file_name(name)?;
        let mut state = self.lock_state()?;
        state.check_alive()?;
        let inode_id = if let Some(&id) = state.live_dir.get(name) {
            id
        } else {
            let id = state.alloc_inode();
            state.live_dir.insert(name.to_owned(), id);
            id
        };
        if let Some(inode) = state.inodes.get_mut(&inode_id) {
            inode.open_handles = inode.open_handles.saturating_add(1);
        }
        Ok(FaultSimFile {
            state: Arc::clone(&self.state),
            inode_id,
            writable: true,
        })
    }

    fn open_read_only(&self, name: &str) -> Result<Self::File, VfsError> {
        validate_file_name(name)?;
        let mut state = self.lock_state()?;
        state.check_alive()?;
        let inode_id = *state.live_dir.get(name).ok_or(VfsError::NotFound)?;
        if let Some(inode) = state.inodes.get_mut(&inode_id) {
            inode.open_handles = inode.open_handles.saturating_add(1);
        }
        Ok(FaultSimFile {
            state: Arc::clone(&self.state),
            inode_id,
            writable: false,
        })
    }

    fn remove(&self, name: &str) -> Result<(), VfsError> {
        validate_file_name(name)?;
        let mut state = self.lock_state()?;
        state.check_alive()?;
        let inode_id = *state.live_dir.get(name).ok_or(VfsError::NotFound)?;
        if state.config.enforce_windows_handle_semantics
            && state
                .inodes
                .get(&inode_id)
                .is_some_and(|inode| inode.open_handles > 0)
        {
            return Err(VfsError::IoFailure);
        }
        state.live_dir.remove(name);
        state.gc_unreferenced_inodes();
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), VfsError> {
        validate_file_name(from)?;
        validate_file_name(to)?;
        let mut state = self.lock_state()?;
        state.check_alive()?;
        let src_id = *state.live_dir.get(from).ok_or(VfsError::NotFound)?;
        if state.config.enforce_windows_handle_semantics {
            if state
                .inodes
                .get(&src_id)
                .is_some_and(|inode| inode.open_handles > 0)
            {
                return Err(VfsError::IoFailure);
            }
            if let Some(&dst_id) = state.live_dir.get(to)
                && dst_id != src_id
                && state
                    .inodes
                    .get(&dst_id)
                    .is_some_and(|inode| inode.open_handles > 0)
            {
                return Err(VfsError::IoFailure);
            }
        }
        state.live_dir.remove(from);
        state.live_dir.insert(to.to_owned(), src_id);
        state.gc_unreferenced_inodes();
        Ok(())
    }

    fn sync_dir(&self) -> Result<(), VfsError> {
        let mut state = self.lock_state()?;
        state.check_alive()?;
        state.durable_dir = state.live_dir.clone();
        state.gc_unreferenced_inodes();
        state.stats.sync_dir_calls = state.stats.sync_dir_calls.saturating_add(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use std::vec::Vec;

    use super::{FaultConfig, FaultSimFs, UnsyncedWritePolicy};
    use crate::{FileSystem, StorageFile, VfsError};

    #[test]
    fn synced_data_and_dir_survive_power_loss_while_unsynced_are_lost() {
        let mut cfg = FaultConfig::new(42);
        cfg.unsynced_write_policy = UnsyncedWritePolicy::DropAll;
        let fs = FaultSimFs::with_config(cfg);

        // Create and sync file A.
        let mut f = fs.open_or_create("wal.log").unwrap();
        f.write_all_at(0, b"committed").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir().unwrap();

        // Append unsynced bytes and create an unsynced file B.
        f.write_all_at(9, b"_unflushed").unwrap();
        drop(f);
        let mut temp = fs.open_or_create("MANIFEST.tmp").unwrap();
        temp.write_all_at(0, b"new_manifest").unwrap();
        temp.sync_data().unwrap();
        drop(temp);
        // Intentionally omit fs.sync_dir()!

        fs.simulate_power_loss_and_reboot().unwrap();

        // Unsynced file disappears; synced file retains only committed bytes.
        assert!(!fs.exists("MANIFEST.tmp").unwrap());
        assert!(fs.exists("wal.log").unwrap());
        let ro = fs.open_read_only("wal.log").unwrap();
        assert_eq!(ro.len().unwrap(), 9);
        let mut buf = [0u8; 9];
        ro.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"committed");
    }

    #[test]
    fn write_budget_triggers_deterministic_torn_write() {
        let run = |seed: u64| -> Vec<u8> {
            let fs = FaultSimFs::new(seed);
            let mut f = fs.open_or_create("wal.log").unwrap();
            f.write_all_at(0, b"HEADER__").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir().unwrap();

            fs.set_write_budget(Some(5)).unwrap();
            let err = f.write_all_at(8, b"PAYLOAD_12345678").unwrap_err();
            assert_eq!(err, VfsError::SimulatedPowerLoss);
            assert!(fs.is_crashed().unwrap());
            drop(f);

            fs.simulate_power_loss_and_reboot().unwrap();
            let ro = fs.open_read_only("wal.log").unwrap();
            let len = ro.len().unwrap() as usize;
            let mut out = vec![0u8; len];
            ro.read_exact_at(0, &mut out).unwrap();
            out
        };

        let first = run(2026);
        let second = run(2026);
        assert_eq!(
            first, second,
            "same seed must reproduce byte-identical torn image"
        );
        assert_eq!(&first[..8], b"HEADER__");
        assert_eq!(first.len(), 13);
        assert_ne!(
            &first[8..13],
            b"PAYLO",
            "tail byte should be torn/corrupted"
        );
    }

    #[test]
    fn windows_handle_semantics_prevent_delete_or_overwrite_of_open_files() {
        let fs = FaultSimFs::new(7);
        let mut sst_old = fs.open_or_create("0001.sst").unwrap();
        sst_old.write_all_at(0, b"old").unwrap();
        sst_old.sync_data().unwrap();
        fs.sync_dir().unwrap();

        let mut sst_new = fs.open_or_create("0002.sst").unwrap();
        sst_new.write_all_at(0, b"new").unwrap();
        sst_new.sync_data().unwrap();
        drop(sst_new);

        // Open handle blocks remove and rename-overwrite.
        assert_eq!(fs.remove("0001.sst"), Err(VfsError::IoFailure));
        assert_eq!(fs.rename("0002.sst", "0001.sst"), Err(VfsError::IoFailure));

        // Dropping the handle allows deferred reclamation.
        drop(sst_old);
        fs.rename("0002.sst", "0001.sst").unwrap();
        fs.sync_dir().unwrap();
        assert!(!fs.exists("0002.sst").unwrap());
    }

    #[test]
    fn exclusive_lock_conflicts_and_resets_after_reboot() {
        let fs = FaultSimFs::new(99);
        let lock = fs.lock_exclusive("LOCK").unwrap();
        assert_eq!(
            fs.lock_exclusive("LOCK").unwrap_err(),
            VfsError::AlreadyLocked
        );

        fs.simulate_power_loss_and_reboot().unwrap();
        // Volatile lock was cleared by reboot; dropping stale guard must not unlock new holder.
        let lock_after_reboot = fs.lock_exclusive("LOCK").unwrap();
        drop(lock);
        assert_eq!(
            fs.lock_exclusive("LOCK").unwrap_err(),
            VfsError::AlreadyLocked
        );
        drop(lock_after_reboot);
        let _lock3 = fs.lock_exclusive("LOCK").unwrap();
    }
}
