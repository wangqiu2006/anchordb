use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use hal_vfs::fault_sim::FaultSimFs;
use hal_vfs::std_fs::StdFileSystem;
use hal_vfs::{FileSystem, StorageFile, VfsError};
use kv_core::{
    FormatError, KvEngine, KvError, KvIterator, KvTransaction, KvTransactionExt, LsmConfig,
    LsmEngine,
};

struct TestRng(u64);

impl TestRng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % bound
    }
}

type OracleBatch = Vec<(Vec<u8>, Option<Vec<u8>>)>;

fn dump_all<F: FileSystem>(engine: &LsmEngine<F>) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, KvError> {
    let txn = engine.begin_txn(true)?;
    let mut iter = txn.scan(Bound::Unbounded, Bound::Unbounded)?;
    let mut out = BTreeMap::new();
    while let Some((k, v)) = iter.next_entry()? {
        out.insert(k.to_vec(), v.to_vec());
    }
    Ok(out)
}

fn apply_oracle(oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>, ops: &[(Vec<u8>, Option<Vec<u8>>)]) {
    for (k, v) in ops {
        match v {
            Some(val) => {
                oracle.insert(k.clone(), val.clone());
            }
            None => {
                oracle.remove(k);
            }
        }
    }
}

#[test]
fn crash_recovery_1000_random_seeds() {
    let cfg = LsmConfig {
        memtable_flush_bytes: 384,
        block_size: 256,
        page_size: 1024,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    };

    for seed in 1..=1000u64 {
        let mut rng = TestRng::new(seed ^ 0xA5A5_5A5A_1234_5678);
        let fs = FaultSimFs::new(seed);
        let engine = LsmEngine::open(fs.clone(), cfg).unwrap();

        let mut committed = BTreeMap::new();
        let mut in_flight: Option<OracleBatch> = None;

        // Run a random workload of multi-key puts, deletes, aborts, and checkpoints.
        let rounds = 8 + rng.next_usize(12);
        let crash_round = rng.next_usize(rounds);

        for round in 0..rounds {
            if round == crash_round {
                // Arm a deterministic byte write budget so power cuts mid-WAL / mid-Flush / mid-Manifest.
                let budget = rng.next_usize(700);
                fs.set_write_budget(Some(budget)).unwrap();
            }

            let action = rng.next_usize(10);
            if action == 0 {
                // Explicit abort must never leak into durable state.
                let mut txn = engine.begin_txn(false).unwrap();
                let k = format!("aborted-{seed}-{round}").into_bytes();
                txn.put(&k, b"never_visible").unwrap();
                txn.abort();
                continue;
            }
            if action == 1 {
                if let Err(KvError::Vfs(VfsError::SimulatedPowerLoss)) = engine.checkpoint() {
                    break;
                }
                continue;
            }

            let mut txn = engine.begin_txn(false).unwrap();
            let op_count = 1 + rng.next_usize(4);
            let mut batch_ops = Vec::with_capacity(op_count);
            for _ in 0..op_count {
                let key_id = rng.next_usize(24);
                let key = format!("k{key_id:02}").into_bytes();
                if rng.next_usize(4) == 0 {
                    txn.delete(&key).unwrap();
                    batch_ops.push((key, None));
                } else {
                    let val_len = 8 + rng.next_usize(48);
                    let val =
                        format!("s{seed}-r{round}-k{key_id}-{}", "x".repeat(val_len)).into_bytes();
                    txn.put(&key, &val).unwrap();
                    batch_ops.push((key, Some(val)));
                }
            }

            match txn.commit() {
                Ok(()) => {
                    apply_oracle(&mut committed, &batch_ops);
                }
                Err(KvError::Vfs(VfsError::SimulatedPowerLoss)) => {
                    in_flight = Some(batch_ops);
                    break;
                }
                Err(other) => {
                    panic!("unexpected commit error on seed {seed}, round {round}: {other}");
                }
            }
        }

        // Pull the power plug and reboot!
        drop(engine);
        fs.simulate_power_loss_and_reboot().unwrap();

        let recovered = LsmEngine::open(fs.clone(), cfg).unwrap();
        let actual = dump_all(&recovered).unwrap();

        let mut committed_plus_inflight = committed.clone();
        if let Some(ops) = &in_flight {
            apply_oracle(&mut committed_plus_inflight, ops);
        }

        assert!(
            actual == committed || actual == committed_plus_inflight,
            "seed {seed}: recovered state violated atomicity/durability!\nactual={actual:?}\ncommitted={committed:?}\nwith_inflight={committed_plus_inflight:?}"
        );

        // Verify the recovered engine remains fully writable and survives another reboot.
        let mut expected_after = actual;
        let mut post_txn = recovered.begin_txn(false).unwrap();
        let sentinel_key = format!("post-{seed}").into_bytes();
        let sentinel_val = b"survived_second_boot".to_vec();
        post_txn.put(&sentinel_key, &sentinel_val).unwrap();
        post_txn.commit().unwrap();
        expected_after.insert(sentinel_key, sentinel_val);

        drop(recovered);
        fs.simulate_power_loss_and_reboot().unwrap();

        let reopened = LsmEngine::open(fs, cfg).unwrap();
        assert_eq!(
            dump_all(&reopened).unwrap(),
            expected_after,
            "seed {seed} post-recovery"
        );
    }
}

#[test]
fn scan_and_point_lookup_interleaved_within_same_txn() {
    let fs = FaultSimFs::new(77);
    let engine = LsmEngine::open(fs, LsmConfig::default()).unwrap();

    let mut w = engine.begin_txn(false).unwrap();
    w.put(b"idx:01", b"pk:100").unwrap();
    w.put(b"idx:02", b"pk:200").unwrap();
    w.put(b"pk:100", b"row_alice").unwrap();
    w.put(b"pk:200", b"row_bob").unwrap();
    // Verify Read-Your-Own-Writes + interleaved scan & point lookup via `&self`.
    {
        let mut idx_iter = w
            .scan(Bound::Included(b"idx:00"), Bound::Excluded(b"idx:99"))
            .unwrap();
        let mut rows = Vec::new();
        while let Some((_idx_key, pk)) = idx_iter.next_entry().unwrap() {
            let row = w.get(pk).unwrap().unwrap();
            rows.push(row);
        }
        assert_eq!(rows, vec![b"row_alice".to_vec(), b"row_bob".to_vec()]);
    }
    w.commit().unwrap();
}

#[test]
fn concurrent_snapshot_with_compaction_deferred_reclamation_on_windows_semantics() {
    let fs = FaultSimFs::new(88);
    let cfg = LsmConfig {
        memtable_flush_bytes: 128,
        block_size: 256,
        page_size: 1024,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    };
    let engine = LsmEngine::open(fs, cfg).unwrap();

    // Create initial SSTable.
    let mut t1 = engine.begin_txn(false).unwrap();
    for i in 0..8 {
        t1.put(format!("k{i:02}").as_bytes(), b"version_1_padding_data")
            .unwrap();
    }
    t1.commit().unwrap();
    engine.checkpoint().unwrap();
    assert_eq!(engine.active_table_count().unwrap(), 1);

    // Hold a long-lived read snapshot pinning the old SSTable handle.
    let reader_snap = engine.begin_txn(true).unwrap();

    // Overwrite and flush enough times to trigger automatic Compaction while reader is active.
    for round in 2..=5 {
        let mut w = engine.begin_txn(false).unwrap();
        for i in 0..8 {
            w.put(
                format!("k{i:02}").as_bytes(),
                format!("version_{round}_padding_data").as_bytes(),
            )
            .unwrap();
        }
        w.commit().unwrap();
        engine.checkpoint().unwrap();
    }
    engine.compact_all().unwrap();

    // Old SSTable pinned by reader_snap must still be deferred (never fail with Windows handle error).
    assert!(engine.pending_reclaim_count().unwrap() >= 1);
    assert_eq!(
        reader_snap.get(b"k00").unwrap().unwrap(),
        b"version_1_padding_data"
    );

    // Once snapshot is dropped, next maintenance pass reclaims the pinned SSTable cleanly.
    drop(reader_snap);
    let mut trigger = engine.begin_txn(false).unwrap();
    trigger.put(b"cleanup", b"ok").unwrap();
    trigger.commit().unwrap();
    assert_eq!(engine.pending_reclaim_count().unwrap(), 0);
}

#[test]
fn mid_file_wal_bit_rot_is_rejected_on_open() {
    let fs = FaultSimFs::new(55);
    let cfg = LsmConfig {
        memtable_flush_bytes: 1024 * 1024,
        ..LsmConfig::default()
    };
    let engine = LsmEngine::open(fs.clone(), cfg).unwrap();

    let mut t1 = engine.begin_txn(false).unwrap();
    t1.put(b"account_a", b"1000").unwrap();
    t1.commit().unwrap();

    let mut t2 = engine.begin_txn(false).unwrap();
    t2.put(b"account_b", b"2000").unwrap();
    t2.commit().unwrap();
    drop(engine);

    // Corrupt a byte inside the first WAL record while leaving the second record intact.
    let mut wal = fs.open_or_create("wal-000002.log").unwrap();
    let mut b = [0u8; 1];
    wal.read_exact_at(64 + 34, &mut b).unwrap();
    b[0] ^= 0x10;
    wal.write_all_at(64 + 34, &b).unwrap();
    wal.sync_data().unwrap();
    drop(wal);

    let err = match LsmEngine::open(fs, cfg) {
        Ok(_) => panic!("mid-file WAL corruption must not open silently"),
        Err(err) => err,
    };
    assert_eq!(err, KvError::Format(FormatError::ChecksumMismatch));
}

#[test]
fn std_fs_real_disk_persistence_flush_compaction_and_reopen() {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir: PathBuf =
        std::env::temp_dir().join(format!("anchordb_lsm_std_{}_{}", std::process::id(), id));

    let cfg = LsmConfig {
        memtable_flush_bytes: 256,
        block_size: 256,
        page_size: 4096,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    };

    {
        let fs = StdFileSystem::open(&dir).unwrap();
        let engine = LsmEngine::open(fs, cfg).unwrap();

        // Exclusive lock must block a second open on the same directory.
        let fs_dup = StdFileSystem::open(&dir).unwrap();
        assert!(matches!(
            LsmEngine::open(fs_dup, cfg),
            Err(KvError::Vfs(VfsError::AlreadyLocked))
        ));

        for i in 0..40u32 {
            let mut txn = engine.begin_txn(false).unwrap();
            txn.put(
                format!("user:{i:03}").as_bytes(),
                format!("profile_payload_{i:03}").as_bytes(),
            )
            .unwrap();
            if i >= 5 && i % 5 == 0 {
                txn.delete(format!("user:{:03}", i - 5).as_bytes()).unwrap();
            }
            txn.commit().unwrap();
        }
        engine.compact_all().unwrap();
    }

    // Reopen from disk and validate exact contents and bounded range scan.
    {
        let fs = StdFileSystem::open(&dir).unwrap();
        let engine = LsmEngine::open(fs, cfg).unwrap();
        let txn = engine.begin_txn(true).unwrap();

        assert_eq!(txn.get(b"user:000").unwrap(), None);
        assert_eq!(
            txn.get(b"user:001").unwrap(),
            Some(b"profile_payload_001".to_vec())
        );

        let mut iter = txn
            .scan(Bound::Excluded(b"user:009"), Bound::Included(b"user:015"))
            .unwrap();
        let mut keys = Vec::new();
        while let Some((k, _)) = iter.next_entry().unwrap() {
            keys.push(String::from_utf8(k.to_vec()).unwrap());
        }
        // user:010 was deleted when i=15; user:015 was deleted when i=20.
        assert_eq!(keys, vec!["user:011", "user:012", "user:013", "user:014"]);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
