//! Hermitage-style Snapshot Isolation (SI) anomaly verification suite for `LsmEngine`.

use std::ops::Bound;
use std::sync::Arc;
use std::thread;

use hal_vfs::fault_sim::FaultSimFs;
use kv_core::{
    KvEngine, KvError, KvIterator, KvTransaction, KvTransactionExt, LsmConfig, LsmEngine,
    inspect_database,
};

fn seed_engine(seed: u64) -> Result<(FaultSimFs, LsmEngine<FaultSimFs>), KvError> {
    let fs = FaultSimFs::new(seed);
    let engine = LsmEngine::open(fs.clone(), LsmConfig::default())?;
    let mut init = engine.begin_txn(false)?;
    init.put(b"k1", b"10")?;
    init.put(b"k2", b"20")?;
    init.commit()?;
    Ok((fs, engine))
}

#[test]
fn hermitage_g1a_g1b_prevent_dirty_and_intermediate_reads() {
    let (_fs, engine) = seed_engine(101).unwrap();

    let mut t1 = engine.begin_txn(false).unwrap();
    t1.put(b"k1", b"101").unwrap();

    // G1a/G1b: concurrent reader must not see uncommitted or intermediate values.
    let r = engine.begin_txn(true).unwrap();
    assert_eq!(r.get(b"k1").unwrap(), Some(b"10".to_vec()));

    // Intermediate write inside T1 before final value.
    t1.put(b"k1", b"202").unwrap();
    assert_eq!(r.get(b"k1").unwrap(), Some(b"10".to_vec()));
    t1.abort();

    let r2 = engine.begin_txn(true).unwrap();
    assert_eq!(r2.get(b"k1").unwrap(), Some(b"10".to_vec()));
}

#[test]
fn hermitage_p4_lost_update_is_prevented_by_first_committer_wins() {
    let (_fs, engine) = seed_engine(102).unwrap();

    let mut t1 = engine.begin_txn(false).unwrap();
    let mut t2 = engine.begin_txn(false).unwrap();

    // Both read k1 = 10 and attempt concurrent increment.
    assert_eq!(t1.get(b"k1").unwrap(), Some(b"10".to_vec()));
    assert_eq!(t2.get(b"k1").unwrap(), Some(b"10".to_vec()));

    t1.put(b"k1", b"11").unwrap();
    t2.put(b"k1", b"12").unwrap();

    // First committer wins; second committer MUST fail with WriteConflict (no lost update).
    t1.commit().unwrap();
    assert_eq!(t2.commit(), Err(KvError::WriteConflict));

    // History is automatically GC'd once no active write transactions remain.
    assert_eq!(engine.tracked_commit_history_len().unwrap(), 0);

    let check = engine.begin_txn(true).unwrap();
    assert_eq!(check.get(b"k1").unwrap(), Some(b"11".to_vec()));
}

#[test]
fn hermitage_pmp_repeatable_read_and_phantom_prevention_within_snapshot() {
    let (_fs, engine) = seed_engine(103).unwrap();

    let reader = engine.begin_txn(true).unwrap();
    let mut it1 = reader.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    let mut before = Vec::new();
    while let Some((k, v)) = it1.next_entry().unwrap() {
        before.push((k.to_vec(), v.to_vec()));
    }

    // Concurrent writer modifies k1 and inserts a new phantom key k3.
    let mut writer = engine.begin_txn(false).unwrap();
    writer.put(b"k1", b"999").unwrap();
    writer.put(b"k3", b"30").unwrap();
    writer.commit().unwrap();

    // Reader's second scan inside the same snapshot must see identical rows (no phantoms).
    let mut it2 = reader.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    let mut after = Vec::new();
    while let Some((k, v)) = it2.next_entry().unwrap() {
        after.push((k.to_vec(), v.to_vec()));
    }
    assert_eq!(before, after);
}

#[test]
fn hermitage_disjoint_concurrent_writes_both_commit_under_si() {
    let (_fs, engine) = seed_engine(104).unwrap();

    let mut t1 = engine.begin_txn(false).unwrap();
    let mut t2 = engine.begin_txn(false).unwrap();

    // Disjoint keys (k1 vs k2) do not conflict under Snapshot Isolation.
    t1.put(b"k1", b"111").unwrap();
    t2.put(b"k2", b"222").unwrap();

    t1.commit().unwrap();
    t2.commit().unwrap();

    let r = engine.begin_txn(true).unwrap();
    assert_eq!(r.get(b"k1").unwrap(), Some(b"111".to_vec()));
    assert_eq!(r.get(b"k2").unwrap(), Some(b"222".to_vec()));
    assert_eq!(engine.tracked_commit_history_len().unwrap(), 0);
}

#[test]
fn multithreaded_counter_cas_retry_loop_never_loses_updates() {
    let fs = FaultSimFs::new(105);
    let engine = Arc::new(LsmEngine::open(fs, LsmConfig::default()).unwrap());

    {
        let mut init = engine.begin_txn(false).unwrap();
        init.put(b"counter", &0u64.to_le_bytes()).unwrap();
        init.commit().unwrap();
    }

    let threads = 4usize;
    let increments_per_thread = 25u64;
    let mut handles = Vec::new();

    for _ in 0..threads {
        let eng = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for _ in 0..increments_per_thread {
                loop {
                    let mut txn = eng.begin_txn(false).unwrap();
                    let raw = txn.get(b"counter").unwrap().unwrap();
                    let cur = u64::from_le_bytes(raw.try_into().unwrap());
                    txn.put(b"counter", &(cur + 1).to_le_bytes()).unwrap();
                    match txn.commit() {
                        Ok(()) => break,
                        Err(KvError::WriteConflict) => continue,
                        Err(err) => panic!("unexpected error: {err}"),
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let r = engine.begin_txn(true).unwrap();
    let final_raw = r.get(b"counter").unwrap().unwrap();
    let final_val = u64::from_le_bytes(final_raw.try_into().unwrap());
    assert_eq!(final_val, (threads as u64) * increments_per_thread);
    assert_eq!(engine.tracked_commit_history_len().unwrap(), 0);
}

#[test]
fn doctor_inspect_verifies_manifest_sstables_and_wal() {
    let (fs, engine) = seed_engine(106).unwrap();
    engine.checkpoint().unwrap();
    let mut t = engine.begin_txn(false).unwrap();
    t.put(b"k3", b"30").unwrap();
    t.commit().unwrap();
    drop(engine);

    let report = inspect_database(&fs).unwrap();
    assert_eq!(report.format_version, 1);
    assert_eq!(report.sst_count, 1);
    assert_eq!(report.total_sst_entries, 2);
    assert_eq!(report.wal_records, 1);
    assert!(!report.wal_had_torn_tail);
}
