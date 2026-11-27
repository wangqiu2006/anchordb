//! Criterion performance benchmark baseline for `kv_core` (`LsmEngine` + `SST2` Bloom Filter).
//!
//! Run with:
//! `cargo bench -p kv_core`

use std::hint::black_box;
use std::ops::Bound;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use hal_vfs::fault_sim::FaultSimFs;
use kv_core::{KvEngine, KvIterator, KvTransaction, LsmConfig, LsmEngine};

fn build_populated_engine(num_keys: u32) -> Result<LsmEngine<FaultSimFs>, kv_core::KvError> {
    let fs = FaultSimFs::new(42);
    let cfg = LsmConfig {
        memtable_flush_bytes: 32 * 1024,
        block_size: 4096,
        page_size: 4096,
        compaction_trigger_tables: 8,
        sync_on_commit: false,
    };
    let engine = LsmEngine::open(fs, cfg)?;

    // Insert even keys in 200-key batches so multiple SSTables are flushed.
    let mut i = 0u32;
    while i < num_keys {
        let mut txn = engine.begin_txn(false)?;
        let end = (i + 200).min(num_keys);
        for k in i..end {
            let key = format!("key-{:08}", k * 2);
            let val = format!("value-payload-{:08}-anchor-db-bench-data", k * 2);
            txn.put(key.as_bytes(), val.as_bytes())?;
        }
        txn.commit()?;
        i = end;
    }
    engine.checkpoint()?;
    Ok(engine)
}

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_write");
    group.throughput(Throughput::Elements(100));

    group.bench_function("batch_100_put_commit_sync_false", |b| {
        b.iter_batched(
            || {
                let fs = FaultSimFs::new(1001);
                let cfg = LsmConfig {
                    memtable_flush_bytes: 256 * 1024,
                    block_size: 4096,
                    page_size: 4096,
                    compaction_trigger_tables: 4,
                    sync_on_commit: false,
                };
                LsmEngine::open(fs, cfg)
            },
            |engine_res| {
                if let Ok(engine) = engine_res
                    && let Ok(mut txn) = engine.begin_txn(false)
                {
                    for i in 0..100u32 {
                        let k = format!("k-{i:06}");
                        let v = format!("v-{i:06}-payload-bytes");
                        let _ = txn.put(k.as_bytes(), v.as_bytes());
                    }
                    let _ = black_box(txn.commit());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_put_commit_sync_true", |b| {
        let fs = FaultSimFs::new(1002);
        let cfg = LsmConfig {
            memtable_flush_bytes: 1024 * 1024,
            block_size: 4096,
            page_size: 4096,
            compaction_trigger_tables: 8,
            sync_on_commit: true,
        };
        let Ok(engine) = LsmEngine::open(fs, cfg) else {
            return;
        };
        let mut seq = 0u64;
        b.iter(|| {
            seq = seq.wrapping_add(1);
            if let Ok(mut txn) = engine.begin_txn(false) {
                let k = format!("w-{seq:010}");
                let _ = txn.put(k.as_bytes(), b"sync-commit-payload-32-bytes!!!!");
                let _ = black_box(txn.commit());
            }
        });
    });

    group.finish();
}

fn bench_point_lookup(c: &mut Criterion) {
    let Ok(engine) = build_populated_engine(4_000) else {
        return;
    };

    let mut group = c.benchmark_group("kv_point_lookup_sst2_bloom");
    group.throughput(Throughput::Elements(1));

    let present_keys: Vec<Vec<u8>> = (0..1_000u32)
        .map(|i| format!("key-{:08}", (i * 4) * 2).into_bytes())
        .collect();
    let absent_keys: Vec<Vec<u8>> = (0..1_000u32)
        .map(|i| format!("key-{:08}", (i * 4) * 2 + 1).into_bytes())
        .collect();

    let mut hit_idx = 0usize;
    let mut read_buf = [0u8; 128];
    group.bench_function("sst_get_hit_4k_keys", |b| {
        b.iter(|| {
            let key = &present_keys[hit_idx % present_keys.len()];
            hit_idx = hit_idx.wrapping_add(1);
            if let Ok(txn) = engine.begin_txn(true) {
                let _ = black_box(txn.get_into(key, &mut read_buf));
            }
        });
    });

    let mut miss_idx = 0usize;
    group.bench_function("sst_get_miss_bloom_filtered_4k_keys", |b| {
        b.iter(|| {
            let key = &absent_keys[miss_idx % absent_keys.len()];
            miss_idx = miss_idx.wrapping_add(1);
            if let Ok(txn) = engine.begin_txn(true) {
                let _ = black_box(txn.get_into(key, &mut read_buf));
            }
        });
    });

    group.finish();
}

fn bench_range_scan(c: &mut Criterion) {
    let Ok(engine) = build_populated_engine(4_000) else {
        return;
    };

    let mut group = c.benchmark_group("kv_range_scan");
    group.throughput(Throughput::Elements(100));

    group.bench_function("scan_100_ordered_keys", |b| {
        b.iter(|| {
            if let Ok(txn) = engine.begin_txn(true)
                && let Ok(mut iter) = txn.scan(
                    Bound::Included(b"key-00001000".as_slice()),
                    Bound::Excluded(b"key-00001200".as_slice()),
                )
            {
                let mut count = 0usize;
                while let Ok(Some((k, v))) = iter.next_entry() {
                    count += k.len() + v.len();
                }
                black_box(count);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_point_lookup,
    bench_range_scan
);
criterion_main!(benches);
