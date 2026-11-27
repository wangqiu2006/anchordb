//! End-to-end embedded desktop/game inventory workflow testing:
//! - Fluent `AnchorDb` API (`db.table("items").filter(...).order_by(...).limit(...)`)
//! - Automatic Primary Key & Secondary Index selection (`explain()`)
//! - Per-query memory budget enforcement
//! - Online schema evolution + power-loss reboot + `AnchorDb::doctor()` verification

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anchordb::hal_vfs::fault_sim::FaultSimFs;
use anchordb::kv_core::LsmConfig;
use anchordb::{
    AccessPath, AnchorDb, ColumnSpec, ColumnType, DbError, IndexSpec, QueryConfig, QueryError,
    SortOrder, Value, col,
};

#[test]
fn game_inventory_embedded_workflow_index_selection_and_crash_recovery() {
    let fs = FaultSimFs::new(20261127);
    let lsm_cfg = LsmConfig {
        memtable_flush_bytes: 512,
        block_size: 256,
        page_size: 1024,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    };
    let db = AnchorDb::open_fs(fs.clone(), lsm_cfg, QueryConfig::default()).unwrap();

    db.create_table(
        "items",
        &[
            ColumnSpec {
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            ColumnSpec {
                name: "name".into(),
                data_type: ColumnType::Text,
                nullable: false,
                default_value: None,
            },
            ColumnSpec {
                name: "rarity".into(),
                data_type: ColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            ColumnSpec {
                name: "level".into(),
                data_type: ColumnType::Int64,
                nullable: false,
                default_value: None,
            },
        ],
        &["id"],
    )
    .unwrap();

    db.create_index(
        "items",
        IndexSpec {
            name: "idx_rarity_level".into(),
            unique: false,
            columns: vec![
                ("rarity".into(), SortOrder::Asc),
                ("level".into(), SortOrder::Desc),
            ],
        },
    )
    .unwrap();

    // Populate inventory inside a single atomic transaction.
    let mut txn = db.begin_txn(false).unwrap();
    let sample = [
        (1, "Wooden Sword", 1, 5),
        (2, "Iron Blade", 2, 18),
        (3, "Dragon Greatsword", 5, 80),
        (4, "Phoenix Staff", 4, 65),
        (5, "Frost Bow", 4, 40),
        (6, "Shadow Dagger", 5, 95),
    ];
    for (id, name, rarity, level) in sample {
        txn.insert(
            "items",
            &[
                Value::Int64(id),
                Value::Text(name.into()),
                Value::Int64(rarity),
                Value::Int64(level),
            ],
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // 1. Primary-key query must choose PrimaryKeyLookup.
    let pk_q = db.table("items").filter(col("id").eq(3));
    assert!(matches!(
        pk_q.explain().unwrap().access_path,
        AccessPath::PrimaryKeyLookup { .. }
    ));
    let pk_rows = pk_q.collect().unwrap();
    assert_eq!(pk_rows.len(), 1);
    assert_eq!(pk_rows[0].get_str("name"), Some("Dragon Greatsword"));

    // 2. Range query on leading index column (`rarity >= 4`) must choose IndexRangeScan.
    let top_epic = db
        .table("items")
        .select(&["name", "rarity", "level"])
        .filter(col("rarity").gte(4))
        .order_by("level", SortOrder::Desc)
        .limit(3);
    assert!(matches!(
        top_epic.explain().unwrap().access_path,
        AccessPath::IndexRangeScan { ref index_name, .. } if index_name == "idx_rarity_level"
    ));

    let rows = top_epic.collect().unwrap();
    let names: Vec<Option<&str>> = rows.iter().map(|r| r.get_str("name")).collect();
    assert_eq!(
        names,
        vec![
            Some("Shadow Dagger"),
            Some("Dragon Greatsword"),
            Some("Phoenix Staff")
        ]
    );

    // 3. Online schema evolution: add `bound` column with frozen default `false`.
    db.add_column(
        "items",
        ColumnSpec {
            name: "bound".into(),
            data_type: ColumnType::Bool,
            nullable: false,
            default_value: Some(Value::Bool(false)),
        },
    )
    .unwrap();

    // 4. Simulate power cut and reboot; verify evolved column & query results persist.
    drop(db);
    fs.simulate_power_loss_and_reboot().unwrap();

    let reopened = AnchorDb::open_fs(fs, lsm_cfg, QueryConfig::default()).unwrap();
    let mythic = reopened
        .table("items")
        .filter(col("rarity").eq(5))
        .order_by("level", SortOrder::Desc)
        .collect()
        .unwrap();
    assert_eq!(mythic.len(), 2);
    assert_eq!(mythic[0].get_str("name"), Some("Shadow Dagger"));
    assert_eq!(mythic[0].get_bool("bound"), Some(false));
}

#[test]
fn query_memory_budget_guards_against_unbounded_materialization() {
    let fs = FaultSimFs::new(777);
    let tiny_budget = QueryConfig {
        memory_budget_bytes: 128,
        chunk_rows: 64,
    };
    let db = AnchorDb::open_fs(fs, LsmConfig::default(), tiny_budget).unwrap();
    db.create_table(
        "logs",
        &[
            ColumnSpec {
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            ColumnSpec {
                name: "body".into(),
                data_type: ColumnType::Text,
                nullable: false,
                default_value: None,
            },
        ],
        &["id"],
    )
    .unwrap();

    let mut txn = db.begin_txn(false).unwrap();
    for i in 0..10 {
        txn.insert("logs", &[Value::Int64(i), Value::Text("x".repeat(64))])
            .unwrap();
    }
    txn.commit().unwrap();

    let err = db.table("logs").collect().unwrap_err();
    assert!(matches!(
        err,
        DbError::Query(QueryError::MemoryBudgetExceeded { limit: 128, .. })
    ));
}

#[test]
fn doctor_on_real_disk_directory_passes_after_commits_and_checkpoint() {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir: PathBuf =
        std::env::temp_dir().join(format!("anchordb_doctor_e2e_{}_{}", std::process::id(), id));

    {
        let db = AnchorDb::open(&dir).unwrap();
        db.create_table(
            "kv_settings",
            &[
                ColumnSpec {
                    name: "k".into(),
                    data_type: ColumnType::Text,
                    nullable: false,
                    default_value: None,
                },
                ColumnSpec {
                    name: "v".into(),
                    data_type: ColumnType::Text,
                    nullable: false,
                    default_value: None,
                },
            ],
            &["k"],
        )
        .unwrap();
        db.insert(
            "kv_settings",
            &[Value::Text("theme".into()), Value::Text("dark".into())],
        )
        .unwrap();
        db.checkpoint().unwrap();
        db.insert(
            "kv_settings",
            &[Value::Text("lang".into()), Value::Text("zh-CN".into())],
        )
        .unwrap();
    }

    let report = AnchorDb::doctor(&dir).unwrap();
    assert_eq!(report.format_version, 1);
    assert_eq!(report.sst_count, 1);
    assert!(report.total_sst_entries >= 4);
    assert_eq!(report.wal_records, 1);
    assert!(!report.wal_had_torn_tail);

    let _ = std::fs::remove_dir_all(&dir);
}
