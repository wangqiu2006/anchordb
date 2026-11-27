use core::ops::Bound;

use hal_vfs::fault_sim::FaultSimFs;
use kv_core::{KvEngine, KvTransaction, LsmConfig, LsmEngine};
use table_codec::{
    CodecError, ColumnSpec, ColumnType, IndexSpec, SortOrder, SystemCatalog, TableStore, Value,
};

fn test_lsm_config() -> LsmConfig {
    LsmConfig {
        memtable_flush_bytes: 512,
        block_size: 256,
        page_size: 1024,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    }
}

#[test]
fn table_crud_unique_and_composite_desc_index_scans() {
    let fs = FaultSimFs::new(2026);
    let engine = LsmEngine::open(fs.clone(), test_lsm_config()).unwrap();

    // Create `items` table with a composite index (rarity ASC, level DESC) and a UNIQUE `code` index.
    let mut txn = engine.begin_txn(false).unwrap();
    let schema = SystemCatalog::create_table(
        &mut txn,
        "items",
        &[
            ColumnSpec {
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            ColumnSpec {
                name: "code".into(),
                data_type: ColumnType::Text,
                nullable: true,
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

    let schema = SystemCatalog::create_index(
        &mut txn,
        schema.table_id,
        IndexSpec {
            name: "idx_rarity_level_desc".into(),
            unique: false,
            columns: vec![
                ("rarity".into(), SortOrder::Asc),
                ("level".into(), SortOrder::Desc),
            ],
        },
    )
    .unwrap();

    let schema = SystemCatalog::create_index(
        &mut txn,
        schema.table_id,
        IndexSpec {
            name: "uidx_code".into(),
            unique: true,
            columns: vec![("code".into(), SortOrder::Asc)],
        },
    )
    .unwrap();

    // Insert rows including multiple NULLs in the UNIQUE column (permitted by SQL standard).
    TableStore::insert_row(
        &mut txn,
        &schema,
        &[
            Value::Int64(1),
            Value::Text("SWORD_01".into()),
            Value::Int64(4),
            Value::Int64(10),
        ],
    )
    .unwrap();
    TableStore::insert_row(
        &mut txn,
        &schema,
        &[
            Value::Int64(2),
            Value::Text("AXE_01".into()),
            Value::Int64(4),
            Value::Int64(50),
        ],
    )
    .unwrap();
    TableStore::insert_row(
        &mut txn,
        &schema,
        &[
            Value::Int64(3),
            Value::Null,
            Value::Int64(4),
            Value::Int64(30),
        ],
    )
    .unwrap();
    TableStore::insert_row(
        &mut txn,
        &schema,
        &[
            Value::Int64(4),
            Value::Null,
            Value::Int64(2),
            Value::Int64(99),
        ],
    )
    .unwrap();

    // Duplicate non-null UNIQUE key must be rejected.
    let dup = TableStore::insert_row(
        &mut txn,
        &schema,
        &[
            Value::Int64(5),
            Value::Text("SWORD_01".into()),
            Value::Int64(5),
            Value::Int64(1),
        ],
    );
    assert_eq!(
        dup,
        Err(CodecError::DuplicateUniqueKey {
            index_name: "uidx_code".into()
        })
    );

    txn.commit().unwrap();

    // Reboot & verify System Catalog + index scan ordering (rarity=4 ordered by level DESC: 50, 30, 10).
    drop(engine);
    fs.simulate_power_loss_and_reboot().unwrap();
    let reopened = LsmEngine::open(fs, test_lsm_config()).unwrap();

    let rtxn = reopened.begin_txn(true).unwrap();
    let loaded_schema = SystemCatalog::get_table_by_name(&rtxn, "items")
        .unwrap()
        .unwrap();
    assert_eq!(loaded_schema, schema);

    let rarity4 = TableStore::scan_index_prefix(
        &rtxn,
        &loaded_schema,
        "idx_rarity_level_desc",
        &[Value::Int64(4)],
    )
    .unwrap();
    let levels: Vec<Value> = rarity4.iter().map(|r| r[3].clone()).collect();
    assert_eq!(
        levels,
        vec![Value::Int64(50), Value::Int64(30), Value::Int64(10)]
    );

    // Range scan with Excluded/Included bounds on unique non-null index.
    let range = TableStore::scan_index_range(
        &rtxn,
        &loaded_schema,
        "uidx_code",
        Bound::Included(&[Value::Text("AXE_01".into())]),
        Bound::Included(&[Value::Text("SWORD_01".into())]),
    )
    .unwrap();
    assert_eq!(range.len(), 2);
    assert_eq!(range[0][0], Value::Int64(2));
    assert_eq!(range[1][0], Value::Int64(1));
    drop(rtxn);

    // Update row 1: change level from 10 to 80 and code from SWORD_01 to BOW_01.
    let mut utxn = reopened.begin_txn(false).unwrap();
    TableStore::update_row(
        &mut utxn,
        &loaded_schema,
        &[Value::Int64(1)],
        &[
            Value::Int64(1),
            Value::Text("BOW_01".into()),
            Value::Int64(4),
            Value::Int64(80),
        ],
    )
    .unwrap();
    // Old code SWORD_01 is now freed and reusable.
    TableStore::insert_row(
        &mut utxn,
        &loaded_schema,
        &[
            Value::Int64(5),
            Value::Text("SWORD_01".into()),
            Value::Int64(5),
            Value::Int64(1),
        ],
    )
    .unwrap();
    utxn.commit().unwrap();

    let vtxn = reopened.begin_txn(true).unwrap();
    let rarity4_after = TableStore::scan_index_prefix(
        &vtxn,
        &loaded_schema,
        "idx_rarity_level_desc",
        &[Value::Int64(4)],
    )
    .unwrap();
    let levels_after: Vec<Value> = rarity4_after.iter().map(|r| r[3].clone()).collect();
    assert_eq!(
        levels_after,
        vec![Value::Int64(80), Value::Int64(50), Value::Int64(30)]
    );
}

#[test]
fn online_schema_evolution_and_index_backfill_preserve_history() {
    let fs = FaultSimFs::new(3030);
    let engine = LsmEngine::open(fs.clone(), test_lsm_config()).unwrap();

    // v1: (id, name, temp_note)
    let mut txn = engine.begin_txn(false).unwrap();
    let s1 = SystemCatalog::create_table(
        &mut txn,
        "players",
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
                name: "temp_note".into(),
                data_type: ColumnType::Text,
                nullable: true,
                default_value: None,
            },
        ],
        &["id"],
    )
    .unwrap();

    TableStore::insert_row(
        &mut txn,
        &s1,
        &[
            Value::Int64(10),
            Value::Text("Alice".into()),
            Value::Text("old_secret".into()),
        ],
    )
    .unwrap();
    TableStore::insert_row(
        &mut txn,
        &s1,
        &[Value::Int64(20), Value::Text("Bob".into()), Value::Null],
    )
    .unwrap();
    txn.commit().unwrap();

    // v2: DROP `temp_note`, v3: RE-ADD `temp_note` with default "fresh", v4: ADD `rank` default 100.
    let mut ddl = engine.begin_txn(false).unwrap();
    let s2 = SystemCatalog::drop_column(&mut ddl, s1.table_id, "temp_note").unwrap();
    assert_eq!(s2.version, 2);

    let s3 = SystemCatalog::add_column(
        &mut ddl,
        s1.table_id,
        ColumnSpec {
            name: "temp_note".into(),
            data_type: ColumnType::Text,
            nullable: false,
            default_value: Some(Value::Text("fresh".into())),
        },
    )
    .unwrap();
    assert_eq!(s3.version, 3);

    let s4 = SystemCatalog::add_column(
        &mut ddl,
        s1.table_id,
        ColumnSpec {
            name: "rank".into(),
            data_type: ColumnType::Int64,
            nullable: false,
            default_value: Some(Value::Int64(100)),
        },
    )
    .unwrap();
    assert_eq!(s4.version, 4);

    // v5: Backfill a new index on the newly evolved `rank` column + `name`.
    let s5 = SystemCatalog::create_index(
        &mut ddl,
        s1.table_id,
        IndexSpec {
            name: "idx_rank_name".into(),
            unique: false,
            columns: vec![
                ("rank".into(), SortOrder::Asc),
                ("name".into(), SortOrder::Asc),
            ],
        },
    )
    .unwrap();
    assert_eq!(s5.version, 5);
    ddl.commit().unwrap();

    // Verify all historical schemas v1..=v5 remain persisted in System Catalog.
    let rtxn = engine.begin_txn(true).unwrap();
    for ver in 1..=5 {
        let hist = SystemCatalog::get_table_version(&rtxn, s1.table_id, ver)
            .unwrap()
            .unwrap();
        assert_eq!(hist.version, ver);
    }

    // Old v1 rows decode seamlessly onto v5 schema without leaking dropped `old_secret`.
    let alice = TableStore::get_row_by_pk(&rtxn, &s5, &[Value::Int64(10)])
        .unwrap()
        .unwrap();
    assert_eq!(
        alice,
        vec![
            Value::Int64(10),
            Value::Text("Alice".into()),
            Value::Text("fresh".into()),
            Value::Int64(100),
        ]
    );

    // Backfilled index over the newly added `rank` column finds both historical rows in order!
    let by_rank =
        TableStore::scan_index_prefix(&rtxn, &s5, "idx_rank_name", &[Value::Int64(100)]).unwrap();
    assert_eq!(by_rank.len(), 2);
    assert_eq!(by_rank[0][1], Value::Text("Alice".into()));
    assert_eq!(by_rank[1][1], Value::Text("Bob".into()));

    // Attempting to drop a column referenced by an active index must fail.
    drop(rtxn);
    let mut bad_ddl = engine.begin_txn(false).unwrap();
    assert!(matches!(
        SystemCatalog::drop_column(&mut bad_ddl, s1.table_id, "rank"),
        Err(CodecError::InvalidSchema(_))
    ));
    bad_ddl.abort();
}
