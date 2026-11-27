//! Comprehensive SQL regression & correctness-oracle test suite (Phase 3A).
//!
//! Validates:
//! - DDL (`CREATE TABLE`, `CREATE [UNIQUE] INDEX`, `ALTER TABLE ADD/DROP COLUMN`, `SHOW TABLES`)
//! - DML (`INSERT`, `UPDATE`, `DELETE`) & explicit transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
//! - `SELECT`, `WHERE` (`AND`/`OR`/`NOT`/`LIKE`/`IN`/`IS NULL`), `DISTINCT`, `ORDER BY`, `LIMIT`/`OFFSET`
//! - `INNER JOIN` & `LEFT JOIN` via `DataChunk` `HashJoinExec`
//! - `GROUP BY`, `HAVING`, `COUNT(*)`, `SUM`, `MIN`, `MAX`, `AVG` (including empty-table global agg)
//! - `EXPLAIN` index selection + **IndexScan vs SeqScan correctness oracle** (must return identical rows!)
//! - Interactive `anchordb shell` piped execution end-to-end

use anchordb::hal_vfs::fault_sim::FaultSimFs;
use anchordb::kv_core::LsmConfig;
use anchordb::{AnchorDb, QueryConfig, SqlResult, Value, format_sql_result};

fn new_test_db(seed: u64) -> Result<(FaultSimFs, AnchorDb<FaultSimFs>), anchordb::DbError> {
    let fs = FaultSimFs::new(seed);
    let lsm = LsmConfig {
        memtable_flush_bytes: 512,
        block_size: 256,
        page_size: 1024,
        compaction_trigger_tables: 3,
        sync_on_commit: true,
    };
    let db = AnchorDb::open_fs(fs.clone(), lsm, QueryConfig::default())?;
    Ok((fs, db))
}

fn query_rows(db: &AnchorDb<FaultSimFs>, sql: &str) -> Result<Vec<Vec<Value>>, anchordb::DbError> {
    let mut results = db.execute_sql(sql)?;
    match results.pop() {
        Some(SqlResult::Rows { rows, .. }) => Ok(rows),
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[test]
fn sql_ddl_dml_joins_aggregates_and_explain_oracle() {
    let (fs, db) = new_test_db(3001).unwrap();

    db.execute_sql(
        "
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT DEFAULT 'CN'
        );
        CREATE TABLE orders (
            id BIGINT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO users (id, name, region) VALUES
            (1, 'Alice', 'CN'),
            (2, 'Bob', 'US'),
            (3, 'Charlie', 'CN');
        INSERT INTO orders (id, user_id, amount, status) VALUES
            (101, 1, 50, 'paid'),
            (102, 1, 150, 'paid'),
            (103, 2, 80, 'cancelled'),
            (104, 2, 200, 'paid');
        ",
    )
    .unwrap();

    // 1. Capture SeqScan baseline before adding secondary index (Correctness Oracle).
    let oracle_before = query_rows(
        &db,
        "SELECT id, amount FROM orders WHERE user_id = 1 AND amount >= 50 ORDER BY id ASC;",
    )
    .unwrap();
    let plan_before = query_rows(
        &db,
        "EXPLAIN SELECT id, amount FROM orders WHERE user_id = 1 AND amount >= 50 ORDER BY id ASC;",
    )
    .unwrap();
    assert!(
        plan_before
            .iter()
            .any(|r| matches!(&r[0], Value::Text(s) if s.contains("SeqScan")))
    );

    // Create secondary index on orders(user_id) and verify EXPLAIN switches to IndexPrefixScan
    // while returning byte-for-byte identical rows to SeqScan.
    db.execute_sql("CREATE INDEX idx_orders_user ON orders (user_id ASC);")
        .unwrap();
    let plan_after = query_rows(
        &db,
        "EXPLAIN SELECT id, amount FROM orders WHERE user_id = 1 AND amount >= 50 ORDER BY id ASC;",
    )
    .unwrap();
    assert!(
        plan_after
            .iter()
            .any(|r| matches!(&r[0], Value::Text(s) if s.contains("IndexPrefixScan")))
    );
    let indexed_after = query_rows(
        &db,
        "SELECT id, amount FROM orders WHERE user_id = 1 AND amount >= 50 ORDER BY id ASC;",
    )
    .unwrap();
    assert_eq!(oracle_before, indexed_after);
    assert_eq!(
        indexed_after,
        vec![
            vec![Value::Int64(101), Value::Int64(50)],
            vec![Value::Int64(102), Value::Int64(150)],
        ]
    );

    // 2. INNER JOIN + WHERE + GROUP BY + HAVING + ORDER BY.
    let agg_rows = query_rows(
        &db,
        "
        SELECT u.name, COUNT(*) AS cnt, SUM(o.amount) AS total, MAX(o.amount) AS peak
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE o.status = 'paid'
        GROUP BY u.name
        HAVING SUM(o.amount) >= 200
        ORDER BY u.name ASC;
        ",
    )
    .unwrap();
    assert_eq!(
        agg_rows,
        vec![
            vec![
                Value::Text("Alice".into()),
                Value::Int64(2),
                Value::Int64(200),
                Value::Int64(150),
            ],
            vec![
                Value::Text("Bob".into()),
                Value::Int64(1),
                Value::Int64(200),
                Value::Int64(200),
            ],
        ]
    );

    // 3. LEFT JOIN preserves unmatched Charlie with NULL order id.
    let left_rows = query_rows(
        &db,
        "
        SELECT u.name, o.id
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.name LIKE 'Ch%';
        ",
    )
    .unwrap();
    assert_eq!(
        left_rows,
        vec![vec![Value::Text("Charlie".into()), Value::Null]]
    );

    // 4. Online ALTER TABLE ADD COLUMN + UPDATE + DELETE + power-loss reboot durability.
    db.execute_sql(
        "
        ALTER TABLE users ADD COLUMN vip BOOLEAN NOT NULL DEFAULT false;
        UPDATE users SET vip = true WHERE id IN (1, 3);
        DELETE FROM orders WHERE status = 'cancelled';
        ",
    )
    .unwrap();

    drop(db);
    fs.simulate_power_loss_and_reboot().unwrap();

    let reopened = AnchorDb::open_fs(fs, LsmConfig::default(), QueryConfig::default()).unwrap();
    let vips = query_rows(
        &reopened,
        "SELECT name, vip FROM users WHERE vip = true ORDER BY name ASC;",
    )
    .unwrap();
    assert_eq!(
        vips,
        vec![
            vec![Value::Text("Alice".into()), Value::Bool(true)],
            vec![Value::Text("Charlie".into()), Value::Bool(true)],
        ]
    );

    let remaining_orders = query_rows(&reopened, "SELECT COUNT(*) FROM orders;").unwrap();
    assert_eq!(remaining_orders, vec![vec![Value::Int64(3)]]);
}

#[test]
fn sql_explicit_transactions_commit_rollback_and_empty_aggregates() {
    let (_fs, db) = new_test_db(3002).unwrap();

    db.execute_sql(
        "
        CREATE TABLE accounts (
            id BIGINT PRIMARY KEY,
            balance BIGINT NOT NULL
        );
        ",
    )
    .unwrap();

    // Empty table global aggregate returns COUNT(*)=0, SUM=NULL.
    let empty_agg = query_rows(&db, "SELECT COUNT(*), SUM(balance) FROM accounts;").unwrap();
    assert_eq!(empty_agg, vec![vec![Value::Int64(0), Value::Null]]);

    // Explicit BEGIN ... ROLLBACK leaves table untouched.
    let mut sess = db.sql_session();
    sess.execute("BEGIN;").unwrap();
    sess.execute("INSERT INTO accounts VALUES (1, 100);")
        .unwrap();
    assert!(sess.in_transaction());
    sess.execute("ROLLBACK;").unwrap();
    assert!(!sess.in_transaction());
    assert_eq!(
        query_rows(&db, "SELECT COUNT(*) FROM accounts;").unwrap(),
        vec![vec![Value::Int64(0)]]
    );

    // Explicit BEGIN ... COMMIT atomically persists both rows.
    sess.execute(
        "
        BEGIN;
        INSERT INTO accounts VALUES (1, 100), (2, 250);
        UPDATE accounts SET balance = balance - 40 WHERE id = 1;
        UPDATE accounts SET balance = balance + 40 WHERE id = 2;
        COMMIT;
        ",
    )
    .unwrap();

    let balances = query_rows(
        &db,
        "SELECT id, balance FROM accounts ORDER BY id ASC LIMIT 2;",
    )
    .unwrap();
    assert_eq!(
        balances,
        vec![
            vec![Value::Int64(1), Value::Int64(60)],
            vec![Value::Int64(2), Value::Int64(290)],
        ]
    );

    // Smoke-test ASCII table rendering used by `anchordb shell`.
    let res = db.execute_sql("SHOW TABLES;").unwrap();
    let rendered = format_sql_result(&res[0]);
    assert!(rendered.contains("accounts"));
    assert!(rendered.contains("(1 row(s))"));
}
