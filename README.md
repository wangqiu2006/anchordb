# AnchorDB

A modular, cross-platform, power-loss-safe database kernel written in Rust following the **"One Core, Multi-Form"** architecture:

- **`mode-mcu`** (`#![no_std]`): `hal_vfs` + `kv_core` for mid-to-high-end MCUs (≥ 256 KB RAM, primary target **ESP32-S3-N16R8**).
- **`mode-embedded`** (`std`): adds `table_codec` + `query_engine` structured API / lightweight SQL for desktop apps and games.
- **`mode-server`** (`std`): adds MVCC + full SQL + `server_net` (`pgwire` + `pg_catalog` compatibility) for client-server OLTP workloads.

## Workspace Layout

- `crates/hal_vfs` — `#![no_std]` storage & filesystem abstraction, exclusive locks, and deterministic fault injection simulator.
- `crates/kv_core` — `#![no_std]`-compatible ordered KV engine (LSM-Tree, WriteBatch WAL, Manifest, Compaction, MVCC).
- `crates/table_codec` — Memcomparable key encoding, unique/non-unique secondary indexes, and versioned row schema evolution.
- `crates/query_engine` — Structured query API, SQL parser/binder/RBO optimizer, and `DataChunk` pull executor with memory budgets.
- `crates/server_net` — PostgreSQL wire protocol (`pgwire`) and basic `pg_catalog` compatibility layer.
- `crates/anchordb` — Unified facade crate and CLI entry (`shell` / `serve` / `doctor`).

## Quick Verification

```bash
# 1. Bare-metal no_std verification
cargo check -p hal_vfs -p kv_core -p anchordb --no-default-features --features mode-mcu --target thumbv7em-none-eabihf

# 2. Embedded & server feature matrix
cargo test -p anchordb --no-default-features --features mode-embedded
cargo test -p anchordb --features mode-server

# 3. Formatting & lints
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```
