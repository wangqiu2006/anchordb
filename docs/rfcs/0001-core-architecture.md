# RFC 0001: AnchorDB Core Architecture & Long-Term Invariants

- **Status**: Accepted
- **Date**: 2026-11-27

## 1. Layered Architecture & Unidirectional Dependencies

Dependencies MUST strictly flow downward; reverse or cyclic dependencies are prohibited:

```text
anchordb (Facade / CLI)
  └── server_net (pgwire + pg_catalog)
        └── query_engine (Structured API / SQL / DataChunk Executor)
              └── table_codec (Memcomparable Keys / Indexes / Schema Evolution)
                    └── kv_core (Ordered KV / LSM-Tree / WAL / MVCC)
                          └── hal_vfs (FileSystem / StorageFile / Fault Simulator)
```

## 2. Stable Public Boundaries

The following traits constitute the long-term architectural seams:
- `hal_vfs::FileSystem`, `hal_vfs::StorageFile`, `hal_vfs::FileLock`
- `kv_core::KvEngine`, `kv_core::KvTransaction`, `kv_core::KvIterator`

Upper layers (`table_codec`, `query_engine`, `server_net`) MUST interact with storage exclusively through `kv_core`'s traits so storage backends can evolve without breaking upper layers.

## 3. On-Disk Persistence Invariants

1. **Byte Order**: All fixed-width integers in headers, WAL records, SSTables, and Manifests MUST use **Little-Endian** encoding. (Order-preserving Memcomparable keys in `table_codec` explicitly define their own Big-Endian/bytewise sort encoding.)
2. **Self-Describing Headers & Integrity**: Every persistent file/block MUST carry a magic tag, format version, and `CRC32C` checksum.
3. **Version Compatibility**:
   - Superblock Header (64 bytes fixed): `Magic (4B) | FormatVersion (u16) | PageSize (u16) | MinReaderVersion (u16) | MinWriterVersion (u16) | CompatFlags (u32) | IncompatFlags (u32) | Reserved (40B) | CRC32C (u32)`.
   - Open rejected if engine version `< MinReaderVersion` or unknown `IncompatFlags` are set.
   - Read-only allowed if `MinReaderVersion <= engine version < MinWriterVersion`.
   - Read-write allowed if engine version `>= MinWriterVersion`.
4. **Crash Safety**:
   - Transactions buffer writes locally (`WriteBatch`) and commit with a single WAL append + `sync_data()`.
   - Recovery may only truncate an incomplete torn tail block; mid-file CRC corruption MUST return `CorruptedData` and never silently drop committed data.
   - Manifest/file swaps MUST follow `write temp -> sync_data() -> rename() -> sync_dir()`.

## 4. Platform & Resource Boundaries

- **MCU Target (`mode-mcu`)**: Targets mid-to-high-end MCUs with ≥ 256 KB RAM (primary validation board: **ESP32-S3-N16R8** with 512 KB SRAM, 8 MB PSRAM, 16 MB Flash; static budget 64–128 KB for `kv_core`). Core `hal_vfs` and `kv_core` MUST compile under `#![no_std]` with zero mandatory heap allocation in core trait signatures.
- **Windows File Handle Semantics**: Because Windows forbids deleting or overwriting open files, SSTable reclamation MUST use reference-counted deferred deletion.
- **Safety & Lints**: `unsafe_code = "deny"`, `clippy::unwrap_used = "deny"`, and `clippy::expect_used = "deny"` are enforced workspace-wide (with test-only exceptions via `clippy.toml`).
