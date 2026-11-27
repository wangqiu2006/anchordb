#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "alloc")]
pub mod bloom;
pub mod engine;
pub mod format;
#[cfg(feature = "std")]
pub mod lsm;
#[cfg(feature = "alloc")]
pub mod manifest;
#[cfg(feature = "alloc")]
pub mod sstable;
#[cfg(feature = "alloc")]
pub mod wal;

#[cfg(feature = "alloc")]
pub use bloom::{BloomFilter, DEFAULT_BLOOM_BITS_PER_KEY};
#[cfg(feature = "alloc")]
pub use engine::KvTransactionExt;
pub use engine::{KvEngine, KvEntry, KvError, KvIterator, KvTransaction};
pub use format::{
    CURRENT_FORMAT_VERSION, FormatError, SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE, SuperblockHeader,
    VersionCompatibility, crc32c,
};
#[cfg(feature = "std")]
pub use lsm::{DoctorReport, LsmConfig, LsmEngine, LsmIter, LsmTxn, inspect_database};
