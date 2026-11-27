#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod engine;
pub mod format;

#[cfg(feature = "alloc")]
pub use engine::KvTransactionExt;
pub use engine::{KvEngine, KvEntry, KvError, KvIterator, KvTransaction};
pub use format::{
    CURRENT_FORMAT_VERSION, FormatError, SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE, SuperblockHeader,
    VersionCompatibility, crc32c,
};
