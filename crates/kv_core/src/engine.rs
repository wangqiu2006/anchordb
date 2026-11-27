use core::fmt;
use core::ops::Bound;

use hal_vfs::VfsError;

use crate::format::FormatError;

/// Unified error type returned by `kv_core` engines and transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvError {
    Vfs(VfsError),
    Format(FormatError),
    BufferTooSmall { required: usize },
    WriteConflict,
    ReadOnlyTransaction,
    InvalidArgument,
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Vfs(err) => write!(f, "vfs error: {err}"),
            Self::Format(err) => write!(f, "format error: {err}"),
            Self::BufferTooSmall { required } => {
                write!(f, "output buffer too small, requires {required} bytes")
            }
            Self::WriteConflict => f.write_str("transaction write conflict detected"),
            Self::ReadOnlyTransaction => f.write_str("write attempted on read-only transaction"),
            Self::InvalidArgument => f.write_str("invalid argument"),
        }
    }
}

impl core::error::Error for KvError {}

impl From<VfsError> for KvError {
    fn from(value: VfsError) -> Self {
        Self::Vfs(value)
    }
}

impl From<FormatError> for KvError {
    fn from(value: FormatError) -> Self {
        Self::Format(value)
    }
}

/// Top-level ordered key-value engine boundary.
pub trait KvEngine {
    type Txn<'a>: KvTransaction
    where
        Self: 'a;

    fn begin_txn(&self, read_only: bool) -> Result<Self::Txn<'_>, KvError>;
    fn checkpoint(&self) -> Result<(), KvError>;
}

/// Borrowed key-value pair yielded by a [`KvIterator`].
pub type KvEntry<'a> = (&'a [u8], &'a [u8]);

/// Zero-allocation ordered iterator over key-value entries.
pub trait KvIterator {
    fn next_entry(&mut self) -> Result<Option<KvEntry<'_>>, KvError>;
}

/// Transactional KV contract designed for both `#![no_std]` zero-alloc and concurrent upper layers.
///
/// Read operations take `&self` so callers can iterate a secondary index (`scan`) while performing
/// point lookups (`get_into`) within the same transaction without borrow conflicts.
pub trait KvTransaction {
    type Iter<'a>: KvIterator
    where
        Self: 'a;

    fn get_into(&self, key: &[u8], out: &mut [u8]) -> Result<Option<usize>, KvError>;

    fn scan<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
    ) -> Result<Self::Iter<'a>, KvError>;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), KvError>;
    fn delete(&mut self, key: &[u8]) -> Result<(), KvError>;
    fn commit(self) -> Result<(), KvError>;
    fn abort(self);
}

#[cfg(feature = "alloc")]
pub trait KvTransactionExt: KvTransaction {
    fn get(&self, key: &[u8]) -> Result<Option<alloc::vec::Vec<u8>>, KvError> {
        let mut stack_buf = [0u8; 256];
        match self.get_into(key, &mut stack_buf) {
            Ok(Some(len)) => Ok(Some(stack_buf[..len].to_vec())),
            Ok(None) => Ok(None),
            Err(KvError::BufferTooSmall { required }) => {
                let mut heap_buf = alloc::vec![0u8; required];
                match self.get_into(key, &mut heap_buf)? {
                    Some(len) => {
                        heap_buf.truncate(len);
                        Ok(Some(heap_buf))
                    }
                    None => Ok(None),
                }
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(feature = "alloc")]
impl<T: KvTransaction + ?Sized> KvTransactionExt for T {}
