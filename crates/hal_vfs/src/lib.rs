#![no_std]

#[cfg(feature = "std")]
extern crate std;

use core::fmt;

/// Unified storage and filesystem error codes across host OS, MCU flash, and fault simulators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsError {
    NotFound,
    AlreadyLocked,
    IoFailure,
    CorruptedData,
    NoSpace,
    SimulatedPowerLoss,
}

impl fmt::Display for VfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::NotFound => "storage entry not found",
            Self::AlreadyLocked => "storage directory is already exclusively locked",
            Self::IoFailure => "underlying storage I/O failure",
            Self::CorruptedData => "corrupted storage data detected",
            Self::NoSpace => "no space left on storage device",
            Self::SimulatedPowerLoss => "simulated power loss triggered by fault injector",
        };
        f.write_str(msg)
    }
}

impl core::error::Error for VfsError {}

/// Byte-addressable persistent file or logical volume abstraction (WAL, SSTable, Manifest).
pub trait StorageFile {
    fn len(&self) -> Result<u64, VfsError>;

    fn is_empty(&self) -> Result<bool, VfsError> {
        Ok(self.len()? == 0)
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), VfsError>;
    fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<(), VfsError>;
    fn sync_data(&mut self) -> Result<(), VfsError>;
    fn truncate(&mut self, size: u64) -> Result<(), VfsError>;
}

/// Exclusive directory lock handle. Implementations release the lock on drop.
pub trait FileLock {}

/// Platform-neutral filesystem abstraction supporting crash-safe metadata operations.
pub trait FileSystem {
    type File: StorageFile;
    type Lock: FileLock;

    fn lock_exclusive(&self, name: &str) -> Result<Self::Lock, VfsError>;
    fn open_or_create(&self, name: &str) -> Result<Self::File, VfsError>;
    fn open_read_only(&self, name: &str) -> Result<Self::File, VfsError>;
    fn remove(&self, name: &str) -> Result<(), VfsError>;
    fn rename(&self, from: &str, to: &str) -> Result<(), VfsError>;

    /// Flushes directory metadata so newly created or renamed files survive power loss.
    fn sync_dir(&self) -> Result<(), VfsError>;
}

#[cfg(test)]
mod tests {
    use super::VfsError;

    #[test]
    fn vfs_error_display_is_non_empty() {
        let errors = [
            VfsError::NotFound,
            VfsError::AlreadyLocked,
            VfsError::IoFailure,
            VfsError::CorruptedData,
            VfsError::NoSpace,
            VfsError::SimulatedPowerLoss,
        ];
        for err in errors {
            assert!(!std::format!("{err}").is_empty());
        }
    }
}
