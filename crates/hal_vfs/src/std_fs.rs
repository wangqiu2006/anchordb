//! Standard OS filesystem backend for desktop and server targets (`std`).

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::{FileLock, FileSystem, StorageFile, VfsError, validate_file_name};

fn map_io_err(err: &std::io::Error) -> VfsError {
    match err.kind() {
        ErrorKind::NotFound => VfsError::NotFound,
        ErrorKind::StorageFull => VfsError::NoSpace,
        _ => VfsError::IoFailure,
    }
}

/// Exclusive OS file lock backed by `std::fs::File::try_lock` (released on drop).
#[derive(Debug)]
pub struct StdFileLock {
    _file: File,
}

impl FileLock for StdFileLock {}

/// Positioned file handle backed by `std::fs::File`.
#[derive(Debug)]
pub struct StdStorageFile {
    file: File,
}

impl StorageFile for StdStorageFile {
    fn len(&self) -> Result<u64, VfsError> {
        self.file
            .metadata()
            .map(|meta| meta.len())
            .map_err(|e| map_io_err(&e))
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), VfsError> {
        if buf.is_empty() {
            return Ok(());
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file
                .read_exact_at(buf, offset)
                .map_err(|e| map_io_err(&e))
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut total = 0usize;
            while total < buf.len() {
                let current_offset = offset
                    .checked_add(total as u64)
                    .ok_or(VfsError::IoFailure)?;
                let read_n = self
                    .file
                    .seek_read(&mut buf[total..], current_offset)
                    .map_err(|e| map_io_err(&e))?;
                if read_n == 0 {
                    return Err(VfsError::IoFailure);
                }
                total += read_n;
            }
            Ok(())
        }
    }

    fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<(), VfsError> {
        if data.is_empty() {
            return Ok(());
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file
                .write_all_at(data, offset)
                .map_err(|e| map_io_err(&e))
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut total = 0usize;
            while total < data.len() {
                let current_offset = offset
                    .checked_add(total as u64)
                    .ok_or(VfsError::IoFailure)?;
                let written_n = self
                    .file
                    .seek_write(&data[total..], current_offset)
                    .map_err(|e| map_io_err(&e))?;
                if written_n == 0 {
                    return Err(VfsError::IoFailure);
                }
                total += written_n;
            }
            Ok(())
        }
    }

    fn sync_data(&mut self) -> Result<(), VfsError> {
        self.file.sync_data().map_err(|e| map_io_err(&e))
    }

    fn truncate(&mut self, size: u64) -> Result<(), VfsError> {
        self.file.set_len(size).map_err(|e| map_io_err(&e))
    }
}

/// Directory-scoped OS filesystem implementation.
#[derive(Debug, Clone)]
pub struct StdFileSystem {
    root: PathBuf,
}

impl StdFileSystem {
    /// Opens or creates the root database directory.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, VfsError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|e| map_io_err(&e))?;
        Ok(Self { root })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve(&self, name: &str) -> Result<PathBuf, VfsError> {
        validate_file_name(name)?;
        Ok(self.root.join(name))
    }
}

impl FileSystem for StdFileSystem {
    type File = StdStorageFile;
    type Lock = StdFileLock;

    fn lock_exclusive(&self, name: &str) -> Result<Self::Lock, VfsError> {
        let path = self.resolve(name)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| map_io_err(&e))?;

        match file.try_lock() {
            Ok(()) => Ok(StdFileLock { _file: file }),
            Err(TryLockError::WouldBlock) => Err(VfsError::AlreadyLocked),
            Err(TryLockError::Error(err)) => Err(map_io_err(&err)),
        }
    }

    fn exists(&self, name: &str) -> Result<bool, VfsError> {
        let path = self.resolve(name)?;
        path.try_exists().map_err(|e| map_io_err(&e))
    }

    fn open_or_create(&self, name: &str) -> Result<Self::File, VfsError> {
        let path = self.resolve(name)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| map_io_err(&e))?;
        Ok(StdStorageFile { file })
    }

    fn open_read_only(&self, name: &str) -> Result<Self::File, VfsError> {
        let path = self.resolve(name)?;
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| map_io_err(&e))?;
        Ok(StdStorageFile { file })
    }

    fn remove(&self, name: &str) -> Result<(), VfsError> {
        let path = self.resolve(name)?;
        fs::remove_file(path).map_err(|e| map_io_err(&e))
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), VfsError> {
        let src = self.resolve(from)?;
        let dst = self.resolve(to)?;
        fs::rename(src, dst).map_err(|e| map_io_err(&e))
    }

    fn sync_dir(&self) -> Result<(), VfsError> {
        #[cfg(unix)]
        {
            let dir = File::open(&self.root).map_err(|e| map_io_err(&e))?;
            dir.sync_all().map_err(|e| map_io_err(&e))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::format;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::StdFileSystem;
    use crate::{FileSystem, StorageFile, VfsError};

    fn unique_temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("anchordb_vfs_{tag}_{pid}_{id}"))
    }

    #[test]
    fn std_fs_read_write_rename_truncate_and_lock() {
        let dir = unique_temp_dir("basic");
        let fs = StdFileSystem::open(&dir).unwrap();

        // Exclusive lock blocks a second holder and releases on drop.
        let lock = fs.lock_exclusive("LOCK").unwrap();
        assert_eq!(
            fs.lock_exclusive("LOCK").unwrap_err(),
            VfsError::AlreadyLocked
        );
        drop(lock);
        let _lock2 = fs.lock_exclusive("LOCK").unwrap();

        // Write, sync, read back, truncate.
        let mut f = fs.open_or_create("wal.tmp").unwrap();
        assert!(f.is_empty().unwrap());
        f.write_all_at(0, b"hello anchordb").unwrap();
        f.sync_data().unwrap();
        assert_eq!(f.len().unwrap(), 14);

        let mut buf = [0u8; 8];
        f.read_exact_at(6, &mut buf).unwrap();
        assert_eq!(&buf, b"anchordb");

        f.truncate(5).unwrap();
        assert_eq!(f.len().unwrap(), 5);
        drop(f);

        // Atomic rename & directory sync.
        fs.rename("wal.tmp", "wal.log").unwrap();
        fs.sync_dir().unwrap();
        assert!(!fs.exists("wal.tmp").unwrap());
        assert!(fs.exists("wal.log").unwrap());

        let ro = fs.open_read_only("wal.log").unwrap();
        let mut head = [0u8; 5];
        ro.read_exact_at(0, &mut head).unwrap();
        assert_eq!(&head, b"hello");
        drop(ro);

        fs.remove("wal.log").unwrap();
        assert!(!fs.exists("wal.log").unwrap());

        drop(_lock2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
