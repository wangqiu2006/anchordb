//! `#![no_std]` MCU SPI NOR Flash hardware geometry & alignment checks.
//!
//! Primary validation target: **ESP32-S3-N16R8** (16 MiB SPI NOR Flash,
//! 4 KiB erase sector, 256 B program page).

use crate::VfsError;

/// Hardware geometry descriptor for a raw SPI NOR Flash partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NorFlashGeometry {
    pub capacity_bytes: u32,
    pub erase_sector_size: u32,
    pub program_page_size: u32,
}

impl NorFlashGeometry {
    /// Typical SPI NOR partition layout for ESP32-S3-N16R8 (4 KiB erase / 256 B program).
    #[must_use]
    pub const fn esp32_s3_partition(capacity_bytes: u32) -> Self {
        Self {
            capacity_bytes,
            erase_sector_size: 4096,
            program_page_size: 256,
        }
    }

    pub const fn validate_erase_range(&self, offset: u32, len: u32) -> Result<(), VfsError> {
        if self.erase_sector_size == 0
            || !offset.is_multiple_of(self.erase_sector_size)
            || !len.is_multiple_of(self.erase_sector_size)
        {
            return Err(VfsError::InvalidAlignment);
        }
        match offset.checked_add(len) {
            Some(end) if end <= self.capacity_bytes => Ok(()),
            _ => Err(VfsError::NoSpace),
        }
    }

    pub const fn validate_program_range(&self, offset: u32, len: u32) -> Result<(), VfsError> {
        if self.program_page_size == 0
            || !offset.is_multiple_of(self.program_page_size)
            || !len.is_multiple_of(self.program_page_size)
        {
            return Err(VfsError::InvalidAlignment);
        }
        match offset.checked_add(len) {
            Some(end) if end <= self.capacity_bytes => Ok(()),
            _ => Err(VfsError::NoSpace),
        }
    }
}

/// Low-level raw SPI NOR Flash contract for future bare-partition MCU backends.
///
/// Note: Raw NOR Flash can only clear bits (`1 -> 0`) during programming and requires
/// sector-aligned erase (`0 -> 1`, resetting all bytes to `0xFF`) before rewriting.
pub trait RawNorFlash {
    fn geometry(&self) -> NorFlashGeometry;
    fn read(&self, offset: u32, buf: &mut [u8]) -> Result<(), VfsError>;
    fn program(&mut self, offset: u32, data: &[u8]) -> Result<(), VfsError>;
    fn erase(&mut self, offset: u32, len: u32) -> Result<(), VfsError>;
}

/// Wrapper enforcing NOR Flash alignment and bounds checks before dispatching to hardware.
#[derive(Debug)]
pub struct CheckedNorFlash<F> {
    inner: F,
}

impl<F: RawNorFlash> CheckedNorFlash<F> {
    pub const fn new(inner: F) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> F {
        self.inner
    }

    pub fn geometry(&self) -> NorFlashGeometry {
        self.inner.geometry()
    }

    pub fn read(&self, offset: u32, buf: &mut [u8]) -> Result<(), VfsError> {
        let len = u32::try_from(buf.len()).map_err(|_| VfsError::NoSpace)?;
        let end = offset.checked_add(len).ok_or(VfsError::NoSpace)?;
        if end > self.inner.geometry().capacity_bytes {
            return Err(VfsError::NoSpace);
        }
        self.inner.read(offset, buf)
    }

    pub fn program(&mut self, offset: u32, data: &[u8]) -> Result<(), VfsError> {
        let len = u32::try_from(data.len()).map_err(|_| VfsError::NoSpace)?;
        self.inner.geometry().validate_program_range(offset, len)?;
        self.inner.program(offset, data)
    }

    pub fn erase(&mut self, offset: u32, len: u32) -> Result<(), VfsError> {
        self.inner.geometry().validate_erase_range(offset, len)?;
        self.inner.erase(offset, len)
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckedNorFlash, NorFlashGeometry, RawNorFlash};
    use crate::VfsError;

    struct MockNorFlash<const N: usize> {
        bytes: [u8; N],
        geo: NorFlashGeometry,
    }

    impl<const N: usize> MockNorFlash<N> {
        fn new() -> Self {
            Self {
                bytes: [0xFF; N],
                geo: NorFlashGeometry::esp32_s3_partition(N as u32),
            }
        }
    }

    impl<const N: usize> RawNorFlash for MockNorFlash<N> {
        fn geometry(&self) -> NorFlashGeometry {
            self.geo
        }

        fn read(&self, offset: u32, buf: &mut [u8]) -> Result<(), VfsError> {
            let start = offset as usize;
            buf.copy_from_slice(&self.bytes[start..start + buf.len()]);
            Ok(())
        }

        fn program(&mut self, offset: u32, data: &[u8]) -> Result<(), VfsError> {
            // NOR Flash physics: programming can only clear bits (1 -> 0).
            let start = offset as usize;
            for (dst, &src) in self.bytes[start..start + data.len()].iter_mut().zip(data) {
                if (*dst & src) != src {
                    return Err(VfsError::IoFailure);
                }
                *dst &= src;
            }
            Ok(())
        }

        fn erase(&mut self, offset: u32, len: u32) -> Result<(), VfsError> {
            let start = offset as usize;
            self.bytes[start..start + len as usize].fill(0xFF);
            Ok(())
        }
    }

    #[test]
    fn checked_nor_flash_enforces_alignment_and_erase_before_rewrite() {
        let mut flash = CheckedNorFlash::new(MockNorFlash::<8192>::new());

        // Misaligned program / erase must fail.
        assert_eq!(
            flash.program(10, &[0xAA; 256]),
            Err(VfsError::InvalidAlignment)
        );
        assert_eq!(flash.erase(512, 4096), Err(VfsError::InvalidAlignment));

        // Aligned program succeeds on erased 0xFF page.
        let payload = [0x5A; 256];
        flash.program(0, &payload).unwrap();
        let mut readback = [0u8; 256];
        flash.read(0, &mut readback).unwrap();
        assert_eq!(readback, payload);

        // Overwriting 0 bits back to 1 without sector erase fails (NOR physics).
        assert_eq!(flash.program(0, &[0xFF; 256]), Err(VfsError::IoFailure));

        // Erasing the 4 KiB sector restores 0xFF and permits reprogramming.
        flash.erase(0, 4096).unwrap();
        flash.program(0, &[0xA5; 256]).unwrap();
        flash.read(0, &mut readback).unwrap();
        assert_eq!(readback, [0xA5; 256]);
    }
}
