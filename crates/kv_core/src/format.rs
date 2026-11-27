use core::fmt;

pub const SUPERBLOCK_MAGIC: [u8; 4] = *b"ANCR";
pub const SUPERBLOCK_SIZE: usize = 64;
pub const CURRENT_FORMAT_VERSION: u16 = 1;

/// Errors returned while encoding, decoding, or validating persistent formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatError {
    InvalidMagic,
    ChecksumMismatch,
    IncompatibleVersion {
        format_version: u16,
        min_reader_version: u16,
        engine_version: u16,
    },
    UnsupportedIncompatFlags(u32),
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => f.write_str("invalid file magic header"),
            Self::ChecksumMismatch => f.write_str("CRC32C checksum verification failed"),
            Self::IncompatibleVersion {
                format_version,
                min_reader_version,
                engine_version,
            } => write!(
                f,
                "incompatible format version {format_version} (requires >= {min_reader_version}, engine is {engine_version})"
            ),
            Self::UnsupportedIncompatFlags(flags) => {
                write!(f, "unsupported incompatible feature flags: 0x{flags:08x}")
            }
        }
    }
}

impl core::error::Error for FormatError {}

/// Access capability allowed after evaluating header version fields against the running engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCompatibility {
    ReadWrite,
    ReadOnly,
}

/// Fixed 64-byte Little-Endian file / superblock header.
///
/// Layout (64 bytes total):
/// - `0..4`   Magic ("ANCR")
/// - `4..6`   FormatVersion (`u16`)
/// - `6..8`   PageSize (`u16`)
/// - `8..10`  MinReaderVersion (`u16`)
/// - `10..12` MinWriterVersion (`u16`)
/// - `12..16` CompatFlags (`u32`)
/// - `16..20` IncompatFlags (`u32`)
/// - `20..60` Reserved (`40B`, zeroed)
/// - `60..64` CRC32C (`u32` over bytes `0..60`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuperblockHeader {
    pub format_version: u16,
    pub page_size: u16,
    pub min_reader_version: u16,
    pub min_writer_version: u16,
    pub compat_flags: u32,
    pub incompat_flags: u32,
}

impl SuperblockHeader {
    #[must_use]
    pub const fn new(page_size: u16) -> Self {
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            page_size,
            min_reader_version: CURRENT_FORMAT_VERSION,
            min_writer_version: CURRENT_FORMAT_VERSION,
            compat_flags: 0,
            incompat_flags: 0,
        }
    }

    #[must_use]
    pub fn encode(&self) -> [u8; SUPERBLOCK_SIZE] {
        let mut buf = [0u8; SUPERBLOCK_SIZE];
        buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC);
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.page_size.to_le_bytes());
        buf[8..10].copy_from_slice(&self.min_reader_version.to_le_bytes());
        buf[10..12].copy_from_slice(&self.min_writer_version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.compat_flags.to_le_bytes());
        buf[16..20].copy_from_slice(&self.incompat_flags.to_le_bytes());
        let checksum = crc32c(&buf[0..60]);
        buf[60..64].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    pub fn decode(raw: &[u8; SUPERBLOCK_SIZE]) -> Result<Self, FormatError> {
        if raw[0..4] != SUPERBLOCK_MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        let expected_crc = u32::from_le_bytes([raw[60], raw[61], raw[62], raw[63]]);
        let actual_crc = crc32c(&raw[0..60]);
        if expected_crc != actual_crc {
            return Err(FormatError::ChecksumMismatch);
        }
        Ok(Self {
            format_version: u16::from_le_bytes([raw[4], raw[5]]),
            page_size: u16::from_le_bytes([raw[6], raw[7]]),
            min_reader_version: u16::from_le_bytes([raw[8], raw[9]]),
            min_writer_version: u16::from_le_bytes([raw[10], raw[11]]),
            compat_flags: u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]),
            incompat_flags: u32::from_le_bytes([raw[16], raw[17], raw[18], raw[19]]),
        })
    }

    pub fn check_compatibility(
        &self,
        engine_version: u16,
        supported_incompat_flags: u32,
    ) -> Result<VersionCompatibility, FormatError> {
        let unsupported = self.incompat_flags & !supported_incompat_flags;
        if unsupported != 0 {
            return Err(FormatError::UnsupportedIncompatFlags(unsupported));
        }
        if engine_version < self.min_reader_version {
            return Err(FormatError::IncompatibleVersion {
                format_version: self.format_version,
                min_reader_version: self.min_reader_version,
                engine_version,
            });
        }
        if engine_version < self.min_writer_version {
            Ok(VersionCompatibility::ReadOnly)
        } else {
            Ok(VersionCompatibility::ReadWrite)
        }
    }
}

/// Computes IEEE/Castagnoli CRC-32C checksum in pure `#![no_std]` Rust.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::{FormatError, SuperblockHeader, VersionCompatibility, crc32c};

    #[test]
    fn crc32c_matches_standard_test_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn superblock_roundtrip_and_corruption_detection() {
        let header = SuperblockHeader::new(4096);
        let mut encoded = header.encode();
        let decoded = SuperblockHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);

        // Flip a payload bit and verify CRC32C catches it.
        encoded[12] ^= 0x01;
        assert_eq!(
            SuperblockHeader::decode(&encoded),
            Err(FormatError::ChecksumMismatch)
        );
    }

    #[test]
    fn superblock_version_compatibility_rules() {
        let mut header = SuperblockHeader::new(1024);
        header.format_version = 3;
        header.min_reader_version = 2;
        header.min_writer_version = 3;

        assert!(matches!(
            header.check_compatibility(1, 0),
            Err(FormatError::IncompatibleVersion { .. })
        ));
        assert_eq!(
            header.check_compatibility(2, 0).unwrap(),
            VersionCompatibility::ReadOnly
        );
        assert_eq!(
            header.check_compatibility(3, 0).unwrap(),
            VersionCompatibility::ReadWrite
        );

        header.incompat_flags = 0b10;
        assert_eq!(
            header.check_compatibility(3, 0b01),
            Err(FormatError::UnsupportedIncompatFlags(0b10))
        );
    }
}
