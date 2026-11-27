#![no_std]

#[cfg(feature = "mode-embedded")]
extern crate std;

pub use hal_vfs;
pub use kv_core;

#[cfg(feature = "mode-embedded")]
pub use table_codec;

#[cfg(feature = "mode-embedded")]
pub use query_engine;

#[cfg(feature = "mode-server")]
pub use server_net;

/// Compile-time form factor selected via Cargo features.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildProfile {
    Mcu,
    Embedded,
    Server,
}

impl BuildProfile {
    #[must_use]
    pub const fn current() -> Self {
        #[cfg(feature = "mode-server")]
        {
            Self::Server
        }
        #[cfg(all(feature = "mode-embedded", not(feature = "mode-server")))]
        {
            Self::Embedded
        }
        #[cfg(not(feature = "mode-embedded"))]
        {
            Self::Mcu
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mcu => "mode-mcu",
            Self::Embedded => "mode-embedded",
            Self::Server => "mode-server",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BuildProfile;

    #[test]
    fn build_profile_matches_active_features() {
        let profile = BuildProfile::current();
        assert!(!profile.as_str().is_empty());
    }
}
