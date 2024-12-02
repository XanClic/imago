//! Core functionality.
//!
//! Provides access to different image formats via `FormatAccess` objects.

pub mod access;
pub mod builder;
pub mod drivers;
pub mod gate;
#[cfg(feature = "sync-wrappers")]
pub mod sync_wrappers;
pub mod wrapped;

/// List of imago formats.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
    /// Raw format (no metadata at all, everything stored 1:1)
    Raw,

    /// Qcow2 format (version 2 or 3)
    Qcow2,
}
