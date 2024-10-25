//! Null storage.
//!
//! Discard all written data, and return zeroes when read.

use crate::io_buffers::{IoVector, IoVectorMut};
use crate::{Storage, StorageOpenOptions};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// Null storage object.
///
/// Reading from this will always return zeroes, writing to it does nothing (except to potentially
/// grow its virtual “file length”).
pub struct Null {
    /// Virtual “file length”.
    size: AtomicU64,
}

impl Null {
    /// Create a new null storage object with the given initial virtual size.
    pub fn new(size: u64) -> Self {
        Null { size: size.into() }
    }
}

impl Storage for Null {
    async fn open(_opts: StorageOpenOptions) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Cannot open null storage",
        ))
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.size.load(Ordering::Relaxed))
    }

    async fn readv(&self, mut bufv: IoVectorMut<'_>, _offset: u64) -> io::Result<()> {
        bufv.fill(0);
        Ok(())
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        let Some(end) = offset.checked_add(bufv.len()) else {
            return Err(io::Error::other("Write too long"));
        };

        self.size.fetch_max(end, Ordering::Relaxed);
        Ok(())
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        let Some(end) = offset.checked_add(length) else {
            return Err(io::Error::other("Write too long"));
        };

        self.size.fetch_max(end, Ordering::Relaxed);
        Ok(())
    }
}
