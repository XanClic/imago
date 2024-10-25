//! Access generic files as images.
//!
//! Allows accessing generic storage objects (`Storage`) as images (i.e. `FormatAccess`).

use crate::format::drivers::{FormatDriverInstance, Mapping};
use crate::Storage;
use async_trait::async_trait;
use std::io;

/// Wraps a storage object without any translation.
pub struct Raw<S: Storage> {
    /// Wrapped storage object.
    inner: S,

    /// Disk size, which is the file size when this object was created.
    size: u64,
}

impl<S: Storage> Raw<S> {
    /// Wrap `inner`, allowing it to be used as a disk image in raw format.
    pub fn new(inner: S) -> io::Result<Self> {
        let size = inner.size()?;
        Ok(Raw { inner, size })
    }
}

#[async_trait(?Send)]
impl<S: Storage> FormatDriverInstance for Raw<S> {
    type Storage = S;

    fn size(&self) -> u64 {
        self.size
    }

    fn collect_storage_dependencies(&self) -> Vec<&'_ S> {
        vec![&self.inner]
    }

    fn writable(&self) -> bool {
        // TODO: Query from `inner`
        true
    }

    async fn get_mapping(&self, offset: u64, max_length: u64) -> io::Result<(Mapping<'_, S>, u64)> {
        let remaining = match self.size.checked_sub(offset) {
            None | Some(0) => return Ok((Mapping::Eof, 0)),
            Some(remaining) => remaining,
        };

        Ok((
            Mapping::Raw {
                storage: &self.inner,
                offset,
                writable: true,
            },
            std::cmp::min(max_length, remaining),
        ))
    }

    async fn ensure_data_mapping(
        &self,
        offset: u64,
        length: u64,
        _overwrite: bool,
    ) -> io::Result<(&'_ S, u64, u64)> {
        let Some(remaining) = self.size.checked_sub(offset) else {
            return Err(io::Error::other("Cannot allocate past the end of file"));
        };
        if length > remaining {
            return Err(io::Error::other("Cannot allocate past the end of file"));
        }

        Ok((&self.inner, offset, length))
    }
}
