//! Implementation for preallocation.
//!
//! Preallocation is used for new images or when growing images.

use super::*;
use crate::storage::ext::write_full_zeroes;

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2<S, F> {
    /// Make the given range zero.
    ///
    /// Bypasses disk bound checking, i.e. can and will write beyond the image end.
    pub(super) async fn preallocate_zero(&self, mut offset: u64, length: u64) -> io::Result<()> {
        let max_offset = offset.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Preallocate range overflow")
        })?;

        while offset < max_offset {
            let (zofs, zlen) = self
                .ensure_fixed_mapping(
                    GuestOffset(offset),
                    max_offset - offset,
                    FixedMapping::ZeroRetainAllocation,
                )
                .await?;
            let zofs = zofs.0;
            if zofs > offset {
                self.preallocate_write_data(offset, zofs - offset).await?;
            }
            offset = zofs + zlen;
            if zlen == 0 && offset < max_offset {
                self.preallocate_write_data(offset, max_offset - offset)
                    .await?;
                break;
            }
        }

        Ok(())
    }

    /// Preallocate the given range as data clusters.
    ///
    /// Does not write data beyond trying to ensure `storage_prealloc_mode` for the underlying
    /// clusters.
    ///
    /// Bypasses disk bound checking, i.e. can and will write beyond the image end.
    pub(super) async fn preallocate(
        &self,
        mut offset: u64,
        length: u64,
        storage_prealloc_mode: storage::PreallocateMode,
    ) -> io::Result<()> {
        let max_offset = offset.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Preallocate range overflow")
        })?;

        if let Some(data_file) = self.storage.as_ref() {
            data_file.resize(max_offset, storage_prealloc_mode).await?;
        }

        while offset < max_offset {
            let (file, fofs, flen) = self
                .do_ensure_data_mapping(GuestOffset(offset), max_offset - offset, true)
                .await?;
            // TODO: This is terrible, `do_ensure_data_mapping()` should get a parameter for this
            let file_end_ofs = fofs + flen;
            if let Ok(file_size) = file.size() {
                if file_size < file_end_ofs {
                    file.resize(file_end_ofs, storage_prealloc_mode).await?;
                }
            }
            offset += flen;
        }

        Ok(())
    }

    /// Write zeroes to the given range.
    ///
    /// Bypasses disk bound checking, i.e. can and will write beyond the image end.
    pub(super) async fn preallocate_write_data(
        &self,
        mut offset: u64,
        length: u64,
    ) -> io::Result<()> {
        let max_offset = offset.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Preallocate range overflow")
        })?;

        while offset < max_offset {
            let (file, fofs, flen) = self
                .do_ensure_data_mapping(GuestOffset(offset), max_offset - offset, true)
                .await?;
            write_full_zeroes(file, fofs, flen).await?;
            offset += flen;
        }

        Ok(())
    }
}
