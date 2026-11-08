//! Provides the `StorageExt` struct for more convenient access.
//!
//! `Storage` is provided by the driver, so is supposed to be simple and only contain what’s
//! necessary.  `StorageExt` builds on that to provide more convenient access, e.g. allows
//! unaligned requests and provides write serialization.

use super::drivers::RangeBlockedGuard;
use crate::io_buffers::{IoBuffer, IoVector, IoVectorMut, IoVectorTrait};
use crate::Storage;
use maybe_async::maybe_async;
use std::ops::Range;
use std::{cmp, io};
use tracing::trace;

/// Helper methods for storage objects.
///
/// Provides some more convenient methods for accessing storage objects.
#[maybe_async(AFIT)]
pub trait StorageExt: Storage {
    /// Read data at `offset` into `bufv`.
    ///
    /// Reads until `bufv` is filled completely, i.e. will not do short reads.  When reaching the
    /// end of file, the rest of `bufv` is filled with 0.
    ///
    /// Checks alignment.  If anything does not meet the requirements, enforces it (using ephemeral
    /// bounce buffers).
    #[allow(async_fn_in_trait)] // No need for Send
    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()>;

    /// Read with alignment handling and EOF zero-fill, returning the actual prefix if known.
    ///
    /// Alignment padding is excluded from the count; `None` means the backend cannot count.
    #[allow(async_fn_in_trait)]
    async fn readv_counted(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<Option<u64>>;

    /// Write data from `bufv` to `offset`.
    ///
    /// Writes all data from `bufv`, i.e. will not do short writes.  When reaching the end of file,
    /// it is grown as necessary so that the new end of file will be at `offset + bufv.len()`.
    ///
    /// If growing is not possible, expect writes beyond the end of file (even if only partially)
    /// to fail.
    ///
    /// Checks alignment.  If anything does not meet the requirements, enforces it using bounce
    /// buffers and a read-modify-write cycle that blocks concurrent writes to the affected area.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()>;

    /// Read data at `offset` into `buf`.
    ///
    /// Reads until `buf` is filled completely, i.e. will not do short reads.  When reaching the
    /// end of file, the rest of `buf` is filled with 0.
    ///
    /// Checks alignment.  If anything does not meet the requirements, enforces it (using ephemeral
    /// bounce buffers).
    #[allow(async_fn_in_trait)] // No need for Send
    async fn read<'a>(&'a self, buf: impl Into<IoVectorMut<'a>>, offset: u64) -> io::Result<()>;

    /// Write data from `buf` to `offset`.
    ///
    /// Writes all data from `buf`, i.e. will not do short writes.  When reaching the end of file,
    /// it is grown as necessary so that the new end of file will be at `offset + buf.len()`.
    ///
    /// If growing is not possible, expect writes beyond the end of file (even if only partially)
    /// to fail.
    ///
    /// Checks alignment.  If anything does not meet the requirements, enforces it using bounce
    /// buffers and a read-modify-write cycle that blocks concurrent writes to the affected area.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn write<'a>(&'a self, buf: impl Into<IoVector<'a>>, offset: u64) -> io::Result<()>;

    /// Ensure the given range reads back as zeroes.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()>;

    /// Ensure the given range is allocated and reads back as zeroes.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn write_allocated_zeroes(&self, offset: u64, length: u64) -> io::Result<()>;

    /// Discard the given range, with undefined contents when read back.
    ///
    /// Tell the storage layer this range is no longer needed and need not be backed by actual
    /// storage.  When read back, the data read will be undefined, i.e. not necessarily zeroes.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn discard(&self, offset: u64, length: u64) -> io::Result<()>;

    /// Await concurrent strong write blockers for the given range.
    ///
    /// Strong write blockers are set up for writes that must not be intersected by any other
    /// write.  Await such intersecting concurrent write requests, and return a guard that will
    /// delay such new writes until the guard is dropped.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn weak_write_blocker(&self, range: Range<u64>) -> RangeBlockedGuard<'_>;

    /// Await any concurrent write request for the given range.
    ///
    /// Block the given range for any concurrent write requests until the returned guard object is
    /// dropped.  Existing requests are awaited, and new ones will be delayed.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn strong_write_blocker(&self, range: Range<u64>) -> RangeBlockedGuard<'_>;
}

#[maybe_async(AFIT)]
impl<S: Storage> StorageExt for S {
    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        self.readv_counted(bufv, offset).await.map(|_| ())
    }

    async fn readv_counted(
        &self,
        mut bufv: IoVectorMut<'_>,
        offset: u64,
    ) -> io::Result<Option<u64>> {
        if bufv.is_empty() {
            return Ok(Some(0));
        }

        let request_len = bufv.len();
        let unpadded_end = offset
            .checked_add(request_len)
            .ok_or_else(|| io::Error::other("Read wrap-around"))?;

        let mem_align = self.mem_align();
        let req_align = self.req_align();

        if is_aligned(&bufv, offset, mem_align, req_align) {
            return unsafe { self.pure_readv_counted(bufv, offset) }.await;
        }

        trace!(
            "Unaligned read: 0x{offset:x} + {} (size: {:#x})",
            bufv.len(),
            self.size().unwrap()
        );

        let req_align_mask = req_align as u64 - 1;
        // Length must be aligned to both memory and request alignments
        let len_align_mask = req_align_mask | (mem_align as u64 - 1);
        debug_assert!((len_align_mask + 1).is_multiple_of(req_align as u64));

        let padded_offset = offset & !req_align_mask;
        // This will over-align at the end of file (aligning to exactly the end of file would be
        // sufficient), but it is easier this way.
        let padded_end = unpadded_end
            .checked_add(req_align_mask)
            .ok_or_else(|| io::Error::other("Read wrap-around"))?
            & !req_align_mask;
        // Now also align to memory alignment
        let padded_len = (padded_end - padded_offset + len_align_mask) & !(len_align_mask);
        let padded_end = padded_offset
            .checked_add(padded_len)
            .ok_or_else(|| io::Error::other("Read wrap-around"))?;

        let padded_len: usize = (padded_end - padded_offset)
            .try_into()
            .map_err(|e| io::Error::other(format!("Cannot realign read: {e}")))?;

        trace!("Padded read: {padded_offset:#x} + {padded_len}");

        let mut bounce_buf = IoBuffer::new(padded_len, mem_align)?;
        let actual_len =
            unsafe { self.pure_readv_counted(bounce_buf.as_mut().into(), padded_offset) }.await?;

        let in_buf_ofs = (offset - padded_offset) as usize;
        // Must fit in `usize` because `padded_len: usize`
        let in_buf_end = (unpadded_end - padded_offset) as usize;

        bufv.copy_from_slice(bounce_buf.as_ref_range(in_buf_ofs..in_buf_end).into_slice());

        Ok(actual_len.map(|len| len.saturating_sub(in_buf_ofs as u64).min(request_len)))
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        if bufv.is_empty() {
            return Ok(());
        }

        let unpadded_end = offset
            .checked_add(bufv.len())
            .ok_or_else(|| io::Error::other("Write wrap-around"))?;

        let mem_align = self.mem_align();
        let req_align = self.req_align();

        if is_aligned(&bufv, offset, mem_align, req_align) {
            let _sw_guard = self.weak_write_blocker(offset..unpadded_end).await;

            // Safe: Alignment checked, and weak write blocker set up
            return unsafe { self.pure_writev(bufv, offset) }.await;
        }

        trace!(
            "Unaligned write: {offset:#x} + {} (size: {:#x})",
            bufv.len(),
            self.size().unwrap()
        );

        let req_align_mask = req_align - 1;
        // Length must be aligned to both memory and request alignments
        let len_align_mask = req_align_mask | (mem_align - 1);
        let len_align = req_align_mask + 1;
        debug_assert!(len_align.is_multiple_of(req_align));

        let padded_offset = offset & !(req_align_mask as u64);
        // This will over-align at the end of file (aligning to exactly the end of file would be
        // sufficient), but it is easier this way.  Small TODO, as this will indeed increase the
        // file length (which the over-alignment in `unaligned_readv()` does not).
        let padded_end = unpadded_end
            .checked_add(req_align_mask as u64)
            .ok_or_else(|| io::Error::other("Write wrap-around"))?
            & !(req_align_mask as u64);
        // Now also align to memory alignment
        let padded_len =
            (padded_end - padded_offset + len_align_mask as u64) & !(len_align_mask as u64);
        let padded_end = padded_offset
            .checked_add(padded_len)
            .ok_or_else(|| io::Error::other("Write wrap-around"))?;

        let padded_len: usize = (padded_end - padded_offset)
            .try_into()
            .map_err(|e| io::Error::other(format!("Cannot realign write: {e}")))?;

        trace!("Padded write: {padded_offset:#x} + {padded_len}");

        let mut bounce_buf = IoBuffer::new(padded_len, mem_align)?;
        assert!(padded_len >= len_align && padded_len & len_align_mask == 0);

        // For the strong blocker, just the RMW regions (head and tail) would be enough.  However,
        // we don’t expect any concurrent writes to the non-RMW (pure write) regions (it is
        // unlikely that the guest would write to the same area twice concurrently), so we don’t
        // need to optimize for it.  On the other hand, writes to the RMW regions are likely
        // (adjacent writes), so those will be blocked either way.
        // Instating fewer blockers makes them less expensive to check, though.
        let _sw_guard = self.strong_write_blocker(padded_offset..padded_end).await;

        let in_buf_ofs = (offset - padded_offset) as usize;
        // Must fit in `usize` because `padded_len: usize`
        let in_buf_end = (unpadded_end - padded_offset) as usize;

        // RMW part 1: Read

        let head_len = in_buf_ofs;
        let aligned_head_len = (head_len + len_align_mask) & !len_align_mask;

        let tail_len = padded_len - in_buf_end;
        let aligned_tail_len = (tail_len + len_align_mask) & !len_align_mask;

        if aligned_head_len + aligned_tail_len == padded_len {
            // Must read the whole bounce buffer
            // Safe: Alignment enforced
            unsafe { self.pure_readv(bounce_buf.as_mut().into(), padded_offset) }.await?;
        } else {
            if aligned_head_len > 0 {
                let head_bufv = bounce_buf.as_mut_range(0..aligned_head_len).into();
                // Safe: Alignment enforced
                unsafe { self.pure_readv(head_bufv, padded_offset) }.await?;
            }
            if aligned_tail_len > 0 {
                let tail_start = padded_len - aligned_tail_len;
                let tail_bufv = bounce_buf.as_mut_range(tail_start..padded_len).into();
                // Safe: Alignment enforced
                unsafe { self.pure_readv(tail_bufv, padded_offset + tail_start as u64) }.await?;
            }
        }

        // RMW part 2: Modify
        bufv.copy_into_slice(bounce_buf.as_mut_range(in_buf_ofs..in_buf_end).into_slice());

        // RMW part 3: Write
        // Safe: Alignment enforced, and strong write blocker set up
        unsafe { self.pure_writev(bounce_buf.as_ref().into(), padded_offset) }.await
    }

    async fn read<'a>(&'a self, buf: impl Into<IoVectorMut<'a>>, offset: u64) -> io::Result<()> {
        self.readv(buf.into(), offset).await
    }

    async fn write<'a>(&'a self, buf: impl Into<IoVector<'a>>, offset: u64) -> io::Result<()> {
        self.writev(buf.into(), offset).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        write_efficient_zeroes(self, offset, length, false).await
    }

    async fn write_allocated_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        write_efficient_zeroes(self, offset, length, true).await
    }

    async fn discard(&self, offset: u64, length: u64) -> io::Result<()> {
        let discard_align = self.discard_align();
        debug_assert!(discard_align.is_power_of_two());
        let align_mask = discard_align as u64 - 1;

        let unaligned_end = offset
            .checked_add(length)
            .ok_or_else(|| io::Error::other("Discard wrap-around"))?;
        let aligned_offset = offset
            .checked_add(align_mask)
            .ok_or_else(|| io::Error::other("Discard wrap-around"))?
            & !align_mask;
        let aligned_end = unaligned_end & !align_mask;

        if aligned_end > aligned_offset {
            let _sw_guard = self.weak_write_blocker(aligned_offset..aligned_end).await;
            let aligned_len = aligned_end - aligned_offset;
            // Safe: Alignment checked, and weak write blocker set up
            if let Err(err) = unsafe { self.pure_discard(aligned_offset, aligned_len) }.await {
                // Ignore ENOTSUP errors: Where the fall-back for write-zeroes in case of ENOTSUP
                // is `write_full_zeroes()`, in case of discard, we don’t need to do anything,
                // because the state after discard is undefined anyway (so a no-op is OK).  So the
                // fall-back is just to return `Ok(())`.
                if err.kind() != io::ErrorKind::Unsupported {
                    return Err(err);
                }
            }
        }

        // Nothing to do for the unaligned part; discarding is always just advisory.

        Ok(())
    }

    async fn weak_write_blocker(&self, range: Range<u64>) -> RangeBlockedGuard<'_> {
        self.get_storage_helper().weak_write_blocker(range).await
    }

    async fn strong_write_blocker(&self, range: Range<u64>) -> RangeBlockedGuard<'_> {
        self.get_storage_helper().strong_write_blocker(range).await
    }
}

/// Check whether the given request is aligned.
fn is_aligned<V: IoVectorTrait>(bufv: &V, offset: u64, mem_align: usize, req_align: usize) -> bool {
    debug_assert!(mem_align.is_power_of_two() && req_align.is_power_of_two());

    let req_align_mask = req_align as u64 - 1;

    if offset & req_align_mask != 0 {
        false
    } else if bufv.len() & req_align_mask == 0 {
        bufv.is_aligned(mem_align, req_align)
    } else {
        false
    }
}

/// Write zero data to the given area.
///
/// In contrast to `write_zeroes()` functions, this one will actually write zero data, fully
/// allocated.
#[maybe_async]
pub(crate) async fn write_full_zeroes<S: StorageExt>(
    storage: S,
    mut offset: u64,
    mut length: u64,
) -> io::Result<()> {
    let req_align = storage.req_align();
    let req_align_mask = (req_align - 1) as u64;

    let mem_align = storage.mem_align();
    let max_chunk_len = cmp::max(cmp::max(req_align, mem_align), 1048576) as u64;
    let buflen = cmp::min(length, max_chunk_len) as usize;
    let mut buf = IoBuffer::new(buflen, storage.mem_align())?;
    buf.as_mut().into_slice().fill(0);

    while length > 0 {
        let mut chunk_length = cmp::min(length, max_chunk_len) as usize;
        if offset & req_align_mask != 0 {
            chunk_length = cmp::min(chunk_length, req_align - (offset & req_align_mask) as usize);
        }
        storage
            .write(buf.as_ref_range(0..chunk_length), offset)
            .await?;
        offset += chunk_length as u64;
        length -= chunk_length as u64;
    }

    Ok(())
}

/// Write zeroes efficiently to the given area.
///
/// This implements `write_zeroes()` and `write_allocated_zeroes()`.
///
/// If `allocate` is `true`, use [`Storage::pure_write_allocated_zeroes()`]; else, use
/// [`Storage::pure_write_zeroes()`].
///
/// If the `pure_*` call fails with [`io::ErrorKind::Unsupported`], fall back to
/// [`write_full_zeroes()`].
#[maybe_async]
pub(crate) async fn write_efficient_zeroes<S: StorageExt>(
    storage: S,
    offset: u64,
    length: u64,
    allocate: bool,
) -> io::Result<()> {
    if length == 0 {
        return Ok(());
    }

    let zero_align = storage.zero_align();
    debug_assert!(zero_align.is_power_of_two());
    let align_mask = zero_align as u64 - 1;

    let unaligned_end = offset
        .checked_add(length)
        .ok_or_else(|| io::Error::other("Zero-write wrap-around"))?;
    let aligned_offset = offset
        .checked_add(align_mask)
        .ok_or_else(|| io::Error::other("Zero-write wrap-around"))?
        & !align_mask;
    let aligned_end = unaligned_end & !align_mask;

    if aligned_end > aligned_offset {
        let result = {
            let _sw_guard = storage
                .weak_write_blocker(aligned_offset..aligned_end)
                .await;
            // Safe: Alignment checked, and weak write blocker set up
            if allocate {
                unsafe {
                    storage
                        .pure_write_allocated_zeroes(aligned_offset, aligned_end - aligned_offset)
                }
                .await
            } else {
                unsafe { storage.pure_write_zeroes(aligned_offset, aligned_end - aligned_offset) }
                    .await
            }
        };
        if let Err(err) = result {
            return if err.kind() == io::ErrorKind::Unsupported {
                write_full_zeroes(storage, offset, length).await
            } else {
                Err(err)
            };
        }
    }

    let zero_buf = if aligned_offset > offset || aligned_end < unaligned_end {
        let buflen = if aligned_offset >= aligned_end {
            unaligned_end - offset
        } else {
            cmp::max(aligned_offset - offset, unaligned_end - aligned_end)
        };
        let mut buf = IoBuffer::new(buflen as usize, storage.mem_align())?;
        buf.as_mut().into_slice().fill(0);
        Some(buf)
    } else {
        None
    };

    if aligned_offset >= aligned_end {
        let buf = zero_buf
            .as_ref()
            .unwrap()
            .as_ref_range(0..((unaligned_end - offset) as usize));
        storage.write(buf, offset).await?;
    } else {
        if aligned_offset > offset {
            assert!(aligned_offset <= unaligned_end);
            let buf = zero_buf
                .as_ref()
                .unwrap()
                .as_ref_range(0..((aligned_offset - offset) as usize));
            storage.write(buf, offset).await?;
        }
        if aligned_end < unaligned_end {
            assert!(aligned_end >= offset);
            let buf = zero_buf
                .as_ref()
                .unwrap()
                .as_ref_range(0..((unaligned_end - aligned_end) as usize));
            storage.write(buf, aligned_end).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::mem_storage::{MemStorage, MemStorageState};
    use std::sync::Arc;

    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn readv_counted_legacy_backend_returns_unknown() -> io::Result<()> {
        let storage: Box<dyn crate::storage::DynStorage> = Box::new(crate::null::Null::new(4));
        let mut buf = [0xff; 8];
        let count = storage.readv_counted((&mut buf[..]).into(), 0).await?;
        assert_eq!(count, None);
        assert_eq!(buf, [0; 8]);
        Ok(())
    }

    /// Requests whose end, or padded end, is beyond `u64::MAX` must return an error.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn readv_writev_overflow() {
        let state = Arc::new(MemStorageState::default());
        let mut buf = vec![0u8; 8192];

        // (`.await` must not be used inside of macros, or `maybe_async` will not remove it)

        // Aligned: end overflows
        let mem = MemStorage::new(Arc::clone(&state), false);
        let offset = u64::MAX - 4095;
        let result = mem.read(&mut buf[..], offset).await;
        assert!(result.is_err());
        let result = mem.write(&buf[..], offset).await;
        assert!(result.is_err());

        // Unaligned (request alignment 4096): end overflows
        let mem = MemStorage::new(state, false).with_align(4096);
        let offset = u64::MAX - 10;
        let result = mem.read(&mut buf[..100], offset).await;
        assert!(result.is_err());
        let result = mem.write(&buf[..100], offset).await;
        assert!(result.is_err());

        // Unaligned: end does not overflow, but padded end does
        let offset = u64::MAX - 100;
        let result = mem.read(&mut buf[..50], offset).await;
        assert!(result.is_err());
        let result = mem.write(&buf[..50], offset).await;
        assert!(result.is_err());
    }

    /// Discard and write-zeroes requests whose start, aligned up, is beyond `u64::MAX` must return
    /// an error.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn discard_write_zeroes_overflow() {
        // Discard and zero alignment default to the request alignment
        let mem = MemStorage::new(Arc::new(MemStorageState::default()), false).with_align(4096);

        // End does not overflow, but the start aligned up does
        let offset = u64::MAX - 10;
        let result = mem.discard(offset, 5).await;
        assert!(result.is_err());
        let result = mem.write_zeroes(offset, 5).await;
        assert!(result.is_err());
        let result = mem.write_allocated_zeroes(offset, 5).await;
        assert!(result.is_err());
    }
}
