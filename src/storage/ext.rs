//! Provides the `StorageExt` struct for more convenient access.
//!
//! `Storage` is provided by the driver, so is supposed to be simple and only contain what’s
//! necessary.  `StorageExt` builds on that to provide more convenient access, e.g. allows
//! unaligned requests and provides write serialization.

use super::drivers::RangeBlockedGuard;
use crate::io_buffers::{IoBuffer, IoVector, IoVectorBounceBuffers, IoVectorMut, IoVectorTrait};
use crate::Storage;
use std::ops::Range;
use std::{cmp, io};
use tracing::trace;

/// Helper methods for storage objects.
///
/// Provides some more convenient methods for accessing storage objects.
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
    async fn read(&self, buf: impl Into<IoVectorMut<'_>>, offset: u64) -> io::Result<()>;

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
    async fn write(&self, buf: impl Into<IoVector<'_>>, offset: u64) -> io::Result<()>;

    /// Ensure the given range reads back as zeroes.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()>;

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

impl<S: Storage> StorageExt for S {
    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        if bufv.is_empty() {
            return Ok(());
        }

        let mem_align = self.mem_align();
        let req_align = self.req_align();

        if is_aligned(&bufv, offset, mem_align, req_align) {
            // Safe: Alignment checked
            return unsafe { self.pure_readv(bufv, offset) }.await;
        }

        let req_align_mask = req_align as u64 - 1;

        trace!(
            "Unaligned read: 0x{:x} + {} (size: {:#x})",
            offset,
            bufv.len(),
            self.size().unwrap()
        );

        let unpadded_end = offset + bufv.len();
        let padded_offset = offset & !req_align_mask;
        // This will over-align at the end of file (aligning to exactly the end of file would be
        // sufficient), but it is easier this way.
        let padded_end = (unpadded_end + req_align_mask) & !req_align_mask;

        trace!(
            "Padded read: 0x{:x} + {}",
            padded_offset,
            padded_end - padded_offset
        );

        let pad_head_len = (offset - padded_offset) as usize;
        let mut pad_tail_len = (padded_end - unpadded_end) as usize;

        trace!("Head length: {pad_head_len}; tail length: {pad_tail_len}");

        let mut bounce = IoVectorBounceBuffers::default();
        let bufv = bufv.enforce_alignment_for_read(
            mem_align,
            req_align,
            pad_head_len,
            &mut pad_tail_len,
            &mut bounce,
        )?;

        // Safe: Alignment enforced
        unsafe { self.pure_readv(bufv, padded_offset) }.await
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        if bufv.is_empty() {
            return Ok(());
        }

        let mem_align = self.mem_align();
        let req_align = self.req_align();

        if is_aligned(&bufv, offset, mem_align, req_align) {
            let _sw_guard = self.weak_write_blocker(offset..(offset + bufv.len())).await;

            // Safe: Alignment checked, and weak write blocker set up
            return unsafe { self.pure_writev(bufv, offset) }.await;
        }

        let req_align_mask = req_align as u64 - 1;

        trace!(
            "Unaligned write: {:#x} + {} (size: {:#x})",
            offset,
            bufv.len(),
            self.size().unwrap()
        );

        let unpadded_end = offset + bufv.len();
        let padded_offset = offset & !req_align_mask;
        // This will over-align at the end of file (aligning to exactly the end of file would be
        // sufficient), but it is easier this way.  Small TODO, as this will indeed increase the
        // file length (which the over-alignment in `unaligned_readv()` does not).
        let padded_end = (unpadded_end + req_align_mask) & !req_align_mask;

        trace!(
            "Padded write: {:#x} + {}",
            padded_offset,
            padded_end - padded_offset
        );

        let pad_head_len = (offset - padded_offset) as usize;
        let mut pad_tail_len = (padded_end - unpadded_end) as usize;

        trace!("Head length: {pad_head_len}; tail length: {pad_tail_len}");

        let mut bounce = IoVectorBounceBuffers::default();
        let (bufv, unaligned_head, unaligned_tail) = bufv.enforce_alignment_for_write(
            mem_align,
            req_align,
            pad_head_len,
            &mut pad_tail_len,
            &mut bounce,
        )?;

        let padded_end = padded_offset + bufv.len();

        // For the strong blocker, just the RMW regions (head and tail) would be enough.  However,
        // we don’t expect any concurrent writes to the non-RMW (pure write) regions (it is
        // unlikely that the guest would write to the same area twice concurrently), so we don’t
        // need to optimize for it.  On the other hand, writes to the RMW regions are likely
        // (adjacent writes), so those will be blocked either way.
        // Instating fewer blockers makes them less expensive to check, though.
        let _sw_guard = self.strong_write_blocker(padded_offset..padded_end).await;

        let bufv_unwrapped = bufv.into_inner();

        if pad_head_len > 0 && pad_tail_len > 0 && bufv_unwrapped.len() == 1 {
            let unaligned = unaligned_head.unwrap();
            // Covered by `unaligned_head`.
            assert!(unaligned_tail.is_none());

            // Single buffer for both head and tail.  Must be a bounce buffer because of
            // `pad_head_len > 0`, so we can make it mutable.
            let buf = bufv_unwrapped.first().unwrap();
            let buf = unsafe { std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len()) };

            let retain_start = pad_head_len;
            let retain_end = retain_start + unaligned.len() as usize;
            // from the head case below
            let read_len1 = retain_start.next_multiple_of(cmp::max(mem_align, req_align));
            let read_start1 = 0;
            let read_end1 = read_start1 + read_len1;
            // from the tail case below
            let read_len2 = pad_tail_len.next_multiple_of(cmp::max(mem_align, req_align));
            let read_end2 = buf.len();
            let read_start2 = read_end2.checked_sub(read_len2).unwrap();
            trace!(
                "Single buffer RMW, full length: {}; read ranges: {}..{} (from {:#x}) and {}..{} (from {:#x}); retain range: {}..{}",
                buf.len(),
                read_start1,
                read_end1,
                padded_offset,
                read_start2,
                read_end2,
                padded_end - read_len2 as u64,
                retain_start,
                retain_end,
            );

            if read_start2 <= read_end1 {
                trace!(
                    "Unifying read to {}..{} (from {:#x})",
                    read_start1,
                    read_end2,
                    padded_offset
                );
                self.read(&mut buf[read_start1..read_end2], padded_offset)
                    .await?;
            } else {
                self.read(&mut buf[read_start1..read_end1], padded_offset)
                    .await?;
                self.read(
                    &mut buf[read_start2..read_end2],
                    padded_end - read_len2 as u64,
                )
                .await?;
            }
            unaligned.copy_into_slice(&mut buf[retain_start..retain_end]);
        } else {
            if pad_head_len > 0 {
                let unaligned_head = unaligned_head.unwrap();

                // There must be a head bounce buffer because `pad_head_len > 0`.
                let head_buf = bufv_unwrapped.first().unwrap();
                let head_buf = unsafe {
                    std::slice::from_raw_parts_mut(head_buf.as_ptr() as *mut u8, head_buf.len())
                };

                let retain_start = pad_head_len;
                let retain_end = head_buf.len();
                let read_len = retain_start.next_multiple_of(cmp::max(mem_align, req_align));
                let read_start = 0;
                let read_end = read_start + read_len;
                trace!(
                    "Head buffer RMW, head length: {}; read range: {}..{} (from {:#x}; retain range: {}..{}",
                    head_buf.len(),
                    read_start,
                    read_end,
                    padded_offset,
                    retain_start,
                    retain_end,
                );

                assert!(read_end <= head_buf.len());
                self.read(&mut head_buf[read_start..read_end], padded_offset)
                    .await?;

                assert_eq!(retain_end - retain_start, unaligned_head.len() as usize);
                unaligned_head.copy_into_slice(&mut head_buf[retain_start..retain_end]);
            }

            if pad_tail_len > 0 {
                let unaligned_tail = unaligned_tail.unwrap();

                // There must be a tail bounce buffer one because `pad_tail_len > 0`.
                let tail_buf = bufv_unwrapped.last().unwrap();
                let tail_buf = unsafe {
                    std::slice::from_raw_parts_mut(tail_buf.as_ptr() as *mut u8, tail_buf.len())
                };

                let retain_start = 0;
                let retain_end = tail_buf.len() - pad_tail_len;
                let read_len = pad_tail_len.next_multiple_of(cmp::max(mem_align, req_align));
                let read_end = tail_buf.len();
                let read_start = read_end.checked_sub(read_len).unwrap();
                trace!(
                    "Tail buffer RMW, tail length: {}; read range: {}..{} (from {:#x}); retain range: {}..{}",
                    tail_buf.len(),
                    read_start,
                    read_end,
                    padded_end - read_len as u64,
                    retain_start,
                    retain_end,
                );

                self.read(
                    &mut tail_buf[read_start..read_end],
                    padded_end - read_len as u64,
                )
                .await?;

                assert_eq!(retain_end - retain_start, unaligned_tail.len() as usize);
                unaligned_tail.copy_into_slice(&mut tail_buf[retain_start..retain_end]);
            }
        }

        let bufv = bufv_unwrapped.into();

        // Safe: Alignment enforced, and strong write blocker set up
        unsafe { self.pure_writev(bufv, padded_offset) }.await
    }

    async fn read(&self, buf: impl Into<IoVectorMut<'_>>, offset: u64) -> io::Result<()> {
        self.readv(buf.into(), offset).await
    }

    async fn write(&self, buf: impl Into<IoVector<'_>>, offset: u64) -> io::Result<()> {
        self.writev(buf.into(), offset).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        let zero_align = self.zero_align();
        debug_assert!(zero_align.is_power_of_two());
        let align_mask = zero_align as u64 - 1;

        let unaligned_end = offset
            .checked_add(length)
            .ok_or_else(|| io::Error::other("Zero-write wrap-around"))?;
        let aligned_offset = (offset + align_mask) & !align_mask;
        let aligned_end = unaligned_end & !align_mask;

        if aligned_end > aligned_offset {
            let _sw_guard = self.weak_write_blocker(aligned_offset..aligned_end).await;
            // Safe: Alignment checked, and weak write blocker set up
            unsafe { self.pure_write_zeroes(aligned_offset, aligned_end - aligned_offset) }.await?;
        }

        let zero_buf = if aligned_offset > offset || aligned_end < unaligned_end {
            let mut buf = IoBuffer::new(
                cmp::max(aligned_offset - offset, unaligned_end - aligned_end) as usize,
                self.mem_align(),
            )?;
            buf.as_mut().into_slice().fill(0);
            Some(buf)
        } else {
            None
        };

        if aligned_offset > offset {
            let buf = zero_buf
                .as_ref()
                .unwrap()
                .as_ref_range(0..((aligned_offset - offset) as usize));
            self.write(buf, offset).await?;
        }
        if aligned_end < unaligned_end {
            let buf = zero_buf
                .as_ref()
                .unwrap()
                .as_ref_range(0..((unaligned_end - aligned_end) as usize));
            self.write(buf, aligned_end).await?;
        }

        Ok(())
    }

    async fn discard(&self, offset: u64, length: u64) -> io::Result<()> {
        let discard_align = self.discard_align();
        debug_assert!(discard_align.is_power_of_two());
        let align_mask = discard_align as u64 - 1;

        let unaligned_end = offset
            .checked_add(length)
            .ok_or_else(|| io::Error::other("Discard wrap-around"))?;
        let aligned_offset = (offset + align_mask) & !align_mask;
        let aligned_end = unaligned_end & !align_mask;

        if aligned_end > aligned_offset {
            let _sw_guard = self.weak_write_blocker(offset..(offset + length)).await;
            // Safe: Alignment checked, and weak write blocker set up
            unsafe { self.pure_discard(offset, length) }.await?;
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
