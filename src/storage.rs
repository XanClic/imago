//! Helper functionality to access storage.
//!
//! While not the primary purpose of this crate, to open VM images, we need to be able to access
//! different kinds of storage objects.  Such objects are abstracted behind the `Storage` trait.

use crate::io_buffers::{
    IoBuffer, IoBufferRefTrait, IoVector, IoVectorBounceBuffers, IoVectorMut, IoVectorTrait,
};
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::{cmp, io};
use tracing::trace;

/// Parameters from which a storage object can be constructed.
#[derive(Clone, Default)]
pub struct StorageOpenOptions {
    /// Filename to open.
    pub(crate) filename: Option<PathBuf>,

    /// Whether the object should be opened as writable or read-only.
    pub(crate) writable: bool,

    /// Whether to bypass the host page cache (if applicable).
    pub(crate) direct: bool,
}

/// Implementation for storage objects.
pub trait Storage: Debug + Display + Send + Sized + Sync {
    /// Open a storage object.
    ///
    /// Different storage implementations may require different options.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn open(_opts: StorageOpenOptions) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Cannot open storage objects of type {}",
                std::any::type_name::<Self>()
            ),
        ))
    }

    /// Minimum required alignment for memory buffers.
    fn mem_align(&self) -> usize {
        1
    }

    /// Minimum required alignment for offsets and lengths.
    fn req_align(&self) -> usize {
        1
    }

    /// Storage object length.
    fn size(&self) -> io::Result<u64>;

    /// Read data at `offset` into `bufv`.
    ///
    /// Reads until `bufv` is filled completely, i.e. will not do short reads.  When reaching the
    /// end of file, the rest of `bufv` is filled with 0.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()>;

    /// Write data from `bufv` to `offset`.
    ///
    /// Writes all data from `bufv`, i.e. will not do short writes.  When reaching the end of file,
    /// grow it as necessary so that the new end of file will be at `offset + bufv.len()`.
    ///
    /// If growing is not possible, writes beyond the end of file (even if only partially) should
    /// fail.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()>;

    /// Ensure the given range reads back as zeroes.
    ///
    /// The default implementation writes actual zeroes as data, which is inefficient.  Storage
    /// drivers should override it with a more efficient implementation.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn write_zeroes(&self, mut offset: u64, mut length: u64) -> io::Result<()> {
        let buflen = cmp::min(length, 1048576) as usize;
        let mut buf = IoBuffer::new(buflen, self.mem_align())?;
        buf.as_mut().into_slice().fill(0);

        while length > 0 {
            let chunk_length = cmp::min(length, 1048576) as usize;
            self.writev(buf.as_ref_range(0..chunk_length).into(), offset)
                .await?;
            offset += chunk_length as u64;
            length -= chunk_length as u64;
        }

        Ok(())
    }

    /// Discard the given range, with undefined contents when read back.
    ///
    /// Tell the storage layer this range is no longer needed and need not be backed by actual
    /// storage.  When read back, the data read will be undefined, i.e. not necessarily zeroes.
    ///
    /// No-op implementations therefore explicitly fulfill the interface contract.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn discard(&self, _offset: u64, _length: u64) -> io::Result<()> {
        Ok(())
    }

    /// Flush internal buffers.
    ///
    /// Does not necessarily sync those buffers to disk.  When using `flush()`, consider whether
    /// you want to call `sync()` afterwards.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn flush(&self) -> io::Result<()>;

    /// Sync data already written to the storage hardware.
    ///
    /// This does not necessarily include flushing internal buffers, i.e. `flush`.  When using
    /// `sync()`, consider whether you want to call `flush()` before it.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn sync(&self) -> io::Result<()>;
}

/// Wrapper around storage driver implementations to provide additional common functionality.
#[derive(Debug)]
pub struct StorageWrapper<S: Storage> {
    /// Storage driver instance.
    inner: S,
}

impl<S: Storage> StorageWrapper<S> {
    /// Minimum required alignment for memory buffers.
    pub fn mem_align(&self) -> usize {
        self.inner.mem_align()
    }

    /// Minimum required alignment for offsets and lengths.
    pub fn req_align(&self) -> usize {
        self.inner.req_align()
    }

    /// Storage object length.
    pub fn size(&self) -> io::Result<u64> {
        self.inner.size()
    }

    /// Read data at `offset` into `bufv`.
    ///
    /// Reads until `bufv` is filled completely, i.e. will not do short reads.  When reaching the
    /// end of file, the rest of `bufv` is filled with 0.
    ///
    /// Everything must be aligned properly.
    pub async fn aligned_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        self.inner.readv(bufv, offset).await
    }

    /// Write data from `bufv` to `offset`.
    ///
    /// Writes all data from `bufv`, i.e. will not do short writes.  When reaching the end of file,
    /// grow it as necessary so that the new end of file will be at `offset + bufv.len()`.
    ///
    /// If growing is not possible, writes beyond the end of file (even if only partially) should
    /// fail.
    ///
    /// Everything must be aligned properly.
    pub async fn aligned_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        // TODO await serializing writes
        self.inner.writev(bufv, offset).await
    }

    /// Ensure the given range reads back as zeroes.
    ///
    /// The default implementation writes actual zeroes as data, which is inefficient.  Storage
    /// drivers should override it with a more efficient implementation.
    pub async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        // TODO await serializing writes
        self.inner.write_zeroes(offset, length).await
    }

    /// Discard the given range, with undefined contents when read back.
    ///
    /// Tell the storage layer this range is no longer needed and need not be backed by actual
    /// storage.  When read back, the data read will be undefined, i.e. not necessarily zeroes.
    ///
    /// No-op implementations therefore explicitly fulfill the interface contract.
    pub async fn discard(&self, offset: u64, length: u64) -> io::Result<()> {
        // TODO await serializing writes
        self.inner.discard(offset, length).await
    }

    /// Flush internal buffers.
    ///
    /// Does not necessarily sync those buffers to disk.  When using `flush()`, consider whether
    /// you want to call `sync()` afterwards.
    pub async fn flush(&self) -> io::Result<()> {
        self.inner.flush().await
    }

    /// Sync data already written to the storage hardware.
    ///
    /// This does not necessarily include flushing internal buffers, i.e. `flush`.  When using
    /// `sync()`, consider whether you want to call `flush()` before it.
    pub async fn sync(&self) -> io::Result<()> {
        self.inner.sync().await
    }

    /// Read data at `offset` into `buf`.
    #[allow(async_fn_in_trait)] // No need for Send
    pub async fn read(&self, buf: impl Into<IoVectorMut<'_>>, offset: u64) -> io::Result<()> {
        self.unaligned_readv(buf.into(), offset).await
    }

    /// Read data at `offset` into `bufv`.
    ///
    /// Check alignment.  If anything does not meet the requirements, enforce it.
    pub async fn unaligned_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        if bufv.is_empty() {
            return Ok(());
        }

        let mem_align = self.mem_align();
        let req_align = self.req_align();

        if is_aligned(&bufv, offset, mem_align, req_align, self.size().ok())? {
            return self.inner.readv(bufv, offset).await;
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
        self.inner.readv(bufv, padded_offset).await?;
        Ok(())
    }

    /// Write data from `buf` to `offset`.
    #[allow(async_fn_in_trait)] // No need for Send
    pub async fn write(&self, buf: impl Into<IoVector<'_>>, offset: u64) -> io::Result<()> {
        self.unaligned_writev(buf.into(), offset).await
    }

    /// Write data from `bufv` to `offset`.
    ///
    /// Check alignment.  If anything does not meet the requirements, enforce it.
    #[allow(async_fn_in_trait)] // No need for Send
    pub async fn unaligned_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        // TODO await serializing writes
        if bufv.is_empty() {
            return Ok(());
        }

        let mem_align = self.mem_align();
        let req_align = self.req_align();

        if is_aligned(&bufv, offset, mem_align, req_align, self.size().ok())? {
            return self.inner.writev(bufv, offset).await;
        }

        // FIXME: Write serialization

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
        self.inner.writev(bufv, padded_offset).await?;
        Ok(())
    }
}

impl<S: Storage> From<S> for StorageWrapper<S> {
    fn from(inner: S) -> Self {
        StorageWrapper { inner }
    }
}

/// Check whether the given request is aligned.
fn is_aligned<V: IoVectorTrait>(
    bufv: &V,
    offset: u64,
    mem_align: usize,
    req_align: usize,
    size: Option<u64>,
) -> io::Result<bool> {
    debug_assert!(mem_align.is_power_of_two() && req_align.is_power_of_two());

    let req_align_mask = req_align as u64 - 1;

    Ok(if offset & req_align_mask != 0 {
        false
    } else if bufv.len() & req_align_mask == 0 {
        bufv.is_aligned(mem_align, req_align, false)
    } else if bufv.is_aligned(mem_align, req_align, true) {
        if let Some(size) = size {
            let end = offset
                .checked_add(bufv.len())
                .ok_or_else(|| io::Error::other("Write wrap-around"))?;
            end == size
        } else {
            false
        }
    } else {
        false
    })
}

/// Allow dynamic use of storage objects (i.e. is object safe).
///
/// When using normal `Storage` objects, they must all be of the same type within a single disk
/// image chain.  For example, every storage object underneath a `FormatAccess<StdFile>` object
/// must be of type `StdFile`.
///
/// `DynStorage` allows the use of `Box<dyn DynStorage>`, which implements `Storage`, to allow
/// mixed storage object types.  Therefore, a `FormatAccess<Box<dyn DynStorage>>` allows e.g. the
/// use of both `Box<StdFile>` and `Box<Null>` storage objects together.  (`Arc` instead of `Box`
/// works, too.)
///
/// Async functions in `DynStorage` return boxed futures (`Pin<Box<dyn Future>>`), which makes them
/// slighly less efficient than async functions in `Storage`, hence the distinction.
pub trait DynStorage: Debug + Display + Send + Sync {
    /// Wrapper around [`Storage::mem_align()`].
    fn mem_align(&self) -> usize;

    /// Wrapper around [`Storage::req_align()`].
    fn req_align(&self) -> usize;

    /// Wrapper around [`Storage::size()`].
    fn size(&self) -> io::Result<u64>;

    /// Object-safe wrapper around [`Storage::readv()`].
    fn readv<'a>(
        &'a self,
        bufv: IoVectorMut<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>;

    /// Object-safe wrapper around [`Storage::writev()`].
    fn writev<'a>(
        &'a self,
        bufv: IoVector<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>;

    /// Object-safe wrapper around [`Storage::write_zeroes()`].
    fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::discard()`].
    fn discard(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::flush()`].
    fn flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::sync()`].
    fn sync(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;
}

impl<S: Storage> Storage for &S {
    fn mem_align(&self) -> usize {
        (*self).mem_align()
    }

    fn req_align(&self) -> usize {
        (*self).req_align()
    }

    fn size(&self) -> io::Result<u64> {
        (*self).size()
    }

    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        (*self).readv(bufv, offset).await
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        (*self).writev(bufv, offset).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        (*self).write_zeroes(offset, length).await
    }

    async fn discard(&self, offset: u64, length: u64) -> io::Result<()> {
        (*self).discard(offset, length).await
    }

    async fn flush(&self) -> io::Result<()> {
        (*self).flush().await
    }

    async fn sync(&self) -> io::Result<()> {
        (*self).sync().await
    }
}

impl<S: Storage> DynStorage for S {
    fn mem_align(&self) -> usize {
        S::mem_align(self)
    }

    fn req_align(&self) -> usize {
        S::req_align(self)
    }

    fn size(&self) -> io::Result<u64> {
        S::size(self)
    }

    fn readv<'a>(
        &'a self,
        bufv: IoVectorMut<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        Box::pin(S::readv(self, bufv, offset))
    }

    fn writev<'a>(
        &'a self,
        bufv: IoVector<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        Box::pin(S::writev(self, bufv, offset))
    }

    fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(S::write_zeroes(self, offset, length))
    }

    fn discard(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(S::discard(self, offset, length))
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(S::flush(self))
    }

    fn sync(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(S::sync(self))
    }
}

impl Storage for Box<dyn DynStorage> {
    async fn open(_opts: StorageOpenOptions) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Cannot open dynamic storage instances",
        ))
    }

    fn mem_align(&self) -> usize {
        <Self as DynStorage>::mem_align(self)
    }

    fn req_align(&self) -> usize {
        <Self as DynStorage>::req_align(self)
    }

    fn size(&self) -> io::Result<u64> {
        <Self as DynStorage>::size(self)
    }

    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        <Self as DynStorage>::readv(self, bufv, offset).await
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        <Self as DynStorage>::writev(self, bufv, offset).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        <Self as DynStorage>::write_zeroes(self, offset, length).await
    }

    async fn discard(&self, offset: u64, length: u64) -> io::Result<()> {
        <Self as DynStorage>::discard(self, offset, length).await
    }

    async fn flush(&self) -> io::Result<()> {
        <Self as DynStorage>::flush(self).await
    }

    async fn sync(&self) -> io::Result<()> {
        <Self as DynStorage>::sync(self).await
    }
}

impl Storage for Arc<dyn DynStorage> {
    async fn open(_opts: StorageOpenOptions) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Cannot open dynamic storage instances",
        ))
    }

    fn mem_align(&self) -> usize {
        <Self as DynStorage>::mem_align(self)
    }

    fn req_align(&self) -> usize {
        <Self as DynStorage>::req_align(self)
    }

    fn size(&self) -> io::Result<u64> {
        <Self as DynStorage>::size(self)
    }

    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        <Self as DynStorage>::readv(self, bufv, offset).await
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        <Self as DynStorage>::writev(self, bufv, offset).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        <Self as DynStorage>::write_zeroes(self, offset, length).await
    }

    async fn discard(&self, offset: u64, length: u64) -> io::Result<()> {
        <Self as DynStorage>::discard(self, offset, length).await
    }

    async fn flush(&self) -> io::Result<()> {
        <Self as DynStorage>::flush(self).await
    }

    async fn sync(&self) -> io::Result<()> {
        <Self as DynStorage>::sync(self).await
    }
}

impl StorageOpenOptions {
    /// Create default options.
    pub fn new() -> Self {
        StorageOpenOptions::default()
    }

    /// Set a filename to open.
    pub fn filename<P: AsRef<Path>>(mut self, filename: P) -> Self {
        self.filename = Some(filename.as_ref().to_owned());
        self
    }

    /// Whether the storage should be writable or not.
    pub fn write(mut self, write: bool) -> Self {
        self.writable = write;
        self
    }

    /// Whether to bypass the host page cache (if applicable).
    pub fn direct(mut self, direct: bool) -> Self {
        self.direct = direct;
        self
    }
}

impl<S: Storage> Display for StorageWrapper<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        <S as Display>::fmt(&self.inner, f)
    }
}

impl<S: Storage> Deref for StorageWrapper<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.inner
    }
}
