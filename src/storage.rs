//! Helper functionality to access storage.
//!
//! While not the primary purpose of this crate, to open VM images, we need to be able to access
//! different kinds of storage objects.  Such objects are abstracted behind the `Storage` trait.

use crate::io_buffers::{IoBuffer, IoVector, IoVectorBounceBuffers, IoVectorMut};
use std::fmt::{Debug, Display};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::{cmp, io};

/// Parameters from which a storage object can be constructed.
#[derive(Default)]
pub struct StorageOpenOptions {
    /// Filename to open.
    pub(crate) filename: Option<PathBuf>,

    /// Whether the object should be opened as writable or read-only.
    pub(crate) writable: bool,

    /// Whether to bypass the host page cache (if applicable).
    pub(crate) direct: bool,
}

/// Provides access to generic storage objects.
pub trait Storage: Debug + Display + Sized {
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
}

/// Helper methods for storage objects.
///
/// Provides some simpler methods for accessing storage objects.
pub trait StorageExt: Storage {
    /// Read data at `offset` into `buf`.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn read(&self, buf: impl Into<IoVectorMut<'_>>, offset: u64) -> io::Result<()> {
        self.unaligned_readv(buf.into(), offset).await
    }

    /// Read data at `offset` into `bufv`.
    ///
    /// Check alignment.  If anything does not meet the requirements, enforce it.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn unaligned_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        let mem_align = self.mem_align();
        let req_align = self.req_align();
        let req_align_mask = req_align as u64 - 1;

        debug_assert!(mem_align.is_power_of_two() && req_align.is_power_of_two());

        if offset & req_align_mask == 0
            && bufv.len() & req_align_mask == 0
            && bufv.is_aligned(mem_align, req_align)
        {
            return self.readv(bufv, offset).await;
        }

        let unpadded_end = offset + bufv.len();
        let padded_offset = offset & !req_align_mask;
        let padded_end = (unpadded_end + req_align_mask) & !req_align_mask;

        let pad_head_len = (offset - padded_offset) as usize;
        let mut head_buf = (pad_head_len > 0)
            .then(|| IoBuffer::new(pad_head_len, mem_align))
            .transpose()?;

        let pad_tail_len = (padded_end - unpadded_end) as usize;
        let mut tail_buf = (pad_tail_len > 0)
            .then(|| IoBuffer::new(pad_tail_len, mem_align))
            .transpose()?;

        let bufv = if let Some(head_buf) = head_buf.as_mut() {
            bufv.with_inserted(0, head_buf.as_mut().into_slice())
        } else {
            bufv
        };

        let bufv = if let Some(tail_buf) = tail_buf.as_mut() {
            bufv.with_pushed(tail_buf.as_mut().into_slice())
        } else {
            bufv
        };

        let mut bounce = IoVectorBounceBuffers::default();
        let bufv = bufv.enforce_alignment_for_read(mem_align, req_align, &mut bounce)?;
        self.readv(bufv, offset).await
    }

    /// Write data from `buf` to `offset`.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn write(&self, buf: impl Into<IoVector<'_>>, offset: u64) -> io::Result<()> {
        self.unaligned_writev(buf.into(), offset).await
    }

    /// Write data from `bufv` to `offset`.
    ///
    /// Check alignment.  If anything does not meet the requirements, enforce it.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn unaligned_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        let mem_align = self.mem_align();
        let req_align = self.req_align();
        let req_align_mask = req_align as u64 - 1;

        debug_assert!(mem_align.is_power_of_two() && req_align.is_power_of_two());

        if offset & req_align_mask == 0
            && bufv.len() & req_align_mask == 0
            && bufv.is_aligned(mem_align, req_align)
        {
            return self.writev(bufv, offset).await;
        }

        todo!("RMW requires write serialization capabilities")
    }
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
pub trait DynStorage: Debug + Display {
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
}

impl<S: Storage> StorageExt for S {}

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
