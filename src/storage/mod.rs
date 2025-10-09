//! Helper functionality to access storage.
//!
//! While not the primary purpose of this crate, to open VM images, we need to be able to access
//! different kinds of storage objects.  Such objects are abstracted behind the `Storage` trait.

pub mod drivers;
pub mod ext;

use crate::io_buffers::{IoVector, IoVectorMut};
use drivers::CommonStorageHelper;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

/// Parameters from which a storage object can be constructed.
#[derive(Clone, Debug, Default)]
pub struct StorageOpenOptions {
    /// Filename to open.
    pub(crate) filename: Option<PathBuf>,

    /// Whether the object should be opened as writable or read-only.
    pub(crate) writable: bool,

    /// Whether to bypass the host page cache (if applicable).
    pub(crate) direct: bool,

    /// macOS-only: Use fsync() instead of F_FULLFSYNC on `sync()` method.
    #[cfg(target_os = "macos")]
    pub(crate) relaxed_sync: bool,
}

/// Parameters from which a new storage object can be created.
#[derive(Clone, Debug)]
pub struct StorageCreateOptions {
    /// Options to open the image, includes the filename.
    ///
    /// `writable` should be ignored, created files should always be opened as writable.
    pub(crate) open_opts: StorageOpenOptions,

    /// Initial size.
    pub(crate) size: u64,

    /// Preallocation mode.
    pub(crate) prealloc_mode: PreallocateMode,

    /// Whether to overwrite an existing file.
    pub(crate) overwrite: bool,
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

    /// Synchronous wrapper around [`Storage::open()`].
    #[cfg(feature = "sync-wrappers")]
    fn open_sync(opts: StorageOpenOptions) -> io::Result<Self> {
        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(Self::open(opts))
    }

    /// Create a storage object and open it.
    ///
    /// Different storage implementations may require different options.
    ///
    /// Note that newly created storage objects are always opened as writable.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn create_open(_opts: StorageCreateOptions) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Cannot create storage objects of type {}",
                std::any::type_name::<Self>()
            ),
        ))
    }

    /// Create a storage object.
    ///
    /// Different storage implementations may require different options.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn create(opts: StorageCreateOptions) -> io::Result<()> {
        Self::create_open(opts).await?;
        Ok(())
    }

    /// Minimum required alignment for memory buffers.
    fn mem_align(&self) -> usize {
        1
    }

    /// Minimum required alignment for offsets and lengths.
    fn req_align(&self) -> usize {
        1
    }

    /// Minimum required alignment for zero writes.
    fn zero_align(&self) -> usize {
        1
    }

    /// Minimum required alignment for effective discards.
    fn discard_align(&self) -> usize {
        1
    }

    /// Storage object length.
    fn size(&self) -> io::Result<u64>;

    /// Resolve the given path relative to this storage object.
    ///
    /// `relative` need not really be a relative path; it is up to the storage driver to check
    /// whether it is an absolute path that does not need to be changed, or a relative path that
    /// needs to be resolved.
    ///
    /// Must not return a relative path.
    ///
    /// The returned `PathBuf` should be usable with `StorageOpenOptions::filename()`.
    fn resolve_relative_path<P: AsRef<Path>>(&self, _relative: P) -> io::Result<PathBuf> {
        Err(io::ErrorKind::Unsupported.into())
    }

    /// Return a filename, if possible.
    ///
    /// Using the filename for [`StorageOpenOptions::filename()`] should open the same storage
    /// object.
    fn get_filename(&self) -> Option<PathBuf> {
        None
    }

    /// Read data at `offset` into `bufv`.
    ///
    /// Reads until `bufv` is filled completely, i.e. will not do short reads.  When reaching the
    /// end of file, the rest of `bufv` is filled with 0.
    ///
    /// # Safety
    /// This is a pure read from storage.  The request must be fully aligned to
    /// [`Self::mem_align()`] and [`Self::req_align()`], and safeguards we want to implement for
    /// safe concurrent access may not be available.
    ///
    /// Use [`StorageExt::readv()`](crate::StorageExt::readv()) instead.
    #[allow(async_fn_in_trait)] // No need for Send
    async unsafe fn pure_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()>;

    /// Write data from `bufv` to `offset`.
    ///
    /// Writes all data from `bufv`, i.e. will not do short writes.  When reaching the end of file,
    /// grow it as necessary so that the new end of file will be at `offset + bufv.len()`.
    ///
    /// If growing is not possible, writes beyond the end of file (even if only partially) should
    /// fail.
    ///
    /// # Safety
    /// This is a pure write to storage.  The request must be fully aligned to
    /// [`Self::mem_align()`] and [`Self::req_align()`], and safeguards we want to implement for
    /// safe concurrent access may not be available.
    ///
    /// Use [`StorageExt::writev()`](crate::StorageExt::writev()) instead.
    #[allow(async_fn_in_trait)] // No need for Send
    async unsafe fn pure_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()>;

    /// Ensure the given range reads back as zeroes.
    ///
    /// The default implementation writes actual zeroes as data, which is inefficient.  Storage
    /// drivers should override it with a more efficient implementation.
    ///
    /// # Safety
    /// This is a pure write to storage.  The request must be fully aligned to
    /// [`Self::zero_align()`], and safeguards we want to implement for safe concurrent access may
    /// not be available.
    ///
    /// Use [`StorageExt::write_zeroes()`](crate::StorageExt::write_zeroes()) instead.
    #[allow(async_fn_in_trait)] // No need for Send
    async unsafe fn pure_write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        ext::write_full_zeroes(self, offset, length).await
    }

    /// Discard the given range, with undefined contents when read back.
    ///
    /// Tell the storage layer this range is no longer needed and need not be backed by actual
    /// storage.  When read back, the data read will be undefined, i.e. not necessarily zeroes.
    ///
    /// No-op implementations therefore explicitly fulfill the interface contract.
    ///
    /// # Safety
    /// This is a pure write to storage.  The request must be fully aligned to
    /// [`Self::discard_align()`], and safeguards we want to implement for safe concurrent access
    /// may not be available.
    ///
    /// Use [`StorageExt::discard()`](crate::StorageExt::discard()) instead.
    #[allow(async_fn_in_trait)] // No need for Send
    async unsafe fn pure_discard(&self, _offset: u64, _length: u64) -> io::Result<()> {
        Ok(())
    }

    /// Flush internal buffers.
    ///
    /// Does not necessarily sync those buffers to disk.  When using `flush()`, consider whether
    /// you want to call `sync()` afterwards.
    ///
    /// Note that this will not drop the buffers, so they may still be used to serve later
    /// accesses.  Use [`Storage::invalidate_cache()`] to drop all buffers.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn flush(&self) -> io::Result<()>;

    /// Sync data already written to the storage hardware.
    ///
    /// This does not necessarily include flushing internal buffers, i.e. `flush`.  When using
    /// `sync()`, consider whether you want to call `flush()` before it.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn sync(&self) -> io::Result<()>;

    /// Drop internal buffers.
    ///
    /// This drops all internal buffers, but does not flush them!  All cached data is reloaded on
    /// subsequent accesses.
    ///
    /// # Safety
    /// Not flushing internal buffers may cause corruption.  You must ensure the underlying storage
    /// state is consistent.
    #[allow(async_fn_in_trait)] // No need for Send
    async unsafe fn invalidate_cache(&self) -> io::Result<()>;

    /// Return the storage helper object (used by the [`StorageExt`](crate::StorageExt)
    /// implementation).
    fn get_storage_helper(&self) -> &CommonStorageHelper;

    /// Resize to the given size.
    ///
    /// Set the size of this storage object to `new_size`.  If `new_size` is smaller than the
    /// current size, ignore `prealloc_mode` and discard the data after `new_size`.
    ///
    /// If `new_size` is larger than the current size, `prealloc_mode` determines whether and how
    /// the new range should be allocated; it is possible some preallocation modes are not
    /// supported, in which case an [`std::io::ErrorKind::Unsupported`] is returned.
    #[allow(async_fn_in_trait)] // No need for Send
    async fn resize(&self, _new_size: u64, _prealloc_mode: PreallocateMode) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
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
pub trait DynStorage: Any + Debug + Display + Send + Sync {
    /// Wrapper around [`Storage::mem_align()`].
    fn dyn_mem_align(&self) -> usize;

    /// Wrapper around [`Storage::req_align()`].
    fn dyn_req_align(&self) -> usize;

    /// Wrapper around [`Storage::zero_align()`].
    fn dyn_zero_align(&self) -> usize;

    /// Wrapper around [`Storage::discard_align()`].
    fn dyn_discard_align(&self) -> usize;

    /// Wrapper around [`Storage::size()`].
    fn dyn_size(&self) -> io::Result<u64>;

    /// Wrapper around [`Storage::resolve_relative_path()`].
    fn dyn_resolve_relative_path(&self, relative: &Path) -> io::Result<PathBuf>;

    /// Wrapper around [`Storage::get_filename()`]
    fn dyn_get_filename(&self) -> Option<PathBuf>;

    /// Object-safe wrapper around [`Storage::pure_readv()`].
    ///
    /// # Safety
    /// Same considerations are for [`Storage::pure_readv()`] apply.
    unsafe fn dyn_pure_readv<'a>(
        &'a self,
        bufv: IoVectorMut<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>;

    /// Object-safe wrapper around [`Storage::pure_writev()`].
    ///
    /// # Safety
    /// Same considerations are for [`Storage::pure_writev()`] apply.
    unsafe fn dyn_pure_writev<'a>(
        &'a self,
        bufv: IoVector<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>;

    /// Object-safe wrapper around [`Storage::pure_write_zeroes()`].
    ///
    /// # Safety
    /// Same considerations are for [`Storage::pure_write_zeroes()`] apply.
    unsafe fn dyn_pure_write_zeroes(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::pure_discard()`].
    ///
    /// # Safety
    /// Same considerations are for [`Storage::pure_discard()`] apply.
    unsafe fn dyn_pure_discard(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::flush()`].
    fn dyn_flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::sync()`].
    fn dyn_sync(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Object-safe wrapper around [`Storage::invalidate_cache()`].
    ///
    /// # Safety
    /// Same considerations are for [`Storage::invalidate_cache()`] apply.
    unsafe fn dyn_invalidate_cache(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;

    /// Wrapper around [`Storage::get_storage_helper()`].
    fn dyn_get_storage_helper(&self) -> &CommonStorageHelper;

    /// Wrapper around [`Storage::resize()`].
    fn dyn_resize(
        &self,
        new_size: u64,
        prealloc_mode: PreallocateMode,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>>;
}

/// Storage object preallocation modes.
///
/// When resizing or creating storage objects, this mode determines whether and how the new data
/// range is to be preallocated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PreallocateMode {
    /// No preallocation.
    ///
    /// Reading the new range may return random data.
    None,

    /// Ensure range reads as zeroes.
    ///
    /// Does not necessarily allocate data, but has to ensure the new range will read back as
    /// zeroes.
    Zero,

    /// Extent preallocation.
    ///
    /// Do not write data, but ensure all new extents are allocated.
    Allocate,

    /// Full data preallocation.
    ///
    /// Write zeroes to the whole range.
    WriteData,
}

impl<S: Storage> Storage for &S {
    fn mem_align(&self) -> usize {
        (*self).mem_align()
    }

    fn req_align(&self) -> usize {
        (*self).req_align()
    }

    fn zero_align(&self) -> usize {
        (*self).zero_align()
    }

    fn discard_align(&self) -> usize {
        (*self).discard_align()
    }

    fn size(&self) -> io::Result<u64> {
        (*self).size()
    }

    fn resolve_relative_path<P: AsRef<Path>>(&self, relative: P) -> io::Result<PathBuf> {
        (*self).resolve_relative_path(relative)
    }

    fn get_filename(&self) -> Option<PathBuf> {
        (*self).get_filename()
    }

    async unsafe fn pure_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        unsafe { (*self).pure_readv(bufv, offset).await }
    }

    async unsafe fn pure_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        unsafe { (*self).pure_writev(bufv, offset).await }
    }

    async unsafe fn pure_write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        unsafe { (*self).pure_write_zeroes(offset, length).await }
    }

    async unsafe fn pure_discard(&self, offset: u64, length: u64) -> io::Result<()> {
        unsafe { (*self).pure_discard(offset, length).await }
    }

    async fn flush(&self) -> io::Result<()> {
        (*self).flush().await
    }

    async fn sync(&self) -> io::Result<()> {
        (*self).sync().await
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        unsafe { (*self).invalidate_cache().await }
    }

    fn get_storage_helper(&self) -> &CommonStorageHelper {
        (*self).get_storage_helper()
    }

    async fn resize(&self, new_size: u64, prealloc_mode: PreallocateMode) -> io::Result<()> {
        (*self).resize(new_size, prealloc_mode).await
    }
}

impl<S: Storage + 'static> DynStorage for S {
    fn dyn_mem_align(&self) -> usize {
        <S as Storage>::mem_align(self)
    }

    fn dyn_req_align(&self) -> usize {
        <S as Storage>::req_align(self)
    }

    fn dyn_zero_align(&self) -> usize {
        <S as Storage>::zero_align(self)
    }

    fn dyn_discard_align(&self) -> usize {
        <S as Storage>::discard_align(self)
    }

    fn dyn_size(&self) -> io::Result<u64> {
        <S as Storage>::size(self)
    }

    fn dyn_resolve_relative_path(&self, relative: &Path) -> io::Result<PathBuf> {
        <S as Storage>::resolve_relative_path(self, relative)
    }

    fn dyn_get_filename(&self) -> Option<PathBuf> {
        <S as Storage>::get_filename(self)
    }

    unsafe fn dyn_pure_readv<'a>(
        &'a self,
        bufv: IoVectorMut<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        Box::pin(unsafe { <S as Storage>::pure_readv(self, bufv, offset) })
    }

    unsafe fn dyn_pure_writev<'a>(
        &'a self,
        bufv: IoVector<'a>,
        offset: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        Box::pin(unsafe { <S as Storage>::pure_writev(self, bufv, offset) })
    }

    unsafe fn dyn_pure_write_zeroes(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(unsafe { <S as Storage>::pure_write_zeroes(self, offset, length) })
    }

    unsafe fn dyn_pure_discard(
        &self,
        offset: u64,
        length: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(unsafe { <S as Storage>::pure_discard(self, offset, length) })
    }

    fn dyn_flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(<S as Storage>::flush(self))
    }

    fn dyn_sync(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(<S as Storage>::sync(self))
    }

    unsafe fn dyn_invalidate_cache(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(unsafe { <S as Storage>::invalidate_cache(self) })
    }

    fn dyn_get_storage_helper(&self) -> &CommonStorageHelper {
        <S as Storage>::get_storage_helper(self)
    }

    fn dyn_resize(
        &self,
        new_size: u64,
        prealloc_mode: PreallocateMode,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + '_>> {
        Box::pin(<S as Storage>::resize(self, new_size, prealloc_mode))
    }
}

impl Storage for Box<dyn DynStorage> {
    async fn open(opts: StorageOpenOptions) -> io::Result<Self> {
        // TODO: When we have more drivers, choose different defaults depending on the options
        // given.  Right now, only `File` really supports being opened through options, so it is an
        // obvious choice.
        Ok(Box::new(crate::file::File::open(opts).await?))
    }

    async fn create_open(opts: StorageCreateOptions) -> io::Result<Self> {
        // Same as `Self::open()`.
        Ok(Box::new(crate::file::File::create_open(opts).await?))
    }

    fn mem_align(&self) -> usize {
        self.as_ref().dyn_mem_align()
    }

    fn req_align(&self) -> usize {
        self.as_ref().dyn_req_align()
    }

    fn zero_align(&self) -> usize {
        self.as_ref().dyn_zero_align()
    }

    fn discard_align(&self) -> usize {
        self.as_ref().dyn_discard_align()
    }

    fn size(&self) -> io::Result<u64> {
        self.as_ref().dyn_size()
    }

    fn resolve_relative_path<P: AsRef<Path>>(&self, relative: P) -> io::Result<PathBuf> {
        self.as_ref().dyn_resolve_relative_path(relative.as_ref())
    }

    fn get_filename(&self) -> Option<PathBuf> {
        self.as_ref().dyn_get_filename()
    }

    async unsafe fn pure_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_readv(bufv, offset).await }
    }

    async unsafe fn pure_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_writev(bufv, offset).await }
    }

    async unsafe fn pure_write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_write_zeroes(offset, length).await }
    }

    async unsafe fn pure_discard(&self, offset: u64, length: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_discard(offset, length).await }
    }

    async fn flush(&self) -> io::Result<()> {
        self.as_ref().dyn_flush().await
    }

    async fn sync(&self) -> io::Result<()> {
        self.as_ref().dyn_sync().await
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        unsafe { self.as_ref().dyn_invalidate_cache().await }
    }

    fn get_storage_helper(&self) -> &CommonStorageHelper {
        self.as_ref().dyn_get_storage_helper()
    }

    async fn resize(&self, new_size: u64, prealloc_mode: PreallocateMode) -> io::Result<()> {
        self.as_ref().dyn_resize(new_size, prealloc_mode).await
    }
}

impl Storage for Arc<dyn DynStorage> {
    async fn open(opts: StorageOpenOptions) -> io::Result<Self> {
        Box::<dyn DynStorage>::open(opts).await.map(Into::into)
    }

    async fn create_open(opts: StorageCreateOptions) -> io::Result<Self> {
        Box::<dyn DynStorage>::create_open(opts)
            .await
            .map(Into::into)
    }

    fn mem_align(&self) -> usize {
        self.as_ref().dyn_mem_align()
    }

    fn req_align(&self) -> usize {
        self.as_ref().dyn_req_align()
    }

    fn zero_align(&self) -> usize {
        self.as_ref().dyn_zero_align()
    }

    fn discard_align(&self) -> usize {
        self.as_ref().dyn_discard_align()
    }

    fn size(&self) -> io::Result<u64> {
        self.as_ref().dyn_size()
    }

    fn resolve_relative_path<P: AsRef<Path>>(&self, relative: P) -> io::Result<PathBuf> {
        self.as_ref().dyn_resolve_relative_path(relative.as_ref())
    }

    fn get_filename(&self) -> Option<PathBuf> {
        self.as_ref().dyn_get_filename()
    }

    async unsafe fn pure_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_readv(bufv, offset) }.await
    }

    async unsafe fn pure_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_writev(bufv, offset) }.await
    }

    async unsafe fn pure_write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_write_zeroes(offset, length) }.await
    }

    async unsafe fn pure_discard(&self, offset: u64, length: u64) -> io::Result<()> {
        unsafe { self.as_ref().dyn_pure_discard(offset, length) }.await
    }

    async fn flush(&self) -> io::Result<()> {
        self.as_ref().dyn_flush().await
    }

    async fn sync(&self) -> io::Result<()> {
        self.as_ref().dyn_sync().await
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        unsafe { self.as_ref().dyn_invalidate_cache().await }
    }

    fn get_storage_helper(&self) -> &CommonStorageHelper {
        self.as_ref().dyn_get_storage_helper()
    }

    async fn resize(&self, new_size: u64, prealloc_mode: PreallocateMode) -> io::Result<()> {
        self.as_ref().dyn_resize(new_size, prealloc_mode).await
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

    /// macOS-only: whether to use relaxed synchronization on `File`.
    ///
    /// If relaxed synchronization is enabled, `File::sync()` will use the `fsync()` syscall
    /// instead of `fcntl(F_FULLFSYNC)`, which is a lighter synchronization mechanism that flushes
    /// the filesystem cache to the drive, but doesn't request the drive to flush its internal
    /// buffers to persistent storage.
    #[cfg(target_os = "macos")]
    pub fn relaxed_sync(mut self, relaxed_sync: bool) -> Self {
        self.relaxed_sync = relaxed_sync;
        self
    }

    /// Get the set filename (if any).
    pub fn get_filename(&self) -> Option<&Path> {
        self.filename.as_deref()
    }

    /// Return the set writable state.
    pub fn get_writable(&self) -> bool {
        self.writable
    }

    /// Return the set direct state.
    pub fn get_direct(&self) -> bool {
        self.direct
    }

    /// macOS-only: return the relaxed synchronization state.
    #[cfg(target_os = "macos")]
    pub fn get_relaxed_sync(&self) -> bool {
        self.relaxed_sync
    }
}

impl StorageCreateOptions {
    /// Create default options.
    pub fn new() -> Self {
        StorageCreateOptions::default()
    }

    /// Set the filename of the file to create.
    pub fn filename<P: AsRef<Path>>(self, filename: P) -> Self {
        self.modify_open_opts(|o| o.filename(filename))
    }

    /// Set the initial size.
    pub fn size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Set the desired preallocation mode.
    pub fn preallocate(mut self, prealloc_mode: PreallocateMode) -> Self {
        self.prealloc_mode = prealloc_mode;
        self
    }

    /// Whether to overwrite an existing file.
    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Modify the options used for opening the file.
    pub fn modify_open_opts<F: FnOnce(StorageOpenOptions) -> StorageOpenOptions>(
        mut self,
        f: F,
    ) -> Self {
        self.open_opts = f(self.open_opts);
        self
    }

    /// Get the set filename (if any).
    pub fn get_filename(&self) -> Option<&Path> {
        self.open_opts.filename.as_deref()
    }

    /// Get the set size.
    pub fn get_size(&self) -> u64 {
        self.size
    }

    /// Get the preallocation mode.
    pub fn get_preallocate(&self) -> PreallocateMode {
        self.prealloc_mode
    }

    /// Check whether to overwrite an existing file.
    pub fn get_overwrite(&self) -> bool {
        self.overwrite
    }

    /// Get the options for opening the created file.
    pub fn get_open_options(self) -> StorageOpenOptions {
        self.open_opts
    }
}

impl Default for StorageCreateOptions {
    fn default() -> Self {
        StorageCreateOptions {
            open_opts: Default::default(),
            size: 0,
            prealloc_mode: PreallocateMode::None,
            overwrite: false,
        }
    }
}
