//! Access generic files as images.
//!
//! Allows accessing generic storage objects (`Storage`) as images (i.e. `FormatAccess`).

use crate::format::builder::{
    FormatCreateBuilder, FormatCreateBuilderBase, FormatDriverBuilder, FormatDriverBuilderBase,
};
use crate::format::drivers::FormatDriverInstance;
use crate::format::gate::ImplicitOpenGate;
use crate::format::{Format, PreallocateMode};
use crate::sync_primitives::{Mutex, MutexGuard};
use crate::{
    storage, DenyImplicitOpenGate, ShallowMapping, Storage, StorageExt, StorageOpenOptions,
};
use maybe_async::maybe_async;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Wraps a storage object without any translation.
#[derive(Debug)]
pub struct Raw<S: Storage + 'static> {
    /// Wrapped storage object.
    inner: S,

    /// Whether this image may be modified.
    writable: bool,

    /// Disk size, which is the file size when this object was created.
    ///
    /// Copy of the value in `resize_lock`, for lock-free reading.  Must only be changed via
    /// [`Raw::set_size()`].
    size: AtomicU64,

    /// Disk size, for resize operations.
    ///
    /// Every resize path (growing and shrinking) must hold this lock throughout, and change the
    /// size only via [`Raw::set_size()`].  This serializes resizes, so a resize to a smaller size
    /// cannot overtake one to a larger size and shrink the storage below the image size.
    resize_lock: Mutex<u64>,
}

#[maybe_async]
impl<S: Storage + 'static> Raw<S> {
    /// Create a new [`FormatDriverBuilder`] instance for the given image.
    pub fn builder(image: S) -> RawOpenBuilder<S> {
        RawOpenBuilder::new(image)
    }

    /// Create a new [`FormatDriverBuilder`] instance for an image under the given path.
    pub fn builder_path<P: AsRef<Path>>(image_path: P) -> RawOpenBuilder<S> {
        RawOpenBuilder::new_path(image_path)
    }

    /// Create a new [`FormatCreateBuilder`] instance for the given file.
    pub fn create_builder(image: S) -> RawCreateBuilder<S> {
        RawCreateBuilder::new(image)
    }

    /// Wrap `inner`, allowing it to be used as a disk image in raw format.
    pub async fn open_image(inner: S, writable: bool) -> io::Result<Self> {
        let size = inner.size()?;
        Ok(Raw {
            inner,
            writable,
            size: size.into(),
            resize_lock: Mutex::new(size),
        })
    }

    /// Open the given path as a storage object, and wrap it in `Raw`.
    pub async fn open_path<P: AsRef<Path>>(path: P, writable: bool) -> io::Result<Self> {
        let storage_opts = StorageOpenOptions::new().write(writable).filename(path);
        let inner = S::open(storage_opts).await?;
        Self::open_image(inner, writable).await
    }

    /// Wrap `inner`, allowing it to be used as a disk image in raw format.
    #[cfg(feature = "sync-wrappers")]
    pub fn open_image_sync(inner: S, writable: bool) -> io::Result<Self> {
        let size = inner.size()?;
        Ok(Raw {
            inner,
            writable,
            size: size.into(),
            resize_lock: Mutex::new(size),
        })
    }

    #[cfg(feature = "sync-wrappers")]
    /// Synchronous wrapper around [`Raw::open_path()`].
    pub fn open_path_sync<P: AsRef<Path>>(path: P, writable: bool) -> io::Result<Self> {
        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(Self::open_path(path, writable))
    }

    /// Set the disk size.
    ///
    /// Requires the guard of `resize_lock`, which must be held throughout the whole resize
    /// operation.
    fn set_size(&self, size_guard: &mut MutexGuard<'_, u64>, new_size: u64) {
        **size_guard = new_size;
        self.size.store(new_size, Ordering::Relaxed);
    }
}

#[maybe_async(?Send)]
impl<S: Storage + 'static> FormatDriverInstance for Raw<S> {
    type Storage = S;

    fn format(&self) -> Format {
        Format::Raw
    }

    async unsafe fn probe(_storage: &S) -> io::Result<bool>
    where
        Self: Sized,
    {
        Ok(true)
    }

    fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    fn zero_granularity(&self) -> Option<u64> {
        None
    }

    fn collect_storage_dependencies(&self) -> Vec<&S> {
        vec![&self.inner]
    }

    fn writable(&self) -> bool {
        self.writable
    }

    #[allow(clippy::needless_lifetimes)] // Elidable in sync, but async needs a named lifetime for the boxed future bound
    async fn get_mapping<'a>(
        &'a self,
        offset: u64,
        max_length: u64,
    ) -> io::Result<(ShallowMapping<'a, S>, u64)> {
        let remaining = match self.size().checked_sub(offset) {
            None | Some(0) => return Ok((ShallowMapping::Eof {}, 0)),
            Some(remaining) => remaining,
        };

        Ok((
            ShallowMapping::Raw {
                storage: &self.inner,
                offset,
                read_exact: false,
                writable: true,
            },
            std::cmp::min(max_length, remaining),
        ))
    }

    #[allow(clippy::needless_lifetimes)] // Elidable in sync, but async needs a named lifetime for the boxed future bound
    async fn ensure_data_mapping<'a>(
        &'a self,
        offset: u64,
        length: u64,
        _overwrite: bool,
    ) -> io::Result<(&'a S, u64, u64)> {
        let Some(remaining) = self.size().checked_sub(offset) else {
            return Err(io::Error::other("Cannot allocate past the end of file"));
        };
        if length > remaining {
            return Err(io::Error::other("Cannot allocate past the end of file"));
        }

        Ok((&self.inner, offset, length))
    }

    async fn ensure_zero_mapping(&self, offset: u64, length: u64) -> io::Result<(u64, u64)> {
        let zero_align = self.inner.zero_align();
        assert!(zero_align.is_power_of_two());

        let zero_align_mask = zero_align as u64 - 1;

        let aligned_end = (offset + length) & !zero_align_mask;
        let aligned_offset = (offset + zero_align_mask) & !zero_align_mask;
        let aligned_length = aligned_end.saturating_sub(aligned_offset);
        if aligned_length == 0 {
            return Ok((aligned_offset, 0));
        }

        // FIXME: Introduce request flags, and request no fallback
        self.inner
            .write_zeroes(aligned_offset, aligned_length)
            .await?;
        Ok((aligned_offset, aligned_length))
    }

    async unsafe fn discard_to_zero_unsafe(
        &self,
        offset: u64,
        length: u64,
    ) -> io::Result<(u64, u64)> {
        self.ensure_zero_mapping(offset, length).await
    }

    async unsafe fn discard_to_any_unsafe(
        &self,
        offset: u64,
        length: u64,
    ) -> io::Result<(u64, u64)> {
        let discard_align = self.inner.discard_align();
        assert!(discard_align.is_power_of_two());

        let discard_align_mask = discard_align as u64 - 1;

        let aligned_end = (offset + length) & !discard_align_mask;
        let aligned_offset = (offset + discard_align_mask) & !discard_align_mask;
        let aligned_length = aligned_end.saturating_sub(aligned_offset);
        if aligned_length == 0 {
            return Ok((aligned_offset, 0));
        }

        self.inner.discard(aligned_offset, aligned_length).await?;
        Ok((aligned_offset, aligned_length))
    }

    async unsafe fn discard_to_backing_unsafe(
        &self,
        offset: u64,
        length: u64,
    ) -> io::Result<(u64, u64)> {
        unsafe { self.discard_to_zero_unsafe(offset, length).await }
    }

    async fn flush(&self) -> io::Result<()> {
        // No internal buffers to flush
        self.inner.flush().await
    }

    async fn sync(&self) -> io::Result<()> {
        self.inner.sync().await
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        // No internal buffers to drop
        // Safe: Caller says we should do this
        unsafe { self.inner.invalidate_cache() }.await
    }

    async fn resize_grow(
        &self,
        new_size: u64,
        format_prealloc_mode: PreallocateMode,
    ) -> io::Result<()> {
        let mut size_guard = self.resize_lock.lock().await;
        if new_size <= *size_guard {
            return Ok(()); // only grow, else do nothing
        }

        let storage_prealloc_mode = match format_prealloc_mode {
            PreallocateMode::None => storage::PreallocateMode::None,
            PreallocateMode::Zero | PreallocateMode::FormatAllocate => {
                storage::PreallocateMode::Zero
            }
            PreallocateMode::FullAllocate => storage::PreallocateMode::Allocate,
            PreallocateMode::WriteData => storage::PreallocateMode::WriteData,
        };

        // Grow the storage first, so the image never extends beyond it
        self.inner.resize(new_size, storage_prealloc_mode).await?;

        self.set_size(&mut size_guard, new_size);
        Ok(())
    }

    async fn resize_shrink(&mut self, new_size: u64) -> io::Result<()> {
        // Not strictly necessary with `&mut self`, but ensures all resize paths look the same
        let mut size_guard = self.resize_lock.lock().await;
        let old_size = *size_guard;
        if new_size >= old_size {
            return Ok(()); // only shrink, else do nothing
        }

        // Shrink the image first, so it never extends beyond the storage
        self.set_size(&mut size_guard, new_size);

        if let Err(err) = self
            .inner
            .resize(new_size, storage::PreallocateMode::None)
            .await
        {
            self.set_size(&mut size_guard, old_size);
            return Err(err);
        }

        Ok(())
    }
}

impl<S: Storage + 'static> Display for Raw<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "raw[{}]", self.inner)
    }
}

/// Options builder for opening a raw image.
pub struct RawOpenBuilder<S: Storage + 'static>(FormatDriverBuilderBase<S>);

#[maybe_async(AFIT)]
impl<S: Storage + 'static> FormatDriverBuilder<S> for RawOpenBuilder<S> {
    type Format = Raw<S>;
    const FORMAT: Format = Format::Raw;

    fn new(image: S) -> Self {
        RawOpenBuilder(FormatDriverBuilderBase::new(image))
    }

    fn new_path<P: AsRef<Path>>(path: P) -> Self {
        RawOpenBuilder(FormatDriverBuilderBase::new_path(path))
    }

    fn write(mut self, writable: bool) -> Self {
        self.0.set_write(writable);
        self
    }

    fn storage_open_options(mut self, options: StorageOpenOptions) -> Self {
        self.0.set_storage_open_options(options);
        self
    }

    async fn open<G: ImplicitOpenGate<S>>(self, mut gate: G) -> io::Result<Self::Format> {
        let writable = self.0.get_writable();
        let file = self.0.open_image(&mut gate).await?;
        Raw::open_image(file, writable).await
    }

    fn get_image_path(&self) -> Option<PathBuf> {
        self.0.get_image_path()
    }

    fn get_writable(&self) -> bool {
        self.0.get_writable()
    }

    fn get_storage_open_options(&self) -> Option<&StorageOpenOptions> {
        self.0.get_storage_opts()
    }
}

/// Creation builder for a new raw image.
pub struct RawCreateBuilder<S: Storage + 'static>(FormatCreateBuilderBase<S>);

#[maybe_async(AFIT)]
impl<S: Storage + 'static> FormatCreateBuilder<S> for RawCreateBuilder<S> {
    const FORMAT: Format = Format::Raw;
    type DriverBuilder = RawOpenBuilder<S>;

    fn new(image: S) -> Self {
        RawCreateBuilder(FormatCreateBuilderBase::new(image))
    }

    fn size(mut self, size: u64) -> Self {
        self.0.set_size(size);
        self
    }

    fn preallocate(mut self, prealloc_mode: PreallocateMode) -> Self {
        self.0.set_preallocate(prealloc_mode);
        self
    }

    fn get_size(&self) -> u64 {
        self.0.get_size()
    }

    fn get_preallocate(&self) -> PreallocateMode {
        self.0.get_preallocate()
    }

    async fn create(self) -> io::Result<()> {
        self.create_open(DenyImplicitOpenGate::default(), |image| {
            Ok(Raw::builder(image))
        })
        .await?;
        Ok(())
    }

    async fn create_open<
        G: ImplicitOpenGate<S>,
        F: FnOnce(S) -> io::Result<Self::DriverBuilder>,
    >(
        self,
        open_gate: G,
        open_builder_fn: F,
    ) -> io::Result<Raw<S>> {
        let size = self.0.get_size();
        let prealloc = self.0.get_preallocate();
        let image = self.0.get_image();

        // Clear of data (and allow for full preallocation, if requested)
        if image.size()? > 0 {
            image.resize(size, storage::PreallocateMode::None).await?;
        }

        let img = open_builder_fn(image)?.write(true).open(open_gate).await?;
        if size > 0 {
            img.resize_grow(size, prealloc).await?;
        }

        Ok(img)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_buffers::{IoVector, IoVectorMut};
    use crate::null::Null;
    use crate::storage::drivers::CommonStorageHelper;
    use crate::FormatAccess;
    use std::sync::atomic::AtomicBool;

    /// Null storage whose resizes can be made to fail, or be held back.
    #[derive(Debug, Default)]
    struct ResizeTestStorage {
        /// Storage size.
        size: AtomicU64,

        /// Whether `resize()` should fail.
        fail_resize: AtomicBool,

        /// Hold back resizing to this size until `release_resize` is notified (0: none).
        #[cfg(feature = "async")]
        hold_resize_to: AtomicU64,

        /// Releases a held back resize.
        #[cfg(feature = "async")]
        release_resize: tokio::sync::Notify,

        /// Storage helper.
        common_storage_helper: CommonStorageHelper,
    }

    impl ResizeTestStorage {
        fn new(size: u64) -> Self {
            ResizeTestStorage {
                size: size.into(),
                ..Default::default()
            }
        }
    }

    impl Display for ResizeTestStorage {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "resize-test:[{}B]", self.size.load(Ordering::Relaxed))
        }
    }

    #[maybe_async(AFIT)]
    impl Storage for ResizeTestStorage {
        fn size(&self) -> io::Result<u64> {
            Ok(self.size.load(Ordering::Relaxed))
        }

        async unsafe fn pure_readv(
            &self,
            mut bufv: IoVectorMut<'_>,
            _offset: u64,
        ) -> io::Result<()> {
            bufv.fill(0);
            Ok(())
        }

        async unsafe fn pure_writev(&self, _bufv: IoVector<'_>, _offset: u64) -> io::Result<()> {
            Ok(())
        }

        async fn flush(&self) -> io::Result<()> {
            Ok(())
        }

        async fn sync(&self) -> io::Result<()> {
            Ok(())
        }

        async unsafe fn invalidate_cache(&self) -> io::Result<()> {
            Ok(())
        }

        fn get_storage_helper(&self) -> &CommonStorageHelper {
            &self.common_storage_helper
        }

        async fn resize(
            &self,
            new_size: u64,
            _prealloc_mode: storage::PreallocateMode,
        ) -> io::Result<()> {
            #[cfg(feature = "async")]
            if self.hold_resize_to.load(Ordering::Relaxed) == new_size {
                self.release_resize.notified().await;
            }

            if self.fail_resize.load(Ordering::Relaxed) {
                return Err(io::Error::other("Resize failed"));
            }

            self.size.store(new_size, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Return the image’s storage.
    fn storage(image: &FormatAccess<ResizeTestStorage>) -> &ResizeTestStorage {
        image.collect_storage_dependencies()[0]
    }

    /// A failed resize must leave the image size unchanged.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_failed_resize() {
        let raw = Raw::open_image(ResizeTestStorage::new(1024), true)
            .await
            .unwrap();
        let mut image = FormatAccess::new(raw);

        storage(&image).fail_resize.store(true, Ordering::Relaxed);

        image
            .resize_grow(2048, PreallocateMode::None)
            .await
            .unwrap_err();
        assert_eq!(image.size(), 1024);
        assert_eq!(storage(&image).size().unwrap(), 1024);

        image.resize_shrink(512).await.unwrap_err();
        assert_eq!(image.size(), 1024);
        assert_eq!(storage(&image).size().unwrap(), 1024);

        // Once resizing works again, sizes must follow
        storage(&image).fail_resize.store(false, Ordering::Relaxed);

        image
            .resize_grow(2048, PreallocateMode::None)
            .await
            .unwrap();
        assert_eq!(image.size(), 2048);
        assert_eq!(storage(&image).size().unwrap(), 2048);

        image.resize_shrink(512).await.unwrap();
        assert_eq!(image.size(), 512);
        assert_eq!(storage(&image).size().unwrap(), 512);
    }

    /// A grow to a smaller size must not overtake a concurrent grow to a larger size, which would
    /// shrink the storage below the image size.
    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_concurrent_grow() {
        let raw = Raw::open_image(ResizeTestStorage::new(1024), true)
            .await
            .unwrap();
        let image = FormatAccess::new(raw);

        // Hold back the smaller grow, which starts first, so the larger one could complete first
        storage(&image)
            .hold_resize_to
            .store(2048, Ordering::Relaxed);

        // `join3()` polls in order: The smaller grow first, then the larger grow, then the release
        let (small, large, ()) = futures::future::join3(
            image.resize_grow(2048, PreallocateMode::None),
            image.resize_grow(4096, PreallocateMode::None),
            async { storage(&image).release_resize.notify_one() },
        )
        .await;
        small.unwrap();
        large.unwrap();

        assert_eq!(image.size(), 4096);
        assert_eq!(storage(&image).size().unwrap(), 4096);
    }

    /// Requests reaching beyond the disk size must fail without touching (and thus growing) the
    /// underlying storage.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_access_beyond_disk_size() {
        let size = 1024;
        let raw = Raw::open_image(Null::new(size), true).await.unwrap();
        let mut image = FormatAccess::new(raw);

        let check = |image: &FormatAccess<Null>, result: io::Result<()>, req: &str| {
            let err = result.expect_err(req);
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{req}: {err}");
            let storage_size = image.collect_storage_dependencies()[0].size().unwrap();
            assert_eq!(storage_size, size, "{req}");
        };

        // Entirely beyond the end, and crossing the end
        for offset in [size, size / 2] {
            let result = image.write(&[0u8; 4096][..], offset).await;
            check(&image, result, &format!("write() at {offset}"));

            let result = image.write_zeroes(offset, 4096).await;
            check(&image, result, &format!("write_zeroes() at {offset}"));

            let result = image.discard_to_zero(offset, 4096).await;
            check(&image, result, &format!("discard_to_zero() at {offset}"));

            let result = image.discard_to_any(offset, 4096).await;
            check(&image, result, &format!("discard_to_any() at {offset}"));

            let result = image.discard_to_backing(offset, 4096).await;
            check(&image, result, &format!("discard_to_backing() at {offset}"));

            let result = image
                .ensure_data_mapping(offset, 4096, true)
                .await
                .map(|_| ());
            check(
                &image,
                result,
                &format!("ensure_data_mapping() at {offset}"),
            );
        }

        // Empty requests at the end are fine
        image.write(&[0u8; 0][..], size).await.unwrap();
        image.write_zeroes(size, 0).await.unwrap();
        image.discard_to_zero(size, 0).await.unwrap();
        image.discard_to_any(size, 0).await.unwrap();
        image.discard_to_backing(size, 0).await.unwrap();

        // Empty data mappings, at the end and anywhere else
        for offset in [size, 0] {
            let (_, _, length) = image.ensure_data_mapping(offset, 0, true).await.unwrap();
            assert_eq!(length, 0, "ensure_data_mapping() at {offset}");
        }

        assert_eq!(image.size(), size);
    }
}
