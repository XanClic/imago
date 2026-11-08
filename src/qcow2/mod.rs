//! Qcow2 implementation.

mod allocation;
mod builder;
mod cache;
#[cfg(all(test, feature = "async"))]
mod cancel_tests;
mod compressed;
#[cfg(all(test, feature = "async"))]
mod concurrent_flush_tests;
mod cow;
#[cfg(test)]
mod durability_tests;
mod io_func;
mod mappings;
mod metadata;
mod preallocation;
#[cfg(feature = "sync-wrappers")]
mod sync_wrappers;
mod types;

use crate::async_lru_cache::AsyncLruCache;
use crate::format::builder::{FormatCreateBuilder, FormatDriverBuilder};
use crate::format::drivers::{FormatDriverInstance, PreparedDataMapping};
use crate::format::gate::{ImplicitOpenGate, PermissiveImplicitOpenGate};
use crate::format::wrapped::WrappedFormat;
use crate::format::{Format, PreallocateMode};
use crate::io_buffers::IoVectorMut;
use crate::misc_helpers::{invalid_data, ResultErrorContext};
use crate::raw::Raw;
use crate::sync_primitives::{Mutex, RwLock};
use crate::{storage, FormatAccess, ShallowMapping, Storage, StorageExt, StorageOpenOptions};
use allocation::{Allocator, ClustersToFreeEntry, ClustersToFreeList, PendingClusters};
pub use builder::{Qcow2CreateBuilder, Qcow2OpenBuilder};
use cache::MetadataCaches;
use mappings::{FixedMapping, PreparedMappingState};
use maybe_async::maybe_async;
use metadata::*;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::{cmp, io};
use types::*;

/// Access qcow2 images.
///
/// Allows access to qcow2 images (v2 and v3), referencing the following objects:
/// - Metadata storage object: The image file itself
/// - Data file (storage object): May be the image file itself, or an external data file
/// - Backing image `WrappedFormat<S>`: A backing disk image in any format
#[must_use = "qcow2 images must be flushed before closing"]
pub struct Qcow2<S: Storage + 'static, F: WrappedFormat<S> + 'static = FormatAccess<S>> {
    /// Image file (which contains the qcow2 metadata).
    metadata: Arc<S>,

    /// Whether this image may be modified.
    writable: bool,

    /// Whether the user explicitly assigned a data file storage object (or `None`).
    storage_set: bool,
    /// Data file storage object; will use `metadata` if `None`.
    storage: Option<Arc<S>>,
    /// Whether the user explicitly assigned a backing file (or `None`).
    backing_set: bool,
    /// Backing image.
    backing: Option<F>,
    /// Base options to be used for implicitly opened storage objects.
    storage_open_options: StorageOpenOptions,

    /// Qcow2 header.
    header: Arc<Header>,
    /// L1 table.
    l1_table: RwLock<L1Table>,

    /// L2 and refblock caches
    caches: Arc<MetadataCaches<S>>,

    /// Allocates clusters.
    ///
    /// Is `None` for read-only images.
    allocator: Option<Mutex<Allocator<S>>>,

    /// Allocations that have been removed from the image metadata, but not yet freed.
    ///
    /// Shared with the allocator.  Must be freed via [`Qcow2::process_clusters_to_free()`].
    clusters_to_free: Arc<ClustersToFreeList>,
}

#[maybe_async]
impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2<S, F> {
    /// Create a new [`FormatDriverBuilder`] instance for the given image.
    pub fn builder(image: S) -> Qcow2OpenBuilder<S, F> {
        Qcow2OpenBuilder::new(image)
    }

    /// Create a new [`FormatDriverBuilder`] instance for an image under the given path.
    pub fn builder_path<P: AsRef<Path>>(image_path: P) -> Qcow2OpenBuilder<S, F> {
        Qcow2OpenBuilder::new_path(image_path)
    }

    /// Create a new [`FormatCreateBuilder`] instance to format the given file.
    pub fn create_builder(image: S) -> Qcow2CreateBuilder<S, F> {
        Qcow2CreateBuilder::<S, F>::new(image)
    }

    /// Internal implementation for opening a qcow2 image.
    ///
    /// Does not open external dependencies.
    async fn do_open(
        metadata: S,
        writable: bool,
        storage_open_options: StorageOpenOptions,
        require_self_contained: bool,
    ) -> io::Result<Self> {
        let header = Arc::new(Header::load(&metadata, require_self_contained).await?);

        let cb = header.cluster_bits();
        let l1_offset = header.l1_table_offset();
        let l1_cluster = l1_offset
            .checked_cluster(cb)
            .ok_or_else(|| invalid_data("Unaligned L1 table: {l1_offset}"))?;

        let l1_table =
            L1Table::load(&metadata, &header, l1_cluster, header.l1_table_entries()).await?;

        let metadata = Arc::new(metadata);
        let caches = Arc::new(MetadataCaches::new(&metadata, &header, 128, 32));
        let clusters_to_free = Arc::new(ClustersToFreeList::default());

        let allocator = if writable {
            let mut allocator = Allocator::new(
                Arc::clone(&metadata),
                Arc::clone(&header),
                Arc::clone(&caches),
                Arc::clone(&clusters_to_free),
            )
            .await?;
            if require_self_contained {
                allocator.validate_active_metadata(&l1_table).await?;
            }
            Some(Mutex::new(allocator))
        } else {
            None
        };

        Ok(Qcow2 {
            metadata,

            writable,

            storage_set: false,
            storage: None,
            backing_set: false,
            backing: None,
            storage_open_options,

            header,
            l1_table: RwLock::new(l1_table),

            caches,
            allocator,
            clusters_to_free,
        })
    }

    /// Opens a qcow2 file.
    ///
    /// `metadata` is the file containing the qcow2 metadata.  If `writable` is not set, no
    /// modifications are permitted.
    ///
    /// This will not open any other storage objects needed, i.e. no backing image, no external
    /// data file.  If you want to handle those manually, check whether an external data file is
    /// needed via [`Qcow2::requires_external_data_file()`], and, if necessary, assign one via
    /// [`Qcow2::set_data_file()`]; and assign a backing image via [`Qcow2::set_backing()`].
    ///
    /// If you want to use the implicit references given in the image header, use
    /// [`Qcow2::open_implicit_dependencies()`].
    pub async fn open_image(metadata: S, writable: bool) -> io::Result<Self> {
        Self::do_open(metadata, writable, StorageOpenOptions::new(), false).await
    }

    /// Open a qcow2 file at the given path.
    ///
    /// Open the file as a storage object via [`Storage::open()`], with write access if specified,
    /// then pass that object to [`Qcow2::open_image()`].
    ///
    /// This will not open any other storage objects needed, i.e. no backing image, no external
    /// data file.  If you want to handle those manually, check whether an external data file is
    /// needed via [`Qcow2::requires_external_data_file()`], and, if necessary, assign one via
    /// [`Qcow2::set_data_file()`]; and assign a backing image via [`Qcow2::set_backing()`].
    ///
    /// If you want to use the implicit references given in the image header, use
    /// [`Qcow2::open_implicit_dependencies()`].
    pub async fn open_path<P: AsRef<Path>>(path: P, writable: bool) -> io::Result<Self> {
        let storage_opts = StorageOpenOptions::new().write(writable).filename(path);
        let metadata = S::open(storage_opts).await?;
        Self::do_open(metadata, writable, StorageOpenOptions::new(), false).await
    }

    /// Does this qcow2 image require an external data file?
    ///
    /// Conversely, if this is `false`, this image must not use an external data file.
    pub fn requires_external_data_file(&self) -> bool {
        self.header.external_data_file()
    }

    /// External data file filename given in the image header.
    ///
    /// Note that even if an image requires an external data file, the header may not contain its
    /// filename.  In this case, an external data file must be set explicitly via
    /// [`Qcow2::set_data_file()`].
    pub fn implicit_external_data_file(&self) -> Option<&String> {
        self.header.external_data_filename()
    }

    /// Backing image filename given in the image header.
    pub fn implicit_backing_file(&self) -> Option<&String> {
        self.header.backing_filename()
    }

    /// Backing image format given in the image header.
    ///
    /// If this is `None`, the backing image’s format should be probed.  Note that this may be
    /// dangerous if guests have write access to the backing file: Given a raw image, a guest can
    /// write a qcow2 header into it, resulting in the image being opened as qcow2 the next time,
    /// allowing the guest to read arbitrary files (e.g. by setting them as backing files).
    pub fn implicit_backing_format(&self) -> Option<&String> {
        self.header.backing_format()
    }

    /// Assign the data file.
    ///
    /// `None` means using the same data storage for both metadata and data, which should be used
    /// if [`Qcow2::requires_external_data_file()`] is `false`.
    pub fn set_data_file(&mut self, file: Option<S>) {
        self.storage = file.map(Arc::new);
        self.caches.set_data_file(self.storage.clone());
        self.storage_set = true;
    }

    /// Assign a backing image.
    ///
    /// `None` means no backing image, i.e. reading from unallocated areas will produce zeroes.
    pub fn set_backing(&mut self, backing: Option<F>) {
        self.backing = backing;
        self.backing_set = true;
    }

    /// Get the data storage object.
    ///
    /// If we have an external data file, return that.  Otherwise, return the image (metadata)
    /// file.
    fn storage(&self) -> &S {
        self.storage.as_deref().unwrap_or(&self.metadata)
    }

    /// Return the image’s implicit data file (as given in the image header).
    async fn open_implicit_data_file<G: ImplicitOpenGate<S>>(
        &self,
        gate: &mut G,
    ) -> io::Result<Option<S>> {
        if !self.header.external_data_file() {
            return Ok(None);
        }

        let Some(filename) = self.header.external_data_filename() else {
            return Err(io::Error::other(
                "Image requires external data file, but no filename given",
            ));
        };

        let absolute = self
            .metadata
            .resolve_relative_path(filename)
            .err_context(|| format!("Cannot resolve external data file name {filename}"))?;

        let opts = self
            .storage_open_options
            .clone()
            .write(self.writable)
            .filename(absolute.clone());

        let file = gate
            .open_storage(opts)
            .await
            .err_context(|| format!("External data file {absolute:?}"))?;
        Ok(Some(file))
    }

    /// Wrap `file` in the `Raw` format.  Helper for [`Qcow2::implicit_backing_file()`].
    async fn open_raw_backing_file<G: ImplicitOpenGate<S>>(
        &self,
        file: S,
        gate: &mut G,
    ) -> io::Result<F> {
        let opts = Raw::builder(file).storage_open_options(self.storage_open_options.clone());
        let raw = gate.open_format(opts).await?;
        Ok(F::wrap(raw))
    }

    /// Wrap `file` in the `Qcow2` format.  Helper for [`Qcow2::implicit_backing_file()`].
    async fn open_qcow2_backing_file<G: ImplicitOpenGate<S>>(
        &self,
        file: S,
        gate: &mut G,
    ) -> io::Result<F> {
        let opts =
            Qcow2::<S>::builder(file).storage_open_options(self.storage_open_options.clone());
        // This is recursive, and in async mode, Box::pin is needed for recursion
        #[cfg(feature = "async")]
        let qcow2 = Box::pin(gate.open_format(opts)).await?;
        #[cfg(feature = "sync")]
        let qcow2 = gate.open_format(opts)?;
        Ok(F::wrap(qcow2))
    }

    /// Return the image’s implicit backing image (as given in the image header).
    ///
    /// Anything opened will be passed through `gate`.
    async fn open_implicit_backing_file<G: ImplicitOpenGate<S>>(
        &self,
        gate: &mut G,
    ) -> io::Result<Option<F>> {
        let Some(filename) = self.header.backing_filename() else {
            return Ok(None);
        };

        let absolute = self
            .metadata
            .resolve_relative_path(filename)
            .err_context(|| format!("Cannot resolve backing file name {filename}"))?;

        let file_opts = self
            .storage_open_options
            .clone()
            .filename(absolute.clone())
            .write(false);

        let file = gate
            .open_storage(file_opts)
            .await
            .err_context(|| format!("Backing file {absolute:?}"))?;

        let result = match self.header.backing_format().map(|f| f.as_str()) {
            Some("qcow2") => self.open_qcow2_backing_file(file, gate).await.map(Some),
            Some("raw") | Some("file") => self.open_raw_backing_file(file, gate).await.map(Some),

            Some(fmt) => Err(io::Error::other(format!("Unknown backing format {fmt}"))),

            // Reasonably safe: The backing image is supposed to be read-only.  We could run into
            // trouble if a guest is on a raw image, which is then snapshotted, and now we see a
            // qcow2 image; but let’s rely on such images always having a backing format set.
            None => match unsafe { Self::probe(&file) }.await {
                Ok(true) => self.open_qcow2_backing_file(file, gate).await.map(Some),
                Ok(false) => self.open_raw_backing_file(file, gate).await.map(Some),
                Err(err) => Err(err),
            },
        };

        result.err_context(|| format!("Backing file {absolute:?}"))
    }

    /// Open all implicit dependencies.
    ///
    /// Qcow2 images have dependencies:
    /// - The metadata file, which is the image file itself.
    /// - The data file, which may be the same as the metadata file, or may be an external data
    ///   file.
    /// - A backing disk image in any format.
    ///
    /// All of this can be set explicitly:
    /// - The metadata file is always given explicitly to [`Qcow2::open_image()`].
    /// - The data file can be set via [`Qcow2::set_data_file()`].
    /// - The backing image can be set via [`Qcow2::set_backing()`].
    ///
    /// But the image header can also provide “default” references to the data file and a backing
    /// image, which we call *implicit* dependencies.  This function opens all such implicit
    /// dependencies if they have not been overridden with prior calls to
    /// [`Qcow2::set_data_file()`] or [`Qcow2::set_backing()`], respectively.
    ///
    /// Any image or file is opened through `gate`.
    pub async fn open_implicit_dependencies_gated<G: ImplicitOpenGate<S>>(
        &mut self,
        mut gate: G,
    ) -> io::Result<()> {
        if !self.storage_set {
            let file = self.open_implicit_data_file(&mut gate).await?;
            self.set_data_file(file);
        }

        if !self.backing_set {
            self.backing = self.open_implicit_backing_file(&mut gate).await?;
            self.backing_set = true;
        }

        Ok(())
    }

    /// Open all implicit dependencies, ungated.
    ///
    /// Same as [`Qcow2::open_implicit_dependencies_gated`], but does not perform any gating on
    /// implicitly opened images/files.
    ///
    /// See the cautionary notes on [`PermissiveImplicitOpenGate`] on
    /// [`FormatDriverInstance::probe()`] on why this may be dangerous.
    pub async fn open_implicit_dependencies(&mut self) -> io::Result<()> {
        self.open_implicit_dependencies_gated(PermissiveImplicitOpenGate::default())
            .await
    }

    /// Require write access, i.e. return an error for read-only images.
    fn need_writable(&self) -> io::Result<()> {
        self.writable
            .then_some(())
            .ok_or_else(|| io::Error::other("Image is read-only"))
    }

    /// Require write access and invalidate autoclear features before modifying the image.
    async fn prepare_write(&self) -> io::Result<()> {
        self.header.ensure_healthy()?;
        self.need_writable()?;
        self.header
            .clear_autoclear_features(self.metadata.as_ref())
            .await
    }

    /// Check whether `offset + length` is within the disk size.
    fn check_disk_bounds<D: Display>(&self, offset: u64, length: u64, req: D) -> io::Result<()> {
        let size = self.header.size();
        let length_until_eof = size.saturating_sub(offset);
        if length_until_eof >= length {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Cannot {req} beyond the disk size ({length} + {offset} > {size})"),
            ))
        }
    }

    /// Check whether we support the given preallocation mode.
    ///
    /// `with_backing` designates whether the (new) image (should) have a backing file.
    fn check_valid_preallocation(
        prealloc_mode: PreallocateMode,
        with_backing: bool,
    ) -> io::Result<()> {
        if !with_backing {
            return Ok(());
        }

        match prealloc_mode {
            PreallocateMode::None | PreallocateMode::Zero => Ok(()),

            PreallocateMode::FormatAllocate
            | PreallocateMode::FullAllocate
            | PreallocateMode::WriteData => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Preallocation is not yet supported for images with a backing file",
            )),
        }
    }
}

#[maybe_async(?Send)]
impl<S: Storage, F: WrappedFormat<S>> FormatDriverInstance for Qcow2<S, F> {
    type Storage = S;

    fn format(&self) -> Format {
        Format::Qcow2
    }

    async unsafe fn probe(metadata: &S) -> io::Result<bool>
    where
        Self: Sized,
    {
        let mut magic_version = [0u8; 8];
        metadata.read(&mut magic_version[..], 0).await?;

        let magic = u32::from_be_bytes((&magic_version[..4]).try_into().unwrap());
        let version = u32::from_be_bytes((&magic_version[4..]).try_into().unwrap());
        Ok(magic == MAGIC && (version == 2 || (version == 3)))
    }

    fn size(&self) -> u64 {
        self.header.size()
    }

    fn zero_granularity(&self) -> Option<u64> {
        self.header.require_version(3).ok()?;
        Some(self.header.cluster_size() as u64)
    }

    fn collect_storage_dependencies(&self) -> Vec<&S> {
        let mut v = self
            .backing
            .as_ref()
            .map(|b| b.inner().collect_storage_dependencies())
            .unwrap_or_default();

        v.push(&self.metadata);
        if let Some(storage) = self.storage.as_ref() {
            v.push(storage);
        }

        v
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
        self.header.ensure_healthy()?;
        let length_until_eof = match self.header.size().checked_sub(offset) {
            None | Some(0) => return Ok((ShallowMapping::Eof {}, 0)),
            Some(length) => length,
        };

        let max_length = cmp::min(max_length, length_until_eof);
        let offset = GuestOffset(offset);
        let (mapping, length) = self.do_get_mapping(offset, max_length).await?;
        if let ShallowMapping::Raw {
            storage, offset, ..
        } = &mapping
        {
            let storage_size = storage.size()?;
            if length != 0
                && offset
                    .checked_add(length)
                    .is_none_or(|end| end > storage_size)
            {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "Qcow2 data range {offset:#x} + {length:#x} exceeds storage size {storage_size:#x}"
                    ),
                ));
            }
        }
        Ok((mapping, length))
    }

    #[allow(clippy::needless_lifetimes)] // Elidable in sync, but async needs a named lifetime for the boxed future bound
    async fn prepare_data_mapping<'a>(
        &'a self,
        offset: u64,
        length: u64,
        overwrite: bool,
    ) -> io::Result<PreparedDataMapping<'a, S>> {
        self.header.ensure_healthy()?;
        self.check_disk_bounds(offset, length, "allocate")?;

        if length == 0 {
            return Ok(PreparedDataMapping::existing(self.storage(), 0, 0));
        }

        self.prepare_write().await?;
        let (host_offset, mapped_length, state) = self
            .do_prepare_data_mapping(GuestOffset(offset), length, overwrite, false)
            .await?;
        let length = mapped_length.min(length);

        let pdm = if let Some(state) = state {
            PreparedDataMapping::to_commit(self.storage(), host_offset, length, Box::new(state))
        } else {
            PreparedDataMapping::existing(self.storage(), host_offset, length)
        };
        Ok(pdm)
    }

    async fn commit_data_mapping(&self, mapping: PreparedDataMapping<'_, S>) -> io::Result<()> {
        self.header.ensure_healthy()?;
        let Some(state) = mapping.into_state() else {
            // Mapped already
            return Ok(());
        };

        // On error, dropping the state discards the prepared mapping
        let state = state
            .downcast::<PreparedMappingState>()
            .ok()
            .filter(|state| state.belongs_to(self))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Prepared data mapping does not belong to this image",
                )
            })?;

        self.do_commit_data_mapping(*state).await;
        self.header.ensure_healthy()
    }

    async fn ensure_zero_mapping(&self, offset: u64, length: u64) -> io::Result<(u64, u64)> {
        self.prepare_write().await?;
        self.check_disk_bounds(offset, length, "write")?;

        self.ensure_fixed_mapping(
            GuestOffset(offset),
            length,
            FixedMapping::ZeroRetainAllocation,
        )
        .await
        .map(|(ofs, len)| (ofs.0, len))
    }

    async unsafe fn discard_to_zero_unsafe(
        &self,
        offset: u64,
        length: u64,
    ) -> io::Result<(u64, u64)> {
        self.prepare_write().await?;
        self.check_disk_bounds(offset, length, "discard")?;

        // Safe to discard: We have a mutable `self` reference
        // Note this will return an `Unsupported` error for v2 images.  That’s OK, safely
        // discarding on them is a hairy affair, and they are really outdated by now.
        self.ensure_fixed_mapping(GuestOffset(offset), length, FixedMapping::ZeroDiscard)
            .await
            .map(|(ofs, len)| (ofs.0, len))
    }

    async unsafe fn discard_to_any_unsafe(
        &self,
        offset: u64,
        length: u64,
    ) -> io::Result<(u64, u64)> {
        // Safe: Our caller guarantees that invalidating mappings is safe
        unsafe { self.discard_to_zero_unsafe(offset, length).await }
    }

    async unsafe fn discard_to_backing_unsafe(
        &self,
        offset: u64,
        length: u64,
    ) -> io::Result<(u64, u64)> {
        self.prepare_write().await?;
        self.check_disk_bounds(offset, length, "discard")?;

        // Safe to discard: We have a mutable `self` reference
        self.ensure_fixed_mapping(GuestOffset(offset), length, FixedMapping::FullDiscard)
            .await
            .map(|(ofs, len)| (ofs.0, len))
    }

    async fn readv_special(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        match self.do_readv_special(bufv, GuestOffset(offset)).await? {
            None => Ok(()),
            Some((_, tail_offset)) => Err(io::Error::other(format!(
                "Range at {tail_offset:#x} is not compressed (anymore)"
            ))),
        }
    }

    async fn try_readv_special<'a>(
        &self,
        bufv: IoVectorMut<'a>,
        offset: u64,
    ) -> io::Result<Option<(IoVectorMut<'a>, u64)>> {
        self.do_readv_special(bufv, GuestOffset(offset)).await
    }

    async fn flush(&self) -> io::Result<()> {
        self.header.ensure_healthy()?;
        // Free what is pending before writing back the refblocks
        self.process_clusters_to_free().await;
        self.header.ensure_healthy()?;
        self.caches.flush_all().await?;
        self.metadata.flush().await?;
        if let Some(storage) = self.storage.as_ref() {
            storage.flush().await?;
        }
        // Backing file is read-only, so need not be flushed from us.
        self.header.ensure_healthy()
    }

    async fn sync(&self) -> io::Result<()> {
        self.header.ensure_healthy()?;
        self.metadata.sync().await?;
        if let Some(storage) = self.storage.as_ref() {
            storage.sync().await?;
        }
        // Backing file is read-only, so need not be synced from us.
        self.header.ensure_healthy()
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        self.header.ensure_healthy()?;
        // Forget clusters that were still to be freed: Freeing them would modify the image, which
        // must not happen here, and they are unused only per our outdated view of the image
        // anyway.  (If they really are unused, they are leaked.)
        self.clusters_to_free.take();

        // Safe: Caller says we should do this
        unsafe { self.caches.invalidate_l2() }.await?;
        if let Some(allocator) = self.allocator.as_ref() {
            let allocator = allocator.lock().await;
            // Safe: Caller says we should do this
            unsafe { allocator.invalidate_rb_cache() }.await?;
        }

        // Safe: Caller says we should do this
        unsafe { self.metadata.invalidate_cache() }.await?;
        if let Some(storage) = self.storage.as_ref() {
            // Safe: Caller says we should do this
            unsafe { storage.invalidate_cache() }.await?;
        }
        if let Some(backing) = self.backing.as_ref() {
            // Safe: Caller says we should do this
            unsafe { backing.inner().invalidate_cache() }.await?;
        }

        // TODO: Ideally we would reload the whole image header, but that would require putting it
        // in a lock.  We probably do not want to put things like cluster_bits behind a lock.  For
        // the time being, all we need to reload are things that are mutable at runtime anyway
        // (because the source instance would not have been able to change other things), so just
        // reload the L1 and refcount table positions.
        let new_header = Header::load(self.metadata.as_ref(), false).await?;
        self.header.update(&new_header)?;

        if let Some(allocator) = self.allocator.as_ref() {
            *allocator.lock().await = Allocator::new(
                Arc::clone(&self.metadata),
                Arc::clone(&self.header),
                Arc::clone(&self.caches),
                Arc::clone(&self.clusters_to_free),
            )
            .await?;
        }

        // Alignment checked in `load()`
        let l1_cluster = self
            .header
            .l1_table_offset()
            .cluster(self.header.cluster_bits());

        *self.l1_table.write().await = L1Table::load(
            self.metadata.as_ref(),
            &self.header,
            l1_cluster,
            self.header.l1_table_entries(),
        )
        .await?;

        Ok(())
    }

    async fn resize_grow(&self, new_size: u64, prealloc_mode: PreallocateMode) -> io::Result<()> {
        self.prepare_write().await?;

        let old_size = self.size();
        let grown_length = new_size.saturating_sub(old_size);
        if grown_length == 0 {
            return Ok(()); // only grow, else do nothing
        }

        Self::check_valid_preallocation(prealloc_mode, self.backing.is_some())?;

        if let Some(data_file) = self.storage.as_ref() {
            // Options that allocate data mappings in qcow2 will resize the data file via
            // `preallocate()`.  Those that don’t won’t, so they need to be handled here.
            match prealloc_mode {
                PreallocateMode::None => {
                    data_file
                        .resize(new_size, storage::PreallocateMode::None)
                        .await?;
                }
                PreallocateMode::Zero => {
                    data_file
                        .resize(new_size, storage::PreallocateMode::Zero)
                        .await?;
                }
                PreallocateMode::FormatAllocate
                | PreallocateMode::FullAllocate
                | PreallocateMode::WriteData => (),
            }
        }

        // QEMU requires the L1 table to at least match the image’s size.
        // On that note, note that this would make an L1 state’s data visible to the guest (and
        // also effectively invalidate it, because it is no longer L1 state, but just data), but
        // QEMU does not care either.  (We could see whether there are allocated clusters after the
        // image end to find out.)
        {
            let l1_locked = self.l1_table.write().await;
            let l1_index =
                GuestOffset(new_size.saturating_sub(1)).l1_index(self.header.cluster_bits());
            let _l1_locked = self.grow_l1_table(l1_locked, l1_index).await?;
        }

        // Preallocate the entire new range (beyond the current image end)
        match prealloc_mode {
            PreallocateMode::None => (),
            PreallocateMode::Zero => self.preallocate_zero(old_size, grown_length).await?,
            PreallocateMode::FormatAllocate => {
                self.preallocate(old_size, grown_length, storage::PreallocateMode::Zero)
                    .await?;
            }
            PreallocateMode::FullAllocate => {
                self.preallocate(old_size, grown_length, storage::PreallocateMode::Allocate)
                    .await?;
            }
            PreallocateMode::WriteData => {
                self.preallocate(old_size, grown_length, storage::PreallocateMode::WriteData)
                    .await?
            }
        }

        // Now that preallocation is complete, it’s safe to actually set the new size (otherwise
        // someone might see a backing image’s data peek through briefly in case it is longer than
        // `old_size`)
        self.header
            .write_size(self.metadata.as_ref(), new_size)
            .await
    }

    async fn resize_shrink(&mut self, new_size: u64) -> io::Result<()> {
        self.prepare_write().await?;

        let old_size = self.size();
        if new_size >= old_size {
            return Ok(()); // only shrink, else do nothing
        }

        if let Some(data_file) = self.storage.as_ref() {
            data_file
                .resize(new_size, storage::PreallocateMode::None)
                .await?;
        }

        let mut offset = new_size;
        while offset < old_size {
            match self.discard_to_backing(offset, old_size - offset).await {
                Ok((_, 0)) => break, // cannot discard tail
                Ok((dofs, dlen)) => offset = dofs + dlen,
                // Basically ignore errors, but stop trying to discard
                Err(_) => break,
            }
        }

        // Shrink after discarding (so we can discard)
        self.header
            .write_size(self.metadata.as_ref(), new_size)
            .await
    }
}

impl<S: Storage + 'static, F: WrappedFormat<S>> Debug for Qcow2<S, F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Qcow2")
            .field("metadata", &self.metadata)
            .field("storage_set", &self.storage_set)
            .field("storage", &self.storage)
            .field("backing_set", &self.backing_set)
            .field("backing", &self.backing)
            .finish()
    }
}

impl<S: Storage + 'static, F: WrappedFormat<S>> Display for Qcow2<S, F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "qcow2[{}]", self.metadata)
    }
}
