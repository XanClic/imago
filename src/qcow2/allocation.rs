//! Cluster allocation.
//!
//! Functionality for allocating single clusters and ranges of clusters, and general handling of
//! refcount structures.

use super::cache::RefBlockCacheBackend;
use super::*;
use std::mem;
use tokio::sync::MutexGuard;
use tracing::{event, warn, Level};

/// Central facility for cluster allocation.
pub(super) struct Allocator<S: Storage> {
    /// Qcow2 metadata file.
    file: Arc<S>,

    /// Qcow2 refcount table.
    reftable: RefTable,

    /// The first free cluster index in the qcow2 file, to speed up allocation.
    first_free_cluster: HostCluster,

    /// Qcow2 image header.
    header: Arc<Header>,

    /// Refblock cache.
    rb_cache: AsyncLruCache<HostCluster, RefBlock, RefBlockCacheBackend<S>>,
}

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2<S, F> {
    /// Return the central allocator instance.
    ///
    /// Returns an error for read-only images.
    async fn allocator(&self) -> io::Result<MutexGuard<'_, Allocator<S>>> {
        Ok(self
            .allocator
            .as_ref()
            .ok_or_else(|| io::Error::other("Image is read-only"))?
            .lock()
            .await)
    }

    /// Allocate one metadata cluster.
    ///
    /// Metadata clusters are allocated exclusively in the metadata (image) file.
    pub(super) async fn allocate_meta_cluster(&self) -> io::Result<HostCluster> {
        self.allocate_meta_clusters(ClusterCount(1)).await
    }

    /// Allocate multiple continuous metadata clusters.
    ///
    /// Useful e.g. for the L1 table or refcount table.
    pub(super) async fn allocate_meta_clusters(
        &self,
        count: ClusterCount,
    ) -> io::Result<HostCluster> {
        self.allocator().await?.allocate_clusters(count).await
    }

    /// Allocate one data clusters for the given guest cluster.
    ///
    /// Without an external data file, data clusters are allocated in the image file, just like
    /// metadata clusters.
    ///
    /// With an external data file, data clusters aren’t really allocated, but just put there at
    /// the same offset as their guest offset.  Their refcount is not tracked by the qcow2 metadata
    /// structures (which only cover the metadata (image) file).
    pub(super) async fn allocate_data_cluster(
        &self,
        guest_cluster: GuestCluster,
    ) -> io::Result<HostCluster> {
        if self.header.external_data_file() {
            Ok(HostCluster(guest_cluster.0))
        } else {
            let mut allocator = self.allocator().await?;

            // Allocate clusters before setting up L2 entries
            self.l2_cache.depend_on(&allocator.rb_cache).await?;

            allocator.allocate_clusters(ClusterCount(1)).await
        }
    }

    /// Allocate the data cluster with the given index.
    ///
    /// Without a `mandatory_host_cluster` given, this is the same as
    /// [`Qcow2::allocate_data_cluster()`].
    ///
    /// With a `mandatory_host_cluster` given, try to allocate that cluster.  If that is not
    /// possible because it is already allocated, return `Ok(None)`.
    pub(super) async fn allocate_data_cluster_at(
        &self,
        guest_cluster: GuestCluster,
        mandatory_host_cluster: Option<HostCluster>,
    ) -> io::Result<Option<HostCluster>> {
        let Some(mandatory_host_cluster) = mandatory_host_cluster else {
            return self.allocate_data_cluster(guest_cluster).await.map(Some);
        };

        if self.header.external_data_file() {
            let cluster = HostCluster(guest_cluster.0);
            Ok((cluster == mandatory_host_cluster).then_some(cluster))
        } else {
            let mut allocator = self.allocator().await?;

            // Allocate clusters before setting up L2 entries
            self.l2_cache.depend_on(&allocator.rb_cache).await?;

            let cluster = allocator
                .allocate_cluster_at(mandatory_host_cluster)
                .await?
                .then_some(mandatory_host_cluster);
            Ok(cluster)
        }
    }

    /// Free metadata clusters (i.e. decrement their refcount).
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    pub(super) async fn free_meta_clusters(&self, cluster: HostCluster, count: ClusterCount) {
        if let Ok(mut allocator) = self.allocator().await {
            allocator.free_clusters(cluster, count).await
        }
    }

    /// Free data clusters (i.e. decrement their refcount).
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    pub(super) async fn free_data_clusters(&self, cluster: HostCluster, count: ClusterCount) {
        if !self.header.external_data_file() {
            if let Ok(mut allocator) = self.allocator().await {
                // Clear L2 entries before deallocating clusters
                if let Err(err) = allocator.rb_cache.depend_on(&self.l2_cache).await {
                    warn!("Leaking clusters; cannot set up cache inter-dependency with L2 cache: {err}");
                    return;
                }

                allocator.free_clusters(cluster, count).await;
            }
        }
    }
}

impl<S: Storage> Allocator<S> {
    /// Create a new allocator for the given image file.
    pub async fn new(image: Arc<S>, header: Arc<Header>) -> io::Result<Self> {
        let cb = header.cluster_bits();
        let rt_offset = header.reftable_offset();
        let rt_cluster = rt_offset.checked_cluster(cb).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unaligned refcount table: {rt_offset}"),
            )
        })?;

        let reftable = RefTable::load(
            image.as_ref(),
            &header,
            rt_cluster,
            header.reftable_entries(),
        )
        .await?;

        let rb_cache_backend = RefBlockCacheBackend::new(Arc::clone(&image), Arc::clone(&header));
        let rb_cache = AsyncLruCache::new(rb_cache_backend, 32);

        Ok(Allocator {
            file: image,
            reftable,
            first_free_cluster: HostCluster(0),
            header,
            rb_cache,
        })
    }

    /// Flush the refcount block cache.
    pub async fn flush_rb_cache(&self) -> io::Result<()> {
        self.rb_cache.flush().await
    }

    /// Invaidate the refcount block cache.
    ///
    /// # Safety
    /// May cause image corruption, you must guarantee the on-disk state is consistent.
    pub async unsafe fn invalidate_rb_cache(&self) -> io::Result<()> {
        // Safe: Caller says so.
        unsafe { self.rb_cache.invalidate() }.await
    }

    /// Allocate clusters in the image file.
    async fn allocate_clusters(&mut self, count: ClusterCount) -> io::Result<HostCluster> {
        let mut index = self.first_free_cluster;
        loop {
            let alloc_count = self.allocate_clusters_at(index, count).await?;
            if alloc_count == count {
                return Ok(index);
            }

            index += alloc_count + ClusterCount(1);
            if index.offset(self.header.cluster_bits()) > MAX_OFFSET {
                return Err(io::Error::other("Cannot grow qcow2 file any further"));
            }
        }
    }

    /// Allocate the given clusters in the image file.
    ///
    /// Allocate up to `count` unallocated clusters starting from `index`.  When encountering an
    /// already allocated cluster (or any other error), stop, and free the clusters that were just
    /// newly allocated.
    ///
    /// Returns the number of clusters that could be allocated (starting from `index`), which may
    /// be 0 if `index` has already been allocated.  Note again that in case this is less than
    /// `count`, those clusters will have been freed again already, so this is just a hint to
    /// callers that the cluster at `index + count` is already allocated.
    async fn allocate_clusters_at(
        &mut self,
        mut index: HostCluster,
        mut count: ClusterCount,
    ) -> io::Result<ClusterCount> {
        let start_index = index;

        while count > ClusterCount(0) {
            match self.allocate_cluster_at(index).await {
                // Successful allocation
                Ok(true) => (),

                // Already allocated, or some real error occurred
                result => {
                    self.free_clusters(start_index, index - start_index).await;
                    return result.map(|_| index - start_index);
                }
            };

            count -= ClusterCount(1);
            index += ClusterCount(1);
        }

        Ok(index - start_index)
    }

    /// Allocate the given cluster in the image file.
    ///
    /// Return `Ok(true)` if allocation was successful, or `Ok(false)` if the cluster was already
    /// allocated before.
    async fn allocate_cluster_at(&mut self, index: HostCluster) -> io::Result<bool> {
        let rb_bits = self.header.rb_bits();
        let (rt_index, rb_index) = index.rt_rb_indices(rb_bits);

        let rb = self.ensure_rb(rt_index).await?;
        let mut rb = rb.lock_write().await;
        let can_allocate = rb.is_zero(rb_index);
        if can_allocate {
            rb.increment(rb_index)?;
        }

        // We now know this is allocated
        if index == self.first_free_cluster {
            self.first_free_cluster = index + ClusterCount(1);
        }

        Ok(can_allocate)
    }

    /// Get the refblock referenced by the given reftable index, if any.
    ///
    /// If there is no refblock for the given reftable index, return `Ok(None)`.
    async fn get_rb(&mut self, rt_index: usize) -> io::Result<Option<Arc<RefBlock>>> {
        let rt_entry = self.reftable.get(rt_index);
        if let Some(rb_offset) = rt_entry.refblock_offset() {
            let cb = self.header.cluster_bits();
            let rb_cluster = rb_offset.checked_cluster(cb).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unaligned refcount block with index {rt_index}; refcount table entry: {rt_entry:?}"),
                )
            })?;

            self.rb_cache.get_or_insert(rb_cluster).await.map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a refblock for the given reftable index.
    ///
    /// If there already is a refblock at that index, return it.  Otherwise, create one and hook it
    /// up.
    async fn ensure_rb(&mut self, rt_index: usize) -> io::Result<Arc<RefBlock>> {
        if let Some(rb) = self.get_rb(rt_index).await? {
            return Ok(rb);
        }

        if !self.reftable.in_bounds(rt_index) {
            self.grow_reftable(rt_index).await?;
            // `grow_reftable` will allocate new refblocks, so check the index again
            if let Some(rb) = self.get_rb(rt_index).await? {
                return Ok(rb);
            }
        }

        let mut rb = RefBlock::new_cleared(self.file.as_ref(), &self.header)?;

        // When allocating new refblocks, we always place them such that they describe themselves.
        // TODO: There may be more efficient ways, this is just quite an easy one.
        let rb_cluster = HostCluster::from_ref_indices(rt_index, 0, self.header.rb_bits());
        rb.set_cluster(rb_cluster);
        rb.lock_write().await.increment(0)?;
        rb.write(self.file.as_ref()).await?;

        self.reftable.enter_refblock(rt_index, &rb)?;
        self.reftable
            .write_entry(self.file.as_ref(), rt_index)
            .await?;

        let rb = Arc::new(rb);
        self.rb_cache.insert(rb_cluster, Arc::clone(&rb)).await?;
        Ok(rb)
    }

    /// Create a new refcount table covering at least `at_least_index`.
    ///
    /// Create a new reftable of the required size, copy all existing refblock references into it,
    /// ensure it is refcounted itself (also creating new refblocks if necessary), and have the
    /// image header reference the new refcount table.
    async fn grow_reftable(&mut self, at_least_index: usize) -> io::Result<()> {
        let cb = self.header.cluster_bits();
        let rb_bits = self.header.rb_bits();
        let rb_entries = 1 << rb_bits;

        let mut new_rt = self.reftable.clone_and_grow(&self.header, at_least_index);
        let rt_clusters = ClusterCount::from_byte_size(new_rt.byte_size() as u64, cb);

        // Find free range
        let (mut rt_index, mut rb_index) = self.first_free_cluster.rt_rb_indices(rb_bits);
        let mut free_cluster_index: Option<HostCluster> = None;
        let mut free_cluster_count = ClusterCount(0);

        // Number of clusters required to allocate both the new reftable and all new refblocks.
        // Note that `clone_and_grow()` *guarantees* we can fit the final count in there.
        let mut required_clusters = rt_clusters;

        while free_cluster_count < required_clusters {
            // `clone_and_grow()` guarantees it can fit
            assert!(new_rt.in_bounds(rt_index));

            let rt_entry = new_rt.get(rt_index);
            let Some(rb_offset) = rt_entry.refblock_offset() else {
                let start_index = HostCluster::from_ref_indices(rt_index, 0, rb_bits);
                free_cluster_index.get_or_insert(start_index);
                free_cluster_count += ClusterCount(rb_entries as u64);
                // Need to allocate this RB
                required_clusters += ClusterCount(1);
                continue;
            };

            let rb_cluster = rb_offset.checked_cluster(cb).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unaligned refcount block with index {rt_index}; refcount table entry: {rt_entry:?}"),
                )
            })?;

            let rb = self.rb_cache.get_or_insert(rb_cluster).await?;
            for i in rb_index..rb_entries {
                if rb.is_zero(i) {
                    let index = HostCluster::from_ref_indices(rt_index, i, rb_bits);
                    free_cluster_index.get_or_insert(index);
                    free_cluster_count += ClusterCount(1);

                    if free_cluster_count >= required_clusters {
                        break;
                    }
                } else if free_cluster_index.is_some() {
                    free_cluster_index.take();
                    free_cluster_count = ClusterCount(0);
                    required_clusters = rt_clusters; // reset
                }
            }

            rb_index = 0;
            rt_index += 1;
        }

        let mut index = free_cluster_index.unwrap();
        let mut count = required_clusters;

        // Put refblocks first
        let rt_index_start = index.rt_index(rb_bits);
        let rt_index_end = (index + count).0.div_ceil(rb_entries as u64) as usize;

        let mut refblocks = Vec::<Arc<RefBlock>>::new();
        for rt_i in rt_index_start..rt_index_end {
            if let Some(rb_offset) = new_rt.get(rt_i).refblock_offset() {
                // Checked in the loop above
                let rb_cluster = rb_offset.checked_cluster(cb).unwrap();
                let rb = self.rb_cache.get_or_insert(rb_cluster).await?;
                refblocks.push(rb);
                continue;
            }

            let mut rb = RefBlock::new_cleared(self.file.as_ref(), &self.header)?;
            rb.set_cluster(index);
            new_rt.enter_refblock(rt_i, &rb)?;
            let rb = Arc::new(rb);
            self.rb_cache.insert(index, Arc::clone(&rb)).await?;
            refblocks.push(rb);
            index += ClusterCount(1);
            count -= ClusterCount(1);
        }

        assert!(count >= rt_clusters);
        new_rt.set_cluster(index);

        // Now set allocation information
        let start_index = free_cluster_index.unwrap();
        let end_index = index + rt_clusters;

        for index in start_index.0..end_index.0 {
            let index = HostCluster(index);
            let (rt_i, rb_i) = index.rt_rb_indices(rb_bits);

            // `refblocks[0]` is for `rt_index_start`
            let rb_vec_i = rt_i - rt_index_start;
            // Incrementing from 0 to 1 must succeed
            refblocks[rb_vec_i]
                .lock_write()
                .await
                .increment(rb_i)
                .unwrap();
        }

        // Any errors from here on may lead to leaked clusters if there are refblocks in
        // `refblocks` that are already part of the old reftable.
        // TODO: Try to clean that up, though it seems quite hard for little gain.
        self.rb_cache.flush().await?;
        new_rt.write(self.file.as_ref()).await?;

        self.header.set_reftable(&new_rt)?;
        self.header
            .write_reftable_pointer(self.file.as_ref())
            .await?;

        // Must set new reftable before calling `free_clusters()`
        let mut old_reftable = mem::replace(&mut self.reftable, new_rt);
        if let Some(old_rt_cluster) = old_reftable.get_cluster() {
            let old_rt_size = old_reftable.cluster_count(cb);
            old_reftable.unset_cluster();
            self.free_clusters(old_rt_cluster, old_rt_size).await;
        }

        Ok(())
    }

    /// Free clusters (i.e. decrement their refcount).
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    async fn free_clusters(&mut self, start: HostCluster, mut count: ClusterCount) {
        if count.0 == 0 {
            return;
        }

        if start < self.first_free_cluster {
            self.first_free_cluster = start;
        }

        let rb_bits = self.header.rb_bits();
        let rb_entries = 1 << rb_bits;
        let (mut rt_index, mut rb_index) = start.rt_rb_indices(rb_bits);

        while count > ClusterCount(0) {
            let in_rb_count = cmp::min((rb_entries - rb_index) as u64, count.0) as usize;

            match self.get_rb(rt_index).await {
                Ok(Some(rb)) => {
                    let mut rb = rb.lock_write().await;
                    for i in rb_index..(rb_index + in_rb_count) {
                        if let Err(err) = rb.decrement(i) {
                            event!(Level::WARN, "Failed to free cluster: {err}");
                        }
                    }
                }

                Ok(None) => {
                    event!(
                        Level::WARN,
                        "Failed to free {in_rb_count} clusters: Not allocated"
                    )
                }
                Err(err) => event!(Level::WARN, "Failed to free {in_rb_count} clusters: {err}"),
            }

            count -= ClusterCount(in_rb_count as u64);
            rb_index = 0;
            rt_index += 1;
        }
    }
}
