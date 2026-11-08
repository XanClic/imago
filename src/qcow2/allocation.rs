//! Cluster allocation.
//!
//! Functionality for allocating single clusters and ranges of clusters, and general handling of
//! refcount structures.

use super::*;
use crate::sync_primitives::MutexGuard;
use parking_lot::Mutex as SyncMutex;
use std::mem;
use tracing::{event, warn, Level};

/// List of clusters that are no longer in use, and should be freed.
///
/// Shared between [`Qcow2`] and its [`Allocator`].
#[derive(Default)]
pub(super) struct ClustersToFreeList {
    /// The list itself.
    ///
    /// Synchronous mutex because it is never to be held across `.await` points.
    list: SyncMutex<Vec<ClustersToFreeEntry>>,
}

impl ClustersToFreeList {
    /// Mark `entry` as a cluster to be freed.
    pub fn push(&self, entry: ClustersToFreeEntry) {
        self.list.lock().push(entry);
    }

    /// Check whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.list.lock().is_empty()
    }

    /// Remove all entries from the list and return them.
    pub fn take(&self) -> Vec<ClustersToFreeEntry> {
        mem::take(&mut *self.list.lock())
    }

    /// Create a guard that enters `entry` into the list when dropped, unless disarmed.
    ///
    /// This is for use in allocation paths, where a cluster is allocated, not yet hooked up, and
    /// anything might cause the hooking-up not to happen, in which case `entry` should be dropped.
    /// Once it is hooked up, the guard will be disarmed, thus making the cluster permanent.
    pub fn pending_guard(self: &Arc<Self>, entry: ClustersToFreeEntry) -> PendingClusters {
        PendingClusters {
            clusters_to_free: Arc::clone(self),
            entry: Some(entry),
        }
    }
}

/// Guard for newly allocated clusters that are not yet referenced by any metadata.
///
/// If the operation that allocated them fails or is cancelled (i.e. its future is dropped) before
/// it can reference them, dropping this guard enters them into [`ClustersToFreeList`], so they are
/// not leaked.  Once they are referenced (at least in memory), disarm the guard.
#[must_use = "Dropping the guard frees the clusters"]
pub(super) struct PendingClusters {
    /// List to enter the clusters into.
    clusters_to_free: Arc<ClustersToFreeList>,

    /// The clusters; `None` when disarmed.
    entry: Option<ClustersToFreeEntry>,
}

impl PendingClusters {
    /// First cluster of the allocation.
    pub fn cluster(&self) -> HostCluster {
        // Only `None` after `make_persistent()`, which consumes `self`
        self.entry.as_ref().unwrap().first_cluster
    }

    /// Disarm the guard, moving the clusters from a pending state (to be freed when something goes
    /// wrong) to a permanent state, because they are referenced now.
    pub fn make_persistent(mut self) {
        self.entry.take();
    }
}

impl Drop for PendingClusters {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            self.clusters_to_free.push(entry);
        }
    }
}

/// Entry in [`ClustersToFreeList`]: A range of clusters to be freed.
pub(super) struct ClustersToFreeEntry {
    /// First cluster in the range.
    first_cluster: HostCluster,

    /// Number of clusters in the range.
    count: ClusterCount,

    /// Whether these are metadata clusters (or data clusters).
    metadata: bool,
}

impl ClustersToFreeEntry {
    /// Create an entry for the given range of data clusters.
    pub fn data(first_cluster: HostCluster, count: ClusterCount) -> Self {
        ClustersToFreeEntry {
            first_cluster,
            count,
            metadata: false,
        }
    }

    /// Create an entry for the given range of metadata clusters.
    pub fn metadata(first_cluster: HostCluster, count: ClusterCount) -> Self {
        ClustersToFreeEntry {
            first_cluster,
            count,
            metadata: true,
        }
    }
}

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

    /// L2 and refblock caches with dependency management.
    caches: Arc<MetadataCaches<S>>,

    /// Clusters that are no longer referenced by image metadata, but not yet freed.
    ///
    /// Shared with [`Qcow2`].
    clusters_to_free: Arc<ClustersToFreeList>,
}

#[maybe_async]
impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2<S, F> {
    /// Return the central allocator instance.
    ///
    /// Returns an error for read-only images.
    async fn allocator(&self) -> io::Result<MutexGuard<'_, Allocator<S>>> {
        self.header.ensure_healthy()?;
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| io::Error::other("Image is read-only"))?
            .lock()
            .await;
        self.header.ensure_healthy()?;
        Ok(allocator)
    }

    /// Allocate one metadata cluster.
    ///
    /// Metadata clusters are allocated exclusively in the metadata (image) file.
    ///
    /// The returned guard frees the cluster when dropped, unless it is disarmed by referencing the
    /// cluster.
    pub(super) async fn allocate_meta_cluster(&self) -> io::Result<PendingClusters> {
        self.allocate_meta_clusters(ClusterCount(1)).await
    }

    /// Allocate multiple continuous metadata clusters.
    ///
    /// Useful e.g. for the L1 table or refcount table.
    ///
    /// The returned guard frees the clusters when dropped, unless it is disarmed by referencing
    /// the clusters.
    pub(super) async fn allocate_meta_clusters(
        &self,
        count: ClusterCount,
    ) -> io::Result<PendingClusters> {
        self.allocator()
            .await?
            .allocate_clusters(count, None, true)
            .await
    }

    /// Allocate one data clusters for the given guest cluster.
    ///
    /// Without an external data file, data clusters are allocated in the image file, just like
    /// metadata clusters.
    ///
    /// With an external data file, data clusters aren’t really allocated, but just put there at
    /// the same offset as their guest offset.  Their refcount is not tracked by the qcow2 metadata
    /// structures (which only cover the metadata (image) file).
    ///
    /// The returned guard frees the cluster when dropped, unless it is disarmed by referencing the
    /// cluster.  (With an external data file, freeing is a no-op.)
    pub(super) async fn allocate_data_cluster(
        &self,
        guest_cluster: GuestCluster,
    ) -> io::Result<PendingClusters> {
        self.header.ensure_healthy()?;
        if self.header.external_data_file() {
            let cluster = HostCluster(guest_cluster.0);
            Ok(self
                .clusters_to_free
                .pending_guard(ClustersToFreeEntry::data(cluster, ClusterCount(1))))
        } else {
            let mut allocator = self.allocator().await?;

            // Allocate clusters before setting up L2 entries
            self.caches.l2_depends_on_rb().await?;

            let allocation = allocator
                .allocate_clusters(ClusterCount(1), None, false)
                .await?;
            // A concurrent flush may have snapshotted the RB before its refcount increment.
            self.caches.l2_depends_on_rb().await?;
            Ok(allocation)
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
    ) -> io::Result<Option<PendingClusters>> {
        self.header.ensure_healthy()?;
        let Some(mandatory_host_cluster) = mandatory_host_cluster else {
            return self.allocate_data_cluster(guest_cluster).await.map(Some);
        };

        let allocated = if self.header.external_data_file() {
            HostCluster(guest_cluster.0) == mandatory_host_cluster
        } else {
            let mut allocator = self.allocator().await?;

            // Allocate clusters before setting up L2 entries
            self.caches.l2_depends_on_rb().await?;

            allocator
                .allocate_cluster_at(mandatory_host_cluster)
                .await?
        };

        let allocation = allocated.then(|| {
            self.clusters_to_free
                .pending_guard(ClustersToFreeEntry::data(
                    mandatory_host_cluster,
                    ClusterCount(1),
                ))
        });
        if allocation.is_some() && !self.header.external_data_file() {
            // Reestablish the dependency after the refcount change.
            self.caches.l2_depends_on_rb().await?;
        }
        Ok(allocation)
    }

    /// Free all clusters in `self.clusters_to_free`.
    ///
    /// Best-effort operation, like [`Allocator::process_clusters_to_free()`].
    pub(super) async fn process_clusters_to_free(&self) {
        // Skip taking the allocator lock if there is nothing to do
        if self.clusters_to_free.is_empty() {
            return;
        }

        if let Ok(mut allocator) = self.allocator().await {
            allocator.process_clusters_to_free().await;
        }
    }
}

#[maybe_async]
impl<S: Storage> Allocator<S> {
    /// Create a new allocator for the given image file.
    pub async fn new(
        image: Arc<S>,
        header: Arc<Header>,
        caches: Arc<MetadataCaches<S>>,
        clusters_to_free: Arc<ClustersToFreeList>,
    ) -> io::Result<Self> {
        let cb = header.cluster_bits();
        let rt_offset = header.reftable_offset();
        let rt_cluster = rt_offset
            .checked_cluster(cb)
            .ok_or_else(|| invalid_data(format!("Unaligned refcount table: {rt_offset}")))?;

        let reftable = RefTable::load(
            image.as_ref(),
            &header,
            rt_cluster,
            header.reftable_entries(),
        )
        .await?;

        Ok(Allocator {
            file: image,
            reftable,
            first_free_cluster: HostCluster(0),
            header,
            caches,
            clusters_to_free,
        })
    }

    /// Validate active metadata, allocation ownership and COPIED bits before enabling writes.
    pub async fn validate_active_metadata(&mut self, l1: &L1Table) -> io::Result<()> {
        let cb = self.header.cluster_bits();
        let mut l2_clusters: Vec<_> = (0..l1.entries())
            .filter_map(|index| l1.get(index).l2_offset())
            .map(|offset| offset.cluster(cb))
            .collect();
        l2_clusters.sort_unstable();
        l2_clusters.dedup();

        let mut metadata = vec![(0, 1)];
        for (offset, count) in [
            (self.header.l1_table_offset(), l1.cluster_count()),
            (
                self.header.reftable_offset(),
                self.header.reftable_clusters(),
            ),
        ] {
            if count.0 != 0 {
                let start = offset.cluster(cb).0;
                metadata.push((start, start + count.0));
            }
        }
        metadata.extend(
            (0..self.reftable.entries())
                .filter_map(|index| self.reftable.get(index).refblock_offset())
                .map(|offset| {
                    let cluster = offset.cluster(cb).0;
                    (cluster, cluster + 1)
                }),
        );
        let mut references: Vec<_> = metadata
            .iter()
            .flat_map(|&(start, end)| (start..end).map(|cluster| (HostCluster(cluster), None)))
            .collect();
        metadata.extend(l2_clusters.iter().map(|cluster| (cluster.0, cluster.0 + 1)));
        metadata.sort_unstable();
        if metadata.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(invalid_data("Active qcow2 metadata overlaps"));
        }

        for index in 0..l1.entries() {
            let entry = l1.get(index);
            let Some(offset) = entry.l2_offset() else {
                if entry.is_copied() {
                    return Err(invalid_data("Unallocated L2 table has COPIED bit set"));
                }
                continue;
            };
            let cluster = offset.cluster(cb);
            references.push((cluster, Some(entry.is_copied())));
            let l2 = L2Table::load(
                self.file.as_ref(),
                &self.header,
                cluster,
                self.header.l2_entries(),
            )
            .await?;
            for index in 0..l2.entries() {
                let entry = l2.get(index);
                let Some((start, count)) = entry.allocation(cb, false) else {
                    if entry.is_copied() {
                        return Err(invalid_data("Unallocated data cluster has COPIED bit set"));
                    }
                    continue;
                };
                let end = start.0 + count.0;
                let next = metadata.partition_point(|&(_, end)| end <= start.0);
                if metadata.get(next).is_some_and(|&(start, _)| start < end) {
                    return Err(invalid_data(format!(
                        "QCOW2 data overlaps active metadata at host cluster {}",
                        start.0,
                    )));
                }
                let copied = (!entry.is_compressed()).then_some(entry.is_copied());
                references.extend((start.0..end).map(|cluster| (HostCluster(cluster), copied)));
            }
        }

        references.sort_unstable_by_key(|&(cluster, _)| cluster);
        for group in references.chunk_by(|left, right| left.0 == right.0) {
            let cluster = group[0].0;
            let (rt_index, rb_index) = cluster.rt_rb_indices(self.header.rb_bits());
            let refcount = self
                .get_rb(rt_index)
                .await?
                .map_or(0, |rb| rb.get(rb_index));
            let required = group.len() as u64;
            if refcount < required {
                return Err(invalid_data(format!(
                    "QCOW2 cluster {} has refcount {refcount}, needs at least {required} active references",
                    cluster.0,
                )));
            }
            if group
                .iter()
                .any(|&(_, copied)| copied.is_some_and(|copied| copied != (refcount == 1)))
            {
                return Err(invalid_data(format!(
                    "QCOW2 cluster {} has inconsistent COPIED bit for refcount {refcount}",
                    cluster.0,
                )));
            }
        }
        Ok(())
    }

    /// Free all clusters in `self.clusters_to_free` (i.e. decrement their refcount).
    ///
    /// Must not be called while allocating clusters: Freeing data clusters requires the refblock
    /// cache to depend on the L2 cache, which is the opposite of what allocating requires.
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    async fn process_clusters_to_free(&mut self) {
        if self.header.ensure_healthy().is_err() {
            return;
        }
        let (metadata_clusters, mut data_clusters): (Vec<_>, Vec<_>) = self
            .clusters_to_free
            .take()
            .into_iter()
            .partition(|entry| entry.metadata);

        // The refcount structures only track allocations in the metadata file (the qcow2 file), so
        // when freeing data clusters with an external data file, nothing is to be done
        if self.header.external_data_file() {
            data_clusters.clear();
        }

        if metadata_clusters.is_empty() && data_clusters.is_empty() {
            return;
        }

        // Writes whose futures have been dropped may still be in flight to these clusters, and
        // must not be performed after the clusters have been reused.
        // For data clusters, that can be any cancelled guest write; for metadata clusters, it
        // could for example be a canceled L1 or reftable entry write, after which the table is
        // grown and the old one freed.
        // In any case, all of these clusters will be in the qcow2 (metadata) file, because data
        // clusters in external data files do not need this treatment.
        if let Err(err) = self.file.settle_writes().await {
            warn!("Deferring cluster release; cannot settle writes: {err}");
            for entry in metadata_clusters.into_iter().chain(data_clusters) {
                self.clusters_to_free.push(entry);
            }
            return;
        }

        if !metadata_clusters.is_empty() {
            // Old L1/L2/reftable references must be durable before their clusters are reusable.
            let result = match self.file.flush().await {
                Ok(()) => self.file.sync().await,
                Err(err) => Err(err),
            };
            if let Err(err) = result {
                warn!("Deferring cluster release; cannot persist metadata pointers: {err}");
                for entry in metadata_clusters.into_iter().chain(data_clusters) {
                    self.clusters_to_free.push(entry);
                }
                return;
            }
        }
        for entry in metadata_clusters {
            self.free_clusters(entry.first_cluster, entry.count).await;
        }

        if data_clusters.is_empty() {
            return;
        }

        // Removed mappings must be durable before their data can be punched out.
        let prepare = match self.caches.rb_depends_on_l2().await {
            Ok(()) => self.caches.flush_l2().await,
            Err(err) => Err(err),
        };
        if let Err(err) = prepare {
            warn!("Cannot persist removed mappings; deferring data cluster release: {err}");
            for entry in data_clusters {
                self.clusters_to_free.push(entry);
            }
            return;
        }

        for entry in data_clusters {
            self.free_clusters(entry.first_cluster, entry.count).await;
        }
    }

    /// Invaidate the refcount block cache.
    ///
    /// # Safety
    /// May cause image corruption, you must guarantee the on-disk state is consistent.
    pub async unsafe fn invalidate_rb_cache(&self) -> io::Result<()> {
        // Safe: Caller says so.
        unsafe { self.caches.invalidate_rb() }.await
    }

    /// Allocate clusters in the image file.
    ///
    /// `end_cluster` should only be used when allocating refblocks.  When reaching this cluster
    /// index, abort trying to allocate.  (This is used for allocating refblocks, to prevent
    /// infinite recursion and speed things up.)
    ///
    /// `metadata` specifies whether the clusters are to be used for metadata (true) or data
    /// (false), in order to create the right kind of [`ClustersToFreeEntry`] for the returned
    /// [`PendingClusters`] guard.  That returned guard frees the cluster when dropped, unless it
    /// is disarmed by referencing the cluster.
    async fn allocate_clusters(
        &mut self,
        count: ClusterCount,
        end_cluster: Option<HostCluster>,
        metadata: bool,
    ) -> io::Result<PendingClusters> {
        self.header.ensure_healthy()?;
        let mut index = self.first_free_cluster;
        loop {
            if end_cluster == Some(index) {
                return Err(io::Error::other("Maximum cluster index reached"));
            }

            let alloc_count = self.allocate_clusters_at(index, count).await?;
            if alloc_count == count {
                let to_free_entry = if metadata {
                    ClustersToFreeEntry::metadata(index, count)
                } else {
                    ClustersToFreeEntry::data(index, count)
                };
                return Ok(self.clusters_to_free.pending_guard(to_free_entry));
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
            // Note that `ensure_rb()` in `allocate_cluster_at()` may allocate clusters (new
            // refblocks), and also a new refcount table.  This can interfere with us allocating a
            // large continuous region like so (A is our allocation, R is a refblock, imagine a
            // refblock covers four clusters):
            //
            // |AAAA| -- allocated four clusters need new refblock
            // |AAAA|R   | -- made refblock self-describing, but now allocation cannot go on
            //
            // This gets resolved by us retrying, and future refblocks using the region that has
            // now become free but already has refblocks to cover it:
            //
            // |    |RAAA| -- retry after refblock; need a new refblock again
            // |R   |RAAA|AAAA| -- the new refblock allocates itself in the region we abandoned
            //
            // However, eventually, the new refblocks will run into the new start of our allocation
            // again:
            //
            // |RRRR|RAAA|AAAA|AAAA|AAAA|AAAA| -- need new refblock
            // |RRRR|RAAA|AAAA|AAAA|AAAA|AAAA|R   | -- allocation cannot go on, again
            // |RRRR|R   |    |    |    |    |RAAA| -- another attempt
            // |RRRR|RRRR|R...|    |    |    |RAAA|AAAA|AAAA|AAAA|AAAA|...
            //
            // As you can see, the hole we leave behind gets larger each time.  So eventually, this
            // must converge.
            //
            // The same applies to the refcount table being allocated instead of just refblocks.

            let result = self.allocate_cluster_at(index).await;
            if !matches!(result, Ok(true)) {
                // Already allocated, or some real error occurred; free everything allocated so far.
                // Do not go through `self.clusters_to_free`: As described above, the freed range
                // must be available for allocation immediately, so that retrying converges.
                self.free_clusters(start_index, index - start_index).await;
                return result.map(|_| index - start_index);
            }

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
        self.header.ensure_healthy()?;
        let rb_bits = self.header.rb_bits();
        let (rt_index, rb_index) = index.rt_rb_indices(rb_bits);

        let rb = self.ensure_rb(rt_index).await?;
        let mut rb = rb.lock_write().await;
        self.header.ensure_healthy()?;
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
        self.header.ensure_healthy()?;
        let rt_entry = self.reftable.get(rt_index);
        if let Some(rb_offset) = rt_entry.refblock_offset() {
            let cb = self.header.cluster_bits();
            let rb_cluster = rb_offset.checked_cluster(cb).ok_or_else(|| {
                invalid_data(format!("Unaligned refcount block with index {rt_index}; refcount table entry: {rt_entry:?}"))
            })?;

            self.caches.rb_get_or_insert(rb_cluster).await.map(Some)
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

        let mut new_rb = RefBlock::new_cleared(self.file.as_ref(), &self.header)?;

        // This is the first cluster covered by the new refblock
        let rb_cluster = HostCluster::from_ref_indices(rt_index, 0, self.header.rb_bits());

        // Try to allocate a cluster in the already existing refcount structures.
        // By stopping looking for clusters at `rb_cluster`, we ensure that we will not land here
        // in this exact function again, trying to allocate the very same refblock (it is possible
        // we allocate one before the current one, though), and so prevent any possible infinite
        // recursion.
        let alloc_fut_or_result = self.allocate_clusters(ClusterCount(1), Some(rb_cluster), true);

        // Recursion is possible, though, so in async mode, any future must be boxed
        #[cfg(feature = "async")]
        let alloc_result = Box::pin(alloc_fut_or_result).await;
        // Whereas in sync mode, we now already have the result
        #[cfg(feature = "sync")]
        let alloc_result = alloc_fut_or_result;

        self.header.ensure_healthy()?;
        let new_rb_allocation = if let Ok(new_rb_allocation) = alloc_result {
            new_rb.set_cluster(new_rb_allocation.cluster());
            Some(new_rb_allocation)
        } else {
            // Place the refblock such that it covers itself.  Its refcount is stored only in
            // itself, so until it is referenced, there is nothing that could be leaked.
            new_rb.set_cluster(rb_cluster);
            new_rb.lock_write().await.increment(0)?;
            None
        };
        if new_rb_allocation.is_none() {
            let mut initialization = self.header.publication_guard();
            let result = new_rb.write(self.file.as_ref()).await;
            initialization.disarm();
            result?;
        } else {
            new_rb.write(self.file.as_ref()).await?;
        }
        self.caches.flush_rb().await?;

        self.header.ensure_healthy()?;
        self.reftable
            .enter_refblock(rt_index, &new_rb, new_rb_allocation)?;
        let mut publication = self.header.publication_guard();
        self.reftable
            .write_entry(self.file.as_ref(), rt_index)
            .await?;
        self.header.ensure_healthy()?;
        publication.disarm();

        let new_rb = Arc::new(new_rb);
        self.caches.rb_insert(Arc::clone(&new_rb)).await?;
        Ok(new_rb)
    }

    /// Create a new refcount table covering at least `at_least_index`.
    ///
    /// Create a new reftable of the required size, copy all existing refblock references into it,
    /// ensure it is refcounted itself (also creating new refblocks if necessary), and have the
    /// image header reference the new refcount table.
    async fn grow_reftable(&mut self, at_least_index: usize) -> io::Result<()> {
        self.header.ensure_healthy()?;
        let cb = self.header.cluster_bits();
        let rb_bits = self.header.rb_bits();
        let rb_entries = 1 << rb_bits;

        let mut new_rt = self.reftable.clone_and_grow(&self.header, at_least_index)?;
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
            if let Some(rb_offset) = rt_entry.refblock_offset() {
                let rb_cluster = rb_offset.checked_cluster(cb).ok_or_else(|| {
                    invalid_data(format!(
                        "Unaligned refcount block with index {rt_index}; refcount table entry: {rt_entry:?}"
                    ))
                })?;

                let rb = self.caches.rb_get_or_insert(rb_cluster).await?;
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
            } else {
                let start_index = HostCluster::from_ref_indices(rt_index, rb_index, rb_bits);
                free_cluster_index.get_or_insert(start_index);
                free_cluster_count += ClusterCount((rb_entries - rb_index) as u64);
                // Need to allocate this RB
                required_clusters += ClusterCount(1);
            }

            rb_index = 0;
            rt_index += 1;
        }

        let mut index = free_cluster_index.unwrap();
        let mut count = required_clusters;

        // Put refblocks first
        let rt_index_start = index.rt_index(rb_bits);
        let rt_index_end = (index + count).0.div_ceil(rb_entries as u64) as usize;

        let mut refblocks = Vec::<(Arc<RefBlock>, bool)>::new();
        for rt_i in rt_index_start..rt_index_end {
            if let Some(rb_offset) = new_rt.get(rt_i).refblock_offset() {
                // Checked in the loop above
                let rb_cluster = rb_offset.checked_cluster(cb).unwrap();
                let rb = self.caches.rb_get_or_insert(rb_cluster).await?;
                refblocks.push((rb, false));
                continue;
            }

            let mut rb = RefBlock::new_cleared(self.file.as_ref(), &self.header)?;
            rb.set_cluster(index);
            new_rt.enter_refblock(rt_i, &rb, None)?;
            refblocks.push((Arc::new(rb), true));
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
                .0
                .lock_write()
                .await
                .increment(rb_i)
                .unwrap();
        }

        // Any errors from here on may lead to leaked clusters if there are refblocks in
        // `refblocks` that are already part of the old reftable.
        // TODO: Try to clean that up, though it seems quite hard for little gain.
        self.caches.flush_rb().await?;
        let mut publication = self.header.publication_guard();
        for (rb, is_new) in &refblocks {
            if *is_new {
                rb.write(self.file.as_ref()).await?;
            }
        }
        new_rt.write(self.file.as_ref()).await?;
        self.file.flush().await?;
        self.file.sync().await?;

        let header_locked = self.header.publication_lock.lock().await;
        self.header.ensure_healthy()?;
        self.header.set_reftable(&new_rt)?;
        self.header
            .write_reftable_pointer(self.file.as_ref())
            .await?;

        // Must set new reftable before freeing the old reftable’s clusters
        let mut old_reftable = mem::replace(&mut self.reftable, new_rt);
        publication.disarm();
        drop(header_locked);
        if let Some(old_rt_cluster) = old_reftable.get_cluster() {
            let old_rt_size = old_reftable.cluster_count();
            old_reftable.unset_cluster();
            // Do not run `process_clusters_to_free()` here: We are in the middle of allocating
            // clusters, and that function would flip the L2-RB cache dependency. Our caller will
            // drain the list eventually.
            self.clusters_to_free
                .push(ClustersToFreeEntry::metadata(old_rt_cluster, old_rt_size));
        }

        for (rb, is_new) in refblocks {
            if is_new {
                self.caches.rb_insert(rb).await?;
            }
        }
        self.header.ensure_healthy()
    }

    /// Free clusters (i.e. decrement their refcount).
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    async fn free_clusters(&mut self, start: HostCluster, mut count: ClusterCount) {
        if count.0 == 0 || self.header.ensure_healthy().is_err() {
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
                    if self.header.ensure_healthy().is_err() {
                        return;
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::null::Null;
    use crate::test::mem_storage::{create_image, MemStorage, MemStorageState};
    use std::sync::atomic::Ordering;

    /// Exercise one metadata publication site.
    #[maybe_async]
    async fn publish_metadata(qcow2: &Qcow2<MemStorage>, site: usize) -> io::Result<()> {
        match site {
            0 => qcow2.ensure_l2(GuestOffset(0)).await.map(|_| ()),
            1 => qcow2.allocator().await?.ensure_rb(1).await.map(|_| ()),
            2 => {
                let l1 = qcow2.l1_table.write().await;
                let index = l1.entries();
                qcow2.grow_l1_table(l1, index).await.map(|_| ())
            }
            3 => {
                let index = qcow2.header.reftable_entries();
                qcow2.allocator().await?.grow_reftable(index).await
            }
            4 => {
                qcow2
                    .header
                    .write_size(qcow2.metadata.as_ref(), qcow2.size() + 512)
                    .await
            }
            _ => unreachable!(),
        }
    }

    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_metadata_publication_fail_closed() -> io::Result<()> {
        #[cfg(feature = "sync")]
        let cancellation_modes = [false];
        #[cfg(feature = "async")]
        let cancellation_modes = [false, true];
        for site in 0..5 {
            for cancel in cancellation_modes {
                let state = Arc::new(MemStorageState::default());
                let qcow2 = create_image(&state, 64 * 1024).await;
                let prepared = qcow2.prepare_data_mapping(32 * 1024, 512, true).await?;
                let pending = qcow2.allocate_meta_cluster().await?;
                let cluster = pending.cluster();
                let (rt_index, rb_index) = cluster.rt_rb_indices(qcow2.header.rb_bits());
                let rb = qcow2.allocator().await?.get_rb(rt_index).await?.unwrap();
                qcow2.flush().await?;
                let offset = match site {
                    0 => qcow2.header.l1_table_offset().0,
                    1 => qcow2.header.reftable_offset().0 + 8,
                    _ => 0,
                };
                let before = state.data.lock()[offset as usize..offset as usize + 8].to_vec();
                if cancel {
                    #[cfg(feature = "async")]
                    {
                        *state.hold_range.lock() = Some(offset..offset + 8);
                        state.hold_writes.store(true, Ordering::Relaxed);
                        let mut publication = Box::pin(publish_metadata(&qcow2, site));
                        assert!(
                            crate::test::mem_storage::poll_once(publication.as_mut()).is_pending()
                        );
                        assert_eq!(state.held.lock()[0].0, offset);
                        let waiter_site = if site == 2 { 3 } else { site };
                        let mut waiter = Box::pin(publish_metadata(&qcow2, waiter_site));
                        assert!(crate::test::mem_storage::poll_once(waiter.as_mut()).is_pending());
                        assert_eq!(state.held.lock().len(), 1);
                        drop(publication);
                        waiter.await.err().unwrap();
                    }
                } else {
                    *state.fail_write_offset.lock() = Some(offset);
                    publish_metadata(&qcow2, site).await.err().unwrap();
                    assert_eq!(*state.failed.lock(), [offset]);
                }
                assert_eq!(
                    &state.data.lock()[offset as usize..offset as usize + 8],
                    before.as_slice()
                );
                assert!(qcow2.header.ensure_healthy().is_err());
                assert_eq!(qcow2.size(), 64 * 1024);
                qcow2.commit_data_mapping(prepared).await.unwrap_err();
                qcow2
                    .resize_grow(4 * 1024 * 1024, PreallocateMode::None)
                    .await
                    .unwrap_err();
                qcow2.get_mapping(0, 512).await.err().unwrap();
                qcow2.flush().await.unwrap_err();
                qcow2.sync().await.unwrap_err();
                drop(pending);
                qcow2.process_clusters_to_free().await;
                let mut allocator = qcow2.allocator.as_ref().unwrap().lock().await;
                allocator.process_clusters_to_free().await;
                allocator.free_clusters(cluster, ClusterCount(1)).await;
                assert_eq!(rb.get(rb_index), 1);
                allocator.allocate_cluster_at(cluster).await.unwrap_err();
                drop(allocator);
                #[cfg(feature = "async")]
                if cancel {
                    state.complete_held_writes();
                    state.release_held_writes();
                    qcow2.flush().await.unwrap_err();
                    qcow2.get_mapping(0, 512).await.err().unwrap();
                }
            }
        }
        Ok(())
    }

    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_metadata_publication_cache_write_retryable() -> io::Result<()> {
        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024).await;
        for (offset, fail_sync) in [(0, false), (32768, true)] {
            state.fail_next_write.store(!fail_sync, Ordering::Relaxed);
            state.fail_next_sync.store(fail_sync, Ordering::Relaxed);
            qcow2.ensure_l2(GuestOffset(offset)).await.err().unwrap();
            let l1_offset = (qcow2.header.l1_table_offset().0 + offset / 32768 * 8) as usize;
            assert_eq!(&state.data.lock()[l1_offset..l1_offset + 8], &[0; 8]);
            qcow2.header.ensure_healthy().unwrap();
            qcow2.ensure_data_mapping(offset, 512, true).await?;
            qcow2.flush().await?;
        }
        let rb_offset = state.read_u64(qcow2.header.reftable_offset().0);
        qcow2.ensure_data_mapping(512, 512, true).await?;
        qcow2.ensure_zero_mapping(0, 512).await?;
        *state.fail_write_offset.lock() = Some(rb_offset);
        qcow2.flush().await.unwrap_err();
        qcow2.header.ensure_healthy().unwrap();
        qcow2.flush().await?;
        qcow2.ensure_data_mapping(0, 512, true).await?;
        qcow2.flush().await?;
        Ok(())
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_metadata_publication_private_rb_and_serialized_header() -> io::Result<()> {
        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024).await;
        let index = qcow2.header.reftable_entries();
        let cluster = HostCluster::from_ref_indices(index, 0, qcow2.header.rb_bits());
        qcow2.allocator().await?.first_free_cluster = cluster;
        qcow2.flush().await?;
        let offset = cluster.offset(qcow2.header.cluster_bits()).0;
        *state.hold_range.lock() = Some(offset..offset + 512);
        state.hold_writes.store(true, Ordering::Relaxed);
        let mut preparation = Box::pin(publish_metadata(&qcow2, 3));
        assert!(crate::test::mem_storage::poll_once(preparation.as_mut()).is_pending());
        assert_eq!(state.held.lock()[0].0, offset);
        drop(preparation);
        state.complete_held_writes();
        state.release_held_writes();
        let reused = [0x5a; 512];
        state.write(offset, &reused);
        qcow2.caches.flush_rb().await.unwrap_err();
        assert_eq!(
            &state.data.lock()[offset as usize..offset as usize + 512],
            &reused
        );
        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024).await;
        qcow2.flush().await?;
        *state.hold_range.lock() = Some(0..512);
        state.hold_writes.store(true, Ordering::Relaxed);
        let mut l1_publication = Box::pin(publish_metadata(&qcow2, 2));
        assert!(crate::test::mem_storage::poll_once(l1_publication.as_mut()).is_pending());
        let mut rt_publication = Box::pin(publish_metadata(&qcow2, 3));
        assert!(crate::test::mem_storage::poll_once(rt_publication.as_mut()).is_pending());
        let new_size = qcow2.size() + 512;
        let mut size_publication =
            Box::pin(qcow2.header.write_size(qcow2.metadata.as_ref(), new_size));
        assert!(crate::test::mem_storage::poll_once(size_publication.as_mut()).is_pending());
        assert_eq!(state.held.lock().len(), 1);
        state.release_held_writes();
        l1_publication.await?;
        rt_publication.await?;
        size_publication.await?;
        let reopened = Header::load(qcow2.metadata.as_ref(), false).await?;
        assert_eq!(reopened.size(), new_size);
        assert_eq!(reopened.l1_table_offset(), qcow2.header.l1_table_offset());
        assert_eq!(reopened.l1_table_entries(), qcow2.header.l1_table_entries());
        assert_eq!(reopened.reftable_offset(), qcow2.header.reftable_offset());
        assert_eq!(reopened.reftable_entries(), qcow2.header.reftable_entries());
        qcow2.flush().await?;
        Ok(())
    }

    /// Growing past an unmapped refblock range must preserve the next occupied refblock.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_grow_reftable_keeps_in_use_refblock() {
        let header = Arc::new(Header::new(9, 6, None, None, None));
        let rb_entries = header.rb_entries();
        let null = Arc::new(Null::new(1 << 30));
        let caches = Arc::new(MetadataCaches::new(&null, &header, 128, 32));

        // A 65-cluster reftable must skip the unmapped 64-cluster range at index 4094.
        let mut data = vec![RefTableEntry::default(); 4096];
        for (rt_index, occupied) in [(4093, rb_entries), (4095, 2)] {
            let mut rb = RefBlock::new_cleared(null.as_ref(), &header).unwrap();
            rb.set_cluster(HostCluster((rt_index * rb_entries) as u64));
            {
                let mut writer = rb.lock_write().await;
                for index in 0..occupied {
                    writer.increment(index).unwrap();
                }
            }
            data[rt_index] =
                <RefTableEntry as TableEntry>::try_from_plain(rb.get_offset().unwrap().0, &header)
                    .unwrap();
            caches.rb_insert(Arc::new(rb)).await.unwrap();
        }
        let rb_b = caches
            .rb_get_or_insert(HostCluster((4095 * rb_entries) as u64))
            .await
            .unwrap();
        let mut allocator = Allocator {
            file: Arc::clone(&null),
            reftable: RefTable::from_data(data.into_boxed_slice(), &header),
            first_free_cluster: HostCluster((4093 * rb_entries) as u64),
            header: Arc::clone(&header),
            caches: Arc::clone(&caches),
            clusters_to_free: Arc::new(ClustersToFreeList::default()),
        };

        // Trigger reftable growth just one index beyond the current bound.
        allocator.grow_reftable(4096).await.unwrap();

        assert_eq!(
            (rb_b.get(0), rb_b.get(1)),
            (1, 1),
            "reftable growth changed already-owned clusters"
        );
    }
}
