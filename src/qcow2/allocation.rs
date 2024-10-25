//! Cluster allocation.
//!
//! Functionality for allocating single clusters and ranges of clusters, and general handling of
//! refcount structures.

use super::*;
use tokio::sync::MutexGuard;

/// Central facility for cluster allocation.
pub(super) struct Allocator {
    /// Qcow2 refcount table.
    reftable: RefTable,

    /// The first free cluster index in the qcow2 file, to speed up allocation.
    first_free_cluster: HostCluster,

    /// Qcow2 image header.
    header: Arc<Header>,
}

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2<S, F> {
    /// Return the central allocator instance.
    ///
    /// Returns an error for read-only images.
    async fn allocator(&self) -> io::Result<MutexGuard<'_, Allocator>> {
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
        self.allocator()
            .await?
            .allocate_clusters(&self.metadata, count)
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
    pub(super) async fn allocate_data_cluster(
        &self,
        guest_cluster: GuestCluster,
    ) -> io::Result<HostCluster> {
        if self.header.external_data_file() {
            Ok(HostCluster(
                guest_cluster.raw_index(self.header.cluster_bits()),
            ))
        } else {
            self.allocator()
                .await?
                .allocate_clusters(&self.metadata, ClusterCount(1))
                .await
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
            let cb = self.header.cluster_bits();
            let cluster = HostCluster(guest_cluster.raw_index(cb));
            Ok((cluster == mandatory_host_cluster).then_some(cluster))
        } else {
            let cluster = self
                .allocator()
                .await?
                .allocate_cluster_at(&self.metadata, mandatory_host_cluster)
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
            allocator
                .free_clusters(&self.metadata, cluster, count)
                .await
        }
    }

    /// Free data clusters (i.e. decrement their refcount).
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    pub(super) async fn free_data_clusters(&self, cluster: HostCluster, count: ClusterCount) {
        if !self.header.external_data_file() {
            self.free_meta_clusters(cluster, count).await;
        }
    }
}

impl Allocator {
    /// Create a new allocator for the given image file.
    pub async fn new<S: Storage>(image: S, header: Arc<Header>) -> io::Result<Self> {
        let cb = header.cluster_bits();
        let rt_offset = header.reftable_offset();
        let rt_cluster = rt_offset.checked_cluster(cb).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unaligned refcount table: {rt_offset}"),
            )
        })?;

        let reftable =
            RefTable::load(image, &header, rt_cluster, header.reftable_entries()).await?;

        Ok(Allocator {
            reftable,
            first_free_cluster: HostCluster(0),
            header,
        })
    }

    /// Get `cluster`’s index in its refcount block.
    fn cluster_to_rb_index(&self, cluster: HostCluster) -> usize {
        (cluster.0 % (self.header.rb_entries() as u64)) as usize
    }

    /// Get the refcount table entry index covering `cluster`.
    fn cluster_to_rt_index(&self, cluster: HostCluster) -> usize {
        (cluster.0 >> self.header.rb_bits()) as usize
    }

    /// Construct a cluster index from its reftable and refblock indices.
    fn cluster_from_ref_indices(&self, rt_index: usize, rb_index: usize) -> HostCluster {
        HostCluster(((rt_index as u64) << self.header.rb_bits()) + rb_index as u64)
    }

    /// Allocate clusters in the image file.
    async fn allocate_clusters<S: Storage>(
        &mut self,
        image: &S,
        count: ClusterCount,
    ) -> io::Result<HostCluster> {
        let mut index = self.first_free_cluster;
        loop {
            let alloc_count = self.allocate_clusters_at(image, index, count).await?;
            if alloc_count == count {
                if count == ClusterCount(1) || index == self.first_free_cluster {
                    self.first_free_cluster = index + count;
                }
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
    async fn allocate_clusters_at<S: Storage>(
        &mut self,
        image: &S,
        mut index: HostCluster,
        mut count: ClusterCount,
    ) -> io::Result<ClusterCount> {
        let start_index = index;

        while count > ClusterCount(0) {
            match self.allocate_cluster_at(image, index).await {
                // Successful allocation
                Ok(true) => (),

                // Already allocated, or some real error occurred
                result => {
                    self.free_clusters(image, start_index, index - start_index)
                        .await;
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
    async fn allocate_cluster_at<S: Storage>(
        &mut self,
        image: &S,
        index: HostCluster,
    ) -> io::Result<bool> {
        let rt_index = self.cluster_to_rt_index(index);
        let rb_index = self.cluster_to_rb_index(index);

        let mut rb = self.ensure_rb(image, rt_index).await?;
        if !rb.is_zero(rb_index) {
            return Ok(false);
        }
        let clean = !rb.is_modified();
        rb.increment(rb_index)?;
        rb.write_entry(image, rb_index).await?;
        if clean {
            rb.clear_modified();
        }
        Ok(true)
    }

    /// Load the refblock from the given cluster.
    async fn load_rb<S: Storage>(
        &self,
        image: &S,
        rb_cluster: HostCluster,
    ) -> io::Result<RefBlock> {
        RefBlock::load(image, &self.header, rb_cluster).await
    }

    /// Get the refblock referenced by the given reftable index, if any.
    ///
    /// If there is no refblock for the given reftable index, return `Ok(None)`.
    async fn get_rb<S: Storage>(
        &mut self,
        image: &S,
        rt_index: usize,
    ) -> io::Result<Option<RefBlock>> {
        // TODO: Cache refblocks
        let rt_entry = self.reftable.get(rt_index);
        if let Some(rb_offset) = rt_entry.refblock_offset() {
            let cb = self.header.cluster_bits();
            let rb_cluster = rb_offset.checked_cluster(cb).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unaligned refcount block with index {rt_index}; refcount table entry: {rt_entry:?}"),
                )
            })?;
            self.load_rb(image, rb_cluster).await.map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a refblock for the given reftable index.
    ///
    /// If there already is a refblock at that index, return it.  Otherwise, create one and hook it
    /// up.
    async fn ensure_rb<S: Storage>(&mut self, image: &S, rt_index: usize) -> io::Result<RefBlock> {
        if let Some(rb) = self.get_rb(image, rt_index).await? {
            return Ok(rb);
        }

        if !self.reftable.in_bounds(rt_index) {
            self.grow_reftable(image, rt_index).await?;
        }

        let mut rb = RefBlock::new_cleared(image, &self.header)?;

        // When allocating new refblocks, we always place them such that they describe themselves.
        // TODO: There may be more efficient ways, this is just quite an easy one.
        rb.set_cluster(self.cluster_from_ref_indices(rt_index, 0));
        rb.increment(0)?;
        rb.write(image).await?;

        self.reftable.enter_refblock(rt_index, &rb)?;
        self.reftable.write_entry(image, rt_index).await?;

        Ok(rb)
    }

    /// Create a new refcount table covering at least `at_least_index`.
    ///
    /// Create a new reftable of the required size, copy all existing refblock references into it,
    /// ensure it is refcounted itself (also creating new refblocks if necessary), and have the
    /// image header reference the new refcount table.
    async fn grow_reftable<S: Storage>(
        &mut self,
        image: &S,
        at_least_index: usize,
    ) -> io::Result<()> {
        let cb = self.header.cluster_bits();

        let mut new_rt = self.reftable.clone_and_grow(&self.header, at_least_index);
        let rt_clusters = ClusterCount::from_byte_size(self.reftable.byte_size(), cb);

        // Find free range
        let mut rt_index = self.cluster_to_rt_index(self.first_free_cluster);
        let mut rb_index = self.cluster_to_rb_index(self.first_free_cluster);
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
                let start_index = self.cluster_from_ref_indices(rt_index, 0);
                free_cluster_index.get_or_insert(start_index);
                free_cluster_count += ClusterCount(self.header.rb_entries());
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

            let rb = self.load_rb(image, rb_cluster).await?;
            for i in rb_index..self.header.rb_entries() {
                if rb.is_zero(i) {
                    let index = self.cluster_from_ref_indices(rt_index, i);
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
        let rt_index_start = self.cluster_to_rt_index(index);
        let rt_index_end = (index + count).0.div_ceil(self.header.rb_entries() as u64) as usize;

        let mut refblocks = Vec::<RefBlock>::new();
        for rt_i in rt_index_start..rt_index_end {
            if let Some(rb_offset) = new_rt.get(rt_i).refblock_offset() {
                // Checked in the loop above
                let rb_cluster = rb_offset.checked_cluster(cb).unwrap();
                let rb = self.load_rb(image, rb_cluster).await?;
                refblocks.push(rb);
                continue;
            }

            let mut rb = RefBlock::new_cleared(image, &self.header)?;
            rb.set_cluster(index);
            new_rt.enter_refblock(rt_i, &rb)?;
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

            // `refblocks[0]` is for `rt_index_start`
            let rb_vec_i = self.cluster_to_rt_index(index) - rt_index_start;
            // Incrementing from 0 to 1 must succeed
            refblocks[rb_vec_i]
                .increment(self.cluster_to_rb_index(index))
                .unwrap();
        }

        // Any errors from here on may lead to leaked clusters if there are refblocks in
        // `refblocks` that are already part of the old reftable.
        // TODO: Try to clean that up, though it seems quite hard for little gain.
        for rb in refblocks {
            rb.write(image).await?;
        }
        new_rt.write(image).await?;

        self.header.set_reftable(&new_rt)?;
        self.header.write_reftable_pointer(image).await?;

        self.reftable = new_rt;
        Ok(())
    }

    /// Free clusters (i.e. decrement their refcount).
    ///
    /// Best-effort operation.  On error, the given clusters may be leaked, but no errors are ever
    /// returned (because there is no good way to handle such errors anyway).
    async fn free_clusters<S: Storage>(
        &mut self,
        image: &S,
        start: HostCluster,
        mut count: ClusterCount,
    ) {
        if start < self.first_free_cluster {
            self.first_free_cluster = start;
        }

        let rb_entries = self.header.rb_entries();
        let mut rb_index = self.cluster_to_rb_index(start);
        let mut rt_index = self.cluster_to_rt_index(start);

        while count > ClusterCount(0) {
            let in_rb_count = cmp::min(rb_entries - rb_index, count.0);

            match self.get_rb(image, rt_index).await {
                Ok(Some(mut rb)) => {
                    for i in rb_index..(rb_index + in_rb_count) {
                        if let Err(err) = rb.decrement(i) {
                            eprintln!("Warning: Failed to free cluster: {err}");
                        }
                    }
                    if let Err(err) = rb.write(image).await {
                        eprintln!("Warning: Failed to commit {in_rb_count} freed clusters: {err}");
                    }
                }

                Ok(None) => {
                    eprintln!("Warning: Failed to free {in_rb_count} clusters: Not allocated")
                }
                Err(err) => eprintln!("Warning: Failed to free {in_rb_count} clusters: {err}"),
            }

            count -= ClusterCount(in_rb_count);
            rb_index = 0;
            rt_index += 1;
        }
    }
}
