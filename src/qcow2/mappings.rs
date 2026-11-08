//! Get and establish cluster mappings.

use super::*;
use crate::sync_primitives::RwLockWriteGuard;

#[maybe_async]
impl<S: Storage, F: WrappedFormat<S>> Qcow2<S, F> {
    /// Get the given range’s mapping information.
    ///
    /// Underlying implementation for [`Qcow2::get_mapping()`].
    pub(super) async fn do_get_mapping(
        &self,
        offset: GuestOffset,
        max_length: u64,
    ) -> io::Result<(ShallowMapping<'_, S>, u64)> {
        let Some(l2_table) = self.get_l2(offset, false).await? else {
            let cb = self.header.cluster_bits();
            let len = cmp::min(offset.remaining_in_l2_table(cb), max_length);
            let mapping = if let Some(backing) = self.backing.as_ref() {
                ShallowMapping::Indirect {
                    layer: backing.inner(),
                    offset: offset.0,
                    writable: false,
                }
            } else {
                ShallowMapping::Zero { explicit: false }
            };
            return Ok((mapping, len));
        };

        self.do_get_mapping_with_l2(offset, max_length, &l2_table)
            .await
    }

    /// Get the given range’s mapping information, when we already have the L2 table.
    pub(super) async fn do_get_mapping_with_l2(
        &self,
        offset: GuestOffset,
        max_length: u64,
        l2_table: &L2Table,
    ) -> io::Result<(ShallowMapping<'_, S>, u64)> {
        let cb = self.header.cluster_bits();

        // Get mapping at `offset`
        let mut current_guest_cluster = offset.cluster(cb);
        let first_mapping = l2_table.get_mapping(current_guest_cluster)?;
        let return_mapping = match first_mapping {
            L2Mapping::DataFile {
                host_cluster,
                copied,
            } => ShallowMapping::Raw {
                storage: self.storage(),
                offset: host_cluster.relative_offset(offset, cb).0,
                read_exact: true,
                writable: copied,
            },

            L2Mapping::Backing { backing_offset } => {
                if let Some(backing) = self.backing.as_ref() {
                    ShallowMapping::Indirect {
                        layer: backing.inner(),
                        offset: backing_offset + offset.in_cluster_offset(cb) as u64,
                        writable: false,
                    }
                } else {
                    ShallowMapping::Zero { explicit: false }
                }
            }

            L2Mapping::Zero {
                host_cluster: _,
                copied: _,
            } => ShallowMapping::Zero { explicit: true },

            L2Mapping::Compressed {
                host_offset: _,
                length: _,
            } => ShallowMapping::Special { offset: offset.0 },
        };

        // Find out how long this consecutive mapping is, but only within the current L2 table
        let mut consecutive_length = offset.remaining_in_cluster(cb);
        let mut preceding_mapping = first_mapping;
        while consecutive_length < max_length {
            let Some(next) = current_guest_cluster.next_in_l2(cb) else {
                break;
            };
            current_guest_cluster = next;

            let mapping = l2_table.get_mapping(current_guest_cluster)?;
            if !mapping.is_consecutive(&preceding_mapping, cb) {
                break;
            }

            preceding_mapping = mapping;
            consecutive_length += self.header.cluster_size() as u64;
        }

        consecutive_length = cmp::min(consecutive_length, max_length);
        Ok((return_mapping, consecutive_length))
    }

    /// Prepare mapping the given range by data clusters.
    ///
    /// Underlying implementation for [`Qcow2::prepare_data_mapping()`].
    ///
    /// Return the host offset to write the data to, the length that could be mapped, and, unless
    /// the whole returned range is mapped to that host offset already, the state to pass to
    /// [`Qcow2::do_commit_data_mapping()`] to make the mapping visible.  That state keeps the L2
    /// table locked for modifications; dropping it frees the clusters allocated for the mapping.
    ///
    /// `skip_cow` is equivalent to [`Qcow2::ensure_data_mapping()`]’s `overwrite`: It indicates
    /// the area is to be overwritten, so COW can be skipped on it.  `skip_cow_to_eof` indicates
    /// that the mapping will go until the EOF, so no COW needs to be performed at all past
    /// `offset`.  Only use this for preallocation on resize or create.
    pub(super) async fn do_prepare_data_mapping(
        &self,
        offset: GuestOffset,
        length: u64,
        skip_cow: bool,
        skip_cow_to_eof: bool,
    ) -> io::Result<(u64, u64, Option<PreparedMappingState>)> {
        let l2_table = self.ensure_l2(offset).await?;

        // Fast path for if everything is already allocated, which should be the common case at
        // runtime.
        // It must really be everything, though; we know our caller will want to have everything
        // allocated eventually, so if anything is missing, go down to the allocation path so we
        // try to allocate clusters such that they are not fragmented (if possible) and we can
        // return as big of a single mapping as possible.
        let existing = self
            .do_get_mapping_with_l2(offset, length, &l2_table)
            .await?;
        if let ShallowMapping::Raw {
            offset,
            writable: true,
            ..
        } = existing.0
        {
            if existing.1 >= length {
                return Ok((offset, existing.1, None));
            }
        }

        let l2_table = l2_table.lock_write().await;

        let mut l2_changes = Vec::new();
        let res = self
            .prepare_data_mapping_no_cleanup(
                offset,
                length,
                skip_cow,
                skip_cow_to_eof,
                &l2_table,
                &mut l2_changes,
            )
            .await;

        let (host_offset, length) = match res {
            Ok(mapped) => mapped,
            Err(err) => {
                // Release what has been allocated so far
                drop(l2_changes);
                drop(l2_table);
                self.process_clusters_to_free().await;
                return Err(err);
            }
        };

        // Everything may turn out to be mapped already, e.g. when a concurrent request has mapped
        // it while we were waiting for the lock
        let prepared_mapping = (!l2_changes.is_empty()).then(|| PreparedMappingState {
            l2_table,
            l2_changes,
            clusters_to_free: Arc::clone(&self.clusters_to_free),
        });

        Ok((host_offset, length, prepared_mapping))
    }

    /// Make a mapping prepared by [`Qcow2::do_prepare_data_mapping()`] visible.
    ///
    /// Underlying implementation for [`Qcow2::commit_data_mapping()`].
    pub(super) async fn do_commit_data_mapping(&self, state: PreparedMappingState) {
        let PreparedMappingState {
            mut l2_table,
            l2_changes,
            clusters_to_free: _,
        } = state;

        for (l2_index, host_cluster, new_allocation) in l2_changes {
            if let Some(leaked) = l2_table.map_cluster(l2_index, host_cluster, new_allocation) {
                self.clusters_to_free.push(leaked);
            }
        }
        drop(l2_table);

        self.process_clusters_to_free().await;
    }

    /// Make the given range be mapped by a fixed kind of clusters.
    ///
    /// Allows zeroing or discarding clusters.  `mapping` says which kind of mapping to create.
    ///
    /// Return the offset of the first affected cluster, and the byte length affected (may be 0).
    pub(super) async fn ensure_fixed_mapping(
        &self,
        offset: GuestOffset,
        length: u64,
        mapping: FixedMapping,
    ) -> io::Result<(GuestOffset, u64)> {
        match mapping {
            FixedMapping::ZeroDiscard | FixedMapping::ZeroRetainAllocation => {
                self.header.require_version(3)?;
            }
            FixedMapping::FullDiscard => (),
        }

        let cb = self.header.cluster_bits();

        // We can only touch full clusters
        let cluster_align_mask = self.header.cluster_size() as u64 - 1;
        let end = (offset + length).0;
        let aligned_end = if end == self.header.size() {
            // Up-align operations until the image end to a full cluster (the remainder of this
            // cluster is not used for anything)
            (end + cluster_align_mask) & !cluster_align_mask
        } else {
            // Otherwise, align down (only full clusters)
            end & !cluster_align_mask
        };
        let aligned_offset = (offset + cluster_align_mask).0 & !cluster_align_mask;
        let aligned_length = aligned_end.saturating_sub(aligned_offset);

        // We have aligned this, so we can unwrap
        let first_cluster = GuestOffset(aligned_offset).checked_cluster(cb).unwrap();
        let cluster_count = ClusterCount::checked_from_byte_size(aligned_length, cb).unwrap();

        if cluster_count.0 == 0 {
            return Ok((GuestOffset(aligned_offset), 0));
        }

        let l2_table = self.ensure_l2(first_cluster.offset(cb)).await?;
        let l2_table = l2_table.lock_write().await;

        let res = self
            .ensure_fixed_mapping_no_cleanup(first_cluster, cluster_count, mapping, l2_table)
            .await;

        self.process_clusters_to_free().await;

        let count = res?;

        let affected_offset = first_cluster.offset(cb);
        let affected_length = count.byte_size(cb);

        let head = affected_offset - offset;
        // We may overshoot for the last cluster in the image, limit the returned value to the
        // range given by the caller
        let affected_length = cmp::min(affected_length, length.saturating_sub(head));

        Ok((affected_offset, affected_length))
    }

    /// Get the L2 table referenced by the given L1 table index, if any.
    ///
    /// `writable` says whether the L2 table should be modifiable.
    ///
    /// If the L1 table index does not point to any L2 table, or the existing entry is not
    /// modifiable but `writable` is true, return `Ok(None)`.
    pub(super) async fn get_l2(
        &self,
        offset: GuestOffset,
        writable: bool,
    ) -> io::Result<Option<Arc<L2Table>>> {
        let cb = self.header.cluster_bits();

        let l1_entry = self.l1_table.read().await.get(offset.l1_index(cb));
        if let Some(l2_offset) = l1_entry.l2_offset() {
            if writable && !l1_entry.is_copied() {
                return Ok(None);
            }
            let l2_cluster = l2_offset.checked_cluster(cb).ok_or_else(|| {
                invalid_data(format!(
                    "Unaligned L2 table for {offset:?}; L1 entry: {l1_entry:?}"
                ))
            })?;

            self.caches.l2_get_or_insert(l2_cluster).await.map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a L2 table for the given L1 table index.
    ///
    /// If there already is an L2 table at that index, return it.  Otherwise, create one and hook
    /// it up.
    pub(super) async fn ensure_l2(&self, offset: GuestOffset) -> io::Result<Arc<L2Table>> {
        let cb = self.header.cluster_bits();

        if let Some(l2) = self.get_l2(offset, true).await? {
            return Ok(l2);
        }

        self.prepare_write().await?;

        let mut l1_locked = self.l1_table.write().await;
        let l1_index = offset.l1_index(cb);
        if !l1_locked.in_bounds(l1_index) {
            l1_locked = self.grow_l1_table(l1_locked, l1_index).await?;
        }

        let l1_entry = l1_locked.get(l1_index);
        let mut l2_table = if let Some(l2_offset) = l1_entry.l2_offset() {
            let l2_cluster = l2_offset.checked_cluster(cb).ok_or_else(|| {
                invalid_data(format!(
                    "Unaligned L2 table for {offset:?}; L1 entry: {l1_entry:?}"
                ))
            })?;

            let l2 = self.caches.l2_get_or_insert(l2_cluster).await?;
            if l1_entry.is_copied() {
                return Ok(l2);
            }

            L2Table::clone(&l2)
        } else {
            L2Table::new_cleared(&self.header)
        };

        let l2_allocation = self.allocate_meta_cluster().await?;
        l2_table.set_cluster(l2_allocation.cluster());
        l2_table.write(self.metadata.as_ref()).await?;

        l1_locked.enter_l2_table(l1_index, &l2_table, Some(l2_allocation))?;
        l1_locked
            .write_entry(self.metadata.as_ref(), l1_index)
            .await?;

        // Free old L2 table, if any
        if let Some(l2_offset) = l1_entry.l2_offset() {
            self.clusters_to_free.push(ClustersToFreeEntry::metadata(
                l2_offset.cluster(cb),
                ClusterCount(1),
            ));
            self.process_clusters_to_free().await;
        }

        let l2_table = Arc::new(l2_table);
        self.caches.l2_insert(Arc::clone(&l2_table)).await?;
        Ok(l2_table)
    }

    /// Create a new L1 table covering at least `at_least_index`.
    ///
    /// Create a new L1 table of the required size with all the entries of the previous L1 table.
    pub(super) async fn grow_l1_table<'a>(
        &self,
        mut l1_locked: RwLockWriteGuard<'a, L1Table>,
        at_least_index: usize,
    ) -> io::Result<RwLockWriteGuard<'a, L1Table>> {
        let mut new_l1 = l1_locked.clone_and_grow(at_least_index, &self.header)?;

        let l1_allocation = self.allocate_meta_clusters(new_l1.cluster_count()).await?;

        new_l1.set_cluster(l1_allocation.cluster());
        new_l1.write(self.metadata.as_ref()).await?;

        self.header.set_l1_table(&new_l1, Some(l1_allocation))?;
        self.header
            .write_l1_table_pointer(self.metadata.as_ref())
            .await?;

        if let Some(old_l1_cluster) = l1_locked.get_cluster() {
            let old_l1_size = l1_locked.cluster_count();
            l1_locked.unset_cluster();
            self.clusters_to_free
                .push(ClustersToFreeEntry::metadata(old_l1_cluster, old_l1_size));
            self.process_clusters_to_free().await;
        }

        *l1_locked = new_l1;

        Ok(l1_locked)
    }

    /// Inner implementation for [`Qcow2::do_prepare_data_mapping()`].
    ///
    /// Does not modify the L2 table: Whatever must be entered into it to make the mapping visible
    /// is pushed to `l2_changes` (L2 index, host cluster, and the guard for the host cluster if it
    /// has been newly allocated).
    ///
    /// Does not do any clean-up: In case of error, the caller must drop `l2_changes`, which enters
    /// the clusters allocated so far into `self.clusters_to_free`, and free those.
    async fn prepare_data_mapping_no_cleanup(
        &self,
        offset: GuestOffset,
        full_length: u64,
        skip_cow: bool,
        skip_cow_to_eof: bool,
        l2_table: &L2TableWriteGuard,
        l2_changes: &mut Vec<(usize, HostCluster, Option<PendingClusters>)>,
    ) -> io::Result<(u64, u64)> {
        let cb = self.header.cluster_bits();

        let partial_skip_cow = skip_cow.then(|| {
            let start = offset.in_cluster_offset(cb);
            let end = if skip_cow_to_eof {
                1 << cb
            } else {
                cmp::min(start as u64 + full_length, 1 << cb) as usize
            };
            start..end
        });

        let mut current_guest_cluster = offset.cluster(cb);

        // Without a mandatory host offset, this should never return `Ok(None)`
        let prepared = self
            .prepare_cow_cluster(current_guest_cluster, None, partial_skip_cow, l2_table)
            .await?
            .ok_or_else(|| io::Error::other("Internal allocation error"))?;
        let host_cluster = prepared.host_cluster;
        if let Some((l2_index, new_allocation)) = prepared.set_mapping {
            l2_changes.push((l2_index, host_cluster, new_allocation));
        }

        let host_offset_start = host_cluster.relative_offset(offset, cb);
        let mut allocated_length = offset.remaining_in_cluster(cb);
        let mut current_host_cluster = host_cluster;

        while allocated_length < full_length {
            let Some(next) = current_guest_cluster.next_in_l2(cb) else {
                break;
            };
            current_guest_cluster = next;

            let chunk_length = cmp::min(full_length - allocated_length, 1 << cb) as usize;
            let partial_skip_cow = match (skip_cow, skip_cow_to_eof) {
                (false, _) => None,
                (true, false) => Some(0..chunk_length),
                (true, true) => Some(0..(1 << cb)),
            };

            let next_host_cluster = current_host_cluster + ClusterCount(1);
            let prepared = self
                .prepare_cow_cluster(
                    current_guest_cluster,
                    Some(next_host_cluster),
                    partial_skip_cow,
                    l2_table,
                )
                .await?;

            let Some(prepared) = prepared else {
                // Cannot continue continuous mapping range
                break;
            };
            let host_cluster = prepared.host_cluster;
            assert!(host_cluster == next_host_cluster);
            if let Some((l2_index, new_allocation)) = prepared.set_mapping {
                l2_changes.push((l2_index, host_cluster, new_allocation));
            }
            current_host_cluster = host_cluster;

            allocated_length += chunk_length as u64;
        }

        Ok((host_offset_start.0, allocated_length))
    }

    /// Inner implementation for [`Qcow2::ensure_fixed_mapping()`].
    ///
    /// Does not do any clean-up: The L2 table will probably be modified, but not written to disk.
    /// Any existing allocations that have been removed from it (and are thus leaked) are entered
    /// into `self.clusters_to_free`, but not freed.
    ///
    /// The caller must do both, ensuring it is done both in case of success and in case of error.
    ///
    /// Allows zeroing or discarding clusters.  `mapping` says which kind of mapping to create.
    async fn ensure_fixed_mapping_no_cleanup(
        &self,
        first_cluster: GuestCluster,
        count: ClusterCount,
        mapping: FixedMapping,
        mut l2_table: L2TableWriteGuard,
    ) -> io::Result<ClusterCount> {
        let cb = self.header.cluster_bits();
        let mut cluster = first_cluster;
        let end_cluster = first_cluster + count;
        let mut done = ClusterCount(0);

        while cluster < end_cluster {
            let l2i = cluster.l2_index(cb);
            let leaked = match mapping {
                FixedMapping::ZeroDiscard => l2_table.zero_cluster(l2i, false)?,
                FixedMapping::ZeroRetainAllocation => l2_table.zero_cluster(l2i, true)?,
                FixedMapping::FullDiscard => l2_table.discard_cluster(l2i),
            };
            if let Some(leaked) = leaked {
                self.clusters_to_free.push(leaked);
            }

            done += ClusterCount(1);
            let Some(next) = cluster.next_in_l2(cb) else {
                break;
            };
            cluster = next;
        }

        Ok(done)
    }
}

/// State of a data mapping that has been prepared, but not committed yet.
///
/// Created by [`Qcow2::do_prepare_data_mapping()`], to be passed to
/// [`Qcow2::do_commit_data_mapping()`].
///
/// Dropping this discards the prepared mapping, i.e. enters the clusters that have been allocated
/// for it into the list of clusters to free.
pub(super) struct PreparedMappingState {
    /// Write guard on the L2 table that is to be modified.
    ///
    /// Held so that nobody else modifies the affected L2 entries before the mapping is committed
    /// or dropped; in particular, nobody else may prepare a mapping for the same guest clusters.
    ///
    /// This also occupies an L2 cache entry for as long as it exists, because the cache only
    /// evicts tables that nothing else refers to.  Prepared mappings are held across data I/O, so
    /// holding several of them can pin the whole L2 cache: If no entry can be evicted, loading
    /// another L2 table fails with an I/O error (“Cannot evict entry from cache; everything is in
    /// use”) instead of waiting for an entry to become available.
    l2_table: L2TableWriteGuard,

    /// What to enter into the L2 table: L2 index, host cluster, and the guard for the host cluster
    /// if it has been newly allocated.
    l2_changes: Vec<(usize, HostCluster, Option<PendingClusters>)>,

    /// The image’s list of clusters to free, to identify which image this state belongs to.
    clusters_to_free: Arc<ClustersToFreeList>,
}

impl PreparedMappingState {
    /// Check whether this state belongs to the given qcow2 image instance
    pub fn belongs_to<S: Storage + 'static, F: WrappedFormat<S> + 'static>(
        &self,
        qcow2: &Qcow2<S, F>,
    ) -> bool {
        Arc::ptr_eq(&self.clusters_to_free, &qcow2.clusters_to_free)
    }
}

/// Possible mapping types for [`Qcow2::ensure_fixed_mapping()`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FixedMapping {
    /// Make all clusters zero clusters, discarding previous allocations.
    ///
    /// Note this breaks existing mapping information, which must be communicated somehow, for
    /// example by requiring mutable access to the `Qcow2` object.
    ZeroDiscard,

    /// Make all clusters zero clusters, retaining previous allocations.
    ///
    /// Retains previous data cluster allocations in the form of preallocated zero clusters, but
    /// cannot retain previously existing compressed cluster allocations.  Because those mappings
    /// are not returned through the mapping interface, however, concurrent accesses should be
    /// reasonably safe.
    ///
    /// (Writing to zeroed data cluster mappings will just have no effect.)
    ZeroRetainAllocation,

    /// Fully remove clusters’ mappings, allowing backing data to appear.
    ///
    /// Note this breaks existing mapping information, which must be communicated somehow, for
    /// example by requiring mutable access to the `Qcow2` object.
    FullDiscard,
}

#[cfg(test)]
mod tests {
    //! Tests for establishing mappings.

    use super::*;
    use crate::format::drivers::FormatDriverInstance;
    use crate::test::mem_storage::{MemStorage, MemStorageState};
    use crate::{DenyImplicitOpenGate, Mapping};

    /// Create a 512-byte-cluster image of the given size and refcount width on a [`MemStorage`]
    /// with the given state.
    #[maybe_async]
    async fn create_image(
        state: &Arc<MemStorageState>,
        size: u64,
        refcount_width: usize,
    ) -> Qcow2<MemStorage> {
        Qcow2::<MemStorage>::create_builder(MemStorage::new(Arc::clone(state), false))
            .cluster_size(512)
            .refcount_width(refcount_width)
            .size(size)
            .create_open(DenyImplicitOpenGate::default(), |image| {
                Ok(Qcow2::<MemStorage>::builder(image)
                    .backing(None)
                    .write(true))
            })
            .await
            .unwrap()
    }

    /// Load the reftable from storage.
    #[cfg(feature = "async")]
    #[maybe_async]
    async fn load_reftable(qcow2: &Qcow2<MemStorage>) -> RefTable {
        let cb = qcow2.header.cluster_bits();
        let rt_cluster = qcow2.header.reftable_offset().checked_cluster(cb).unwrap();
        RefTable::load(
            qcow2.metadata.as_ref(),
            &qcow2.header,
            rt_cluster,
            qcow2.header.reftable_entries(),
        )
        .await
        .unwrap()
    }

    /// Two concurrent flushes writing back the same L2 table, with the table modified in between:
    /// The modification must end up on disk, i.e. the older copy must not be written last.
    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_concurrent_l2_writeback() {
        use futures::task::noop_waker;
        use std::future::Future;
        use std::sync::atomic::Ordering;
        use std::task::{Context, Poll};

        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024, 16).await;
        let cb = qcow2.header.cluster_bits();

        // Map guest clusters 0 and 1 (all in the same L2 table), and write everything
        qcow2.ensure_data_mapping(0, 1024, true).await.unwrap();
        qcow2.flush().await.unwrap();

        // Modify the L2 table, so the next flush will write it
        qcow2.ensure_data_mapping(1024, 512, true).await.unwrap();

        let l2_cluster = {
            let l1 = qcow2.l1_table.read().await;
            l1.get(GuestOffset(0).l1_index(cb))
                .l2_offset()
                .unwrap()
                .checked_cluster(cb)
                .unwrap()
        };
        let l2_offset = l2_cluster.offset(cb).0;

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Flush A: Keep it pending on writing the L2 table, after it has copied its entries
        *state.hold_range.lock() = Some(l2_offset..l2_offset + 512);
        state.hold_writes.store(true, Ordering::Relaxed);
        let mut flush_a = Box::pin(qcow2.flush());
        assert!(flush_a.as_mut().poll(&mut cx).is_pending());
        assert_eq!(state.held.lock().len(), 1);
        state.hold_writes.store(false, Ordering::Relaxed);

        // Modify the L2 table again: Make guest cluster 1 a zero cluster
        qcow2.ensure_zero_mapping(512, 512).await.unwrap();

        // Flush B: Must write the L2 table after flush A
        let mut flush_b = Box::pin(qcow2.flush());
        let flush_b_result = match flush_b.as_mut().poll(&mut cx) {
            Poll::Ready(result) => Some(result),
            Poll::Pending => None,
        };

        state.release_held_writes();
        let Poll::Ready(result) = flush_a.as_mut().poll(&mut cx) else {
            panic!("Flush A did not complete");
        };
        result.unwrap();
        match flush_b_result {
            Some(result) => result.unwrap(),
            None => flush_b.await.unwrap(),
        }

        // Guest cluster 1’s L2 entry must have the zero flag (bit 0) on disk
        let entry_offset = (l2_offset + 8) as usize;
        let data = state.data.lock();
        let entry = u64::from_be_bytes(data[entry_offset..entry_offset + 8].try_into().unwrap());
        assert!(
            entry & 1 != 0,
            "zeroing guest cluster 1 lost on disk (L2 entry {entry:#x})"
        );
    }

    /// Two concurrent flushes writing back the same refblock, with a refcount changed in between:
    /// The change must end up on disk, i.e. the older copy must not be written last.
    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_concurrent_refblock_writeback() {
        use futures::task::noop_waker;
        use std::future::Future;
        use std::sync::atomic::Ordering;
        use std::task::{Context, Poll};

        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024, 16).await;
        let rb_bits = qcow2.header.rb_bits();
        let refcount_bytes = 2; // 16-bit refcounts
        qcow2.flush().await.unwrap();

        // Modify the refblock, so the next flush will write it
        let first_pending = qcow2.allocate_meta_cluster().await.unwrap();
        let first = first_pending.cluster();
        first_pending.make_persistent();
        assert_eq!(first.rt_index(rb_bits), 0);
        let rb_offset = load_reftable(&qcow2)
            .await
            .get(0)
            .refblock_offset()
            .unwrap()
            .0;

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Flush A: Keep it pending on writing the refblock, after it has copied its data
        *state.hold_range.lock() = Some(rb_offset..rb_offset + 512);
        state.hold_writes.store(true, Ordering::Relaxed);
        let mut flush_a = Box::pin(qcow2.flush());
        assert!(flush_a.as_mut().poll(&mut cx).is_pending());
        assert_eq!(state.held.lock().len(), 1);
        state.hold_writes.store(false, Ordering::Relaxed);

        // Modify the refblock again
        let second_pending = qcow2.allocate_meta_cluster().await.unwrap();
        let second = second_pending.cluster();
        second_pending.make_persistent();
        assert_eq!(second.rt_index(rb_bits), 0);

        // Flush B: Must write the refblock after flush A
        let mut flush_b = Box::pin(qcow2.flush());
        let flush_b_result = match flush_b.as_mut().poll(&mut cx) {
            Poll::Ready(result) => Some(result),
            Poll::Pending => None,
        };

        state.release_held_writes();
        let Poll::Ready(result) = flush_a.as_mut().poll(&mut cx) else {
            panic!("Flush A did not complete");
        };
        result.unwrap();
        match flush_b_result {
            Some(result) => result.unwrap(),
            None => flush_b.await.unwrap(),
        }

        // The second cluster’s refcount must be 1 on disk
        let refcount_offset =
            (rb_offset + (second.rb_index(rb_bits) * refcount_bytes) as u64) as usize;
        let data = state.data.lock();
        let refcount = u16::from_be_bytes(
            data[refcount_offset..refcount_offset + refcount_bytes]
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            refcount, 1,
            "refcount increment for {second:?} lost on disk"
        );
    }

    /// Fully discarding (to the backing file) must work on qcow2 v2 images, too, because it only
    /// requires setting L2 entries to 0.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_full_discard_v2() {
        // Create a v3 image, then turn it into a v2 image: A v2 header is the same, except for the
        // version, and all v3 fields having their default values (which imago creates by default,
        // e.g. 16-bit refcounts)
        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024, 16).await;
        qcow2.flush().await.unwrap();
        drop(qcow2);
        state.data.lock()[4..8].copy_from_slice(&2u32.to_be_bytes());

        let qcow2 = Qcow2::<MemStorage>::builder(MemStorage::new(state, false))
            .backing(None)
            .write(true)
            .open(DenyImplicitOpenGate::default())
            .await
            .unwrap();
        qcow2.header.require_version(2).unwrap();
        qcow2.header.require_version(3).unwrap_err();
        let mut image = FormatAccess::new(qcow2);

        image.write(&[0x11u8; 512][..], 0).await.unwrap();
        let (
            Mapping::Raw {
                offset: discarded_offset,
                ..
            },
            _,
        ) = image.get_mapping(0, 512).await.unwrap()
        else {
            panic!("Expected a data mapping");
        };

        image.discard_to_backing(0, 512).await.unwrap();
        let (mapping, _) = image.get_mapping(0, 512).await.unwrap();
        assert!(
            !matches!(mapping, Mapping::Raw { .. }),
            "Discarded cluster still mapped: {mapping:?}"
        );

        // The discarded cluster has been freed, so the next allocation can reuse it
        image.write(&[0x22u8; 512][..], 512).await.unwrap();
        let (Mapping::Raw { offset, .. }, _) = image.get_mapping(512, 512).await.unwrap() else {
            panic!("Expected a data mapping");
        };
        assert_eq!(offset, discarded_offset);
    }

    /// Establishing mappings beyond the disk size must fail, also when starting at offset 0.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_mapping_beyond_disk_size() {
        let size = 64 * 1024;
        let qcow2 = create_image(&Arc::default(), size, 16).await;
        let length = size + 512;

        let check = |result: io::Result<_>, req| match result {
            Ok(_) => panic!("{req} of {length} bytes at 0 beyond the disk size ({size}) succeeded"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{req}: {err}"),
        };

        check(
            qcow2
                .ensure_data_mapping(0, length, false)
                .await
                .map(|_| ()),
            "ensure_data_mapping",
        );
        check(
            qcow2.ensure_zero_mapping(0, length).await.map(|_| ()),
            "ensure_zero_mapping",
        );
        // Safe: Nothing is accessing the image concurrently
        check(
            unsafe { qcow2.discard_to_zero_unsafe(0, length) }
                .await
                .map(|_| ()),
            "discard_to_zero_unsafe",
        );
        // Safe: Nothing is accessing the image concurrently
        check(
            unsafe { qcow2.discard_to_backing_unsafe(0, length) }
                .await
                .map(|_| ()),
            "discard_to_backing_unsafe",
        );
    }

    /// A prepared data mapping must become visible only once it is committed; dropping it instead
    /// must leave the range as it was, and free the cluster allocated for it.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_prepare_commit_data_mapping() {
        let state = Arc::new(MemStorageState::default());
        let image = FormatAccess::new(create_image(&state, 64 * 1024, 16).await);
        let mut buf = [0xffu8; 512];

        // Allocate guest cluster 0 (so the L2 table exists)
        image.write(&[0x11u8; 512][..], 0).await.unwrap();

        // Prepare a mapping for guest cluster 1 and write data to it, but then drop it
        let mapping = image.prepare_data_mapping(512, 512, true).await.unwrap();
        assert!(!mapping.is_nonblocking());
        assert_eq!(mapping.length(), 512);
        let host_offset = mapping.offset();
        mapping
            .storage()
            .write(&[0x22u8; 512][..], host_offset)
            .await
            .unwrap();

        image.read(&mut buf[..], 512).await.unwrap();
        assert!(buf == [0u8; 512], "Uncommitted mapping is visible");

        drop(mapping);
        image.read(&mut buf[..], 512).await.unwrap();
        assert!(buf == [0u8; 512], "Dropped mapping is visible");

        // Flushing frees the dropped mapping’s cluster, so preparing a mapping again reuses it;
        // commit it this time
        image.flush().await.unwrap();
        let mapping = image.prepare_data_mapping(512, 512, true).await.unwrap();
        assert!(!mapping.is_nonblocking());
        assert_eq!(mapping.offset(), host_offset);
        mapping
            .storage()
            .write(&[0x33u8; 512][..], host_offset)
            .await
            .unwrap();
        image.commit_data_mapping(mapping).await.unwrap();

        image.read(&mut buf[..], 512).await.unwrap();
        assert!(buf == [0x33u8; 512], "Committed mapping is not visible");

        // Now that the cluster is mapped, there is nothing to commit
        let mapping = image.prepare_data_mapping(512, 512, true).await.unwrap();
        assert!(mapping.is_nonblocking());
        assert_eq!(mapping.offset(), host_offset);
        image.commit_data_mapping(mapping).await.unwrap();
    }

    /// A prepared data mapping must be rejected by an image it does not belong to (and discarded).
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_commit_data_mapping_on_other_image() {
        let state = Arc::new(MemStorageState::default());
        let image = FormatAccess::new(create_image(&state, 64 * 1024, 16).await);
        let other_state = Arc::new(MemStorageState::default());
        let other_image = FormatAccess::new(create_image(&other_state, 64 * 1024, 16).await);

        let mapping = image.prepare_data_mapping(0, 512, true).await.unwrap();
        assert!(!mapping.is_nonblocking());
        let err = other_image.commit_data_mapping(mapping).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // Nothing has been mapped in either image
        for image in [&image, &other_image] {
            let (mapping, _) = image.get_mapping(0, 512).await.unwrap();
            assert!(
                matches!(mapping, Mapping::Zero { explicit: false }),
                "Cluster has been mapped"
            );
        }
    }

    /// Read a big-endian 64-bit value from the storage.
    #[cfg(feature = "async")]
    fn read_u64(state: &MemStorageState, offset: u64) -> u64 {
        let data = state.data.lock();
        let offset = offset as usize;
        u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap())
    }

    /// If a flush is dropped before it has written back a modified L2 table, the table must still
    /// be considered modified, so that a later flush writes it.
    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_dropped_l2_writeback_keeps_table_modified() {
        use crate::StorageExt;
        use futures::FutureExt;

        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024, 16).await;
        let cb = qcow2.header.cluster_bits();

        // Map guest cluster 0 (so the L2 table exists), and write everything
        let mapping = qcow2.prepare_data_mapping(0, 512, true).await.unwrap();
        qcow2.commit_data_mapping(mapping).await.unwrap();
        qcow2.flush().await.unwrap();

        // Modify the L2 table: Map guest cluster 1
        let mapping = qcow2.prepare_data_mapping(512, 512, true).await.unwrap();
        let host_offset = mapping.offset();
        qcow2.commit_data_mapping(mapping).await.unwrap();

        let l2_offset = {
            let l1 = qcow2.l1_table.read().await;
            l1.get(GuestOffset(0).l1_index(cb)).l2_offset().unwrap().0
        };

        // Flush, but have writing the L2 table wait for a concurrent request blocking its range,
        // and drop the flush while it waits
        let blocker = qcow2
            .metadata
            .strong_write_blocker(l2_offset..(l2_offset + 512))
            .await;
        assert!(qcow2.flush().now_or_never().is_none());
        drop(blocker);

        // A later flush must write the L2 table
        qcow2.flush().await.unwrap();
        let entry = read_u64(&state, l2_offset + 8);
        assert_eq!(
            entry & 0x00ff_ffff_ffff_fe00,
            host_offset,
            "mapping guest cluster 1 lost on disk (L2 entry {entry:#x})"
        );
    }

    /// If a flush is dropped before it has written back a modified refblock, the refblock must
    /// still be considered modified, so that a later flush writes it.
    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_dropped_refblock_writeback_keeps_block_modified() {
        use crate::StorageExt;
        use futures::FutureExt;

        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 64 * 1024, 16).await;
        let cb = qcow2.header.cluster_bits();
        let rb_bits = qcow2.header.rb_bits();
        let refcount_bytes = 2; // 16-bit refcounts

        // Map guest cluster 0 (so the L2 table exists), and write everything
        let mapping = qcow2.prepare_data_mapping(0, 512, true).await.unwrap();
        qcow2.commit_data_mapping(mapping).await.unwrap();
        qcow2.flush().await.unwrap();

        // Modify the refblock: Allocate a data cluster for guest cluster 1
        let mapping = qcow2.prepare_data_mapping(512, 512, true).await.unwrap();
        let host_offset = mapping.offset();
        qcow2.commit_data_mapping(mapping).await.unwrap();
        let host_cluster = HostOffset(host_offset).cluster(cb);
        assert_eq!(host_cluster.rt_index(rb_bits), 0);

        let rb_offset = read_u64(&state, qcow2.header.reftable_offset().0) & !511;

        // Flush, but have writing the refblock wait for a concurrent request blocking its range,
        // and drop the flush while it waits
        let blocker = qcow2
            .metadata
            .strong_write_blocker(rb_offset..(rb_offset + 512))
            .await;
        assert!(qcow2.flush().now_or_never().is_none());
        drop(blocker);

        // A later flush must write the refblock
        qcow2.flush().await.unwrap();
        let refcount_offset =
            (rb_offset + (host_cluster.rb_index(rb_bits) * refcount_bytes) as u64) as usize;
        let data = state.data.lock();
        let refcount = u16::from_be_bytes(
            data[refcount_offset..refcount_offset + refcount_bytes]
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            refcount, 1,
            "refcount increment for {host_cluster:?} lost on disk"
        );
    }

    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_self_contained_active_metadata() -> io::Result<()> {
        let state = Arc::new(MemStorageState::default());
        let access = FormatAccess::new(crate::test::mem_storage::create_image(&state, 32768).await);
        access.write(&[0x5a; 1024][..], 0).await?;
        access.flush().await?;
        drop(access);

        state.data.lock()[88..96].copy_from_slice(&(1u64 << 63).to_be_bytes());
        let clean = state.data.lock().clone();
        let load_u64 = |at| u64::from_be_bytes(clean[at..at + 8].try_into().unwrap());
        let l1 = load_u64(40) as usize;
        let l2 = (load_u64(l1) & !(1 << 63)) as usize;
        let data = (load_u64(l2) & !(1 << 63)) as usize;
        let rt = load_u64(48) as usize;
        let rb = load_u64(rt) as usize;
        let data_refcount = rb + data / 512 * 2;
        let l2_refcount = rb + l2 / 512 * 2;

        for (at, bytes) in [
            (36, 0u32.to_be_bytes().to_vec()),
            (36, (L1Table::MAX_ENTRIES as u32 + 1).to_be_bytes().to_vec()),
            (40, (rt as u64).to_be_bytes().to_vec()),
            (l1, (rb as u64 | 1 << 63).to_be_bytes().to_vec()),
            (l2, (rb as u64 | 1 << 63).to_be_bytes().to_vec()),
            (l2, (rb as u64 | 1 << 62).to_be_bytes().to_vec()),
            (rb, 0u64.to_be_bytes().to_vec()),
            (rt, (l2 as u64).to_be_bytes().to_vec()),
            (data_refcount, 0u16.to_be_bytes().to_vec()),
            (data_refcount, 2u16.to_be_bytes().to_vec()),
            (l2_refcount, 2u16.to_be_bytes().to_vec()),
            (l2, (data as u64).to_be_bytes().to_vec()),
            (l1, (l2 as u64).to_be_bytes().to_vec()),
            (l2 + 8, load_u64(l2).to_be_bytes().to_vec()),
        ] {
            let mut malformed = clean.clone();
            malformed[at..at + bytes.len()].copy_from_slice(&bytes);
            state.data.lock().clone_from(&malformed);
            let result = Qcow2::<MemStorage>::builder(MemStorage::new(Arc::clone(&state), false))
                .write(true)
                .require_self_contained(true)
                .open(DenyImplicitOpenGate::default())
                .await;
            assert_eq!(result.err().unwrap().kind(), io::ErrorKind::InvalidData);
            assert_eq!(*state.data.lock(), malformed);
        }

        // Shared data and shared L2 tables have accurate clear COPIED bits.
        let mut shared = clean.clone();
        for (entry, cluster) in [(l1, l2), (l2, data)] {
            shared[entry..entry + 8].copy_from_slice(&(cluster as u64).to_be_bytes());
            let refcount = rb + cluster / 512 * 2;
            shared[refcount..refcount + 2].copy_from_slice(&2u16.to_be_bytes());
        }
        for valid in [clean, shared] {
            state.data.lock().clone_from(&valid);
            let image = Qcow2::<MemStorage>::builder(MemStorage::new(Arc::clone(&state), false))
                .write(true)
                .require_self_contained(true)
                .open(DenyImplicitOpenGate::default())
                .await?;
            assert_eq!(*state.data.lock(), valid);
            let image = FormatAccess::new(image);
            image.write(&[0x6b; 512][..], 512).await?;
            assert_eq!(&state.data.lock()[88..96], &[0; 8]);
            let mut original = [0; 512];
            image.read(&mut original[..], 0).await?;
            assert_eq!(original, [0x5a; 512]);
            image.flush().await?;
        }
        Ok(())
    }

    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_external_data_file_open_mode() -> io::Result<()> {
        use crate::file::File;
        use crate::format::gate::ImplicitOpenGate;
        use crate::FormatDriverBuilder;

        struct DataFileGate(bool);

        #[maybe_async::maybe_async(AFIT)]
        impl ImplicitOpenGate<File> for DataFileGate {
            async fn open_format<F: FormatDriverBuilder<File>>(
                &mut self,
                builder: F,
            ) -> io::Result<FormatAccess<File>> {
                DenyImplicitOpenGate::default().open_format(builder).await
            }

            async fn open_storage(&mut self, options: StorageOpenOptions) -> io::Result<File> {
                assert_eq!(options.get_writable(), self.0);
                File::open(options).await
            }
        }

        let path =
            std::env::temp_dir().join(format!("imago-qcow2-external-{}.qcow2", std::process::id()));
        let data_path = path.with_extension("data");
        let _cleanup = crate::misc_helpers::DropGuard::new(|| {
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(&data_path);
        });
        std::fs::write(&path, []).unwrap();
        std::fs::write(&data_path, []).unwrap();
        let metadata = File::open(StorageOpenOptions::new().filename(&path).write(true)).await?;
        let data = File::open(StorageOpenOptions::new().filename(&data_path).write(true)).await?;
        let qcow2 = Qcow2::<File>::create_builder(metadata)
            .cluster_size(512)
            .size(1024)
            .data_file(
                data_path.file_name().unwrap().to_str().unwrap().to_owned(),
                data,
            )
            .create_open(DenyImplicitOpenGate::default(), |image| {
                Ok(Qcow2::<File>::builder(image))
            })
            .await?;
        let image = FormatAccess::new(qcow2);
        image.write(&[0x5au8; 512][..], 0).await?;
        image.flush().await?;
        drop(image);
        let original_metadata = std::fs::read(&path).unwrap();
        let original_data = std::fs::read(&data_path).unwrap();
        for writable in [false, true] {
            for implicit in [false, true] {
                let metadata =
                    File::open(StorageOpenOptions::new().filename(&path).write(writable)).await?;
                let qcow2 = if implicit {
                    let mut qcow2 = Qcow2::<File>::open_image(metadata, writable).await?;
                    qcow2
                        .open_implicit_dependencies_gated(DataFileGate(writable))
                        .await?;
                    qcow2
                } else {
                    Qcow2::<File>::builder(metadata)
                        .write(writable)
                        .open(DataFileGate(writable))
                        .await?
                };
                let image = FormatAccess::new(qcow2);
                let mut buf = [0; 512];
                image.read(&mut buf[..], 0).await?;
                assert_eq!(buf, [0x5a; 512]);
                drop(image);
                assert_eq!(std::fs::read(&path).unwrap(), original_metadata);
                assert_eq!(std::fs::read(&data_path).unwrap(), original_data);
            }
        }
        Ok(())
    }

    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_read_truncated_image() -> io::Result<()> {
        let state = Arc::new(MemStorageState::default());
        let qcow2 = crate::test::mem_storage::create_image(&state, 1024).await;
        let header = Arc::clone(&qcow2.header);
        let image = FormatAccess::new(qcow2);
        image.write(&[0x5au8; 512][..], 0).await?;
        image.flush().await?;
        let (Mapping::Raw { offset, .. }, _) = image.get_mapping(0, 512).await? else {
            panic!("Expected a data mapping");
        };
        let original = state.data.lock().clone();
        drop(image);

        let path = std::env::temp_dir().join(format!(
            "imago-qcow2-read-truncated-{}.qcow2",
            std::process::id()
        ));
        let _cleanup = crate::misc_helpers::DropGuard::new(|| {
            let _ = std::fs::remove_file(&path);
        });
        let open_file = || {
            crate::file::File::from_open_file(
                std::fs::File::open(&path).unwrap(),
                StorageOpenOptions::new().filename(&path),
            )
            .unwrap()
        };
        std::fs::write(&path, &original).unwrap();
        let external = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let image = FormatAccess::new(
            Qcow2::<crate::file::File>::builder(open_file())
                .open(DenyImplicitOpenGate::default())
                .await?,
        );
        external.set_len(offset + 511).unwrap();
        let mut buf = [0xa5; 512];
        let error = image.read(&mut buf[..], 0).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        image.read(&mut buf[..511], 0).await?;
        assert_eq!(&buf[..511], &[0x5a; 511]);
        image.read(&mut buf[..], 512).await?;
        assert_eq!(buf, [0; 512]);
        buf.fill(0xa5);
        image.read(&mut buf[..], 1024).await?;
        assert_eq!(buf, [0; 512]);
        drop(image);

        let read_offset = |offset: usize| {
            u64::from_be_bytes(original[offset..offset + 8].try_into().unwrap())
                & 0x00ff_ffff_ffff_fe00
        };
        let l1_offset = header.l1_table_offset().0;
        let rt_offset = header.reftable_offset().0;
        let l2_offset = read_offset(l1_offset as usize);
        let rb_offset = read_offset(rt_offset as usize);
        // L1, L2 and RT share Table::load; refblocks have their own loader.
        for (offset, refblock) in [(l2_offset, false), (rb_offset, true)] {
            std::fs::write(&path, &original).unwrap();
            let storage = open_file();
            external.set_len(offset + 511).unwrap();
            let cluster = HostCluster(offset / 512);
            let error = if refblock {
                RefBlock::load(&storage, &header, cluster)
                    .await
                    .err()
                    .unwrap()
            } else {
                L2Table::load(&storage, &header, cluster, header.l2_entries())
                    .await
                    .err()
                    .unwrap()
            };
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }

        for length in [71, 103, 111, 113] {
            std::fs::write(&path, &original).unwrap();
            let storage = open_file();
            external.set_len(length).unwrap();
            let error = Header::load(&storage, false).await.err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }
        let mut v2 = original[..72].to_vec();
        v2[4..8].copy_from_slice(&2u32.to_be_bytes());
        std::fs::write(&path, v2).unwrap();
        Header::load(&open_file(), false).await?;

        let compressed = miniz_oxide::deflate::compress_to_vec(&[0x5a; 512], 6);
        let l2 = read_offset(l1_offset as usize) as usize;
        let mut compressed_image = original.clone();
        compressed_image[l2..l2 + 8].copy_from_slice(&((1u64 << 62) | offset).to_be_bytes());
        compressed_image[offset as usize..offset as usize + compressed.len()]
            .copy_from_slice(&compressed);
        for present in [compressed.len(), 2] {
            state.data.lock().clone_from(&compressed_image);
            let image = FormatAccess::new(
                Qcow2::<MemStorage>::builder(MemStorage::new(Arc::clone(&state), false))
                    .write(true)
                    .open(DenyImplicitOpenGate::default())
                    .await?,
            );
            state.data.lock().truncate(offset as usize + present);
            let mut buf = [0xa5; 512];
            let result = image.read(&mut buf[..], 0).await;
            if present == compressed.len() {
                result?;
                assert_eq!(buf, [0x5a; 512]);
                image.write(&[0xab][..], 32).await?;
                image.read(&mut buf[..], 0).await?;
                let mut expected = [0x5a; 512];
                expected[32] = 0xab;
                assert_eq!(buf, expected);
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
                assert_eq!(buf, [0xa5; 512]);
            }
            image.flush().await?;
        }
        Ok(())
    }
}
