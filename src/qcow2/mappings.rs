//! Get and establish cluster mappings.

use super::*;
use tokio::sync::RwLockWriteGuard;

impl<S: Storage, F: WrappedFormat<S>> Qcow2<S, F> {
    /// Get the given range’s mapping information.
    ///
    /// Underlying implementation for [`Qcow2::get_mapping()`].
    pub(super) async fn do_get_mapping(
        &self,
        offset: GuestOffset,
        max_length: u64,
    ) -> io::Result<(Mapping<'_, S>, u64)> {
        let Some(l2_table) = self.get_l2(offset).await? else {
            let cb = self.header.cluster_bits();
            let len = cmp::min(offset.remaining_in_l2_table(cb), max_length);
            let mapping = if let Some(backing) = self.backing.as_ref() {
                Mapping::Indirect {
                    layer: backing.unwrap(),
                    offset: offset.0,
                    writable: false,
                }
            } else {
                Mapping::Zero
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
    ) -> io::Result<(Mapping<'_, S>, u64)> {
        let cb = self.header.cluster_bits();

        // FIXME
        let _cow_lock = self.cow_lock.read().await;

        // Get mapping at `offset`
        let mut current_guest_cluster = offset.cluster(cb);
        let first_mapping = l2_table.get_mapping(current_guest_cluster)?;
        let return_mapping = match first_mapping {
            L2Mapping::DataFile {
                host_cluster,
                copied,
            } => Mapping::Raw {
                storage: self.storage(),
                offset: host_cluster.relative_offset(offset, cb).0,
                writable: copied,
            },

            L2Mapping::Backing { backing_offset } => {
                if let Some(backing) = self.backing.as_ref() {
                    Mapping::Indirect {
                        layer: backing.unwrap(),
                        offset: backing_offset + offset.in_cluster_offset(cb) as u64,
                        writable: false,
                    }
                } else {
                    Mapping::Zero
                }
            }

            L2Mapping::Zero {
                host_cluster: _,
                copied: _,
            } => Mapping::Zero,

            L2Mapping::Compressed {
                host_offset: _,
                length: _,
            } => Mapping::Special { offset: offset.0 },
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

    /// Make the given range be mapped by data clusters.
    ///
    /// Underlying implementation for [`Qcow2::ensure_data_mapping()`].
    pub(super) async fn do_ensure_data_mapping(
        &self,
        offset: GuestOffset,
        length: u64,
        overwrite: bool,
    ) -> io::Result<(&'_ S, u64, u64)> {
        let mut l2_table = self.ensure_l2(offset).await?;

        // TODO: Is there a more optimized way?
        // We do not want to write-lock the COW guard until we need to.  But upgrading the COW lock
        // from Read to Write is not possible; so once we drop the Read variant, we will need to
        // re-check the whole table anyway.
        // FWIW, this is a fast path for already-present allocations, so not too terrible.
        let existing = self
            .do_get_mapping_with_l2(offset, length, &l2_table)
            .await?;
        if let Mapping::Raw {
            storage,
            offset,
            writable: true,
        } = existing.0
        {
            return Ok((storage, offset, existing.1));
        }

        // FIXME
        let cow_lock = self.cow_lock.write().await;
        let mut leaked_allocations = Vec::<(HostCluster, ClusterCount)>::new();

        let res = self
            .ensure_data_mapping_no_cleanup(
                offset,
                length,
                overwrite,
                &mut l2_table,
                &mut leaked_allocations,
            )
            .await;

        if l2_table.is_modified() {
            // TODO: What to do on error?  Currently, we just accept that clusters will be leaked
            // and we will have to reload the table from disk.
            l2_table.write(&self.metadata).await?;
        }
        drop(cow_lock);

        for alloc in leaked_allocations {
            self.free_data_clusters(alloc.0, alloc.1).await;
        }
        let (host_offset, length) = res?;

        Ok((self.storage(), host_offset, length))
    }

    /// Load the L2 table in `l2_cluster`.
    async fn load_l2(&self, l2_cluster: HostCluster) -> io::Result<L2Table> {
        L2Table::load(
            &self.metadata,
            &self.header,
            l2_cluster,
            self.header.l2_entries(),
        )
        .await
    }

    /// Get the L2 table referenced by the given L1 table index, if any.
    ///
    /// If the L1 table index does not point to any L2 table, return `Ok(None)`.
    pub(super) async fn get_l2(&self, offset: GuestOffset) -> io::Result<Option<L2Table>> {
        let cb = self.header.cluster_bits();

        // TODO: Cache L2s
        let l1_entry = self.l1_table.read().await.get(offset.l1_index(cb));
        if let Some(l2_offset) = l1_entry.l2_offset() {
            let l2_cluster = l2_offset.checked_cluster(cb).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unaligned L2 table for {offset:?}; L1 entry: {l1_entry:?}"),
                )
            })?;
            self.load_l2(l2_cluster).await.map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a L2 table for the given L1 table index.
    ///
    /// If there already is an L2 table at that index, return it.  Otherwise, create one and hook
    /// it up.
    pub(super) async fn ensure_l2(&self, offset: GuestOffset) -> io::Result<L2Table> {
        let cb = self.header.cluster_bits();

        if let Some(l2) = self.get_l2(offset).await? {
            return Ok(l2);
        }

        self.need_writable()?;

        let mut l1_locked = self.l1_table.write().await;
        let l1_index = offset.l1_index(cb);
        if !l1_locked.in_bounds(l1_index) {
            l1_locked = self.grow_l1_table(l1_locked, l1_index).await?;
        }

        let l1_entry = l1_locked.get(l1_index);
        if let Some(l2_offset) = l1_entry.l2_offset() {
            drop(l1_locked);

            let l2_cluster = l2_offset.checked_cluster(cb).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unaligned L2 table for {offset:?}; L1 entry: {l1_entry:?}"),
                )
            })?;

            return self.load_l2(l2_cluster).await;
        }

        let l2_cluster = self.allocate_meta_cluster().await?;
        let mut l2_table = L2Table::new_cleared(&self.header);
        l2_table.set_cluster(l2_cluster);
        l2_table.write(&self.metadata).await?;

        l1_locked.enter_l2_table(l1_index, &l2_table)?;
        l1_locked.write_entry(&self.metadata, l1_index).await?;

        Ok(l2_table)
    }

    /// Create a new L1 table covering at least `at_least_index`.
    ///
    /// Create a new L1 table of the required size with all the entries of the previous L1 table.
    async fn grow_l1_table<'a>(
        &self,
        mut l1_locked: RwLockWriteGuard<'a, L1Table>,
        at_least_index: usize,
    ) -> io::Result<RwLockWriteGuard<'a, L1Table>> {
        let cb = self.header.cluster_bits();

        let mut new_l1 = l1_locked.clone_and_grow(at_least_index, &self.header);

        let l1_start = self
            .allocate_meta_clusters(new_l1.cluster_count(cb))
            .await?;

        new_l1.set_cluster(l1_start);
        new_l1.write(&self.metadata).await?;

        self.header.set_l1_table(&new_l1)?;
        self.header.write_l1_table_pointer(&self.metadata).await?;

        if let Some(old_l1_cluster) = l1_locked.get_cluster() {
            let old_l1_size = l1_locked.cluster_count(cb);
            l1_locked.unset_cluster();
            self.free_meta_clusters(old_l1_cluster, old_l1_size).await;
        }

        *l1_locked = new_l1;

        Ok(l1_locked)
    }

    /// Inner implementation for [`Qcow2::do_ensure_data_mapping()`].
    ///
    /// Does not do any clean-up: The L2 table will probably be modified, but not written to disk.
    /// Any existing allocations that have been removed from it (and are thus leaked) are entered
    /// into `leaked_allocations`, but not freed.
    ///
    /// The caller must do both, ensuring it is done both in case of success and in case of error.
    async fn ensure_data_mapping_no_cleanup(
        &self,
        offset: GuestOffset,
        full_length: u64,
        overwrite: bool,
        l2_table: &mut L2Table,
        leaked_allocations: &mut Vec<(HostCluster, ClusterCount)>,
    ) -> io::Result<(u64, u64)> {
        let cb = self.header.cluster_bits();

        let partial_skip_cow = overwrite.then(|| {
            let start = offset.in_cluster_offset(cb);
            let end = cmp::min(start as u64 + full_length, 1 << cb) as usize;
            start..end
        });

        let mut current_guest_cluster = offset.cluster(cb);

        // Without a mandatory host offset, this should never return `Ok(None)`
        let host_cluster = self
            .cow_cluster(
                current_guest_cluster,
                None,
                partial_skip_cow,
                l2_table,
                leaked_allocations,
            )
            .await?
            .ok_or_else(|| io::Error::other("Internal allocation error"))?;

        let host_offset_start = host_cluster.relative_offset(offset, cb);
        let mut allocated_length = offset.remaining_in_cluster(cb);
        let mut current_host_cluster = host_cluster;

        while allocated_length < full_length {
            let Some(next) = current_guest_cluster.next_in_l2(cb) else {
                break;
            };
            current_guest_cluster = next;

            let chunk_length = cmp::min(full_length - allocated_length, 1 << cb) as usize;
            let partial_skip_cow = overwrite.then(|| 0..chunk_length);

            let next_host_cluster = current_host_cluster + ClusterCount(1);
            let host_cluster = self
                .cow_cluster(
                    current_guest_cluster,
                    Some(next_host_cluster),
                    partial_skip_cow,
                    l2_table,
                    leaked_allocations,
                )
                .await?;

            let Some(host_cluster) = host_cluster else {
                // Cannot continue continuous mapping range
                break;
            };
            assert!(host_cluster == next_host_cluster);
            current_host_cluster = host_cluster;

            allocated_length += chunk_length as u64;
        }

        Ok((host_offset_start.0, allocated_length))
    }
}
