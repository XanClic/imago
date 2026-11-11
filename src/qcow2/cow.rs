//! Copy-on-write operations.
//!
//! Implements copy-on-write when writing to clusters that are not simple allocated data clusters.

use super::*;
use crate::io_buffers::IoBuffer;

/// Host cluster prepared for a guest cluster by [`Qcow2::prepare_cow_cluster()`].
pub(super) struct PreparedCluster {
    /// Where data for the guest cluster is to be written to.
    pub host_cluster: HostCluster,

    /// What to pass to [`L2TableWriteGuard::map_cluster()`] to establish the mapping of guest to
    /// host cluster (L2 index, and the guard for `host_cluster` if it has been newly allocated).
    ///
    /// `None` if the guest cluster is mapped to `host_cluster` already.
    pub set_mapping: Option<(usize, Option<PendingClusters>)>,
}

#[maybe_async]
impl<S: Storage, F: WrappedFormat<S>> Qcow2<S, F> {
    /// Do copy-on-write for the given guest cluster, if necessary, without changing the L2 table
    /// yet.
    ///
    /// If the given guest cluster is backed by an allocated copied data cluster, return that
    /// cluster, so it can just be written into.
    ///
    /// Otherwise, allocate a new data cluster and copy the previously visible cluster contents
    /// there:
    /// - For non-copied data clusters, copy the cluster contents.
    /// - For zero clusters, write zeroes.
    /// - For unallocated clusters, copy data from the backing file (if any, zeroes otherwise).
    /// - For compressed clusters, decompress the data and write it into the new cluster.
    ///
    /// `cluster` is the guest cluster to COW.
    ///
    /// `mandatory_host_cluster` may specify the cluster that must be used for the new allocation,
    /// or that an existing data cluster allocation must match.  If it does not match, or that
    /// cluster is already allocated and cannot be used, return `Ok(None)`.
    ///
    /// `partial_skip_cow` may give an in-cluster range that is supposed to be overwritten
    /// immediately anyway, i.e. that need not be copied.
    ///
    /// `l2_table` is the L2 table for `offset`.
    ///
    /// Return the mapping into which to write, which may be a newly allocated cluster or the old
    /// cluster incase it was already safe to write to.  If the mapping requires an L2 entry to be
    /// changed, this function does not do so, but leaves it to the caller to pass the returned
    /// mapping information to [`L2TableWriteGuard::map_cluster()`].  Until then, `cluster` retains
    /// its previous mapping.  The caller should set the mapping only once the data for `cluster`
    /// has been written, so that the new cluster’s previous contents never become visible.
    pub(super) async fn prepare_cow_cluster(
        &self,
        cluster: GuestCluster,
        mandatory_host_cluster: Option<HostCluster>,
        partial_skip_cow: Option<Range<usize>>,
        l2_table: &L2TableWriteGuard,
    ) -> io::Result<Option<PreparedCluster>> {
        // No need to do COW when writing the full cluster
        let full_skip_cow = if let Some(skip) = partial_skip_cow.as_ref() {
            skip.start == 0 && skip.end == self.header.cluster_size()
        } else {
            false
        };

        let existing_mapping = l2_table.get_mapping(cluster)?;
        if let L2Mapping::DataFile {
            host_cluster,
            copied: true,
        } = existing_mapping
        {
            if let Some(mandatory_host_cluster) = mandatory_host_cluster {
                if host_cluster != mandatory_host_cluster {
                    return Ok(None);
                }
            }
            return Ok(Some(PreparedCluster {
                host_cluster,
                set_mapping: None,
            }));
        };

        self.prepare_write().await?;

        // New cluster index, and, if newly allocated, a `PendingClusters` guard
        let new_cluster = match existing_mapping {
            L2Mapping::Zero {
                host_cluster: Some(host_cluster),
                copied: true,
            } if mandatory_host_cluster.is_none_or(|required| required == host_cluster) => {
                Some((host_cluster, None))
            }
            _ => self
                .allocate_data_cluster_at(cluster, mandatory_host_cluster)
                .await?
                .map(|allocation| (allocation.cluster(), Some(allocation))),
        };

        let Some((new_cluster, new_allocation)) = new_cluster else {
            // Allocation at `mandatory_host_cluster` failed
            return Ok(None);
        };

        if !full_skip_cow {
            match existing_mapping {
                L2Mapping::DataFile {
                    host_cluster: _,
                    copied: true,
                } => unreachable!(),

                L2Mapping::DataFile {
                    host_cluster,
                    copied: false,
                } => {
                    self.cow_copy_storage(
                        self.storage(),
                        host_cluster,
                        new_cluster,
                        partial_skip_cow,
                    )
                    .await?
                }

                L2Mapping::Backing { backing_offset } => {
                    if let Some(backing) = self.backing.as_ref() {
                        self.cow_copy_format(backing, backing_offset, new_cluster, partial_skip_cow)
                            .await?
                    } else {
                        self.cow_zero(new_cluster, partial_skip_cow).await?
                    }
                }

                L2Mapping::Zero {
                    host_cluster: _,
                    copied: _,
                } => self.cow_zero(new_cluster, partial_skip_cow).await?,

                L2Mapping::Compressed {
                    host_offset: _,
                    length: _,
                } => self.cow_compressed(l2_table, cluster, new_cluster).await?,
            }
        }

        let l2i = cluster.l2_index(self.header.cluster_bits());
        Ok(Some(PreparedCluster {
            host_cluster: new_cluster,
            set_mapping: Some((l2i, new_allocation)),
        }))
    }

    /// Calculate what range of a cluster we need to COW.
    ///
    /// Given potentially a range to skip, calculate what we should COW.  The range will only be
    /// taken into account if it is at one end of the cluster, to always yield a continuous range
    /// to COW (one without a hole in the middle).
    ///
    /// The returned range is also aligned to `alignment` if possible.
    fn get_cow_range(
        &self,
        partial_skip_cow: Option<Range<usize>>,
        alignment: usize,
    ) -> Option<Range<usize>> {
        let mut copy_range = 0..self.header.cluster_size();
        if let Some(partial_skip_cow) = partial_skip_cow {
            if partial_skip_cow.start == copy_range.start {
                copy_range.start = partial_skip_cow.end;
            } else if partial_skip_cow.end == copy_range.end {
                copy_range.end = partial_skip_cow.start;
            }
        }

        if copy_range.is_empty() {
            return None;
        }

        let alignment = cmp::min(alignment, self.header.cluster_size());
        debug_assert!(alignment.is_power_of_two());
        let mask = alignment - 1;

        if copy_range.start & mask != 0 {
            copy_range.start &= !mask;
        }
        if copy_range.end & mask != 0 {
            copy_range.end = (copy_range.end & !mask) + alignment;
        }

        Some(copy_range)
    }

    /// Copy data from one data file cluster to another.
    ///
    /// Used for COW on non-copied data clusters.
    async fn cow_copy_storage(
        &self,
        from: &S,
        from_cluster: HostCluster,
        to_cluster: HostCluster,
        partial_skip_cow: Option<Range<usize>>,
    ) -> io::Result<()> {
        let to = self.storage();

        let align = cmp::max(from.req_align(), to.req_align());
        let Some(cow_range) = self.get_cow_range(partial_skip_cow, align) else {
            return Ok(());
        };

        let mut buf = IoBuffer::new(cow_range.end - cow_range.start, from.mem_align())?;

        let cb = self.header.cluster_bits();
        let from_offset = from_cluster.offset(cb);
        let to_offset = to_cluster.offset(cb);

        let requested = buf.len() as u64;
        if let Some(actual) = from
            .readv_counted((&mut buf).into(), from_offset.0 + cow_range.start as u64)
            .await?
        {
            if actual < requested {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Short qcow2 COW read: {actual} < {requested}"),
                ));
            }
        }

        to.write(&buf, to_offset.0 + cow_range.start as u64).await?;

        Ok(())
    }

    /// Copy data from another image into our data file.
    ///
    /// Used for COW on clusters served by a backing image.
    async fn cow_copy_format(
        &self,
        from: &F,
        from_offset: u64,
        to_cluster: HostCluster,
        partial_skip_cow: Option<Range<usize>>,
    ) -> io::Result<()> {
        let to = self.storage();
        let from = from.inner();

        let align = cmp::max(from.req_align(), to.req_align());
        let Some(cow_range) = self.get_cow_range(partial_skip_cow, align) else {
            return Ok(());
        };

        let mut buf = IoBuffer::new(cow_range.end - cow_range.start, from.mem_align())?;

        let to_offset = to_cluster.offset(self.header.cluster_bits());

        from.read(&mut buf, from_offset + cow_range.start as u64)
            .await?;

        to.write(&buf, to_offset.0 + cow_range.start as u64).await?;

        Ok(())
    }

    /// Fill the given cluster with zeroes.
    ///
    /// Used for COW on zero clusters.
    async fn cow_zero(
        &self,
        to_cluster: HostCluster,
        partial_skip_cow: Option<Range<usize>>,
    ) -> io::Result<()> {
        let to = self.storage();

        let align = to.req_align();
        let Some(cow_range) = self.get_cow_range(partial_skip_cow, align) else {
            return Ok(());
        };

        let to_offset = to_cluster.offset(self.header.cluster_bits());
        to.write_zeroes(
            to_offset.0 + cow_range.start as u64,
            (cow_range.end - cow_range.start) as u64,
        )
        .await?;

        Ok(())
    }

    /// Decompress a cluster into the target cluster.
    ///
    /// Used for COW on compressed clusters.  `cluster` must be compressed in `l2_table`.
    async fn cow_compressed(
        &self,
        l2_table: &L2TableWriteGuard,
        cluster: GuestCluster,
        to_cluster: HostCluster,
    ) -> io::Result<()> {
        let to = self.storage();

        let Some(compressed_guard) = l2_table.compressed_read_guard(cluster)? else {
            // We hold the write guard, so the mapping cannot have changed since the caller looked
            // it up
            unreachable!();
        };

        let mut buf = IoBuffer::new(self.header.cluster_size(), to.mem_align())?;
        compressed_guard
            .read_compressed(to, buf.as_mut().into_slice())
            .await?;

        let to_offset = to_cluster.offset(self.header.cluster_bits());
        to.write(&buf, to_offset.0).await?;

        Ok(())
    }
}
