//! Helper types.
//!
//! Contains types like `GuestOffset` or `HostCluster`.  This strong typing ensures there is no
//! confusion between what is what.

use super::*;
use std::fmt::{self, Display, Formatter};
use std::ops::{Add, AddAssign, Sub, SubAssign};

/// Guest offset split into its components.
#[derive(Clone, Copy, Debug)]
pub(super) struct GuestOffset {
    /// Index in the L1 table.
    pub l1_index: usize,
    /// Index in the L2 table.
    pub l2_index: usize,
    /// Offset in the cluster.
    pub in_cluster_offset: usize,
}

/// Guest cluster index.
#[derive(Clone, Copy, Debug)]
pub(super) struct GuestCluster {
    /// Index in the L1 table.
    pub l1_index: usize,
    /// Index in the L2 table.
    pub l2_index: usize,
}

/// Host cluster offset.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct HostOffset(pub u64);

/// Host cluster index.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct HostCluster(pub u64);

/// Cluster count.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ClusterCount(pub usize);

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2<S, F> {
    /// Split the given `offset` into its components.
    pub(super) fn split_guest_offset(&self, offset: u64) -> GuestOffset {
        GuestOffset::from_raw_offset(offset, self.header.cluster_bits())
    }
}

impl GuestOffset {
    /// Create a `GuestOffset` from its raw `u64` value.
    pub fn from_raw_offset(offset: u64, cluster_bits: u32) -> Self {
        let cluster_size = 1 << cluster_bits;
        let in_cluster_offset = (offset % cluster_size) as usize;
        let cluster_index = offset / cluster_size;
        let l2_entries = 1 << (cluster_bits - 3);
        let l2_index = (cluster_index % l2_entries) as usize;
        let l1_index = (cluster_index / l2_entries) as usize;

        GuestOffset {
            l1_index,
            l2_index,
            in_cluster_offset,
        }
    }

    /// Return the containing cluster’s index.
    pub fn cluster(self) -> GuestCluster {
        GuestCluster {
            l1_index: self.l1_index,
            l2_index: self.l2_index,
        }
    }

    /// How many bytes remain in this cluster after this offset.
    pub fn remaining_in_cluster(self, cluster_bits: u32) -> u64 {
        ((1 << cluster_bits) - self.in_cluster_offset) as u64
    }

    /// How many bytes remain in this L2 table after this offset.
    pub fn remaining_in_l2_table(self, cluster_bits: u32) -> u64 {
        // See `Header::l2_entries()`
        let l2_entries = 1 << (cluster_bits - 3);
        let after_this = ((l2_entries - (self.l2_index + 1)) as u64) << cluster_bits;
        self.remaining_in_cluster(cluster_bits) + after_this
    }

    /// Turn this strongly typed offset into its raw `u64` value.
    pub fn raw_offset(self, cluster_bits: u32) -> u64 {
        let cluster_index = ((self.l1_index as u64) << (cluster_bits - 3)) + self.l2_index as u64;
        (cluster_index << cluster_bits) + self.in_cluster_offset as u64
    }
}

impl GuestCluster {
    /// Return this cluster’s offset.
    pub fn offset(self) -> GuestOffset {
        GuestOffset {
            l1_index: self.l1_index,
            l2_index: self.l2_index,
            in_cluster_offset: 0,
        }
    }

    /// Turn this strongly typed index into its raw `u64` value.
    pub fn raw_index(self, cluster_bits: u32) -> u64 {
        ((self.l1_index as u64) << (cluster_bits - 3)) + self.l2_index as u64
    }

    /// Return this cluster’s offset in its raw `u64` form.
    pub fn raw_offset(self, cluster_bits: u32) -> u64 {
        self.raw_index(cluster_bits) << cluster_bits
    }

    /// Return the next cluster in this L2 table, if any.
    ///
    /// Return `None` if this is the last cluster in this L2 table.
    pub fn next_in_l2(self, cluster_bits: u32) -> Option<GuestCluster> {
        // See `Header::l2_entries()`
        let l2_entries = 1 << (cluster_bits - 3);
        let l2_index = self.l2_index.checked_add(1)?;
        if l2_index >= l2_entries {
            None
        } else {
            Some(GuestCluster {
                l1_index: self.l1_index,
                l2_index,
            })
        }
    }

    /// Return the first cluster in the next L2 table.
    pub fn first_in_next_l2(self) -> GuestCluster {
        GuestCluster {
            l1_index: self.l1_index + 1,
            l2_index: 0,
        }
    }
}

impl HostOffset {
    /// Return the offset from the start of the containing host cluster.
    pub fn in_cluster_offset(self, cluster_bits: u32) -> usize {
        (self.0 % (1 << cluster_bits)) as usize
    }

    /// Return the containing cluster’s index.
    pub fn cluster(self, cluster_bits: u32) -> HostCluster {
        HostCluster(self.0 >> cluster_bits)
    }

    /// If this offset points to the start of a cluster, get its index.
    ///
    /// If this offset points inside of a cluster, return `None`.  As oposed to just `cluster()`,
    /// this will not discard information: `self.checked_cluster(cb).unwrap().offset() == self`,
    /// because there is no in-cluster offset that could be lost.
    pub fn checked_cluster(self, cluster_bits: u32) -> Option<HostCluster> {
        (self.in_cluster_offset(cluster_bits) == 0).then_some(self.cluster(cluster_bits))
    }
}

impl HostCluster {
    /// Return this cluster’s offset.
    pub fn offset(self, cluster_bits: u32) -> HostOffset {
        HostOffset(self.0 << cluster_bits)
    }

    /// Returns the host offset corresponding to `guest_offset`.
    ///
    /// Assuming `guest_offset.cluster()` is mapped to `self`, return the exact host offset
    /// matching `guest_offset`.
    ///
    /// Same as `self.offset(cb) + guest_offset.in_cluster_offset`.
    pub fn relative_offset(self, guest_offset: GuestOffset, cluster_bits: u32) -> HostOffset {
        self.offset(cluster_bits) + guest_offset.in_cluster_offset
    }
}

impl ClusterCount {
    /// Get how many clusters are required to cover `byte_size`.
    ///
    /// This rounds up.
    pub fn from_byte_size(byte_size: usize, cluster_bits: u32) -> Self {
        ClusterCount(byte_size.div_ceil(1 << cluster_bits))
    }

    /// Return the full byte size of this many clusters.
    pub fn byte_size(self, cluster_bits: u32) -> usize {
        self.0 << cluster_bits
    }
}

impl Add<ClusterCount> for HostCluster {
    type Output = Self;

    fn add(self, rhs: ClusterCount) -> Self {
        HostCluster(self.0 + rhs.0 as u64)
    }
}

impl AddAssign<ClusterCount> for HostCluster {
    fn add_assign(&mut self, rhs: ClusterCount) {
        self.0 += rhs.0 as u64;
    }
}

impl Sub<ClusterCount> for HostCluster {
    type Output = Self;

    fn sub(self, rhs: ClusterCount) -> Self {
        HostCluster(self.0 - rhs.0 as u64)
    }
}

impl SubAssign<ClusterCount> for HostCluster {
    fn sub_assign(&mut self, rhs: ClusterCount) {
        self.0 -= rhs.0 as u64;
    }
}

impl Sub<HostCluster> for HostCluster {
    type Output = ClusterCount;

    fn sub(self, rhs: Self) -> ClusterCount {
        ClusterCount((self.0 - rhs.0) as usize)
    }
}

impl Add<ClusterCount> for ClusterCount {
    type Output = Self;

    fn add(self, rhs: ClusterCount) -> Self {
        ClusterCount(self.0 + rhs.0)
    }
}

impl AddAssign<ClusterCount> for ClusterCount {
    fn add_assign(&mut self, rhs: ClusterCount) {
        self.0 += rhs.0;
    }
}

impl Sub<ClusterCount> for ClusterCount {
    type Output = Self;

    fn sub(self, rhs: ClusterCount) -> Self {
        ClusterCount(self.0 - rhs.0)
    }
}

impl SubAssign<ClusterCount> for ClusterCount {
    fn sub_assign(&mut self, rhs: ClusterCount) {
        self.0 -= rhs.0;
    }
}

impl Add<usize> for HostOffset {
    type Output = Self;

    fn add(self, rhs: usize) -> Self {
        HostOffset(self.0 + rhs as u64)
    }
}

impl Sub<usize> for HostOffset {
    type Output = Self;

    fn sub(self, rhs: usize) -> Self {
        HostOffset(self.0 - rhs as u64)
    }
}

impl Sub<HostOffset> for HostOffset {
    type Output = usize;

    fn sub(self, rhs: Self) -> usize {
        (self.0 - rhs.0).try_into().unwrap()
    }
}

impl Display for HostOffset {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:x}", self.0)
    }
}

impl Display for ClusterCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
