//! Support for compressed clusters.

use super::*;
use crate::io_buffers::IoBuffer;
use crate::sync_primitives::RwLockEitherGuard;
use miniz_oxide::inflate::core::{decompress as inflate, DecompressorOxide};
use miniz_oxide::inflate::TINFLStatus;
use std::num::NonZero;

/// Read guard for a compressed cluster.
///
/// Readers generally do not need to lock L2 tables: Modifications replace mappings atomically, and
/// allocations referenced by a (data) mapping are not freed while the image can be accessed
/// concurrently.  However, allocations referenced by compressed mappings may be freed when they
/// are replaced (by concurrent writes), so the L2 table must not be modified from looking up the
/// compressed mapping until its data has been read.  This guard ensures that.
#[derive(Debug)]
pub(super) struct CompressedClusterReadGuard<'a> {
    /// Offset of the compressed data.
    host_offset: HostOffset,

    /// Upper limit on the number of bytes that comprise the compressed data.
    length: NonZero<u64>,

    /// log2 of the cluster size.
    cluster_bits: u32,

    /// Held lock on the L2 table, preventing modifications.
    _lock: RwLockEitherGuard<'a, ()>,
}

#[maybe_async]
impl<'a> CompressedClusterReadGuard<'a> {
    /// Create a new guard for reading compressed data from the given cluster, under `lock`, which
    /// prevents concurrent modifications of that cluster.
    ///
    /// `lock` must be a read or write lock on `L2Table.writer_lock`.
    ///
    /// Return `None` if `cluster` is not compressed (checked under the guard).
    pub fn new(
        l2_table: &L2Table,
        cluster: GuestCluster,
        lock: RwLockEitherGuard<'a, ()>,
    ) -> io::Result<Option<Self>> {
        let L2Mapping::Compressed {
            host_offset,
            length,
        } = l2_table.get_mapping(cluster)?
        else {
            return Ok(None);
        };

        Ok(Some(CompressedClusterReadGuard {
            host_offset,
            length,
            cluster_bits: l2_table.cluster_bits(),
            _lock: lock,
        }))
    }

    /// Read the compressed cluster.
    ///
    /// Read the compressed data from `storage` (which must be the image’s data file) and
    /// decompress it into `buf` (which must have a length of exactly one cluster).
    pub async fn read_compressed<S: Storage>(&self, storage: &S, buf: &mut [u8]) -> io::Result<()> {
        debug_assert!(buf.len() == 1 << self.cluster_bits);

        // Must fit (really shouldn’t be compressed if this exceeds the cluster size anyway)
        let length = self.length.get().try_into().map_err(io::Error::other)?;
        let mut compressed_buf = IoBuffer::new(length, storage.mem_align())?;
        let actual = storage
            .readv_counted((&mut compressed_buf).into(), self.host_offset.0)
            .await?;
        let available = actual.map_or(length, |actual| cmp::min(actual, length as u64) as usize);
        let short_read = available < length;

        let mut dec_ox = DecompressorOxide::new();
        let (status, _read, written) = inflate(
            &mut dec_ox,
            &compressed_buf.as_ref().into_slice()[..available],
            buf,
            0,
            0,
        );

        if short_read
            && (written < buf.len()
                || matches!(
                    status,
                    TINFLStatus::NeedsMoreInput | TINFLStatus::FailedCannotMakeProgress
                ))
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Truncated compressed cluster (host offset {}+{available}, expected up to {length})",
                    self.host_offset.0,
                ),
            ));
        }

        // Because `length` will generally exceed the actual length, `HasMoreOutput` is expected
        // and can be ignored
        if status != TINFLStatus::Done && status != TINFLStatus::HasMoreOutput {
            return Err(io::Error::other(format!(
                "Failed to decompress cluster (host offset {}+{length}): {status:?}",
                self.host_offset.0,
            )));
        }
        if written < buf.len() {
            return Err(io::Error::other(format!(
                "Failed to decompress cluster (host offset {}+{length}): Decompressed {written} bytes, expected {}",
                self.host_offset.0,
                buf.len(),
            )));
        }

        Ok(())
    }
}
