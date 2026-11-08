//! Tests for requests whose futures are dropped while the storage still has them in flight, and
//! for requests racing each other.
//!
//! Dropping a future does not necessarily stop the I/O it has issued (e.g. with io_uring, the
//! kernel will still perform it), so such requests may complete at any later point.

use super::*;
use crate::test::mem_storage::{MemStorage, MemStorageState};
use crate::{DenyImplicitOpenGate, Mapping};
use futures::FutureExt;
use std::sync::atomic::Ordering;
use std::task::Poll;

/// Create a 512-byte-cluster image of the given size on a [`MemStorage`] with the given state.
async fn create_image(state: &Arc<MemStorageState>, size: u64) -> FormatAccess<MemStorage> {
    FormatAccess::new(crate::test::mem_storage::create_image(state, size).await)
}

/// Return the host offset `offset` is mapped to; panic if it is not mapped to raw data.
async fn host_offset(image: &FormatAccess<MemStorage>, offset: u64) -> u64 {
    match image.get_mapping(offset, 1).await.unwrap().0 {
        Mapping::Raw { offset, .. } => offset,
        mapping => panic!("Expected raw data mapping at {offset:#x}, got {mapping:?}"),
    }
}

/// A write whose future is dropped while it is still in flight must not overwrite data in a
/// cluster that has been freed (by discarding it) and reallocated in the meantime.
///
/// `discard_to_zero()` takes a mutable reference so no concurrent request can hold a mapping to
/// the discarded cluster, but that does not cover requests whose futures have been dropped.
#[tokio::test]
async fn test_dropped_write_vs_discard_and_realloc() {
    let state = Arc::new(MemStorageState::default());
    let mut image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0
    image.write(&[0x11u8; 512][..], 0).await.unwrap();
    let freed_host_offset = host_offset(&image, 0).await;

    // Issue a write to guest cluster 0, and drop its future while it is still in flight
    state.hold_writes.store(true, Ordering::Relaxed);
    assert!(image.write(&[0x22u8; 512][..], 0).now_or_never().is_none());
    assert_eq!(state.held.lock().len(), 1);
    state.hold_writes.store(false, Ordering::Relaxed);

    // Free guest cluster 0’s host cluster
    image.discard_to_zero(0, 512).await.unwrap();

    // Allocate guest cluster 1, which must reuse that host cluster for this test to be meaningful
    let new_data = [0x33u8; 512];
    image.write(&new_data[..], 512).await.unwrap();
    assert_eq!(host_offset(&image, 512).await, freed_host_offset);

    // Now the dropped write completes (unless it has already been settled before)
    state.complete_held_writes();

    let mut buf = [0u8; 512];
    image.read(&mut buf[..], 512).await.unwrap();
    assert!(
        buf == new_data,
        "Expected {:#04x} bytes, read {:#04x} bytes",
        new_data[0],
        buf[0],
    );
}

/// A dropped L1 entry write must prevent growth and reuse while a late write can still land.
#[tokio::test]
async fn test_dropped_l1_entry_write_vs_l1_growth_and_realloc() {
    let state = Arc::new(MemStorageState::default());
    let mut image = create_image(&state, 64 * 1024).await;
    let l1_offset = read_u64(&state, 40);
    *state.hold_range.lock() = Some(l1_offset..l1_offset + 512);
    state.hold_writes.store(true, Ordering::Relaxed);
    let mut write = Box::pin(image.write(&[0x22u8; 512][..], 0));
    assert!(crate::test::mem_storage::poll_once(write.as_mut()).is_pending());
    assert_eq!(state.held.lock().len(), 1);
    drop(write);
    state.hold_writes.store(false, Ordering::Relaxed);
    *state.hold_range.lock() = None;
    image
        .resize(4 * 1024 * 1024, PreallocateMode::None)
        .await
        .unwrap_err();
    image.write(&[0x33u8; 512][..], 512).await.unwrap_err();
    state.complete_held_writes();
    image.flush().await.unwrap_err();
}

/// Return the cluster-aligned host offset (assuming a cluster size of 512) of the first held
/// write, which must exist.
fn first_held_cluster(state: &MemStorageState) -> u64 {
    state.held.lock().first().expect("Expected a held write").0 & !511
}

/// If a write allocating a new data cluster is cancelled while copying data into it (COW), the
/// cluster must be freed, not leaked.
#[tokio::test]
async fn test_cancelled_cow_frees_cluster() {
    let state = Arc::new(MemStorageState::default());
    let image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0 (so the L2 table exists)
    image.write(&[0x11u8; 512][..], 0).await.unwrap();

    // Partially write guest cluster 1, so the rest of it must be COW-ed into the new cluster, and
    // drop the future while that write is in flight
    state.hold_writes.store(true, Ordering::Relaxed);
    let mut write = Box::pin(image.write(&[0x22u8; 256][..], 512));
    assert!(crate::test::mem_storage::poll_once(write.as_mut()).is_pending());
    drop(write);
    state.hold_writes.store(false, Ordering::Relaxed);
    let cancelled_cluster = first_held_cluster(&state);

    // Guest cluster 1 must not be mapped
    let mapping = image.get_mapping(512, 512).await.unwrap().0;
    assert!(
        !matches!(mapping, Mapping::Raw { .. }),
        "Cancelled write left a mapping: {mapping:?}"
    );

    // Flushing frees the clusters from the cancelled write (after settling its writes)
    image.flush().await.unwrap();

    // So the next allocation can reuse it
    image.write(&[0x33u8; 512][..], 1024).await.unwrap();
    assert_eq!(host_offset(&image, 1024).await, cancelled_cluster);
}

/// If a write allocating a new data cluster fails while copying data into it (COW), the cluster
/// must be freed, not leaked.
#[tokio::test]
async fn test_failed_cow_frees_cluster() {
    let state = Arc::new(MemStorageState::default());
    let image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0 (so the L2 table exists)
    image.write(&[0x11u8; 512][..], 0).await.unwrap();

    // Partially write guest cluster 1, so the rest of it must be COW-ed into the new cluster, and
    // let that write fail
    state.fail_next_write.store(true, Ordering::Relaxed);
    image.write(&[0x22u8; 256][..], 512).await.unwrap_err();
    let failed_cluster = {
        let failed = state.failed.lock();
        assert_eq!(failed.len(), 1, "Expected exactly one failed write");
        failed[0] & !511
    };

    // The failed write has freed the cluster, so the next allocation can reuse it
    image.write(&[0x33u8; 512][..], 1024).await.unwrap();
    assert_eq!(host_offset(&image, 1024).await, failed_cluster);
}

/// If the data write of a write that allocates a cluster fails, the guest cluster must keep its
/// previous mapping, and the new cluster must be freed.
#[tokio::test]
async fn test_failed_write_keeps_mapping() {
    let state = Arc::new(MemStorageState::default());
    let image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0 (so the L2 table exists)
    image.write(&[0x11u8; 512][..], 0).await.unwrap();

    // Fully overwrite guest cluster 1, and let the data write fail
    state.fail_next_write.store(true, Ordering::Relaxed);
    image.write(&[0x22u8; 512][..], 512).await.unwrap_err();
    let failed_cluster = {
        let failed = state.failed.lock();
        assert_eq!(failed.len(), 1, "Expected exactly one failed write");
        failed[0] & !511
    };

    // Guest cluster 1 must not be mapped
    if let Mapping::Raw { offset, .. } = image.get_mapping(512, 512).await.unwrap().0 {
        panic!("Failed write left a mapping to host offset {offset:#x}");
    }

    // Flushing frees the cluster from the failed write, so the next allocation can reuse it
    image.flush().await.unwrap();
    image.write(&[0x33u8; 512][..], 1024).await.unwrap();
    assert_eq!(host_offset(&image, 1024).await, failed_cluster);
}

/// If a write that allocates a cluster is dropped while its data write is in flight, the guest
/// cluster must keep its previous mapping, and the new cluster must be freed.
#[tokio::test]
async fn test_dropped_write_keeps_mapping() {
    let state = Arc::new(MemStorageState::default());
    let image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0 (so the L2 table exists)
    image.write(&[0x11u8; 512][..], 0).await.unwrap();

    // Fully overwrite guest cluster 1, and drop the future while the data write is in flight
    state.hold_writes.store(true, Ordering::Relaxed);
    let mut write = Box::pin(image.write(&[0x22u8; 512][..], 512));
    assert!(crate::test::mem_storage::poll_once(write.as_mut()).is_pending());
    drop(write);
    state.hold_writes.store(false, Ordering::Relaxed);
    let dropped_cluster = first_held_cluster(&state);

    // Guest cluster 1 must not be mapped
    if let Mapping::Raw { offset, .. } = image.get_mapping(512, 512).await.unwrap().0 {
        panic!("Dropped write left a mapping to host offset {offset:#x}");
    }

    // Flushing frees the cluster from the dropped write (after settling its writes), so the next
    // allocation can reuse it
    image.flush().await.unwrap();
    image.write(&[0x33u8; 512][..], 1024).await.unwrap();
    assert_eq!(host_offset(&image, 1024).await, dropped_cluster);
}

/// Open the image on a [`MemStorage`] with the given state.
async fn open_image(state: &Arc<MemStorageState>) -> FormatAccess<MemStorage> {
    let qcow2 = Qcow2::<MemStorage>::builder(MemStorage::new(Arc::clone(state), false))
        .backing(None)
        .write(true)
        .open(DenyImplicitOpenGate::default())
        .await
        .unwrap();

    FormatAccess::new(qcow2)
}

#[tokio::test]
async fn test_cancelled_resize_rejects_retry() -> io::Result<()> {
    for (new_size, late_write, expected_size) in [
        (1536, false, 2048),
        (1536, true, 1536),
        (2560, false, 2048),
        (2560, true, 2048),
    ] {
        let state = Arc::new(MemStorageState::default());
        let qcow2 = crate::test::mem_storage::create_image(&state, 2048).await;
        let metadata = Arc::clone(&qcow2.metadata);
        let mut image = FormatAccess::new(qcow2);
        image.write(&[0x5au8; 512][..], 0).await?;
        image.flush().await?;
        *state.hold_range.lock() = Some(0..72);
        state.hold_writes.store(late_write, Ordering::Relaxed);
        let blocker = if late_write {
            None
        } else {
            Some(metadata.strong_write_blocker(0..72).await)
        };
        assert!(image
            .resize(new_size, PreallocateMode::None)
            .now_or_never()
            .is_none());
        state.hold_writes.store(false, Ordering::Relaxed);
        drop(blocker);
        assert_eq!(image.size(), 2048);
        image
            .resize(new_size, PreallocateMode::None)
            .await
            .unwrap_err();
        image.sync().await.unwrap_err();
        state.complete_held_writes();
        image.flush().await.unwrap_err();
        drop(image);
        let reopened = open_image(&state).await;
        assert_eq!(reopened.size(), expected_size);
        let mut buf = [0; 512];
        reopened.read(&mut buf[..], 0).await?;
        assert_eq!(buf, [0x5a; 512]);
    }
    Ok(())
}

/// Read a big-endian 64-bit value from the storage.
fn read_u64(state: &MemStorageState, offset: u64) -> u64 {
    let data = state.data.lock();
    let offset = offset as usize;
    u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap())
}

/// Create a 512-byte-cluster image where the guest cluster at `guest_offset` is a compressed
/// cluster containing `data`. Return the image and the compressed data's host offset.
///
/// imago cannot write compressed clusters, so put the compressed data and the compressed L2 entry
/// directly into the image file.
async fn create_compressed_image(
    state: &Arc<MemStorageState>,
    guest_offset: u64,
    data: &[u8; 512],
) -> (FormatAccess<MemStorage>, u64) {
    let image = create_image(state, 64 * 1024).await;

    // Allocate a host cluster
    image.write(&[0x11u8; 512][..], guest_offset).await.unwrap();
    let compressed_host_offset = host_offset(&image, guest_offset).await;
    image.flush().await.unwrap();
    drop(image);

    // Turn the guest cluster into a compressed cluster: Put compressed data into its host cluster,
    // and replace its L2 entry by a compressed cluster descriptor. With 512-byte clusters, the
    // descriptor has 61 offset bits, followed by the number of additional sectors (0 here).
    let compressed = miniz_oxide::deflate::compress_to_vec(data, 6);
    assert!(compressed.len() <= 512);
    state.write(compressed_host_offset, &compressed);

    let l1_offset = read_u64(state, 40);
    let l2_offset = read_u64(state, l1_offset) & 0x00ff_ffff_ffff_fe00;
    let l2_entry = (1u64 << 62) | compressed_host_offset;
    state.write(l2_offset + 8, &l2_entry.to_be_bytes());

    let image = open_image(state).await;
    let mut buf = [0u8; 512];

    // Check that it’s a compressed cluster
    let mapping = image.get_mapping(guest_offset, 512).await.unwrap();
    assert!(matches!(mapping.0, Mapping::Special { .. }));
    assert_eq!(mapping.1, 512);

    // And verify the content
    image.read(&mut buf[..], guest_offset).await.unwrap();
    assert!(buf == *data, "Compressed cluster not read back correctly");

    (image, compressed_host_offset)
}

/// Reading a compressed cluster while a concurrent write replaces it (freeing the compressed data)
/// must not return data from another cluster that has reused the freed host cluster.
#[tokio::test]
async fn test_compressed_read_vs_cow() {
    // The offset of the cluster where everything happens
    let guest_offset = 512;

    let state = Arc::new(MemStorageState::default());
    let data = [0x44u8; 512];
    let (image, compressed_host_offset) =
        create_compressed_image(&state, guest_offset, &data).await;
    let mut buf = [0u8; 512];

    // Start reading the compressed cluster, keeping the read of the compressed data pending
    state.hold_reads.store(true, Ordering::Relaxed);
    let mut read_buf = [0u8; 512];
    let mut read = Box::pin(image.read(&mut read_buf[..], guest_offset));
    assert!(crate::test::mem_storage::poll_once(read.as_mut()).is_pending());
    assert_eq!(*state.held_reads.lock(), [compressed_host_offset]);
    state.hold_reads.store(false, Ordering::Relaxed);

    // Concurrently overwrite guest cluster 1, replacing the compressed mapping, which frees the
    // compressed data’s host cluster
    let mut cow_write = Box::pin(image.write(&[0x55u8; 512][..], guest_offset));
    if crate::test::mem_storage::poll_once(cow_write.as_mut()).is_ready() {
        // The write did not wait for the read, so the freed host cluster can now be reused
        image.write(&[0x66u8; 512][..], 1024).await.unwrap();
        assert_eq!(host_offset(&image, 1024).await, compressed_host_offset);
    }

    // Let the read of the compressed data complete
    state.release_reads.store(true, Ordering::Relaxed);
    let Poll::Ready(result) = crate::test::mem_storage::poll_once(read.as_mut()) else {
        panic!("Read did not complete");
    };
    result.unwrap();
    drop(read);
    assert!(
        read_buf == data,
        "Expected {:#04x} bytes, read {:#04x} bytes",
        data[0],
        read_buf[0],
    );

    cow_write.await.unwrap();
    image.read(&mut buf[..], guest_offset).await.unwrap();
    assert!(buf == [0x55u8; 512], "COW write not visible");
}

/// If a concurrent write replaces a compressed cluster between `get_mapping()` returning a special
/// mapping and reading it via `readv_special()`, the read must return the new data (and not fail
/// because the range is no longer special).
#[tokio::test]
async fn test_compressed_read_vs_mapping_change() {
    let guest_offset = 512;

    let state = Arc::new(MemStorageState::default());
    let (image, _) = create_compressed_image(&state, guest_offset, &[0x44u8; 512]).await;

    // Partially overwrite guest cluster 1, so the compressed data must be COW-ed; keep that write
    // pending on a read while it holds the L2 table’s write lock (the first read it does while
    // holding it is for allocating the new cluster, i.e. loading a refblock)
    state.hold_reads.store(true, Ordering::Relaxed);
    let mut cow_write = Box::pin(image.write(&[0x55u8; 256][..], guest_offset));
    assert!(crate::test::mem_storage::poll_once(cow_write.as_mut()).is_pending());
    assert!(!state.held_reads.lock().is_empty());
    state.hold_reads.store(false, Ordering::Relaxed);

    // Start reading guest cluster 1: `get_mapping()` returns a special mapping, then reading it
    // must wait for the write
    let mut read_buf = [0u8; 512];
    let mut read = Box::pin(image.read(&mut read_buf[..], guest_offset));
    assert!(crate::test::mem_storage::poll_once(read.as_mut()).is_pending());

    // Let the write complete, replacing the compressed mapping
    state.release_reads.store(true, Ordering::Relaxed);
    let Poll::Ready(result) = crate::test::mem_storage::poll_once(cow_write.as_mut()) else {
        panic!("Write did not complete");
    };
    result.unwrap();

    // The read must see the new mapping and data
    read.await.unwrap();
    assert!(
        read_buf[..256] == [0x55u8; 256] && read_buf[256..] == [0x44u8; 256],
        "Expected new data (0x55, then 0x44), read {:#04x}, then {:#04x}",
        read_buf[0],
        read_buf[256],
    );
}

/// While a write that allocates a cluster is in flight, reads of that guest cluster must not see
/// what the cluster held before.
///
/// Here, the cluster had held another guest cluster’s data, was freed by a discard, and is reused.
#[tokio::test]
async fn test_read_during_write_to_reused_cluster() {
    let state = Arc::new(MemStorageState::default());
    let mut image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0, then free its host cluster
    image.write(&[0x11u8; 512][..], 0).await.unwrap();
    let freed_host_offset = host_offset(&image, 0).await;
    image.discard_to_zero(0, 512).await.unwrap();

    // Seed stale data after physical discard to check that the unmapped guest cannot see it.
    state.data.lock()[freed_host_offset as usize..freed_host_offset as usize + 512].fill(0x11);

    // The freed host cluster still holds the old data
    assert_eq!(state.data.lock()[freed_host_offset as usize], 0x11);

    // Fully overwrite guest cluster 1, which must reuse that host cluster for this test to be
    // meaningful, and keep the data write in flight
    *state.hold_range.lock() = Some(freed_host_offset..(freed_host_offset + 512));
    state.delay_writes.store(true, Ordering::Relaxed);
    let mut write = Box::pin(image.write(&[0x22u8; 512][..], 512));
    assert!(crate::test::mem_storage::poll_once(write.as_mut()).is_pending());
    assert_eq!(*state.delayed_writes.lock(), [freed_host_offset]);
    state.delay_writes.store(false, Ordering::Relaxed);
    *state.hold_range.lock() = None;

    // Until the write is done, guest cluster 1 must read as before, i.e. as zeroes
    let mut buf = [0xffu8; 512];
    image.read(&mut buf[..], 512).await.unwrap();
    assert!(
        buf == [0u8; 512],
        "Expected zeroes, read {:#04x} bytes",
        buf[0],
    );

    // Let the write complete; it must have landed in the reused host cluster, which is now also
    // what guest cluster 1 maps to
    state.release_writes.store(true, Ordering::Relaxed);
    let Poll::Ready(result) = crate::test::mem_storage::poll_once(write.as_mut()) else {
        panic!("Write did not complete");
    };
    result.unwrap();
    drop(write);

    assert_eq!(host_offset(&image, 512).await, freed_host_offset);
    let freed_host_offset = freed_host_offset as usize;
    {
        let data = state.data.lock();
        assert!(
            data[freed_host_offset..(freed_host_offset + 512)] == [0x22u8; 512],
            "Write did not land in the reused host cluster, its first byte is {:#04x}",
            data[freed_host_offset],
        );
    }

    // So reading guest cluster 1 must now show the new data
    let mut buf = [0u8; 512];
    image.read(&mut buf[..], 512).await.unwrap();
    assert!(
        buf == [0x22u8; 512],
        "Expected {:#04x} bytes, read {:#04x} bytes",
        0x22,
        buf[0],
    );
}

/// While a write to a zeroed cluster that has retained its allocation is in flight, reads of that
/// guest cluster must not see the data from before it was zeroed.
#[tokio::test]
async fn test_read_during_write_to_zeroed_cluster() {
    let state = Arc::new(MemStorageState::default());
    let image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0, then zero it (which retains the allocation)
    image.write(&[0x11u8; 512][..], 0).await.unwrap();
    let retained_host_offset = host_offset(&image, 0).await;
    image.write_zeroes(0, 512).await.unwrap();

    // The host cluster keeps its allocation and its old content; only its mapping reads as zeroes
    assert_eq!(state.data.lock()[retained_host_offset as usize], 0x11);

    // Fully overwrite guest cluster 0, which must reuse the retained host cluster for this test to
    // be meaningful, and keep the data write in flight
    *state.hold_range.lock() = Some(retained_host_offset..(retained_host_offset + 512));
    state.delay_writes.store(true, Ordering::Relaxed);
    let mut write = Box::pin(image.write(&[0x22u8; 512][..], 0));
    assert!(crate::test::mem_storage::poll_once(write.as_mut()).is_pending());
    assert_eq!(*state.delayed_writes.lock(), [retained_host_offset]);
    state.delay_writes.store(false, Ordering::Relaxed);
    *state.hold_range.lock() = None;

    // Until the write is done, guest cluster 0 must read as before, i.e. as zeroes
    let mut buf = [0xffu8; 512];
    image.read(&mut buf[..], 0).await.unwrap();
    assert!(
        buf == [0u8; 512],
        "Expected zeroes, read {:#04x} bytes",
        buf[0],
    );

    // Let the write complete; it must have landed in the retained host cluster, which guest
    // cluster 0 must still map to
    state.release_writes.store(true, Ordering::Relaxed);
    let Poll::Ready(result) = crate::test::mem_storage::poll_once(write.as_mut()) else {
        panic!("Write did not complete");
    };
    result.unwrap();
    drop(write);

    assert_eq!(host_offset(&image, 0).await, retained_host_offset);
    let retained_host_offset = retained_host_offset as usize;
    {
        let data = state.data.lock();
        assert!(
            data[retained_host_offset..(retained_host_offset + 512)] == [0x22u8; 512],
            "Write did not land in the retained host cluster, its first byte is {:#04x}",
            data[retained_host_offset],
        );
    }

    // So reading guest cluster 0 must now show the new data
    let mut buf = [0u8; 512];
    image.read(&mut buf[..], 0).await.unwrap();
    assert!(
        buf == [0x22u8; 512],
        "Expected {:#04x} bytes, read {:#04x} bytes",
        0x22,
        buf[0],
    );
}

/// Two concurrent writes into different parts of the same unallocated cluster must both end up in
/// it.
#[tokio::test]
async fn test_concurrent_writes_to_unallocated_cluster() {
    let state = Arc::new(MemStorageState::default());
    let mut image = create_image(&state, 64 * 1024).await;

    // Allocate guest cluster 0 (so the L2 table exists), and allocate and free a host cluster, so
    // we know where the next allocation will be
    image.write(&[0x11u8; 512][..], 0).await.unwrap();
    image.write(&[0x44u8; 512][..], 512).await.unwrap();
    let host = host_offset(&image, 512).await;
    image.discard_to_zero(512, 512).await.unwrap();

    // Write the first half of guest cluster 1, delaying the data write (but not the write that
    // fills the second half of the new cluster)
    *state.hold_range.lock() = Some(host..(host + 256));
    state.hold_writes.store(true, Ordering::Relaxed);
    let mut first = Box::pin(image.write(&[0x22u8; 256][..], 512));
    assert!(crate::test::mem_storage::poll_once(first.as_mut()).is_pending());
    assert_eq!(state.held.lock()[0].0, host);
    state.hold_writes.store(false, Ordering::Relaxed);
    *state.hold_range.lock() = None;

    // Concurrently write the second half; this may or may not wait for the first write
    let mut second = Box::pin(image.write(&[0x33u8; 256][..], 512 + 256));
    let second_done = match crate::test::mem_storage::poll_once(second.as_mut()) {
        Poll::Ready(result) => {
            result.unwrap();
            true
        }
        Poll::Pending => false,
    };

    // Let the first write complete
    state.release_held_writes();
    let Poll::Ready(result) = crate::test::mem_storage::poll_once(first.as_mut()) else {
        panic!("First write did not complete");
    };
    result.unwrap();
    if !second_done {
        second.await.unwrap();
    }

    let mut buf = [0u8; 512];
    image.read(&mut buf[..], 512).await.unwrap();
    assert!(
        buf[..256] == [0x22u8; 256] && buf[256..] == [0x33u8; 256],
        "Expected 0x22, then 0x33; read {:#04x}, then {:#04x}",
        buf[0],
        buf[256],
    );
}

/// A write with parallelization enabled that needs several separate allocations in the same L2
/// table must complete (and not wait for itself).
#[tokio::test]
async fn test_parallel_write_with_several_allocations() {
    let state = Arc::new(MemStorageState::default());
    let mut image = create_image(&state, 64 * 1024).await;
    image.set_async_write_parallelization(4);

    // Allocate guest clusters 0 through 3, and free the host clusters of 1 and 3, so the next two
    // allocations are not adjacent
    image.write(&[0x11u8; 4 * 512][..], 0).await.unwrap();
    let first_free = host_offset(&image, 512).await;
    let second_free = host_offset(&image, 3 * 512).await;
    assert_ne!(second_free, first_free + 512);
    image.discard_to_zero(512, 512).await.unwrap();
    image.discard_to_zero(3 * 512, 512).await.unwrap();

    // Write guest clusters 4 and 5 in a single request
    let mut write = Box::pin(image.write(&[0x55u8; 2 * 512][..], 4 * 512));
    let Poll::Ready(result) = crate::test::mem_storage::poll_once(write.as_mut()) else {
        panic!("Write did not complete");
    };
    result.unwrap();
    drop(write);

    // This test is only meaningful if that needed two separate allocations
    assert_eq!(host_offset(&image, 4 * 512).await, first_free);
    assert_eq!(host_offset(&image, 5 * 512).await, second_free);

    let mut buf = [0u8; 2 * 512];
    image.read(&mut buf[..], 4 * 512).await.unwrap();
    assert!(buf == [0x55u8; 2 * 512], "Data not read back correctly");
}
