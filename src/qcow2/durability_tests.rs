//! Model volatile host writes independently from writes made durable by sync.

use super::*;
use crate::test::mem_storage::{create_image, MemStorage, MemStorageState, StorageEvent};
use std::sync::atomic::Ordering;

#[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
async fn l2_eviction_retries_payload_dependency() -> io::Result<()> {
    for failure in ["sync", "write"] {
        let metadata = Arc::new(MemStorageState::default());
        let payload = Arc::new(MemStorageState::default());
        let mut qcow2 =
            Qcow2::<MemStorage>::create_builder(MemStorage::new(Arc::clone(&metadata), false))
                .size(65536)
                .cluster_size(512)
                .data_file(
                    "payload".into(),
                    MemStorage::new(Arc::clone(&payload), false),
                )
                .create_open(crate::DenyImplicitOpenGate::default(), |storage| {
                    Ok(Qcow2::<MemStorage>::builder(storage).write(true))
                })
                .await?;
        let l2 = qcow2.ensure_l2(GuestOffset(0)).await?;
        let l2_offset = l2.get_offset().unwrap().0;
        drop(l2);
        qcow2.ensure_l2(GuestOffset(32768)).await?;
        qcow2.flush().await?;
        qcow2.sync().await?;
        qcow2.caches = Arc::new(MetadataCaches::new(&qcow2.metadata, &qcow2.header, 1, 32));
        qcow2.caches.set_data_file(qcow2.storage.clone());
        qcow2.allocator = Some(Mutex::new(
            Allocator::new(
                Arc::clone(&qcow2.metadata),
                Arc::clone(&qcow2.header),
                Arc::clone(&qcow2.caches),
                Arc::clone(&qcow2.clusters_to_free),
            )
            .await?,
        ));
        let image = FormatAccess::new(qcow2);
        *payload.events.lock() = Some(Vec::new());
        image.write(&[0x5a; 512][..], 0).await?;
        image.write(&[0x6b; 512][..], 512).await?;
        assert!(!payload
            .events
            .lock()
            .as_ref()
            .unwrap()
            .iter()
            .any(|event| matches!(event, StorageEvent::Sync)));

        if failure == "sync" {
            payload.fail_next_sync.store(true, Ordering::Relaxed);
        } else {
            *metadata.fail_write_offset.lock() = Some(l2_offset);
        }
        let mut actual = [0; 512];
        let result = image.read(&mut actual[..], 32768).await;
        assert!(result.is_err());
        assert_eq!(
            payload
                .events
                .lock()
                .as_ref()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, StorageEvent::Write(..)))
                .count(),
            2,
            "eviction must flush both buffered payload writes"
        );
        assert_eq!(
            &metadata.data.lock()[l2_offset as usize..l2_offset as usize + 16],
            &[0; 16]
        );
        image.read(&mut actual[..], 0).await?;
        assert_eq!(actual, [0x5a; 512]);

        *payload.events.lock() = Some(Vec::new());
        image.read(&mut actual[..], 32768).await?;
        assert_eq!(actual, [0; 512]);
        let events = payload.events.lock().take().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, StorageEvent::Sync))
                .count(),
            1,
            "eviction retry must retain and batch the payload dependency"
        );
        assert_ne!(
            &metadata.data.lock()[l2_offset as usize..l2_offset as usize + 8],
            &[0; 8]
        );
        image.read(&mut actual[..], 512).await?;
        assert_eq!(actual, [0x6b; 512]);
        image.flush().await?;
        image.sync().await?;
    }
    Ok(())
}

fn load_u64(data: &[u8], offset: usize) -> u64 {
    data.get(offset..offset + 8)
        .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
        .unwrap_or(0)
}

fn assert_owned_clusters(data: &[u8], event_index: usize) {
    let cluster_size = 512;
    let l1 = load_u64(data, 40) as usize;
    let rt = load_u64(data, 48) as usize;
    let entries = u32::from_be_bytes(data[36..40].try_into().unwrap()) as usize;
    let owned = |offset: usize| {
        let cluster = offset / cluster_size;
        let rb = load_u64(data, rt + cluster / 256 * 8) as usize;
        let entry = rb + cluster % 256 * 2;
        let refcount = data
            .get(entry..entry + 2)
            .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
            .unwrap_or(0);
        assert!(
            rb != 0 && refcount != 0,
            "unowned cluster at {offset:#x} after storage event {event_index}"
        );
    };
    let rt_clusters = u32::from_be_bytes(data[56..60].try_into().unwrap()) as usize;
    for (offset, length) in [
        (0, cluster_size),
        (rt, rt_clusters * cluster_size),
        (l1, entries * 8),
    ] {
        for cluster in (offset..offset + length).step_by(cluster_size) {
            owned(cluster);
        }
    }
    for index in 0..rt_clusters * cluster_size / 8 {
        let rb = load_u64(data, rt + index * 8) as usize;
        if rb != 0 {
            owned(rb);
        }
    }
    for index in 0..entries {
        let l2 = (load_u64(data, l1 + index * 8) & 0x00ff_ffff_ffff_fe00) as usize;
        if l2 == 0 {
            continue;
        }
        owned(l2);
        for index in 0..cluster_size / 8 {
            let entry = load_u64(data, l2 + index * 8);
            let offset = (entry & 0x00ff_ffff_ffff_fe00) as usize;
            if offset != 0 {
                owned(offset);
            }
        }
    }
}

fn apply_write(data: &mut Vec<u8>, offset: u64, payload: &[u8]) {
    let start = offset as usize;
    data.resize(data.len().max(start + payload.len()), 0);
    data[start..start + payload.len()].copy_from_slice(payload);
}

fn check_power_cuts(mut durable: Vec<u8>, events: Vec<StorageEvent>) {
    let mut pending = Vec::new();
    for (index, event) in events.into_iter().enumerate() {
        match event {
            StorageEvent::Write(offset, payload) => {
                let mut crash = durable.clone();
                apply_write(&mut crash, offset, &payload);
                assert_owned_clusters(&crash, index);
                pending.push((offset, payload));
            }
            StorageEvent::Sync => {
                for (offset, payload) in pending.drain(..) {
                    apply_write(&mut durable, offset, &payload);
                }
                assert_owned_clusters(&durable, index);
            }
        }
    }
}

#[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
async fn metadata_dependencies_survive_reordered_host_writeback() -> io::Result<()> {
    let state = Arc::new(MemStorageState::default());
    let mut image = create_image(&state, 32768).await;
    image.flush().await?;
    image.sync().await?;
    // Force both caches to evict dirty tables during the large allocation below.
    image.caches = Arc::new(MetadataCaches::new(&image.metadata, &image.header, 2, 2));
    image.allocator = Some(Mutex::new(
        Allocator::new(
            Arc::clone(&image.metadata),
            Arc::clone(&image.header),
            Arc::clone(&image.caches),
            Arc::clone(&image.clusters_to_free),
        )
        .await?,
    ));
    let baseline = state.data.lock().clone();
    *state.events.lock() = Some(Vec::new());

    let mut access = FormatAccess::new(image);
    access.write(&[0x5a; 512][..], 0).await?;
    access.flush().await?;
    access.sync().await?;
    // Grow across L1 and refcount table boundaries, free clusters, and reuse them.
    let size = 9 * 1024 * 1024;
    access.resize_grow(size, PreallocateMode::None).await?;
    access
        .write(vec![0x6b; size as usize].as_slice(), 0)
        .await?;
    access.flush().await?;
    access.sync().await?;
    access.discard_to_zero(0, size).await?;
    access.resize_shrink(32768).await?;
    access.write(&[0x7c; 512][..], 0).await?;
    access.flush().await?;
    access.sync().await?;
    let events = state.events.lock().take().unwrap();
    check_power_cuts(baseline, events);
    Ok(())
}
