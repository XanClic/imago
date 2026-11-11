//! Regression for an allocation overtaking a concurrent refcount-cache flush.

use super::*;
use crate::test::mem_storage::{create_image, MemStorageState};
use std::sync::atomic::Ordering;
use std::task::Poll;

#[tokio::test]
async fn concurrent_flush_preserves_cluster_ownership() -> io::Result<()> {
    // The second case extends an allocated cluster, requiring a contiguous host allocation.
    for (write_offset, length) in [(1024, 512), (512, 1024)] {
        let state = Arc::new(MemStorageState::default());
        let qcow2 = create_image(&state, 32768).await;
        let rb_offset = state.read_u64(state.read_u64(48));
        let rb = qcow2
            .caches
            .rb_get_or_insert(HostCluster(rb_offset / 512))
            .await?;
        let image = FormatAccess::new(qcow2);
        image.write(&[0x11; 512][..], 0).await?;
        image.flush().await?;
        image.sync().await?;

        // Dirty the existing RB without creating a second L2 table.
        image.write(&[0x22; 512][..], 512).await?;
        // This lock only controls scheduling: pause the next normal write after its
        // dependency announcement, before its refcount increment.
        let rb_writer = rb.lock_write().await;
        let payload = vec![0x33; length];
        let mut write = Box::pin(image.write(&payload[..], write_offset));
        assert!(crate::test::mem_storage::poll_once(write.as_mut()).is_pending());

        *state.hold_range.lock() = Some(rb_offset..rb_offset + 512);
        state.hold_writes.store(true, Ordering::Relaxed);
        let mut flush = Box::pin(image.flush());
        assert!(crate::test::mem_storage::poll_once(flush.as_mut()).is_pending());
        assert_eq!(state.held.lock().len(), 1);
        state.hold_writes.store(false, Ordering::Relaxed);

        drop(rb_writer);
        let completed = match crate::test::mem_storage::poll_once(write.as_mut()) {
            Poll::Ready(result) => {
                result.unwrap();
                true
            }
            Poll::Pending => false,
        };
        state.release_held_writes();
        let Poll::Ready(result) = crate::test::mem_storage::poll_once(flush.as_mut()) else {
            panic!("flush did not complete after releasing the refblock write");
        };
        result.unwrap();
        drop(flush);
        if !completed {
            write.as_mut().await?;
        }
        drop(write);
        // No extra flush: it would hide the concurrent flush's missing refcount.
        image.sync().await?;
        assert!(state.held.lock().is_empty());

        let l1 = state.read_u64(40);
        let l2 = state.read_u64(l1) & 0x00ff_ffff_ffff_fe00;
        let original_host = state.read_u64(l2 + 2 * 8) & 0x00ff_ffff_ffff_fe00;
        if original_host == 0 {
            // The write completed after flush and its new mapping need not be durable yet.
            assert!(!completed);
            image.flush().await?;
            image.sync().await?;
            continue;
        }
        let refcount_at = rb_offset as usize + original_host as usize / 512 * 2;
        let stored_refcount = u16::from_be_bytes(
            state.data.lock()[refcount_at..refcount_at + 2]
                .try_into()
                .unwrap(),
        );
        assert_ne!(
            stored_refcount, 0,
            "concurrent flush published an unowned cluster"
        );
        image.flush().await?;
        image.sync().await?;
    }
    Ok(())
}
