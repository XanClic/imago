//! In-memory storage driver for tests.

use crate::io_buffers::{IoVector, IoVectorMut};
use crate::qcow2::Qcow2;
use crate::storage::drivers::CommonStorageHelper;
use crate::storage::{PreallocateMode, Storage};
use crate::{DenyImplicitOpenGate, FormatCreateBuilder, FormatDriverBuilder};
use parking_lot::Mutex;
use std::fmt::{self, Display, Formatter};
use std::io;
#[cfg(feature = "async")]
use std::mem;
#[cfg(feature = "async")]
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::{Context, Poll, Waker};

#[maybe_async::maybe_async]
pub(crate) async fn create_image(state: &Arc<MemStorageState>, size: u64) -> Qcow2<MemStorage> {
    Qcow2::<MemStorage>::create_builder(MemStorage::new(Arc::clone(state), false))
        .cluster_size(512)
        .size(size)
        .create_open(DenyImplicitOpenGate::default(), |image| {
            Ok(Qcow2::<MemStorage>::builder(image)
                .backing(None)
                .write(true))
        })
        .await
        .unwrap()
}

/// State shared between a [`MemStorage`] object and the test.
#[derive(Debug, Default)]
pub struct MemStorageState {
    /// Storage contents.
    pub data: Mutex<Vec<u8>>,

    /// While set, writes on `hold_range` are not performed immediately, but entered into `held`.
    #[cfg(feature = "async")]
    pub hold_writes: AtomicBool,

    /// Specifies which writes to hold; hold all writes if `None`.
    #[cfg(feature = "async")]
    pub hold_range: Mutex<Option<Range<u64>>>,

    /// Writes that have been issued, but not yet performed (offset and data).
    #[cfg(feature = "async")]
    pub held: Mutex<Vec<(u64, Vec<u8>)>>,

    /// While set, writes on `hold_range` do not complete until `release_writes` is set (their
    /// offsets are entered into `delayed_writes`).  Unlike writes held via `hold_writes`, they then
    /// complete normally.
    #[cfg(feature = "async")]
    pub delay_writes: AtomicBool,

    /// Offsets of writes that have been delayed.
    #[cfg(feature = "async")]
    pub delayed_writes: Mutex<Vec<u64>>,

    /// Allows delayed writes to complete.
    #[cfg(feature = "async")]
    pub release_writes: AtomicBool,

    /// Wakers of held writes, woken when they are released.
    #[cfg(feature = "async")]
    pub held_write_wakers: Mutex<Vec<Waker>>,

    /// If set, the next write fails (and its offset is entered into `failed`), independently of
    /// `hold_range`.
    pub fail_next_write: AtomicBool,

    /// Offsets of writes that have failed.
    pub failed: Mutex<Vec<u64>>,

    /// While set, reads do not complete until `release_reads` is set (their offsets are entered
    /// into `held_reads`).
    #[cfg(feature = "async")]
    pub hold_reads: AtomicBool,

    /// Offsets of reads that have been held.
    #[cfg(feature = "async")]
    pub held_reads: Mutex<Vec<u64>>,

    /// Allows held reads to complete (reading the data present at that point).
    #[cfg(feature = "async")]
    pub release_reads: AtomicBool,
}

impl MemStorageState {
    /// Whether a write of `len` bytes to `offset` is to be held.
    #[cfg(feature = "async")]
    fn should_hold(&self, offset: u64, len: u64) -> bool {
        self.hold_writes.load(Ordering::Relaxed) && self.in_hold_range(offset, len)
    }

    /// Whether a write of `len` bytes to `offset` is to be delayed.
    #[cfg(feature = "async")]
    fn should_delay(&self, offset: u64, len: u64) -> bool {
        self.delay_writes.load(Ordering::Relaxed) && self.in_hold_range(offset, len)
    }

    /// Whether a write of `len` bytes to `offset` intersects `hold_range`.
    #[cfg(feature = "async")]
    fn in_hold_range(&self, offset: u64, len: u64) -> bool {
        self.hold_range
            .lock()
            .as_ref()
            .is_none_or(|range| offset < range.end && offset + len > range.start)
    }

    /// Write `buf` to `offset`, growing the storage if necessary.
    pub fn write(&self, offset: u64, buf: &[u8]) {
        let start = offset as usize;
        let end = start + buf.len();
        let mut data = self.data.lock();
        if data.len() < end {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
    }

    /// Perform all held writes, even if their futures have been dropped.
    #[cfg(feature = "async")]
    pub fn complete_held_writes(&self) {
        let held = mem::take(&mut *self.held.lock());
        for (offset, data) in held {
            self.write(offset, &data);
        }
    }

    /// Let held writes (current and future ones) complete, performing them.
    ///
    /// Writes already performed by [`MemStorageState::complete_held_writes()`] are not performed
    /// again.
    #[cfg(feature = "async")]
    pub fn release_held_writes(&self) {
        let wakers = {
            let mut wakers = self.held_write_wakers.lock();
            self.release_writes.store(true, Ordering::Relaxed);
            mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }

    /// Check whether held writes have been released; if not, register to be woken on release.
    #[cfg(feature = "async")]
    fn poll_write_release(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut wakers = self.held_write_wakers.lock();
        if self.release_writes.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// In-memory storage.
///
/// Supports alignment requirements, injecting write errors, a fixed size, and (in async mode)
/// holding back writes, which then remain in flight when their futures are dropped, or complete
/// once released.
#[derive(Debug)]
pub struct MemStorage {
    /// State shared with the test.
    state: Arc<MemStorageState>,

    /// Memory and request alignment.
    align: usize,

    /// If set, resizing fails.
    fixed_size: bool,

    /// Helper for implementing [`Storage`].
    helper: CommonStorageHelper,
}

impl MemStorage {
    /// Create a storage object using the given shared state.
    pub fn new(state: Arc<MemStorageState>, fixed_size: bool) -> Self {
        MemStorage {
            state,
            align: 1,
            fixed_size,
            helper: Default::default(),
        }
    }

    /// Set the memory and request alignment.
    pub fn with_align(mut self, align: usize) -> Self {
        self.align = align;
        self
    }
}

impl Display for MemStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mem[align={}, fixed_size={:?}]",
            self.align, self.fixed_size
        )
    }
}

#[maybe_async::maybe_async(AFIT)]
impl Storage for MemStorage {
    fn mem_align(&self) -> usize {
        self.align
    }

    fn req_align(&self) -> usize {
        self.align
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.state.data.lock().len() as u64)
    }

    async unsafe fn pure_readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        unsafe { self.pure_readv_counted(bufv, offset).await }.map(|_| ())
    }

    async unsafe fn pure_readv_counted(
        &self,
        mut bufv: IoVectorMut<'_>,
        offset: u64,
    ) -> io::Result<Option<u64>> {
        #[cfg(feature = "async")]
        if self.state.hold_reads.load(Ordering::Relaxed) {
            self.state.held_reads.lock().push(offset);
            std::future::poll_fn(|_| {
                if self.state.release_reads.load(Ordering::Relaxed) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }

        let data = self.state.data.lock();
        let offset = offset as usize;
        let len = bufv.len() as usize;
        let mut bounce = vec![0u8; len];
        let copy_len = data.len().saturating_sub(offset).min(len);
        if copy_len > 0 {
            bounce[..copy_len].copy_from_slice(&data[offset..offset + copy_len]);
        }
        bufv.copy_from_slice(&bounce);
        Ok(Some(copy_len as u64))
    }

    async unsafe fn pure_writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        let mut bounce = vec![0u8; bufv.len() as usize];
        bufv.copy_into_slice(&mut bounce);

        if self.state.fail_next_write.swap(false, Ordering::Relaxed) {
            self.state.failed.lock().push(offset);
            return Err(io::Error::other("Injected write error"));
        }

        #[cfg(feature = "async")]
        if self.state.should_delay(offset, bounce.len() as u64) {
            self.state.delayed_writes.lock().push(offset);
            std::future::poll_fn(|_| {
                if self.state.release_writes.load(Ordering::Relaxed) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }

        #[cfg(feature = "async")]
        if self.state.should_hold(offset, bounce.len() as u64) {
            self.state.held.lock().push((offset, bounce.clone()));
            // Hold until released; if we are dropped before, the data will still be written
            // eventually by [`MemStorageState::complete_held_writes()`]
            std::future::poll_fn(|cx| self.state.poll_write_release(cx)).await;

            // Perform the write, unless `complete_held_writes()` already has
            let mut held = self.state.held.lock();
            if let Some(index) = held.iter().position(|(o, d)| *o == offset && *d == bounce) {
                held.remove(index);
                drop(held);
                self.state.write(offset, &bounce);
            }
            return Ok(());
        }

        self.state.write(offset, &bounce);
        Ok(())
    }

    async fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    async fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    async fn settle_writes(&self) -> io::Result<()> {
        #[cfg(feature = "async")]
        self.state.complete_held_writes();
        Ok(())
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        Ok(())
    }

    fn get_storage_helper(&self) -> &CommonStorageHelper {
        &self.helper
    }

    async fn resize(&self, new_size: u64, _prealloc_mode: PreallocateMode) -> io::Result<()> {
        if self.fixed_size {
            return Err(io::Error::other("Resize not supported"));
        }
        self.state.data.lock().resize(new_size as usize, 0);
        Ok(())
    }
}
