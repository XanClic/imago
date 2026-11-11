//! Provides functionality for the L2 and refblock caches.

use super::*;
use crate::async_lru_cache::AsyncLruCacheBackend;
use tracing::trace;

/// I/O back-end for the L2 table cache.
struct L2CacheBackend<S: Storage> {
    /// Qcow2 metadata file.
    file: Arc<S>,

    /// Qcow2 header.
    header: Arc<Header>,

    /// Payload storage, updated when the image's dependencies are opened.
    data_file: Arc<parking_lot::RwLock<Arc<S>>>,
}

/// I/O back-end for the refblock cache.
struct RefBlockCacheBackend<S: Storage> {
    /// Qcow2 metadata file.
    file: Arc<S>,

    /// Qcow2 header.
    header: Arc<Header>,
}

impl<S: Storage> L2CacheBackend<S> {
    /// Create a new `L2CacheBackend` for the metadata and payload storage.
    pub fn new(
        file: Arc<S>,
        header: Arc<Header>,
        data_file: Arc<parking_lot::RwLock<Arc<S>>>,
    ) -> Self {
        L2CacheBackend {
            file,
            header,
            data_file,
        }
    }
}

impl<S: Storage> RefBlockCacheBackend<S> {
    /// Create a new `RefBlockCacheBackend`.
    ///
    /// `file` is the qcow2 metadata (image) file.
    pub fn new(file: Arc<S>, header: Arc<Header>) -> Self {
        RefBlockCacheBackend { file, header }
    }
}

#[maybe_async(AFIT)]
impl<S: Storage> AsyncLruCacheBackend for L2CacheBackend<S> {
    type Key = HostCluster;
    type Value = L2Table;

    async fn load(&self, l2_cluster: HostCluster) -> io::Result<L2Table> {
        trace!("Loading L2 table");
        self.header.ensure_healthy()?;
        let table = L2Table::load(
            self.file.as_ref(),
            &self.header,
            l2_cluster,
            self.header.l2_entries(),
        )
        .await?;
        self.header.ensure_healthy()?;
        Ok(table)
    }

    async fn flush(&self, l2_cluster: HostCluster, l2_table: &L2Table) -> io::Result<()> {
        trace!("Flushing L2 table");
        let data_file = self.data_file.read().clone();
        l2_table
            .write_if_modified(
                self.file.as_ref(),
                data_file.as_ref(),
                &self.header,
                l2_cluster,
            )
            .await
    }

    unsafe fn evict(&self, _l2_cluster: HostCluster, l2_table: L2Table) {
        trace!(
            "Evicting L2 table {}",
            l2_table.get_offset().unwrap_or(HostOffset(0))
        );
        l2_table.clear_modified();
    }
}

#[maybe_async(AFIT)]
impl<S: Storage> AsyncLruCacheBackend for RefBlockCacheBackend<S> {
    type Key = HostCluster;
    type Value = RefBlock;

    async fn load(&self, rb_cluster: HostCluster) -> io::Result<RefBlock> {
        self.header.ensure_healthy()?;
        let block = RefBlock::load(self.file.as_ref(), &self.header, rb_cluster).await?;
        self.header.ensure_healthy()?;
        Ok(block)
    }

    async fn flush(&self, rb_cluster: HostCluster, refblock: &RefBlock) -> io::Result<()> {
        refblock
            .write_if_modified(self.file.as_ref(), &self.header, rb_cluster)
            .await
    }

    unsafe fn evict(&self, _rb_cluster: HostCluster, refblock: RefBlock) {
        refblock.clear_modified();
    }
}

/// Current flush dependency direction between L2 and refblock caches
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CacheDependency {
    /// No dependency set yet
    #[default]
    None,
    /// Write refblock cache before writing anything from the L2 cache (during cluster allocation)
    L2DependsOnRb,
    /// Write L2 cache before writing anything from the refblock cache (during cluster freeing)
    RbDependsOnL2,
}

/// Qcow2 metadata caches with flush-ordering coordination.
///
/// When allocating clusters, we need to increment the refcounts (from 0 to 1) before writing the
/// L2 pointers.  Therefore, before anything is written from the L2 cache to disk, we need to flush
/// the refblock cache and make those writes durable in the backing storage.
///
/// When freeing clusters, we need to clear the L2 pointers before decrementing the refcounts (to
/// 0).  Therefore before anything is written from the refblock cache to disk, we need to flush the
/// L2 cache and make those writes durable in the backing storage.
///
/// This structure takes care to heed those dependencies (though the general qcow2 code still needs
/// to announce them by calling `.l2_depends_on_rb()` for allocations and `.rb_depends_on_l2()` for
/// freeing).
///
/// Announcing a dependency takes the `direction` lock and flushes the other cache while holding
/// it, so `direction` is the innermost lock: Never acquire a metadata table’s `writer_lock` (the
/// lock guarding modifications to its entries, e.g. on [`L2Table`]) while holding that direction
/// lock.
///
/// Conversely, note that for an allocating write, an L2 table is held write-locked across its data
/// I/O while its prepared data mapping is uncommitted, and allocating the clusters announces its
/// dependency (taking the `direction` lock) while that write lock is held.  Therefore, writing a
/// table back to disk must never take the table’s `writer_lock`, because doing so would deadlock
/// against the still-held lock.  It does take the table’s separate `writeback_lock` (see
/// [`L2Table::write_if_modified()`]) and briefly its payload snapshot lock. Neither acquires
/// `direction` or a metadata writer lock.
pub(super) struct MetadataCaches<S: Storage> {
    /// Backing storage for durability barriers between dependent metadata writes.
    file: Arc<S>,

    /// Shared with L2 writeback so eviction flushes the actual payload storage.
    data_file: Arc<parking_lot::RwLock<Arc<S>>>,

    /// L2 table cache
    l2: AsyncLruCache<HostCluster, L2Table, L2CacheBackend<S>>,

    /// Refblock cache
    rb: AsyncLruCache<HostCluster, RefBlock, RefBlockCacheBackend<S>>,

    /// Current dependency direction
    ///
    /// Wrapped methods hold a read guard for their entire execution,
    /// preventing the direction from changing mid-operation.  Direction
    /// switch methods take a write guard.
    direction: RwLock<CacheDependency>,
}

#[maybe_async]
impl<S: Storage> MetadataCaches<S> {
    /// Create metadata caches for the given file, with the given header.
    ///
    /// The L2 cache is going to hold `l2_entries` tables, the refblock cache will have
    /// `rb_entries` refcount blocks.
    pub fn new(file: &Arc<S>, header: &Arc<Header>, l2_entries: usize, rb_entries: usize) -> Self {
        let data_file = Arc::new(parking_lot::RwLock::new(Arc::clone(file)));
        let l2_backend =
            L2CacheBackend::new(Arc::clone(file), Arc::clone(header), Arc::clone(&data_file));
        let rb_backend = RefBlockCacheBackend::new(Arc::clone(file), Arc::clone(header));

        MetadataCaches {
            file: Arc::clone(file),
            data_file,
            l2: AsyncLruCache::new(l2_backend, l2_entries),
            rb: AsyncLruCache::new(rb_backend, rb_entries),
            direction: Default::default(),
        }
    }

    /// Update the payload storage used by cached L2 writebacks.
    pub fn set_data_file(&self, file: Option<Arc<S>>) {
        *self.data_file.write() = file.unwrap_or_else(|| Arc::clone(&self.file));
    }

    /// Make sure the refblock cache is flushed before the L2 cache.
    ///
    /// Use before allocating new clusters.
    pub async fn l2_depends_on_rb(&self) -> io::Result<()> {
        let mut dir = self.direction.write().await;
        if *dir == CacheDependency::RbDependsOnL2 {
            self.l2.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
        }
        *dir = CacheDependency::L2DependsOnRb;
        Ok(())
    }

    /// Make sure the L2 cache is flushed before the refblock cache.
    ///
    /// Use before freeing clusters.
    pub async fn rb_depends_on_l2(&self) -> io::Result<()> {
        let mut dir = self.direction.write().await;
        if *dir == CacheDependency::L2DependsOnRb {
            self.rb.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
        }
        *dir = CacheDependency::RbDependsOnL2;
        Ok(())
    }

    /// Persist removed mappings before reclaiming their underlying data.
    pub async fn flush_l2(&self) -> io::Result<()> {
        let direction = self.direction.read().await;
        if *direction == CacheDependency::L2DependsOnRb {
            self.rb.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
        }
        self.l2.flush().await?;
        self.file.flush().await?;
        self.file.sync().await
    }

    /// Flush both L2 and refblock cache to disk.
    pub async fn flush_all(&self) -> io::Result<()> {
        let dir = self.direction.read().await;
        if *dir == CacheDependency::L2DependsOnRb {
            self.rb.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
            self.l2.flush().await?;
        } else {
            self.l2.flush().await?;
            if *dir == CacheDependency::RbDependsOnL2 {
                self.file.flush().await?;
                self.file.sync().await?;
            }
            self.rb.flush().await?;
        }

        Ok(())
    }

    /// Retrieve an L2 table from the cache.
    ///
    /// See [`AsyncLruCache::get_or_insert()`] for details.
    pub async fn l2_get_or_insert(&self, cluster_index: HostCluster) -> io::Result<Arc<L2Table>> {
        let dir = self.direction.read().await;
        if let Some(l2) = self.l2.get_or_insert(cluster_index, false).await? {
            return Ok(l2);
        }

        if *dir == CacheDependency::L2DependsOnRb {
            self.rb.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
        }

        // `unwrap()` is safe: Will never return `Ok(None)` with `may_flush` set to true.
        let l2 = self.l2.get_or_insert(cluster_index, true).await?.unwrap();
        Ok(l2)
    }

    /// Force-insert an L2 table.
    ///
    /// The L2 table must have a cluster associated with it ([`L2Table::set_cluster()`]).
    ///
    /// See [`AsyncLruCache::insert()`] for details.
    pub async fn l2_insert(&self, table: Arc<L2Table>) -> io::Result<()> {
        let cluster = table
            .get_cluster()
            .ok_or_else(|| io::Error::other("L2 table does not have a cluster"))?;
        let dir = self.direction.read().await;
        if !self.l2.insert(cluster, Arc::clone(&table), false).await? {
            if *dir == CacheDependency::L2DependsOnRb {
                self.rb.flush().await?;
                self.file.flush().await?;
                self.file.sync().await?;
            }

            let inserted = self.l2.insert(cluster, table, true).await?;
            // Will always insert with `may_flush` set to true.
            assert!(inserted);
        }

        Ok(())
    }

    /// Invalidate the L2 cache.
    ///
    /// # Safety
    /// May cause image corruption, you must guarantee the on-disk state is consistent.
    pub async unsafe fn invalidate_l2(&self) -> io::Result<()> {
        unsafe { self.l2.invalidate() }.await
    }

    /// Retrieve a refblock from the cache.
    ///
    /// See [`AsyncLruCache::get_or_insert()`] for details.
    pub async fn rb_get_or_insert(&self, cluster_index: HostCluster) -> io::Result<Arc<RefBlock>> {
        let dir = self.direction.read().await;
        if let Some(rb) = self.rb.get_or_insert(cluster_index, false).await? {
            return Ok(rb);
        }

        if *dir == CacheDependency::RbDependsOnL2 {
            self.l2.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
        }

        // `unwrap()` is safe: Will never return `Ok(None)` with `may_flush` set to true.
        let rb = self.rb.get_or_insert(cluster_index, true).await?.unwrap();
        Ok(rb)
    }

    /// Force-insert a refblock.
    ///
    /// The refblock must have a cluster associated with it ([`RefBlock::set_cluster()`]).
    ///
    /// See [`AsyncLruCache::insert()`] for details.
    pub async fn rb_insert(&self, rb: Arc<RefBlock>) -> io::Result<()> {
        let cluster = rb
            .get_cluster()
            .ok_or_else(|| io::Error::other("Refblock does not have a cluster"))?;
        let dir = self.direction.read().await;
        if !self.rb.insert(cluster, Arc::clone(&rb), false).await? {
            if *dir == CacheDependency::RbDependsOnL2 {
                self.l2.flush().await?;
                self.file.flush().await?;
                self.file.sync().await?;
            }

            let inserted = self.rb.insert(cluster, rb, true).await?;
            // Will always insert with `may_flush` set to true.
            assert!(inserted);
        }

        Ok(())
    }

    /// Flush the refblock cache and make it durable before publishing metadata pointers.
    pub async fn flush_rb(&self) -> io::Result<()> {
        let dir = self.direction.read().await;
        if *dir == CacheDependency::RbDependsOnL2 {
            self.l2.flush().await?;
            self.file.flush().await?;
            self.file.sync().await?;
        }
        self.rb.flush().await?;
        self.file.flush().await?;
        self.file.sync().await
    }

    /// Invalidate the refblock cache.
    ///
    /// # Safety
    /// May cause image corruption, you must guarantee the on-disk state is consistent.
    pub async unsafe fn invalidate_rb(&self) -> io::Result<()> {
        unsafe { self.rb.invalidate() }.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::null::Null;

    fn make_test_caches() -> MetadataCaches<Null> {
        let null = Arc::new(Null::new(1 << 16));
        let header = Arc::new(Header::new(16, 1, None, None, None));
        MetadataCaches::new(&null, &header, 16, 16)
    }

    #[cfg(feature = "sync")]
    fn block_on<T>(v: T) -> T {
        v
    }

    #[cfg(feature = "async")]
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// Direction switching is idempotent and doesn't error.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_direction_switch() {
        let caches = make_test_caches();

        caches.l2_depends_on_rb().await.unwrap();
        caches.l2_depends_on_rb().await.unwrap();
        caches.rb_depends_on_l2().await.unwrap();
        caches.rb_depends_on_l2().await.unwrap();
        caches.l2_depends_on_rb().await.unwrap();
    }

    /// Cross-cache eviction must not deadlock.
    ///
    /// Fill both caches to capacity with l2→rb active.  Two threads
    /// concurrently trigger eviction on each cache.  The coordinator
    /// flushes deps before map.write(), so no cross-cache lock
    /// nesting occurs.
    #[test]
    fn test_cross_cache_eviction_no_deadlock() {
        use std::thread;

        let null = Arc::new(Null::new(1 << 20));
        let header = Arc::new(Header::new(16, 1, None, None, None));
        let caches = Arc::new(MetadataCaches::new(&null, &header, 1, 1));

        block_on(caches.l2_depends_on_rb()).unwrap();

        // Create entries with cluster set and modified cleared so
        // flush is a no-op (avoids needing real I/O on Null storage).
        let mut l2_entry = L2Table::new_cleared(&header);
        l2_entry.set_cluster(HostCluster(0));
        l2_entry.clear_modified();
        let mut rb_entry = RefBlock::new_cleared(null.as_ref(), &header).unwrap();
        rb_entry.set_cluster(HostCluster(0));
        rb_entry.clear_modified();

        block_on(caches.l2_insert(Arc::new(l2_entry))).unwrap();
        block_on(caches.rb_insert(Arc::new(rb_entry))).unwrap();

        let c1 = Arc::clone(&caches);
        let c2 = Arc::clone(&caches);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let bar1 = Arc::clone(&barrier);
        let bar2 = Arc::clone(&barrier);

        // Thread 1: insert new l2 entry (evicts old one, dep-flushes rb)
        let header1 = Arc::clone(&header);
        let t1 = thread::spawn(move || {
            let mut entry = L2Table::new_cleared(&header1);
            entry.set_cluster(HostCluster(1));
            entry.clear_modified();
            bar1.wait();
            block_on(c1.l2_insert(Arc::new(entry))).unwrap();
        });

        // Thread 2: insert new rb entry (evicts old one)
        let header2 = Arc::clone(&header);
        let null2 = Arc::new(Null::new(1 << 20));
        let t2 = thread::spawn(move || {
            let mut entry = RefBlock::new_cleared(null2.as_ref(), &header2).unwrap();
            entry.set_cluster(HostCluster(1));
            entry.clear_modified();
            bar2.wait();
            block_on(c2.rb_insert(Arc::new(entry))).unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    /// Concurrent direction switching must not deadlock or corrupt
    /// the direction state.
    #[test]
    fn test_concurrent_direction_switch() {
        use std::thread;

        let caches = Arc::new(make_test_caches());

        let c1 = Arc::clone(&caches);
        let c2 = Arc::clone(&caches);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let bar1 = Arc::clone(&barrier);
        let bar2 = Arc::clone(&barrier);

        let t1 = thread::spawn(move || {
            bar1.wait();
            block_on(c1.l2_depends_on_rb()).unwrap();
        });

        let t2 = thread::spawn(move || {
            bar2.wait();
            block_on(c2.rb_depends_on_l2()).unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    /// Direction switch must flush the opposing cache's dirty entries.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_direction_switch_flushes_opposing() {
        let null = Arc::new(Null::new(1 << 20));
        let header = Arc::new(Header::new(16, 1, None, None, None));
        let caches = MetadataCaches::new(&null, &header, 16, 16);

        // Insert a dirty rb entry
        let mut rb_entry = RefBlock::new_cleared(null.as_ref(), &header).unwrap();
        rb_entry.set_cluster(HostCluster(0));
        // Leave modified = true (new_cleared sets it)
        let rb_arc = Arc::new(rb_entry);
        caches.rb_insert(Arc::clone(&rb_arc)).await.unwrap();

        assert!(rb_arc.is_modified());

        // Set l2→rb, then switch to rb→l2 — must flush rb
        caches.l2_depends_on_rb().await.unwrap();
        caches.rb_depends_on_l2().await.unwrap();

        // rb was flushed during the direction switch
        assert!(!rb_arc.is_modified());
    }

    /// Dep cache must be flushed before the main cache evicts.
    ///
    /// With l2→rb active, inserting into a full l2 cache must flush
    /// rb (the dependency) before evicting the l2 entry.
    #[maybe_async::test(feature = "sync", async(feature = "async", tokio::test))]
    async fn test_dep_flushed_before_eviction() {
        let null = Arc::new(Null::new(1 << 20));
        let header = Arc::new(Header::new(16, 1, None, None, None));
        let caches = MetadataCaches::new(&null, &header, 1, 16);

        caches.l2_depends_on_rb().await.unwrap();

        // Insert a dirty rb entry
        let mut rb_entry = RefBlock::new_cleared(null.as_ref(), &header).unwrap();
        rb_entry.set_cluster(HostCluster(0));
        let rb_arc = Arc::new(rb_entry);
        caches.rb_insert(Arc::clone(&rb_arc)).await.unwrap();

        // Fill l2 cache (size 1)
        let mut l2_entry = L2Table::new_cleared(&header);
        l2_entry.set_cluster(HostCluster(0));
        l2_entry.clear_modified();
        caches.l2_insert(Arc::new(l2_entry)).await.unwrap();

        // Insert another l2 entry — triggers eviction.
        // The coordinator must flush rb (dep) before the l2 eviction.
        let mut l2_entry2 = L2Table::new_cleared(&header);
        l2_entry2.set_cluster(HostCluster(1));
        l2_entry2.clear_modified();
        caches.l2_insert(Arc::new(l2_entry2)).await.unwrap();

        // rb must have been flushed by the dep flush
        assert!(
            !rb_arc.is_modified(),
            "rb entry must be flushed before l2 eviction"
        );
    }
}
