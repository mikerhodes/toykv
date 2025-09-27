use compaction::SimpleCompactionPolicy;
use memtables::Memtables;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::{ops::Bound, path::Path};

use error::ToyKVError;
use kvrecord::KVValue;
use merge_iterator2::MergeIterator;
use table::SSTables;

mod block;
mod blockiterator;
mod compaction;
mod concat_iterator;
pub mod error;
mod kvrecord;
mod memtable;
mod memtables;
mod merge_iterator2;
mod table;
mod wal;
mod walindex;

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum WALSync {
    Full,
    Off,
}

#[derive(Default)]
pub struct ToyKVMetrics {
    pub sst_flushes: AtomicU64,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub compacts: AtomicU64,
}

pub struct ToyKV {
    /// d is the folder that the KV store owns.
    pub metrics: ToyKVMetrics,
    state: Arc<RwLock<ToyKVState>>,
}

/// ToyKVState is the modifiable state of the database.
struct ToyKVState {
    /// sstables are the on-disk primary database
    /// storage.
    sstables: SSTables,

    /// memtables are the in memory tables
    memtables: Memtables,

    /// Protect against running more than one
    /// compaction from different threads.
    compacting: bool,
}

struct ToyKVOptions {
    wal_sync: WALSync,
    wal_write_threshold: u64,
    target_sstable_size_bytes: u64,
}

pub struct ToyKVBuilder {
    options: ToyKVOptions,
}

impl Default for ToyKVOptions {
    fn default() -> Self {
        Self {
            wal_sync: WALSync::Full,
            wal_write_threshold: 250_000, // At 1kb kv pairs, 256MB
            target_sstable_size_bytes: 256 * 1024 * 1024, // bytes
        }
    }
}

impl ToyKVBuilder {
    pub fn new() -> ToyKVBuilder {
        ToyKVBuilder {
            options: ToyKVOptions::default(),
        }
    }

    pub fn wal_sync(mut self, sync: WALSync) -> Self {
        self.options.wal_sync = sync;
        self
    }

    /// How many writes to a WAL before we SSTable it.
    pub fn wal_write_threshold(mut self, max: u64) -> Self {
        self.options.wal_write_threshold = max;
        self
    }

    pub fn target_sstable_size_bytes(mut self, size: u64) -> Self {
        self.options.target_sstable_size_bytes = size;
        self
    }

    pub fn open(self, d: &Path) -> Result<ToyKV, ToyKVError> {
        if !d.is_dir() {
            return Err(ToyKVError::DataDirMissing);
        }

        let memtables = Memtables::new(
            d.to_path_buf(),
            self.options.wal_sync,
            self.options.wal_write_threshold,
            self.options.target_sstable_size_bytes,
        )?;
        let sstables = table::SSTables::new(d)?;
        Ok(ToyKV {
            state: Arc::new(RwLock::new(ToyKVState {
                memtables,
                sstables,
                compacting: false,
            })),
            metrics: Default::default(),
        })
    }
}

/// Open a database using default settings.
pub fn open(d: &Path) -> Result<ToyKV, ToyKVError> {
    ToyKVBuilder::new().open(d)
}

const MAX_KEY_SIZE: usize = 10_240; // 10kb
const MAX_VALUE_SIZE: usize = 102_400; // 100kb

impl ToyKV {
    /// Set key k to v.
    pub fn set<S, T>(&self, k: S, v: T) -> Result<(), ToyKVError>
    where
        S: Into<Vec<u8>>,
        T: Into<Vec<u8>>,
    {
        let k: Vec<u8> = k.into();
        let v: Vec<u8> = v.into();
        if k.len() == 0 {
            return Err(ToyKVError::KeyEmpty);
        }
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }
        if v.len() == 0 {
            return Err(ToyKVError::ValueEmpty);
        }
        if v.len() > MAX_VALUE_SIZE {
            return Err(ToyKVError::ValueTooLarge);
        }
        self.write(k, kvrecord::KVValue::Some(v))
    }

    /// Delete the stored value for k.
    pub fn delete<S>(&self, k: S) -> Result<(), ToyKVError>
    where
        S: Into<Vec<u8>>,
    {
        let k: Vec<u8> = k.into();
        if k.len() == 0 {
            return Err(ToyKVError::KeyEmpty);
        }
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }
        self.write(k, KVValue::Deleted)
    }

    /// Internal method to write a KVValue to WAL and memtable
    fn write(&self, k: Vec<u8>, v: KVValue) -> Result<(), ToyKVError> {
        let mut state = self.state.write().unwrap();
        // Save which metric to implement after successful
        let metric = match v {
            KVValue::Deleted => &self.metrics.deletes,
            KVValue::Some(_) => &self.metrics.writes,
        };
        state.memtables.write(k, v)?;
        metric.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flush the oldest memtable to disk and discard it.
    /// Periodically call this from a background thread,
    /// or when set or delete is rejected with ToyKVError::NeedFlush.
    pub fn flush_oldest_memtable(&self) -> Result<(), ToyKVError> {
        // TODO copy pattern from compact to allow the write out of
        // the memtable to be done outside the locks.
        let mut state = self.state.write().unwrap();
        if state.memtables.can_flush() {
            // TODO
            // Later, add thread that periodically checks
            // for frozen memtables and flushes; meaning the
            // rejected write is backpressure.
            // Add vec of frozen tables, so user can tune
            // for the write rate vs. memory used?
            let w = state.sstables.build_sstable_writer();
            let (r, id) = state.memtables.write_oldest_memtable(w)?;
            state
                .sstables
                .commit_new_sstable(r)
                .expect("Error committing new sstable; restart for safety.");
            state
                .memtables
                .drop_memtable(id)
                .expect("Error dropping old memtable; restart for safety.");
            self.metrics.sst_flushes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    // Attempt to run a compaction on the stored data. The exact
    // compaction carried out is opaque to the caller.
    pub fn compact(&mut self) -> Result<(), ToyKVError> {
        // v2 compaction just compacts all existing sstables
        // in L0 into a single L0 table. This is implemented
        // with SimpleCompactionPolicy.
        let policy = SimpleCompactionPolicy::new();

        // Guard ensures only one compaction can happen at a time.
        let _guard = CompactionGuard::acquire(self.state.clone())?;

        // Get a table compaction task from the sstables structure.
        let c_task = {
            let state = self.state.read().unwrap();
            state.sstables.build_compaction_task_v2(&policy)?
        };

        // Compaction task is safe to run outside state lock
        let c_result = match c_task {
            Some(c_task) => c_task.compact_v2()?,
            None => return Ok(()), // No compaction needed
        };

        // Commit the compacted table while holding write lock.
        {
            let mut state = self.state.write().unwrap();
            state.sstables.try_commit_compaction_v3(policy, c_result)?;
        }

        self.metrics.compacts.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Retrieve the number of live sstables (ie, those in use, not
    /// awaiting GC).
    pub fn live_sstables(&self) -> usize {
        let state = self.state.read().unwrap();
        state.sstables.len()
    }

    /// Get the value for k.
    pub fn get<S>(&self, k: S) -> Result<Option<Vec<u8>>, ToyKVError>
    where
        S: Into<Vec<u8>>,
    {
        let k: Vec<u8> = k.into();
        if k.len() == 0 {
            return Err(ToyKVError::KeyEmpty);
        }
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }

        let r = {
            let state = self.state.read().unwrap();
            let r = state.memtables.get(&k);
            match r {
                Some(r) => Some(r),
                None => state.sstables.get(&k)?,
            }
        };
        // This is the moment where we consume the KVValue::Deleted
        // to hide it from the caller.
        let r = match r {
            None => None,
            Some(kvv) => match kvv {
                KVValue::Deleted => None,
                KVValue::Some(v) => Some(v),
            },
        };

        self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        Ok(r)
    }
    /// Perform a graceful shutdown.
    pub fn shutdown(&mut self) {}

    // /// Immediately terminate (for use during testing, "pretend to crash")
    // pub(crate) fn terminate(&mut self) {}

    // scan has a lifetime parameter to ensure that the resulting
    // iterator cannot outlive the toykv instance.
    pub fn scan<'a>(
        &'a self,
        start_key: Option<&'a [u8]>,
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<KVIterator, ToyKVError> {
        let lower_bound = match start_key {
            None => Bound::Unbounded,
            Some(k) => Bound::Included(k.to_vec()),
        };

        let state = self.state.read().unwrap();
        let mt_iters = state.memtables.iters(lower_bound, upper_bound.clone());
        let sst_iter = state.sstables.iters(start_key, upper_bound.clone())?;
        drop(state);

        let mut m = MergeIterator::new();
        for t in mt_iters.into_iter() {
            m.add_iterator(t);
        }
        m.add_iterator(sst_iter);

        Ok(KVIterator { i: m })
    }
}

pub struct KV {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct KVIterator {
    i: MergeIterator,
}

impl Iterator for KVIterator {
    type Item = Result<KV, ToyKVError>;

    fn next(&mut self) -> Option<Self::Item> {
        // This is an external API, so skip deleted items.
        loop {
            match self.i.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(kvr)) => match kvr.value {
                    KVValue::Deleted => continue,
                    KVValue::Some(x) => {
                        return Some(Ok(KV {
                            key: kvr.key,
                            value: x,
                        }))
                    }
                },
            }
        }
    }
}

/// CompactionGuard uses the compacting flag on state to ensure
/// only one compaction can happen at a time. Dropping the
/// guard allows a new compaction to start.
struct CompactionGuard {
    state: Arc<RwLock<ToyKVState>>,
    active: bool,
}

impl CompactionGuard {
    /// If returns Ok, compaction can go ahead. Otherwise, there
    /// is an already ongoing compaction.
    fn acquire(state: Arc<RwLock<ToyKVState>>) -> Result<Self, ToyKVError> {
        // Test and set compacting flag while locked
        {
            let mut s = state.write().unwrap();
            if s.compacting {
                return Err(ToyKVError::CompactionAlreadyRunning);
            }
            s.compacting = true;
        }

        Ok(CompactionGuard {
            state,
            active: true,
        })
    }

    // Allow early release
    fn release(&mut self) {
        if self.active {
            if let Ok(mut state) = self.state.write() {
                state.compacting = false;
            }
            self.active = false;
        }
    }
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        self.release();
    }
}
