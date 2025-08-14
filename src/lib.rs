use memtable::Memtable;
use std::iter::repeat_with;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::{io::Error, ops::Bound, path::Path};
use std::{mem, thread};
use walindex::WALIndex;

use error::ToyKVError;
use kvrecord::KVValue;
use merge_iterator2::MergeIterator;
use table::SSTables;

mod block;
mod blockiterator;
pub mod error;
mod kvrecord;
mod memtable;
mod merge_iterator;
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
}

pub struct ToyKV {
    /// d is the folder that the KV store owns.
    d: PathBuf,
    pub metrics: ToyKVMetrics,
    wal_write_threshold: u64,
    wal_sync: WALSync,
    state: Arc<RwLock<ToyKVState>>,
}

/// ToyKVState is the modifiable state of the database.
struct ToyKVState {
    wal_index: WALIndex,

    /// active_memtable stores new writes before they
    /// are moved to sstables. They have a WAL for
    /// durability.
    active_memtable: Memtable,
    /// frozen_memtable exists to be written to disk
    /// in the background, such that we can still
    /// accept writes to the active memtable.
    frozen_memtable: Option<Arc<Memtable>>,
    /// sstables are the on-disk primary database
    /// storage.
    sstables: SSTables,
    /// Set to true while frozen_memtable is being
    /// written
    flushing: (Arc<Mutex<bool>>, Arc<Condvar>),
}

struct ToyKVOptions {
    wal_sync: WALSync,
    wal_write_threshold: u64,
    // max_sstable_size: u64,
}

pub struct ToyKVBuilder {
    options: ToyKVOptions,
}

impl Default for ToyKVOptions {
    fn default() -> Self {
        Self {
            wal_sync: WALSync::Full,
            wal_write_threshold: 250_000, // At 1kb kv pairs, 256MB
                                          // max_sstable_size: 16 * 1024 * 1024,
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

    // pub fn max_sstable_size(mut self, size: usize) -> Self {
    //     self.options.max_sstable_size = size;
    //     self
    // }

    pub fn open(self, d: &Path) -> Result<ToyKV, ToyKVError> {
        if !d.is_dir() {
            return Err(ToyKVError::DataDirMissing);
        }

        let wal_index_path = d.join("wal_index.json");
        let mut wal_index = WALIndex::open(wal_index_path)?;

        // If there is a WAL on disk, load the active memtable
        // from it, otherwise initialise a memtable with a fresh
        // WAL file on disk.
        let active_wal_path = match wal_index.active_wal() {
            None => new_wal_path(d),
            Some(x) => x,
        };
        wal_index.set_active_wal(&active_wal_path)?;
        println!("builder: load active memtable (if any)");
        let active_memtable =
            Memtable::new(active_wal_path, self.options.wal_sync)?;

        // Unlike active memtable, we don't need to have a frozen
        // memtable unless we exited running before we managed
        // to save the frozen memtable to disk. Mostly the
        // frozen memtable is transient, as it's saved to disk
        // almost immediately.
        println!("builder: load frozen memtable (if any)");
        let frozen_memtable = match wal_index.frozen_wal() {
            None => None,
            Some(x) => Some(Arc::new(Memtable::new(x, self.options.wal_sync)?)),
        };

        let sstables = table::SSTables::new(d)?;
        Ok(ToyKV {
            d: d.to_path_buf(),
            state: Arc::new(RwLock::new(ToyKVState {
                wal_index,
                active_memtable,
                frozen_memtable,
                sstables,
                flushing: (
                    Arc::new(Mutex::new(false)),
                    Arc::new(Condvar::new()),
                ),
            })),
            metrics: Default::default(),
            wal_write_threshold: self.options.wal_write_threshold,
            wal_sync: self.options.wal_sync,
        })
    }
}

fn new_wal_path(dir: &Path) -> PathBuf {
    let s: String = repeat_with(fastrand::alphanumeric).take(16).collect();
    dir.join(format!("{}.wal", s))
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
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
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
        self.metrics.deletes.fetch_add(1, Ordering::Relaxed);
        self.write(k, KVValue::Deleted)
    }

    /// Internal method to write a KVValue to WAL and memtable
    fn write(&self, k: Vec<u8>, v: KVValue) -> Result<(), ToyKVError> {
        let (flush_mutex, flush_condvar, wal_writes) = {
            let guard = self.state.read().unwrap();
            (
                guard.flushing.0.clone(),
                guard.flushing.1.clone(),
                guard.active_memtable.wal_writes(),
            )
        };

        // Maintain a lock on the database during writing
        if wal_writes >= self.wal_write_threshold {
            // Block while any existing flush completes
            {
                let mut flushing = flush_mutex.lock().unwrap();
                while *flushing {
                    println!("waiting for flushing");
                    flushing = flush_condvar.wait(flushing).unwrap();
                }
            }

            let mut w_guard = self.state.write().unwrap();

            // If there are several write threads trying to write during
            // a flush, they will all be woken up and race for the write
            // lock. The first will freeze the active memtable and kick
            // off a new async flush, if so, we should not start a new one,
            // as we would be flushing a new active memtable rather than
            // a full one.
            if *flush_mutex.lock().unwrap() {
                // No need to flush the memtable if already flushing
                false
            } else {
                *flush_mutex.lock().unwrap() = true;

                // Create a new active memtable
                let wal_path = new_wal_path(&self.d);
                w_guard.wal_index.set_active_wal(&wal_path)?;
                let new_memtable = Memtable::new(wal_path, self.wal_sync)?;
                // Set the new memtable active
                let old_active_memtable =
                    mem::replace(&mut w_guard.active_memtable, new_memtable);
                // Set the previous active memtable to the frozen one
                w_guard
                    .wal_index
                    .set_frozen_wal(&old_active_memtable.wal_path())?;
                w_guard.frozen_memtable = Some(Arc::new(old_active_memtable));
                // Make write visible via memtable now it's persisted to WAL
                w_guard.active_memtable.write(k, v)?;

                // We need to flush the frozen memtable
                let new_thread_state = self.state.clone();
                let _handler = thread::spawn(move || {
                    ToyKV::flush_frozen_memtable(new_thread_state);
                });
                // _handler.join().expect("interrupted");
                // TODO join on shutdown for safety
                // this might be much easier with a single background
                // worker thread that we can join. It can have the Arc of
                // the state all the time. We can use a channel to push
                // work?
                // self flushing_handler = handler then join it in shutdown()
                self.metrics.sst_flushes.fetch_add(1, Ordering::Relaxed);
                true
            }
        } else {
            // Just need to make visibile in the active_memtable
            let mut w_guard = self.state.write().unwrap();
            w_guard.active_memtable.write(k, v)?;
            false // no need to flush
        };

        Ok(())
    }

    /// Flush the frozen_memtable; safe to call from a separate thread as
    /// errors will panic; `state` will be poisoned if there is an error
    /// committing the new memtable to indexes.
    fn flush_frozen_memtable(state: Arc<RwLock<ToyKVState>>) {
        // I wonder if a given call to this should be given
        // a specific memtable to flush. It feels a little
        // slapdash right now, or at least hard to be sure
        // everything interleaves correctly.
        //
        // Grab a read lock and extract everything from the state
        // that we need.
        let (flush_mutex, flush_condvar, w, frozen_memtable) = {
            let guard = state.read().unwrap();
            (
                guard.flushing.0.clone(),
                guard.flushing.1.clone(),
                guard.sstables.build_sstable_writer(),
                match &guard.frozen_memtable {
                    None => None,
                    Some(fm) => Some(fm.clone()),
                },
            )
        };

        if let Some(fm) = frozen_memtable {
            // Now write out the memtable in the thread.
            // If this fails, it's safe to retry later,
            // so we don't need to poison the state RwLock.
            match w.write(fm.iter()) {
                Ok(r) => {
                    // Success, write out new state
                    let mut w_guard = state.write().unwrap();

                    // The commit_new_sstable and remove_frozen_wal calls
                    // are set up to panic on failure so `state` gets
                    // poisoned and the main app will crash. Restarting
                    // is safest at this point, reloading the frozen
                    // memtable from WAL. It's safe to reload the frozen
                    // memtable even if commit_new_sstable succeeds because
                    // the sstable and the WAL file contain the same data.
                    // TODO this would be safer if there was just one file.

                    w_guard.sstables.commit_new_sstable(r).expect(
                        "Error committing new sstable; restart for safety.",
                    );
                    w_guard.wal_index.remove_frozen_wal().expect(
                        "Error committing WAL index; restart for safety.",
                    );
                    w_guard.frozen_memtable = None;
                }
                Err(_) => {
                    // Failed.
                    // Nothing extra to do here.
                }
            };
        }

        // Flush is complete, wake everyone up.
        *flush_mutex.lock().unwrap() = false;
        flush_condvar.notify_all();

        // TODO To remove the orphaned WAL we should
        // execute a cleanup that removes all the
        // files that are not in the SSTable index
        // or the WAL index.
        // We could safely do that in this thread.
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

        let state = self.state.read().unwrap();
        let r = state.active_memtable.get(&k);
        let r = match r {
            Some(r) => Some(r),
            None => match &state.frozen_memtable {
                None => None,
                Some(x) => x.get(&k),
            },
        };
        let r = match r {
            Some(r) => Some(r),
            None => state.sstables.get(&k)?,
        };
        drop(state);

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
    pub fn shutdown(&mut self) {
        let guard = self.state.read().unwrap();
        dbg!(&guard.wal_index);
        // dbg!(&guard.sstables.sstables_index);
    }

    // /// Immediately terminate (for use during testing, "pretend to crash")
    // pub(crate) fn terminate(&mut self) {}

    // scan has a lifetime parameter to ensure that the resulting
    // iterator cannot outlive the toykv instance.
    pub fn scan<'a>(
        &'a self,
        start_key: Option<&'a [u8]>,
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<KVIterator, Error> {
        let lower_bound = match start_key {
            None => Bound::Unbounded,
            Some(k) => Bound::Included(k.to_vec()),
        };

        let mut m = MergeIterator::<'a>::new(upper_bound.clone());

        let state = self.state.read().unwrap();
        let mti = state
            .active_memtable
            .range(lower_bound.clone(), upper_bound.clone());
        m.add_iterator(mti);

        if let Some(x) = &state.frozen_memtable {
            let fmti = x.range(lower_bound, upper_bound.clone());
            m.add_iterator(fmti);
        }

        let iters = state.sstables.iters(start_key)?;
        for t in iters {
            m.add_iterator(t);
        }
        drop(state);

        Ok(KVIterator { i: m })
    }
}

pub struct KV {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct KVIterator<'a> {
    i: MergeIterator<'a>,
}

impl<'a> Iterator for KVIterator<'a> {
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
