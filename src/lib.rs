use memtable::Memtable;
use std::iter::repeat_with;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
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
    /// Set to true while frozen_memtable is being
    /// written
    flushing: bool,
    /// sstables are the on-disk primary database
    /// storage.
    sstables: SSTables,
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
        let active_memtable =
            Memtable::new(active_wal_path, self.options.wal_sync)?;

        // Unlike active memtable, we don't need to have a frozen
        // memtable unless we exited running before we managed
        // to save the frozen memtable to disk. Mostly the
        // frozen memtable is transient, as it's saved to disk
        // almost immediately.
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
                flushing: false,
                sstables,
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
        // Maintain a lock on the database during writing
        let do_flush = false;
        {
        let mut state = self.state.write().unwrap();

        state.active_memtable.write(k, v)?;
        if state.active_memtable.wal_writes() >= self.wal_write_threshold {

            wait on self.state.condvar stuff goes here

            // Next create a new active memtable, and freeze the
            // previous one.
            let wal_path = new_wal_path(&self.d);
            state.wal_index.set_active_wal(&wal_path)?;
            let new_memtable = Memtable::new(wal_path, self.wal_sync)?;
            let old_active_memtable =
                mem::replace(&mut state.active_memtable, new_memtable);
            state
                .wal_index
                .set_frozen_wal(&old_active_memtable.wal_path())?;
            state.frozen_memtable = Some(Arc::new(old_active_memtable));

            // Now kick off a thread for flushing
            // Clone the RwLock state for the new thread, and kick
            // it off.
            state.flushing = true;

            do_flush = true;
        }
        }

        if do_flush {
            let new_thread_state = self.state.clone();
            let handler = thread::spawn(move || {
                let r = ToyKV::flush_frozen_memtable(new_thread_state);
                r.expect("Error flushing frozen memtable");
            });
            self handler = handler then join it in shutdown()
            self.metrics.sst_flushes.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    fn flush_frozen_memtable(
        state: Arc<RwLock<ToyKVState>>,
    ) -> Result<(), ToyKVError> {
        // To flush the memtable, first we have to get a read
        // lock on the frozen memtable and then write it to
        // disk, getting the filename as we do so.
        // If there are no errors when writing the sstable,
        // then we get a write lock on the state and update
        // it with the new sstable and remove the frozen
        // memtable. We also set flushing to false as we're
        // done flushing (the cleanup could be done whenever,
        // we just do it now).
        // Finally, we cleanup the disk of the now unneeded
        // frozen memtable.
        let mut fname = None;
        let mut frozen_memtable = None;
        let mut bloom_hasher = None;

        {
            let state = state.read().unwrap();
            if let Some(fm) = &state.frozen_memtable {
                frozen_memtable = Some(fm.clone());
                fname = Some(state.sstables.next_sstable_fname());
                bloom_hasher = Some(state.sstables.bloom_hasher());
            }
        }

        // Now write out the memtable in the thread
        // let frozen_memtable =
        if let Some(ft) = frozen_memtable {
            let fname = fname.unwrap();

            SSTables::write_new_sstable(
                fname.as_path(),
                bloom_hasher.unwrap(),
                ft.iter(),
            )?;

            // Update the state
            let mut state = state.write().unwrap();
            state.sstables.commit_new_sstable(fname)?;
            state.flushing = false;
            Some(mem::replace(&mut state.frozen_memtable, None).unwrap())
        } else {
            None
        };

        // TODO will have to do this in GC? Cannot take mutable
        // because of Arc.
        // if let Some(ft) = frozen_memtable {
        //     ft.cleanup_disk()?;
        // }
        Ok(())
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
    pub fn shutdown(&mut self) {}

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
