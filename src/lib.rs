use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::{io::Error, ops::Bound, path::Path};

use ouroboros::self_referencing;

use error::ToyKVError;
use kvrecord::{KVRecord, KVValue};
use merge_iterator::MergeIterator;
use table::SSTables;
use wal::WAL;

mod block;
mod blockiterator;
pub mod error;
mod kvrecord;
mod merge_iterator;
mod table;
mod wal;

#[derive(PartialEq, Eq, Debug)]
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
    pub metrics: ToyKVMetrics,
    wal_write_threshold: u64,
    state: RwLock<ToyKVState>,
}

/// ToyKVState is the modifiable state of the database.
struct ToyKVState {
    memtable: Arc<SkipMap<Vec<u8>, KVValue>>,
    wal: WAL,
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

        let mut wal = wal::new(d, self.options.wal_sync);
        let memtable = wal.replay()?;
        let sstables = table::SSTables::new(d)?;
        Ok(ToyKV {
            state: RwLock::new(ToyKVState {
                memtable: Arc::new(memtable),
                wal,
                sstables,
            }),
            metrics: Default::default(),
            wal_write_threshold: self.options.wal_write_threshold,
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
        let mut state = self.state.write().unwrap();

        state.wal.write(&k, (&v).into())?;
        state.memtable.insert(k, v);
        if state.wal.wal_writes >= self.wal_write_threshold {
            let memtable = &state.memtable;
            let fname = state.sstables.write_new_sstable(&memtable)?;
            state.sstables.commit_new_sstable(fname)?;
            state.wal.reset()?;
            state.memtable.clear();
            self.metrics.sst_flushes.fetch_add(1, Ordering::Relaxed);
        }
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
        let mt = state.memtable.clone();
        let r = mt.get(&k);
        let r = match r {
            Some(r) => Some(r.value().to_owned()),
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
        end_key: Bound<Vec<u8>>,
    ) -> Result<KVIterator, Error> {
        let lower_bound = match start_key {
            None => Bound::Unbounded,
            Some(k) => Bound::Included(k.to_vec()),
        };

        let state = self.state.read().unwrap();
        let mt = state.memtable.clone();
        let iters = state.sstables.iters(start_key)?;
        drop(state);

        let mut m = MergeIterator::<'a>::new(end_key.clone());

        let mti = MemtableIteratorBuilder {
            memtable: mt,
            it_builder: |mt| mt.range((lower_bound, end_key)),
        }
        .build();
        m.add_iterator(mti);

        for t in iters.into_iter() {
            m.add_iterator(t);
        }

        Ok(KVIterator { i: m })
    }
}

// Make this a type because it's so long to write out
type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    Vec<u8>,
    (Bound<Vec<u8>>, Bound<Vec<u8>>),
    Vec<u8>,
    KVValue,
>;

// I realised quite quickly that I needed to somehow put the memtable Arc
// clone into a struct alongside the iterator that borrowed it. But I
// couldn't for the life of me construct something that the compiler
// would accept; usually I could see why. It took me a while to figure
// out that I needed a crate to help with it.
#[self_referencing]
struct MemtableIterator {
    memtable: Arc<SkipMap<Vec<u8>, KVValue>>,
    #[borrows(memtable)]
    #[not_covariant]
    it: SkipMapRangeIter<'this>,
}

impl MemtableIterator {
    fn entry_to_kvrecord(
        entry: Entry<Vec<u8>, KVValue>,
    ) -> Result<KVRecord, Error> {
        Ok(KVRecord {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }
}

impl Iterator for MemtableIterator {
    type Item = Result<KVRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_it_mut(|iter| match iter.next() {
            None => None,
            Some(entry) => Some(MemtableIterator::entry_to_kvrecord(entry)),
        })
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
