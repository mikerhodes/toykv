use std::{collections::BTreeMap, io::Error, path::Path};

use error::ToyKVError;
use kvrecord::{KVRecord, KVValue};
use merge_iterator::MergeIterator;
use sstable::SSTables;
use wal::WAL;

pub mod error;
mod kvrecord;
mod merge_iterator;
mod sstable;
mod wal;

#[derive(PartialEq, Eq, Debug)]
pub enum WALSync {
    Full,
    Off,
}

#[derive(Default)]
pub struct ToyKVMetrics {
    pub sst_flushes: u64,
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
}

pub struct ToyKV {
    /// d is the folder that the KV store owns.
    memtable: BTreeMap<Vec<u8>, KVValue>,
    wal: WAL,
    sstables: SSTables,
    pub metrics: ToyKVMetrics,
}

/// Open a database using default settings.
pub fn open(d: &Path) -> Result<ToyKV, ToyKVError> {
    with_sync(d, WALSync::Full)
}

/// Open a database using a specific WALSync strategy.
pub fn with_sync(d: &Path, sync: WALSync) -> Result<ToyKV, ToyKVError> {
    if !d.is_dir() {
        return Err(ToyKVError::DataDirMissing);
    }

    let mut wal = wal::new(d, sync);
    let memtable = wal.replay()?;
    let sstables = sstable::new_sstables(d)?;
    Ok(ToyKV {
        memtable,
        wal,
        sstables,
        metrics: Default::default(),
    })
}

/// How many writes to a WAL before we SSTable it.
const WAL_WRITE_THRESHOLD: u32 = 1000;
const MAX_KEY_SIZE: usize = 10_240; // 10kb
const MAX_VALUE_SIZE: usize = 102_400; // 100kb

impl ToyKV {
    /// Set key k to v.
    pub fn set<S, T>(&mut self, k: S, v: T) -> Result<(), ToyKVError>
    where
        S: Into<Vec<u8>>,
        T: Into<Vec<u8>>,
    {
        let k: Vec<u8> = k.into();
        let v: Vec<u8> = v.into();
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }
        if v.len() > MAX_VALUE_SIZE {
            return Err(ToyKVError::ValueTooLarge);
        }
        self.metrics.writes += 1;
        self.write(k, kvrecord::KVValue::Some(v))
    }

    /// Delete the stored value for k.
    pub fn delete<S>(&mut self, k: S) -> Result<(), ToyKVError>
    where
        S: Into<Vec<u8>>,
    {
        let k: Vec<u8> = k.into();
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }
        self.metrics.deletes += 1;
        self.write(k, KVValue::Deleted)
    }

    /// Internal method to write a KVValue to WAL and memtable
    fn write(&mut self, k: Vec<u8>, v: KVValue) -> Result<(), ToyKVError> {
        self.wal.write(&k, (&v).into())?;
        self.memtable.insert(k, v);
        if self.wal.wal_writes >= WAL_WRITE_THRESHOLD {
            self.sstables.write_new_sstable(&self.memtable)?;
            self.wal.reset()?;
            self.memtable.clear();
            self.metrics.sst_flushes += 1;
        }
        Ok(())
    }

    /// Get the value for k.
    pub fn get<S>(&mut self, k: S) -> Result<Option<Vec<u8>>, ToyKVError>
    where
        S: Into<Vec<u8>>,
    {
        let k: Vec<u8> = k.into();
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }
        let r = self.memtable.get(&k);
        let r = match r {
            Some(r) => Some(r.to_owned()),
            None => self.sstables.get(&k)?,
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

        self.metrics.reads += 1;
        Ok(r)
    }
    /// Perform a graceful shutdown.
    pub fn shutdown(&mut self) {}

    // /// Immediately terminate (for use during testing, "pretend to crash")
    // pub(crate) fn terminate(&mut self) {}

    pub fn scan<'a>(&'a self) -> KVIterator<'a> {
        let mut m = MergeIterator::<'a>::new();
        m.add_iterator(
            self.memtable
                .iter()
                .map(|(key, value)| -> Result<KVRecord, Error> {
                    Ok(KVRecord {
                        key: key.clone(),     // or key.to_owned()
                        value: value.clone(), // assuming KVValue implements Clone
                    })
                }),
        );
        // TODO we're also cloning file-handles here but that's
        // a bit better than the whole memtable!!
        for t in self.sstables.iters() {
            m.add_iterator(t);
        }
        KVIterator { i: m }
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
                Some(Ok(kvr)) if kvr.value == KVValue::Deleted => continue,
                Some(Ok(kvr)) => {
                    return Some(Ok(KV {
                        key: kvr.key,
                        value: vec![],
                    }))
                }
            }
        }
    }
}
