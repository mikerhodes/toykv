use std::{collections::BTreeMap, path::Path};

use kvrecord::KVValue;
use sstable::SSTables;
use wal::WAL;

mod kvrecord;
mod merge_iterator;
mod sstable;
mod wal;

#[derive(Debug)]
pub enum ToyKVError {
    GenericError,
    DataDirMissing,
    FileError(std::io::Error),
    BadWALState,
    BadWALSeq { expected: u32, actual: u32 },
    KeyTooLarge,
    ValueTooLarge,
}

impl From<std::io::Error> for ToyKVError {
    fn from(value: std::io::Error) -> Self {
        ToyKVError::FileError(value)
    }
}

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
}
