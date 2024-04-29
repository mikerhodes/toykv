use std::{collections::BTreeMap, path::Path};

use sstable::SSTables;
use wal::WAL;

mod kvrecord;
mod sstable;
mod wal;

#[derive(Debug)]
pub enum ToyKVError {
    GenericError,
    DataDirMissing,
    FileError(std::io::Error),
    BadWALState,
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
}

pub struct ToyKV {
    /// d is the folder that the KV store owns.
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
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
    pub fn set(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), ToyKVError> {
        if k.len() > MAX_KEY_SIZE {
            return Err(ToyKVError::KeyTooLarge);
        }
        if v.len() > MAX_VALUE_SIZE {
            return Err(ToyKVError::ValueTooLarge);
        }
        self.wal.write(&k, &v)?;
        self.memtable.insert(k, v);
        if self.wal.wal_writes >= WAL_WRITE_THRESHOLD {
            self.sstables.write_new_sstable(&self.memtable)?;
            self.wal.reset()?;
            self.memtable.clear();
            self.metrics.sst_flushes += 1;
        }
        self.metrics.writes += 1;
        Ok(())
    }

    /// Get the value for k.
    pub fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>, ToyKVError> {
        let r = self.memtable.get(k);
        let r = match r {
            Some(r) => Some(r.clone()),
            None => self.sstables.get(k)?,
        };
        self.metrics.reads += 1;
        Ok(r)
    }

    /// Perform a graceful shutdown.
    pub fn shutdown(&mut self) {}

    // /// Immediately terminate (for use during testing, "pretend to crash")
    // pub(crate) fn terminate(&mut self) {}
}
