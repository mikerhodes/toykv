use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

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
}

impl From<std::io::Error> for ToyKVError {
    fn from(value: std::io::Error) -> Self {
        ToyKVError::FileError(value)
    }
}

#[derive(Default)]
pub struct ToyKVMetrics {
    pub sst_flushes: u64,
    pub reads: u64,
    pub writes: u64,
}

pub struct ToyKV {
    /// d is the folder that the KV store owns.
    d: PathBuf,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
    wal: WAL,
    pub metrics: ToyKVMetrics,
}

pub fn open(d: &Path) -> Result<ToyKV, ToyKVError> {
    if !d.is_dir() {
        return Err(ToyKVError::DataDirMissing);
    }

    let mut wal = wal::new(d);
    let memtable = wal.replay()?;
    Ok(ToyKV {
        d: d.to_path_buf(),
        memtable,
        wal,
        metrics: Default::default(),
    })
}

/// How many writes to a WAL before we SSTable it.
const WAL_WRITE_THRESHOLD: u32 = 1000;

impl ToyKV {
    /// Sets key k to v.
    pub fn set(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), ToyKVError> {
        self.wal.write(&k, &v)?;
        self.memtable.insert(k, v);
        if self.wal.wal_writes >= WAL_WRITE_THRESHOLD {
            let mut sst = sstable::new_writer(self.d.as_path())?;
            for entry in &self.memtable {
                sst.write(entry.0, entry.1)?;
            }
            sst.finalise()?;
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
            None => {
                let sst = sstable::new_reader(self.d.as_path())?;
                sst.get(k)?
            }
        };
        self.metrics.reads += 1;
        Ok(r)
    }

    /// Perform a graceful shutdown.
    pub fn shutdown(&mut self) {}

    // /// Immediately terminate (for use during testing, "pretend to crash")
    // pub(crate) fn terminate(&mut self) {}
}
