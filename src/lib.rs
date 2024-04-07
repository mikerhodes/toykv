use std::{collections::BTreeMap, path::Path};

use wal::WAL;

mod kvrecord;
mod wal;

#[derive(Debug)]
pub enum ToyKVError {
    GenericError,
    FileError(std::io::Error),
}

impl From<std::io::Error> for ToyKVError {
    fn from(value: std::io::Error) -> Self {
        ToyKVError::FileError(value)
    }
}

pub struct ToyKV<'a> {
    /// d is the folder that the KV store owns.
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
    wal: WAL<'a>,
}

pub fn open(d: &Path) -> Result<ToyKV, ToyKVError> {
    // TODO check path, fail if it doesn't exist.
    // TODO initialise if empty (new WAL?)
    let mut wal = wal::new(d);
    let memtable = wal.replay()?;
    Ok(ToyKV { memtable, wal })
}

impl<'a> ToyKV<'a> {
    /// Sets key k to v.
    pub fn set(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), ToyKVError> {
        self.wal.write(&k, &v)?;
        self.memtable.insert(k, v);
        Ok(())
    }

    /// Get the value for k.
    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, ToyKVError> {
        let r: Vec<u8> = self.memtable[k].clone();
        Ok(Some(r))
    }

    /// Perform a graceful shutdown.
    pub fn shutdown(&mut self) {}

    // /// Immediately terminate (for use during testing, "pretend to crash")
    // pub(crate) fn terminate(&mut self) {}
}
