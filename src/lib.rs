use std::{collections::BTreeMap, path::Path};

#[derive(Debug)]
pub enum ToyKVError {
    GenericError,
}

pub struct ToyKV<'a> {
    /// d is the folder that the KV store owns.
    d: &'a Path,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
}

pub fn open(d: &Path) -> ToyKV {
    // TODO check path, fail if it doesn't exist.
    // TODO initialise if empty (new WAL?)
    let memtable = BTreeMap::new();
    ToyKV { d, memtable }
}

impl<'a> ToyKV<'a> {
    /// Sets key k to v.
    pub fn set(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), ToyKVError> {
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

    /// Immediately terminate (for use during testing, "pretend to crash")
    pub(crate) fn terminate(&mut self) {}
}
