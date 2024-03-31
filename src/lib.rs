use std::path::Path;

#[derive(Debug)]
pub enum ToyKVError {
    GenericError,
}

pub struct ToyKV<'a> {
    /// d is the folder that the KV store owns.
    d: &'a Path,
}

pub fn new(d: &Path) -> ToyKV {
    // TODO check path, fail if it doesn't exist.
    // TODO initialise if empty (new WAL?)
    ToyKV { d }
}

impl<'a> ToyKV<'a> {
    /// Sets key k to v.
    pub fn set(&mut self, k: &[u8], v: &[u8]) -> Result<(), ToyKVError> {
        Ok(())
    }

    /// Get the value for k.
    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, ToyKVError> {
        let r: Vec<u8> = vec![];
        Ok(Some(r))
    }

    /// Perform a graceful shutdown.
    pub fn shutdown(&mut self) {}

    /// Immediately terminate (for use during testing, "pretend to crash")
    pub(crate) fn terminate(&mut self) {}
}
