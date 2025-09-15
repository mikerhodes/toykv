#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToyKVError {
    GenericError,
    DataDirMissing,
    Io(std::io::ErrorKind, String),
    BadWALState,
    BadWALSeq { expected: u32, actual: u32 },
    KeyTooLarge,
    ValueTooLarge,
    KeyEmpty,
    ValueEmpty,
    DatabaseShutdown,
    // memtables are full and a flush is needed
    NeedFlush,
    CompactionCommitFailure(String),
    CompactionAlreadyRunning,
}

impl From<&std::io::Error> for ToyKVError {
    fn from(e: &std::io::Error) -> Self {
        ToyKVError::Io(e.kind(), e.to_string())
    }
}
impl From<std::io::Error> for ToyKVError {
    fn from(e: std::io::Error) -> Self {
        ToyKVError::from(&e)
    }
}

// If we want From<ToyKVError> for std::io::Error
// https://github.com/spacejam/sled/blob/005c023ca94d424d8e630125e4c21320ed160031/src/result.rs#L102
