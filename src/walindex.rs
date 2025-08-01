use serde::{Deserialize, Serialize};
use std::{
    fs::{self},
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
pub(crate) struct WALIndexFile {
    pub(crate) active_wal: Option<PathBuf>,
    pub(crate) frozen_wal: Option<PathBuf>,
}

pub(crate) struct WALIndex {
    wal_index_file: WALIndexFile,
    backing_file_path: PathBuf,
}

impl WALIndex {
    /// Read an index from disk, if it exists. Otherwise,
    /// create and return a new index file.
    pub(crate) fn open(p: PathBuf) -> Result<WALIndex, Error> {
        match fs::read_to_string(&p) {
            Ok(data) => {
                let wif: WALIndexFile = serde_json::from_str(&data)?;
                Ok(WALIndex {
                    wal_index_file: wif,
                    backing_file_path: p,
                })
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let wif = WALIndexFile {
                    active_wal: None,
                    frozen_wal: None,
                };
                Ok(WALIndex {
                    wal_index_file: wif,
                    backing_file_path: p,
                })
            }
            Err(x) => Err(x),
        }
    }

    /// Set active WAL and write the index file
    pub(crate) fn set_active_wal(&mut self, p: &Path) -> Result<(), Error> {
        self.wal_index_file.active_wal = Some(p.to_path_buf());
        fs::write(
            &self.backing_file_path,
            serde_json::to_string(&self.wal_index_file)?,
        )
    }

    pub(crate) fn active_wal(&self) -> Option<PathBuf> {
        match &self.wal_index_file.active_wal {
            None => None,
            Some(x) => Some(x.clone()),
        }
    }
    /// Set active WAL and write the index file
    pub(crate) fn set_frozen_wal(&mut self, p: &Path) -> Result<(), Error> {
        self.wal_index_file.frozen_wal = Some(p.to_path_buf());
        fs::write(
            &self.backing_file_path,
            serde_json::to_string(&self.wal_index_file)?,
        )
    }

    pub(crate) fn frozen_wal(&self) -> Option<PathBuf> {
        match &self.wal_index_file.frozen_wal {
            None => None,
            Some(x) => Some(x.clone()),
        }
    }
}
