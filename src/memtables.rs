use std::{
    io::Error,
    iter::repeat_with,
    mem,
    path::{Path, PathBuf},
};

use memtable::Memtable;
use walindex::WALIndex;

use crate::{
    error::ToyKVError, kvrecord::KVValue, memtable, walindex, WALSync,
};

pub(crate) struct Memtables {
    d: PathBuf,
    wal_index: WALIndex,

    /// active_memtable stores new writes before they
    /// are moved to sstables. They have a WAL for
    /// durability.
    active_memtable: Memtable,
    /// frozen_memtable exists to be written to disk
    /// in the background, such that we can still
    /// accept writes to the active memtable.
    frozen_memtable: Option<Memtable>,
    wal_write_threshold: u64,
    wal_sync: WALSync,
}

impl Memtables {
    pub(crate) fn new(
        d: PathBuf,
        wal_sync: WALSync,
        wal_write_threshold: u64,
    ) -> Result<Self, ToyKVError> {
        let wal_index_path = d.join("wal_index.json");
        let mut wal_index = WALIndex::open(wal_index_path)?;

        // If there is a WAL on disk, load the active memtable
        // from it, otherwise initialise a memtable with a fresh
        // WAL file on disk.
        let active_wal_path = match wal_index.active_wal() {
            None => new_wal_path(&d),
            Some(x) => x,
        };
        wal_index.set_active_wal(&active_wal_path)?;
        let active_memtable = Memtable::new(active_wal_path, wal_sync)?;

        // Unlike active memtable, we don't need to have a frozen
        // memtable unless we exited running before we managed
        // to save the frozen memtable to disk. Mostly the
        // frozen memtable is transient, as it's saved to disk
        // almost immediately.
        let frozen_memtable = match wal_index.frozen_wal() {
            None => None,
            Some(x) => Some(Memtable::new(x, wal_sync)?),
        };

        Ok(Memtables {
            d,
            wal_index,
            active_memtable,
            frozen_memtable,
            wal_write_threshold,
            wal_sync,
        })
    }
    pub(crate) fn write(
        &mut self,
        k: Vec<u8>,
        v: KVValue,
    ) -> Result<(), ToyKVError> {
        if self.active_memtable.wal_writes() >= self.wal_write_threshold {
            let new_wal_path = new_wal_path(&self.d);
            let old_wal_path = self.wal_index.active_wal().unwrap();

            self.wal_index.set_active_wal(&new_wal_path)?;
            self.wal_index.set_frozen_wal(&old_wal_path)?;

            let new_active_memtable =
                Memtable::new(new_wal_path, self.wal_sync)?;

            self.frozen_memtable = Some(mem::replace(
                &mut self.active_memtable,
                new_active_memtable,
            ));
        }
        self.active_memtable.write(k, v)?;
        Ok(())
    }

    /// Return true if there is no longer space writes in memtables
    pub(crate) fn needs_flush(&self) -> bool {
        // No space in active memtable and we have a frozen on already.
        if self.active_memtable.wal_writes() >= self.wal_write_threshold {
            self.frozen_memtable.is_some()
        } else {
            false
        }
    }

    /// Return true if there is a non-active memtable available to flush.
    pub(crate) fn can_flush(&self) -> bool {
        return self.frozen_memtable.is_some();
    }

    pub(crate) fn write_frozen_memtable(
        &self,
        w: crate::table::SSTableWriter,
    ) -> Result<crate::table::SSTableWriterResult, Error> {
        assert!(self.frozen_memtable.is_some(), "frozen_memtable is None");
        w.write(self.frozen_memtable.as_ref().unwrap().iter())
    }

    pub(crate) fn drop_frozen_memtable(&mut self) -> Result<(), ToyKVError> {
        self.wal_index.remove_frozen_wal()?;
        let ft = mem::replace(&mut self.frozen_memtable, None);
        match ft {
            Some(mut ft) => ft.cleanup_disk(),
            None => Ok(()),
        }
    }

    pub(crate) fn get(&self, k: &[u8]) -> Option<KVValue> {
        match self.active_memtable.get(k) {
            Some(r) => Some(r),
            None => match &self.frozen_memtable {
                None => None,
                Some(t) => t.get(k),
            },
        }
    }

    pub(crate) fn iters(
        &self,
        lower_bound: std::ops::Bound<Vec<u8>>,
        upper_bound: std::ops::Bound<Vec<u8>>,
    ) -> Vec<memtable::MemtableIterator> {
        let mut iters = vec![self
            .active_memtable
            .range(lower_bound.clone(), upper_bound.clone())];
        if let Some(ft) = &self.frozen_memtable {
            iters.push(ft.range(lower_bound, upper_bound))
        }
        iters
    }
}

fn new_wal_path(dir: &Path) -> PathBuf {
    let s: String = repeat_with(fastrand::alphanumeric).take(16).collect();
    dir.join(format!("{}.wal", s))
}
