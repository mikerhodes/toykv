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
    frozen_memtables: Vec<Memtable>,
    max_frozen_memtables: usize,
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
        let mut frozen_memtables = vec![];
        for p in wal_index.frozen_wals() {
            frozen_memtables.push(Memtable::new(p, wal_sync)?);
        }
        Ok(Memtables {
            d,
            wal_index,
            active_memtable,
            frozen_memtables,
            max_frozen_memtables: 1,
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
            self.wal_index.set_active_wal(&new_wal_path)?;
            let new_active_memtable =
                Memtable::new(new_wal_path, self.wal_sync)?;

            let frozen_memtable =
                mem::replace(&mut self.active_memtable, new_active_memtable);

            self.frozen_memtables.insert(0, frozen_memtable);
            self.wal_index.set_frozen_wals(
                self.frozen_memtables.iter().map(|x| x.wal_path()).collect(),
            )?;
        }
        self.active_memtable.write(k, v)?;
        Ok(())
    }

    /// Return true if there is no longer space writes in memtables
    pub(crate) fn needs_flush(&self) -> bool {
        // No space in active memtable and we already have max_frozen_memtables.
        if self.active_memtable.wal_writes() >= self.wal_write_threshold {
            self.frozen_memtables.len() >= self.max_frozen_memtables
        } else {
            false
        }
    }

    /// Return true if there is a non-active memtable available to flush.
    pub(crate) fn can_flush(&self) -> bool {
        !self.frozen_memtables.is_empty()
    }

    // Write the oldest memtable to disk, returning the result
    // of the write and the ID for the memtable (to pass to drop_memtable).
    pub(crate) fn write_oldest_memtable(
        &self,
        w: crate::table::SSTableWriter,
    ) -> Result<(crate::table::SSTableWriterResult, String), Error> {
        assert!(!self.frozen_memtables.is_empty(), "frozen_memtables empty!");
        let ft = self.frozen_memtables.last().unwrap();
        let r = w.write(ft.iter())?;
        Ok((r, ft.id()))
    }

    pub(crate) fn drop_memtable(
        &mut self,
        id: String,
    ) -> Result<(), ToyKVError> {
        let idx = self.frozen_memtables.iter().position(|x| x.id() == id);

        let idx = match idx {
            Some(x) => x,
            None => return Ok(()), // already gone
        };

        let mut dropped_table = self.frozen_memtables.remove(idx);
        self.wal_index.set_frozen_wals(
            self.frozen_memtables.iter().map(|x| x.wal_path()).collect(),
        )?;
        dropped_table.cleanup_disk()?;

        Ok(())
    }

    pub(crate) fn get(&self, k: &[u8]) -> Option<KVValue> {
        let r = self.active_memtable.get(k);
        if let Some(_) = r {
            return r;
        }
        for t in self.frozen_memtables.iter() {
            let r = t.get(k);
            if let Some(_) = r {
                return r;
            }
        }
        None
    }

    pub(crate) fn iters(
        &self,
        lower_bound: std::ops::Bound<Vec<u8>>,
        upper_bound: std::ops::Bound<Vec<u8>>,
    ) -> Vec<memtable::MemtableIterator> {
        let mut iters = vec![self
            .active_memtable
            .range(lower_bound.clone(), upper_bound.clone())];
        for t in self.frozen_memtables.iter() {
            iters.push(t.range(lower_bound.clone(), upper_bound.clone()))
        }
        iters
    }
}

fn new_wal_path(dir: &Path) -> PathBuf {
    let s: String = repeat_with(fastrand::alphanumeric).take(16).collect();
    dir.join(format!("{}.wal", s))
}
