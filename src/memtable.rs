use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::{io::Error, ops::Bound};

use ouroboros::self_referencing;

use crate::error::ToyKVError;
use crate::kvrecord::{KVRecord, KVValue};
use crate::wal::{self, WAL};

/// The memtable and WAL
pub(crate) struct Memtable {
    memtable: Arc<SkipMap<Vec<u8>, KVValue>>,
    wal: WAL,
}

impl Memtable {
    pub(crate) fn new(
        wal_path: PathBuf,
        wal_sync: crate::WALSync,
    ) -> Result<Self, ToyKVError> {
        let mut wal = wal::new(wal_path, wal_sync);
        let memtable = Arc::new(wal.replay()?);
        Ok(Self { memtable, wal })
    }

    pub(crate) fn wal_path(&self) -> PathBuf {
        self.wal.wal_path()
    }

    pub(crate) fn write(
        &mut self,
        k: Vec<u8>,
        v: KVValue,
    ) -> Result<(), ToyKVError> {
        self.wal.write(&k, &v)?;
        self.memtable.insert(k, v);
        Ok(())
    }

    /// The number of writes to the current WAL file.
    pub(crate) fn wal_writes(&self) -> u64 {
        self.wal.wal_writes
    }

    /// Remove the ondisk WAL for this memtable. Use after
    /// writing the memtable to an sstable.
    pub(crate) fn cleanup_disk(&mut self) -> Result<(), ToyKVError> {
        self.wal.delete()?;
        Ok(())
    }

    pub(crate) fn get(&self, k: &[u8]) -> Option<KVValue> {
        let r = self.memtable.get(k);
        match r {
            Some(r) => Some(r.value().to_owned()),
            None => None,
        }
    }

    /// Retrieve a MemtableIterator over all entries in the memtable.
    pub(crate) fn iter(&self) -> MemtableIterator {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }

    /// Retrieve a MemtableIterator over the given bounds.
    pub(crate) fn range(
        &self,
        lower_bound: Bound<Vec<u8>>,
        end_key: Bound<Vec<u8>>,
    ) -> MemtableIterator {
        MemtableIteratorBuilder {
            memtable: self.memtable.clone(),
            it_builder: |mt| mt.range((lower_bound, end_key)),
        }
        .build()
    }
}

// Make this a type because it's so long to write out
type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    Vec<u8>,
    (Bound<Vec<u8>>, Bound<Vec<u8>>),
    Vec<u8>,
    KVValue,
>;

// I realised quite quickly that I needed to somehow put the memtable Arc
// clone into a struct alongside the iterator that borrowed it. But I
// couldn't for the life of me construct something that the compiler
// would accept; usually I could see why. It took me a while to figure
// out that I needed a crate to help with it.
#[self_referencing]
pub(crate) struct MemtableIterator {
    memtable: Arc<SkipMap<Vec<u8>, KVValue>>,
    #[borrows(memtable)]
    #[not_covariant]
    it: SkipMapRangeIter<'this>,
}

impl MemtableIterator {
    pub(crate) fn len(&self) -> usize {
        self.borrow_memtable().len()
    }

    fn entry_to_kvrecord(
        entry: Entry<Vec<u8>, KVValue>,
    ) -> Result<KVRecord, Error> {
        Ok(KVRecord {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }
}

impl Iterator for MemtableIterator {
    type Item = Result<KVRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_it_mut(|iter| match iter.next() {
            None => None,
            Some(entry) => Some(MemtableIterator::entry_to_kvrecord(entry)),
        })
    }
}
