use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::{io::Error, ops::Bound};

use ouroboros::self_referencing;

use crate::error::ToyKVError;
use crate::kvrecord::{KVRecord, KVValue};
use crate::wal::{self, WAL};

pub(crate) struct MemtableMap {
    map: Arc<SkipMap<Vec<u8>, KVValue>>,
    estimated_size_bytes: u64,
}

impl MemtableMap {
    pub(crate) fn write(&mut self, k: Vec<u8>, v: KVValue) {
        self.update_estimated_size(&k, &v);
        self.map.insert(k, v);
    }

    /// Updates the estimated size of the memtable given the
    /// new entry (k,v).
    fn update_estimated_size(&mut self, k: &Vec<u8>, v: &KVValue) {
        // If the value is in the memtable already,
        // remove the size from estimate.
        let old_size = match self.map.get(k) {
            Some(r) => calculate_size_bytes(k, r.value()),
            None => 0,
        };
        assert!(self.estimated_size_bytes >= old_size,
            "Data corruption: old_size should always have previously been added to estimated_size_bytes");
        self.estimated_size_bytes -= old_size;
        // Add size of new entry
        self.estimated_size_bytes += calculate_size_bytes(k, v);
    }

    pub(crate) fn get(&self, k: &[u8]) -> Option<KVValue> {
        let r = self.map.get(k);
        match r {
            Some(r) => Some(r.value().to_owned()),
            None => None,
        }
    }
}

/// The memtable and WAL
pub(crate) struct Memtable {
    memtable: MemtableMap,
    wal: WAL,
}

impl Memtable {
    pub(crate) fn new(
        wal_path: PathBuf,
        wal_sync: crate::WALSync,
    ) -> Result<Self, ToyKVError> {
        let mut wal = wal::new(wal_path, wal_sync);
        let mut memtable = MemtableMap {
            map: Arc::new(SkipMap::new()),
            estimated_size_bytes: 0,
        };
        wal.replay(&mut memtable)?;
        Ok(Self { memtable, wal })
    }

    // An opaque ID for the memtable
    pub(crate) fn id(&self) -> String {
        self.wal.wal_path().to_str().unwrap().to_string()
    }

    pub(crate) fn len(&self) -> usize {
        self.memtable.map.len()
    }

    // Path to the WAL on disk
    pub(crate) fn wal_path(&self) -> PathBuf {
        self.wal.wal_path().clone()
    }

    pub(crate) fn write(
        &mut self,
        k: Vec<u8>,
        v: KVValue,
    ) -> Result<(), ToyKVError> {
        self.wal.write(&k, &v)?;
        self.memtable.write(k, v);
        Ok(())
    }

    /// Estimated size of memtable data in bytes
    pub(crate) fn estimated_size_bytes(&self) -> u64 {
        self.memtable.estimated_size_bytes
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
        self.memtable.get(k)
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
            memtable: self.memtable.map.clone(),
            it_builder: |mt| mt.range((lower_bound, end_key)),
        }
        .build()
    }
}

/// Return entry size for (k,v)
fn calculate_size_bytes(k: &Vec<u8>, v: &KVValue) -> u64 {
    (k).len() as u64
        + match v {
            KVValue::Deleted => 0,
            KVValue::Some(x) => x.len(),
        } as u64
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

#[cfg(test)]
mod tests {
    use crate::WALSync::Off;

    use super::*;
    use tempfile::tempdir;

    //
    // A couple of helpers to increase test clarity.
    //

    /// Helper for writing a key with a given length of value
    fn write_kv(mt: &mut Memtable, key: &[u8], v_len: usize) {
        let v = vec![b'V'; v_len];
        mt.write(key.to_vec(), KVValue::Some(v)).unwrap();
    }
    /// Helper for deleting a key
    fn delete_kv(mt: &mut Memtable, key: &[u8]) {
        mt.write(key.to_vec(), KVValue::Deleted).unwrap();
    }

    #[test]
    fn test_size_estimation_basic_inserts() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let mut memtable = Memtable::new(wal_path, Off).unwrap();

        // Initially should be empty
        assert_eq!(memtable.estimated_size_bytes(), 0);

        write_kv(&mut memtable, b"key1", 6);
        assert_eq!(memtable.estimated_size_bytes(), 10);

        write_kv(&mut memtable, b"key2", 6);
        assert_eq!(memtable.estimated_size_bytes(), 20);

        write_kv(&mut memtable, b"longerkey", 234);
        assert_eq!(memtable.estimated_size_bytes(), 263);
    }

    #[test]
    fn test_size_estimation_key_updates() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let mut memtable = Memtable::new(wal_path, Off).unwrap();

        write_kv(&mut memtable, b"key", 5);
        assert_eq!(memtable.estimated_size_bytes(), 8);

        write_kv(&mut memtable, b"key", 3);
        assert_eq!(memtable.estimated_size_bytes(), 6);

        write_kv(&mut memtable, b"key", 15);
        assert_eq!(memtable.estimated_size_bytes(), 18);

        write_kv(&mut memtable, b"key", 15); // same value
        assert_eq!(memtable.estimated_size_bytes(), 18);

        write_kv(&mut memtable, b"key", 1800);
        assert_eq!(memtable.estimated_size_bytes(), 1803);

        delete_kv(&mut memtable, b"key");
        assert_eq!(memtable.estimated_size_bytes(), 3);
    }

    #[test]
    fn test_size_estimation_mixed_operations() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let mut memtable = Memtable::new(wal_path, Off).unwrap();

        // Insert some key-value pairs
        write_kv(&mut memtable, b"key1", 6);
        write_kv(&mut memtable, b"key2", 6);
        write_kv(&mut memtable, b"key3", 6);
        write_kv(&mut memtable, b"key1", 8);
        write_kv(&mut memtable, b"key2", 8);
        assert_eq!(memtable.estimated_size_bytes(), 34);

        // Delete leaves key2 in the table, but removes the 6-byte value
        delete_kv(&mut memtable, b"key2");
        assert_eq!(memtable.estimated_size_bytes(), 26);

        // Delete non-existent key should still add key size
        delete_kv(&mut memtable, b"key4");
        assert_eq!(memtable.estimated_size_bytes(), 30);

        // Delete leaves key1 in the table, but removes the 8-byte value
        delete_kv(&mut memtable, b"key1");
        assert_eq!(memtable.estimated_size_bytes(), 22);

        // Re-insert key1 with new value
        write_kv(&mut memtable, b"key1", 12);
        assert_eq!(memtable.estimated_size_bytes(), 34);
    }

    #[test]
    fn test_size_estimation_wal_replay() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");

        // First, create a memtable and write some data
        {
            let mut memtable1 = Memtable::new(wal_path.clone(), Off).unwrap();
            write_kv(&mut memtable1, b"key1", 6);
            write_kv(&mut memtable1, b"key2", 6);
            write_kv(&mut memtable1, b"key1", 14);
            write_kv(&mut memtable1, b"key3", 6);
            delete_kv(&mut memtable1, b"key2");
            delete_kv(&mut memtable1, b"key4"); // non-existent
            let expected_size = memtable1.estimated_size_bytes();

            // Now create a new memtable that will replay the WAL
            let memtable2 = Memtable::new(wal_path.clone(), Off).unwrap();
            assert_eq!(memtable2.estimated_size_bytes(), expected_size);

            // Verify the values are also correct
            assert_eq!(
                memtable2.get(b"key1"),
                Some(KVValue::Some(b"VVVVVVVVVVVVVV".to_vec()))
            );
            assert_eq!(memtable2.get(b"key2"), Some(KVValue::Deleted));
            assert_eq!(
                memtable2.get(b"key3"),
                Some(KVValue::Some(b"VVVVVV".to_vec()))
            );
            assert_eq!(memtable2.get(b"key4"), Some(KVValue::Deleted));
        }
    }
}
