#![allow(dead_code)]
// sstable format:
// --------------------------------------------------------------------------
// |  Block Section   |        Meta Section       |          Extra          |
// --------------------------------------------------------------------------
// | data block | ... |          metadata         | meta block offset (u32) |
// --------------------------------------------------------------------------

use std::{
    collections::BTreeMap,
    io::{Cursor, Error, Read},
    iter::repeat_with,
    path::{Path, PathBuf},
};

const BLOOM_HASH_KEY: &[u8; 16] =
    &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

use builder::TableBuilder;
use iterator::TableIterator;
use siphasher::sip::SipHasher13;
use tableindex::SSTableIndex;

use crate::kvrecord::KVValue;

mod builder;
mod iterator;
mod tableindex;

/// Manage and search a set of SSTable files on disk.
pub(crate) struct SSTables {
    d: PathBuf,
    sstables_index: SSTableIndex,
    sstables: SSTablesReader,

    // Ensure we share the same hashing between
    // builder and iterator for bloom filter.
    bloom_hasher: SipHasher13,
}

impl SSTables {
    /// Create a new SSTables whose files live in the directory d.
    pub(crate) fn new(d: &Path) -> Result<SSTables, Error> {
        let index_path = d.join("sstable_index.json");
        let sstables_index = SSTableIndex::open(index_path)?;
        let files = sstables_index.levels.l0.clone();
        let hasher = SipHasher13::new_with_key(BLOOM_HASH_KEY);
        let sstables = SSTablesReader::new(files, hasher)?;
        Ok(SSTables {
            d: d.to_path_buf(),
            sstables_index,
            sstables,
            bloom_hasher: hasher,
        })
    }

    /// Write a new SSTable to the set managed by this
    /// SSTables. After this method returns, the contents
    /// of the memtable are durable on disk and are used
    /// by future calls to `get`.
    pub(crate) fn write_new_sstable(
        &mut self,
        memtable: &BTreeMap<Vec<u8>, KVValue>,
    ) -> Result<(), Error> {
        let fname = next_sstable_fname(self.d.as_path());

        let mut sst = TableBuilder::new(self.bloom_hasher, memtable.len());
        for entry in memtable {
            assert!(sst.add(entry.0, &entry.1.into()).is_ok());
        }
        sst.write(fname.as_path())?;

        // Commit the new sstable to the index.
        self.sstables_index.levels.l0.insert(0, fname);
        self.sstables_index.write()?;

        // Load new SSTablesReader for the new state.
        self.sstables = SSTablesReader::new(
            self.sstables_index.levels.l0.clone(),
            self.bloom_hasher,
        )?;

        Ok(())
    }

    /// Retrieve the latest value for `k` in the on disk
    /// set of sstables.
    pub(crate) fn get(&mut self, k: &[u8]) -> Result<Option<KVValue>, Error> {
        self.sstables.get(k)
    }

    pub(crate) fn iters(&self) -> Result<Vec<TableIterator>, Error> {
        let mut vec = vec![];
        for table_path in &self.sstables_index.levels.l0 {
            let it = TableIterator::new(table_path.clone(), self.bloom_hasher)?;
            vec.push(it);
        }
        Ok(vec)
    }
}
/// Iterate entries in an on-disk SSTable.
struct SSTablesReader {
    /// tables maintains a set of BufReaders on every sstable
    /// file in the set. This isn't that scalable.
    tables: Vec<TableIterator>,
    bloom_hasher: SipHasher13,
}

/// SSTablesReader provides operations over a set of sstable files
/// on disk. As sstable files are immutable, this can be considered
/// a reader over a given snapshot of the database (at least, the
/// sstables portion of it).
impl SSTablesReader {
    /// Create a new SSTableFileReader that is able to search
    /// for keys in the Vec of sstable files, organised newest
    /// to oldest.
    fn new(
        sstable_files: Vec<PathBuf>,
        bloom_hasher: SipHasher13,
    ) -> Result<SSTablesReader, Error> {
        let mut tables: Vec<TableIterator> = vec![];
        for p in sstable_files {
            tables.push(TableIterator::new(p, bloom_hasher)?);
        }
        Ok(SSTablesReader {
            tables,
            bloom_hasher,
        })
    }

    /// Search through the SSTables available to this reader for
    /// a key. Return an Option with its value.
    fn get(&mut self, k: &[u8]) -> Result<Option<KVValue>, Error> {
        // self.tables is in the right order for scanning the sstables
        // on disk. Read each to find k. If no SSTable file contains
        // k, return None.
        // let mut tables_searched = 0;
        let hash = self.bloom_hasher.hash(k);
        for t in self
            .tables
            .iter_mut()
            .filter(|t| t.might_contain_hashed_key(hash))
        {
            // dbg!("t in tables");
            // tables_searched += 1;
            t.seek_to_key(k)?;
            match t.next() {
                Some(Ok(v)) if v.key == k => {
                    // dbg!(tables_searched);
                    return Ok(Some(v.value));
                }
                Some(Ok(_)) | None => continue, // not in this sstable
                Some(Err(x)) => return Err(x),
            }
        }
        // Otherwise, we didn't find it.
        Ok(None)
    }
}

fn next_sstable_fname(dir: &Path) -> PathBuf {
    let s: String = repeat_with(fastrand::alphanumeric).take(16).collect();
    dir.join(format!("{}.sstable", s))
}

#[derive(Debug)]
pub(crate) struct BlockMeta {
    pub(crate) start_offset: u32,
    pub(crate) end_offset: u32,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
}

impl BlockMeta {
    /// Encodes self into a buffer, consuming self.
    fn encode(self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(self.start_offset.to_be_bytes());
        buf.extend(self.end_offset.to_be_bytes());
        buf.extend((self.first_key.len() as u16).to_be_bytes());
        buf.extend(self.first_key);
        buf.extend((self.last_key.len() as u16).to_be_bytes());
        buf.extend(self.last_key);
        buf
    }

    pub(crate) fn decode(data: &[u8]) -> BlockMeta {
        let mut u32buf: [u8; 4] = [0u8; 4];
        let mut u16buf: [u8; 2] = [0u8; 2];

        let mut c = Cursor::new(data);

        assert!(matches!(c.read(&mut u32buf), Ok(4)));
        let start_offset = u32::from_be_bytes(u32buf);
        assert!(matches!(c.read(&mut u32buf), Ok(4)));
        let end_offset = u32::from_be_bytes(u32buf);

        assert!(matches!(c.read(&mut u16buf), Ok(2)));
        let fkeylen = u16::from_be_bytes(u16buf);
        let mut first_key = vec![0; fkeylen as usize];
        assert!(
            matches!(c.read(&mut first_key), Ok(n) if n == fkeylen as usize)
        );

        assert!(matches!(c.read(&mut u16buf), Ok(2)));
        let lkeylen = u16::from_be_bytes(u16buf);
        let mut last_key = vec![0; lkeylen as usize];
        assert!(
            matches!(c.read(&mut last_key), Ok(n) if n == lkeylen as usize)
        );

        BlockMeta {
            start_offset,
            end_offset,
            first_key,
            last_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::ErrorKind};

    use builder::TableBuilder;
    use iterator::TableIterator;

    use super::*;
    use crate::kvrecord::{KVRecord, KVValue, KVWriteValue};

    // TableBuilder Tests
    #[test]
    fn test_table_builder_new() {
        // Test that creating a new TableBuilder doesn't crash
        let _table_builder = TableBuilder::new(SipHasher13::new(), 0);
    }

    #[test]
    fn test_table_builder_add_single_entry() {
        // Test that adding a single entry doesn't crash
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1);
        let result =
            table_builder.add(b"test_key", &KVWriteValue::Some(b"test_value"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_table_builder_add_multiple_entries() {
        // Test that adding multiple entries doesn't crash
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 3);

        let result1 =
            table_builder.add(b"key1", &KVWriteValue::Some(b"value1"));
        assert!(result1.is_ok());

        let result2 =
            table_builder.add(b"key2", &KVWriteValue::Some(b"value2"));
        assert!(result2.is_ok());

        let result3 = table_builder.add(b"key3", &KVWriteValue::Deleted);
        assert!(result3.is_ok());
    }

    #[test]
    fn test_table_builder_many_entries() {
        // Test that adding many entries doesn't crash
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let value = [b'L'; 500];
            let result =
                table_builder.add(key.as_bytes(), &KVWriteValue::Some(&value));
            assert!(result.is_ok());
        }
        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");

        // Writing empty table should not crash
        let result = table_builder.write(temp_file.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_table_builder_estimate_size() {
        // Test that estimate_size doesn't crash
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Should work on empty table
        let _size = table_builder.estimate_size();

        // Should work after adding entries
        let _ =
            table_builder.add(b"test_key", &KVWriteValue::Some(b"test_value"));
        let _size = table_builder.estimate_size();
    }

    #[test]
    fn test_table_builder_write() {
        // Test that writing to a file doesn't crash
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Add some test data
        let _ = table_builder.add(b"key1", &KVWriteValue::Some(b"value1"));
        let _ = table_builder.add(b"key2", &KVWriteValue::Some(b"value2"));
        let _ = table_builder.add(b"key3", &KVWriteValue::Deleted);

        // Create a temporary file
        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");

        // Writing should not crash
        let result = table_builder.write(temp_file.path());
        assert!(result.is_ok());
    }

    // Round-trip Tests (TableBuilder -> TableIterator)
    #[test]
    fn test_round_trip_single_entry() {
        // Test round-trip with a single entry
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);
        table_builder
            .add(b"hello", &KVWriteValue::Some(b"world"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();

        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"hello");
        assert_eq!(first_record.value, KVValue::Some(b"world".to_vec()));

        // Should be only one entry
        assert!(table_iterator.next().is_none());
    }

    #[test]
    fn test_round_trip_multiple_entries() {
        // Test round-trip with multiple entries
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        let test_data = vec![
            (b"apple".as_slice(), KVWriteValue::Some(b"red fruit")),
            (b"banana".as_slice(), KVWriteValue::Some(b"yellow fruit")),
            (b"cherry".as_slice(), KVWriteValue::Some(b"small red fruit")),
            (b"date".as_slice(), KVWriteValue::Some(b"brown fruit")),
        ];

        for (key, value) in &test_data {
            table_builder.add(*key, value).unwrap();
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), 4);

        assert_eq!(records[0].key, b"apple");
        assert_eq!(records[0].value, KVValue::Some(b"red fruit".to_vec()));

        assert_eq!(records[1].key, b"banana");
        assert_eq!(records[1].value, KVValue::Some(b"yellow fruit".to_vec()));

        assert_eq!(records[2].key, b"cherry");
        assert_eq!(
            records[2].value,
            KVValue::Some(b"small red fruit".to_vec())
        );

        assert_eq!(records[3].key, b"date");
        assert_eq!(records[3].value, KVValue::Some(b"brown fruit".to_vec()));
    }

    #[test]
    fn test_round_trip_with_deleted_entries() {
        // Test round-trip with both live and deleted entries
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_alive1", &KVWriteValue::Some(b"data1"))
            .unwrap();
        table_builder
            .add(b"b_deleted1", &KVWriteValue::Deleted)
            .unwrap();
        table_builder
            .add(b"c_alive2", &KVWriteValue::Some(b"data2"))
            .unwrap();
        table_builder
            .add(b"d_deleted2", &KVWriteValue::Deleted)
            .unwrap();
        table_builder
            .add(b"e_alive3", &KVWriteValue::Some(b"data3"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), 5);

        assert_eq!(records[0].key, b"a_alive1");
        assert_eq!(records[0].value, KVValue::Some(b"data1".to_vec()));

        assert_eq!(records[1].key, b"b_deleted1");
        assert_eq!(records[1].value, KVValue::Deleted);

        assert_eq!(records[2].key, b"c_alive2");
        assert_eq!(records[2].value, KVValue::Some(b"data2".to_vec()));

        assert_eq!(records[3].key, b"d_deleted2");
        assert_eq!(records[3].value, KVValue::Deleted);

        assert_eq!(records[4].key, b"e_alive3");
        assert_eq!(records[4].value, KVValue::Some(b"data3".to_vec()));
    }

    #[test]
    fn test_round_trip_large_entries() {
        // Test round-trip with large entries that should span multiple blocks
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        let entries = 10123;

        // Create entries large enough to force multiple blocks
        for i in 0..entries {
            let key = format!("large_key_{:06}", i);
            let value = vec![b'X'; 1000]; // 1KB value
            table_builder
                .add(key.as_bytes(), &KVWriteValue::Some(&value))
                .unwrap();
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        println!(
            "file size: {}",
            fs::metadata(temp_file.path()).unwrap().len()
        );

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), entries);

        for (i, record) in records.iter().enumerate() {
            let expected_key = format!("large_key_{:06}", i);
            assert_eq!(record.key, expected_key.as_bytes());

            match &record.value {
                KVValue::Some(v) => {
                    assert_eq!(v.len(), 1000);
                    assert!(v.iter().all(|&b| b == b'X'));
                }
                KVValue::Deleted => panic!("Unexpected deleted value"),
            }
        }
    }

    #[test]
    fn test_round_trip_many_small_entries() {
        // Test round-trip with many small entries that span multiple blocks
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        let num_entries = 1000;
        for i in 0..num_entries {
            let key = format!("key{:06}", i);
            let value = format!("value{:06}", i);
            table_builder
                .add(key.as_bytes(), &KVWriteValue::Some(value.as_bytes()))
                .unwrap();
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), num_entries);

        for (i, record) in records.iter().enumerate() {
            let expected_key = format!("key{:06}", i);
            let expected_value = format!("value{:06}", i);

            assert_eq!(record.key, expected_key.as_bytes());
            assert_eq!(
                record.value,
                KVValue::Some(expected_value.as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_round_trip_mixed_entry_sizes() {
        // Test round-trip with entries of varying sizes
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Small entry
        table_builder.add(b"a", &KVWriteValue::Some(b"1")).unwrap();

        // Medium entry
        table_builder
            .add(b"b_medium_key", &KVWriteValue::Some(b"medium_value_data"))
            .unwrap();

        // Large entry
        let large_key = vec![b'c'; 100];
        let large_value = vec![b'V'; 500];
        table_builder
            .add(&large_key, &KVWriteValue::Some(&large_value))
            .unwrap();

        // Deleted entry
        table_builder
            .add(b"deleted_entry", &KVWriteValue::Deleted)
            .unwrap();

        // Another small entry
        table_builder
            .add(b"z_final", &KVWriteValue::Some(b"last"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), 5);

        assert_eq!(records[0].key, b"a");
        assert_eq!(records[0].value, KVValue::Some(b"1".to_vec()));

        assert_eq!(records[1].key, b"b_medium_key");
        assert_eq!(
            records[1].value,
            KVValue::Some(b"medium_value_data".to_vec())
        );

        assert_eq!(records[2].key, large_key);
        assert_eq!(records[2].value, KVValue::Some(large_value));

        assert_eq!(records[3].key, b"deleted_entry");
        assert_eq!(records[3].value, KVValue::Deleted);

        assert_eq!(records[4].key, b"z_final");
        assert_eq!(records[4].value, KVValue::Some(b"last".to_vec()));
    }

    #[test]
    fn test_round_trip_empty_table() {
        // Test round-trip with empty table
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let r = table_builder.write(temp_file.path());

        // TableBuilder should refuse to write an empty table.
        assert!(r.is_err());
        assert!(r.err().unwrap().kind() == ErrorKind::InvalidInput);
    }

    #[test]
    fn test_round_trip_keys_with_special_bytes() {
        // Test round-trip with keys/values containing special byte values
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Keys and values with null bytes
        table_builder
            .add(b"key\x00with_null", &KVWriteValue::Some(b"value\x00null"))
            .unwrap();

        // Keys and values with high bytes
        table_builder
            .add(b"key\xff\xfe", &KVWriteValue::Some(b"value\xff\xfe\xfd"))
            .unwrap();

        // Mixed special characters
        table_builder
            .add(
                b"mixed\x01\x80\xff",
                &KVWriteValue::Some(b"data\x00\x7f\x80\xff"),
            )
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), 3);

        assert_eq!(records[0].key, b"key\x00with_null");
        assert_eq!(records[0].value, KVValue::Some(b"value\x00null".to_vec()));

        assert_eq!(records[1].key, b"key\xff\xfe");
        assert_eq!(
            records[1].value,
            KVValue::Some(b"value\xff\xfe\xfd".to_vec())
        );

        assert_eq!(records[2].key, b"mixed\x01\x80\xff");
        assert_eq!(
            records[2].value,
            KVValue::Some(b"data\x00\x7f\x80\xff".to_vec())
        );
    }

    #[test]
    fn test_round_trip_partial_iteration() {
        // Test that TableIterator can be partially consumed
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        for i in 0..10 {
            let key = format!("item{:02}", i);
            let value = format!("data{:02}", i);
            table_builder
                .add(key.as_bytes(), &KVWriteValue::Some(value.as_bytes()))
                .unwrap();
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator but only consume first few entries
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();

        // Consume first 3 entries
        let first = table_iterator.next().unwrap().unwrap();
        assert_eq!(first.key, b"item00");

        let second = table_iterator.next().unwrap().unwrap();
        assert_eq!(second.key, b"item01");

        let third = table_iterator.next().unwrap().unwrap();
        assert_eq!(third.key, b"item02");

        // Continue with the rest
        let remaining: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let remaining = remaining.unwrap();

        assert_eq!(remaining.len(), 7);
        assert_eq!(remaining[0].key, b"item03");
        assert_eq!(remaining[6].key, b"item09");
    }

    #[test]
    fn test_round_trip_max_size_entries() {
        // Test round-trip with maximum size entries
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Create maximum size key and value (within u16 limits)
        let max_key = vec![b'K'; 1000]; // Large but not maximum to avoid memory issues
        let max_value = vec![b'V'; 2000];

        table_builder
            .add(&max_key, &KVWriteValue::Some(&max_value))
            .unwrap();

        // Add a normal entry after
        table_builder
            .add(b"normal", &KVWriteValue::Some(b"entry"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Read back with TableIterator
        let table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].key, max_key);
        assert_eq!(records[0].value, KVValue::Some(max_value));

        assert_eq!(records[1].key, b"normal");
        assert_eq!(records[1].value, KVValue::Some(b"entry".to_vec()));
    }

    // TableIterator::seek_to_key tests
    #[test]
    fn test_seek_to_key_exact_match() {
        // Test seeking to a key that exists exactly in the table
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Using alphabetical prefixes to ensure proper ordering
        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();
        table_builder
            .add(b"d_date", &KVWriteValue::Some(b"brown fruit"))
            .unwrap();
        table_builder
            .add(b"e_elderberry", &KVWriteValue::Some(b"purple fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Test seeking to "c_cherry" which exists in the table
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"c_cherry").unwrap();

        // After seeking, next() should return "c_cherry" first
        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"c_cherry");
        assert_eq!(
            first_record.value,
            KVValue::Some(b"small red fruit".to_vec())
        );

        // Then "d_date"
        let second_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(second_record.key, b"d_date");
        assert_eq!(second_record.value, KVValue::Some(b"brown fruit".to_vec()));

        // Then "e_elderberry"
        let third_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(third_record.key, b"e_elderberry");
        assert_eq!(third_record.value, KVValue::Some(b"purple fruit".to_vec()));

        // Should be no more records
        assert!(table_iterator.next().is_none());
    }

    #[test]
    fn test_seek_to_key_non_existent_key() {
        // Test seeking to a key that doesn't exist - should start from next key
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();
        table_builder
            .add(b"e_elderberry", &KVWriteValue::Some(b"purple fruit"))
            .unwrap();
        table_builder
            .add(b"g_grape", &KVWriteValue::Some(b"green fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Test seeking to "b_banana" which doesn't exist - should start from "c_cherry"
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"b_banana").unwrap();

        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"c_cherry");
        assert_eq!(
            first_record.value,
            KVValue::Some(b"small red fruit".to_vec())
        );

        // Test seeking to "d_date" which doesn't exist - should start from "e_elderberry"
        table_iterator.seek_to_key(b"d_date").unwrap();
        let next_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(next_record.key, b"e_elderberry");
        assert_eq!(next_record.value, KVValue::Some(b"purple fruit".to_vec()));
    }

    #[test]
    fn test_seek_to_key_before_first_key() {
        // Test seeking to a key that comes before all keys in the table
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();
        table_builder
            .add(b"d_date", &KVWriteValue::Some(b"brown fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Seek to a key before all entries
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"a_apple").unwrap();

        // Should start from the first key
        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"b_banana");
        assert_eq!(first_record.value, KVValue::Some(b"yellow fruit".to_vec()));
    }

    #[test]
    fn test_seek_to_key_after_last_key() {
        // Test seeking to a key that comes after all keys in the table
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Seek to a key after all entries
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"z_zucchini").unwrap();

        // Should have no more records
        assert!(table_iterator.next().is_none());
    }

    #[test]
    fn test_seek_to_key_first_key() {
        // Test seeking to the first key in the table
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Seek to the first key
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"a_apple").unwrap();

        // Should return all records starting from the first
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, b"a_apple");
        assert_eq!(records[1].key, b"b_banana");
        assert_eq!(records[2].key, b"c_cherry");
    }

    #[test]
    fn test_seek_to_key_last_key() {
        // Test seeking to the last key in the table
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Seek to the last key
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"c_cherry").unwrap();

        // Should return only the last record
        let records: Result<Vec<KVRecord>, _> = table_iterator.collect();
        let records = records.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"c_cherry");
        assert_eq!(
            records[0].value,
            KVValue::Some(b"small red fruit".to_vec())
        );
    }

    #[test]
    fn test_seek_to_key_with_deleted_entries() {
        // Test seeking when the table contains deleted entries
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Deleted)
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();
        table_builder
            .add(b"d_date", &KVWriteValue::Deleted)
            .unwrap();
        table_builder
            .add(b"e_elderberry", &KVWriteValue::Some(b"purple fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Seek to deleted entry "b_banana"
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"b_banana").unwrap();

        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"b_banana");
        assert_eq!(first_record.value, KVValue::Deleted);

        // Next should be "c_cherry"
        let second_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(second_record.key, b"c_cherry");
        assert_eq!(
            second_record.value,
            KVValue::Some(b"small red fruit".to_vec())
        );
    }

    #[test]
    fn test_seek_to_key_multiple_seeks() {
        // Test multiple seeks on the same iterator
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        table_builder
            .add(b"c_cherry", &KVWriteValue::Some(b"small red fruit"))
            .unwrap();
        table_builder
            .add(b"d_date", &KVWriteValue::Some(b"brown fruit"))
            .unwrap();
        table_builder
            .add(b"e_elderberry", &KVWriteValue::Some(b"purple fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();

        // First seek to "c_cherry"
        table_iterator.seek_to_key(b"c_cherry").unwrap();
        let record = table_iterator.next().unwrap().unwrap();
        assert_eq!(record.key, b"c_cherry");

        // Second seek to "b_banana" (backward seek)
        table_iterator.seek_to_key(b"b_banana").unwrap();
        let record = table_iterator.next().unwrap().unwrap();
        assert_eq!(record.key, b"b_banana");

        // Third seek to "e_elderberry" (forward seek)
        table_iterator.seek_to_key(b"e_elderberry").unwrap();
        let record = table_iterator.next().unwrap().unwrap();
        assert_eq!(record.key, b"e_elderberry");

        // Fourth seek to end of table
        table_iterator.seek_to_key(b"z_zebra").unwrap();
        assert!(table_iterator.next().is_none());
    }

    #[test]
    fn test_seek_to_key_large_table() {
        // Test seeking in a table with many entries (spanning multiple blocks)
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        // Create many entries to force multiple blocks
        for i in 0..1000 {
            let key = format!("key{:06}", i);
            let value = format!("value{:06}", i);
            table_builder
                .add(key.as_bytes(), &KVWriteValue::Some(value.as_bytes()))
                .unwrap();
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();

        // Seek to a key in the middle
        table_iterator.seek_to_key(b"key000500").unwrap();

        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"key000500");
        assert_eq!(first_record.value, KVValue::Some(b"value000500".to_vec()));

        // Verify next few records are in sequence
        let second_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(second_record.key, b"key000501");

        let third_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(third_record.key, b"key000502");
    }

    #[test]
    fn test_seek_to_key_empty_key() {
        // Test seeking with an empty key
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1000);

        table_builder
            .add(b"a_apple", &KVWriteValue::Some(b"red fruit"))
            .unwrap();
        table_builder
            .add(b"b_banana", &KVWriteValue::Some(b"yellow fruit"))
            .unwrap();

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Seek to empty key (should start from first key)
        let mut table_iterator = TableIterator::new(
            temp_file.path().to_path_buf(),
            SipHasher13::new(),
        )
        .unwrap();
        table_iterator.seek_to_key(b"").unwrap();

        let first_record = table_iterator.next().unwrap().unwrap();
        assert_eq!(first_record.key, b"a_apple");
        assert_eq!(first_record.value, KVValue::Some(b"red fruit".to_vec()));
    }
}
