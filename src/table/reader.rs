use sbbf_rs_safe::Filter;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Error, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{block::Block, table::BlockMeta};

pub(crate) struct TableReader {
    pub(crate) p: PathBuf,
    f: Mutex<BufReader<File>>,
    pub(crate) bm: Vec<BlockMeta>,
    // bloom filter
    bloom: Filter,

    /// First key in this table
    first_key: Vec<u8>,
    /// Last key in this table
    last_key: Vec<u8>,
}
impl TableReader {
    /// Return a new TableIterator postioned at the
    /// start of the table.
    pub(crate) fn new(path: PathBuf) -> Result<TableReader, Error> {
        let f = OpenOptions::new().read(true).open(&path)?;
        let br =
            Mutex::new(BufReader::with_capacity(8 * 1024 /* 8kib */, f));
        let bm = TableReader::load_block_meta(&br)?;
        assert!(bm.len() > 0);
        let first_key = bm[0].first_key.clone();
        let last_key = bm[bm.len() - 1].last_key.clone();
        let bloom = TableReader::load_bloom_filter(&br)?;
        assert!(bloom.is_some(), "could not load bloom filter from table");
        {
            let mut br = br.lock().unwrap();
            br.rewind()?;
        }
        Ok(TableReader {
            f: br,
            p: path,
            bm,
            bloom: bloom.unwrap(),
            first_key,
            last_key,
        })
    }

    /// Return a tuple of (first_key, last_key) for this table
    pub(crate) fn key_range(&self) -> (&Vec<u8>, &Vec<u8>) {
        return (&self.first_key, &self.last_key);
    }

    pub(crate) fn might_contain_hashed_key(&self, hash: u64) -> bool {
        self.bloom.contains_hash(hash)
    }

    fn load_bloom_filter(
        mf: &Mutex<BufReader<File>>,
    ) -> Result<Option<Filter>, Error> {
        // File tail structure:
        //
        // block_metadata | bloom | block_metadata offset: u32 | bloom offset: u32
        //
        // So to read the metadata we need to read:
        // file[metadata offset..bloom offset];

        let mut f = mf.lock().unwrap();

        // First, get the end of the bloom filter by seeking
        let bloom_end_offset = f.seek(SeekFrom::End(-8))?;

        // Next, read the bloom start offset from the end of the file
        f.seek(SeekFrom::End(-4))?;
        let mut u32bytebuf = [0u8; 4];
        f.read_exact(&mut u32bytebuf)?;
        let bloom_start_offset = u32::from_be_bytes(u32bytebuf);

        let bloom_len = bloom_end_offset as usize - bloom_start_offset as usize;
        f.seek(SeekFrom::Start(bloom_start_offset as u64))?;
        let mut bloombuf = vec![0u8; bloom_len];
        f.read_exact(&mut bloombuf[..])?;

        Ok(Filter::from_bytes(&bloombuf[..]))
    }

    /// load block metadata from a reader
    // TODO this should really be in a MetadataBlock encode/decode pair
    fn load_block_meta(
        mf: &Mutex<BufReader<File>>,
    ) -> Result<Vec<BlockMeta>, Error> {
        // File tail structure:
        //
        // block_metadata | bloom | block_metadata offset | bloom offset
        //
        // So to read the metadata we need to read:
        // file[metadata offset..bloom offset];

        let mut f = mf.lock().unwrap();

        // Read the two u32 offsets
        let mut u32bytebuf = [0u8; 4];
        f.seek(SeekFrom::End(-8))?;
        f.read_exact(&mut u32bytebuf)?;
        let bm_start_offset = u32::from_be_bytes(u32bytebuf);
        f.read_exact(&mut u32bytebuf)?;
        let bloom_start_offset = u32::from_be_bytes(u32bytebuf);
        let bm_end_offset = bloom_start_offset as u64;

        f.seek(SeekFrom::Start(bm_start_offset as u64))?;

        let mut result = vec![];

        loop {
            // read length of next blockmeta
            f.read_exact(&mut u32bytebuf)?;
            let bm_len = u32::from_be_bytes(u32bytebuf);

            // read blockmeta into buf and decode into result vec
            let mut bm_buf = vec![0u8; bm_len as usize];
            f.read_exact(&mut bm_buf)?;
            result.push(BlockMeta::decode(&bm_buf));

            // Did we read the last blockmeta?
            let p = f.stream_position()?;
            assert!(
                p <= bm_end_offset,
                "corrupted blockmeta section in sstable"
            );
            if p == bm_end_offset {
                break;
            }
        }

        Ok(result)
    }

    pub(crate) fn load_block(
        &self,
        bm: &BlockMeta,
    ) -> Result<Arc<Block>, std::io::Error> {
        let mut br = self.f.lock().unwrap();
        let mut block_data =
            vec![0u8; (bm.end_offset - bm.start_offset) as usize];
        br.seek(SeekFrom::Start(bm.start_offset as u64))?;
        br.read_exact(&mut block_data)?;
        Ok(Arc::new(Block::decode(&block_data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use siphasher::sip::SipHasher13;

    use crate::{kvrecord::KVValue, table::builder::TableBuilder};

    fn add_kv(table_builder: &mut TableBuilder, key: &[u8], var_name: &[u8]) {
        table_builder
            .add(key, &KVValue::Some(var_name.into()))
            .unwrap();
    }

    // Tests for TableReader::key_range() function
    #[test]
    fn test_key_range_simple() {
        // Simple test: table with a few keys in alphabetical order
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 10);
        add_kv(&mut table_builder, b"apple", b"red");
        add_kv(&mut table_builder, b"banana", b"yellow");
        add_kv(&mut table_builder, b"cherry", b"red");
        add_kv(&mut table_builder, b"date", b"brown");
        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        // Create TableReader and test key_range
        let table_reader =
            TableReader::new(temp_file.path().to_path_buf()).unwrap();
        let (first_key, last_key) = table_reader.key_range();

        assert_eq!(first_key, &b"apple".to_vec());
        assert_eq!(last_key, &b"date".to_vec());
    }

    #[test]
    fn test_key_range_single_key() {
        // Edge case: table with only one key
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 1);
        add_kv(&mut table_builder, b"onlykey", b"onlyvalue");
        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let table_reader =
            TableReader::new(temp_file.path().to_path_buf()).unwrap();
        let (first_key, last_key) = table_reader.key_range();

        assert_eq!(first_key, &b"onlykey".to_vec());
        assert_eq!(last_key, &b"onlykey".to_vec());
        assert_eq!(first_key, last_key);
    }

    #[test]
    fn test_key_range_minimal_keys() {
        // Edge case: keys with minimal byte values
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 10);
        add_kv(&mut table_builder, b"\x01", b"first");
        add_kv(&mut table_builder, b"a", b"middle");
        add_kv(&mut table_builder, b"\xff", b"last");
        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let table_reader =
            TableReader::new(temp_file.path().to_path_buf()).unwrap();
        let (first_key, last_key) = table_reader.key_range();

        assert_eq!(first_key, &b"\x01".to_vec());
        assert_eq!(last_key, &b"\xff".to_vec());
    }

    #[test]
    fn test_key_range_special_byte_values() {
        // Edge case: keys containing null bytes and other special values
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 10);
        add_kv(&mut table_builder, b"key\x00null", b"value1");
        add_kv(&mut table_builder, b"key\x01one", b"value2");
        add_kv(&mut table_builder, b"key\x7fmax", b"value3");
        add_kv(&mut table_builder, b"key\xfehigh", b"value4");
        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let table_reader =
            TableReader::new(temp_file.path().to_path_buf()).unwrap();
        let (first_key, last_key) = table_reader.key_range();

        assert_eq!(first_key, &b"key\x00null".to_vec());
        assert_eq!(last_key, &b"key\xfehigh".to_vec());
    }

    #[test]
    fn test_key_range_large_number_of_keys() {
        // Test with a large number of keys that span multiple blocks
        let mut table_builder = TableBuilder::new(SipHasher13::new(), 10000);

        let num_keys = 5000;
        // Use zero-padded keys to maintain lexicographic ordering
        for i in 0..num_keys {
            let key = format!("key{:06}", i);
            let value = format!("value{:0512}", i);
            add_kv(&mut table_builder, key.as_bytes(), value.as_bytes())
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let table_reader =
            TableReader::new(temp_file.path().to_path_buf()).unwrap();
        let (first_key, last_key) = table_reader.key_range();

        assert!(first_key < last_key);
        assert_eq!(first_key, &b"key000000".to_vec());
        assert_eq!(last_key, &b"key004999".to_vec());
    }
}
