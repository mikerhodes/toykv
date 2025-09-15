#![allow(dead_code)]
// sstable format:
// -----------------------------------------------------------------------------
// |  Block Section   | Meta Section     |          Offsets                     |
// -----------------------------------------------------------------------------
// | data block | ... | bloom | metadata | bloom (u32) | block meta index (u32) |
// -----------------------------------------------------------------------------
//                    ^-------^--------------                   |
//                            `---------------------------------/
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Error, Read, Seek, SeekFrom},
    ops::Bound,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use sbbf_rs_safe::Filter;
use siphasher::sip::SipHasher13;

use crate::{
    block::Block, blockiterator::BlockIterator, kvrecord::KVRecord,
    table::BlockMeta,
};

pub(crate) struct TableReader {
    pub(crate) p: PathBuf, // TODO remove pub after SSTablesReader doesn't need it
    f: Mutex<BufReader<File>>,
    bm: Vec<BlockMeta>,
    // bloom filter
    bloom: Filter,
}
impl TableReader {
    /// Return a new TableIterator postioned at the
    /// start of the table.
    pub(crate) fn new(path: PathBuf) -> Result<TableReader, Error> {
        let f = OpenOptions::new().read(true).open(&path)?;
        let br =
            Mutex::new(BufReader::with_capacity(8 * 1024 /* 8kib */, f));
        let bm = TableReader::load_block_meta(&br)?;
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
        })
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

    fn load_block(&self, bm: &BlockMeta) -> Result<Arc<Block>, std::io::Error> {
        let mut br = self.f.lock().unwrap();
        let mut block_data =
            vec![0u8; (bm.end_offset - bm.start_offset) as usize];
        br.seek(SeekFrom::Start(bm.start_offset as u64))?;
        br.read_exact(&mut block_data)?;
        Ok(Arc::new(Block::decode(&block_data)))
    }
}

pub(crate) struct TableIterator {
    tr: Arc<TableReader>,
    // current block
    bi: BlockIterator,
    b_idx: usize,
    upper_bound: Bound<Vec<u8>>,
}

impl TableIterator {
    /// Return a new TableIterator postioned at the
    /// start of the table.
    pub(crate) fn new(
        path: PathBuf,
        _hasher: SipHasher13,
    ) -> Result<TableIterator, Error> {
        let tr = Arc::new(TableReader::new(path.clone())?);
        let first_block = tr.load_block(&tr.bm[0])?;
        Ok(TableIterator {
            tr,
            bi: BlockIterator::create(first_block),
            b_idx: 0,
            upper_bound: Bound::Unbounded,
        })
    }

    pub(crate) fn new_with_tablereader(
        tr: Arc<TableReader>,
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<TableIterator, Error> {
        let first_block = tr.load_block(&tr.bm[0])?;
        Ok(TableIterator {
            tr,
            bi: BlockIterator::create(first_block),
            b_idx: 0,
            upper_bound,
        })
    }

    pub(crate) fn new_seeked_with_tablereader(
        tr: Arc<TableReader>,
        key: &[u8],
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<TableIterator, Error> {
        let seeked_block_idx = TableIterator::seek_to_key_int(&tr.bm, key)?;
        // position tableiterator at selected block
        let bi = BlockIterator::create_and_seek_to_key(
            tr.load_block(&tr.bm[seeked_block_idx])?,
            Bound::Included(&key[..]),
        );
        Ok(TableIterator {
            tr,
            bi,
            b_idx: seeked_block_idx,
            upper_bound,
        })
    }

    /// Get the path to the underlying file for this TableIterator.
    pub(crate) fn table_path(&self) -> PathBuf {
        self.tr.p.clone()
    }

    pub(crate) fn might_contain_hashed_key(&self, hash: u64) -> bool {
        self.tr.might_contain_hashed_key(hash)
    }

    fn seek_to_key_int(
        idx: &Vec<BlockMeta>,
        key: &[u8],
    ) -> Result<usize, Error> {
        // TODO Use Bound for key to seek to. I wonder if it's easier
        // when using excluded start key to instead bump the key up
        // to the next value by incrementing the last byte. No,
        // because it's a slice and we'd edit the key!
        // https://skyzh.github.io/mini-lsm/week1-04-sst.html#task-2-sst-iterator
        // In the doc it says to just use the start key. I think that means
        // you have to check the start key of the following block rather
        // than the end key of this block to see if the key falls in the
        // gap. That solves the problem of the "falls between blocks" case.
        // (and you can do a check on the end key of the block you settle
        // on to figure whether you should skip to the next block).
        // After finding the block, set an attr on the iterator for the
        // seeked key, so that next() can skip until it gets there.

        assert!(key.len() > 0, "key length must be >0");

        let mut left = 0;
        let mut right = idx.len() - 1;
        let mut cut = 0;
        // dbg!(left, right, &key);

        while left <= right {
            cut = (left + right) / 2;

            // dbg!(left, right, cut);

            // Happens when there is only one block. Other times?
            if left == right {
                cut = left;
                break;
            }

            let bm = &idx[cut];
            let bm_next = &idx[cut + 1];

            // TODO key is after last in table? Do we have a test?
            // Probably should get some edge case tests written.

            if key < &bm.first_key[..] && cut == 0 {
                // key is earlier than first key in table, break
                break;
            } else if key < &bm.first_key[..] {
                // take the left half
                right = cut - 1;
            } else if key >= &bm.first_key[..] && key < &bm_next.first_key[..] {
                // Check whether the key falls between this block and
                // the next, if so, start at the next.
                if key > &bm.last_key[..] {
                    cut += 1;
                }
                break;
            } else {
                // take the right half
                left = cut + 1;
            }
        }

        Ok(cut)
    }

    /// Seek to a key in the table. next() will resume from
    /// the first entry with key, or the entry following where
    /// key would be.
    pub(crate) fn seek_to_key(&mut self, key: &[u8]) -> Result<(), Error> {
        if key.len() == 0 {
            // All keys are greater than a null key
            return self.rewind();
        }

        let cut = TableIterator::seek_to_key_int(&self.tr.bm, key)?;

        // position tableiterator at selected block
        self.b_idx = cut;
        self.bi = BlockIterator::create_and_seek_to_key(
            self.tr.load_block(&self.tr.bm[cut])?,
            Bound::Included(&key[..]),
        );

        Ok(())
    }

    fn rewind(&mut self) -> Result<(), Error> {
        self.b_idx = 0;
        self.bi = BlockIterator::create(self.tr.load_block(&self.tr.bm[0])?);
        Ok(())
    }

    fn load_next_block(&mut self) -> Result<Option<Arc<Block>>, Error> {
        self.b_idx += 1;
        if self.b_idx >= self.tr.bm.len() {
            return Ok(None);
        }
        let bm = &self.tr.bm[self.b_idx];
        Ok(Some(self.tr.load_block(bm)?))
    }
}

impl Iterator for TableIterator {
    type Item = Result<KVRecord, Error>;

    /// Return the next KVRecord in the file.
    fn next(&mut self) -> Option<Self::Item> {
        // Loop until we load a record, error loading data,
        // or reach the end of blocks.
        loop {
            let next_record = self.bi.next();

            if let Some(kvrecord) = next_record {
                // Check end key
                match &self.upper_bound {
                    Bound::Included(x) if kvrecord.key > *x => {
                        return None;
                    }
                    Bound::Excluded(x) if kvrecord.key >= *x => {
                        return None;
                    }
                    _ => (),
                };

                return Some(Ok(kvrecord));
            }

            let new_block = self.load_next_block();
            match new_block {
                Err(x) => {
                    return Some(Err(x));
                }
                Ok(None) => {
                    return None;
                }
                Ok(Some(b)) => {
                    self.bi = BlockIterator::create(b);
                }
            }
        }
    }
}
