#![allow(dead_code)]
// sstable format:
// --------------------------------------------------------------------------
// |  Block Section   |        Meta Section       |          Extra          |
// --------------------------------------------------------------------------
// | data block | ... |          metadata         | meta block offset (u32) |
// --------------------------------------------------------------------------

use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Error, Read, Seek, SeekFrom},
    ops::Bound,
    path::PathBuf,
    sync::Arc,
};

use sbbf_rs_safe::Filter;
use siphasher::sip::SipHasher13;

use crate::{
    block::Block, blockiterator::BlockIterator, kvrecord::KVRecord,
    table::BlockMeta,
};

pub(crate) struct TableIterator {
    p: PathBuf,
    f: BufReader<File>,
    bm: Vec<BlockMeta>,
    // current block
    bi: BlockIterator,
    b_idx: usize,
    // bloom filter
    bloom: Filter,
    hasher: SipHasher13,
}

impl TableIterator {
    /// Return a new TableIterator postioned at the
    /// start of the table.
    pub(crate) fn new(
        path: PathBuf,
        hasher: SipHasher13,
    ) -> Result<TableIterator, Error> {
        let f = OpenOptions::new().read(true).open(&path)?;
        let mut br = BufReader::with_capacity(8 * 1024, f);
        let bm = TableIterator::load_block_meta(&mut br)?;
        let bloom = TableIterator::load_bloom_filter(&mut br)?;
        assert!(bloom.is_some(), "could not load bloom filter from table");
        let first_block = TableIterator::load_block(&mut br, &bm[0])?;
        br.rewind()?;
        Ok(TableIterator {
            f: br,
            p: path,
            bm,
            bi: BlockIterator::create(first_block),
            b_idx: 0,
            bloom: bloom.unwrap(),
            hasher,
        })
    }

    // Use bloom filter to check whether key is in table (can false positive).
    pub(crate) fn might_contain_key(&self, key: &[u8]) -> bool {
        let hash = self.hasher.hash(key);
        self.bloom.contains_hash(hash)
    }
    pub(crate) fn might_contain_hashed_key(&self, hash: u64) -> bool {
        self.bloom.contains_hash(hash)
    }

    fn load_bloom_filter(
        f: &mut BufReader<File>,
    ) -> Result<Option<Filter>, Error> {
        // File tail structure:
        //
        // block_metadata | bloom | block_metadata offset: u32 | bloom offset: u32
        //
        // So to read the metadata we need to read:
        // file[metadata offset..bloom offset];

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
        f: &mut BufReader<File>,
    ) -> Result<Vec<BlockMeta>, Error> {
        // File tail structure:
        //
        // block_metadata | bloom | block_metadata offset | bloom offset
        //
        // So to read the metadata we need to read:
        // file[metadata offset..bloom offset];

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

    /// Seek to a key in the table. next() will resume from
    /// the first entry with key, or the entry following where
    /// key would be.
    pub(crate) fn seek_to_key(&mut self, key: &[u8]) -> Result<(), Error> {
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

        if key.len() == 0 {
            // All keys are greater than a null key
            return self.rewind();
        }

        let mut left = 0;
        let mut right = self.bm.len() - 1;
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

            let bm = &self.bm[cut];
            let bm_next = &self.bm[cut + 1];

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

        // position tableiterator at selected block
        self.b_idx = cut;
        self.bi = BlockIterator::create_and_seek_to_key(
            TableIterator::load_block(&mut self.f, &self.bm[cut])?,
            Bound::Included(&key[..]),
        );

        Ok(())
    }

    fn rewind(&mut self) -> Result<(), Error> {
        self.b_idx = 0;
        self.bi = BlockIterator::create(TableIterator::load_block(
            &mut self.f,
            &self.bm[0],
        )?);
        Ok(())
    }

    fn load_block(
        br: &mut BufReader<File>,
        bm: &BlockMeta,
    ) -> Result<Arc<Block>, std::io::Error> {
        let mut block_data =
            vec![0u8; (bm.end_offset - bm.start_offset) as usize];
        br.seek(SeekFrom::Start(bm.start_offset as u64))?;
        br.read_exact(&mut block_data)?;
        Ok(Arc::new(Block::decode(&block_data)))
    }

    fn load_next_block(&mut self) -> Result<Option<Arc<Block>>, Error> {
        self.b_idx += 1;
        if self.b_idx >= self.bm.len() {
            return Ok(None);
        }
        let bm = &self.bm[self.b_idx];
        let mut block_data =
            vec![0u8; (bm.end_offset - bm.start_offset) as usize];
        self.f.seek(SeekFrom::Start(bm.start_offset as u64))?;
        self.f.read_exact(&mut block_data)?;
        Ok(Some(Arc::new(Block::decode(&block_data))))
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

            if let Some(x) = next_record {
                return Some(Ok(x));
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
