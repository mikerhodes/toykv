#![allow(dead_code)]
// sstable format:
// -----------------------------------------------------------------------------
// |  Block Section   | Meta Section     |          Offsets                     |
// -----------------------------------------------------------------------------
// | data block | ... | bloom | metadata | bloom (u32) | block meta index (u32) |
// -----------------------------------------------------------------------------
//                    ^-------^--------------                   |
//                            `---------------------------------/
use std::{io::Error, ops::Bound, path::PathBuf, sync::Arc};

use siphasher::sip::SipHasher13;

use crate::{
    block::Block, blockiterator::BlockIterator, kvrecord::KVRecord,
    table::BlockMeta,
};

use crate::table::reader::TableReader;

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

    /// Create a new bounded TableIterator for tr.
    /// Bound::Excluded is not supported and will panic.
    pub(crate) fn new_bounded_with_tablereader(
        tr: Arc<TableReader>,
        lower_bound: Bound<Vec<u8>>,
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<TableIterator, Error> {
        match lower_bound {
            Bound::Unbounded => {
                TableIterator::new_with_tablereader(tr, upper_bound)
            }
            Bound::Included(key) => TableIterator::new_seeked_with_tablereader(
                tr,
                &key,
                upper_bound,
            ),
            Bound::Excluded(_) => {
                // TODO use a ToyKVError instead.
                panic!("TableIterator doesn't support Bound::Excluded")
            }
        }
    }

    /// Create a TableReader that's seeked to start (Bound::Unbounded) and
    /// will stop at upper_bound.
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

    /// Create a TableReader that's seeked to k (Bound::Included) and
    /// will stop at upper_bound.
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

    /// Return the first/last keys in this iterator as tuple
    pub(crate) fn key_range(&self) -> (&Vec<u8>, &Vec<u8>) {
        self.tr.key_range()
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
