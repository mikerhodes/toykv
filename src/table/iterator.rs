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

use crate::error::ToyKVError;
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
    fn new_with_tablereader(
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
    fn new_seeked_with_tablereader(
        tr: Arc<TableReader>,
        key: &[u8],
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<TableIterator, Error> {
        let bound_key = Bound::Included(key);
        let seeked_block_idx =
            TableIterator::seek_to_block_by_key(&tr.bm, bound_key)?;
        // position tableiterator at selected block
        let bi = BlockIterator::create_and_seek_to_key(
            tr.load_block(&tr.bm[seeked_block_idx])?,
            bound_key,
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

    /// Return the index of the first block that contains an entry
    /// matching bound_key.
    fn seek_to_block_by_key(
        idx: &Vec<BlockMeta>,
        bound_key: Bound<&[u8]>,
    ) -> Result<usize, Error> {
        let key = match bound_key {
            // No bound is trivially first block
            Bound::Unbounded => return Ok(0),
            Bound::Included(k) => k,
            Bound::Excluded(k) => k,
        };

        assert!(key.len() > 0, "key length must be >0");
        assert!(idx.len() < i64::MAX as usize, "too many blocks in reader");

        let mut left: i64 = 0;
        let mut right: i64 = idx.len() as i64 - 1;
        let mut mid: i64 = 0;

        while left <= right {
            mid = (left + right) / 2;
            let block_meta = &idx[mid as usize];

            if *key >= block_meta.first_key[..]
                && *key <= block_meta.last_key[..]
            {
                break;
            } else if *key < block_meta.first_key[..] {
                right = mid - 1
            } else {
                left = mid + 1
            }
        }

        // Handle cases where we should fall over to the next
        // block because the last_key is outside the bound.
        // Previously we only checked first_key.
        let last_key = &idx[mid as usize].last_key[..];
        mid = match bound_key {
            Bound::Excluded(k) if k >= last_key => mid + 1,
            Bound::Included(k) if k > last_key => mid + 1,
            _ => mid,
        };

        // Perhaps we could return None if we've found the
        // key is beyond the end of the last block. For now
        // allow the last block to be (needlessly) processed.
        if mid >= idx.len() as i64 {
            mid = idx.len() as i64 - 1
        }

        Ok(mid as usize)
    }

    /// Seek to a key in the table. next() will resume from
    /// the first entry with key, or the entry following where
    /// key would be.
    pub(crate) fn seek_to_key(&mut self, key: &[u8]) -> Result<(), Error> {
        if key.len() == 0 {
            // All keys are greater than a null key
            return self.rewind();
        }

        let cut = TableIterator::seek_to_block_by_key(
            &self.tr.bm,
            Bound::Included(key),
        )?;

        // position tableiterator at selected block
        self.b_idx = cut;
        self.bi = BlockIterator::create_and_seek_to_key(
            self.tr.load_block(&self.tr.bm[cut])?,
            Bound::Included(key),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::BlockMeta;

    fn bm(first: &str, last: &str) -> BlockMeta {
        BlockMeta {
            start_offset: 0,
            end_offset: 0,
            first_key: first.as_bytes().to_vec(),
            last_key: last.as_bytes().to_vec(),
        }
    }

    #[test]
    fn seek_unbounded_returns_first_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Unbounded);
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn seek_included_key_within_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"f"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn seek_included_first_key_in_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"e"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn seek_included_last_key_in_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"g"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn seek_included_key_between_blocks() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"d"));
        assert_eq!(result.unwrap(), 1);
    }
    #[test]
    fn seek_included_sole_key_in_block() {
        let idx = vec![bm("a", "a"), bm("c", "e"), bm("g", "i")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"a"));
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn seek_included_key_beyond_last_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"z"));
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn seek_excluded_first_key_in_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Excluded(b"e"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn seek_excluded_sole_key_in_block() {
        let idx = vec![bm("a", "a"), bm("c", "e"), bm("g", "i")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Excluded(b"a"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn seek_excluded_last_key_in_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Excluded(b"g"));
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn seek_excluded_key_beyond_last_block() {
        let idx = vec![bm("a", "c"), bm("e", "g"), bm("i", "k")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Excluded(b"z"));
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn seek_included_key_beyond_only_block() {
        let idx = vec![bm("a", "c")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Included(b"z"));
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn seek_excluded_key_beyond_only_block() {
        let idx = vec![bm("a", "c")];
        let result =
            TableIterator::seek_to_block_by_key(&idx, Bound::Excluded(b"z"));
        assert_eq!(result.unwrap(), 0);
    }
}

impl Iterator for TableIterator {
    type Item = Result<KVRecord, ToyKVError>;

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
                    return Some(Err(x.into()));
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
