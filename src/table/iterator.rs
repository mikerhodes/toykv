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
    path::PathBuf,
    sync::Arc,
};

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
    seek_key: Option<Vec<u8>>,
}

impl TableIterator {
    /// Return a new TableIterator postioned at the
    /// start of the table.
    pub(crate) fn new(path: PathBuf) -> Result<TableIterator, Error> {
        let f = OpenOptions::new().read(true).open(&path)?;
        let mut br = BufReader::with_capacity(8 * 1024, f);
        let bm = TableIterator::load_block_meta(&mut br)?;
        let first_block = TableIterator::load_block(&mut br, &bm[0])?;
        br.rewind()?;
        Ok(TableIterator {
            f: br,
            p: path,
            bm,
            bi: BlockIterator::create(first_block),
            b_idx: 0,
            seek_key: None,
        })
    }

    /// load block metadata from a reader
    // TODO this should really be in a MetadataBlock encode/decode pair
    fn load_block_meta(
        f: &mut BufReader<File>,
    ) -> Result<Vec<BlockMeta>, Error> {
        let mut u32bytebuf = [0u8; 4];

        // File ends with a u32 that tells us where the
        // metadata block starts.
        let bm_end_offset = f.seek(SeekFrom::End(-4))?;
        f.read_exact(&mut u32bytebuf)?;
        let bm_start_offset = u32::from_be_bytes(u32bytebuf);
        println!("bm_start_offset {}", bm_start_offset);

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
        // Find the block by binary searching self.bm
        // Load the block and iterate to the key so bi is in the
        // right place for next()
        // Not clear how we find the key and yet have not iterated
        // to it... perhaps next() returns a kvrecord we stash
        // here then advances the iterator or something.
        // Or we could have a start_key attr on TableIterator,
        // which we set, and if it's set, next() skips 'til it
        // hits it (so we just set self.bi to a new BlockIterator for
        // the block the binary search decides on, or we could have
        // blockiterator itself support a start key). Or perhaps
        // rust's iterator has a skip until function (although again
        // that might advance the iterator just too far).
        //
        // easiest way it to go through the blockmetas in reverse,
        // stopping at the first where the start key is below or equal.
        // Which would be the same as going forward and picking the one
        // before where the start key was greater than or equal.
        // Which is the same in a way.
        // What's the binary search way. Do I have to look to either side
        // to find whether we've got there? Let's say we just look at the
        // first key. If it's greater than the key we're looking for, we
        // know we have to look to the left. If it's equal we know we
        // use that block. If it's less, then it _could_ be that block,
        // or could be to the right. So we check the end key. to see if
        // it's in this block, or we have to go right.
        // Either way, we take the slice of the left or right, then
        // go around again.
        // We have to be careful we don't end up in a funny place when
        // the key isn't in any block. In which case, it will be less
        // than the start key, but greater than the end key of the
        // previous block.
        // In the doc it says to just use the start key. I think that means
        // you have to check the start key of the following block rather
        // than the end key of this block to see if the key falls in the
        // gap. That solves the problem of the "not in any block" case.
        // (and you can do a check on the end key of the block you settle
        // on to figure whether you should skip to the next block).
        // After finding the block, set an attr on the iterator for the
        // seeked key, so that next() can skip until it gets there.

        let key = Vec::from(key);

        if key.len() == 0 {
            // All keys are greater than a null key
            return self.rewind();
        }
        if self.bm.len() == 1 {
            self.seek_key = Some(key);
            return self.rewind();
        }
        let mut left = 0;
        let mut right = self.bm.len() - 1;
        let mut cut: usize;
        loop {
            cut = (left + right) / 2;
            let bm = &self.bm[cut];
            if bm.first_key > key {
                // go left
                right = cut;
            } else if bm.first_key == key {
                // we're there
                break;
            } else if bm.first_key < key {
                // check next block to see if we are in the right place
                // else go right.
                if cut == self.bm.len() - 1 {
                    // must be this block as it's the last
                    break;
                }
                if self.bm[cut + 1].first_key > key {
                    // next block is higher than key, key must be in
                    // this block or not exist.
                    break;
                }
                // else go right
                left = cut;
            }
        }

        // position iterator at selected block
        self.b_idx = cut;
        self.bi = BlockIterator::create(TableIterator::load_block(
            &mut self.f,
            &self.bm[cut],
        )?);
        self.seek_key = Some(key);

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
                // If there is a seek key, skip keys that are less
                // than the seek key. Otherwise, start returning
                // keys, and None the seek_key as we are past it.
                if let Some(seek_key) = &self.seek_key {
                    if &x.key < seek_key {
                        continue;
                    } else {
                        self.seek_key = None;
                    }
                }
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
