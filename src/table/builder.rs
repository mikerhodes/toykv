#![allow(dead_code)]
// sstable format:
// -----------------------------------------------------------------------------
// |  Block Section   | Meta Section     |          Offsets                     |
// -----------------------------------------------------------------------------
// | data block | ... | bloom | metadata | bloom (u32) | block meta index (u32) |
// -----------------------------------------------------------------------------
//                    ^-------^--------------                   |
//                            `---------------------------------/

use std::{fs::File, io::Write, mem, path::Path, time::Instant};

use sbbf_rs_safe::Filter;
use siphasher::sip::SipHasher13;

use crate::{
    block::{BlockBuilder, BlockBuilderError},
    kvrecord::KVWriteValue,
};

use super::BlockMeta;
pub(crate) struct TableBuilder {
    /// current block builder
    bb: BlockBuilder,
    /// first key of current block
    first_key: Option<Vec<u8>>,
    /// last key of current block
    last_key: Option<Vec<u8>>,
    /// sstable data block
    data: Vec<u8>,
    /// metadata blocks
    metadata: Vec<u8>,
    /// start time for builder
    start: Instant,
    /// BloomFilter for table
    bloom: Filter,
    hasher: SipHasher13,
}
impl TableBuilder {
    pub(super) fn new(
        hasher: SipHasher13,
        expected_writes: usize,
    ) -> TableBuilder {
        // Each block = 256 bits of space
        // For a 1% false positive, 10.5 bits per insert
        // Blocks => expected * 10.5 / 256
        // https://github.com/apache/parquet-format/blob/master/BloomFilter.md#sizing-an-sbbf
        let blocks = (expected_writes as f64 * 10.5 / 256f64) as usize;
        let bloom = Filter::new(blocks, expected_writes);
        TableBuilder {
            bb: BlockBuilder::new(),
            first_key: None,
            last_key: None,
            data: vec![],
            metadata: vec![],
            start: Instant::now(),
            bloom,
            hasher,
        }
    }
    pub(super) fn add(
        &mut self,
        key: &[u8],
        value: &KVWriteValue,
    ) -> Result<(), BlockBuilderError> {
        let mut r = self.bb.add(key, value);

        // If full finalise block and start new one
        if let Err(BlockBuilderError::BlockFull) = r {
            self.finalise_block();
            r = self.bb.add(key, value);
        }

        // If still an error, return it
        if let Err(x) = r {
            return Err(x);
        }

        let hash = self.hasher.hash(key);
        self.bloom.insert_hash(hash);

        // Set first key if new block, advance
        // last key to current key.
        if let None = self.first_key {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());

        Ok(())
    }

    pub(super) fn write(&mut self, p: &Path) -> std::io::Result<()> {
        // Write out data block if we need to
        self.finalise_block();

        if self.data.len() == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No data to write.",
            ));
        }

        // println!(
        //     "file stats: data len {}, metadata len {}",
        //     self.data.len(),
        //     self.metadata.len()
        // );

        let mut f = File::create(p)?;

        // Write raw data
        f.write(&self.data)?;
        f.write(&self.metadata)?;
        f.write(&self.bloom.as_bytes())?;

        // Write offsets
        let metadata_offset = self.data.len() as u32;
        f.write(&metadata_offset.to_be_bytes())?;
        let bloom_offset = (self.data.len() + self.metadata.len()) as u32;
        f.write(&bloom_offset.to_be_bytes())?;

        f.flush()?;
        f.sync_all()?;

        let elapsed_time = self.start.elapsed();
        println!("Writing sstable took {}ms.", elapsed_time.as_millis());

        Ok(())
    }

    /// Return the estimated size of this table.
    pub(super) fn estimate_size(&self) -> usize {
        // data probably much larger than the metadata, so use that
        return self.data.len();
    }

    /// Write the block out to our data buffer, and prepare a
    /// new block builder, reset first/last key.
    fn finalise_block(&mut self) {
        // Retrieve current block's values while
        // resetting builder state for next block.
        let fk = mem::take(&mut self.first_key);
        let lk = mem::take(&mut self.last_key);
        let bb = mem::replace(&mut self.bb, BlockBuilder::new());

        // fk will always be set if there is data in
        // the block.
        if fk == None {
            return; // nothing to do
        }
        // if we are here, fk, lk and bb should contain data

        let block_data = bb.build().encode();

        let bm = BlockMeta {
            start_offset: self.data.len() as u32,
            end_offset: (self.data.len() + block_data.len()) as u32,
            first_key: fk.unwrap(),
            last_key: lk.unwrap(),
        };
        let encoded = bm.encode();

        self.metadata.extend((encoded.len() as u32).to_be_bytes());
        self.metadata.extend(encoded);
        self.data.extend(block_data);
    }
}
