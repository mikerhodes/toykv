#![allow(dead_code)]
// Crib the block format from
// https://skyzh.github.io/mini-lsm/week1-03-block.html.
// These blocks are later formed into sstables.
//
// The block encoding format in our course is as follows:
//
// -----------------------------------------------------------------------------
// |         Data Section      |         Offset Section      |      Extra      |
// -----------------------------------------------------------------------------
// | Entry #1 | ... | Entry #N | Offset #1 | ... | Offset #N | num_of_elements |
// -----------------------------------------------------------------------------
//
// Each entry is a key-value pair.
//
// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
//
// Key length and value length are both 2 bytes,
// which means their maximum lengths are 65535.
// (Internally stored as u16)

use crate::kvrecord::KVWriteValue;

const BLOCK_SIZE: usize = 4096;

pub(crate) enum BlockBuilderError {
    BlockFull,
    KeyTooLarge,
    ValueTooLarge,
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Entry {
    key_len: u16,
    key: Vec<u8>,
    value_len: u16,
    value: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Block {
    data: Vec<u8>, // A vec of raw Entry data, at offsets
    offsets: Vec<u8>,
}

pub(crate) struct BlockBuilder {
    entries: Vec<Entry>,
    current_size: usize,
}

impl BlockBuilder {
    fn new() -> BlockBuilder {
        BlockBuilder {
            entries: vec![],
            current_size: 0,
        }
    }

    /// Returns true if k/v successfully added to the block, or
    /// false if the block is full.
    fn add(&mut self, key: &[u8], value: KVWriteValue) -> Result<(), BlockBuilderError> {
        // Ensure the below unwraps will succeed
        if key.len() > u16::MAX as usize {
            return Err(BlockBuilderError::KeyTooLarge);
        }
        if let KVWriteValue::Some(x) = value {
            if x.len() > u16::MAX as usize {
                return Err(BlockBuilderError::ValueTooLarge);
            }
        }

        // TODO instead of having the Vec<Entry> we could
        // encode the entry directly here and have the builder
        // be maintaining a Vec<u8> of the data and a Vec<u16>
        // of offsets, such that making the block at the end
        // is trivial.

        let e = Entry {
            key_len: u16::try_from(key.len()).unwrap(),
            key: key.to_vec(),
            value_len: match value {
                KVWriteValue::Some(x) => u16::try_from(x.len()).unwrap(),
                KVWriteValue::Deleted => 0,
            },
            value: match value {
                KVWriteValue::Some(x) => x.to_vec(),
                KVWriteValue::Deleted => vec![],
            },
        };

        if self.entries.len() > 0 && self.current_size + e.size() > BLOCK_SIZE {
            return Err(BlockBuilderError::BlockFull);
        }

        self.current_size += e.size();
        self.entries.push(e);

        Ok(())
    }
}

impl Block {
    fn decode(data: Vec<u8>) -> Block {}

    /// Encode to a byte vector. This may be longer or shorter than
    /// the BLOCK_SIZE, as large k/v pairs will create large blocks,
    /// while smaller k/v pairs will end up with some slack space
    /// (that we don't pad).
    /// The sstable file maintains an index of blocks in its metadata
    /// to account for this variability.
    fn encode(&self) -> Vec<u8> {}
}
impl Entry {
    fn size(&self) -> usize {
        return 2 + self.key_len as usize + 2 + self.value_len as usize;
    }
    fn decode(data: Vec<u8>) -> Block {}

    fn encode(&self) -> Vec<u8> {}
}
