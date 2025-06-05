#![allow(dead_code)]
use crate::{
    block::{Block, Entry},
    kvrecord::{KVRecord, KVValue},
};

pub(crate) struct BlockIterator {
    b: Block,
    curr_offset_idx: usize,
}
impl BlockIterator {
    fn create(b: Block) -> BlockIterator {
        BlockIterator {
            b,
            curr_offset_idx: 0,
        }
    }
}
impl Iterator for BlockIterator {
    type Item = KVRecord;

    fn next(&mut self) -> Option<Self::Item> {
        // There is an entry between each index in the
        // offsets array, so we need to decode a block
        // from the data slice for each offset, until
        // we get to the end.
        let start = match self.b.offsets.get(self.curr_offset_idx) {
            Some(o) => *o as usize,
            None => return None, // run off the end of offsets, we're done
        };
        let end = match self.b.offsets.get(self.curr_offset_idx + 1) {
            Some(o) => *o as usize,
            None => self.b.data.len(), // last entry, read to end of data
        };
        let entry_bytes: &[u8] = &self.b.data[start..end];
        let e = Entry::decode(entry_bytes);

        self.curr_offset_idx += 1;

        Some(KVRecord {
            key: e.key,
            value: KVValue::Some(e.value),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block::{BlockBuilder, Block},
        kvrecord::{KVWriteValue, KVValue},
    };

    use super::*;

    #[test]
    fn test_block_iterator_iteration() {
        // Create a block with test data using BlockBuilder
        let mut builder = BlockBuilder::new();
        
        // Add some test entries
        builder.add(b"apple", KVWriteValue::Some(b"red fruit")).unwrap();
        builder.add(b"banana", KVWriteValue::Some(b"yellow fruit")).unwrap();
        builder.add(b"cherry", KVWriteValue::Some(b"small red fruit")).unwrap();
        builder.add(b"date", KVWriteValue::Some(b"brown fruit")).unwrap();
        
        // Build the block
        let block = builder.build();
        
        // Create an iterator for the block
        let mut iterator = BlockIterator::create(block);
        
        // Test iteration by collecting all items
        let items: Vec<KVRecord> = iterator.collect();
        
        // Verify we got the expected number of items
        assert_eq!(items.len(), 4);
        
        // Verify the keys and values are correct and in the right order
        assert_eq!(items[0].key, b"apple");
        assert_eq!(items[0].value, KVValue::Some(b"red fruit".to_vec()));
        
        assert_eq!(items[1].key, b"banana");
        assert_eq!(items[1].value, KVValue::Some(b"yellow fruit".to_vec()));
        
        assert_eq!(items[2].key, b"cherry");
        assert_eq!(items[2].value, KVValue::Some(b"small red fruit".to_vec()));
        
        assert_eq!(items[3].key, b"date");
        assert_eq!(items[3].value, KVValue::Some(b"brown fruit".to_vec()));
    }
}
