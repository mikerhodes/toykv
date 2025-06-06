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
            value: if e.value.len() > 0 {
                KVValue::Some(e.value)
            } else {
                KVValue::Deleted
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block::{Block, BlockBuilder},
        kvrecord::{KVValue, KVWriteValue},
    };

    use super::*;

    #[test]
    fn test_block_iterator_iteration() {
        // Create a block with test data using BlockBuilder
        let mut builder = BlockBuilder::new();

        // Add some test entries
        builder
            .add(b"apple", KVWriteValue::Some(b"red fruit"))
            .unwrap();
        builder
            .add(b"banana", KVWriteValue::Some(b"yellow fruit"))
            .unwrap();
        builder
            .add(b"cherry", KVWriteValue::Some(b"small red fruit"))
            .unwrap();
        builder
            .add(b"date", KVWriteValue::Some(b"brown fruit"))
            .unwrap();

        // Build the block
        let block = builder.build();

        // Create an iterator for the block
        let iterator = BlockIterator::create(block);

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

    #[test]
    fn test_block_iterator_empty_block() {
        // Test iterator on empty block
        let builder = BlockBuilder::new();
        let empty_block = builder.build();
        let mut iterator = BlockIterator::create(empty_block);

        // Should return None immediately for empty block
        assert_eq!(iterator.next(), None);
        assert_eq!(iterator.next(), None); // Multiple calls should still return None

        // Collecting should give empty vector
        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_block_iterator_single_entry() {
        // Test iterator with exactly one entry
        let mut builder = BlockBuilder::new();
        builder
            .add(b"only_key", KVWriteValue::Some(b"only_value"))
            .unwrap();

        let block = builder.build();
        let mut iterator = BlockIterator::create(block);

        // First call should return the entry
        let first = iterator.next();
        assert!(first.is_some());
        let record = first.unwrap();
        assert_eq!(record.key, b"only_key");
        assert_eq!(record.value, KVValue::Some(b"only_value".to_vec()));

        // Second call should return None
        assert_eq!(iterator.next(), None);

        // Subsequent calls should still return None
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_block_iterator_with_deleted_entries() {
        // Test iterator behavior with deleted entries - this exposes a bug!
        // The iterator doesn't properly handle deleted entries
        let mut builder = BlockBuilder::new();
        builder.add(b"live1", KVWriteValue::Some(b"data1")).unwrap();
        builder.add(b"deleted1", KVWriteValue::Deleted).unwrap();
        builder.add(b"live2", KVWriteValue::Some(b"data2")).unwrap();
        builder.add(b"deleted2", KVWriteValue::Deleted).unwrap();

        let block = builder.build();
        let iterator = BlockIterator::create(block);

        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), 4);

        // BUG: The iterator currently creates KVValue::Some for deleted entries
        // instead of KVValue::Deleted. The deleted entries have empty values,
        // so they become KVValue::Some(vec![]) instead of KVValue::Deleted
        assert_eq!(items[0].key, b"live1");
        assert_eq!(items[0].value, KVValue::Some(b"data1".to_vec()));

        assert_eq!(items[1].key, b"deleted1");
        assert_eq!(items[1].value, KVValue::Deleted);

        assert_eq!(items[2].key, b"live2");
        assert_eq!(items[2].value, KVValue::Some(b"data2".to_vec()));

        assert_eq!(items[3].key, b"deleted2");
        assert_eq!(items[3].value, KVValue::Deleted);
    }

    #[test]
    fn test_block_iterator_exhaustion_behavior() {
        // Test iterator behavior after exhaustion
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", KVWriteValue::Some(b"value1")).unwrap();
        builder.add(b"key2", KVWriteValue::Some(b"value2")).unwrap();

        let block = builder.build();
        let mut iterator = BlockIterator::create(block);

        // Consume all entries
        let first = iterator.next();
        assert!(first.is_some());
        let second = iterator.next();
        assert!(second.is_some());

        // Iterator should be exhausted now
        assert_eq!(iterator.next(), None);
        assert_eq!(iterator.next(), None);
        assert_eq!(iterator.next(), None); // Multiple calls should be safe

        // Try collecting after exhaustion
        let remaining: Vec<KVRecord> = iterator.collect();
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_block_iterator_step_by_step() {
        // Test step-by-step iteration instead of collect()
        let mut builder = BlockBuilder::new();
        builder.add(b"first", KVWriteValue::Some(b"1")).unwrap();
        builder.add(b"second", KVWriteValue::Some(b"2")).unwrap();
        builder.add(b"third", KVWriteValue::Some(b"3")).unwrap();

        let block = builder.build();
        let mut iterator = BlockIterator::create(block);

        // First entry
        let first = iterator.next().unwrap();
        assert_eq!(first.key, b"first");
        assert_eq!(first.value, KVValue::Some(b"1".to_vec()));

        // Second entry
        let second = iterator.next().unwrap();
        assert_eq!(second.key, b"second");
        assert_eq!(second.value, KVValue::Some(b"2".to_vec()));

        // Third entry
        let third = iterator.next().unwrap();
        assert_eq!(third.key, b"third");
        assert_eq!(third.value, KVValue::Some(b"3".to_vec()));

        // Should be exhausted
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_block_iterator_large_entries() {
        // Test iterator with maximum size entries
        let mut builder = BlockBuilder::new();

        // Create large key and value (but within u16 limits)
        let large_key = vec![b'K'; 1000];
        let large_value = vec![b'V'; 2000];

        builder
            .add(&large_key, KVWriteValue::Some(&large_value))
            .unwrap();

        let block = builder.build();
        let mut iterator = BlockIterator::create(block);

        let entry = iterator.next().unwrap();
        assert_eq!(entry.key.len(), 1000);
        assert_eq!(entry.value, KVValue::Some(large_value));
        assert!(entry.key.iter().all(|&b| b == b'K'));

        // Should be exhausted after one large entry
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_block_iterator_many_small_entries() {
        // Test iterator with many small entries
        let mut builder = BlockBuilder::new();
        let num_entries = 100;

        for i in 0..num_entries {
            let key = format!("key{:03}", i);
            let value = format!("val{:03}", i);
            match builder.add(key.as_bytes(), KVWriteValue::Some(value.as_bytes())) {
                Ok(()) => {}
                Err(_) => break, // Block is full
            }
        }

        let block = builder.build();
        let expected_count = block.offsets.len(); // Actual entries that fit
        let iterator = BlockIterator::create(block);

        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), expected_count);

        // Verify entries are in order and correct
        for (i, item) in items.iter().enumerate() {
            let expected_key = format!("key{:03}", i);
            let expected_value = format!("val{:03}", i);
            assert_eq!(item.key, expected_key.as_bytes());
            assert_eq!(
                item.value,
                KVValue::Some(expected_value.as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_block_iterator_mixed_entry_sizes() {
        // Test iterator with entries of varying sizes
        let mut builder = BlockBuilder::new();

        // Tiny entry
        builder.add(b"a", KVWriteValue::Some(b"1")).unwrap();

        // Medium entry
        builder
            .add(
                b"medium_sized_key",
                KVWriteValue::Some(b"medium_sized_value"),
            )
            .unwrap();

        // Large entry (but reasonable)
        let large_key = vec![b'L'; 100];
        let large_value = vec![b'V'; 200];
        builder
            .add(&large_key, KVWriteValue::Some(&large_value))
            .unwrap();

        // Deleted entry
        builder
            .add(b"deleted_entry", KVWriteValue::Deleted)
            .unwrap();

        // Another small entry
        builder.add(b"b", KVWriteValue::Some(b"2")).unwrap();

        let block = builder.build();
        let iterator = BlockIterator::create(block);

        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), 5);

        // Verify each entry
        assert_eq!(items[0].key, b"a");
        assert_eq!(items[0].value, KVValue::Some(b"1".to_vec()));

        assert_eq!(items[1].key, b"medium_sized_key");
        assert_eq!(
            items[1].value,
            KVValue::Some(b"medium_sized_value".to_vec())
        );

        assert_eq!(items[2].key.len(), 100);
        assert!(items[2].key.iter().all(|&b| b == b'L'));
        assert_eq!(items[2].value, KVValue::Some(large_value));

        assert_eq!(items[3].key, b"deleted_entry");
        assert_eq!(items[3].value, KVValue::Deleted);

        assert_eq!(items[4].key, b"b");
        assert_eq!(items[4].value, KVValue::Some(b"2".to_vec()));
    }

    #[test]
    fn test_block_iterator_duplicate_keys() {
        // Test iterator with duplicate keys (which BlockBuilder allows)
        let mut builder = BlockBuilder::new();

        builder
            .add(b"duplicate", KVWriteValue::Some(b"first"))
            .unwrap();
        builder
            .add(b"other", KVWriteValue::Some(b"middle"))
            .unwrap();
        builder
            .add(b"duplicate", KVWriteValue::Some(b"second"))
            .unwrap();
        builder.add(b"duplicate", KVWriteValue::Deleted).unwrap();

        let block = builder.build();
        let iterator = BlockIterator::create(block);

        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), 4);

        // All duplicate key entries should be preserved in order
        assert_eq!(items[0].key, b"duplicate");
        assert_eq!(items[0].value, KVValue::Some(b"first".to_vec()));

        assert_eq!(items[1].key, b"other");
        assert_eq!(items[1].value, KVValue::Some(b"middle".to_vec()));

        assert_eq!(items[2].key, b"duplicate");
        assert_eq!(items[2].value, KVValue::Some(b"second".to_vec()));

        assert_eq!(items[3].key, b"duplicate");
        assert_eq!(items[3].value, KVValue::Deleted);
    }

    #[test]
    fn test_block_iterator_boundary_offsets() {
        // Test iterator with entries that create specific offset patterns
        let mut builder = BlockBuilder::new();

        // Create entries with predictable sizes to test offset boundaries
        // Entry 1: key="x" (1) + value="y" (1) + headers (4) = 6 bytes total
        builder.add(b"x", KVWriteValue::Some(b"y")).unwrap();

        // Entry 2: key="ab" (2) + value="cd" (2) + headers (4) = 8 bytes total
        // Should start at offset 6
        builder.add(b"ab", KVWriteValue::Some(b"cd")).unwrap();

        // Entry 3: key="efg" (3) + value="hij" (3) + headers (4) = 10 bytes total
        // Should start at offset 14
        builder.add(b"efg", KVWriteValue::Some(b"hij")).unwrap();

        let block = builder.build();

        // Verify expected offsets before testing iterator
        assert_eq!(block.offsets.len(), 3);
        assert_eq!(block.offsets[0], 0);
        assert_eq!(block.offsets[1], 6);
        assert_eq!(block.offsets[2], 14);

        let iterator = BlockIterator::create(block);

        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), 3);

        assert_eq!(items[0].key, b"x");
        assert_eq!(items[0].value, KVValue::Some(b"y".to_vec()));

        assert_eq!(items[1].key, b"ab");
        assert_eq!(items[1].value, KVValue::Some(b"cd".to_vec()));

        assert_eq!(items[2].key, b"efg");
        assert_eq!(items[2].value, KVValue::Some(b"hij".to_vec()));
    }

    #[test]
    fn test_block_iterator_decode_from_encoded_block() {
        // Test iterator on a block that has been encoded and decoded
        let mut builder = BlockBuilder::new();

        builder
            .add(b"encode_test", KVWriteValue::Some(b"should_work"))
            .unwrap();
        builder.add(b"deleted_test", KVWriteValue::Deleted).unwrap();
        builder
            .add(b"final_test", KVWriteValue::Some(b"final_value"))
            .unwrap();

        let original_block = builder.build();
        let encoded = original_block.encode();
        let decoded_block = Block::decode(&encoded);

        // Iterator should work the same on decoded block
        let iterator = BlockIterator::create(decoded_block);

        let items: Vec<KVRecord> = iterator.collect();
        assert_eq!(items.len(), 3);

        assert_eq!(items[0].key, b"encode_test");
        assert_eq!(items[0].value, KVValue::Some(b"should_work".to_vec()));

        assert_eq!(items[1].key, b"deleted_test");
        assert_eq!(items[1].value, KVValue::Deleted);

        assert_eq!(items[2].key, b"final_test");
        assert_eq!(items[2].value, KVValue::Some(b"final_value".to_vec()));
    }

    #[test]
    fn test_block_iterator_partial_consumption() {
        // Test that iterator state is maintained across partial consumption
        let mut builder = BlockBuilder::new();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            builder
                .add(key.as_bytes(), KVWriteValue::Some(value.as_bytes()))
                .unwrap();
        }

        let block = builder.build();
        let mut iterator = BlockIterator::create(block);

        // Consume first 2 entries
        let first = iterator.next().unwrap();
        assert_eq!(first.key, b"key0");

        let second = iterator.next().unwrap();
        assert_eq!(second.key, b"key1");

        // Collect remaining entries
        let remaining: Vec<KVRecord> = iterator.collect();
        assert_eq!(remaining.len(), 3);

        assert_eq!(remaining[0].key, b"key2");
        assert_eq!(remaining[1].key, b"key3");
        assert_eq!(remaining[2].key, b"key4");
    }
}
