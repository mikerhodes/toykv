#![allow(dead_code)]
use std::sync::Arc;

use crate::{
    block::{Block, Entry},
    kvrecord::{KVRecord, KVValue},
};

pub(crate) struct BlockIterator {
    b: Arc<Block>,
    curr_offset_idx: usize,
}
impl BlockIterator {
    fn create(b: Arc<Block>) -> BlockIterator {
        BlockIterator {
            b,
            curr_offset_idx: 0,
        }
    }

    fn create_and_seek_to_key(b: Arc<Block>, k: &[u8]) -> BlockIterator {
        let it = BlockIterator {
            b: b.clone(),
            curr_offset_idx: 0,
        };
        // use this iterator to find the offset idx for the first key
        // yes this is pretty inefficient what with copying key and value :(
        // It's probably not that much work to add a method that reads the
        // key at an offset and uses that here instead, once we have the
        // tests. (But it will break the "entry" abstraction, maybe we
        // can have a borrow-decode that gives you byte slices).
        let mut idx = 0;
        for b in it {
            if &b.key[..] >= k {
                break;
            }
            idx += 1;
        }

        // If the key isn't in the block, idx will be at the end
        // so next() will return None.
        BlockIterator {
            b,
            curr_offset_idx: idx,
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
        let iterator = BlockIterator::create(Arc::new(block));

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
        let mut iterator = BlockIterator::create(Arc::new(empty_block));

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
        let mut iterator = BlockIterator::create(Arc::new(block));

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
        let iterator = BlockIterator::create(Arc::new(block));

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
        let mut iterator = BlockIterator::create(Arc::new(block));

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
        let mut iterator = BlockIterator::create(Arc::new(block));

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
        let mut iterator = BlockIterator::create(Arc::new(block));

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
            match builder
                .add(key.as_bytes(), KVWriteValue::Some(value.as_bytes()))
            {
                Ok(()) => {}
                Err(_) => break, // Block is full
            }
        }

        let block = builder.build();
        let expected_count = block.offsets.len(); // Actual entries that fit
        let iterator = BlockIterator::create(Arc::new(block));

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
        let iterator = BlockIterator::create(Arc::new(block));

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
        let iterator = BlockIterator::create(Arc::new(block));

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

        let iterator = BlockIterator::create(Arc::new(block));

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
        let iterator = BlockIterator::create(Arc::new(decoded_block));

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
        let mut iterator = BlockIterator::create(Arc::new(block));

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

    #[test]
    fn test_block_iterator_seek_to_key() {
        // Test that iterator state is maintained across partial consumption
        let mut builder = BlockBuilder::new();

        for i in 0..50 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            builder
                .add(key.as_bytes(), KVWriteValue::Some(value.as_bytes()))
                .unwrap();
        }

        let block = Arc::new(builder.build());

        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"key001");
        let first = it.next().unwrap();
        assert_eq!(first.key, b"key001");
        it = BlockIterator::create_and_seek_to_key(block.clone(), b"key010");
        let first = it.next().unwrap();
        assert_eq!(first.key, b"key010");
        it = BlockIterator::create_and_seek_to_key(block.clone(), b"key049");
        let first = it.next().unwrap();
        assert_eq!(first.key, b"key049");
        it = BlockIterator::create_and_seek_to_key(block.clone(), b"aaa");
        let first = it.next().unwrap();
        assert_eq!(first.key, b"key000");
        it = BlockIterator::create_and_seek_to_key(block.clone(), b"zzz");
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_block_iterator_seek_to_key_empty() {
        // Test that iterator state is maintained across partial consumption
        let builder = BlockBuilder::new();
        let block = Arc::new(builder.build());
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"aaa");
        assert_eq!(it.next(), None);
        it = BlockIterator::create_and_seek_to_key(block.clone(), b"zzz");
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_seek_empty_key_target() {
        // Test seeking to empty key b""
        let mut builder = BlockBuilder::new();
        builder.add(b"a", KVWriteValue::Some(b"1")).unwrap();
        builder.add(b"b", KVWriteValue::Some(b"2")).unwrap();
        let block = Arc::new(builder.build());

        let mut it = BlockIterator::create_and_seek_to_key(block, b"");
        let first = it.next().unwrap();
        assert_eq!(first.key, b"a"); // Should position at first key
    }

    #[test]
    fn test_seek_to_key_containing_null_bytes() {
        // Test with keys containing null bytes and other special values
        let mut builder = BlockBuilder::new();
        builder
            .add(b"before\x00key", KVWriteValue::Some(b"val1"))
            .unwrap();
        builder
            .add(b"normal_key", KVWriteValue::Some(b"val2"))
            .unwrap();
        builder
            .add(b"after\xff\x00", KVWriteValue::Some(b"val3"))
            .unwrap();
        let block = Arc::new(builder.build());

        // Test seeking to keys with null bytes
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"before\x00");
        let result = it.next().unwrap();
        assert_eq!(result.key, b"before\x00key");

        // Test seeking to exact null byte sequences
        let mut it =
            BlockIterator::create_and_seek_to_key(block, b"normal\x00");
        let result = it.next().unwrap();
        assert_eq!(result.key, b"normal_key");
    }

    #[test]
    fn test_seek_prefix_vs_full_key_comparison() {
        // Test cases where target key is prefix of existing key or vice versa
        let mut builder = BlockBuilder::new();
        builder.add(b"key", KVWriteValue::Some(b"short")).unwrap();
        builder
            .add(b"key_extended", KVWriteValue::Some(b"long"))
            .unwrap();
        builder
            .add(b"key_more", KVWriteValue::Some(b"longer"))
            .unwrap();
        let block = Arc::new(builder.build());

        // Prefix first key
        let mut it = BlockIterator::create_and_seek_to_key(block.clone(), b"k");
        assert_eq!(it.next().unwrap().key, b"key");
        // Prefix plus lower-than content
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"kaaaaaaa");
        assert_eq!(it.next().unwrap().key, b"key");
        // Prefix key middle
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"key_");
        assert_eq!(it.next().unwrap().key, b"key_extended");
        let mut it = BlockIterator::create_and_seek_to_key(
            block.clone(),
            b"key_extendeda",
        );
        assert_eq!(it.next().unwrap().key, b"key_more");
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"key_m");
        assert_eq!(it.next().unwrap().key, b"key_more");
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"key_a");
        assert_eq!(it.next().unwrap().key, b"key_extended");
        // Longer/higher than last key
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"key_morea");
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_seek_single_byte_differences() {
        // Test keys that differ by single byte in various positions
        let mut builder = BlockBuilder::new();
        builder.add(b"aaa", KVWriteValue::Some(b"1")).unwrap();
        builder.add(b"aab", KVWriteValue::Some(b"2")).unwrap();
        builder.add(b"aba", KVWriteValue::Some(b"3")).unwrap();
        builder.add(b"baa", KVWriteValue::Some(b"4")).unwrap();
        let block = Arc::new(builder.build());

        // Should find exact matches and next greater for non-matches
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"aaa");
        assert_eq!(it.next().unwrap().key, b"aaa");

        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"aaa\x01");
        assert_eq!(it.next().unwrap().key, b"aab");

        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"aa\xff");
        assert_eq!(it.next().unwrap().key, b"aba");
    }

    #[test]
    fn test_seek_with_duplicate_keys() {
        // Test seeking when block has duplicate keys
        // TODO should builder be preventing this?
        let mut builder = BlockBuilder::new();
        builder.add(b"dup", KVWriteValue::Some(b"first")).unwrap();
        builder.add(b"dup", KVWriteValue::Some(b"second")).unwrap();
        let block = Arc::new(builder.build());

        // Should position at first occurrence
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"dup");
        let first = it.next().unwrap();
        assert_eq!(first.key, b"dup");
        assert_eq!(first.value, KVValue::Some(b"first".to_vec()));

        // Verify we can iterate through all duplicates
        let second = it.next().unwrap();
        assert_eq!(second.key, b"dup");
        assert_eq!(second.value, KVValue::Some(b"second".to_vec()));
    }

    #[test]
    fn test_seek_with_extreme_byte_values() {
        // Test with keys containing min/max byte values
        let mut builder = BlockBuilder::new();
        builder
            .add(b"\x00\x00\x00", KVWriteValue::Some(b"min"))
            .unwrap();
        builder
            .add(b"\x00\xff\x00", KVWriteValue::Some(b"mixed1"))
            .unwrap();
        builder
            .add(b"\x80\x80\x80", KVWriteValue::Some(b"middle"))
            .unwrap();
        builder
            .add(b"\xff\x00\xff", KVWriteValue::Some(b"mixed2"))
            .unwrap();
        builder
            .add(b"\xff\xff\xff", KVWriteValue::Some(b"max"))
            .unwrap();
        let block = Arc::new(builder.build());

        // Test seeking to various extreme values
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"\x00");
        assert_eq!(it.next().unwrap().key, b"\x00\x00\x00");

        let mut it = BlockIterator::create_and_seek_to_key(
            block.clone(),
            b"\xff\xff\xff\xff",
        );
        assert_eq!(it.next(), None); // Should be past end

        let mut it = BlockIterator::create_and_seek_to_key(block, b"\x80");
        assert_eq!(it.next().unwrap().key, b"\x80\x80\x80");
    }

    #[test]
    fn test_multiple_seeks_on_same_block() {
        // Test multiple seek operations on the same block
        let v = || KVWriteValue::Some(b"fruit");
        let mut builder = BlockBuilder::new();
        builder.add(b"apple", v()).unwrap();
        builder.add(b"banana", v()).unwrap();
        builder.add(b"cherry", v()).unwrap();
        let block = Arc::new(builder.build());

        // Multiple seeks should be independent
        let it = |x| BlockIterator::create_and_seek_to_key(block.clone(), x);
        let mut it1 = it(b"banana");
        let mut it2 = it(b"apple");
        let mut it3 = it(b"cherry");
        let mut it4 = it(b"banana");

        assert_eq!(it1.next().unwrap().key, b"banana");
        assert_eq!(it2.next().unwrap().key, b"apple");
        assert_eq!(it3.next().unwrap().key, b"cherry");
        assert_eq!(it4.next().unwrap().key, b"banana");

        // Continue iteration independently
        assert_eq!(it1.next().unwrap().key, b"cherry");
        assert_eq!(it2.next().unwrap().key, b"banana");
        assert_eq!(it3.next(), None);
        assert_eq!(it4.next().unwrap().key, b"cherry");
    }

    #[test]
    fn test_seek_with_deleted_entries_mixed() {
        // Test seeking behavior with mix of live and deleted entries
        let mut builder = BlockBuilder::new();
        builder
            .add(b"a_alive1", KVWriteValue::Some(b"data"))
            .unwrap();
        builder.add(b"a_deleted1", KVWriteValue::Deleted).unwrap();
        builder
            .add(b"b_alive2", KVWriteValue::Some(b"data"))
            .unwrap();
        builder.add(b"b_deleted2", KVWriteValue::Deleted).unwrap();
        builder
            .add(b"c_alive3", KVWriteValue::Some(b"data"))
            .unwrap();
        let block = Arc::new(builder.build());

        // Seek to deleted key
        let mut it =
            BlockIterator::create_and_seek_to_key(block.clone(), b"a_deleted1");
        let result = it.next().unwrap();
        assert_eq!(result.key, b"a_deleted1");
        assert_eq!(result.value, KVValue::Deleted);
        let result = it.next().unwrap();
        assert_eq!(result.key, b"b_alive2");

        // Seek between live and deleted
        let mut it =
            BlockIterator::create_and_seek_to_key(block, b"b_alive2\xff");
        assert_eq!(it.next().unwrap().key, b"b_deleted2");
    }

    #[test]
    fn test_seek_arc_block_memory_behavior() {
        // Test that Arc<Block> is handled properly across multiple seeks
        let mut builder = BlockBuilder::new();
        builder.add(b"memory", KVWriteValue::Some(b"test")).unwrap();
        builder.add(b"ref", KVWriteValue::Some(b"count")).unwrap();
        let block = Arc::new(builder.build());

        let original_strong_count = Arc::strong_count(&block);

        let it = |x| BlockIterator::create_and_seek_to_key(block.clone(), x);
        {
            // Create multiple iterators from same block
            let _it1 = it(b"memory");
            let _it2 = it(b"ref");
            let _it3 = it(b"nonexistent");

            // Reference count should increase appropriately
            assert!(Arc::strong_count(&block) > original_strong_count);
        }

        // After dropping iterators, count should return to original
        assert_eq!(Arc::strong_count(&block), original_strong_count);
    }
}
