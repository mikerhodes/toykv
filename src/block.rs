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

use std::io::{Cursor, Read};

use crate::kvrecord::KVWriteValue;

const BLOCK_SIZE: usize = 4096;

#[derive(Debug, PartialEq)]
pub(crate) enum BlockBuilderError {
    BlockFull,
    KeyTooLarge,
    ValueTooLarge,
}

pub(crate) struct BlockBuilder {
    // serialised entries
    entry_data: Vec<u8>,
    // offsets of entries into entries
    offsets: Vec<u16>,
}
impl BlockBuilder {
    pub(crate) fn new() -> BlockBuilder {
        BlockBuilder {
            entry_data: vec![],
            offsets: vec![],
        }
    }

    /// Returns true if k/v successfully added to the block, or
    /// false if the block is full.
    pub(crate) fn add(&mut self, key: &[u8], value: KVWriteValue) -> Result<(), BlockBuilderError> {
        // Make sure we don't make it too big by accident.
        assert!(BLOCK_SIZE < u16::MAX as usize);

        // Ensure key and value sizes fit in the u16 we store them as
        if key.len() > u16::MAX as usize {
            return Err(BlockBuilderError::KeyTooLarge);
        }
        if let KVWriteValue::Some(x) = value {
            if x.len() > u16::MAX as usize {
                return Err(BlockBuilderError::ValueTooLarge);
            }
        }

        let e = Entry {
            key: key.to_vec(),
            value: match value {
                KVWriteValue::Some(x) => x.to_vec(),
                KVWriteValue::Deleted => vec![],
            },
        };

        // If it is our first entry, we can go over the BLOCK_SIZE, as
        // otherwise we couldn't store the block.
        if !self.entry_data.is_empty() && self.entry_data.len() + e.size() > BLOCK_SIZE {
            return Err(BlockBuilderError::BlockFull);
        }
        // entry_data.len() must fit into u16 --- because it must be
        // less than BLOCK_SIZE, which we assert is < u16::MAX.
        self.offsets.push(self.entry_data.len() as u16);
        self.entry_data.extend(e.encode());

        Ok(())
    }

    /// build returns the completed Block, consuming self in the
    /// process.
    pub(crate) fn build(self) -> Block {
        Block {
            data: self.entry_data,
            offsets: self.offsets,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Block {
    data: Vec<u8>, // A vec of raw Entry data, at offsets
    offsets: Vec<u16>,
}
impl Block {
    // fn decode(data: &[u8]) -> Block {
    // A block is a series of encoded entries,
    // followed by the offset of each entry,
    // followed by the total number of entries.
    // }

    /// Encode to a byte vector. This may be longer or shorter than
    /// the BLOCK_SIZE, as large k/v pairs will create large blocks,
    /// while smaller k/v pairs will end up with some slack space
    /// (that we don't pad).
    /// The sstable file maintains an index of block offsets in its
    /// metadata to account for this variability.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];

        buf.extend(self.data.clone());
        for o in &self.offsets {
            buf.extend(o.to_be_bytes());
        }
        buf.extend((self.offsets.len() as u32).to_be_bytes());

        buf
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
}
impl Entry {
    fn size(&self) -> usize {
        return 2 + self.key.len() + 2 + self.value.len();
    }
    fn decode(data: &[u8]) -> Entry {
        let mut u16buf: [u8; 2] = [0u8; 2];
        let mut c = Cursor::new(data);

        assert!(matches!(c.read(&mut u16buf), Ok(2)));
        let keylen = u16::from_be_bytes(u16buf);
        let mut key = vec![0; keylen as usize];
        assert!(matches!(c.read(&mut key), Ok(n) if n == keylen as usize));

        assert!(matches!(c.read(&mut u16buf), Ok(2)));
        let valuelen = u16::from_be_bytes(u16buf);
        let mut value = vec![0; valuelen as usize];
        assert!(matches!(c.read(&mut value), Ok(n) if n == valuelen as usize));

        Entry { key, value }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];

        buf.extend((self.key.len() as u16).to_be_bytes());
        buf.extend(self.key.clone());
        buf.extend((self.value.len() as u16).to_be_bytes());
        buf.extend(self.value.clone());

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_roundtrip() {
        let original = Entry {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };

        let encoded = original.encode();
        let decoded = Entry::decode(&encoded);

        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.value, original.value);
    }

    #[test]
    fn test_entry_size() {
        let entry = Entry {
            key: b"test mctestface".to_vec(),
            value: b"data".to_vec(),
        };

        assert_eq!(entry.size(), 23); // 2 + 15 + 2 + 4
        assert_eq!(entry.size(), entry.encode().len());
    }

    #[test]
    #[should_panic]
    fn test_decode_insufficient_data_for_keylen() {
        let data = vec![0u8]; // Only 1 byte, need 2 for keylen
        Entry::decode(&data);
    }

    #[test]
    #[should_panic]
    fn test_decode_insufficient_data_for_key() {
        let mut data = vec![];
        data.extend(5u16.to_be_bytes()); // Says key is 5 bytes
        data.extend(b"hi"); // But only provide 2 bytes
        Entry::decode(&data);
    }

    #[test]
    #[should_panic]
    fn test_decode_insufficient_data_for_valuelen() {
        let mut data = vec![];
        data.extend(2u16.to_be_bytes()); // Key length
        data.extend(b"hi"); // Key data
        data.push(0u8); // Only 1 byte for value length, need 2
        Entry::decode(&data);
    }

    #[test]
    #[should_panic]
    fn test_decode_insufficient_data_for_value() {
        let mut data = vec![];
        data.extend(2u16.to_be_bytes()); // Key length
        data.extend(b"hi"); // Key data
        data.extend(10u16.to_be_bytes()); // Says value is 10 bytes
        data.extend(b"short"); // But only provide 5 bytes
        Entry::decode(&data);
    }

    // BlockBuilder Tests
    #[test]
    fn test_block_builder_new() {
        let builder = BlockBuilder::new();
        assert_eq!(builder.entry_data.len(), 0);
        assert_eq!(builder.offsets.len(), 0);
    }

    #[test]
    fn test_block_builder_empty_build() {
        let builder = BlockBuilder::new();
        let block = builder.build();
        assert_eq!(block.data.len(), 0);
        assert_eq!(block.offsets.len(), 0);
    }

    #[test]
    fn test_block_builder_add_single_entry() {
        let mut builder = BlockBuilder::new();
        let result = builder.add(b"key", KVWriteValue::Some(b"value"));
        assert!(result.is_ok());

        let block = builder.build();
        assert_eq!(block.offsets.len(), 1);
        assert_eq!(block.offsets[0], 0); // First entry at offset 0

        // Entry size: 2 + 3 + 2 + 5 = 12 bytes
        assert_eq!(block.data.len(), 12);
    }

    #[test]
    fn test_block_builder_add_multiple_entries() {
        let mut builder = BlockBuilder::new();

        // Add first entry
        let result1 = builder.add(b"key1", KVWriteValue::Some(b"value1"));
        assert!(result1.is_ok());

        // Add second entry
        let result2 = builder.add(b"key2", KVWriteValue::Some(b"value2"));
        assert!(result2.is_ok());

        let block = builder.build();
        assert_eq!(block.offsets.len(), 2);
        // First entry at offset 0
        assert_eq!(block.offsets[0], 0);
        // Second entry at offset 14 - first entry is 14 bytes, next starts
        // at 15th array location, ie 14 in zero-based array.
        assert_eq!(block.offsets[1], 14);
    }

    #[test]
    fn test_block_builder_add_deleted_value() {
        let mut builder = BlockBuilder::new();
        let result = builder.add(b"deleted_key", KVWriteValue::Deleted);
        assert!(result.is_ok());

        let block = builder.build();
        assert_eq!(block.offsets.len(), 1);
        // Entry size: 2 + 11 + 2 + 0 = 15 bytes (deleted values have 0 length)
        assert_eq!(block.data.len(), 15);
    }

    #[test]
    fn test_block_builder_key_too_large() {
        let mut builder = BlockBuilder::new();
        let large_key = vec![b'x'; (u16::MAX as usize) + 1];
        let result = builder.add(&large_key, KVWriteValue::Some(b"value"));

        assert!(matches!(result, Err(BlockBuilderError::KeyTooLarge)));
    }

    #[test]
    fn test_block_builder_value_too_large() {
        let mut builder = BlockBuilder::new();
        let large_value = vec![b'x'; (u16::MAX as usize) + 1];
        let result = builder.add(b"key", KVWriteValue::Some(&large_value));

        assert!(matches!(result, Err(BlockBuilderError::ValueTooLarge)));
    }

    #[test]
    fn test_block_builder_max_size_key_and_value() {
        let mut builder = BlockBuilder::new();
        let max_key = vec![b'k'; u16::MAX as usize];
        let max_value = vec![b'v'; u16::MAX as usize];
        let result = builder.add(&max_key, KVWriteValue::Some(&max_value));

        // Should succeed with maximum sizes
        assert!(result.is_ok());
    }

    #[test]
    fn test_block_builder_first_entry_can_exceed_block_size() {
        let mut builder = BlockBuilder::new();
        // Create an entry larger than BLOCK_SIZE
        let large_key = vec![b'k'; 2000];
        let large_value = vec![b'v'; 3000]; // Total entry > 4096 bytes

        let result = builder.add(&large_key, KVWriteValue::Some(&large_value));
        assert!(result.is_ok()); // First entry should succeed even if large

        let block = builder.build();
        assert!(block.data.len() > BLOCK_SIZE);
    }

    #[test]
    fn test_block_builder_subsequent_entry_blocked_by_size() {
        let mut builder = BlockBuilder::new();

        // Add a large first entry (close to BLOCK_SIZE)
        let large_key = vec![b'k'; 2000];
        let large_value = vec![b'v'; 2085]; // ~4085 bytes + 4 byte overhead
        let result1 = builder.add(&large_key, KVWriteValue::Some(&large_value));
        assert!(result1.is_ok());

        // Try to add another entry that would exceed BLOCK_SIZE
        let result2 = builder.add(b"key2", KVWriteValue::Some(b"value2"));
        assert!(matches!(result2, Err(BlockBuilderError::BlockFull)));
    }

    #[test]
    fn test_block_builder_fits_multiple_small_entries() {
        let mut builder = BlockBuilder::new();
        let mut entries_added = 0;

        // Add many small entries until we approach BLOCK_SIZE
        // We should fit 227*(6+2+8+2) = 4086 bytes
        // Set our range loop larger to ensure we exit.
        for i in 0..500 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);

            match builder.add(key.as_bytes(), KVWriteValue::Some(value.as_bytes())) {
                Ok(()) => entries_added += 1,
                Err(BlockBuilderError::BlockFull) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        // Should be able to add many small entries
        assert_eq!(entries_added, 227);

        let block = builder.build();
        assert_eq!(block.offsets.len(), entries_added);
        // Block size should be close to but not exceed BLOCK_SIZE
        assert_eq!(block.data.len(), 4086);
    }

    #[test]
    fn test_block_builder_offset_tracking() {
        let mut builder = BlockBuilder::new();

        // Add entries of known sizes
        builder.add(b"a", KVWriteValue::Some(b"1")).unwrap(); // 2+1+2+1 = 6 bytes
        builder.add(b"bb", KVWriteValue::Some(b"22")).unwrap(); // 2+2+2+2 = 8 bytes
        builder.add(b"ccc", KVWriteValue::Some(b"333")).unwrap(); // 2+3+2+3 = 10 bytes

        let block = builder.build();
        assert_eq!(block.offsets.len(), 3);
        assert_eq!(block.offsets[0], 0); // First entry at 0
        assert_eq!(block.offsets[1], 6); // Second entry at 6
        assert_eq!(block.offsets[2], 14); // Third entry at 14 (6+8)
    }

    #[test]
    fn test_block_builder_mixed_deleted_and_live_values() {
        let mut builder = BlockBuilder::new();

        // len 5+2+5+2 = 14
        builder.add(b"live1", KVWriteValue::Some(b"data1")).unwrap();
        // len 8 + 2 + 0 + 2 = 12
        builder.add(b"deleted1", KVWriteValue::Deleted).unwrap();
        // len 14
        builder.add(b"live2", KVWriteValue::Some(b"data2")).unwrap();
        // len 12
        builder.add(b"deleted2", KVWriteValue::Deleted).unwrap();

        let block = builder.build();
        assert_eq!(block.offsets.len(), 4);

        // Verify the data was written correctly by checking total size
        assert_eq!(block.data.len(), 14 + 12 + 14 + 12);
    }

    #[test]
    fn test_block_size_constant_sanity() {
        // Verify our assumption that BLOCK_SIZE < u16::MAX holds
        assert!(BLOCK_SIZE < u16::MAX as usize);
        assert_eq!(BLOCK_SIZE, 4096);
    }

    #[test]
    fn test_block_builder_exactly_at_block_size_boundary() {
        let mut builder = BlockBuilder::new();

        // Let's make 32 byte entries, 4096/32 = 128
        // 14 + 2 + 14 + 2

        // Add entries until we're close to the limit
        for i in 1..129 {
            let key = format!("keykeykey{:05}", i);
            let value = format!("valuevalue{:04}", i);
            let result = builder.add(key.as_bytes(), KVWriteValue::Some(value.as_bytes()));
            assert!(
                result.is_ok(),
                "Failed to add entry {} at size {}",
                i,
                builder.entry_data.len()
            );
        }

        // The next entry should fail due to block being full
        let result = builder.add(b"overflow", KVWriteValue::Some(b"overflow"));
        assert!(matches!(result, Err(BlockBuilderError::BlockFull)));

        let block = builder.build();
        assert_eq!(block.data.len(), BLOCK_SIZE);
        assert_eq!(block.offsets.len(), 128)
    }

    #[test]
    fn test_block_encode() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", KVWriteValue::Some(b"value1")).unwrap();
        builder.add(b"key2", KVWriteValue::Some(b"value2")).unwrap();

        let block = builder.build();
        let encoded = block.encode();

        // Encoded should contain: entry_data + all offsets + num_entries
        // 2 entries * 14 bytes each = 28 bytes of entry data
        // 2 offsets * 2 bytes each = 4 bytes of offset data
        // 1 num_entries * 4 bytes = 4 bytes of num_entries
        // Total = 36 bytes
        assert_eq!(encoded.len(), 36);

        // Verify that the encoded data starts with the entry data
        assert_eq!(&encoded[0..block.data.len()], &block.data);

        // Verify that the offsets are in the middle
        let offsets_start = block.data.len();
        assert_eq!(
            &encoded[offsets_start..offsets_start + 2],
            &0u16.to_be_bytes()
        );
        assert_eq!(
            &encoded[offsets_start + 2..offsets_start + 4],
            &14u16.to_be_bytes()
        );

        // Verify that the number of entries is at the end
        let num_entries_bytes = &encoded[32..36];
        assert_eq!(num_entries_bytes, &2u32.to_be_bytes());
    }

    #[test]
    fn test_block_builder_comprehensive_scenario() {
        let mut builder = BlockBuilder::new();
        let mut total_entries = 0;

        // Test a realistic scenario with mixed entry sizes
        let test_data = vec![
            (b"user:1".as_slice(), KVWriteValue::Some(b"john_doe")),
            (b"user:2".as_slice(), KVWriteValue::Some(b"jane_smith")),
            (b"config:timeout".as_slice(), KVWriteValue::Some(b"30")),
            (b"config:retries".as_slice(), KVWriteValue::Some(b"3")),
            (b"deleted_user:old".as_slice(), KVWriteValue::Deleted),
            (b"session:abc123".as_slice(), KVWriteValue::Some(b"active")),
        ];

        // 97 + 12 + 12 = 121

        for (key, value) in test_data {
            match builder.add(key, value) {
                Ok(()) => total_entries += 1,
                Err(BlockBuilderError::BlockFull) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert_eq!(total_entries, 6); // All entries should fit

        let block = builder.build();
        assert_eq!(block.offsets.len(), 6);
        assert_eq!(block.data.len(), 121);

        // Test encoding
        let encoded = block.encode();
        assert_eq!(encoded.len(), block.data.len() + (6 * 2) + 4); // data + 6 offsets * 2 bytes each + 4 bytes for num_entries
    }

    #[test]
    fn test_block_encode_num_entries_field() {
        // Test empty block
        let empty_builder = BlockBuilder::new();
        let empty_block = empty_builder.build();
        let empty_encoded = empty_block.encode();

        // Empty block should have 4 bytes for num_entries = 0
        assert_eq!(empty_encoded.len(), 4);
        assert_eq!(empty_encoded, &0u32.to_be_bytes());

        // Test single entry block
        let mut single_builder = BlockBuilder::new();
        single_builder
            .add(b"key", KVWriteValue::Some(b"value"))
            .unwrap();
        let single_block = single_builder.build();
        let single_encoded = single_block.encode();

        // Extract num_entries from the end
        let num_entries_start = single_encoded.len() - 4;
        assert_eq!(&single_encoded[num_entries_start..], &1u32.to_be_bytes());

        // Test multiple entries block
        let mut multi_builder = BlockBuilder::new();
        let num_test_entries = 5;
        for i in 0..num_test_entries {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            multi_builder
                .add(key.as_bytes(), KVWriteValue::Some(value.as_bytes()))
                .unwrap();
        }
        let multi_block = multi_builder.build();
        let multi_encoded = multi_block.encode();

        // Extract num_entries from the end
        let num_entries_start = multi_encoded.len() - 4;
        assert_eq!(
            &multi_encoded[num_entries_start..],
            (num_test_entries as u32).to_be_bytes()
        );

        // Test with deleted entries
        let mut deleted_builder = BlockBuilder::new();
        deleted_builder
            .add(b"live", KVWriteValue::Some(b"data"))
            .unwrap();
        deleted_builder.add(b"dead", KVWriteValue::Deleted).unwrap();
        deleted_builder
            .add(b"also_live", KVWriteValue::Some(b"more_data"))
            .unwrap();
        let deleted_block = deleted_builder.build();
        let deleted_encoded = deleted_block.encode();

        // Extract num_entries from the end
        let num_entries_start = deleted_encoded.len() - 4;
        assert_eq!(&deleted_encoded[num_entries_start..], &3u32.to_be_bytes());
    }
}
