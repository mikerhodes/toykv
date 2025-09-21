use std::{ops::Bound, sync::Arc};

use crate::{
    error::ToyKVError,
    table::{iterator::TableIterator, reader::TableReader},
};

/// ConcatIterator allows treating a series of sstable files as a single
/// large sorted run, ie as if they were one file. It allows seeking and
/// iterating over the contents of the sorted run, abstracting the
/// different files underneath.
struct ConcatIterator {
    readers: Vec<Arc<TableReader>>,
    current_reader_idx: usize,
    current_table_iterator: TableIterator,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
}

impl ConcatIterator {
    /// Create a bounded iterator that will scan the sorted run in readers.
    fn new(
        readers: Vec<Arc<TableReader>>,
        lower_bound: Bound<Vec<u8>>,
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<Self, ToyKVError> {
        let start_table_idx =
            ConcatIterator::find_starting_table_idx(&readers, &lower_bound);
        let current_table_iterator =
            TableIterator::new_bounded_with_tablereader(
                readers[start_table_idx].clone(),
                lower_bound.clone(),
                upper_bound.clone(),
            )?;
        Ok(ConcatIterator {
            readers,
            current_reader_idx: start_table_idx,
            current_table_iterator,
            lower_bound,
            upper_bound,
        })
    }

    /// Return the index of the TableReader in readers that
    /// contains the bound b. If the bound is unbounded or
    /// lower than the first reader, return 0. If the bound
    /// ends up being after the last reader (ie, outside the
    /// sorted run), return the last reader anyway; we can
    /// use TableIterator to seek past it and return zero
    /// results during iteration.
    fn find_starting_table_idx(
        readers: &Vec<Arc<TableReader>>,
        b: &Bound<Vec<u8>>,
    ) -> usize {
        // b => Unbounded, return 0.
        // b => Included(k), search for k <= last_key, or last block.
        // b => Excluded(k), search for k < last_key, or last block.
        if *b == Bound::Unbounded {
            0
        } else {
            let mut idx: usize = 0;

            // A simple linear scan for now.
            while idx < readers.len() {
                let (_, last_key) = readers[idx].key_range();
                // Break at the first table where the last key
                // is after the bound. If we never break,
                // return last block index, even though the
                // key isn't in that block either.
                match b {
                    Bound::Unbounded => panic!("Unreachable"),
                    Bound::Included(k) if k <= last_key => break,
                    Bound::Included(_) => {}
                    Bound::Excluded(k) if k < last_key => break,
                    Bound::Excluded(_) => {}
                }
                idx += 1;
            }

            // If we've gone through all readers without breaking,
            // return the last reader index
            if idx >= readers.len() {
                readers.len() - 1
            } else {
                idx
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{kvrecord::KVValue, table::builder::TableBuilder};
    use siphasher::sip::SipHasher13;
    use std::sync::Arc;

    fn create_test_table(keys: Vec<&[u8]>) -> Arc<TableReader> {
        let mut table_builder =
            TableBuilder::new(SipHasher13::new(), keys.len());

        for key in keys {
            table_builder
                .add(key, &KVValue::Some(b"test_value".to_vec()))
                .unwrap();
        }

        let temp_file =
            tempfile::NamedTempFile::new().expect("Failed to create temp file");
        table_builder.write(temp_file.path()).unwrap();

        let table_reader =
            TableReader::new(temp_file.path().to_path_buf()).unwrap();
        Arc::new(table_reader)
    }

    #[test]
    fn test_idx_of_bound_unbounded() {
        // Test with unbounded, should always return 0
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Unbounded,
        );
        assert_eq!(result, 0);
    }

    #[test]
    fn test_idx_of_bound_included_first_table() {
        // Key falls in the first table
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"apple".to_vec()),
        );
        assert_eq!(result, 0);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"banana".to_vec()),
        );
        assert_eq!(result, 0);
    }

    #[test]
    fn test_idx_of_bound_included_middle_table() {
        // Key falls in the middle table
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"cherry".to_vec()),
        );
        assert_eq!(result, 1);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"date".to_vec()),
        );
        assert_eq!(result, 1);
    }

    #[test]
    fn test_idx_of_bound_included_last_table() {
        // Key falls in the last table
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"elderberry".to_vec()),
        );
        assert_eq!(result, 2);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"fig".to_vec()),
        );
        assert_eq!(result, 2);
    }

    #[test]
    fn test_idx_of_bound_included_beyond_last_table() {
        // Key is beyond all tables, should return last table index
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"zebra".to_vec()),
        );
        assert_eq!(result, 2); // Should return last table index (2)
    }

    #[test]
    fn test_idx_of_bound_included_before_first_table() {
        // Key is before the first table, should return 0
        let table1 = create_test_table(vec![b"cherry", b"date"]);
        let table2 = create_test_table(vec![b"elderberry", b"fig"]);
        let table3 = create_test_table(vec![b"grape", b"honey"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"apple".to_vec()),
        );
        assert_eq!(result, 0);
    }

    #[test]
    fn test_idx_of_bound_excluded_first_table() {
        // Key falls in the first table with excluded bound
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        // For excluded, we need key < last_key, so "apple" (excluded) should still find table 0
        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Excluded(b"apple".to_vec()),
        );
        assert_eq!(result, 0);
    }

    #[test]
    fn test_idx_of_bound_excluded_boundary() {
        // Test excluded boundary conditions
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        // Excluded "banana" should look for first table where "banana" < last_key
        // Since "banana" is the last key of table1, it should move to table2
        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Excluded(b"banana".to_vec()),
        );
        assert_eq!(result, 1);

        // Excluded "date" should move to table3
        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Excluded(b"date".to_vec()),
        );
        assert_eq!(result, 2);
    }

    #[test]
    fn test_idx_of_bound_excluded_beyond_all() {
        // Key excluded beyond all tables
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let table2 = create_test_table(vec![b"cherry", b"date"]);
        let table3 = create_test_table(vec![b"elderberry", b"fig"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Excluded(b"zebra".to_vec()),
        );
        assert_eq!(result, 2); // Should return last table index
    }

    #[test]
    fn test_idx_of_bound_single_table() {
        // Test with only one table
        let table1 = create_test_table(vec![b"apple", b"banana"]);
        let readers = vec![table1];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Unbounded,
        );
        assert_eq!(result, 0);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"apple".to_vec()),
        );
        assert_eq!(result, 0);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"zebra".to_vec()),
        );
        assert_eq!(result, 0);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Excluded(b"banana".to_vec()),
        );
        assert_eq!(result, 0);
    }

    #[test]
    fn test_idx_of_bound_edge_case_keys() {
        // Test with special byte sequences
        let table1 = create_test_table(vec![b"\x00", b"\x01"]);
        let table2 = create_test_table(vec![b"\x7f", b"\x80"]);
        let table3 = create_test_table(vec![b"\xfe", b"\xff"]);
        let readers = vec![table1, table2, table3];

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"\x00".to_vec()),
        );
        assert_eq!(result, 0);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"\x7f".to_vec()),
        );
        assert_eq!(result, 1);

        let result = ConcatIterator::find_starting_table_idx(
            &readers,
            &Bound::Included(b"\xff".to_vec()),
        );
        assert_eq!(result, 2);
    }
}
