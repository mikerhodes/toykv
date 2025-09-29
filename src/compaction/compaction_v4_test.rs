#[cfg(test)]
mod tests {
    use std::{fs, io, ops::Bound, path::PathBuf};

    use crate::{
        compaction::SimpleCompactionPolicy,
        kvrecord::KVValue,
        memtable::Memtable,
        merge_iterator2::MergeIterator,
        table::{tableindex::SSTableIndex, SSTables, SSTABLE_INDEX_FNAME},
        ToyKV, ToyKVBuilder, ToyKVError, WALSync,
    };

    /// High values for WAL write threshold that we should
    /// never hit --- for use in tests checking for correctness
    /// when splitting tables on size.
    const HIGH_WRITE_THRESHOLD: u64 = 2_500_000;

    /// Test that compaction creates multiple files when target size is small
    #[test]
    fn compaction_creates_multiple_files_small_target() -> Result<(), ToyKVError>
    {
        let tmp_dir = tempfile::tempdir().unwrap();

        // Use a very small target size to force multiple files
        let target_size = 1024; // 1KB target per file
        const KV_SIZE_BYTES: usize = 128;
        let writes = 100;

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .target_sstable_size_bytes(target_size)
                .open(tmp_dir.path())?;

            // Write enough data to create multiple L0 tables
            for n in 1..=writes {
                let key = format!("key_{:06}", n).into_bytes(); // 10
                set_with_flush_if_needed_len(&mut db, key, KV_SIZE_BYTES - 10)?;
                // 128 bytes total

                // 9 writes per 1024 (8 => 1024 bytes, + 1 over the target size)
                // 100 writes => 12 tables, 1 active mt, 1 frozen mt, 10 ssts
                // 12 because 100 / 9 => 11.1, so round up.
            }

            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let original_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(
                original_idx.levels.l0.len(),
                10,
                "Should have 10 L0 tables"
            );

            // Compact and verify multiple L1 files are created
            db.compact()?;

            // Should now have only 1 "live" sstable set, but multiple physical files
            assert_eq!(
                db.live_sstables(),
                10,
                "Should have 10 live sstables from l1"
            );

            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(new_idx.levels.l1.len(), 10, "l1 should have 10 files");

            // Verify all data is still readable
            for n in 1..=writes {
                let key = format!("key_{:06}", n).into_bytes();
                let expected_value = [b'V'; KV_SIZE_BYTES - 10].to_vec();
                let got = db.get(key)?;
                assert_eq!(
                    got.unwrap(),
                    expected_value,
                    "Failed to read key {}",
                    n
                );
            }
        }

        Ok(())
    }

    /// Test that compaction creates single file when target size is large
    #[test]
    fn compaction_creates_single_file_large_target() -> Result<(), ToyKVError> {
        let tmp_dir = tempfile::tempdir().unwrap();

        let target_size = 10 * 1024 * 1024; // 10MB target per file
        const KV_SIZE_BYTES: usize = 1024;
        let writes = 3 * 10 * 1024 - 100; // 1 active, 1 frozen, 1 sstable

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .target_sstable_size_bytes(target_size)
                .open(tmp_dir.path())?;

            // Write data to create multiple L0 tables
            for n in 1..=writes {
                let key = format!("key_{:06}", n).into_bytes(); // 10
                set_with_flush_if_needed_len(&mut db, key, KV_SIZE_BYTES - 10)?;
                // 1024 bytes total

                // 10MB => 10 * 1024 writes
                // 3500 writes => 1 active, 1 frozen, 1 sst
            }

            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let original_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(
                original_idx.levels.l0.len(),
                1,
                "Should have 1 L0 tables"
            );

            // Compact and verify multiple L1 files are created
            db.compact()?;
            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(new_idx.levels.l1.len(), 1, "l1 should have 1 file");

            // Verify all data is still readable
            for n in 1..=writes {
                let key = format!("key_{:06}", n).into_bytes();
                let expected_value = [b'V'; KV_SIZE_BYTES - 10].to_vec();
                let got = db.get(key)?;
                assert_eq!(
                    got.unwrap(),
                    expected_value,
                    "Failed to read key {}",
                    n
                );
            }
        }

        Ok(())
    }

    /// Test compaction with varying data sizes to trigger file splits at different points
    #[test]
    fn compaction_file_splits_with_varying_data_sizes() -> Result<(), ToyKVError>
    {
        let tmp_dir = tempfile::tempdir().unwrap();

        let writes = 200;
        let target_size = 128 * 1024; // 128KB target per file

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .target_sstable_size_bytes(target_size)
                .open(tmp_dir.path())?;

            // Write data with varying sizes to test split behavior
            for n in 1..=writes {
                let key = format!("key_{:06}", n).into_bytes();
                // Vary value sizes to create irregular table sizes
                let padding_size = (n % 50) * 256 + 10; // 10 to 12,810 bytes
                set_with_flush_if_needed_len(&mut db, key, padding_size)?;
            }

            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let original_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(
                original_idx.levels.l0.len(),
                8,
                "Should have 1 L0 tables"
            );
            db.compact()?;
            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(new_idx.levels.l1.len(), 8, "l1 should have 1 file");

            // Verify all data is preserved and accessible
            for n in 1..=writes {
                let key = format!("key_{:06}", n).into_bytes();
                let got = db.get(key)?;
                assert!(
                    got.is_some(),
                    "Key {} should exist after compaction",
                    n
                );
            }

            // Test scanning across all files
            let mut scan_count = 0;
            let it = db.scan(None, Bound::Unbounded)?;
            for entry in it {
                let entry = entry?;
                scan_count += 1;
                let key_str = String::from_utf8(entry.key).unwrap();
                assert!(
                    key_str.starts_with("key_"),
                    "Unexpected key format in scan"
                );
            }
            assert_eq!(scan_count, 200, "Scan should return all 200 entries");
        }

        Ok(())
    }

    /// Test edge case: compaction with minimal data
    #[test]
    fn compaction_with_minimal_data() -> Result<(), ToyKVError> {
        let tmp_dir = tempfile::tempdir().unwrap();

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .wal_write_threshold(2) // Force flush with minimal data
                .open(tmp_dir.path())?;

            // Write just a few entries
            for n in 1..=5 {
                let key = format!("key{}", n).into_bytes();
                set_with_flush_if_needed_len(&mut db, key, n)?;
            }

            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let original_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(
                original_idx.levels.l0.len(),
                1,
                "Should have 1 L0 tables"
            );
            db.compact()?;
            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(new_idx.levels.l1.len(), 1, "l1 should have 1 file");

            // Verify all data accessible
            for n in 1..=5 {
                let key = format!("key{}", n).into_bytes();
                db.get(key)?;
            }
        }

        Ok(())
    }

    /// Test that file content is correctly distributed across multiple files
    #[test]
    fn compaction_content_distribution_across_files() -> Result<(), ToyKVError>
    {
        let tmp_dir = tempfile::tempdir().unwrap();

        const KV_SIZE_BYTES: usize = 1000;
        let target_size = 128 * 1024; // 128KB
        let writes = 10000;

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .target_sstable_size_bytes(target_size)
                .open(tmp_dir.path())?;

            // Write ordered data to test distribution
            for n in 1..=writes {
                let key = format!("key_{:08}", n).into_bytes(); // 12 byte keys
                set_with_flush_if_needed_len(&mut db, key, KV_SIZE_BYTES - 12)?;
            }

            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let original_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(
                original_idx.levels.l0.len(),
                74,
                "Should have 1 L0 tables"
            );
            db.compact()?;
            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(new_idx.levels.l1.len(), 74, "l1 should have 74 file");

            // Verify complete data integrity through scanning
            let it = db.scan(None, Bound::Unbounded)?;
            let mut scanned_keys = Vec::new();

            for entry in it {
                let entry = entry?;
                let key_str = String::from_utf8(entry.key).unwrap();
                scanned_keys.push(key_str.clone());
                assert!(key_str.starts_with("key_"), "Unexpected key format");
            }
            assert_eq!(scanned_keys.len(), writes, "Should have all keys");

            // Verify keys are in sorted order (important for range queries)
            let mut sorted_keys = scanned_keys.clone();
            sorted_keys.sort();
            assert_eq!(
                scanned_keys, sorted_keys,
                "Keys should be in sorted order after compaction"
            );

            // Verify we can still do point lookups efficiently
            for n in [1, 25, 50, 75, writes] {
                let key = format!("key_{:08}", n).into_bytes();
                let got = db.get(key)?;
                assert!(
                    got.is_some(),
                    "Point lookup should work for key {}",
                    n
                );
            }
        }

        Ok(())
    }

    /// Test compaction behavior with duplicate keys (newer values should win)
    #[test]
    fn compaction_handles_updated_keys_correctly() -> Result<(), ToyKVError> {
        let tmp_dir = tempfile::tempdir().unwrap();

        let target_size = 128 * 1024; // 128kb
        let writes = 10000;
        const ORIGNAL_SIZE: usize = 256;
        const UPDATED_SIZE: usize = 256;

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .target_sstable_size_bytes(target_size)
                .open(tmp_dir.path())?;

            // Write initial values
            for n in 1..=writes {
                let key = format!("key_{:04}", n).into_bytes();
                set_with_flush_if_needed_len(&mut db, key, ORIGNAL_SIZE)?;
            }

            // Overwrite some keys with new values
            for n in 1..=writes {
                if n % 2 == 0 {
                    // Overlap with first set and add new keys
                    let key = format!("key_{:04}", n).into_bytes();
                    set_with_flush_if_needed_len(&mut db, key, UPDATED_SIZE)?;
                }
            }

            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let original_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(
                original_idx.levels.l0.len(),
                29,
                "Should have 1 L0 tables"
            );
            db.compact()?;
            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(
                new_idx.levels.l1.len(),
                21,
                "l1 should have fewer files"
            );

            // Verify the newest values are preserved
            for n in 1..=writes {
                let key = format!("key_{:04}", n).into_bytes();
                let got = db.get(key)?.unwrap();

                if n % 2 == 0 {
                    assert_eq!(
                        got.len(),
                        ORIGNAL_SIZE,
                        "size not ORIGINAL_SIZE"
                    );
                } else {
                    assert_eq!(
                        got.len(),
                        UPDATED_SIZE,
                        "size not UPDATED_SIZE"
                    );
                }
            }

            let count = db
                .scan(None, Bound::Unbounded)?
                .filter(|e| e.is_ok())
                .count();
            assert_eq!(count, writes, "Should have all keys");
        }

        Ok(())
    }

    /// Compaction should retain deleted keys as deleted.
    #[test]
    fn compaction_handles_deleted_keys_correctly() -> Result<(), ToyKVError> {
        let tmp_dir = tempfile::tempdir().unwrap();

        let target_size = 128 * 1024; // 128KB
        let writes = 6000; // 6000 * 0.25kb => 1500KB

        {
            // We work directly with sstables here to have more
            // control over the compaction. We want to ensure
            // that all the writes make it to the sstables, so
            // explicitly create a memtable with all the edits
            // and write it out.
            let mut sstables = SSTables::new(tmp_dir.path(), target_size)?;

            // Test that deleted values, including some that
            // have seen updates, don't get ressurected
            // during compaction.

            // Initial writes
            create_sstable_from_operations(
                &mut sstables,
                tmp_dir.path().join("memtable1.wal"),
                1..=writes,
                |memtable, key| set_memtable(memtable, key, 256),
            )?;
            // Update some keys
            create_sstable_from_operations(
                &mut sstables,
                tmp_dir.path().join("memtable2.wal"),
                (1..=writes).step_by(4),
                |memtable, key| set_memtable(memtable, key, 256),
            )?;
            // Delete updated and some original
            create_sstable_from_operations(
                &mut sstables,
                tmp_dir.path().join("memtable3.wal"),
                (1..=writes).step_by(2),
                |memtable, key| delete_memtable(memtable, key),
            )?;

            // Check L0 has the three sstables we just wrote
            // They are not going to be target_size as we made
            // them up without checking their size --- that's
            // why we have more files in L1 later, where the size
            // was accounted for.
            let idx_fname = tmp_dir.path().join(SSTABLE_INDEX_FNAME);
            let new_idx = SSTableIndex::open(idx_fname.clone())?;
            assert_eq!(new_idx.levels.l0.len(), 3, "l0 should have 3 files");
            assert_eq!(new_idx.levels.l1.len(), 0, "l1 should be empty");

            // Compact
            let policy = SimpleCompactionPolicy::new();
            let c_task = sstables.build_compaction_task_v2(&policy)?.unwrap();
            let c_result = c_task.compact_v2()?;
            sstables.try_commit_compaction_v3(policy, c_result)?;

            // Check L0 is now empty and L1 has files
            let new_idx = SSTableIndex::open(idx_fname)?;
            assert_eq!(new_idx.levels.l0.len(), 0, "l0 should be empty");
            assert_eq!(
                new_idx.levels.l1.len(),
                7, // 3000 entries (we deleted some!) @ 256bytes each
                "l1 should have 7 files"
            );

            // Verify deletes are still deleted
            for n in (1..=writes).step_by(2) {
                // Overlap with first set and add new keys
                let key = format!("key_{:04}", n).into_bytes();
                dbg!(sstables.get(&key[..])?);
                assert!(
                    sstables.get(&key[..]).is_ok_and(|v| v.is_none()),
                    "deleted items should stay deleted"
                );
            }
            // Verify other items still there.
            for n in (2..=writes).step_by(2) {
                // Overlap with first set and add new keys
                let key = format!("key_{:04}", n).into_bytes();
                assert!(
                    sstables
                        .get(&key[..])
                        .is_ok_and(|v| v
                            .is_some_and(|x| matches!(x, KVValue::Some(_)))),
                    "Other items should remain"
                );
            }

            // Check scan also only returns half
            let count = sstables
                .iters(None, Bound::Unbounded)?
                .filter(|e| e.is_ok())
                .count();
            assert_eq!(count, writes / 2, "Should have half of the keys");
        }

        Ok(())
    }

    /// Test that compaction maintains correct order across file boundaries
    #[test]
    fn compaction_maintains_order_across_file_boundaries(
    ) -> Result<(), ToyKVError> {
        let tmp_dir = tempfile::tempdir().unwrap();

        const KV_SIZE_BYTES: usize = 1024;
        let target_size = 128 * 1024; // Force many small files
        let writes = 1000;

        {
            let mut db = ToyKVBuilder::new()
                .wal_sync(WALSync::Off)
                .wal_write_threshold(HIGH_WRITE_THRESHOLD)
                .target_sstable_size_bytes(target_size)
                .open(tmp_dir.path())?;

            // Write keys in a specific pattern that will span multiple files
            let mut keys = Vec::new();
            for n in 0..writes {
                let key = format!("key_{:08}", n * 7 % 500).into_bytes(); // Create some key spacing
                keys.push(key.clone());
                set_with_flush_if_needed_len(&mut db, key, KV_SIZE_BYTES - 12)?;
            }

            // Sort keys to know expected order
            keys.sort();
            let original_len = keys.len();
            keys.dedup();
            assert!(
                keys.len() < original_len,
                "modulo should have caused updates"
            );

            // Compact it
            db.compact()?;
            assert!(
                count_sstables_in_dir(&tmp_dir)? > 0,
                "Make sure we have written a table to disk"
            );

            let it = db.scan(None, Bound::Unbounded)?;
            let mut scanned_keys = Vec::new();
            for entry in it {
                scanned_keys.push(entry?.key);
            }

            // Check that scanned keys are in correct sorted order
            for i in 1..scanned_keys.len() {
                assert!(
                    scanned_keys[i - 1] <= scanned_keys[i],
                    "Keys not in sorted order at position {}",
                    i
                );
            }

            // Test range queries that span file boundaries
            let mid_key = format!("key_{:08}", 500).into_bytes();
            let it = db.scan(Some(&mid_key), Bound::Unbounded)?;
            let mut range_keys = Vec::new();

            for entry in it {
                range_keys.push(entry?.key);
            }

            // Verify range query results are also ordered
            for i in 1..range_keys.len() {
                assert!(
                    range_keys[i - 1] <= range_keys[i],
                    "Range query keys not in sorted order"
                );
            }

            // Verify range starts from correct position
            if !range_keys.is_empty() {
                assert!(
                    range_keys[0] >= mid_key,
                    "Range query should start from mid_key or after"
                );
            }
        }

        Ok(())
    }

    /// Helper function to count .sstable files in directory
    fn count_sstables_in_dir(
        d: &tempfile::TempDir,
    ) -> Result<usize, ToyKVError> {
        let entries = fs::read_dir(d.path())?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?
            .into_iter()
            .filter(|e| e.extension().map_or(false, |ext| ext == "sstable"))
            .count();
        Ok(entries)
    }

    fn create_sstable_from_operations<F>(
        sstables: &mut SSTables,
        wal_path: PathBuf,
        key_range: impl Iterator<Item = usize>,
        operation: F,
    ) -> Result<(), ToyKVError>
    where
        F: Fn(&mut Memtable, Vec<u8>) -> Result<(), ToyKVError>,
    {
        let mut active_memtable = Memtable::new(wal_path, WALSync::Off)?;

        for n in key_range {
            let key = format!("key_{:04}", n).into_bytes();
            operation(&mut active_memtable, key)?;
        }

        let w = sstables.build_sstable_writer();
        let mut mi = MergeIterator::new();
        mi.add_iterator(active_memtable.iter());
        let r = w.write(1000, mi)?;
        sstables.commit_new_sstable(r)?;

        Ok(())
    }

    /// Helper for writing a key with a given length of value
    fn write_kv(
        mt: &ToyKV,
        key: Vec<u8>,
        v_len: usize,
    ) -> Result<(), ToyKVError> {
        let v = vec![b'V'; v_len];
        mt.set(key.to_vec(), v)
    }

    fn set_memtable(
        mt: &mut Memtable,
        k: Vec<u8>,
        v_len: usize,
    ) -> Result<(), ToyKVError> {
        let v = vec![b'V'; v_len];
        mt.write(k, KVValue::Some(v))
    }
    fn delete_memtable(
        mt: &mut Memtable,
        key: Vec<u8>,
    ) -> Result<(), ToyKVError> {
        mt.write(key, KVValue::Deleted)
    }

    fn set_with_flush_if_needed_len(
        db: &ToyKV,
        k: Vec<u8>,
        v_len: usize,
    ) -> Result<(), ToyKVError> {
        match write_kv(db, k.clone().into(), v_len) {
            Ok(x) => Ok(x),
            Err(ToyKVError::NeedFlush) => {
                db.flush_oldest_memtable()?;
                write_kv(db, k.into(), v_len)
            }
            Err(x) => Err(x),
        }
    }
}
