use serde::{Deserialize, Serialize};
use std::{
    fs::{self},
    io::{Error, ErrorKind},
    path::PathBuf,
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SSTableIndexLevels {
    // Just one level for now
    pub(crate) l0: Vec<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SSTableIndex {
    pub(crate) levels: SSTableIndexLevels,

    #[serde(skip)]
    backing_file_path: PathBuf,
}

impl SSTableIndex {
    /// Read an index from disk, if it exists. Otherwise,
    /// create and return a new index file.
    pub(crate) fn open(p: PathBuf) -> Result<SSTableIndex, Error> {
        match fs::read_to_string(&p) {
            Ok(data) => {
                let levels: SSTableIndexLevels = serde_json::from_str(&data)?;
                Ok(SSTableIndex {
                    levels,
                    backing_file_path: p,
                })
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let levels = SSTableIndexLevels { l0: vec![] };
                Ok(SSTableIndex {
                    levels,
                    backing_file_path: p,
                })
            }
            Err(x) => Err(x),
        }
    }

    /// Prepends a file to the index at a given level, and writes
    /// the index to disk.
    pub(crate) fn write(&mut self) -> Result<(), Error> {
        fs::write(
            &self.backing_file_path,
            serde_json::to_string(&self.levels)?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // SSTableIndex Tests
    #[test]
    fn test_sstable_index_new_from_nonexistent_file() {
        // Test that SSTableIndex::open creates a new index when file doesn't exist
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("nonexistent_index.json");

        let result = SSTableIndex::open(index_path.clone());
        assert!(
            result.is_ok(),
            "Failed to create new index from nonexistent file"
        );

        let index = result.unwrap();
        assert_eq!(
            index.levels.l0.len(),
            0,
            "New index should have empty l0 level"
        );
        assert_eq!(
            index.backing_file_path, index_path,
            "Backing file path should match"
        );
    }

    #[test]
    fn test_sstable_index_add_paths() {
        // Test that paths can be added to the index
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("test_index.json");

        let mut index = SSTableIndex::open(index_path).unwrap();

        // Add some test paths
        let path1 = PathBuf::from("/test/path1.sstable");
        let path2 = PathBuf::from("/test/path2.sstable");
        let path3 = PathBuf::from("/test/path3.sstable");

        index.levels.l0.push(path1.clone());
        index.levels.l0.push(path2.clone());
        index.levels.l0.push(path3.clone());

        assert_eq!(
            index.levels.l0.len(),
            3,
            "Should have 3 paths after adding"
        );
        assert_eq!(index.levels.l0[0], path1, "First path should match");
        assert_eq!(index.levels.l0[1], path2, "Second path should match");
        assert_eq!(index.levels.l0[2], path3, "Third path should match");
    }

    #[test]
    fn test_sstable_index_round_trip_to_disk() {
        // Test that index can be written and read back from disk
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("roundtrip_index.json");

        // Create index with some paths
        let mut index = SSTableIndex::open(index_path.clone()).unwrap();
        let test_paths = vec![
            PathBuf::from("/data/table1.sstable"),
            PathBuf::from("/data/table2.sstable"),
            PathBuf::from("/data/subdir/table3.sstable"),
        ];

        for path in &test_paths {
            index.levels.l0.push(path.clone());
        }

        // Write to disk
        let write_result = index.write();
        assert!(write_result.is_ok(), "Failed to write index to disk");

        // Read back from disk
        let read_result = SSTableIndex::open(index_path);
        assert!(read_result.is_ok(), "Failed to read index from disk");

        let read_index = read_result.unwrap();
        assert_eq!(
            read_index.levels.l0.len(),
            test_paths.len(),
            "Read index should have same number of paths"
        );

        for (i, expected_path) in test_paths.iter().enumerate() {
            assert_eq!(
                read_index.levels.l0[i], *expected_path,
                "Path {} should match after round trip",
                i
            );
        }
    }

    #[test]
    fn test_sstable_index_json_format() {
        // Test that a small example index matches expected JSON string
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("json_test_index.json");

        let mut index = SSTableIndex::open(index_path.clone()).unwrap();

        // Add specific test paths
        index.levels.l0.push(PathBuf::from("file1.sstable"));
        index.levels.l0.push(PathBuf::from("file2.sstable"));

        // Write to disk
        index.write().unwrap();

        // Read the raw JSON content
        let json_content = fs::read_to_string(&index_path).unwrap();

        // Expected JSON - this might fail if the actual format is different
        // The user asked for failing tests if there are bugs, so this test will reveal
        // the actual JSON format if it doesn't match expectations
        let expected_json = r#"{"l0":["file1.sstable","file2.sstable"]}"#;

        assert_eq!(
            json_content.trim(),
            expected_json,
            "JSON format should match expected structure. Actual JSON: {}",
            json_content
        );
    }

    #[test]
    fn test_sstable_index_empty_index_json_format() {
        // Test JSON format of empty index
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("empty_json_index.json");

        let mut index = SSTableIndex::open(index_path.clone()).unwrap();

        // Write empty index to disk
        index.write().unwrap();

        // Read the raw JSON content
        let json_content = fs::read_to_string(&index_path).unwrap();

        // Expected JSON for empty index
        let expected_json = r#"{"l0":[]}"#;

        assert_eq!(
            json_content.trim(),
            expected_json,
            "Empty index JSON should match expected format. Actual JSON: {}",
            json_content
        );
    }

    #[test]
    fn test_sstable_index_multiple_writes() {
        // Test that the index can be written multiple times and maintains consistency
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("multi_write_index.json");

        let mut index = SSTableIndex::open(index_path.clone()).unwrap();

        // First write - empty index
        index.write().unwrap();
        let content1 = fs::read_to_string(&index_path).unwrap();

        // Add a path and write again
        index.levels.l0.push(PathBuf::from("table1.sstable"));
        index.write().unwrap();
        let content2 = fs::read_to_string(&index_path).unwrap();

        // Add another path and write again
        index.levels.l0.push(PathBuf::from("table2.sstable"));
        index.write().unwrap();
        let content3 = fs::read_to_string(&index_path).unwrap();

        // Verify each write produced different content
        assert_ne!(content1, content2);
        assert_ne!(content2, content3);

        // Verify final state by reading back
        let final_index = SSTableIndex::open(index_path).unwrap();
        assert_eq!(
            final_index.levels.l0.len(),
            2,
            "Final index should have 2 paths"
        );
        assert_eq!(final_index.levels.l0[0], PathBuf::from("table1.sstable"));
        assert_eq!(final_index.levels.l0[1], PathBuf::from("table2.sstable"));
    }

    #[test]
    fn test_sstable_index_corrupted_json_handling() {
        // Test how the index handles corrupted JSON files
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = temp_dir.path().join("corrupted_index.json");
        fs::write(&index_path, "{ invalid json content }").unwrap();
        let result = SSTableIndex::open(index_path);
        assert!(result.is_err(), "Opening corrupted JSON should fail");
    }
}
