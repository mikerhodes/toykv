use std::ops::Bound;

use toykv::{error::ToyKVError, ToyKVBuilder, WALSync};

#[test]
fn insert_and_readback() -> Result<(), ToyKVError> {
    let k = "foo";
    let v = "the rain in spain falls mainly on the plain";

    let tmp_dir = tempfile::tempdir().unwrap();

    let mut db = toykv::open(tmp_dir.path())?;
    match db.set(k, v) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    let got = db.get(k.as_bytes())?;
    assert_eq!(
        got.unwrap(),
        Into::<Vec<u8>>::into(v),
        "Did not read back what we put in"
    );

    Ok(())
}
#[test]
fn grace_on_missing_key() -> Result<(), ToyKVError> {
    let k = "foo";
    let v = "the rain in spain falls mainly on the plain";

    let tmp_dir = tempfile::tempdir().unwrap();

    let mut db = toykv::open(tmp_dir.path())?;
    match db.set(k, v) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    let got = db.get("bar")?;
    assert_eq!(got, None, "Didn't get None for missing key");

    Ok(())
}

#[test]
fn data_survive_restart() -> Result<(), ToyKVError> {
    let k = "foo";
    let v = "the rain in spain falls mainly on the plain";

    let tmp_dir = tempfile::tempdir().unwrap();

    let mut db = toykv::open(tmp_dir.path())?;
    match db.set(k, v) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    db.shutdown();

    let mut db2 = toykv::open(tmp_dir.path())?;
    let got = db2.get(k.as_bytes())?;
    assert_eq!(
        got.unwrap(),
        Into::<Vec<u8>>::into(v),
        "Did not read back what we put in"
    );

    Ok(())
}

#[test]
fn write_and_read_sstable() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes(), n.to_le_bytes()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }
    assert_eq!(2, db.metrics.sst_flushes);
    assert_eq!(writes as u64, db.metrics.writes);
    assert_eq!(0, db.metrics.reads);
    for n in 1..(writes + 1) {
        dbg!("read loop db1");
        let got = db.get(n.to_be_bytes())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes(),
            "Did not read back what we put in"
        );
    }
    db.shutdown();

    let mut db2 = toykv::open(tmp_dir.path())?;
    for n in 1..(writes + 1) {
        dbg!("read loop db2");
        let got = db2.get(n.to_be_bytes())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes(),
            "Did not read back what we put in"
        );
    }

    Ok(())
}

/// Tests that deletes are retrievable from both memtable and sstable
#[test]
fn deletes() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    let k = "foo";
    db.set(k, "bar")?;
    assert_eq!(db.get(k)?.unwrap(), "bar".as_bytes().to_vec());

    db.delete(k)?;
    assert!(matches!(db.get(k)?, None));

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }
    assert_eq!(2, db.metrics.sst_flushes);
    assert_eq!(writes as u64 + 1, db.metrics.writes);
    assert_eq!(2, db.metrics.reads);
    assert_eq!(1, db.metrics.deletes);

    assert!(matches!(db.get(k)?, None));

    // We can write it again
    db.set(k, "baz")?;
    assert_eq!(db.get(k)?.unwrap(), "baz".as_bytes().to_vec());

    db.set(k, "blorp")?;
    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }
    assert_eq!(5, db.metrics.sst_flushes); // 5000 writes => 5 flushes
    assert_eq!(writes as u64 * 2 + 3, db.metrics.writes);
    assert_eq!(4, db.metrics.reads);
    assert_eq!(1, db.metrics.deletes);

    assert_eq!(db.get(k)?.unwrap(), "blorp".as_bytes().to_vec());

    db.shutdown();

    let mut db2 = toykv::open(tmp_dir.path())?;
    assert_eq!(db2.get(k)?.unwrap(), "blorp".as_bytes().to_vec());
    db2.delete(k)?;
    assert!(matches!(db2.get(k)?, None));

    Ok(())
}

#[test]
#[ignore = "not yet implemented"]
fn operations_blocked_after_shutdown() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    let mut cnt = 0;
    for _ in db.scan(None, Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes);

    db.shutdown();

    // TODO should be an error
    assert!(matches!(
        db.get(1_i64.to_be_bytes().to_vec()),
        Err(ToyKVError::DatabaseShutdown)
    ));

    Ok(())
}

#[test]
fn scan() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    // emits i as byte array maintaining ordering
    let ordered_bytes = |i: i64| (i as u64 ^ (1u64 << 63)).to_be_bytes();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    for n in 1..(writes + 1) {
        match db.set(ordered_bytes(n).to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    let mut cnt = 0;
    for _ in db.scan(None, Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes);

    let mut cnt = 0;
    for _ in db.scan(Some(ordered_bytes(500).as_slice()), Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes - 499); // 499 as 500 is in result set

    let mut cnt = 0;
    for _ in db.scan(Some(ordered_bytes(2499).as_slice()), Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, 2); // 2 => 2499, 2500

    let mut cnt = 0;
    for _ in db.scan(Some(ordered_bytes(-1400).as_slice()), Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes);

    db.shutdown();

    Ok(())
}

#[test]
fn scan_seek_key_check_next() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    // emits i as byte array maintaining ordering
    let ordered_bytes = |i: i64| (i as u64 ^ (1u64 << 63)).to_be_bytes();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    for n in 1..(writes + 1) {
        match db.set(ordered_bytes(n).to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    // check the first keys
    {
        let mut it = db.scan(Some(&[0]), Bound::Unbounded)?;
        assert_eq!(it.next().unwrap().unwrap().key, ordered_bytes(1));
    }
    {
        let sk = ordered_bytes(-1400);
        let mut it = db.scan(Some(sk.as_slice()), Bound::Unbounded)?;
        assert_eq!(it.next().unwrap().unwrap().key, ordered_bytes(1));
    }
    {
        let sk = ordered_bytes(1500);
        let mut it = db.scan(Some(sk.as_slice()), Bound::Unbounded)?;
        assert_eq!(it.next().unwrap().unwrap().key, ordered_bytes(1500));
    }
    {
        let sk = ordered_bytes(123);
        let mut it = db.scan(Some(sk.as_slice()), Bound::Unbounded)?;
        assert_eq!(it.next().unwrap().unwrap().key, ordered_bytes(123));
    }
    {
        let sk = ordered_bytes(writes * 2);
        let mut it = db.scan(Some(sk.as_slice()), Bound::Unbounded)?;
        assert!(it.next().is_none());
    }
    {
        let mut it = db.scan(Some(&[255]), Bound::Unbounded)?;
        assert!(it.next().is_none());
    }

    db.shutdown();

    Ok(())
}

#[test]
fn scan_on_reopen() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    db.shutdown();

    let db2 = toykv::open(tmp_dir.path())?;
    let mut cnt = 0;
    for _ in db2.scan(None, Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes);
    let mut cnt = 0;
    for _ in db2.scan(
        Some((500 as i64).to_be_bytes().as_slice()),
        Bound::Unbounded,
    )? {
        cnt += 1;
    }
    assert_eq!(cnt, writes - 499); // 499 as 500 is in result set

    Ok(())
}

#[test]
fn scan_with_deletes() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    for n in 1..=writes {
        db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec())?
    }

    for n in 1..=writes {
        if n % 2 == 0 {
            db.delete(n.to_be_bytes().to_vec())?
        }
    }

    let mut cnt = 0;
    for _ in db.scan(None, Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes / 2);

    db.shutdown();

    let db2 = toykv::open(tmp_dir.path())?;
    cnt = 0;
    for _ in db2.scan(None, Bound::Unbounded)? {
        cnt += 1;
    }
    assert_eq!(cnt, writes / 2);

    Ok(())
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn empty_key_and_value() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Test empty key with non-empty value
    assert!(matches!(db.set("", "value"), Err(ToyKVError::KeyEmpty)));

    // Test non-empty key with empty value
    assert!(matches!(db.set("key", ""), Err(ToyKVError::ValueEmpty)));

    // Test both empty
    assert!(matches!(db.set("", ""), Err(ToyKVError::KeyEmpty)));

    Ok(())
}

#[test]
fn binary_keys_and_values() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Test with null bytes
    let key_with_nulls = b"key\x00with\x00nulls";
    let value_with_nulls = b"value\x00with\x00nulls\x00and\xff\xfe\xfd";

    db.set(key_with_nulls.to_vec(), value_with_nulls.to_vec())?;
    assert_eq!(db.get(key_with_nulls.to_vec())?.unwrap(), value_with_nulls);

    // Test with all possible byte values
    let all_bytes_key: Vec<u8> = (0..=255).collect();
    let all_bytes_value: Vec<u8> = (0..=255).rev().collect();

    db.set(all_bytes_key.clone(), all_bytes_value.clone())?;
    assert_eq!(db.get(all_bytes_key)?.unwrap(), all_bytes_value);

    Ok(())
}

#[test]
fn unicode_keys_and_values() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Test various Unicode characters
    let unicode_key = "🔑key世界🌍";
    let unicode_value = "🎯value测试🚀emoji";

    db.set(unicode_key, unicode_value)?;
    assert_eq!(db.get(unicode_key)?.unwrap(), unicode_value.as_bytes());

    // Test with combining characters and normalization edge cases
    let complex_unicode = "é́́́"; // e with multiple combining accents
    db.set(complex_unicode, "normalized")?;
    assert_eq!(db.get(complex_unicode)?.unwrap(), b"normalized");

    Ok(())
}

#[test]
fn key_size_limits() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Test key exactly at limit (10KB = 10240 bytes)
    let max_key = vec![b'k'; 10240];
    db.set(max_key.clone(), "value")?;
    assert_eq!(db.get(max_key)?.unwrap(), b"value");

    // Test key one byte over limit - should fail
    let over_limit_key = vec![b'k'; 10241];
    assert!(matches!(
        db.set(over_limit_key.clone(), "value"),
        Err(ToyKVError::KeyTooLarge)
    ));
    assert!(matches!(
        db.get(over_limit_key.clone()),
        Err(ToyKVError::KeyTooLarge)
    ));
    assert!(matches!(
        db.delete(over_limit_key.clone()),
        Err(ToyKVError::KeyTooLarge)
    ));

    Ok(())
}

#[test]
fn value_size_limits() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Test value exactly at limit (100KB = 102400 bytes)
    let max_value = vec![b'v'; 102400];
    db.set("key", max_value.clone())?;
    assert_eq!(db.get("key")?.unwrap(), max_value);

    // Test value one byte over limit - should fail
    let over_limit_value = vec![b'v'; 102401];
    assert!(matches!(
        db.set("key2", over_limit_value),
        Err(ToyKVError::ValueTooLarge)
    ));

    Ok(())
}

#[test]
fn delete_nonexistent_key() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Delete a key that was never set
    assert_eq!(db.get("nonexistent")?, None);
    db.delete("nonexistent")?;
    assert_eq!(db.get("nonexistent")?, None);
    assert_eq!(db.metrics.deletes, 1);

    // Delete again - should still work
    db.delete("nonexistent")?;
    assert_eq!(db.get("nonexistent")?, None);
    assert_eq!(db.metrics.deletes, 2);

    Ok(())
}

#[test]
fn delete_already_deleted_key() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    db.set("key", "value")?;
    assert_eq!(db.get("key")?.unwrap(), b"value");

    db.delete("key")?;
    assert_eq!(db.get("key")?, None);

    // Delete again - should still work
    db.delete("key")?;
    assert_eq!(db.get("key")?, None);
    assert_eq!(db.metrics.deletes, 2);

    Ok(())
}

#[test]
fn overwrite_same_key_multiple_times() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // Overwrite many times -- ensure in sstables
    for i in 0..2500 {
        db.set("key", format!("value{}", i))?;
    }

    assert_eq!(db.get("key")?.unwrap(), b"value2499");
    assert_eq!(db.metrics.writes, 2500);
    assert_eq!(db.metrics.sst_flushes, 2);

    Ok(())
}

#[test]
fn wal_threshold_boundary() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // Write exactly 999 items (one less than threshold)
    for i in 0..999 {
        db.set(format!("key{}", i), format!("value{}", i))?;
    }
    assert_eq!(db.metrics.sst_flushes, 0);

    // Write the 1000th item - should trigger flush
    db.set("key999", "value999")?;
    assert_eq!(db.metrics.sst_flushes, 1);

    // Write 1000 more - should trigger another flush
    for i in 1000..2000 {
        db.set(format!("key{}", i), format!("value{}", i))?;
    }
    assert_eq!(db.metrics.sst_flushes, 2);

    Ok(())
}

#[test]
fn scan_empty_database() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = toykv::open(tmp_dir.path())?;

    let mut count = 0;
    for _ in db.scan(None, Bound::Unbounded)? {
        count += 1;
    }
    assert_eq!(count, 0);

    Ok(())
}

#[test]
fn scan_database_with_only_deleted_items() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // Add some items then delete them all
    for i in 0..2500 {
        db.set(format!("key{}", i), format!("value{}", i))?;
    }

    for i in 0..2500 {
        db.delete(format!("key{}", i))?;
    }

    let mut count = 0;
    for _ in db.scan(None, Bound::Unbounded)? {
        count += 1;
    }
    assert_eq!(count, 0);

    Ok(())
}

#[test]
fn scan_returns_correct_values() -> Result<(), ToyKVError> {
    let n_items = 2500;
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    let mut keys = vec![];
    let mut values = vec![];
    for i in 0..n_items {
        keys.push(format!("key{:05}", i).into_bytes());
        values.push(format!("key{:05}", i).into_bytes());
    }

    for i in 0..n_items {
        db.set(keys[i].clone(), values[i].clone())?;
    }

    let mut items: Vec<_> = db
        .scan(None, Bound::Unbounded)?
        .collect::<Result<Vec<_>, _>>()?;
    items.sort_by(|a, b| a.key.cmp(&b.key));

    assert_eq!(items.len(), n_items);
    for i in 0..n_items {
        assert_eq!(items[i].key, keys[i]);
        assert_eq!(items[i].value, values[i]);
    }

    Ok(())
}

#[test]
fn key_ordering_edge_cases() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::open(tmp_dir.path())?;

    // Test lexicographic ordering with tricky keys
    let keys = vec![
        "\x00",     // null byte
        "\x00\x01", // null byte followed by 1
        "\x01",     // byte 1
        "a",        // letter a
        "aa",       // double a
        "ab",       // a followed by b
        "b",        // letter b
        "\x7f",     // max byte value rust lets you use hex escape for
                    // "\xff",     // Rust doesn't let you use this
    ];

    // Insert in reverse order
    for (i, key) in keys.iter().rev().enumerate() {
        db.set(*key, format!("value{}", i))?;
    }

    // Scan should return in lexicographic order
    let scanned: Vec<_> = db
        .scan(None, Bound::Unbounded)?
        .collect::<Result<Vec<_>, _>>()?;

    for (i, expected_key) in keys.iter().enumerate() {
        assert_eq!(scanned[i].key, expected_key.as_bytes());
    }

    Ok(())
}

#[test]
fn persistence_after_restart_with_deletes() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    {
        let mut db = toykv::open(tmp_dir.path())?;
        db.set("persistent", "value")?;
        db.set("will_be_deleted", "temp_value")?;
        db.delete("will_be_deleted")?;
        db.shutdown();
    }

    // Restart and verify persistence
    {
        let mut db = toykv::open(tmp_dir.path())?;
        assert_eq!(db.get("persistent")?.unwrap(), b"value");
        assert_eq!(db.get("will_be_deleted")?, None);
    }

    Ok(())
}

#[test]
fn mixed_operations_across_sstable_flushes() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // First batch - will be in SSTable after flush
    for i in 0..1000 {
        db.set(format!("first_{}", i), format!("value_{}", i))?;
    }
    assert_eq!(db.metrics.sst_flushes, 1);

    // Delete some from first batch
    for i in 0..500 {
        db.delete(format!("first_{}", i))?;
    }

    // Add second batch
    for i in 0..1000 {
        db.set(format!("second_{}", i), format!("value2_{}", i))?;
    }
    assert_eq!(db.metrics.sst_flushes, 2);

    // Verify reads work correctly across memtable and SSTable
    for i in 0..500 {
        assert_eq!(db.get(format!("first_{}", i))?, None); // Deleted
    }
    for i in 500..1000 {
        assert_eq!(
            db.get(format!("first_{}", i))?.unwrap(),
            format!("value_{}", i).as_bytes()
        );
    }
    for i in 0..1000 {
        assert_eq!(
            db.get(format!("second_{}", i))?.unwrap(),
            format!("value2_{}", i).as_bytes()
        );
    }

    Ok(())
}

#[test]
fn nonexistent_data_directory() -> Result<(), ToyKVError> {
    use std::path::PathBuf;

    let nonexistent_path = PathBuf::from("/this/path/should/not/exist/12345");
    let result = toykv::open(&nonexistent_path);

    assert!(matches!(result, Err(ToyKVError::DataDirMissing)));

    Ok(())
}

#[test]
fn operations_with_large_batch_across_restart() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    {
        let mut db = ToyKVBuilder::new()
            .wal_sync(WALSync::Off)
            .wal_write_threshold(1000)
            .open(tmp_dir.path())?;

        // Write 5000 items to ensure multiple SSTable flushes
        for i in 0..5000 {
            db.set(u32::to_be_bytes(i).to_vec(), format!("value_{}", i))?;
        }

        // Delete every 3rd item
        for i in (0..5000).step_by(3) {
            db.delete(u32::to_be_bytes(i).to_vec())?;
        }

        db.shutdown();
    }

    // Restart and verify all operations persisted correctly
    {
        let mut db = toykv::open(tmp_dir.path())?;

        for i in 0..5000 {
            let result = db.get(u32::to_be_bytes(i).to_vec())?;
            if i % 3 == 0 {
                assert_eq!(result, None, "Item {} should be deleted", i);
            } else {
                assert_eq!(
                    result.unwrap(),
                    format!("value_{}", i).as_bytes(),
                    "Item {} should exist",
                    i
                );
            }
        }
    }

    Ok(())
}
