use std::{ops::Bound, sync::atomic::Ordering};

use toykv::{error::ToyKVError, ToyKV, ToyKVBuilder, WALSync};

#[test]
fn insert_and_readback() -> Result<(), ToyKVError> {
    let k = "foo";
    let v = "the rain in spain falls mainly on the plain";

    let tmp_dir = tempfile::tempdir().unwrap();

    let db = toykv::open(tmp_dir.path())?;
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

    let db = toykv::open(tmp_dir.path())?;
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

    let db2 = toykv::open(tmp_dir.path())?;
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
        set_with_flush_if_needed(&db, n.to_be_bytes(), n.to_le_bytes())?;
    }
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 1);
    assert_eq!(db.metrics.writes.load(Ordering::Relaxed), writes as u64);
    assert_eq!(db.metrics.reads.load(Ordering::Relaxed), 0);
    for n in 1..(writes + 1) {
        dbg!("read loop db1");
        let got = db.get(n.to_be_bytes())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes(),
            "Did not read back what we put in"
        );
    }
    assert_eq!(db.metrics.reads.load(Ordering::Relaxed), 2500);
    db.shutdown();

    let db2 = toykv::open(tmp_dir.path())?;
    for n in 1..(writes + 1) {
        dbg!("read loop db2");
        let got = db2.get(n.to_be_bytes())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes(),
            "Did not read back what we put in"
        );
    }
    assert_eq!(db2.metrics.reads.load(Ordering::Relaxed), 2500);

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
        set_with_flush_if_needed(
            &db,
            n.to_be_bytes().to_vec(),
            n.to_le_bytes().to_vec(),
        )?;
    }
    assert_eq!(1, db.metrics.sst_flushes.load(Ordering::Relaxed));
    assert_eq!(writes as u64 + 1, db.metrics.writes.load(Ordering::Relaxed));
    assert_eq!(2, db.metrics.reads.load(Ordering::Relaxed));
    assert_eq!(1, db.metrics.deletes.load(Ordering::Relaxed));

    assert!(matches!(db.get(k)?, None));

    // We can write it again
    db.set(k, "baz")?;
    assert_eq!(db.get(k)?.unwrap(), "baz".as_bytes().to_vec());

    db.set(k, "blorp")?;
    for n in 1..(writes + 1) {
        set_with_flush_if_needed(
            &db,
            n.to_be_bytes().to_vec(),
            n.to_le_bytes().to_vec(),
        )?;
    }
    assert_eq!(4, db.metrics.sst_flushes.load(Ordering::Relaxed));
    assert_eq!(
        writes as u64 * 2 + 3,
        db.metrics.writes.load(Ordering::Relaxed)
    );
    assert_eq!(4, db.metrics.reads.load(Ordering::Relaxed));
    assert_eq!(1, db.metrics.deletes.load(Ordering::Relaxed));

    assert_eq!(db.get(k)?.unwrap(), "blorp".as_bytes().to_vec());

    db.shutdown();

    let db2 = toykv::open(tmp_dir.path())?;
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
        set_with_flush_if_needed(
            &db,
            ordered_bytes(n).to_vec(),
            n.to_le_bytes().to_vec(),
        )?;
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
        set_with_flush_if_needed(
            &db,
            ordered_bytes(n).to_vec(),
            n.to_le_bytes().to_vec(),
        )?;
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
        set_with_flush_if_needed(
            &db,
            n.to_be_bytes().to_vec(),
            n.to_le_bytes().to_vec(),
        )?;
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
fn scan_with_upper_bound() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    {
        let mut db = ToyKVBuilder::new()
            .wal_sync(WALSync::Off)
            .wal_write_threshold(1000)
            .open(tmp_dir.path())?;

        for n in 1..(writes + 1) {
            set_with_flush_if_needed(
                &db,
                n.to_be_bytes().to_vec(),
                n.to_le_bytes().to_vec(),
            )?;
        }
        db.shutdown();
    }

    {
        let db = toykv::open(tmp_dir.path())?;
        let i2v = |x: i64| (x as i64).to_be_bytes().to_vec();
        let check = |sk: i64, ub: Bound<Vec<u8>>, expected: usize| {
            assert_eq!(
                db.scan(Some(sk.to_be_bytes().as_slice()), ub)
                    .unwrap()
                    .count(),
                expected
            );
        };

        // First, fully unbounded scan
        let mut cnt = 0;
        for _ in db.scan(None, Bound::Unbounded)? {
            cnt += 1;
        }
        assert_eq!(cnt, writes);

        // Now various upper bounds
        check(500, Bound::Included(i2v(510)), 11);
        check(500, Bound::Excluded(i2v(510)), 10);
        check(500, Bound::Excluded(i2v(3000)), writes as usize - 499);
        check(500, Bound::Excluded(i2v(499)), 0);
    }

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
        set_with_flush_if_needed(
            &db,
            n.to_be_bytes().to_vec(),
            n.to_le_bytes().to_vec(),
        )?;
    }

    for n in 1..=writes {
        if n % 2 == 0 {
            delete_with_flush_if_needed(&db, n.to_be_bytes().to_vec())?
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
    let db = toykv::open(tmp_dir.path())?;

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
    let db = toykv::open(tmp_dir.path())?;

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
    let db = toykv::open(tmp_dir.path())?;

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
    let db = toykv::open(tmp_dir.path())?;

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
    let db = toykv::open(tmp_dir.path())?;

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
    let db = toykv::open(tmp_dir.path())?;

    // Delete a key that was never set
    assert_eq!(db.get("nonexistent")?, None);
    db.delete("nonexistent")?;
    assert_eq!(db.get("nonexistent")?, None);
    assert_eq!(db.metrics.deletes.load(Ordering::Relaxed), 1);

    // Delete again - should still work
    db.delete("nonexistent")?;
    assert_eq!(db.get("nonexistent")?, None);
    assert_eq!(db.metrics.deletes.load(Ordering::Relaxed), 2);

    Ok(())
}

#[test]
fn delete_already_deleted_key() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = toykv::open(tmp_dir.path())?;

    db.set("key", "value")?;
    assert_eq!(db.get("key")?.unwrap(), b"value");

    db.delete("key")?;
    assert_eq!(db.get("key")?, None);

    // Delete again - should still work
    db.delete("key")?;
    assert_eq!(db.get("key")?, None);
    assert_eq!(db.metrics.deletes.load(Ordering::Relaxed), 2);

    Ok(())
}

#[test]
fn overwrite_same_key_multiple_times() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // Overwrite many times -- ensure in sstables
    for i in 0..2500 {
        set_with_flush_if_needed(&db, "key", format!("value{}", i))?;
    }

    assert_eq!(db.get("key")?.unwrap(), b"value2499");
    assert_eq!(db.metrics.writes.load(Ordering::Relaxed), 2500);
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 1);

    Ok(())
}

#[test]
fn wal_threshold_boundary() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // Write exactly 999 items (one less than threshold)
    for i in 0..999 {
        set_with_flush_if_needed(
            &db,
            format!("key{}", i),
            format!("value{}", i),
        )?;
    }
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 0);

    // Write more, will fit in frozen_memtable
    for i in 1000..2000 {
        set_with_flush_if_needed(
            &db,
            format!("key{}", i),
            format!("value{}", i),
        )?;
    }
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 0);

    // Write 1000 more - should finally trigger a flush
    for i in 2000..3000 {
        set_with_flush_if_needed(
            &db,
            format!("key{}", i),
            format!("value{}", i),
        )?;
    }
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 1);

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
    let db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // Add some items then delete them all
    for i in 0..2500 {
        set_with_flush_if_needed(
            &db,
            format!("key{}", i),
            format!("value{}", i),
        )?;
    }

    for i in 0..2500 {
        delete_with_flush_if_needed(&db, format!("key{}", i))?;
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
    let db = ToyKVBuilder::new()
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
        set_with_flush_if_needed(&db, keys[i].clone(), values[i].clone())?;
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
    let db = toykv::open(tmp_dir.path())?;

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
        let db = toykv::open(tmp_dir.path())?;
        assert_eq!(db.get("persistent")?.unwrap(), b"value");
        assert_eq!(db.get("will_be_deleted")?, None);
    }

    Ok(())
}

#[test]
fn mixed_operations_across_sstable_flushes() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db = ToyKVBuilder::new()
        .wal_sync(WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;

    // First batch - will be in SSTable after flush
    for i in 0..2000 {
        db.set(format!("first_{}", i), format!("value_{}", i))?;
    }
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 0);

    // Need to flush as 2000 will fill active + frozen memtables
    db.flush_oldest_memtable()?;

    // Delete some from first batch
    for i in 0..500 {
        db.delete(format!("first_{}", i))?;
    }

    // Let's flush the 500 writes that we've got in the frozen memtable.
    db.flush_oldest_memtable()?;

    // Add second batch
    for i in 0..1000 {
        db.set(format!("second_{}", i), format!("value2_{}", i))?;
    }
    assert_eq!(db.metrics.sst_flushes.load(Ordering::Relaxed), 2);

    // Verify reads work correctly across memtable and SSTable
    for i in 0..500 {
        assert_eq!(db.get(format!("first_{}", i))?, None); // Deleted
    }
    for i in 500..2000 {
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

/// Set k to v in db, flushing if needed
fn set_with_flush_if_needed<S, T>(
    db: &ToyKV,
    k: S,
    v: T,
) -> Result<(), ToyKVError>
where
    S: Into<Vec<u8>> + Clone,
    T: Into<Vec<u8>> + Clone,
{
    match db.set(k.clone(), v.clone()) {
        Ok(x) => Ok(x),
        Err(ToyKVError::NeedFlush) => {
            db.flush_oldest_memtable()?;
            db.set(k, v)
        }
        Err(x) => Err(x),
    }
}
/// Delete k from db, flushing if needed
fn delete_with_flush_if_needed<K>(db: &ToyKV, k: K) -> Result<(), ToyKVError>
where
    K: Into<Vec<u8>> + Clone,
{
    match db.delete(k.clone()) {
        Ok(x) => Ok(x),
        Err(ToyKVError::NeedFlush) => {
            db.flush_oldest_memtable()?;
            db.delete(k)
        }
        Err(x) => Err(x),
    }
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
            set_with_flush_if_needed(
                &db,
                u32::to_be_bytes(i).to_vec(),
                format!("value_{}", i),
            )?;
        }

        // Delete every 3rd item
        for i in (0..5000).step_by(3) {
            delete_with_flush_if_needed(&db, u32::to_be_bytes(i).to_vec())?;
        }

        db.shutdown();
    }

    // Restart and verify all operations persisted correctly
    {
        let db = toykv::open(tmp_dir.path())?;

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

#[test]
fn test_11397_wal_records_bug() -> Result<(), ToyKVError> {
    // This is a regression test for a case where I called
    // `read` instead of `read_exact` in KVRecord to read
    // the header of a WAL entry. After 11397 records (of
    // a particular size, as below, presumably), `read`
    // consistently didn't read the whole header even though
    // it was in the file. The way the error case was written
    // caused this to stop reading the file but not report
    // an error to the caller.
    //
    // Unclear why the read returned fewer bytes at this
    // exact point, but using `read_exact` will continue
    // reading if it has an error. Presumably there was a
    // page fault or something that caused `read` to return
    // early.
    //
    // I happened to spot this during benchmarking where I was
    // trying out different wal write threshold values.
    //
    // Git commit where this was found: e751a95 but it was
    // lurking ever since the WAL was written.
    //
    // Replayed 0 records from WAL. nextseq=0.
    // Running open() took 0.303ms.
    // Writing sstable took 20ms.
    // Running write() took 121ms.
    // Running read() 23900 times took 942ms (0.039ms per read).
    // Replayed 11397 records from WAL. nextseq=11397.
    // thread 'main' panicked at examples/bench.rs:58:17:
    // called `Option::unwrap()` on a `None` value

    // First confirm that 11397 is the threshold with a success run
    {
        let writes = 11397u32;

        let tmp_dir = tempfile::tempdir().unwrap();
        let mut db = ToyKVBuilder::new()
            .wal_sync(toykv::WALSync::Off)
            .wal_write_threshold(12000)
            .open(tmp_dir.path())?;

        for n in 1..(writes + 1) {
            db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec())?
        }
        db.shutdown();

        // See how read time is affected if we open a new database
        let db = toykv::open(tmp_dir.path())?;
        for n in 1..(writes + 1) {
            let got = db.get(n.to_be_bytes().as_slice())?;
            assert_eq!(
                got.unwrap(),
                n.to_le_bytes().as_slice(),
                "Did not read back what we put in"
            );
        }
    }

    // Now test a "too large" version
    {
        let writes = 11398u32;

        let tmp_dir = tempfile::tempdir().unwrap();
        let mut db = ToyKVBuilder::new()
            .wal_sync(toykv::WALSync::Off)
            .wal_write_threshold(12000)
            .open(tmp_dir.path())?;

        for n in 1..(writes + 1) {
            db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec())?
        }
        db.shutdown();

        // See how read time is affected if we open a new database
        let db = toykv::open(tmp_dir.path())?;
        for n in 1..(writes + 1) {
            let got = db.get(n.to_be_bytes().as_slice())?;
            assert_eq!(
                got.unwrap(),
                n.to_le_bytes().as_slice(),
                "Did not read back what we put in"
            );
        }
    }

    Ok(())
}
