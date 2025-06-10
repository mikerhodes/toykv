use toykv::{error::ToyKVError, WALSync};

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

    let mut db = toykv::with_sync(tmp_dir.path(), WALSync::Off)?;
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

    let mut db = toykv::with_sync(tmp_dir.path(), WALSync::Off)?;

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

    let mut db = toykv::with_sync(tmp_dir.path(), WALSync::Off)?;

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    let mut cnt = 0;
    for _ in db.scan()? {
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

    let writes = 2500i64;

    let mut db = toykv::with_sync(tmp_dir.path(), WALSync::Off)?;

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    let mut cnt = 0;
    for _ in db.scan()? {
        cnt += 1;
    }
    assert_eq!(cnt, writes);

    db.shutdown();

    Ok(())
}

#[test]
fn scan_on_reopen() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = toykv::with_sync(tmp_dir.path(), WALSync::Off)?;

    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }

    db.shutdown();

    let db2 = toykv::open(tmp_dir.path())?;
    let mut cnt = 0;
    for _ in db2.scan()? {
        cnt += 1;
    }
    assert_eq!(cnt, writes);

    Ok(())
}

#[test]
fn scan_with_deletes() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let mut db = toykv::with_sync(tmp_dir.path(), WALSync::Off)?;

    for n in 1..=writes {
        db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec())?
    }

    for n in 1..=writes {
        if n % 2 == 0 {
            db.delete(n.to_be_bytes().to_vec())?
        }
    }

    let mut cnt = 0;
    for _ in db.scan()? {
        cnt += 1;
    }
    assert_eq!(cnt, writes / 2);

    db.shutdown();

    let db2 = toykv::open(tmp_dir.path())?;
    cnt = 0;
    for _ in db2.scan()? {
        cnt += 1;
    }
    assert_eq!(cnt, writes / 2);

    Ok(())
}
