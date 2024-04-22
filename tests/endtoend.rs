use toykv::{ToyKVError, WALSync};

#[test]
fn insert_and_readback() -> Result<(), ToyKVError> {
    let k = "foo".to_string();
    let v = "the rain in spain falls mainly on the plain".to_string();

    let tmp_dir = tempfile::tempdir().unwrap();

    let mut db = toykv::open(tmp_dir.path())?;
    match db.set(k.clone().into_bytes(), v.clone().into_bytes()) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    let got = db.get(k.as_bytes())?;
    assert_eq!(
        got.unwrap(),
        v.into_bytes(),
        "Did not read back what we put in"
    );

    Ok(())
}
#[test]
fn grace_on_missing_key() -> Result<(), ToyKVError> {
    let k = "foo".to_string();
    let v = "the rain in spain falls mainly on the plain".to_string();

    let tmp_dir = tempfile::tempdir().unwrap();

    let mut db = toykv::open(tmp_dir.path())?;
    match db.set(k.clone().into_bytes(), v.clone().into_bytes()) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    let got = db.get("bar".as_bytes())?;
    assert_eq!(got, None, "Didn't get None for missing key");

    Ok(())
}

#[test]
fn data_survive_restart() -> Result<(), ToyKVError> {
    let k = "foo".to_string();
    let v = "the rain in spain falls mainly on the plain".to_string();

    let tmp_dir = tempfile::tempdir().unwrap();

    let mut db = toykv::open(tmp_dir.path())?;
    match db.set(k.clone().into_bytes(), v.clone().into_bytes()) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    db.shutdown();

    let mut db2 = toykv::open(tmp_dir.path())?;
    let got = db2.get(k.as_bytes())?;
    assert_eq!(
        got.unwrap(),
        v.into_bytes(),
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
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }
    assert_eq!(2, db.metrics.sst_flushes);
    assert_eq!(writes as u64, db.metrics.writes);
    assert_eq!(0, db.metrics.reads);
    for n in 1..(writes + 1) {
        let got = db.get(n.to_be_bytes().as_slice())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes().as_slice(),
            "Did not read back what we put in"
        );
    }
    db.shutdown();

    let mut db2 = toykv::open(tmp_dir.path())?;
    for n in 1..(writes + 1) {
        let got = db2.get(n.to_be_bytes().as_slice())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes().as_slice(),
            "Did not read back what we put in"
        );
    }

    Ok(())
}
