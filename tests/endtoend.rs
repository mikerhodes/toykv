use toykv::ToyKVError;

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

    let db2 = toykv::open(tmp_dir.path())?;
    let got = db2.get(k.as_bytes())?;
    assert_eq!(
        got.unwrap(),
        v.into_bytes(),
        "Did not read back what we put in"
    );

    Ok(())
}
