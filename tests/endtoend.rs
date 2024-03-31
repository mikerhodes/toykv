use toykv::ToyKVError;

#[test]
fn insert_and_readback() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut db = toykv::new(tmp_dir.path());

    let k = "foo".to_string();
    let v = "the rain in spain falls mainly on the plain".to_string();

    match db.set(&k.as_bytes(), &v.as_bytes()) {
        Ok(it) => it,
        Err(err) => return Err(err),
    };
    let got = db.get(&k.as_bytes())?;

    assert_eq!(
        got.unwrap(),
        v.into_bytes(),
        "Did not read back what we put in"
    );

    db.shutdown();

    Ok(())
}
