use std::time::Instant;

use toykv::ToyKVError;

fn main() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;

    let now = Instant::now();
    let mut db = toykv::open(tmp_dir.path())?;
    let elapsed_time = now.elapsed();
    println!("Running open() took {}ms.", elapsed_time.as_millis());

    let now = Instant::now();
    for n in 1..(writes + 1) {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };
    }
    let elapsed_time = now.elapsed();
    println!("Running write() took {}ms.", elapsed_time.as_millis());
    // assert_eq!(2, db.metrics.sst_flushes);
    // assert_eq!(writes as u64, db.metrics.writes);
    // assert_eq!(0, db.metrics.reads);

    let now = Instant::now();
    for n in 1..(writes + 1) {
        let got = db.get(n.to_be_bytes().as_slice())?;
        assert_eq!(
            got.unwrap(),
            n.to_le_bytes().as_slice(),
            "Did not read back what we put in"
        );
    }
    let elapsed_time = now.elapsed();
    println!("Running read() took {}ms.", elapsed_time.as_millis());

    Ok(())
}