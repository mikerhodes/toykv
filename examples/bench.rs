use std::time::Instant;

use toykv::{error::ToyKVError, ToyKVBuilder};

fn main() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 25000u32;

    let now = Instant::now();
    let mut db = ToyKVBuilder::new()
        .wal_sync(toykv::WALSync::Off)
        .wal_write_threshold(1000)
        .open(tmp_dir.path())?;
    let elapsed_time = now.elapsed();
    println!(
        "Running open() took {}ms.",
        elapsed_time.as_micros() as f64 / 1000f64
    );

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
    println!(
        "Running read() {} times took {}ms ({}ms per read).",
        writes,
        elapsed_time.as_millis(),
        ((elapsed_time / writes).as_micros()) as f64 / 1000.0
    );
    db.shutdown();

    // See how read time is affected if we open a new database
    let mut db = toykv::open(tmp_dir.path())?;
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
    println!(
        "Running read() {} times took {}ms ({}ms per read).",
        writes,
        elapsed_time.as_millis(),
        ((elapsed_time / writes).as_micros()) as f64 / 1000.0
    );

    Ok(())
}
