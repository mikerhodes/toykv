use std::time::Instant;

use toykv::{error::ToyKVError, ToyKVBuilder};

fn main() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 100000u32;

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
    let mut n = 0;
    while n < writes + 1 {
        match db.set(n.to_be_bytes().to_vec(), n.to_le_bytes().to_vec()) {
            Ok(_) => n = n + 1,
            Err(ToyKVError::NeedFlush) => {
                db.flush_oldest_memtable()?;
            }
            Err(err) => return Err(err),
        };
    }
    let elapsed_time = now.elapsed();
    println!("Running write() took {}ms.", elapsed_time.as_millis());
    db.shutdown();

    // See how read time is affected if we open a new database
    let db = toykv::open(tmp_dir.path())?;
    let now = Instant::now();
    let mut scans = 0;
    // scan in batches of 100
    for n in (1..(writes - 200)).step_by(100) {
        let sk = (n).to_be_bytes();
        let ek = (n + 100).to_be_bytes();
        let got = db.scan(
            Some(sk.as_slice()),
            std::ops::Bound::Excluded(ek.to_vec()),
        )?;
        let mut c = 0;
        for _ in got {
            c += 1;
        }
        assert_eq!(c, 100, "Did not read back what we put in");
        scans += 1;
    }
    let elapsed_time = now.elapsed();
    println!(
        "Running scan() {} times took {}ms ({}ms per scan).",
        scans,
        elapsed_time.as_millis(),
        ((elapsed_time / scans).as_micros()) as f64 / 1000.0
    );

    Ok(())
}
