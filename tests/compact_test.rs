use std::{fs, io, sync::atomic::Ordering};

use toykv::{error::ToyKVError, ToyKV, ToyKVBuilder, WALSync};

#[test]
fn compact_saity_ckeck() -> Result<(), ToyKVError> {
    let tmp_dir = tempfile::tempdir().unwrap();

    let writes = 2500i64;
    {
        let mut db = ToyKVBuilder::new()
            .wal_sync(WALSync::Off)
            .wal_write_threshold(10)
            .open(tmp_dir.path())?;

        for n in 1..(writes + 1) {
            set_with_flush_if_needed(&db, n.to_be_bytes(), n.to_le_bytes())?;
        }

        let expected_sstables = 2500 / 10 - 2;
        assert_eq!(
            db.metrics.sst_flushes.load(Ordering::Relaxed),
            expected_sstables
        );
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
        assert_eq!(db.live_sstables(), 248);
        db.shutdown();

        let entries = count_sstables_in_dir(&tmp_dir)?;
        assert_eq!(entries, 248); // ones we just wrote
    }

    // Execute a compact, check we have the new file and also
    // that we only have one "live" entry.
    {
        let mut db = toykv::open(tmp_dir.path())?;
        db.compact()?;
        assert_eq!(db.metrics.compacts.load(Ordering::Relaxed), 1);
        assert_eq!(db.live_sstables(), 1);
        db.shutdown();

        let entries = count_sstables_in_dir(&tmp_dir)?;
        assert_eq!(entries, 249); // old ones + compacted version
    }

    {
        let db = toykv::open(tmp_dir.path())?;
        assert_eq!(db.live_sstables(), 1);
        for n in 1..(writes + 1) {
            dbg!("read loop db2");
            let got = db.get(n.to_be_bytes())?;
            assert_eq!(
                got.unwrap(),
                n.to_le_bytes(),
                "Did not read back what we put in"
            );
        }
        assert_eq!(db.metrics.reads.load(Ordering::Relaxed), 2500);
    }
    Ok(())
}

/// Count the files with .sstable extension in directory d.
fn count_sstables_in_dir(d: &tempfile::TempDir) -> Result<usize, ToyKVError> {
    let entries = fs::read_dir(d.path())?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?
        .into_iter()
        .filter(|e| e.extension().unwrap() == "sstable")
        .count();
    Ok(entries)
}

/// Write k,v to db, flushing if set() receives a NeedFlush error
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
