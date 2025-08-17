// Implements a simple WAL for the database's memtable.

use crossbeam_skiplist::SkipMap;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Error, Read, Seek, Write},
    path::PathBuf,
};

use crate::{
    kvrecord::{KVRecord, KVValue, KVWriteRecord},
    ToyKVError, WALSync,
};

/*
The WAL is a simple sequence of records written to a file, written in the
order they were inserted into toykv.

The main interesting item is the seq, which is expected to incrase by 1
with each item. A u32 allows for 4,294,967,295 records. We should've
flushed the associated memtable to disk long before we get that
far.

We have a WAL header that contains the WAL specific fields of seq and
op, then we embed a KVRecord serialisation.

0       1         5       6
| magic | u32 seq | u8 op | KVRecord |
  -----------------------   --------
    6 byte WAL header         see kvrecord.rs

Valid `op` values:

- 1: SET


*/

const WAL_MAGIC: u8 = b'w';
const OP_SET: u8 = 1u8;

pub(crate) struct WAL {
    wal_path: PathBuf,
    f: Option<File>,
    sync: WALSync,
    nextseq: u32,

    /// Number of writes to the WAL since it created
    pub(crate) wal_writes: u64,
}

pub(crate) fn new(wal_path: PathBuf, sync: WALSync) -> WAL {
    WAL {
        wal_path,
        f: None,
        sync,
        nextseq: 0,
        wal_writes: 0,
    }
}

// TODO
// We should have a state machine here. First you need to replay() the
// WAL both to read through the data to check it's valid and find the
// right seq for appending, and also to reload the database memtable.
// Then, and only then, should you be able to call write().

impl WAL {
    /// Replays the WAL into a memtable. Call this first.
    pub(crate) fn replay(
        &mut self,
    ) -> Result<SkipMap<Vec<u8>, KVValue>, ToyKVError> {
        if self.f.is_some() {
            return Err(ToyKVError::BadWALState);
        }

        let memtable = SkipMap::new();

        let file = match OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(self.wal_path.as_path())
        {
            Ok(it) => it,
            Err(e) => return Err(e.into()),
        };

        let size = fs::metadata(self.wal_path.as_path())?.len();
        // dbg!(size);

        if size == 0 {
            // New WAL file, nothing to replay
            self.f = Some(file);
            return Ok(memtable);
        }

        // 256k buffer covers at least one max size record.
        let mut bytes = BufReader::with_capacity(256 * 1024, &file);

        let mut cnt = 0;
        let mut last_read_seq = 0;
        while bytes.stream_position()? < size {
            // dbg!(bytes.stream_position()?);
            let rec = WALRecord::read_one(&mut bytes)?;
            // dbg!(&rec);
            match rec {
                Some(wr) => {
                    if wr.seq != self.nextseq {
                        return Err(ToyKVError::BadWALSeq {
                            expected: self.nextseq,
                            actual: wr.seq,
                        });
                    }
                    assert_eq!(wr.op, OP_SET, "Unexpected op code");
                    memtable.insert(wr.key, wr.value);
                    last_read_seq = wr.seq;
                    self.nextseq = wr.seq + 1;
                    self.wal_writes += 1;
                    cnt += 1;
                }
                None => break, // assume we hit the end of the WAL file
            };
        }

        println!(
            "Replayed {} records from WAL. last_read_seq={}.",
            cnt, last_read_seq
        );

        self.f = Some(file);

        Ok(memtable)
    }

    /// Appends entry to WAL
    pub(crate) fn write(
        &mut self,
        key: &[u8],
        value: &KVValue,
    ) -> Result<(), ToyKVError> {
        if self.f.is_none() {
            return Err(ToyKVError::BadWALState);
        }

        let seq = self.nextseq;
        // dbg!(seq);

        let file = self.f.as_mut().unwrap();
        WALRecord::write_one(file, seq, key, value)?;
        self.wal_writes += 1;
        // file.flush()?;
        if self.sync == WALSync::Full {
            file.sync_all()?;
        }

        // dbg!(self.wal_writes);

        self.nextseq += 1;

        Ok(())
    }

    pub(crate) fn wal_path(&self) -> PathBuf {
        self.wal_path.clone()
    }

    /// Delete the WAL file from disk.
    /// Take care; this could result in data loss if the
    /// associated memtable is not written to an sstable.
    pub(crate) fn delete(&mut self) -> Result<(), ToyKVError> {
        if self.f.is_none() {
            return Err(ToyKVError::BadWALState);
        }
        // Drop our writer so it closes
        self.f = None;
        fs::remove_file(self.wal_path.as_path())?;
        Ok(())
    }
}

#[derive(Debug)]
/// Read and write WAL records.
struct WALRecord {
    // magic: u8,
    seq: u32,
    op: u8,
    // From embedded KVRecord
    key: Vec<u8>,
    value: KVValue,
}
impl WALRecord {
    /// Read a single WAL record from a WAL file (or other Read struct).
    fn read_one<T: Read>(r: &mut T) -> Result<Option<WALRecord>, Error> {
        let mut header = [0u8; 6];
        r.read_exact(&mut header)?;

        // This might be clearer using byteorder and a reader
        let magic = header[0];
        assert_eq!(magic, WAL_MAGIC, "Unexpected magic byte");
        let seq = u32::from_be_bytes(header[1..5].try_into().unwrap());
        let op = header[5];

        let kv = KVRecord::read_one(r)?;

        match kv {
            None => Ok(None),
            Some(kv) => {
                let wr = WALRecord {
                    seq,
                    op,
                    key: kv.key,
                    value: kv.value,
                };

                // println!("Read WAL record: {:?}", wr);

                Ok(Some(wr))
            }
        }
    }

    /// Write a single WAL record to a WAL file (or other Write struct).
    ///
    /// This doesn't take a WALRecord so we can take slices of the data to
    /// write rather than a copy, as we don't need a copy.
    fn write_one<T: Write>(
        w: &mut T,
        seq: u32,
        key: &[u8],
        value: &KVValue,
    ) -> Result<(), Error> {
        // Create our record and attempt to write
        // it out in one go.
        let mut buf = Vec::<u8>::new();
        buf.push(WAL_MAGIC);
        buf.extend(seq.to_be_bytes());
        buf.push(OP_SET);
        buf.extend(KVWriteRecord { key, value }.serialize());
        w.write_all(&buf)?;

        Ok(())
    }
}
