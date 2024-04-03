// Implements a simple WAL for the database's memtable.

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufReader, Error, ErrorKind, Read, Write},
    path::Path,
};

use crate::ToyKVError;

/*
The WAL is a simple sequence of records written to a file. The main
interesting item is the seq, which is expected to incrase by 1 with
each item. A u32 allows for 4,294,967,295 records. We should've
flushed the associated memtable to disk long before we get that
far.

0       1         5       6            8              12    16            N
| magic | u32 seq | u8 op | u16 keylen | u32 valuelen | pad | key | value |
  ---------------------------------------------------------   ---   -----
                 16 bytes header                               |      |
                                                      keylen bytes    |
                                                                      |
                                                           valuelen bytes

Valid `op` values:

- 1: SET


*/

const MAGIC: u8 = b'w';
const OP_SET: u8 = 1u8;
const PAD: u32 = 0u32;

#[derive(Debug)]
struct WALRecord {
    magic: u8,
    seq: u32,
    op: u8,
    keylen: u16,
    valuelen: u32,
    _pad: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

pub(crate) struct WAL<'a> {
    d: &'a Path,
}

pub(crate) fn new(d: &Path) -> WAL {
    WAL { d }
}

// TODO
// We should have a state machine here. First you need to replay() the
// WAL both to read through the data to check it's valid and find the
// right seq for appending, and also to reload the database memtable.
// Then, and only then, should you be able to call write().

impl<'a> WAL<'a> {
    /// Replays the WAL into a memtable. Call this first.
    pub(crate) fn replay(&mut self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, ToyKVError> {
        let wal_path = self.d.join("db.wal");

        let mut memtable = BTreeMap::new();

        let file = match OpenOptions::new().read(true).open(wal_path) {
            Ok(it) => it,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(memtable),
            Err(e) => return Err(e.into()),
        };

        // A somewhat large buffer as we expect these files to be quite large.
        let mut bytes = BufReader::with_capacity(256 * 1024, file);

        loop {
            let rec = read_wal_record(&mut bytes)?;
            match rec {
                Some(wr) => memtable.insert(wr.key, wr.value),
                None => break, // assume we hit the end of the WAL file
            };
        }

        Ok(memtable)
    }

    /// Appends entry to WAL
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), ToyKVError> {
        let seq = 1u32; // TODO implement sequence numbers for records

        // TODO hold the file open in the WAL struct rather than opening
        // for every write.

        let wal_path = self.d.join("db.wal");
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(wal_path)?;

        // Create our record and attempt to write
        // it out in one go.
        let mut buf = Vec::<u8>::new();
        buf.push(MAGIC);
        buf.extend(seq.to_be_bytes());
        buf.push(OP_SET);
        buf.extend((key.len() as u16).to_be_bytes());
        buf.extend((value.len() as u32).to_be_bytes());
        buf.extend(PAD.to_be_bytes());
        buf.extend(key);
        buf.extend(value);
        file.write_all(&buf)?;
        file.sync_all()?;

        Ok(())
    }
}

/// Read a single WAL record from a WAL file (or other Read struct).
fn read_wal_record<T: Read>(r: &mut T) -> Result<Option<WALRecord>, Error> {
    let mut header = [0u8; 16];
    let n = r.read(&mut header)?;
    if n < 16 {
        // Is this really only Ok if we read zero?
        // 0 < n < 16 probably actually means a corrupt file.
        return Ok(None);
    }

    // This might be clearer using byteorder and a reader
    let magic = header[0];
    assert_eq!(magic, MAGIC, "Unexpected magic byte");
    let seq = u32::from_be_bytes(header[1..5].try_into().unwrap());
    let op = header[5];
    assert_eq!(op, OP_SET, "Unexpected op code");
    let keylen = u16::from_be_bytes(header[6..8].try_into().unwrap());
    let valuelen = u32::from_be_bytes(header[8..12].try_into().unwrap());
    let _pad = u32::from_be_bytes(header[12..16].try_into().unwrap());
    assert_eq!(_pad, PAD, "Unexpected padding of non-zero");

    let mut key = Vec::with_capacity(keylen as usize);
    r.by_ref().take(keylen as u64).read_to_end(&mut key)?;
    let mut value = Vec::with_capacity(valuelen as usize);
    r.by_ref().take(valuelen as u64).read_to_end(&mut value)?;

    let wr = WALRecord {
        magic,
        seq,
        op,
        keylen,
        valuelen,
        _pad,
        key,
        value,
    };

    println!("Read WAL record: {:?}", wr);

    Ok(Some(wr))
}
