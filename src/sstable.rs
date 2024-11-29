use std::{
    collections::BTreeMap,
    fs::{self, DirEntry, File, OpenOptions},
    io::{BufReader, Error, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use crate::{
    kvrecord::{KVRecord, KVWriteRecord, KVWriteValue},
    merge_iterator::{self, MergeIterator},
    KVValue,
};

/// Provides types and functions to manage operations on a
/// set of SSTables on-disk. In toykv, we have one "branch"
/// of SSTables, from newest to oldest.
///
/// As is usual for LSM Tree databases, the write functions
/// are used to flush an in-memory memtable to an on-disk
/// file. These files maintain a chain in a simple way,
/// by naming them using lexographically sortable int file
/// names, 0000000001.data. Ten digits. Thus our single branch
/// is created by just ordering files by their name.
/// (Ten digits as u32::MAX is 4294967295).
///
/// When reading, therefore, we search for the given key
/// from the newest (highest numbered) file to the
/// oldest (lowest numbered) file, scanning through
/// each file until we come to a key that is the key
/// we are after, in which case we return its value,
/// or is greater than the key we are after, in which
/// case we move to the next file.
///
/// As this is toykv and not productionkv, we use a very
/// simple file format of KVRecords streamed to disk.
///
/// Implementation is NOT thread safe. We hold open BufReaders
/// to files, so multiple readers would stomp all over each
/// other. Multiple writers, meanwhile, would just totally
/// make a mess that probably wouldn't be recoverable.

/// Manage and search a set of SSTable files on disk.
pub(crate) struct SSTables {
    d: PathBuf,
    sstables: SSTablesReader,
}

/// Create a new SSTables whose files live in the directory d.
pub(crate) fn new_sstables(d: &Path) -> Result<SSTables, Error> {
    let sstables = new_reader(d)?;
    Ok(SSTables {
        d: d.to_path_buf(),
        sstables,
    })
}

impl SSTables {
    /// Write a new SSTable to the set managed by this
    /// SSTables. After this method returns, the contents
    /// of the memtable are durable on disk and are used
    /// by future calls to `get`.
    pub(crate) fn write_new_sstable(
        &mut self,
        memtable: &BTreeMap<Vec<u8>, KVValue>,
    ) -> Result<(), Error> {
        let mut sst = new_writer(self.d.as_path())?;
        for entry in memtable {
            sst.write(entry.0, entry.1.into())?;
        }
        sst.finalise()?;

        // Need a new reader to regenerate the file list to
        // read from.
        self.sstables = new_reader(self.d.as_path())?;
        Ok(())
    }

    /// Retrieve the latest value for `k` in the on disk
    /// set of sstables.
    pub(crate) fn get(&mut self, k: &[u8]) -> Result<Option<KVValue>, Error> {
        self.sstables.get(k)
    }
}

/// Return the SSTables in dir, ordered newest to oldest.
///
/// The ordering is such that a get for a key should proceed
/// scanning files from left to right in the vec.
fn sorted_sstable_files(dir: &Path) -> Result<Vec<DirEntry>, Error> {
    // List the files
    // filter to actual files
    // filter to data files
    // filter path file_stem can be parsed to u32.
    // now we have valid data files.
    let mut entries: Vec<DirEntry> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|p| p.path().is_file())
        .filter(|p| p.path().extension().is_some_and(|e| e == "data"))
        .filter(|e| {
            e.path().file_stem().is_some_and(|stem| {
                stem.len() == 10 && stem.to_string_lossy().parse::<u32>().is_ok()
            })
        })
        .collect();

    // Order the filenames and find the largest (the zero prefix should give
    // ordering).
    entries.sort_unstable_by_key(|e| {
        let path = e.path();
        let stem = path.file_stem().unwrap();
        stem.to_os_string()
    });

    // Reverse, so the newest sstable comes first in the list.
    entries.reverse();

    Ok(entries)
}

#[cfg(test)]
mod tests_sorted_sstable_files {
    use std::{fs::OpenOptions, io::Error};

    use super::*;

    #[test]
    fn test_sorted_sstable_files() -> Result<(), Error> {
        let tmp_dir = tempfile::tempdir().unwrap();

        // Check that we skip invalid file names and that we
        // order correctly from highest (newest) to lowest (oldest).
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("0000000001.data"))?;
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("0000000010.data"))?;
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("0000060001.data"))?;
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("0000000021.data"))?;

        // Bad file stem length
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("0060001.data"))?;
        // Right length, not a number
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("barbarbarb.data"))?;
        // Right length, number, wrong extension
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("0000000009.bar"))?;

        let got: Vec<PathBuf> = sorted_sstable_files(tmp_dir.path())?
            .iter()
            .map(|d| d.path())
            .collect();

        assert_eq!(
            got,
            vec![
                tmp_dir.path().join("0000060001.data"),
                tmp_dir.path().join("0000000021.data"),
                tmp_dir.path().join("0000000010.data"),
                tmp_dir.path().join("0000000001.data"),
            ]
        );
        Ok(())
    }
}

/// Provides methods to write already-sorted KVRecords to an on-disk file.
struct SSTableFileWriter {
    #[allow(dead_code)] // used in tests to check filepath
    p: PathBuf,
    f: File,
    start: Instant,
}
/// Start writing a new immutable file in the dir path.
fn new_writer(dir: &Path) -> Result<SSTableFileWriter, Error> {
    // So basically here we have to read 00000001.data etc and find the
    // greatest number we have so far, then create a path that is inc that
    // number by one.

    let entries = sorted_sstable_files(dir)?;

    // If we've no files, then make our first file.
    // Otherwise, parse the file_stem into an int (lots of unwrap as we
    // know the filename has a step and it parses to the uint from the
    // filters above).
    // Check it's not max, if so return error.
    // Add 1 to it.
    // Make the filename
    let path = match entries.first() {
        None => dir.join(format!("{:010}.data", 1)),
        Some(e) => {
            let path = e.path();
            let stem = path.file_stem().unwrap();
            let n = stem.to_string_lossy().parse::<u32>().unwrap();
            if n >= u32::MAX - 1 {
                panic!("Run out of SSTable numbers!");
            }
            dir.join(format!("{:010}.data", n + 1))
        }
    };

    println!("[new_writer] opening new SSTable at: {:?}", path);

    // Open that file, and stick it in the SSTableFileWriter.
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path.as_path())?;

    Ok(SSTableFileWriter {
        f,
        p: path.to_path_buf(),
        start: Instant::now(),
    })
}
impl SSTableFileWriter {
    /// Write a key/value pair to the SSTable.
    /// It is assumed that successive calls to write will always use
    /// increasing keys; that is, SSTableFileWriter does not sort keys
    /// itself.
    fn write(&mut self, key: &[u8], value: KVWriteValue) -> Result<(), Error> {
        // TODO pass all things down as KVValue and put that definition next
        // to KVRecord?
        let buf = KVWriteRecord { key, value }.serialize();
        self.f.write_all(&buf)?;
        Ok(())
    }

    /// Perform the final writes to the file. After this the writer
    /// can be safely dropped.
    fn finalise(&mut self) -> Result<(), Error> {
        self.f.flush()?;
        self.f.sync_all()?;
        let elapsed_time = self.start.elapsed();
        println!("Writing sstable took {}ms.", elapsed_time.as_millis());
        Ok(())
    }
}

#[cfg(test)]
mod tests_writer {
    use std::io::Error;

    use crate::kvrecord;

    use super::*;

    #[test]
    fn test_write_sstable() -> Result<(), Error> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000001.data"));
        w.write(&[1, 2, 3], kvrecord::KVWriteValue::Some(&[1, 2, 3, 4]))?;
        w.finalise()?;

        let w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000002.data"));

        // when we drop the previous w, an empty file should
        // end up on disk.
        let w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000003.data"));
        let w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000004.data"));
        Ok(())
    }
}

/// Reader for single SSTable file on disk
struct SSTableFileReader {
    f: BufReader<File>,
    p: PathBuf,
}
fn new_file_reader(path: PathBuf) -> Result<SSTableFileReader, Error> {
    let f = OpenOptions::new().read(true).open(&path)?;
    let br = BufReader::with_capacity(256 * 1024, f);
    Ok(SSTableFileReader { f: br, p: path })
}
impl SSTableFileReader {
    /// Search this SSTable file for a key.
    /// This advances the iterator until it finds the key,
    /// or first item greater than the key. Reset or seek
    /// the iterator after using.
    fn get(&mut self, k: &[u8]) -> Result<Option<KVValue>, Error> {
        self.reset()?;
        for x in self {
            match x {
                Ok(kv) => {
                    // We found it!
                    if kv.key.as_slice() == k {
                        return Ok(Some(kv.value));
                    }
                    // Gone beyond the key, stop scanning
                    // this file, return None as it isn't here.
                    if kv.key.as_slice() > k {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.f.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    fn duplicate(&self) -> SSTableFileReader {
        new_file_reader(self.p.clone()).unwrap()
    }
}

/// Iterate over the file, from wherever we are up to
/// right now. Call reset() to reset the iterator prior
/// to use, to ensure starting from the beginning of the
/// SSTable.
impl Iterator for SSTableFileReader {
    type Item = Result<KVRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let kv = KVRecord::read_one(&mut self.f);
        match kv {
            Ok(kv) => match kv {
                Some(kv) => Some(Ok(kv)),
                None => None, // reached the end of the file
            },
            Err(e) => Some(Err(e)),
        }
    }
}

/// Iterate entries in an on-disk SSTable.
struct SSTablesReader {
    /// tables maintains a set of BufReaders on every sstable
    /// file in the set. This isn't that scalable.
    tables: Vec<SSTableFileReader>,
}

/// Create a new SSTableFileReader that is able to search
/// for keys in the set of SSTables managed within a folder.
fn new_reader(dir: &Path) -> Result<SSTablesReader, Error> {
    let files = sorted_sstable_files(dir)?;
    let mut tables: Vec<SSTableFileReader> = vec![];
    for p in files {
        tables.push(new_file_reader(p.path())?);
    }
    Ok(SSTablesReader { tables })
}

impl SSTablesReader {
    /// Search through the SSTables available to this reader for
    /// a key. Return an Option with its value.
    fn get(&mut self, k: &[u8]) -> Result<Option<KVValue>, Error> {
        // self.tables is in the right order for scanning the sstables
        // on disk. Read each to find k. If no SSTable file contains
        // k, return None.
        for t in self.tables.as_mut_slice() {
            match t.get(k)? {
                Some(v) => return Ok(Some(v)),
                None => continue, // not found in this SSTable
            }
        }
        // Otherwise, we didn't find it.
        Ok(None)
    }

    fn scan(&mut self) -> MergeIterator {
        // We could create a set of new SSTableFileReader
        // iterators for this, but probably we want to figure
        // out what borrowing looks like, because otherwise
        // we have to create new file handles on our files,
        // which seems a bit pointless. But even if pointless
        // might get us started more quickly.
        // merge_iterator::new_merge_iterator(vec![])
        let mut vec = vec![];
        for t in &self.tables {
            vec.push(Box::new(t.duplicate())
                as Box<dyn Iterator<Item = Result<KVRecord, std::io::Error>>>);
        }
        merge_iterator::new_merge_iterator(vec)
    }
}
#[cfg(test)]
mod tests_reader {
    use std::io::Error;

    use crate::kvrecord;

    use super::*;

    #[test]
    fn test_read_sstable() -> Result<(), Error> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000001.data"));
        w.write(&[1, 2, 3], kvrecord::KVWriteValue::Some(&[1, 2, 3, 4]))?;
        w.finalise()?;

        let mut r = new_reader(tmp_dir.path())?;
        assert_eq!(r.get(&[1, 2, 3])?.unwrap(), KVValue::Some(vec![1, 2, 3, 4]));
        assert_eq!(r.get(&[14])?, None);

        Ok(())
    }

    #[test]
    fn test_read_sstables() -> Result<(), Error> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000001.data"));
        w.write(&[1, 2, 3], kvrecord::KVWriteValue::Some(&[1, 2, 3, 4]))?;
        w.write(&[23, 24], kvrecord::KVWriteValue::Some(&[100, 122, 4]))?;
        w.write(&[66, 23, 24], kvrecord::KVWriteValue::Some(&[100, 122, 4]))?;
        w.finalise()?;

        let mut w = new_writer(tmp_dir.path())?;
        w.write(&[23, 24], kvrecord::KVWriteValue::Some(&[100, 122, 4]))?;
        w.finalise()?;
        assert_eq!(w.p, tmp_dir.path().join("0000000002.data"));

        let w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000003.data"));

        let mut w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000004.data"));
        w.write(&[1, 2, 3], kvrecord::KVWriteValue::Some(&[6, 7, 8, 9]))?; // set new value in newer layer
        w.finalise()?;

        let mut r = new_reader(tmp_dir.path())?;
        assert_eq!(r.get(&[1, 2, 3])?.unwrap(), KVValue::Some(vec![6, 7, 8, 9]));
        assert_eq!(r.get(&[23, 24])?.unwrap(), KVValue::Some(vec![100, 122, 4]));
        assert_eq!(r.get(&[14])?, None);

        Ok(())
    }

    fn kv(k: &[u8], v: &[u8]) -> KVRecord {
        KVRecord {
            key: k.into(),
            value: crate::KVValue::Some(v.into()),
        }
    }

    #[test]
    fn test_scan_sstables() -> Result<(), Error> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000001.data"));
        w.write(&[1, 2, 3], kvrecord::KVWriteValue::Some(&[1, 2, 3, 4]))?;
        w.write(&[23, 24], kvrecord::KVWriteValue::Some(&[100, 122, 4]))?;
        w.write(&[66, 23, 24], kvrecord::KVWriteValue::Some(&[100, 122, 4]))?;
        w.finalise()?;

        let mut w = new_writer(tmp_dir.path())?;
        w.write(&[23, 24], kvrecord::KVWriteValue::Some(&[100, 122, 4]))?;
        w.finalise()?;
        assert_eq!(w.p, tmp_dir.path().join("0000000002.data"));

        let w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000003.data"));

        let mut w = new_writer(tmp_dir.path())?;
        assert_eq!(w.p, tmp_dir.path().join("0000000004.data"));
        w.write(&[1, 2, 3], kvrecord::KVWriteValue::Some(&[6, 7, 8, 9]))?; // set new value in newer layer
        w.finalise()?;

        let mut r = new_reader(tmp_dir.path())?;
        let mut records = r.scan();
        assert_eq!(records.next(), Some(kv(&[1, 2, 3], &[6, 7, 8, 9])));
        assert_eq!(records.next(), Some(kv(&[23, 24], &[100, 122, 4])));
        assert_eq!(records.next(), Some(kv(&[66, 23, 24], &[100, 122, 4])));
        assert_eq!(records.next(), None);

        Ok(())
    }
}
