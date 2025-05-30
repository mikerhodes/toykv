use std::{io::Error, iter::Peekable};

use crate::{error::ToyKVError, kvrecord::KVRecord};

// Generation of an error is completely separate from how it is displayed.
// There's no need to be concerned about cluttering complex logic with the display style.
//
// Note that we don't store any extra info about the errors. This means we can't state
// which string failed to parse without modifying our types to carry that information.
// impl fmt::Display for DoubleError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "invalid first item to double")
//     }
// }

/// TableIterator can iterate over either memtables or sstables (or
/// Vecs during testing). We Box it so we can use polymorphism to
/// loop over both memtables and sstables when scanning.
type TableIterator = Box<dyn Iterator<Item = Result<KVRecord, Error>>>;

/// MergeIterator takes a vec of child iterators over KVRecord and
/// emits the KVRecords from all child iterators, ordered by key. It
/// assumes the child iterators are ordered by key. If multiple child
/// iterators contain KVRecords with the same key, the tie is broken
/// by returning the KVRecord from the child iterator with the lowest
/// index.
pub(crate) struct MergeIterator {
    sstables: Vec<Peekable<TableIterator>>,
    stopped: bool,
}

impl MergeIterator {
    pub fn new() -> Self {
        Self {
            sstables: Vec::new(),
            stopped: false,
        }
    }

    pub fn add_iterator<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Result<KVRecord, Error>> + 'static,
    {
        let b: TableIterator = Box::new(iter);
        self.sstables.push(b.peekable());
    }
}

impl Iterator for MergeIterator {
    type Item = Result<KVRecord, ToyKVError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        // NB:
        // Assumes that each iterator in `sstables` contains
        // at most one entry for a given key, and that the
        // keys are ordered.

        // Find the lowest key in our iterator set by peeking
        // at every iterator's next value. Will naturally leave
        // `min` at None and end this iterator when the child
        // iters are exhausted.
        let mut min: Option<Self::Item> = None;
        // TODO chain() can be used when we have memtables iter too
        //      maybe with once() if we just have the one memtable.
        for x in self.sstables.iter_mut() {
            match x.peek() {
                Some(Err(e)) => {
                    self.stopped = true;
                    return Some(Err(ToyKVError::from(e)));
                }
                Some(Ok(kvr)) => {
                    min = match min {
                        None => Some(Ok(kvr.clone())),
                        Some(Ok(ref minkvr)) => {
                            if kvr.key < minkvr.key {
                                Some(Ok(kvr.clone()))
                            } else {
                                min
                            }
                        }
                        Some(Err(e)) => Some(Err(e)),
                    }
                }
                None => {}
            }
        }

        // If we found a min, advance all the iters that have
        // a record with the same key (because we now have the
        // correct KVRecord to return for that key in `min`)
        if let Some(Ok(kvr)) = min.as_ref() {
            let minkey = kvr.key.as_slice();
            for x in self.sstables.iter_mut() {
                match x.peek() {
                    Some(Err(_e)) => {}
                    Some(Ok(kvr)) => {
                        if kvr.key.as_slice() == minkey {
                            x.next(); // advance past this key
                        }
                    }
                    None => {}
                }
            }
        }

        min
    }
}

#[cfg(test)]
mod tests_merge_iterator {
    use std::io::Error;

    // use crate::kvrecord;

    use super::*;

    #[test]
    fn test_merge_zero_vec() -> Result<(), Error> {
        let mut mt = MergeIterator::new();
        assert_eq!(mt.next(), None);
        Ok(())
    }

    fn kv(k: &str, v: &str) -> KVRecord {
        KVRecord {
            key: k.into(),
            value: crate::KVValue::Some(v.into()),
        }
    }

    #[test]
    fn test_merge_one_vec() -> Result<(), Error> {
        let kvr = kv("foo", "bar");
        let expected = kvr.clone();
        let sstable = vec![Ok(kvr)];
        let mut mt = MergeIterator::new();
        mt.add_iterator(sstable.into_iter());

        assert_eq!(mt.next(), Some(Ok(expected)));
        Ok(())
    }

    #[test]
    fn test_merge_two_unique_vecs() -> Result<(), Error> {
        let sstable1 = vec![Ok(kv("aaaa", "barA")), Ok(kv("cccc", "barC"))];
        let sstable2 = vec![Ok(kv("bbbb", "barB")), Ok(kv("dddd", "barD"))];
        let mut mt = MergeIterator::new();
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());

        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert_eq!(mt.next(), Some(Ok(kv("bbbb", "barB"))));
        assert_eq!(mt.next(), Some(Ok(kv("cccc", "barC"))));
        assert_eq!(mt.next(), Some(Ok(kv("dddd", "barD"))));
        Ok(())
    }
    #[test]
    fn test_merge_two_duplicate_key_vecs() -> Result<(), Error> {
        let sstable1 = vec![Ok(kv("aaaa", "barA")), Ok(kv("cccc", "barC"))];
        let sstable2 = vec![
            Ok(kv("aaaa", "barABAD")),
            Ok(kv("cccc", "barCBAD")),
            Ok(kv("dddd", "barD")),
        ];
        let mut mt = MergeIterator::new();
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());

        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert_eq!(mt.next(), Some(Ok(kv("cccc", "barC"))));
        assert_eq!(mt.next(), Some(Ok(kv("dddd", "barD"))));
        Ok(())
    }
    #[test]
    fn test_error_stops_iter() -> Result<(), Error> {
        let sstable1 = vec![
            Ok(kv("aaaa", "barA")),
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "oh no!",
            )),
            Ok(kv("cccc", "barC")),
        ];
        let sstable2 = vec![
            Ok(kv("aaaa", "barABAD")),
            Ok(kv("cccc", "barCBAD")),
            Ok(kv("dddd", "barD")),
        ];
        let mut mt = MergeIterator::new();
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());

        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert!(matches!(mt.next(), Some(Err(_e))));
        assert_eq!(mt.next(), None);
        assert_eq!(mt.next(), None);
        assert_eq!(mt.next(), None);
        assert_eq!(mt.next(), None);
        Ok(())
    }
}
