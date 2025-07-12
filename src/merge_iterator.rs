use std::{io::Error, iter::Peekable, ops::Bound};

use crate::{error::ToyKVError, kvrecord::KVRecord};

/// TableIterator can iterate over either memtables or sstables (or
/// Vecs during testing). We Box it so we can use polymorphism to
/// loop over both memtables and sstables when scanning.
type TableIterator<'a> = Box<dyn Iterator<Item = Result<KVRecord, Error>> + 'a>;

/// MergeIterator takes a vec of child iterators over KVRecord and
/// emits the KVRecords from all child iterators, ordered by key. It
/// assumes the child iterators are ordered by key. If multiple child
/// iterators contain KVRecords with the same key, the tie is broken
/// by returning the KVRecord from the child iterator that was first
/// added using `add_iterator`.
pub(crate) struct MergeIterator<'a> {
    /// lsmtables are the memtables and sstables underlying this MergeIterator
    lsmtables: Vec<Peekable<TableIterator<'a>>>,
    stopped: bool,
    end_key: Bound<Vec<u8>>,
}

impl<'a> MergeIterator<'a> {
    pub fn new(end_key: Bound<Vec<u8>>) -> Self {
        Self {
            lsmtables: Vec::new(),
            stopped: false,
            end_key,
        }
    }

    pub fn add_iterator<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Result<KVRecord, Error>> + 'a,
    {
        let b: TableIterator<'a> = Box::new(iter);
        self.lsmtables.push(b.peekable());
    }
}

impl<'a> Iterator for MergeIterator<'a> {
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
        for x in self.lsmtables.iter_mut() {
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
            for x in self.lsmtables.iter_mut() {
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

        // If we have reached the ends of the bounds
        // set, update min to None
        if let Some(Ok(next_record)) = min.as_ref() {
            min = match &self.end_key {
                Bound::Unbounded => min,
                Bound::Included(x) if next_record.key > *x => None,
                Bound::Included(_) => min,
                Bound::Excluded(x) if next_record.key >= *x => None,
                Bound::Excluded(_) => min,
            }
        }

        if min == None {
            self.stopped = true;
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
        let mut mt = MergeIterator::new(Bound::Unbounded);
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
        let mut mt = MergeIterator::new(Bound::Unbounded);
        mt.add_iterator(sstable.into_iter());
        assert_eq!(mt.next(), Some(Ok(expected)));
        Ok(())
    }

    #[test]
    fn test_merge_one_vec_bound_included() -> Result<(), Error> {
        let kvr = kv("foo", "bar");
        let expected = kvr.clone();
        let sstable = vec![Ok(kvr)];
        let mut mt = MergeIterator::new(Bound::Included("foo".into()));
        mt.add_iterator(sstable.into_iter());
        assert_eq!(mt.next(), Some(Ok(expected)));
        Ok(())
    }

    #[test]
    fn test_merge_one_vec_bound_excluded() -> Result<(), Error> {
        let kvr = kv("foo", "bar");
        let sstable = vec![Ok(kvr)];
        let mut mt = MergeIterator::new(Bound::Excluded("foo".into()));
        mt.add_iterator(sstable.into_iter());
        assert_eq!(mt.next(), None);
        Ok(())
    }

    #[test]
    fn test_merge_two_unique_vecs() -> Result<(), Error> {
        let sstable1 = vec![Ok(kv("aaaa", "barA")), Ok(kv("cccc", "barC"))];
        let sstable2 = vec![Ok(kv("bbbb", "barB")), Ok(kv("dddd", "barD"))];
        let mut mt = MergeIterator::new(Bound::Unbounded);
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());

        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert_eq!(mt.next(), Some(Ok(kv("bbbb", "barB"))));
        assert_eq!(mt.next(), Some(Ok(kv("cccc", "barC"))));
        assert_eq!(mt.next(), Some(Ok(kv("dddd", "barD"))));
        assert_eq!(mt.next(), None);
        Ok(())
    }

    #[test]
    fn test_merge_two_unique_vecs_bound_included() -> Result<(), Error> {
        let sstable1 = vec![Ok(kv("aaaa", "barA")), Ok(kv("cccc", "barC"))];
        let sstable2 = vec![Ok(kv("bbbb", "barB")), Ok(kv("dddd", "barD"))];
        let mut mt = MergeIterator::new(Bound::Included("cccc".into()));
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());
        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert_eq!(mt.next(), Some(Ok(kv("bbbb", "barB"))));
        assert_eq!(mt.next(), Some(Ok(kv("cccc", "barC"))));
        assert_eq!(mt.next(), None);
        Ok(())
    }

    #[test]
    fn test_merge_two_unique_vecs_bound_excluded() -> Result<(), Error> {
        let sstable1 = vec![Ok(kv("aaaa", "barA")), Ok(kv("cccc", "barC"))];
        let sstable2 = vec![Ok(kv("bbbb", "barB")), Ok(kv("dddd", "barD"))];
        let mut mt = MergeIterator::new(Bound::Excluded("cccc".into()));
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());
        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert_eq!(mt.next(), Some(Ok(kv("bbbb", "barB"))));
        assert_eq!(mt.next(), None);
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
        let mut mt = MergeIterator::new(Bound::Unbounded);
        mt.add_iterator(sstable1.into_iter());
        mt.add_iterator(sstable2.into_iter());

        assert_eq!(mt.next(), Some(Ok(kv("aaaa", "barA"))));
        assert_eq!(mt.next(), Some(Ok(kv("cccc", "barC"))));
        assert_eq!(mt.next(), Some(Ok(kv("dddd", "barD"))));
        assert_eq!(mt.next(), None);
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
        let mut mt = MergeIterator::new(Bound::Unbounded);
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
