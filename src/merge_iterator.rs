use std::iter::Peekable;

use crate::kvrecord::KVRecord;

/// MergeIterator takes a vec of child iterators over KVRecord and
/// emits the KVRecords from all child iterators, ordered by key. It
/// assumes the child iterators are ordered by key. If multiple child
/// iterators contain KVRecords with the same key, the tie is broken
/// by returning the KVRecord from the child iterator with the lowest
/// index.
pub(crate) struct MergeIterator<'a> {
    iters: Vec<Peekable<&'a mut (dyn Iterator<Item = KVRecord> + 'a)>>,
}

pub(crate) fn new_merge_iterator<'a>(
    x: Vec<&'a mut dyn Iterator<Item = KVRecord>>,
) -> MergeIterator<'a> {
    MergeIterator {
        iters: x.into_iter().map(|x| x.peekable()).collect(),
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = KVRecord;

    fn next(&mut self) -> Option<Self::Item> {
        // NB:
        // Assumes that each iterator in `iters` contains
        // at most one entry for a given key, and that the
        // keys are ordered.

        // Find the lowest key in our iterator set by peeking
        // at every iterator's next value. Will naturally leave
        // `min` at None and end this iterator when the child
        // iters are exhausted.
        let mut min = None;
        for x in self.iters.iter_mut() {
            match x.peek() {
                Some(kvr) => {
                    min = match min {
                        None => Some(kvr.clone()),
                        Some(ref minkvr) => {
                            if kvr.key < minkvr.key {
                                Some(kvr.clone())
                            } else {
                                min
                            }
                        }
                    }
                }
                None => {}
            }
        }
        // If we found a min, advance all the iters that have
        // a record with the same key (because we now have the
        // correct KVRecord to return for that key in `min`)
        match min.as_ref() {
            None => {}
            Some(kvr) => {
                let minkey = kvr.key.as_slice();
                for x in self.iters.iter_mut() {
                    match x.peek() {
                        Some(kvr) => {
                            if kvr.key.as_slice() == minkey {
                                x.next(); // advance past this key
                            }
                        }
                        None => {}
                    }
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
        let iters = vec![];
        let mut mt = new_merge_iterator(iters);
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
        // The construction of the argument to new_merge_iterator
        // is a mess. Helpful references:
        // https://users.rust-lang.org/t/vector-of-iterators/3876/2
        //    particularly the note on into_iter to avoid borrowed KVRecords
        // https://users.rust-lang.org/t/function-that-returns-impl-iterator-item-box-dyn-iterator-item-i32/92079/2
        //    which notes the explicit "as Box..." hint needed for the compiler
        // Not sure whether we can do something cleaner in the new_merge_iterator
        // method to make this cleaner.
        let kvr = kv("foo", "bar");
        let expected = kvr.clone();
        let mut sstable = vec![kvr].into_iter();
        // Let's read this type for our own edification:
        // A vector containing mutable references to sstables, which
        // we have to cast to mutable references to types that implement
        // this Iterator trait over KVRecord items.
        let iters = vec![&mut sstable as &mut dyn Iterator<Item = KVRecord>];
        let mut mt = new_merge_iterator(iters);
        assert_eq!(mt.next(), Some(expected));
        Ok(())
    }

    #[test]
    fn test_merge_two_unique_vecs() -> Result<(), Error> {
        let mut sstable1 = vec![kv("aaaa", "barA"), kv("cccc", "barC")].into_iter();
        let mut sstable2 = vec![kv("bbbb", "barB"), kv("dddd", "barD")].into_iter();
        let iters = vec![
            &mut sstable1 as &mut dyn Iterator<Item = KVRecord>,
            &mut sstable2 as &mut dyn Iterator<Item = KVRecord>,
        ];
        let mut mt = new_merge_iterator(iters);
        assert_eq!(mt.next(), Some(kv("aaaa", "barA")));
        assert_eq!(mt.next(), Some(kv("bbbb", "barB")));
        assert_eq!(mt.next(), Some(kv("cccc", "barC")));
        assert_eq!(mt.next(), Some(kv("dddd", "barD")));
        Ok(())
    }
    #[test]
    fn test_merge_two_duplicate_key_vecs() -> Result<(), Error> {
        let mut sstable1 = vec![kv("aaaa", "barA"), kv("cccc", "barC")].into_iter();
        let mut sstable2 = vec![
            kv("aaaa", "barABAD"),
            kv("cccc", "barCBAD"),
            kv("dddd", "barD"),
        ]
        .into_iter();
        let iters = vec![
            &mut sstable1 as &mut dyn Iterator<Item = KVRecord>,
            &mut sstable2 as &mut dyn Iterator<Item = KVRecord>,
        ];
        let mut mt = new_merge_iterator(iters);
        assert_eq!(mt.next(), Some(kv("aaaa", "barA")));
        assert_eq!(mt.next(), Some(kv("cccc", "barC")));
        assert_eq!(mt.next(), Some(kv("dddd", "barD")));
        Ok(())
    }
}
