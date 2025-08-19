#![allow(dead_code)]
/// MergeIterator (v2) merges sorted iterators from LSM tree layers
/// (memtable + SSTables). Since the same key can exist in multiple
/// layers, we use a age system where:
/// - Lower age number = more recent data (memtables have ages 0 and 1)
/// - When the same key appears in multiple iterators, only the most recent
///   key is emitted (via the lowest age).
/// - This ensures we implement the LSM's "newer data shadows older data"
///   semantics.
/// - Other than age, keys are ordered lowest-first. Errors are ordered
///   lowest of all in our min-heap, meaning that the iterator can bail
///   immediately on an error.
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    io::Error,
};

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
    stopped: bool,
    heap: MinHeap<QueueItem<'a>>,
    /// Lower age = more recent memtable/sstable
    next_iterator_age: u32,
    /// last_key is the last key we saw, to prevent emitting
    /// duplicate keys.
    last_emitted_key: Option<Vec<u8>>,
}

struct QueueItem<'a> {
    kvrecord: Result<KVRecord, Error>,
    iterator: TableIterator<'a>,
    /// Lower age = more recent memtable/sstable
    age: u32,
}

impl<'a> PartialEq for QueueItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl<'a> PartialOrd for QueueItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> Eq for QueueItem<'a> {}
impl<'a> Ord for QueueItem<'a> {
    /// cmp orders such that the following are lower for our min-heap:
    /// - Errors are lowest
    /// - Then lowest keys
    /// - Then lowest age tie-breaks
    /// This guarantees:
    /// 1. That we exit as early as possible on errors.
    /// 2. That the latest written value appears first (memtables
    ///    first, then sstables).
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.kvrecord, &other.kvrecord) {
            // Errors bubble up immediately
            (Err(_), Ok(_)) => Ordering::Less,
            (Ok(_), Err(_)) => Ordering::Greater,
            (Err(_), Err(_)) => self.age.cmp(&other.age),
            (Ok(s_kvrecord), Ok(o_kvrecord)) => {
                // Primary sort - lexographic by key
                s_kvrecord
                    .key
                    .cmp(&o_kvrecord.key)
                    // Tie-break with age (lower age = winner)
                    .then_with(|| self.age.cmp(&other.age))
            }
        }
    }
}

/// MinHeap wraps a BinaryHeap to make it a min-heap
/// rather than a max-heap.
struct MinHeap<T: Ord> {
    heap: BinaryHeap<Reverse<T>>,
}

impl<T: Ord> MinHeap<T> {
    fn new() -> Self {
        MinHeap {
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, item: T) {
        self.heap.push(Reverse(item));
    }

    fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|Reverse(item)| item)
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl<'a> MergeIterator<'a> {
    pub fn new() -> Self {
        Self {
            stopped: false,
            heap: MinHeap::new(),
            next_iterator_age: 0,
            last_emitted_key: None,
        }
    }

    pub fn add_iterator<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Result<KVRecord, Error>> + 'a,
    {
        let mut b: TableIterator<'a> = Box::new(iter);

        // Call next to put the first item into the iterator, so
        // we don't have to special case this in next() for MergeIterator.
        // Only store this iterator if it's not alrady exhausted.
        if let Some(x) = b.next() {
            self.heap.push(QueueItem {
                kvrecord: x,
                iterator: b,
                age: self.next_iterator_age,
            });
            self.next_iterator_age += 1;
        }
    }
}
impl<'a> Iterator for MergeIterator<'a> {
    type Item = Result<KVRecord, ToyKVError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }

        loop {
            // Heap has become empty as all iterators are
            // exhausted.
            if self.heap.is_empty() {
                self.stopped = true;
                return None;
            }

            let mut heap_item = self.heap.pop().unwrap();

            // Always return errors immediately and stop iteration
            let kvrecord = match heap_item.kvrecord {
                Err(x) => {
                    self.stopped = true;
                    return Some(Err(x.into()));
                }
                Ok(kvr) => kvr,
            };

            // Advance the iterator, and, if it's not exhausted,
            // re-add it to the heap.
            if let Some(x) = heap_item.iterator.next() {
                heap_item.kvrecord = x;
                self.heap.push(heap_item);
            }

            match &self.last_emitted_key {
                // skip duplicate -- older version of same key
                Some(x) if *x == kvrecord.key => continue,
                _ => {
                    // New key, update last_key and return
                    self.last_emitted_key = Some(kvrecord.key.clone());
                    return Some(Ok(kvrecord));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests_merge_iterator {
    use std::io::Error;

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
        let mut mt = MergeIterator::new();
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
