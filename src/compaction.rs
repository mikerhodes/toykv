use std::path::PathBuf;

use siphasher::sip::SipHasher13;

use crate::{
    error::ToyKVError,
    merge_iterator2::MergeIterator,
    table::{
        iterator::TableIterator, SSTableWriter, SSTables, TableIteratorPaths,
    },
};
/// Utility Trait to gather the paths for TableIterators
pub(crate) trait CompactionPolicy {
    /// Retrieve the paths on disk for the TableIterators
    fn build_task(
        &self,
        sstables: &SSTables,
    ) -> Result<CompactionTask, ToyKVError>;
}

/// SimpleCompactionPolicy implements a compaction
/// policy that compacts all available tables.
pub(crate) struct SimpleCompactionPolicy {}

impl SimpleCompactionPolicy {
    pub(crate) fn new() -> Self {
        SimpleCompactionPolicy {}
    }
    fn iters_to_compact(
        &self,
        sstables: &SSTables,
    ) -> Result<Vec<TableIterator>, ToyKVError> {
        sstables
            .iters(None, std::ops::Bound::Unbounded)
            .map_err(|e| e.into())
    }
}
impl CompactionPolicy for SimpleCompactionPolicy {
    fn build_task(
        &self,
        sstables: &SSTables,
    ) -> Result<CompactionTask, ToyKVError> {
        let iters = self.iters_to_compact(sstables)?;
        let input_paths = iters.table_paths();
        Ok(CompactionTask {
            d: sstables.store_directory(),
            bloom_hasher: sstables.bloom_hasher(),
            input_iters: iters,
            input_paths,
        })
    }
}

/// CompactionTask is a self-contained compaction task. It
/// can be retrieved from the SSTables struct when a read lock
/// is held on the struct. On completion of compaction, returns
/// an CompactionTaskResult, which should be passed to
/// SSTables.try_commit_compaction_task_v2() in order to
/// commit the compaction to the store.
/// CompactionTask is single-use, and only one should be
/// run at a time.
pub(crate) struct CompactionTask {
    /// Store directory
    pub(crate) d: PathBuf,
    /// Bloom hasher for sstables
    pub(crate) bloom_hasher: SipHasher13,
    /// TableIterators to merge, ordered as usual, newest to oldest
    pub(crate) input_iters: Vec<TableIterator>,
    /// The files that were compacted in this Task
    pub(crate) input_paths: Vec<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct CompactionTaskResult {
    pub(crate) inputs: Vec<PathBuf>,
    // TODO make this a Vec for sorted runs
    pub(crate) output: PathBuf,
}

impl CompactionTask {
    /// Compact the tables in iters into a single sstable file.
    /// This is a step towards better compaction.
    /// For now, compact into one large file.
    pub(crate) fn compact_v2(self) -> Result<CompactionTaskResult, ToyKVError> {
        let mut mi = MergeIterator::new();
        for iter in self.input_iters {
            mi.add_iterator(iter)
        }
        let w = SSTableWriter {
            fname: SSTables::next_sstable_fname(self.d.as_path()),
            bloom_hasher: self.bloom_hasher,
        };

        // we don't know this for now --- might have to
        // get the user to configure the expected size
        // of a k/v. Used for bloomfilter size.
        let expected_n_keys = 1000;

        let write_result = w.write(expected_n_keys, mi);

        // convert the Result to the right type if it's Ok(..)
        write_result.map(|write_result| CompactionTaskResult {
            inputs: self.input_paths,
            output: write_result.fname,
        })
    }
}
