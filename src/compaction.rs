use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

use siphasher::sip::SipHasher13;

use crate::table::iterator::TableReader;
use crate::table::tableindex::SSTableIndex;
use crate::{
    error::ToyKVError,
    merge_iterator2::MergeIterator,
    table::{
        iterator::TableIterator, SSTableWriter, SSTables, TableIteratorPaths,
    },
};

#[derive(Debug)]
pub(crate) struct CompactionPlan {
    pub(crate) l0: Vec<PathBuf>,
    pub(crate) l1: Vec<PathBuf>,
}

/// Utility Trait to gather the paths for TableIterators
pub(crate) trait CompactionPolicy {
    /// Retrieve the paths on disk for the TableIterators
    fn build_task(
        &self,
        sst_dir: PathBuf,
        sst_bloom_hasher: SipHasher13,
        sst_idx: &SSTableIndex,
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
        sstables: &SSTableIndex,
    ) -> Result<Vec<TableIterator>, ToyKVError> {
        let paths = sstables.levels.l0.clone();
        self.iters(paths, None, std::ops::Bound::Unbounded)
    }

    fn iters(
        &self,
        paths: Vec<PathBuf>,
        k: Option<&[u8]>,
        upper_bound: Bound<Vec<u8>>,
    ) -> Result<Vec<TableIterator>, ToyKVError> {
        let mut vec = vec![];
        for p in paths {
            let tr = Arc::new(TableReader::new(p)?);
            let t = match k {
                Some(k) => TableIterator::new_seeked_with_tablereader(
                    tr,
                    k,
                    upper_bound.clone(),
                )?,
                None => TableIterator::new_with_tablereader(
                    tr,
                    upper_bound.clone(),
                )?,
            };
            vec.push(t);
        }
        Ok(vec)
    }
}

impl CompactionPolicy for SimpleCompactionPolicy {
    fn build_task(
        &self,
        sst_dir: PathBuf,
        sst_bloom_hasher: SipHasher13,
        sst_idx: &SSTableIndex,
    ) -> Result<CompactionTask, ToyKVError> {
        let iters = self.iters_to_compact(sst_idx)?;
        let input_paths = iters.table_paths();
        Ok(CompactionTask {
            d: sst_dir,
            bloom_hasher: sst_bloom_hasher,
            input_iters: iters,
            input_plan: CompactionPlan {
                l0: input_paths,
                l1: vec![],
            },
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
    pub(crate) input_plan: CompactionPlan,
}

#[derive(Debug)]
pub(crate) struct CompactionTaskResult {
    pub(crate) input_plan: CompactionPlan,
    pub(crate) output_plan: CompactionPlan,
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
            input_plan: self.input_plan,
            output_plan: CompactionPlan {
                l1: vec![],
                l0: vec![write_result.fname],
            },
        })
    }
}
