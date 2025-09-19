use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

use siphasher::sip::SipHasher13;

use crate::table::iterator::TableReader;
use crate::table::tableindex::{SSTableIndex, SSTableIndexLevels};
use crate::{
    error::ToyKVError,
    merge_iterator2::MergeIterator,
    table::{iterator::TableIterator, SSTableWriter, SSTables},
};

#[derive(Debug)]
pub(crate) struct CompactionPlan {
    pub(crate) l0: Vec<PathBuf>,
    pub(crate) l1: Vec<PathBuf>,
}

/// Utility Trait to gather the paths for TableIterators
pub(crate) trait CompactionPolicy {
    /// Build a compaction task, if the policy thinks a compaction
    /// is required.
    fn build_task(
        &self,
        sst_dir: PathBuf,
        sst_bloom_hasher: SipHasher13,
        sst_idx: &SSTableIndex,
    ) -> Result<Option<CompactionTask>, ToyKVError>;

    fn create_updated_index(
        &self,
        existing_index: &SSTableIndexLevels,
        c_result: CompactionTaskResult,
    ) -> Result<SSTableIndexLevels, ToyKVError>;
}

/// SimpleCompactionPolicy implements a compaction
/// policy that compacts all available tables.
pub(crate) struct SimpleCompactionPolicy {}

impl SimpleCompactionPolicy {
    pub(crate) fn new() -> Self {
        SimpleCompactionPolicy {}
    }

    /// For simple policy, we only need to compact if we have
    /// several l0 tables.
    fn needs_compaction(&self, sstables: &SSTableIndex) -> bool {
        println!(
            "needs_compaction called with {:?} l0 and {} l1",
            sstables.levels.l0.len(),
            sstables.levels.l1.len(),
        );

        // TODO make the minimum number configurable
        let min_l0_len = 0;

        let l0_len = sstables.levels.l0.len();
        if l0_len > min_l0_len {
            println!("Compaction needed as l0 contains {} tables", l0_len);
            true
        } else {
            println!("Compaction not needed as l0 contains {} tables", l0_len);
            false
        }
    }

    /// Determine a set of tables to compact.
    /// The simple policy returns all tables in l0 and l1 for
    /// a complete recompaction.
    fn plan(&self, sstables: &SSTableIndex) -> CompactionPlan {
        CompactionPlan {
            l0: sstables.levels.l0.clone(),
            l1: sstables.levels.l1.clone(),
        }
    }

    fn iters(
        &self,
        l0: &Vec<PathBuf>,
        l1: &Vec<PathBuf>,
    ) -> Result<Vec<TableIterator>, ToyKVError> {
        let mut vec = vec![];
        for p in l0.iter().chain(l1) {
            let tr = Arc::new(TableReader::new(p.clone())?);
            let t = TableIterator::new_with_tablereader(tr, Bound::Unbounded)?;
            vec.push(t);
        }
        Ok(vec)
    }

    /// Return whether candidate_tail is the tail of v.
    fn is_tail<T: PartialEq>(&self, v: &[T], candidate_tail: &[T]) -> bool {
        if candidate_tail.len() > v.len() {
            return false;
        }
        let potential_tail = &v[v.len() - candidate_tail.len()..];
        potential_tail == candidate_tail
    }
}

impl CompactionPolicy for SimpleCompactionPolicy {
    /// Return a compaction task that executes the simple
    /// compaction policy --- compact all existing L0 and
    /// L1 tables into a single sorted run at L1.
    fn build_task(
        &self,
        sst_dir: PathBuf,
        sst_bloom_hasher: SipHasher13,
        sst_idx: &SSTableIndex,
    ) -> Result<Option<CompactionTask>, ToyKVError> {
        if !self.needs_compaction(sst_idx) {
            return Ok(None);
        }
        let input_plan = self.plan(sst_idx);
        let input_iters = self.iters(&input_plan.l0, &input_plan.l1)?;
        Ok(Some(CompactionTask {
            d: sst_dir,
            bloom_hasher: sst_bloom_hasher,
            input_iters,
            input_plan,
        }))
    }

    /// Create an updated index by merging the existing index
    /// with compaction results. Must happen with a write lock
    /// held on the SSTableIndex.
    ///
    /// This function safely commits compaction changes by:
    /// 1. Validating that the compacted files haven't changed during compaction
    /// 2. Removing compacted files from the index
    /// 3. Adding newly created compacted files to the appropriate levels
    fn create_updated_index(
        &self,
        existing_index: &SSTableIndexLevels,
        c_result: CompactionTaskResult,
    ) -> Result<SSTableIndexLevels, ToyKVError> {
        let c_input_plan = c_result.input_plan;
        let c_output = c_result.output_plan;

        // Validate L0 consistency: The files we compacted should still
        // be at the tail of L0. New memtables may have been flushed to
        // the head during compaction, but the tail (older files) should
        // remain unchanged.
        if !self.is_tail(&existing_index.l0, &c_input_plan.l0) {
            return Err(ToyKVError::CompactionCommitFailure(
                "L0 files targeted for compaction are no longer at the tail - concurrent modification detected".to_string(),
            ));
        }
        // Validate L1 consistency: L1 should be exactly the same as
        // when compaction started.
        // Unlike L0, new files would only be written to L1 if there
        // had been a concurrent compaction, which we don't support.
        if !(existing_index.l1 == c_input_plan.l1) {
            return Err(ToyKVError::CompactionCommitFailure(
                "L1 files have changed during compaction - concurrent modification detected".to_string(),
            ));
        }

        // Build the updated index:
        // - L0: Keep files that weren't compacted (the "head" -
        //       newer memtables written out during compaction)
        //       and remove the files we compacted into L1.
        // - L1: Replace entirely with the compaction output (since
        //       we validated no changes occurred)
        let updated_index = SSTableIndexLevels {
            l0: existing_index
                .l0
                .clone()
                .into_iter()
                .filter(|p| !c_input_plan.l0.contains(p)) // remove compacted
                .collect::<Vec<PathBuf>>(),
            l1: c_output.l1.clone(),
        };

        Ok(updated_index)
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
                l0: vec![],
                l1: vec![write_result.fname],
            },
        })
    }
}
