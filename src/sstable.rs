/// Provides types and functions to manage operations on a
/// set of SSTables on-disk. In toykv, we have one "branch"
/// of SSTables, from newest to oldest.
///
/// As is usual for LSM Tree databases, the write functions
/// are used to flush an in-memory memtable to an on-disk
/// file. These files maintain a chain in a simple way,
/// by naming them using lexographically sortable dates
/// as file names. Thus our single branch is created by
/// just ordering files by their name.
///
/// When reading, therefore, we search for the given key
/// from the newest file to the oldest file, iterating
/// each file until we come to a key that is the key
/// we are after, in which case we return its value,
/// or is greater than the key we are after, in which
/// case we move to the next file.
///
/// As this is toykv and not productionkv, we use a very
/// simple file format of KVRecords streamed to disk.

/// Provides methods to write KVRecords to an on-disk file.
struct SSTableWriter {}

/// Iterate entries in an on-disk SSTable.
struct SSTableReader {}
