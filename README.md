# toykv

An LSM key-value store created so I could learn the basics of writing an LSM (and rust).

It's purely a learning exercise and not suited to being used in any real world
application.

Saying that, it's on its way to being not-awful:

- It supports single K/V read and write, and scanning ranges.
- It's got both memtables (with WAL) and sstables.
- It's got a decent layering of the reading and writing code.
- It uses bloom filters to avoid reading sstables it doesn't need to.
- It's thread-safe.
- It's relatively efficient with file access.

On the other hand, there are a few key missing features:

- I still need to write compaction.
- I'd like to add a checksum --- xxhash probably --- to the sstable blocks and WAL records for data safety.

And some optimisations:

- It'd be good to compress sstable blocks, or use key prefix compression.

Overall, with all those things, it would be a good demonstration LSM store; as for production use-cases, I don't (yet) have the need to take it to that level of robustness.
