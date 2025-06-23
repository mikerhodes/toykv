# Benchmark Progress

## Write/read 25k docs w/1k sstable write WALSync::Off

### 1st Gen SSTables (957fd67d)

```
Replayed 0 records from WAL. nextseq=0.
Running open() took 0.345ms.
Running write() took 251ms.
Running read() 25000 times took 7399ms (0.295ms per read).
Replayed 0 records from WAL. nextseq=0.
Running read() 25000 times took 7580ms (0.303ms per read).
```

Writing sstables took about 5ms per table written out.

### 2nd Gen SSTables

```
Replayed 0 records from WAL. nextseq=0.
Running open() took 0.329ms.
Running write() took 225ms.
Running read() 25000 times took 4889ms (0.195ms per read).
Replayed 0 records from WAL. nextseq=0.
Running read() 25000 times took 4876ms (0.195ms per read).
```

Writing sstables took 3-5ms per table written out. Slightly faster.

Reading is faster.

10000 wal write threshold

```
Replayed 0 records from WAL. nextseq=0.
Running open() took 0.331ms.
Writing sstable took 17ms.
Writing sstable took 13ms.
Running write() took 134ms.
Running read() 25000 times took 1637ms (0.065ms per read).
Replayed 5000 records from WAL. nextseq=5000.
Running read() 25000 times took 1657ms (0.066ms per read).
```

Faster reads, longer sstable writes, but not 10x slower. Nice.
