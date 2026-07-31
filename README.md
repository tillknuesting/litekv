# LiteKV
LiteKV is a simple, lightweight, and efficient in-memory key-value store written in Go.
It supports basic operations like reading, writing, deleting, and updating key-value pairs,
as well as prefix queries over an ordered index, exporting and importing that index, rebuilding it,
and compacting the store. LiteKV uses an append-only data structure,
which provides better write performance and data durability.

## Technical Aspects
### Append-only Storage
LiteKV stores data in an append-only manner, which means that new key-value pairs are always added
to the end of the store, even when updating and deleting existing keys. This approach provides better write performance
and ensures that data remains intact even in the case of a crash or failure. An update appends a new
record and repoints the index at it, so the previous version stays on disk until the next compaction;
a delete appends a tombstone record that shadows every earlier record for that key.
### Compaction
As the store grows, it may accumulate duplicate or deleted entries, which can affect performance and
memory usage. LiteKV provides a compaction feature that keeps only the newest record for each live key
and drops deleted ones, making the store more efficient. Surviving records keep their relative order,
so compaction is deterministic and idempotent.

To compact the store, simply call the Compact method:
```go
if err := kvs.Compact(); err != nil {
    // the data could not be decoded; the store is left unchanged
}
```
### Binary Storage Format

LiteKV uses a custom binary format to store key-value pairs.
Each record is a 13-byte header followed by the key and the value:

| Offset | Size | Field       |
| ------ | ---- | ----------- |
| 0      | 4    | CRC-32 (IEEE), little-endian |
| 4      | 1    | Record type: 0 = normal, 1 = deleted |
| 5      | 4    | Key length, little-endian uint32 |
| 9      | 4    | Value length, little-endian uint32 |
| 13     | *n*  | Key |
| 13+*n* | *m*  | Value |

The checksum covers the type, both lengths, the key and the value, and helps in detecting corruption
or inconsistencies in the data. Keys and values are limited to 4 GiB by the uint32 length fields;
`Write` returns `ErrorRecordTooLarge` for anything larger. Every decode validates the declared lengths
against the bytes actually present, so a truncated or damaged store produces an error rather than a
panic or an outsized allocation.

### Index

The index is a radix tree mapping each key to the offset of its newest record. It holds no key bytes of
its own: every key is already in the data, so a node records where its slice of the key lives rather
than copying it, which is why indexing a key allocates no key bytes and why the tree survives the
reallocation an append can cause. The consequence is that a tree only means anything alongside the exact
data its offsets came from, so replacing `Data` means calling `RebuildIndex`.

The tree is there for `PrefixScan`. It is not a faster hash table, and replacing the map with it cost
speed on the operations a map is good at:

| operation                   | map      | radix tree | |
| --------------------------- | -------- | ---------- | --- |
| prefix query, 10 of 100k keys | 984 µs | 179 ns     | **5500x faster** |
| index lookup, 100k keys     | 10.0 ns  | 48.6 ns    | 4.9x slower |
| index lookup, random keys   | 10.2 ns  | 109.6 ns   | 10.7x slower |
| `Read`, 16-byte value       | 37.4 ns  | 62.6 ns    | 1.7x slower |
| `Write`, existing key       | 58.1 ns  | 50.8 ns    | 1.14x faster |
| `Write`, new key            | 69.1 ns  | 86.3 ns    | 1.25x slower |

A hash table computes one hash and probes one bucket, whichever key it is given. A radix tree walks down
one node per branching point in the key, and each step is a dependent load the processor cannot start
until the previous one lands, so the cost is cache misses that no amount of tuning removes. That is the
trade: point lookups get slower, and prefix queries stop costing a full scan of the store. Keys that
diverge early, like random ones, make the tree deepest and the gap widest.

Memory per key, 100k keys, counting the key copies a map has to make and the tree does not:

| keys                             | map     | radix tree |
| -------------------------------- | ------- | ---------- |
| `user:00000001:profile` (21 B)   | 59 B    | 82 B       |
| `/var/log/service-3/17.log` (24 B) | 67 B  | 93 B       |
| 90-byte paths sharing 80 bytes   | 131 B   | 82 B       |
| 20 random bytes                  | 59 B    | 89 B       |

The tree only comes out ahead once keys are long enough that the prefixes it shares outweigh the 48 bytes
a node costs. For short keys it is the larger of the two.

LiteKV uses byte slices as the underlying data format for storing key-value pairs. Byte slices offer
versatility and flexibility, making it easier to perform various operations such as saving data to disk or using POSIX shared memory.

By using byte slices, LiteKV allows you to seamlessly integrate the stored data with various storage
solutions or inter-process communication methods, enhancing the overall usability and adaptability of the
library in different use cases.

### Concurrency

`KeyValueStore` embeds a reader-writer lock and every method takes it, so the methods are safe to call
from multiple goroutines. The `Data` and `Index` fields are exported so that the store can be backed by
a file or by shared memory; code that touches them directly must hold the lock itself (`RLock` to read,
`Lock` to write), and must call `RebuildIndex` after replacing `Data`.

The lock is sharded on the read side. A plain `sync.RWMutex` serializes its own readers — every `RLock`
writes to the same counter, so that cache line has to be handed from core to core, and ten concurrent
readers end up slower than one (`RLock`/`RUnlock` alone costs 3 ns uncontended and 78 ns at ten-way
contention). Instead the store keeps one mutex per shard, each padded onto its own cache line, and a
read locks only the shard its key hashes to; a write takes all of them. Readers of different keys
therefore stop fighting over one line:

| shards | 1 KiB `View`, 10 goroutines | 16-byte `Write` |
| ------ | --------------------------- | --------------- |
| 1      | 95.8 ns                     | 48.3 ns         |
| 2      | 60.9 ns                     | 52.2 ns         |
| 4      | 43.7 ns                     | 59.5 ns         |
| 8      | 32.8 ns                     | 75.9 ns         |

Both columns are linear in the shard count, so this is a trade rather than a free win. The store uses
the largest power of two that is no larger than both `GOMAXPROCS` and the `maxShards` constant, which
is 4 — most of the read scaling for a third of the write cost. On a single core machine that comes out
as one shard, which behaves exactly like the plain `sync.RWMutex` it replaced. A read-heavy workload
can raise `maxShards`, and a single-reader one can set it to 1.

A hot key is still a hot shard: keys are spread by hash, so many readers of the *same* key contend
exactly as before.

### Recovering a damaged store

`Verify` checks every record against its checksum. `RebuildIndex` reconstructs the index from `Data`;
if it meets a record it cannot decode, which is what a crash part way through an append looks like,
it still installs the index for everything before that point and returns a `*CorruptAtError` carrying
the offset:

```go
var corrupt *litekv.CorruptAtError
if err := kvs.RebuildIndex(); errors.As(err, &corrupt) {
    kvs.Data = kvs.Data[:corrupt.Offset] // drop the damaged tail
    err = kvs.RebuildIndex()
}
```

## Getting Started

To use LiteKV, first import the library:
```go
import (
    "github.com/tillknuesting/litekv"
)
```
Then, create a new instance of KeyValueStore:
```go
kvs := &litekv.KeyValueStore{}
```
You can now perform basic operations on the store:
```go
err := kvs.Write([]byte("foo"), []byte("bar"))
value, err := kvs.Read([]byte("foo"))
err = kvs.Delete([]byte("foo"))
```
`Read` returns a copy of the stored value, so the caller may keep or modify it freely. It reports
`ErrorKeyNotFound` for an unknown key, `ErrorKeyDeleted` for a deleted one, and `ErrorChecksumMismatch`
or `ErrorKeyMismatch` when the record does not match what the index claims.

`View` hands the callback the stored bytes instead of a copy, which for a 1 KiB value is about twice
as fast as `Read` and allocates nothing. The value is only valid until the callback returns, and the
store is locked for reading while it runs, so the callback must not modify the value or call back into
the store:
```go
err := kvs.View([]byte("foo"), func(value []byte) error {
    _, err := w.Write(value)
    return err
})
```

To find every key under a prefix, in ascending byte order:
```go
err := kvs.PrefixScan([]byte("user:"), func(key, value []byte) bool {
    fmt.Printf("%s = %s\n", key, value)
    return true // return false to stop early
})
```
Superseded and deleted records are skipped, so `PrefixScan` shows what `Read` would return. Its cost is
proportional to the number of keys that match, not to the number the store holds. The key and value
alias the store's data and are only valid until the callback returns.

To walk the store without printing it:
```go
err := kvs.ForEach(func(key, value []byte, deleted bool) bool {
    fmt.Printf("%s = %s (deleted: %t)\n", key, value, deleted)
    return true // return false to stop early
})
```
The key and value handed to the callback alias the store's `Data` slice and are only valid until the
callback returns.

## Running Tests

To run the tests for LiteKV, navigate to the project directory and execute:
```go
go test -race ./...
```
## Running Benchmarks
To run the benchmarks for LiteKV, navigate to the project directory and execute:
```go
go test -bench=. ./...
```

## Fuzz Testing
Fuzz testing is a powerful technique to uncover potential issues in your code by providing
a wide range of random inputs.

To run fuzz tests for LiteKV, navigate to the project directory and execute:
```go
go test -fuzz FuzzKeyValueStore_Data
```
`FuzzKeyValueStore_Data` feeds arbitrary bytes in through the `Data` slice, which is how a store backed
by a file or by shared memory is restored, and `FuzzKeyValueStore_WriteReadDelete` fuzzes the write path.
