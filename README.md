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

The index is an adaptive radix tree mapping each key to the offset of its newest record. It holds no key
bytes of its own: every key is already in the data, so a node records where its slice of the key lives
rather than copying it, which is why indexing a key allocates no key bytes and why the tree survives the
reallocation an append can cause. The consequence is that a tree only means anything alongside the exact
data its offsets came from, so replacing `Data` means calling `RebuildIndex`.

Adaptive means each node holds its children in one of three shapes, sized to how many it has: up to 8
labels live in the node itself, up to 48 in a 256-byte slot table, and beyond that the children are
addressed by the label directly with no search at all. Uniform nodes are what make a plain radix tree
slow — searching a sorted list of a hundred children means jumping around a kilobyte of memory, once per
level, per lookup. Adapting the node shape was worth 2-4x on lookups, most of it on keys that branch
widely. Unlike the published tree, whose node prefix is a fixed handful of bytes, this one is an offset
into the data and has no length limit, because the keys are already there to point at.

The tree exists for `PrefixScan`. It is not a faster hash table:

| operation                       | Go map    | radix tree | |
| ------------------------------- | --------- | ---------- | --- |
| prefix query, 10 of 100k keys   | 1.01 ms   | 214 ns     | **4700x faster** |
| index lookup, 100k keys         | 16.3 ns   | 73.1 ns    | 4.5x slower |
| `Read`, 16-byte value           | 37.4 ns   | 45.2 ns    | 1.2x slower |
| `Write`, existing key           | 58.1 ns   | 49.8 ns    | **1.17x faster** |
| `Write`, new key                | 69.1 ns   | 62.3 ns    | **1.11x faster** |

Writes come out ahead because re-indexing a key allocates nothing, where the map copied the key every
time. Lookups do not, and no amount of tuning closes that: a hash table computes one hash and probes one
bucket, whatever the key, while a tree walks one node per branching point and each step is a load that
cannot start until the previous one lands.

That gap widens with the key count, rather than closing, because the chain of dependent loads is what
misses cache. Lookups of `user:%08d:profile` keys, probed in an order unrelated to insertion:

| keys | Go map  | radix tree |
| ---- | ------- | ---------- |
| 10k  | 12.0 ns | 27.9 ns    |
| 100k | 17.9 ns | 77.2 ns    |
| 1M   | 103 ns  | 341 ns     |
| 5M   | 114 ns  | 491 ns     |

Note the access pattern: walking keys in the order they were inserted reads the same path repeatedly and
reports two to four times better than this for both structures. These are the random-access numbers.

Memory per key, 100k keys, counting the key copies a map has to make and the tree does not:

| keys                               | Go map  | radix tree |
| ---------------------------------- | ------- | ---------- |
| `user:00000001:profile` (21 B)     | 59 B    | 114 B      |
| `/var/log/service-3/17.log` (24 B) | 67 B    | 120 B      |
| 90-byte paths sharing 80 bytes     | 131 B   | 114 B      |
| 20 random bytes                    | 59 B    | 96 B       |

The tree only comes out ahead once keys are long enough that the prefixes it shares outweigh the 64 bytes
a node costs. For short keys it is roughly twice the map.

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

LiteKV needs Go 1.26 or newer, which is what `go.mod` asks for. Nothing in the library needs a recent
language feature, so if you have to build it with an older toolchain, lowering that one line is enough.

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
