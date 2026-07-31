# LiteKV

LiteKV is a small key-value store written in Go. Records are appended to a log, an index maps each key
to its newest record, and the whole store is one byte slice you can hold in memory, save yourself, or
have written to disk as it changes.

The design is Bitcask, described by Justin Sheehy and David Smith at Basho Technologies in their 2010
paper *Bitcask: A Log-Structured Hash Table for Fast Key/Value Data*, which credits Eric Brewer for the
idea, and shipped as the storage engine behind Riak. An append-only log holds the records and an
in-memory index holds an offset per key, so a write never seeks, a read is one lookup and one read, a
crash costs at most the record being written, and every key has to fit in memory.

## Getting Started

LiteKV needs Go 1.26 or newer, which is what `go.mod` asks for. Nothing in the library needs a recent
language feature, so if you have to build it with an older toolchain, lowering that one line is enough.

```go
import "github.com/tillknuesting/litekv"
```

The zero value is a working store that touches no disk:

```go
kvs := &litekv.KeyValueStore{}

err := kvs.Write([]byte("foo"), []byte("bar"))
value, err := kvs.Read([]byte("foo"))
err = kvs.Delete([]byte("foo"))
```

`Read` returns a copy of the stored value, so you may keep or modify it freely. It reports
`ErrorKeyNotFound` for a key that was never written, `ErrorKeyDeleted` for one that was deleted, and
`ErrorChecksumMismatch` or `ErrorKeyMismatch` when the record does not match what the index claims.

### Reading without copying

`View` hands the callback the stored bytes instead of a copy, which for a 1 KiB value is about twice as
fast as `Read` and allocates nothing. The bytes are only valid until the callback returns, and the store
is locked for reading while it runs, so the callback must not modify them or call back into the store:

```go
err := kvs.View([]byte("foo"), func(value []byte) error {
    _, err := w.Write(value)
    return err
})
```

### Walking the store

```go
err := kvs.ForEach(func(key, value []byte, deleted bool) bool {
    fmt.Printf("%s = %s (deleted: %t)\n", key, value, deleted)
    return true // return false to stop early
})
```

`ForEach` visits every record in the order it was written, superseded versions and tombstones included,
which is why it reports `deleted`. The key and value alias the store's `Data` slice and are only valid
until the callback returns. `PrintAllKeyValuePairs` is the same walk, printed.

## Durability

The `Data` slice is the whole store, and none of this is required: the zero value keeps everything in
memory and writes nothing anywhere. There are three ways to work with it.

**Keep it in memory and handle persistence yourself.** `Data` is an ordinary byte slice:

```go
os.WriteFile("store.kv", kvs.Data, 0o644) // whenever you like

raw, _ := os.ReadFile("store.kv")
restored := &litekv.KeyValueStore{Data: raw}
discarded, err := restored.Recover() // rebuilds the index, drops a damaged tail
```

**Let the store keep a file**, so every write lands on disk as it happens:

```go
kvs, err := litekv.Open("store.kv", litekv.Options{Sync: litekv.SyncEvery, Interval: time.Second})
if err != nil {
    return err
}
defer kvs.Close()
```

**Mirror writes to something of your own** — a network log, an encrypted file, shared memory — by
implementing three methods:

```go
type Log interface {
    WriteAt(p []byte, off int64) (int, error)
    Truncate(size int64) error
    Sync() error
}

err := kvs.Attach(myLog, litekv.Options{Sync: litekv.SyncNever})
```

An `*os.File` satisfies `Log` as it is. The store only ever appends, always calls `WriteAt` with the
offset of the end of the log, and holds its write lock while it does, so an implementation need not be
safe for concurrent use. `Rewrite` puts the store's current contents into a log that does not have them
yet, and `Detach` goes back to memory only.

### When a write is really written

`write()` returning is not durability — it means the operating system has your bytes, not that the disk
does. That is the choice `SyncPolicy` makes, and it is not a cheap one:

| policy       | per write | survives a process crash | survives losing power     |
| ------------ | --------- | ------------------------ | ------------------------- |
| `SyncAlways` | 3.8 ms    | yes                      | yes                       |
| `SyncEvery`  | 7.0 µs    | yes                      | all but the last interval |
| `SyncNever`  | 5.5 µs    | yes                      | no promises               |
| in memory    | 153 ns    | no                       | no                        |

`SyncAlways` is the default, because losing an acknowledged write should be something you ask for rather
than something that happens quietly. It is also 685x the cost of not syncing, and every reader waits for
it, since the sync happens under the write lock — there is no way to acknowledge a durable write without
waiting for the disk. Those numbers come from an SSD on macOS, where `Sync` is a full barrier; an SD card
in a Raspberry Pi is worse. `Sync` forces a flush at any time, whatever the policy.

Note what every policy has in common: a record is with the operating system by the time `Write` returns,
so **a process that dies loses nothing**, whether it panics, is killed, or exits without closing. Only
losing power loses records. There is a test for that — twenty writes, no sync, no close, all twenty read
back after reopening.

### Shutting down

`Close` syncs, releases the file, and is worth deferring:

```go
defer kvs.Close()
```

A deferred `Close` runs while a panic unwinds. It does *not* run on `os.Exit`, which `log.Fatal` calls,
nor on an unhandled signal, nor on `SIGKILL` — and as above, that costs no records. What `Close` cannot
help with either way is losing power, since a deferred function does not run when the power goes out.
That window is what `SyncEvery` bounds and `SyncAlways` removes.

For a graceful shutdown on a signal, handle it yourself — a library should not take a program's signals
out from under it:

```go
signals := make(chan os.Signal, 1)
signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-signals
    kvs.Close()
    os.Exit(0)
}()
```

One caveat under `SyncEvery`: the timer goroutine holds a reference to the store, so a store that is
abandoned instead of closed keeps that goroutine and its file descriptor for the life of the process.

## Compaction

An update appends a new record and repoints the index at it, and a delete appends a tombstone that
shadows every earlier record for that key, so the log grows with every write whether or not the number
of keys does. Compaction keeps the newest record for each live key and drops the rest:

```go
if err := kvs.Compact(); err != nil {
    // the data could not be decoded; the store is left unchanged
}
```

Surviving records keep their relative order, so compacting the same store twice produces byte-identical
data. For a store with a file, `Compact` rewrites the file too, through a temporary file and a rename,
so an interrupted compaction leaves either the whole old log or the whole new one.

It is stop-the-world: the store is locked for the duration, and peak memory is two copies of the live
records.

## Recovering a damaged store

Three methods, in order of how much they do:

- `Verify` checks every record against its checksum and reports the first that fails. It changes nothing.
- `RebuildIndex` rebuilds the index from `Data`, checking that records decode but not that they are
  intact. If it meets one it cannot decode it installs the index for everything before that point and
  returns a `*CorruptAtError` carrying the offset.
- `Recover` rebuilds the index, checks checksums as it goes, and discards everything from the first
  record that fails either test, truncating the log to match. It reports how many bytes it dropped.
  This is what `Open` does.

```go
raw, _ := os.ReadFile("store.kv")
kvs := &litekv.KeyValueStore{Data: raw}

discarded, err := kvs.Recover()
if discarded > 0 {
    log.Printf("dropped %d bytes of damaged tail", discarded)
}
```

A record that fails its checksum part way through an otherwise intact log ends the log there. Without a
marker to resynchronise on there is no way to know where the next record begins, so everything after it
is discarded — `Verify` first if you would rather look before anything is thrown away.

## Saving the index

Rebuilding the index means reading every record. For a large store you can save the index instead and
load it back, which skips that scan:

```go
saved, err := kvs.SaveIndex()   // gob-encoded map of key to offset
err = kvs.LoadIndex(saved)      // after Data is in place
```

`LoadIndex` replaces the index rather than merging into it, and checks every entry against `Data` first:
an entry that does not point at a record holding that exact key fails with `ErrorKeyMismatch` or
`ErrorCorruptData` and leaves the store untouched. Populate `Data` before calling it.

## Concurrency

`KeyValueStore` embeds a reader-writer lock and every method takes it, so the methods are safe to call
from multiple goroutines. `Data` and `Index` are exported so the store can be backed by a file or by
shared memory; code that touches them directly must hold the lock itself (`RLock` to read, `Lock` to
write), and must call `RebuildIndex` or `Recover` after replacing `Data`.

The lock is sharded on the read side. A plain `sync.RWMutex` serializes its own readers — every `RLock`
writes to the same counter, so that cache line has to be handed from core to core, and ten concurrent
readers end up slower than one (`RLock`/`RUnlock` alone costs 3 ns uncontended and 78 ns at ten-way
contention). Instead the store keeps one mutex per shard, each padded onto its own cache line, and a
read locks only the shard its key hashes to; a write takes all of them:

| shards | 1 KiB `View`, 10 goroutines | 16-byte `Write` |
| ------ | --------------------------- | --------------- |
| 1      | 95.8 ns                     | 48.3 ns         |
| 2      | 60.9 ns                     | 52.2 ns         |
| 4      | 43.7 ns                     | 59.5 ns         |
| 8      | 32.8 ns                     | 75.9 ns         |

Both columns are linear in the shard count, so this is a trade rather than a free win. The store uses the
largest power of two no larger than both `GOMAXPROCS` and the `maxShards` constant, which is 4 — most of
the read scaling for a third of the write cost. On a single core that comes out as one shard, which
behaves exactly like the plain `sync.RWMutex` it replaced. A read-heavy workload can raise `maxShards`,
and a single-reader one can set it to 1.

A hot key is still a hot shard: keys are spread by hash, so many readers of the *same* key contend
exactly as before.

## Binary Storage Format

Each record is a 13-byte header followed by the key and the value:

| Offset | Size | Field                                |
| ------ | ---- | ------------------------------------ |
| 0      | 4    | CRC-32 (IEEE), little-endian         |
| 4      | 1    | Record type: 0 = normal, 1 = deleted |
| 5      | 4    | Key length, little-endian uint32     |
| 9      | 4    | Value length, little-endian uint32   |
| 13     | *n*  | Key                                  |
| 13+*n* | *m*  | Value                                |

The checksum covers the type, both lengths, the key and the value. Keys and values are limited to 4 GiB
by the uint32 length fields, and `Write` returns `ErrorRecordTooLarge` for anything larger. Every decode
validates the declared lengths against the bytes actually present, so a truncated or damaged store
produces an error rather than a panic or an outsized allocation.

The format has no header and no version byte, which is worth knowing before you store anything you care
about: it cannot be changed without breaking every file already written, and nothing in a file says
which version wrote it.

## Limitations

- **Every key must fit in memory.** The index holds each key and an offset, about 59 bytes per key on
  top of the data itself. Ten million keys is roughly 600 MB of index whatever the values weigh.
- **The whole store is in memory too.** `Data` holds every record, live or superseded, until compaction.
- **No range or prefix queries.** The index is a hash map, so keys have no order. A radix tree that gave
  ordered traversal was measured and reverted: prefix queries went from a full scan to 214 ns, but point
  lookups cost 3 to 4.5x more, which was the wrong trade here. It is in the history at `9e3cf2c`.
- **One log, compacted whole.** There are no segments, so compaction stops the world and cannot run in
  the background.
- **One writer at a time.** Writes take every shard of the lock, so they serialize with each other and
  with compaction.

## Running Tests

```bash
go test -race ./...
```

The suite includes a model test that runs thousands of random operations against a plain map and checks
the store still agrees after every one, over all three backings — in memory, an attached log, and a file
— and another that reopens the file part way through, so records have to mean the same thing after a
restart. Coverage is around 93%; what is left is I/O failures that need a filesystem mock.

## Running Benchmarks

```bash
go test -bench=. ./...
```

Benchmarks cover reads and writes at 16 bytes, 1 KiB and 64 KiB, the parallel read paths, compaction,
index rebuilds, and the cost of each sync policy.

## Fuzz Testing

```bash
go test -run xxx -fuzz FuzzKeyValueStore_Data
```

`FuzzKeyValueStore_Data` feeds arbitrary bytes in through the `Data` slice, which is how a store backed
by a file or by shared memory is restored: no input may panic, hang, or make the store forget a key it
had already returned. `FuzzKeyValueStore_WriteReadDelete` fuzzes the write path.
