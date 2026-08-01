# LiteKV

LiteKV is a small key-value store written in Go. Records are appended to a log, an index maps each key
to its newest record, and the whole store is one byte slice you can hold in memory, save yourself, or
have written to disk as it changes. For a store bigger than memory, `DB` splits it across several logs,
keeps only the keys and the newest log in memory, and merges the rest in the background.

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

`Write` tells you whether your record is stored, and nothing else. Freezing a full log and merging are
housekeeping that happens around it, and a failure at either does not mean the record was lost — so
those are reported by `Sync` and `Close` instead, which are the calls that answer "is this store
healthy".

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

## Segments

`Compact` on a single log stops the world: it takes the write lock, copies every live record, and while
it runs there are two copies of the data. The wait grows with the store, and it lands on whichever write
happens to arrive during it.

`DB` splits the store across several logs instead. One is active and takes the writes; when it reaches
`SegmentSize` it is frozen and a new one started, and a frozen log is never written again. The frozen
ones are merged in the background, and because the merge builds a new file rather than editing the old
ones, reads and writes carry on against the old ones the whole time — only the swap at the end needs the
store to itself, and that is a slice being rebuilt.

```go
db, err := litekv.OpenDB("data", litekv.DBOptions{
    Sync:         litekv.SyncEvery,
    Interval:     time.Second,
    SegmentSize:  4 << 20, // freeze a log at 4 MiB
    MergeTrigger: 2,       // merge once two logs of a size have collected
})
defer db.Close()

err = db.Write([]byte("foo"), []byte("bar"))
value, err := db.Read([]byte("foo"))
err = db.Merge()  // or compact the whole store now, rather than waiting
```

Each log keeps its own index, so a lookup asks the active one first and then the frozen ones from newest
to oldest, stopping at the first answer. That is what makes a record in a newer log shadow an older one,
and a tombstone in a newer log shadow a value in an older one. Merging keeps the number of logs small, so
a lookup does not have many to ask.

### Only the keys have to fit in memory

A frozen log holds nothing but its index. Its records stay on the disk and are read back when a key asks
for them, which is what lets a `DB` hold more than fits in memory — the arrangement Bitcask is built on,
and why its own description says only that all the *keys* must fit. What a `DB` keeps in memory is every
key, plus the one log still being written:

| 16,000 records holding 15 MiB | in memory |
| ----------------------------- | --------- |
| one log                       | 20 MiB    |
| segments                      | 2 MiB     |

The cost is on the way back. A value in a frozen log is a read from the file rather than a look at
memory, even when the operating system still has the page:

| read of a 512-byte value | |
| ------------------------ | ------- |
| from memory              | 156 ns  |
| from a frozen log        | 847 ns  |

So a `DB` trades roughly five times the read latency for roughly a tenth of the memory. A
`KeyValueStore` makes the opposite trade and keeps everything in memory, which is the right one while
the store still fits.

The worst wait a single write suffers, writing 128-byte values with half the writes landing on keys
already stored:

| records written | one log, compacted | segments, merged in the background |
| --------------- | ------------------ | ---------------------------------- |
| 10,000          | 21.2 ms            | 4.2 ms                             |
| 40,000          | 18.6 ms            | 4.3 ms                             |

The single log's wait is the compaction itself and grows with what is stored. The segmented one does not
grow, and what is left is not lock contention at all: it is the one sync that makes the merge crash safe,
which on macOS is a full device barrier and stalls other writes at the operating system. That is one
barrier per merge, whatever the store holds.

### Merging by size

Merging every frozen log into one each time means rewriting the whole store, and the cost of that grows
with the store. Instead only logs of roughly the same size are merged together — within a factor of four
— so a large log is rewritten only when enough of its own size has collected beside it. The store settles
at about `MergeTrigger` logs per size class, a handful in total.

Writing 1 KiB records into 1 MiB logs, counting every byte the merges wrote:

| written | merging everything | merging by size |
| ------- | ------------------ | --------------- |
| 31 MiB  | 2.3x over 6 merges | 2.9x over 18 merges |
| 127 MiB | 3.7x over 11 merges | 3.1x over 64 merges |

Merging by size is slightly worse for a small store and pulls ahead as it grows, because its cost is
bounded by the number of size classes — about log₄ of the store — while merging everything keeps
climbing with the store itself. The merges are also many and small rather than few and enormous, which
is what keeps one of them from sitting in front of a write.

`MergeTrigger` defaults to 2, which keeps the number of logs as low as it goes at the price of merging
more often. That is the right way round for fast storage, where the rewriting costs little and the logs
each cost a lookup and an index. Raise it to merge less and hold more logs. Setting it to 1 turns
merging off altogether, which is what an append-only workload of write-once keys wants: nothing is ever
superseded there, so a merge would reclaim nothing.

Two rules make a partial merge safe. Only a contiguous run of logs is merged, since the order they are
asked in is the only thing deciding which version of a key wins. And a tombstone is dropped only by a
merge that reaches the oldest log — anything older left out of the run could still hold the value the
tombstone hides, and dropping it would bring a deleted key back. `Merge` reaches every log by
definition, so it drops them all.

### Hint files

Learning where the keys are means reading every record, which is work proportional to what is stored
rather than to how many keys there are. So a frozen log gets its index written down beside it, as
`0000000001.hint` next to `0000000001.seg`, and opening reads that instead:

| 64,000 keys in 63 MiB | opening |
| --------------------- | ------- |
| with hints            | 4 ms    |
| without               | 82 ms   |

The hints came to 1.3 MiB, about 2% of the log, since a hint holds a key and an offset rather than a
key and a value. On an SSD that is 20x; on a Raspberry Pi's SD card, where the difference is bytes
actually read rather than bytes already cached, it is the difference between a store that opens and one
you wait for.

A hint is only ever a shortcut. It is rejected if it is damaged, truncated, not a hint at all, or
describes a log of a different length than the one beside it — and any of those simply means reading the
log the long way, then writing a fresh hint for next time. A store from before hints existed picks them
up the first time it is opened, and a hint whose log has gone is removed.

The one thing a hint changes is when damage is noticed. A log covered by one is not checked against its
checksums at startup, so a record that has rotted since it was written is found by the read that wants it,
or by `Verify`, rather than by opening the store.

### What a crash leaves behind

The merged log is written beside the others, renamed over the *oldest* of them, and only then are the
rest removed, oldest first. That order is what makes an interrupted merge harmless. At every point the
logs still on disk are the merged one plus the newest few of the ones it replaced, and since those are
newer they are asked first: a key they hold answers from them, and a key they do not falls through to the
merged log, which holds the newest version of everything older. A tombstone is only dropped once every
log that could still hold the value it hides has gone. There is a test that stops the removals at every
point and checks each state reads the same as the finished merge.

A half-built merge left behind by a crash is discarded on the next open, since every log it was merging
is still there.

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

Each record is a 22-byte header followed by the key and the value:

| Offset | Size | Field                                        |
| ------ | ---- | -------------------------------------------- |
| 0      | 4    | CRC-32 (IEEE), little-endian                 |
| 4      | 1    | Record version                               |
| 5      | 1    | Record type: 0 = normal, 1 = deleted         |
| 6      | 8    | Timestamp, nanoseconds since the Unix epoch  |
| 14     | 4    | Key length, little-endian uint32             |
| 18     | 4    | Value length, little-endian uint32           |
| 22     | *n*  | Key                                          |
| 22+*n* | *m*  | Value                                        |

The checksum covers everything after itself, so it does not depend on the layout of what follows. Keys
and values are limited to 4 GiB by the uint32 length fields, and `Write` returns `ErrorRecordTooLarge`
for anything larger. Every decode validates the declared lengths against the bytes actually present, so
a truncated or damaged store produces an error rather than a panic or an outsized allocation.

`Modified` reports when the newest record for a key was written:

```go
written, err := kvs.Modified([]byte("foo"))
```

### Records from before the version byte

The first version of this format had a 13-byte header and no version or timestamp, with the record type
where the version now sits. A type is only ever 0 or 1, so a version of 2 or more cannot be mistaken for
one, and the two layouts sit in the same log without ambiguity.

That means a store written before any of this still opens and still reads. Its records report version 0
and the zero time from `Written`, since they have no timestamp to report and inventing one would be
worse than admitting it. New records are written in the current layout beside them, and compaction
copies both across untouched.

What the version byte buys from here is that the next change to the format does not have to orphan
anything either.

## Limitations

- **A timestamp costs about 60 ns a write.** Reading the clock is 28 ns of it and the wider header the
  rest, which took an in-memory write from roughly 50 ns to 110. It is invisible against a file, where
  the write itself is microseconds, and it is the price of every record knowing when it was written.
- **Opening still costs the keys.** Hints make it proportional to the number of keys rather than to the
  bytes stored, but every key is still read and indexed before the store answers anything.
- **Every key must fit in memory.** The index holds each key and an offset, about 59 bytes per key,
  however large the values are. Ten million keys is roughly 600 MB of index, and no amount of paging
  values out of memory changes that.
- **A `KeyValueStore` holds every record in memory**, live or superseded, until compaction. A `DB` holds
  only the keys and its active log, which is what to reach for once a store outgrows memory.
- **No range or prefix queries.** The index is a hash map, so keys have no order. A radix tree that gave
  ordered traversal was measured and reverted: prefix queries went from a full scan to 214 ns, but point
  lookups cost 3 to 4.5x more, which was the wrong trade here. It is in the history at `9e3cf2c`.
- **One writer at a time.** Writes take every shard of the lock, so they serialize with each other, and
  for a single `KeyValueStore` with compaction as well.
- **Merging still rewrites.** Merging by size bounds the cost at about log₄ of the store rather than
  letting it grow with it, but every record is still rewritten a few times over its life. An append-only
  workload should turn merging off rather than pay for it.
- **A closed `DB` cannot read.** Its values are on the disk and closing shuts the files. A closed
  `KeyValueStore` goes on answering, because its records are in memory.

## Running Tests

```bash
go test -race ./...
```

Durability is checked by watching the disk rather than by trusting the code. The package touches the
filesystem through one seam — open, remove, rename, list, read, mkdir — which a test replaces with one
that records every operation in order and can be told to make any of them fail. That is how the sync policies are held to what
they promise, how `Sync` is shown to reach the frozen logs and not only the active one, and how a merge
is shown to sync its new log before renaming it into place. Orderings like those are most of what makes
a crash survivable, and none of them show up in the result of an operation.

A read that cannot be served is not treated as a log that ends. Bytes that are not a record, or a log
stopping in the middle of one, are a torn tail, and the answer to a torn tail is to cut the log back to
where it went wrong. A disk that will not give the bytes up is a different thing entirely, and answering
it the same way would mean deleting what could not be read. Opening a store off a disk that is failing
returns the failure and leaves every byte where it was.

The same seam covers what happens when the disk refuses. A merge that cannot rename its result leaves
every log it was going to replace still there and still answering. A merge that cannot remove the logs
it replaced lands in exactly the state a crash between those removals leaves, and both the running store
and one opened afterwards from what is on disk answer the same. A hint that cannot be written, renamed
or read costs nothing but the time it would have saved.

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
