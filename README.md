# LiteKV

LiteKV is a small key-value store written in Go. Records are appended to a log, an index maps each key
to its newest record, and the whole store is one byte slice you can hold in memory, save yourself, or
have written to disk as it changes. For a store bigger than memory, `DB` splits it across several logs,
keeps only the keys and the newest log in memory, and merges the rest in the background. A
`KeyValueStore` can be followed by a replica, which is the same log sent somewhere else.

The design is Bitcask, described by Justin Sheehy and David Smith at Basho Technologies in their 2010
paper *Bitcask: A Log-Structured Hash Table for Fast Key/Value Data*, which credits Eric Brewer for the
idea, and shipped as the storage engine behind Riak. An append-only log holds the records and an
in-memory index holds an offset per key, so a write never seeks, a read is one lookup and one read, a
crash costs at most the record being written, and every key has to fit in memory.

It is also a server. `cmd/litekvd` puts an HTTP API in front of a `DB` and `server/` is the package
behind it — see "Serving it over HTTP". Nothing in the library itself opens a socket, and that is the
arrangement rather than an omission.

## Which of the two

`KeyValueStore` is one log. Every record is in memory and so is the index, reads are a map lookup and a
slice, and it can keep a file or not. Reach for it while the store fits in memory, which is most of the
time, and for anything that wants the records as a byte slice it can hand around.

`DB` is the same design split across several logs. Only the keys and the log currently being written are
in memory; the rest of the records stay on the disk and are read back a key at a time. Reach for it when
the store outgrows memory, or when compacting one log in one go has become a stall you notice.

|                          | `KeyValueStore`      | `DB`                                     |
| ------------------------ | -------------------- | ---------------------------------------- |
| Records in memory        | all of them          | the active log only                      |
| Read of a 512-byte value | 120 ns               | 589 ns from a frozen log                 |
| Concurrent reads         | scale with the cores | do not                                   |
| Compaction               | stops the world      | merges in the background                 |
| After `Close`            | still reads          | refuses: its values are behind shut files |
| Data as a byte slice     | yes, `Data`          | no, it is several files                  |
| Replication              | ship the log, byte for byte | ship the records; the files differ |

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

### Several records at once

`Batch` collects records and `WriteBatch` stores all of them or none of them:

```go
var b litekv.Batch
b.Write([]byte("to"), []byte("the value"))
b.Delete([]byte("from"))

err := kvs.WriteBatch(&b)   // or db.WriteBatch(&b)
```

A crash part way through leaves the store as it was. The records go down behind a **marker** — a record
holding no key, whose value is the number of bytes of records that follow it — and recovery discards
from that marker on unless all of it is there and every record in it matches its own checksum. There is
one write to the log for the whole batch, so a batch of ten records is one chance for the disk to stop
half way rather than ten, and the marker is what makes that one survivable.

Later records win, as they do in the log itself: writing a key twice leaves the second value, and
deleting a key written earlier in the same batch leaves it deleted. An empty batch writes nothing. The
keys and values are not copied when they are added, only when the batch is written, so anything handed
to a batch must stay unchanged until then — that is the copy the batch exists to save, and `Write`
still copies before it returns.

**A batch is not a transaction.** Nothing is read, nothing is isolated from a concurrent writer, and
there is nothing to roll back once it is written. It is one durable, atomic append of several records,
which is what "or none of them" means here and all it means.

A `DB` always writes a batch into one log: rotating is housekeeping that happens after the records are
stored, so a batch is never split across the log that filled and the one that replaced it. Over
replication it crosses whole — a leader cuts its stream at the end of a batch and never inside one,
taking a batch bigger than the wire's pieces in one go, exactly as it does a record bigger than them.
Merging drops the markers: by then the records are durable and the merged file is renamed into place
whole, so the atomicity the marker was carrying is being provided by something else.

### Ranges and prefixes

```go
err := db.Prefix([]byte("user:"), func(key, value []byte) bool {
    fmt.Printf("%s = %s\n", key, value)
    return true // false stops early
})

err = db.Range([]byte("a"), []byte("m"), fn)   // from is included, to is not
```

Both visit live keys **in order**: the newest version of each, skipping the records newer logs have
superseded, the keys tombstones have deleted and the records whose expiry has passed. A nil bound runs
to the end of the keys, and an empty prefix is every key. `Range` and `Prefix` are `ForEach` in key
order, over a range of them.

The index is a hash map, so the keys have no order to walk — and it stays a hash map, because an ordered
index instead of it was measured and reverted: a radix tree cost three to four and a half times on point
lookups, which is the wrong trade for a store whose whole shape is point lookups. So a range asks the
keys rather than keeping them in order, and the two halves of a `DB` answer differently:

- **A frozen log's index never changes again.** Its keys are sorted the first time anybody asks that log
  for a range, and kept — a cache that cannot go stale, which is the only reason it is allowed. A range
  is then a binary search and a walk.
- **The log being written changes constantly**, so there is nothing worth keeping. Its keys are filtered
  against the range and only the matches are sorted, which is cheap because the matches are usually few
  and because the log is bounded — that is what rotation is for.

Nothing is paid on the write path, and nothing is paid in memory by a store that never asks for a range.
Against a hundred thousand keys, a prefix matching a hundred of them:

| | |
| ----------------------------------- | -------- |
| `Prefix`                            | 130 µs   |
| the same by walking with `ForEach`  | 45.6 ms  |

That is 350 times, and 32 KB of allocation against 17 MB. What it costs is that every log has to be
asked before anything can be yielded in order, so the answer is gathered and then sorted rather than
streamed: a range over most of a large store holds most of its keys while it runs, and a range over a
few of them holds a few.

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
| `SyncEvery`  | 2.2 µs    | yes                      | all but the last interval |
| `SyncNever`  | 2.1 µs    | yes                      | no promises               |
| in memory    | 121 ns    | no                       | no                        |

`Write` tells you whether your record is stored, and nothing else. Freezing a full log and merging are
housekeeping that happens around it, and a failure at either does not mean the record was lost — so
those are reported by `Sync` and `Close` instead, which are the calls that answer "is this store
healthy".

`SyncAlways` is the default, because losing an acknowledged write should be something you ask for rather
than something that happens quietly. It is also more than a thousand times the cost of not syncing, and every reader waits for
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
    BloomMinKeys: 4096,    // filter a frozen log once it holds this many keys
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
| from the active log      | 120 ns  |
| from a frozen log        | 589 ns  |

So a `DB` trades roughly five times the read latency for roughly a tenth of the memory. The gap closes
as values grow, since the copy starts to matter more than the call: 49 ns against 502 for 16 bytes, and
665 against 1439 for 4 KiB — the last of those is a record just over the page a frozen read asks for, so
it is the one size here that still takes two trips to the disk. A
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

### What rotation costs

Freezing a log is work a write occasionally has to stop for: the store lets go of what it was holding,
opens the log to read from, and writes the index beside it. Averaged over the writes between one
rotation and the next, at 64 KiB logs and 128-byte values, that is 10.4 µs a write against 2.5 µs for a
store whose logs never fill. The median write is untouched at about 2 µs — the cost is a handful of slow
writes, not a tax on all of them.

`SegmentSize` is the knob. Larger logs rotate less and hold more in memory while they are the active one;
smaller ones rotate more and keep memory down. Nothing here is proportional to how much the store already
holds, which is the point of the arrangement.

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
each cost a lookup and an index. Raise it to merge less and hold more logs. Anything below 2 turns
merging off altogether, which is what an append-only workload of write-once keys wants: nothing is ever
superseded there, so a merge would reclaim nothing. `Merge` still works when it is off, so compaction
becomes something you ask for rather than something that happens.

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

Writing a hint does not wait for the disk. Every way an unsynced hint can come back wrong is a way of it
being ignored — bytes that never landed fail its checksum, a rename that never landed leaves no hint —
and both cost a scan of the log and nothing else. Syncing them was four fifths of the time spent writing
to a store whose logs rotate, for something whose loss costs only that scan.

A hint is only ever a shortcut. It is rejected if it is damaged, truncated, not a hint at all, or
describes a log of a different length than the one beside it — and any of those simply means reading the
log the long way, then writing a fresh hint for next time. A store from before hints existed picks them
up the first time it is opened, and a hint whose log has gone is removed.

The one thing a hint changes is when damage is noticed. A log covered by one is not checked against its
checksums at startup, so a record that has rotted since it was written is found by the read that wants it,
or by `Verify`, rather than by opening the store.

Merging keeps out of a write's way only if there is a core for it to run on. On a single core machine it
is the same work in the same place, and a write waits for whatever the scheduler decides — measured at 9
to 19 ms against the 4 ms it costs with cores to spare. Segments still bound what a merge rewrites and
still keep the memory down; what they stop buying is the quiet.

### Bloom filters

A lookup asks every log, newest first, and stops at the first answer. So the most expensive lookup a
`DB` has is one for a key it does not hold: every log, and no early exit. Once a frozen log holds enough
keys it gets a Bloom filter over them, which turns such a key away without the log consulting its index
at all.

The usual reason for a filter does not apply here, and it is worth saying why, because it changes where
the filter helps. A filter normally exists to keep a lookup off the disk; a frozen log here answers a
miss out of its index and never touches the disk to do it. What the filter saves is not I/O but cache.
The index of half a million keys is about 30 MB and a lookup in it is a walk out to memory; a filter over
the same keys is under a megabyte and stays in cache. That is the whole of the win, and it is why there
is a threshold: a small index is already in cache, and then a map lookup is a few nanoseconds that a hash
and six probes cannot beat.

A miss against logs of a given size, with a filter and without:

| keys a log | no filter | filter  |
| ---------- | --------- | ------- |
| 1,000      | 67.9 ns   | 73.3 ns |
| 4,000      | 74.7 ns   | 74.5 ns |
| 16,000     | 78.5 ns   | 77.4 ns |
| 64,000     | 112 ns    | 84.4 ns |
| 256,000    | 250 ns    | 86.6 ns |

The filter's cost barely moves across that range while the index's climbs, which is the argument in one
table. They cross at about four thousand keys, so that is what `BloomMinKeys` defaults to: below it a
filter measures worse, above it only better, and by a quarter of a million keys a miss is three times
cheaper. Set it lower to filter more logs, higher to filter fewer, or negative to build none.

It costs about 10 bits a key, some 3% on top of the index it sits in front of, and about one lookup in a
hundred is a false positive that consults the index anyway and finds nothing. A false negative would be
a different matter — a key reported missing while it sits in the log — so that is what the tests are
mostly about.

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

## Expiry

A record can be given the moment it stops counting:

```go
err := kvs.WriteExpiring([]byte("session"), []byte("token"), time.Now().Add(time.Hour))
```

After that moment the key reads as `ErrorKeyExpired`, which means what `ErrorKeyDeleted` means — there
is no value — and is told apart from it because one says the key was asked to go and the other says it
was told to go by itself. `View` and `Modified` answer the same way, `DB.ForEach` skips it, and writing
over the key brings it back, since the newest record is the one that counts.

**It is an instant, not a duration**, and it is stored as one. A duration would mean something different
on every machine that read the record, and a record that crosses to a follower has to stop counting at
the same moment on both ends. It does: a follower is never told anything about expiry, and reaches the
same answer from the record itself. A moment already past is allowed and writes a record that is expired
when it lands, which is a way of saying "gone, and here is when".

**An expired record is a tombstone until something is entitled to drop it.** It says there is no value,
and an older record for the same key may still be sitting in an older log — so dropping it early brings
that older value back. `Compact` on a single store may drop it outright, since one log has nothing older
anywhere. A `DB` merge may only drop it when the run reaches the oldest log, which is the same rule
tombstones have and for the same reason.

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

Rebuilding the index means visiting every record. You can save the index instead and load it back:

```go
saved, err := kvs.SaveIndex()   // gob-encoded map of key to offset
err = kvs.LoadIndex(saved)      // after Data is in place
```

`LoadIndex` replaces the index rather than merging into it, and checks every entry against `Data` first:
an entry that does not point at a record holding that exact key fails with `ErrorKeyMismatch` or
`ErrorCorruptData` and leaves the store untouched. Populate `Data` before calling it.

With `Data` already in memory this does not pay, and it is worth being plain about why. Rebuilding does
not read the records: it asks each header how long its record is and steps over the value without
touching it, so it costs the number of records rather than the number of bytes — it does not move at all
between 16-byte values and 64 KiB ones, a log from a quarter of a megabyte to 268 of them. Loading is
proportional to the keys as well, and does more per key: a gob decode, then a random-access check of
every entry against the log it claims to describe.

| 256-byte values | rebuild | load    | save    |
| --------------- | ------- | ------- | ------- |
| 4,096 keys      | 418 µs  | 468 µs  | 228 µs  |
| 16,384 keys     | 1.72 ms | 1.96 ms | 797 µs  |
| 65,536 keys     | 10.5 ms | 12.2 ms | 3.18 ms |

The case for saving an index is the store whose `Data` is not in memory yet. Rebuilding then faults in a
page for every record it steps to, which for large values is a page per key across the whole log, while
loading reads one compact file end to end. That is the argument the hint files make for a `DB`, and on
an SD card it is the one that decides. It is not measured here: it needs a cold page cache, and nothing
in the suite has one.

`Recover` is the same walk with every checksum verified, and costs about what `RebuildIndex` does — 400
µs against 418 for 4096 keys — so there is no speed to be had by rebuilding without the checking.

## Replication

The log is already an ordered, checksummed, append-only stream of records, so a follower that holds the
first *N* bytes of a leader's log and is given the bytes after them holds the same store, record for
record. That is all replication is here: shipping the log. It is the WAL-shipping arrangement described
in the leader-follower chapter of *Designing Data-Intensive Applications*, and the one PostgreSQL and
MySQL use — the follower names a position once and the leader streams from there.

Nothing in the library opens a socket. What crosses the gap is a `Position`, which marshals to
twenty-eight bytes, and a run of records; carrying those over TCP, HTTP, a pipe or a file is yours to do. A record
carries its own lengths, so a stream of them needs no framing on top.

```go
// the leader, on a connection
leader.Follow(from, conn, stop, litekv.ReplicaOptions{})

// the follower, on the other end of it
replica.Apply(from, conn, litekv.ReplicaOptions{})
```

`Follow` sends what the follower is missing and then goes on sending as records are written, until
`stop` is closed or the connection stops taking them. `Apply` reads records and appends them, applying
whatever arrived together in one write and one sync, and returns when the connection ends. A slow
follower does not slow the leader down: each batch is copied out under the read lock and written outside
it, which is what makes the replication asynchronous and what lets a follower fall behind.

`Since` is the same thing for a transport that answers requests rather than holding a connection open —
one batch and a return — and costs a round trip per batch:

```go
next, err := leader.Since(pos, w, litekv.ReplicaOptions{})   // one batch
```

`example/` wires a leader and a follower over a connection, end to end, in about fifty lines, and shows
a `DB` followed by another further down. For the same thing over a real socket, with framing, a
connection broken part way through and a reconnect, see [Replication over the
wire](#replication-over-the-wire) below: `server/` has the endpoint and `server/replica_test.go` puts a
leader and a follower through a real listener, which is the only place that says any of this survives a
wire rather than a pipe.

### A position is not an offset

An offset on its own cannot say which log it is an offset into. Two stores can both be a thousand bytes
long and hold entirely different records, and sending the bytes after a thousand to the second of them
splices one history onto the other and leaves a log that decodes perfectly and answers wrongly.

So a position also says where its last record starts and what that record's checksum is, and a leader
checks both against its own log before it sends anything:

```go
type Position struct {
	Offset int64  // bytes of the leader's log the follower holds
	Last   int64  // where the last of those records starts
	Crc    uint32 // that record's checksum
}
```

This is the check Raft makes with `prevLogIndex` and `prevLogTerm`, in the fields this format already
had — no store identifier, no epoch counter, and no change to the record layout. A position that is not
a point in the leader's log gets `ErrorDiverged`, and the only way back is `Reset`, which empties the
follower so it can be sent the whole log from `Position{}`. Compaction on the leader produces exactly
this, since it moves every record.

The same check keeps a follower honest. Nothing marks a store read-only, and nothing needs to: `Apply`
takes the position the batch was cut for and refuses when the store is somewhere else, so a write of the
follower's own is caught by the next batch rather than quietly kept. A batch that arrives twice is
refused for the same reason, with `ErrorPosition`.

### What a follower checks

Every record is decoded and verified against its own checksum before any of it is kept. A leader is not
a reason to trust the wire in between, and a record kept without checking is one no later read can
question. A stream that ends part way through a record keeps the whole records before it and reports the
rest as damaged, along with the position it reached, so carrying on from there loses nothing.

A follower is a store like any other. Give it a file with `Open` and it survives a restart: its position
is where its own log ends, which is what it asks with next time. That is the catch-up recovery every
leader-follower system needs after a follower has been down.

### Reads that are not stale

Replication here is asynchronous, so a replica is behind its leader, and the person who finds out is the
client that wrote to the leader a millisecond ago and read from a replica. Not seeing your own write is
the way this arrangement most often surprises people.

The position is already the fix. Take the leader's after a write, give it to the client, and take it
back with the client's next read:

```go
at := leader.Position()                     // hand this to the client after its write

if err := replica.Reached(at); err != nil { // and back with its next read
	// litekv.ErrorStale: this replica has not got there yet
}
```

That is read-your-writes. Asking with the position of the last *read* instead gives monotonic reads — a
client never sees the store go backwards, however it is routed — and both are things a leader with
replicas otherwise takes away. `Await` is the same question with waiting, which is usually what a read
wants, since a client is a few milliseconds ahead of the stream rather than minutes:

```go
err := replica.Await(at, ctx.Done())        // nil, or ErrorStale when the context ends
```

A single store checks the position rather than merely comparing it: its log is the leader's log byte for
byte, so the record the position names is there to be looked at, and a store that is merely as long
answers `ErrorDiverged`. A `DB` cannot — a follower holds none of the leader's bytes — so `Reached`
compares two positions in one leader's stream and the term decides which position it compares against:
a store that is following judges by how far it has applied, and one that follows nobody, because it never
has or because `Promote` raised it above the term it last applied at, is the leader those positions came
from and judges by its own. A position from a leader that has been replaced gets `ErrorSuperseded`,
since whether that write survived the failover is not something a follower of the new leader can work
out.

None of this says a store is fresh in general. A position is not a clock, and reaching one says only
that what it names is here.

### The number on every record

What makes two of a `DB`'s positions comparable is a number the leader puts on each record as it writes
it, counting from one. A position carries the number its next record will take, so comparing two of them
is comparing two integers — and the awkward case is the one that needs it:

> A position at the start of a log names no record, and the end of the log before it is the same point
> in the stream. Their offsets have nothing in common and their log ids differ by one. A leader whose
> active log is empty hands out the first, which is every leader that has just rotated or been
> snapshotted, and a follower holding every record it ever wrote rests at the second, because that is
> the position that can be checked.

Both carry the same number, so both compare equal, and a replica holding everything says so instead of
saying it is behind.

**It is in the record rather than counted, and that is not an implementation detail.** Merging drops
records — superseded ones always, tombstones when the run reaches the oldest log — so a store counting
what it holds would count fewer than the leader wrote, and two replicas that merged at different times
would answer the same question about the same position differently. A number that travels with the
record is the same number wherever it is read. A follower keeps the numbers on the records it is sent
rather than making its own, and a follower that is promoted carries on from the highest it holds.

The number is handed out under the write lock, which is what puts it in the order the records are in.
Taking it from an atomic counter before the lock would let two writers take numbers in one order and
append in the other, and a position naming the last record would then name a number with a bigger one
behind it. The checksum has to follow the number, so a numbered store checksums the serialized record
under that lock instead of folding the fields before it.

**What it costs.** Eight bytes a record, which for a 16-byte key and a 16-byte value is 54 bytes to 62,
and for a 1 KiB value is under a percent. The write itself is *faster*, because a checksum taken in one
pass over the record beats one folded field by field:

| in-memory write | plain    | numbered |
| --------------- | -------- | -------- |
| 16-byte values  | 231 ns   | 205 ns   |
| 1 KiB values    | 261 ns   | 219 ns   |

A `DB` writing 128-byte values into rotating 64 KiB logs is about 4% slower, which is the extra bytes
arriving at the next rotation sooner rather than anything the write does. A `KeyValueStore` is untouched
in both senses: it does not number, so it holds the bytes it always did and writes them at the speed it
always did.

A store written before any of this has records with no number on them. They report zero, positions in
them carry zero, and `Reached` falls back to comparing log ids and offsets — which is exact everywhere
except at that one boundary, where it says `ErrorStale` while holding everything until one more record
crosses.

### What it costs

| | |
| ------------------------------------ | -------- |
| A record across, leader and follower  | 379 ns   |
| The same, 1 KiB values                | 969 ns   |
| Catching up from nothing              | 1.9 GB/s |

Those leave out the transport, which is the point: they are what this package costs, and any connection
is slower. Two allocations a record, and neither of them a buffer — both ends keep theirs between calls.

A leader that nobody is following pays one atomic swap per write for the notification that would wake
one, which does not measure: 110.9, 111.3 and 111.3 ns against 109.8, 111.4 and 116.6 ns without it,
which is less than three runs of the same code differ by. A leader that *is* being followed pays for
waking it — 107 ns a write becomes 138. That is the worst case and deliberately so: the follower in
`BenchmarkWriteWithAWaiter` wakes on every single record and goes straight back to asking, where a real
one drains what has piled up and asks once. Handing out that channel used to take the store's write
lock, which put the follower in the same queue as the writers and made it 165 ns instead; it is an
atomic now.

`ReplicaOptions.BatchSize` is how much crosses at once and how much either end buffers, a megabyte by
default. Since a stream costs no round trip per batch, it is a memory setting rather than a latency one.
A record larger than a batch still crosses whole, or a log holding one could never be replicated at all.

### Replicating a DB

A `DB` cannot ship its log the way a single store does, and the reason is worth being precise about. Its
logs are merged in the background, and a merge renames its output over the oldest log it replaces — so
the file called `0000000005.seg` can become a different file, with different contents, at a different
length, while a follower thinks it has read forty kilobytes of it. Merging also discards: superseded
records always, tombstones when the run reaches the oldest log. The bytes are not a stream and the
history is not kept.

So what crosses is records, not bytes. The follower appends them to a store of its own and rotates and
merges on its own schedule; the two ends agree on every key and on nothing about their files. That is
the *logical* replication log of DDIA's four, chosen for the reason the book gives — it is not tied to
how either end stores things — and it has the side benefit that leader and follower need not be running
the same build.

Because a merge destroys history, a leader cannot replay its writes from arbitrarily far back, and no
protocol can make it. Replication is therefore a snapshot and then the tail after it:

```go
// the leader
at, release, err := leader.Snapshot(w, litekv.ReplicaOptions{})  // the live records
at, err = leader.Follow(at, release, send, stop, litekv.ReplicaOptions{})

// the follower
err = replica.ApplySnapshot(at, r, litekv.ReplicaOptions{})
at, err = replica.Apply(from, next, r, litekv.ReplicaOptions{})
```

`Follow` hands each batch to a callback along with the position it leads to, rather than writing it to a
writer, because both have to reach the other end. A follower of a `DB` cannot work out where it is from
its own logs the way a follower of a single store can — its files have nothing to do with the leader's —
so the position travels with the records. How it travels is yours: a length, the twenty-eight bytes of
`MarshalBinary`, and the records will do.

`ApplySnapshot` is the other half: it empties the store, applies the records as they arrive — a snapshot
of a store larger than memory costs no more than a small one, which reading it in one piece would give
away — and writes the position down last. A failure anywhere in between leaves a store holding part of a
snapshot and claiming no position at all, which is a follower that needs another one: where it started.

`Apply` takes a batch and the position it leads to. A batch is all or nothing here, unlike the
single-store `Apply`: there a half-applied batch is a fact about the follower's own log and its position
says so, while here the position is something the leader said about the whole batch. A batch that is
damaged or ends part way through a record is refused entirely.

The records go down before the position that claims them. A crash in between leaves a follower having
applied records it does not admit to, and the same batch arrives again — the same records in the same
order, so what they say is unchanged and only the bytes are spent twice. The other order would claim
records that were never written, which is the one that loses data. `Applied` reports how far through a
leader a store has got, and survives closing and reopening because it is written down beside the logs.

`Snapshot` writes one record per live key — the newest version, tombstones skipped, since a follower
starting from nothing has no older value for one to hide — and returns the position they are current as
of. It is consistent without stopping the store: the active log is frozen first, so everything the
snapshot covers is on the disk and immutable, and anything written from then on is the tail rather than
part of the snapshot. Writes and rotation carry on throughout. Merging does not, since a merge may
remove a log and this is reading them.

### Every position names a record, almost

A `DBPosition` is which log and where in it, and the log part is an ordinary `Position` carrying the
same check. That check needs a record to check against, which is what makes the start of a log awkward:
it names nothing, and a frozen log may have been merged and be a different file behind the same name.

So the tail never rests there. A batch crosses from one log into the next rather than stopping at the
boundary, and a follower that has read a log to its end stays at that end rather than stepping to the
start of the next — the end of a log names its last record, the start of one names nothing. The cost is
that a batch may overshoot its size by one record at each log it crosses.

One position escapes this: a snapshot of a store whose active log is empty has nowhere to point but the
start of that log. Used before the log fills it is fine, because a log being written is never merged. If
the log fills and freezes first, the leader says `ErrorDiverged` rather than guess, and the follower
takes another snapshot.

### Fencing, and promoting a replica

Two stores taking writes at once cannot be reconciled. The position check refuses to splice one log onto
another, so nothing is corrupted — but that is integrity, not durability: writes acknowledged by the
wrong leader are discovered to be worthless and thrown away. A checksum cannot tell you that a leader has
no business being one. A term can, because it only ever goes up.

```go
term, err := replica.Promote()   // this store is now the leader, at term+1
```

Every `DBPosition` carries the term it came from, and it is written down beside the logs, so it survives
a restart — a term that did not would be no fence at all. From there:

- a leader asked for records by anything carrying a **newer** term learns it has been replaced, writes
  that term down, and stops taking writes: `ErrorFenced` from `Write`, `Delete` and `Snapshot`;
- a follower refuses a snapshot or a batch from an **older** term, so a leader that has been replaced
  cannot spread its records;
- a follower **adopts** the term of the leader it follows, so promoting one replica fences it against
  the leader it replaced.

Reads carry on throughout. A fenced store is not broken, it is not in charge. `Promote` raises the term
above the highest it has heard of, so a store that was fenced becomes a leader again by being promoted.

What this does not do is decide who should be leader. Something outside — a person, a script, a lease
service — decides, and `Promote` is how the decision is written down.

### Holding the logs a follower still needs

Without something holding them, a follower is at the mercy of the merging going on underneath it. One
that has fallen behind is reading a frozen log and a merge can take it; one that has caught up rests
wherever it read last, which when the log being written is empty is the end of the last frozen log — and
a merge can take that too. Either way the answer is `ErrorDiverged` and a snapshot of the whole store,
which for an idle follower is a routine and expensive surprise.

So `Snapshot` holds the log its position names, and every log after it, and hands that hold to `Follow`,
which takes one of its own and then moves it forward as it reads:

```go
at, release, err := leader.Snapshot(w, opts)  // holds from here on
...                                            // ship it, however long that takes
leader.Follow(at, release, send, stop, opts)   // takes over, and the hold follows the stream
```

From that log onwards, and not only that log: a follower walks forward through the logs, and the newest
frozen ones are exactly what merging takes first, so pinning one at a time would leave it reading into a
run being rewritten as it went. This is what PostgreSQL calls a replication slot, and it pins the same
way. `Hold` is exported for a leader answering with `Since` rather than holding a connection open.

The handover matters and is the one place it can be got wrong. `Follow` takes its own hold before
calling the one it was given, so the log the stream starts from is never unheld for an instant.
Releasing the snapshot's hold yourself and then calling `Follow` leaves a gap — on a machine with one
core, however long it takes that goroutine to be scheduled, which is long enough to lose it every time.

**What it costs is logs.** Everything written since the oldest follower's position stays on the disk,
unmerged, and every lookup asks each of those logs in turn. A follower that goes quiet without releasing
leaves the leader carrying them indefinitely. That is why this is a hold with a release rather than a
list of followers the leader keeps: nothing here pins a disk for a follower that has gone away. `Merge`
ignores holds — it is an explicit request to compact the whole store, and a follower reading one of
those logs will have to take a new snapshot.

**A follower that was away is usually carried forward rather than stranded.** A hold covers a follower
that is connected; one that was away while the merging happened comes back with a position into a log
that has been folded into another. The records carry numbers and a merge keeps them, so the leader reads
the log that now holds those numbers and carries on from the right place — no snapshot, no whole store
over the wire.

It refuses, and answers `ErrorDiverged` as before, when the position carries no number, when the number
is one no log reaches, or when any log it would stream across has dropped records. That last is the one
that matters: a merge which reaches the oldest log drops tombstones, and a follower carried across one
would never hear that a key was deleted. So **a follower must still always be able to take another
snapshot**, and the loop that follows a `DB` has to be written with that in it. The one in `example/` is.

### What replicating a DB costs

| | |
| --------------------------------------------- | ---------- |
| Reading a snapshot out of the logs             | 940 MB/s   |
| Taking one, 1 KiB values                       | 106 MB/s   |
| Taking one, 16-byte values                     | 23 MB/s    |

Taking a snapshot is the slower side and deliberately so: it empties the store first, and every record
is decoded and checked against its own checksum before it is kept. The small-value figure is mostly the
per-record cost of that; at 1 KiB the bytes dominate.

The position file is synced only under `SyncAlways`. It must never be more durable than the records it
claims — a position that survived a power cut naming records that did not would be a follower quietly
missing data, where the other way round costs one batch applied twice.

### One writer in front of many callers

Writes take every shard of the lock, so two goroutines writing do not merely fail to go faster: they
halve the store's throughput. A server with a handler per request is exactly that shape. `Writer` puts
one goroutine in front of the store and lets the handlers queue behind it:

```go
w := db.Writer(litekv.WriterOptions{})
defer w.Close()

err := w.Write(key, value)   // blocks until stored, as db.Write does
```

Its methods block until the records are stored, including the wait for the disk the sync policy asks
for, so a caller has the same promise it always had and may reuse its slices as soon as one returns.

What makes it worth more than an orderly queue is **group commit**: everything waiting when the writer
wakes goes down as one batch, so a hundred callers cost one write to the log and one sync between them.
It never waits for a group to fill — a store nobody else is writing to is one handoff behind a direct
write and nothing else — so the busier the store, the larger the groups, which is the right way round.

| ten goroutines writing 128 bytes | direct  | through a `Writer` |
| -------------------------------- | ------- | ------------------ |
| `SyncAlways`                     | 4.2 ms  | 0.80 ms            |
| `SyncEvery`                      | 4.0 µs  | 2.1 µs             |
| `SyncNever`, with a file         | 5.9 µs  | 2.4 µs             |
| in memory, no file               | 0.53 µs | 1.2 µs             |

The last row is the one to read carefully. What the queue amortizes is the cost of a write to the log,
and when there is no log there is nothing to amortize — only a channel handoff where there used to be a
contended lock, which is the worse of the two. **Put a `Writer` in front of a store with a file, not in
front of one in memory**, and the more the sync policy waits for the disk the more it is worth.

A group is a batch, so it is atomic: a crash loses the whole group or none of it, and no caller is ever
told a write failed while its record survives. A caller's own `WriteBatch` keeps its atomicity inside
the group, its records together and in order. Everything else the store would have said still comes
back — a fenced store still reports `ErrorFenced` through a `Writer`, because it is a queue in front of
the same call and not a different one.

`Close` writes what is already queued and answers those callers before the goroutine goes; anything
arriving afterwards reports `ErrorClosed`. It does not close the store, which is yours to close after
it.

## Serving it over HTTP

Nothing in the library opens a socket and nothing in it should: the store has no idea what a request is,
and keeping it that way is what lets the same code be embedded in a program and served to a network.
`server/` is the other half of that bargain — a package that imports `litekv`, owns the protocol, and
reaches the store through the same exported API any other caller would. It is an `http.Handler` and
nothing else: it does not listen, it does not open the store, and it does not close it.
`cmd/litekvd` does all three.

```bash
go run ./cmd/litekvd -dir /var/lib/litekv -addr 127.0.0.1:8080
```

| flag                | what it is                                                       | default          |
| ------------------- | ---------------------------------------------------------------- | ---------------- |
| `-dir`              | the directory holding the store                                  | required         |
| `-addr`             | the address to listen on                                         | `127.0.0.1:8080` |
| `-sync`             | `always`, `every` or `never`                                     | `always`         |
| `-sync-interval`    | how often to sync under `-sync every`                            | 1s               |
| `-segment-size`     | bytes before a log is frozen                                     | 4 MiB            |
| `-merge-trigger`    | logs of a size before they are merged                            | 2                |
| `-max-value`        | the largest value a write may carry                              | 16 MiB           |
| `-max-batch`        | the largest body `POST /v1/batch` will take                      | 32 MiB           |
| `-max-scan`         | the most pairs a range answers with, and the most `?limit=` may ask for | 1000      |
| `-queue`            | writes that may be waiting before a handler blocks               | 1024             |
| `-leader`           | base URL of a leader to follow                                   | follow nobody    |
| `-token-file`       | file holding a shared bearer token; empty means no auth           | none             |
| `-heartbeat`        | how often an idle leader says it is there                        | 10s              |
| `-idle`             | how long a follower waits to hear it before reconnecting         | 30s              |
| `-read-timeout`     | how long a request has to arrive, headers and body                | 60s              |
| `-write-timeout`    | how long a response has to be written (streams exempt)            | 60s              |
| `-idle-timeout`     | how long an idle keep-alive connection is held                    | 120s             |
| `-shutdown-timeout` | how long requests in flight get once it is asked to stop         | 10s              |

`-sync` defaults to `always` because the library does: a binary that quietly weakened durability
relative to the code it wraps would be the wrong kind of convenient. `-sync every` is the usual trade
and the one to reach for.

It listens on loopback unless told otherwise. There is no authentication and no TLS here yet, so put it
behind a proxy or on a private network before giving it an address a stranger can reach. One `litekvd`
owns a directory: the store is not multi-process safe and nothing checks, so a second one on the same
`-dir` writes over the first one's log.

### The routes

| method      | route            | what it does                          | answers          |
| ----------- | ---------------- | ------------------------------------- | ---------------- |
| `GET`       | `/v1/keys/{key}` | the value, as the body                | 200, 404         |
| `HEAD`      | `/v1/keys/{key}` | the value's length and nothing else   | 200, 404         |
| `PUT`       | `/v1/keys/{key}` | stores the body under the key         | 204              |
| `DELETE`    | `/v1/keys/{key}` | writes a tombstone                    | 204              |
| `GET`       | `/v1/keys`       | a range or a prefix, as NDJSON pairs  | 200, 400         |
| `POST`      | `/v1/batch`      | several records, all of them or none  | 204, 400, 413    |
| `GET`       | `/v1/replica/stream?from=` | the records after a position, streamed | 200, 400, 409 |
| `GET`       | `/v1/status`     | which of the two this node is         | 200              |
| `GET`       | `/health`        | whether this node can serve           | 200, 503         |
| `GET`       | `/metrics`       | Prometheus text                       | 200              |
| `POST`      | `/v1/promote`    | stop following and raise the term     | 200              |

A value is the body, raw. There is no JSON envelope around a caller's bytes and nothing is base64 on the
way through, because a key-value store's whole job is to hand back what it was given; the type is
`application/octet-stream` and the server has no opinion about what is in it. An empty value is a value,
and the `Content-Length` of `0` is what tells it apart from a missing key.

`PUT` takes a `Litekv-Expires` header holding an RFC 3339 time, and writes a record that stops answering
once that instant has passed. It is an instant and not a duration for the same reason the store's expiry
is one: a duration has to be resolved against somebody's clock, and the only clock a client and a server
agree on is the one they both write down. A client thinking in TTLs subtracts.

`DELETE` of a key that was never there answers 204, not 404. The store cannot answer that question
anyway — a delete appends a tombstone without looking for what it hides — so a 404 would be a lie
dressed as a check.

### Spelling a key in a URL

A key is arbitrary bytes and a URL is not. Percent-encoding a path segment carries all of them: Go's
`ServeMux` unescapes segment by segment and a `%2F` is deliberately *not* a separator, so a key holding
slashes, spaces, control bytes, or sequences that are not UTF-8 at all survives the trip unchanged.

```bash
curl -X PUT --data-binary 'nested' http://127.0.0.1:8080/v1/keys/a%2Fb%2Fc
curl http://127.0.0.1:8080/v1/keys/a%2Fb%2Fc      # nested
```

`TestKeyOfAnyBytes` puts thirteen awkward keys through a real socket and a real client rather than
trusting the documentation for any of that, and checks the store holds the bytes the caller meant rather
than the ones the URL was spelled with — reading it back through the same encoding would agree with
itself however wrong both ends were.

The one key with no spelling *here* is the empty one, which the store holds happily. A path wildcard
does not match an empty segment, so `/v1/keys/` is not a route. It is reachable through the routes that
do not spell a key in a path — a batch line writes it and a range hands it back — but not through this
one.

### What a failure says

A failure is a status and a JSON body of one field: `{"error":"key not found"}`. A client that wants to
branch on what went wrong branches on the status, and the sentence is for whoever is reading a terminal.

| status | when                                                                              |
| ------ | --------------------------------------------------------------------------------- |
| 400    | an expiry, a batch line, a range query, or a `from` the server could not read       |
| 404    | no value under that key — never written, deleted, or expired                        |
| 405    | a method that route does not have, with an `Allow` header saying which it does      |
| 409    | the store has been fenced, with `Litekv-Term` carrying the term it is on            |
| 409    | a write aimed at a replica, with `Litekv-Leader` saying where it should go          |
| 412    | a read carrying `Litekv-After` from a store that has not got there                  |
| 504    | the same, after `Litekv-Wait` ran out                                               |
| 413    | a value over `-max-value`, or a batch over `-max-batch`                             |
| 503    | the store is closed, which is what a server on its way down looks like              |
| 500    | anything else                                                                       |

The three ways there can be no value under a key are one status on purpose. The store tells them apart
because it knows whether the key was asked to go, told to go by itself, or was never there, and none of
those change what a caller does next.

A 500 says "internal error" and nothing more. An error from the store can name a path on the server's
disk or an offset in a log, and a stranger has no business with either; it goes to the log instead.

Fencing refuses writes and not reads. A fenced store's records are still records, and refusing to serve
them would take a replica out of service for a reason that has nothing to do with reading — what the
term on the answer is for is telling a client that what it just read may be behind.

### One writer under the handlers

A handler per request is a goroutine per request, and a write takes every shard of the store's lock. An
HTTP server is therefore the worst caller a store of this shape can have: two goroutines writing do not
merely fail to go faster, they halve its throughput. So `server.New` puts a `Writer` in front of the
store and every `PUT` and `DELETE` goes through it. This is the caller "One writer in front of many
callers" was written for, before there was one.

Ten handler goroutines writing a 128-byte value, driven through the handler with recorders so that the
socket — real cost, and the same cost either way — does not bury what is being measured:

| `-sync`  | nothing stored | straight to the store | through the queue | the store's share |
| -------- | -------------- | --------------------- | ----------------- | ----------------- |
| `never`  | 971 ns         | 3,689 ns              | 1,248 ns          | 2,718 → 277 ns    |
| `every`  | 996 ns         | 3,776 ns              | 1,276 ns          | 2,780 → 280 ns    |
| `always` | 1,011 ns       | 3.82 ms               | 779 µs            | 3.82 ms → 778 µs  |

The first column is a request that stores nothing, and it is there so the others can be read: building a
request and a recorder is about a microsecond of every row, and without it the ratio looks smaller than
it is. Take it off and the queue is worth **9.8x** with no sync at all — that is pure lock contention
going away — and **4.9x** under `SyncAlways`, where what is being amortized is one wait for the disk
shared out among everybody waiting. End to end, request and all, it is 3.0x and 4.9x.

A `Server` therefore has to be closed, even though the store it serves is somebody else's: `New` starts
the writer's goroutine. Three things go down and the order is the whole of it — stop taking requests,
close the `Server`, close the store. Any other order answers a request that was already accepted with a
503, or drops a write that was a moment from being acknowledged. `litekvd` does it in that order and
`TestClosingTheServerStopsWritesAndNotReads` holds the middle step to it: a closed `Server` refuses
writes with 503 and goes on answering reads, because the store is still open and still holds everything.

### Several at once, and ranges

Two routes carry more than one record, and both of them carry it as **newline-delimited JSON**: one
object to a line, no array around them, so a body can be produced and consumed a line at a time and
neither end has to hold a large answer as a single JSON value before it can look at any of it.

```bash
curl -X POST --data-binary @- http://127.0.0.1:8080/v1/batch <<'EOF'
{"op":"write","key":"user:1","value":"ada"}
{"op":"write","key":"user:2","value":"grace","expires":"2030-01-01T00:00:00Z"}
{"op":"delete","key":"user:0"}
EOF

curl 'http://127.0.0.1:8080/v1/keys?prefix=user:'
# {"key":"user:1","value":"ada"}
# {"key":"user:2","value":"grace"}
```

`POST /v1/batch` stores every operation or none of them, and answers 204. `"op"` is `"write"` or
`"delete"`, `"expires"` is an RFC 3339 time meaning exactly what the `Litekv-Expires` header means on a
`PUT`, and a delete carrying a value or an expiry is refused rather than having them quietly dropped.
An absent key or value is the empty one, a blank line is skipped, and an empty body stores nothing and
answers 204 — an empty batch is what the engine calls an empty batch.

All or nothing means two things and the route provides both. The engine provides the second:
`WriteBatch` puts the records down behind a marker and recovery discards from that marker on unless
every one of them is there. The route provides the first: the **whole body is parsed** into a
`litekv.Batch` before any of it is handed to the store, so one line the server cannot read refuses the
whole request with a 400 naming that line. A parser that stored as it went would make the marker
pointless — atomic on the disk and torn on the wire.

### A key is bytes and a JSON string is not

Both routes use one encoding rule, in both directions:

- A key or a value is a plain string field — `"key"`, `"value"` — when it is **valid UTF-8**.
- It is a separate base64url field — `"key_b64"`, `"value_b64"` — when it is not. That is
  `base64.RawURLEncoding`: the alphabet of RFC 4648 §5 and **no padding**, since padding carries
  nothing and one spelling of a field is easier to be right about than two.
- Which one is decided by `utf8.Valid` and by nothing else. `encoding/json` replaces a byte that is not
  UTF-8 with U+FFFD rather than refusing it, in both directions, and a store that hands back a
  replacement character where its caller wrote `0xff` has lost that caller's data while answering 200.
- Sending both forms of one field is an error rather than something to resolve, and so is a raw byte
  that is not UTF-8 anywhere in a line — that is what the `_b64` fields are for.

The plain form is the ordinary one and is meant to be: keys people actually have are text, and a body
of them should be readable in a terminal without anything being decoded first.

This is also the only place the **empty key** can be reached. It has no spelling in a path, but a batch
line with no `key` field — or with `"key":""` — writes it, and a range hands it back.

### Reading a range over HTTP

`GET /v1/keys` takes `?prefix=` or `?from=`&`?to=`, with `from` included and `to` excluded, and answers
the matching pairs in key order. Both bounds and the prefix are percent-decoded exactly as a key in a
path is, so they carry any byte a key can hold; `TestBoundOfAnyBytes` puts eleven awkward prefixes
through a real socket the way `TestKeyOfAnyBytes` does for paths.

| the request                     | what it means                                                  |
| ------------------------------- | -------------------------------------------------------------- |
| no parameters at all            | every key, capped by the maximum below                          |
| `?prefix=` with nothing after it| the same thing: an empty prefix is every key, as it is in the engine |
| `?from=` or `?to=` empty        | no bound on that side                                           |
| `prefix` with `from` or `to`    | 400. They are two ways of naming one range, not two to intersect |
| a `from` after its `to`         | an empty range, which is 200 and no lines                       |
| nothing matched                 | 200 and no lines. There is no key here to be missing, so no 404  |
| `?limit=` empty, zero, negative | 400. A client that built the query wrongly should hear about it  |
| `?limit=` over `-max-scan`      | 400, naming the maximum                                         |

The limit is refused rather than quietly lowered because counting the lines against the limit it asked
for is the only way a client can tell that an answer was cut short. Paging is that plus one byte: `from`
is inclusive, so the next page starts at the last key with a `%00` after it.

**What the limit does not buy is a cheap range.** A range is gathered and not streamed — every log has
to be asked before the first key can be yielded in order, and the store's read lock is held for all of
it — so stopping at the limit does not stop the walk that found the keys. What it stops is reading the
records: the value copies, and for a frozen log the system calls that fetch them, which is most of the
cost of a large answer but not the search. A range that has to be cheap has to be narrow. `-max-scan` is
there because rotation and merging want the write lock and would otherwise queue behind whoever is
scanning; it is the cap a client cannot raise.

For the same reason the answer is built in memory and sent afterwards rather than written from inside
the range. The callback runs under the store's read lock, and a client that stopped reading a socket
under it would be deciding when the store is allowed to rotate — the same trade `GET /v1/keys/{key}`
makes by using `Read` instead of `View`. The framing is still NDJSON and a client can still consume it a
line at a time; what it does not get is a lock held open while it does.
### Replication over the wire

Everything the [Replication](#replication) chapter describes now has a route. The leader streams from a
position on the same listener it serves keys on, and a follower is a second `litekvd` pointed at it:

```bash
litekvd -dir /var/lib/litekv   -addr 127.0.0.1:8080
litekvd -dir /var/lib/replica  -addr 127.0.0.1:8081 -leader http://127.0.0.1:8080
```

**One listener and not two.** One port to open, one thing to shut down, one place for authentication to
go when there is any, and it goes through whatever proxy or load balancer a read replica is already
behind — a second raw TCP listener would have needed every one of those again. What it costs is a few
bytes of chunked framing per batch, which against the megabyte a batch defaults to is not a number worth
writing down.

The body is the framing `tcp_test.go` arrived at over a bare socket, carried across unchanged: a kind
byte, the position those records leave a follower at, a length, and the payload, flushed per frame. A
record stream is self-framing, but a reader still has to know where one batch stops and the next
begins, and a snapshot has to be told from a batch because different calls apply them.

**A position on the wire is opaque.** It is base64url of `MarshalBinary`, unpadded, and a follower hands
back the bytes it was given without taking them apart. That is what lets a `DBPosition` gain a field — as
it has twice, for the term and for the sequence number — with nothing on the client side knowing. It is
a cookie, not a structure, and one that is not a position at all is a 400.

**A leader answers divergence with a snapshot, not by hanging up.** Nothing holds a log open for a
follower that is not connected, so a follower that was away long enough always comes back to a position
that is gone — that is the ordinary fate of one that missed a merge, not an unusual path. A leader that
treated it as a failed connection would leave that follower asking for the same dead position forever,
and reconnecting would never help.

**A connection ending is normal.** The follower reconnects with a backoff that doubles from 100 ms to
5 s, half of each wait jittered so that several followers that lost the same leader do not all come
back at the same instant. A connection that stayed up longer than the longest wait was a working one,
so the next attempt starts from the shortest wait again. A leader that refuses — 409 because it has been
replaced, 400 because it could not read the position — is retried at the longest interval rather than
climbing to it: asking again straight away cannot change the answer, and somebody promoting something
can.

Stopping a follower does not cost it its place. The position is written down beside the follower's own
logs by `Apply`, so a follower that comes back reads it out of the store and resumes; nothing in the
process keeps a copy that could go stale. `Close` waits for the goroutine, so a batch being applied when
the stop arrives is finished and written down before the store may be closed.

**Shutting a leader down with a follower attached** needs one thing that is easy to leave out. A stream
is a request that never finishes on its own, and `http.Server.Shutdown` waits for every request rather
than cancelling any of them, so a leader would spend the whole of `-shutdown-timeout` waiting for a
handler that had no intention of returning: **10.05 s against 0.03 s**, measured with two binaries on
loopback. `Server.CloseStreams` ends the streams and refuses new ones, and `litekvd` hands it to
`(*http.Server).RegisterOnShutdown`. A follower whose stream ends that way reconnects, which is what it
does about any connection ending.

What is **not** here yet is roles. Nothing marks a node as a follower, so one started with `-leader` also
takes writes of its own, and a write to it will diverge it from its leader — its own records go into its
own log while the leader's position marches on. `litekvd` says so at startup and that is all it does
about it. Do not write to a node with `-leader` on it.

### A quiet stream and a dead one

They look alike from the follower's end, and that is the problem. A TCP connection that has been
blackholed rather than closed — a cable pulled, a firewall dropping instead of refusing, a leader that
lost power — delivers nothing and reports nothing, and the OS keepalive notices in about fifteen
minutes. A follower that is not being written to has no other way to tell.

So a leader with nothing to send says so: a heartbeat frame every `Heartbeat` (10s by default), carrying
the leader's own position so a follower can see how far behind it is while nothing is being written. It
is not applied and must not be — it names records the follower has not been sent.

A follower that hears nothing for `Idle` (30s by default, three beats) drops the connection and dials
again, which costs it nothing: it comes back at the position it had.

**The deadline is on silence, not on a frame.** A snapshot of a large store is one frame that takes as
long as it takes, and a deadline that only moved when a frame completed would cancel the transfer it was
in the middle of — turning a slow first sync into a loop that never finishes one. Every chunk that
arrives counts as the leader being alive.

Two things this cost, both worth knowing. There are now two goroutines with something to say on one
connection, so every frame goes out under one lock — an `http.ResponseWriter` written by two goroutines
at once is a data race and a corrupted frame, in that order. And the handler waits for its heartbeat
goroutine before returning, because a `ResponseWriter` may not be touched once its handler has returned;
left to stop in its own time it writes into a response net/http is already finishing. The race detector
found the second one. Nothing else would have.

### Which of the two, and reads that are not stale

A node started with `-leader` is a **replica**. It refuses every write with 409 and a `Litekv-Leader`
header saying where to send it — `PUT`, `DELETE` and `POST /v1/batch` alike — and it goes on answering
reads, which is what it is for.

That refusal is not fencing and could not be. A store that is following holds its leader's term, so
`ErrorFenced` never fires, and it will take a write perfectly happily: the record goes into its own log,
the leader's records keep arriving around it, and the two histories never reconcile. No checksum is
wrong and nothing errors. It is the quietest way to lose data this design has, and the only thing that
prevents it is this server knowing which of the two it is — the engine cannot know, because the thing
pulling the records is up here.

```bash
curl -X POST http://127.0.0.1:8081/v1/promote      # {"term":1}
curl http://127.0.0.1:8081/v1/status
# {"role":"leader","term":1,"position":"...","segments":3,"keys":812}
```

`POST /v1/promote` stops the following first and raises the term second, and the order is the point: a
term raised while records are still arriving is a store that has fenced its own leader and then applies
another of its batches. What promotion does not do is decide that this node should be the leader — that
is consensus, and it is not here; see `AGENTS.md` for why an external lease is the pragmatic answer at
this size.

**Reads that are not stale** are the other half, and the reason `Reached` and `Await` were built. Every
write answers with `Litekv-Position`, an opaque cookie for where the store had got to. Send it back as
`Litekv-After` on a later read and a node that has not got there refuses rather than answering with what
it has:

```bash
POS=$(curl -si -X PUT --data-binary 'ada' http://127.0.0.1:8080/v1/keys/user:1 \
      | grep -i '^litekv-position:' | cut -d' ' -f2 | tr -d '\r')

curl -H "Litekv-After: $POS" -H "Litekv-Wait: 2s" http://127.0.0.1:8081/v1/keys/user:1
```

Without `Litekv-Wait` a replica that is behind answers 412 at once, with its own position in
`Litekv-Position` so a client can decide whether to wait here or go elsewhere. With it, the read waits
on `Await` and answers 504 if the time runs out — which says the wait was too short, not that the
records are never coming. This is read-your-writes across a load balancer, and it hides the replication
lag from the client that just wrote; it does not remove it.

### Running it

`GET /health` answers 200 while the store can serve and 503 once it is closing, and it asks the store
the cheapest question that touches its state — no disk. **It is the one route a token does not cover**:
a load balancer probing this node is not a client and has no business holding the secret that opens the
database.

`GET /metrics` is Prometheus text — a counter per route, method and status, a latency histogram per
route, and the store's own numbers:

```
litekv_requests_total{route="/v1/keys/{key}",method="GET",status="200"} 41231
litekv_request_duration_seconds_bucket{route="/v1/keys/{key}",le="0.001"} 41180
litekv_replication_streams 2
litekv_role{role="leader",leader=""} 1
litekv_term 1
litekv_store_keys 812
litekv_store_segments 3
```

`litekv_replication_streams` is the one gauge a request in flight contributes to, and it exists because
`litekv_requests_total` counts requests that *finished*: a replication stream that has been up for a
week has never been counted once. How many followers are attached right now is the number an operator
actually wants.

The route label is the **pattern** and never the path. A label taken from the URL would be one series
per key, and `/metrics` would grow with the store until it was the largest thing this server sends
anybody. Every route is registered through one function so that it cannot be forgotten, and
`TestEveryRouteIsCounted` holds it there.

Requests are logged at Debug, so turning request logging on is a level rather than a flag. A server
logging every request at Info is a server whose log nobody reads, and the failures that matter log
themselves already.

### Keeping strangers out

`-token-file` names a file holding a shared secret that every request must carry as
`Authorization: Bearer <token>`. A file and not a flag value, because an argument is visible in `ps` to
every process on the machine. The same token authenticates this node to its `-leader`.

It covers everything except `/health` — **replication included**, which is the route that matters most,
since it hands the whole database to whoever asks. The comparison is constant time: one that stopped at
the first wrong byte would tell a caller how much of the token it had right, and a few thousand requests
turn that into all of it.

This is a shared secret and nothing else. There are no users, no scopes, and no read-only credential:
anything that can read can also write. It is not a substitute for TLS, which is still not here.

### Timeouts, and the one route exempt from them

Without timeouts a client that opens a connection and sends a byte an hour holds a handler for as long
as it likes, and enough of them are the whole server. `litekvd` sets a 10s header timeout and the three
above.

`-write-timeout` bounds how long a response may take to write, and there is exactly one response here
that is meant to still be being written next week: the replication stream. Rather than go without the
timeout because of that route, **the route takes its own deadline off** — one call to
`http.ResponseController`. `TestAStreamTakesItsWriteDeadlineOff` runs a real follower over a listener
with a 150ms write timeout and counts connections rather than records, because a stream cut by a
deadline is one the follower reconnects to and the records arrive either way. One connection is the
assertion; two means the deadline cut it.

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

Writers get nothing from any of this, and adding them costs. A write takes every shard, so writers
exclude each other, and splitting the same work over more of them only moves four mutexes between more
cores:

| writers | per write | writes a second |
| ------- | --------- | --------------- |
| 1       | 114 ns    | 8.8 M           |
| 2       | 228 ns    | 4.4 M           |
| 4       | 232 ns    | 4.3 M           |
| 8       | 239 ns    | 4.2 M           |

The whole step is from one writer to two, and it halves the throughput of the store; four and eight are
barely worse than two. What costs is leaving the uncontended path rather than how crowded it gets: an
acquisition that stops winning its compare-and-swap parks the goroutine and wakes it through the
runtime, four times over, and once every writer is doing that another one merely joins the queue.

So one writer is not a limit to work around but the shape to build for. Handing writes to a single
goroutine beats sharing the store between several, and it is the arrangement the rest of the design
already assumes.

A reader is not free either, once there is a writer. Readers spread over the shards and a writer takes
all of them, so a write does not slow a read down, it stops it. Reading 1 KiB values on ten goroutines,
against nothing else and then against one goroutine writing as fast as it can:

| writers alongside | per read |
| ----------------- | -------- |
| none              | 47 ns    |
| one               | 145 ns   |
| two               | 147 ns   |

One writer triples the cost of a read, and the second is free, which is the same shape as the table
above and has the same cause. A background writer is not a rounding error on a read-heavy store, and it
is worth deciding what rate it really needs to run at.

None of this applies to `DB`, which has none of it. `DB` guards its segments with a plain
`sync.RWMutex`, so its readers contend on the one counter this whole section is about avoiding:

| 512-byte read | one goroutine | ten goroutines |
| ------------- | ------------- | -------------- |
| active log    | 120 ns        | 154 ns         |
| frozen log    | 589 ns        | 3.5 µs         |

The active log is slower on ten cores than on one; a frozen one is six times slower. What causes it is
the system calls, and that is measured rather than supposed. A frozen read used to take two of them, a
header and then the rest of the record; asking for a page up front, so that anything smaller arrives
whole, took this row from 5.2 µs to 3.5 and the serial read from 839 ns to 589.

Two other explanations were tried and are wrong. It is not the segment list, which used to be rebuilt
and allocated on every read: removing that made a read of the active log a third faster and moved this
table not at all. And nothing locks the file, since a pread needs no lock.

What is left is that a system call blocks whatever else it does, and ten goroutines' worth of blocked
calls leave the runtime shuffling threads to cover them. Halving the calls took a third off; removing
the rest would mean not going to the disk, which is what a frozen log is for. So reading a `DB` hard
from many goroutines is slower than reading it from one, and that is the design rather than a defect
left in it. What is clear enough to act on is that the two halves of this library behave in opposite ways
under load: a `KeyValueStore` reads faster the more goroutines it is given, and a `DB` reads slower.

## How it scales

Every read figure above comes from a store of about a thousand keys, whose index is a few tens of
kilobytes and never leaves L2. That is the best case, and a store which has outgrown the cache does not
get it: the lookup becomes a hash and a walk out to main memory.

| keys      | 16-byte value | 1 KiB value | 16 KiB value |
| --------- | ------------- | ----------- | ------------ |
| 1,024     | 51 ns         | 142 ns      | 1.62 µs      |
| 16,384    | 66 ns         | 217 ns      | 1.81 µs      |
| 131,072   | 198 ns        | 485 ns      |              |
| 1,048,576 | 451 ns        | 622 ns      |              |

Going from a thousand keys to a million costs about 400 ns at 16-byte values and about 480 at 1 KiB —
near enough the same number of nanoseconds either way, because the miss is a fixed tax on finding the
record rather than a multiplier on reading it. So it is 8.8x the cost of a small read and 4.4x of a
larger one: a store of big values hardly notices what a store of small ones cannot ignore. The blanks
are combinations too large to build in memory to measure.

Cores take most of it back. A lookup waiting on memory leaves its core with nothing to do, and another
core can be waiting at the same time, so the misses overlap instead of queueing:

| keys      | 16-byte value | 1 KiB value |
| --------- | ------------- | ----------- |
| 1,024     | 40 ns         | 47 ns       |
| 1,048,576 | 62 ns         | 107 ns      |

Ten goroutines are worth 1.3x at a thousand keys and 7.2x at a million. The store gets *more* out of the
cores it is given the less of it fits in cache, which is the opposite of the write side above, and the
two together are most of what there is to know before sizing one.

As payload the same matrix spans nearly three hundredfold, from 284 Mbit/s reading 16-byte values out
of a million keys to 81 Gbit/s reading 16 KiB values out of a thousand, and 76 Gbit/s at a million keys
with ten goroutines and 1 KiB values. Which is the argument against quoting any single throughput
number for a key-value store, this one included: nothing about "how fast is it" survives contact with
the question of how many keys, how large, and from how many goroutines.

### What everything else costs

At 4096 keys and 256-byte values, on the same machine:

| operation                     | cost   |
| ----------------------------- | ------ |
| `Read` of a key never written | 17 ns  |
| `View`, 16-byte value         | 42 ns  |
| `Read` of a deleted key       | 47 ns  |
| `Read`, 16-byte value         | 49 ns  |
| `Write`, 16-byte value        | 113 ns |
| `Delete`                      | 123 ns |
| `ForEach` over the whole log  | 87 µs  |
| `Verify`                      | 185 µs |
| `Recover`                     | 400 µs |
| `RebuildIndex`                | 418 µs |
| `Compact`                     | 2.0 ms |

Two of those are worth a second look. A key that was never written costs a third of one that was, since
the answer comes out of the index and no record is ever touched. A deleted key costs what a live one
does, because the tombstone saying so is a record like any other and has to be read to be recognised.
So probing for keys you expect to be absent is cheap, and probing for keys you expect to be deleted is
not — they are different answers with very different prices.

`Delete` and a 16-byte `Write` cost the same to within a few nanoseconds, 123 against 126 measured over
the same 4096 keys: a tombstone is an ordinary record, only a shorter one. Deleting does not reclaim
anything, and until a compaction both the value and the tombstone are still in the log.

`DB` pays more for the walk than a `KeyValueStore` does — 2.4 ms against 87 µs — and not only because
its records are on a disk. `ForEach` there has to return each live key once across several logs, so it
carries a set of the keys it has already seen and asks it about every record it passes.

All of these are an Apple M4 with ten cores. What matters is the ratios; a Raspberry Pi will be slower
throughout and will meet its SD card long before it meets any of these.

## Binary Storage Format

Each record is a header followed by the key and the value. A record with no expiry takes 22 bytes of
header:

| Offset | Size | Field                                        |
| ------ | ---- | -------------------------------------------- |
| 0      | 4    | CRC-32 (IEEE), little-endian                 |
| 4      | 1    | Record version, 2                            |
| 5      | 1    | Record type: 0 = normal, 1 = deleted         |
| 6      | 8    | Timestamp, nanoseconds since the Unix epoch  |
| 14     | 4    | Key length, little-endian uint32             |
| 18     | 4    | Value length, little-endian uint32           |
| 22     | *n*  | Key                                          |
| 22+*n* | *m*  | Value                                        |

One written with an expiry takes 30, with the extra field after the timestamp and the version raised
to 3:

| Offset | Size | Field                                        |
| ------ | ---- | -------------------------------------------- |
| 6      | 8    | Timestamp, nanoseconds since the Unix epoch  |
| 14     | 8    | Expires, nanoseconds since the Unix epoch    |
| 22     | 4    | Key length                                   |
| 26     | 4    | Value length                                 |

A record carrying a sequence number takes another eight, after the expiry when there is one. Version 4
is a numbered record with no expiry, at 30 bytes of header, and version 5 is one with both, at 38:

| Offset | Size | Field                                        |
| ------ | ---- | -------------------------------------------- |
| 6      | 8    | Timestamp, nanoseconds since the Unix epoch  |
| 14     | 8    | Expires, when the version says there is one  |
| 14/22  | 8    | Sequence number, from one                    |
| 22/30  | 4    | Key length                                   |
| 26/34  | 4    | Value length                                 |

All of them sit in the same log and always have — the version byte is how a reader tells them apart, and
it is why each wider layout could be added without orphaning anything. They are versions rather than
fields on every record because most records need neither, and eight bytes on each is not free: the
timestamp alone took an in-memory write from roughly 50 ns to 110. A store that sets neither holds
exactly the bytes it always did, which is every `KeyValueStore` — only a `DB` numbers its records, and
only records given an expiry carry one.

A **batch marker** is an ordinary record in one of those layouts, with the record type set to 2, no key,
and an eight-byte value holding the number of bytes of records that follow it. It needed no layout of
its own, which is why adding write batches did not add a version: every reader already decodes it, and
the only thing that changed is what the log walkers do when they see one. A marker holding a key, a
value that is not eight bytes, or a span that runs past the end of the log is refused like a record
that will not decode — the log ends there.

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

- **A write batch is not a transaction.** It is atomic and durable, and that is the whole of it: there
  are no reads in it, no isolation from a concurrent writer, and nothing to roll back once it is
  written. A batch also has to fit in memory twice over — the caller builds it, and the store holds the
  records it serializes from it — and a reader of a log holds one batch at a time while checking it.
- **Expiry is checked on read, not swept.** Nothing walks the store looking for records whose time has
  come; they stop answering the moment they are due and the space comes back at the next compaction or
  merge. A store full of expired records is as large as a store full of live ones until then.
- **A timestamp costs about 60 ns a write.** Reading the clock is 28 ns of it and the wider header the
  rest, which took an in-memory write from roughly 50 ns to 110. It is invisible against a file, where
  the write itself is microseconds, and it is the price of every record knowing when it was written.
- **Opening still costs the keys.** Hints make it proportional to the number of keys rather than to the
  bytes stored, but every key is still read and indexed before the store answers anything.
- **Every key must fit in memory.** The index holds each key and an offset, about 59 bytes per key,
  however large the values are. Ten million keys is roughly 600 MB of index, and no amount of paging
  values out of memory changes that.
- **A read slows down as the index outgrows the cache**, by about 400 ns between a thousand keys and a
  million. Most of the quoted read figures are from small stores and are the best case. See
  "How it scales"; the short version is that it hurts a store of small values and barely touches one of
  large values, and that concurrent readers get most of it back.
- **A `KeyValueStore` holds every record in memory**, live or superseded, until compaction. A `DB` holds
  only the keys and its active log, which is what to reach for once a store outgrows memory.
- **A range is gathered, not streamed.** `Range` and `Prefix` visit live keys in order, but every log has
  to be asked before the first key can be yielded, so a range over most of a large store holds most of
  its keys while it runs. The index is still a hash map: an ordered index instead of it was measured and
  reverted, since a radix tree took point lookups from a full scan to 214 ns for prefixes but cost 3 to
  4.5x on the lookups that are the whole point. It is in the history at `9e3cf2c`.
- **One writer at a time, and a second one costs.** Writes take every shard of the lock, so they
  serialize with each other, and for a single `KeyValueStore` with compaction as well. Two goroutines
  writing do not merely fail to go faster, they halve the store's throughput — 114 ns a write becomes
  228 — and more of them change little after that. Write from one goroutine, or put a `Writer` in front
  of the store and let it be the one goroutine: see "One writer in front of many callers".
- **Merging still rewrites.** Merging by size bounds the cost at about log₄ of the store rather than
  letting it grow with it, but every record is still rewritten a few times over its life. An append-only
  workload should turn merging off rather than pay for it.
- **Background merging wants a spare core.** With one, merging is the same work in the same place as the
  writes, and the stall it was meant to remove comes back.
- **A closed `DB` cannot read.** Its values are on the disk and closing shuts the files. A closed
  `KeyValueStore` goes on answering, because its records are in memory.
- **Applying a batch to a `DB` follower is at least once.** The records reach the disk before the
  position that claims them, so a crash in between means the same batch arrives twice. The records are
  identical and in the same order, so nothing it holds changes; the bytes are spent twice and a merge
  reclaims them.
- **A `DB` follower can be stranded by a merge, even a caught-up one.** There is nothing like a
  replication slot holding a log open for it. A follower that is behind is reading a frozen log, and a
  merge can take that log; one that has caught up rests wherever it read last, which is the end of the
  last frozen log whenever the log being written is empty — and a merge can take that too. It is only
  safe while the store is being written steadily, because then it rests inside the log being written,
  which is never merged. So the answer is always another snapshot, and a loop that follows a `DB` has to
  be written with that in it.
- **Replication is asynchronous, and only that.** A write returns as soon as the leader has it, so a
  leader that dies loses whatever its followers had not received yet. There is no synchronous or
  semi-synchronous mode, no acknowledgement from a follower, and nothing waits for one. `Reached` lets a
  client refuse a replica that has not got to a write it already knows about, which is a different
  thing: it hides the lag from that client, it does not remove it.
- **There is no failover.** Which store is the leader is your decision and nobody else's: `Promote`
  writes the decision down, it does not make it. Raising the term in two places at once puts two stores
  on the same term and gives the guarantee away, so whatever decides has to be the only thing deciding.
  That is consensus, and it is not here — see `AGENTS.md` for why an external lease is the pragmatic
  answer and why Raft for the data path would replace this work rather than sit on it.
- **A fenced leader has to be told, and only replication tells it.** It cannot know it was replaced;
  the news reaches it when something carrying a newer term asks it for records. Until then it goes on
  taking writes, and those writes are lost when it finds out. Fencing bounds the damage, it does not
  prevent it.
- **A follower is a whole copy.** There is no partial replication, no filtering by key, and no way to
  follow one part of a store. The unit is the log.
- **A replica costs the leader a copy.** Each batch is copied out of `Data` under the read lock before
  it is written to the connection, which is what keeps a slow follower from blocking writes. Ten
  followers catching up at once is ten copies.
- **One process owns a directory, and nothing enforces it.** There is no lock file. Two programs with
  the same store open — two `litekvd`s on the same `-dir`, or a binary and a shell script — will write
  over each other's log, and the first either of them hears of it is a checksum that does not match.
- **The HTTP API has no authentication and no TLS.** `litekvd` listens on loopback for that reason. Put
  it behind a proxy or on a private network before it has an address a stranger can reach, and note that
  this applies to every route: there is nothing to stop a client writing as well as reading.
- **The empty key has no spelling in a path.** The store holds it happily and a path wildcard will not
  match an empty segment, so `/v1/keys/` is not a route and the single-key routes cannot reach it. A
  batch line writes it and a range hands it back, which is where it went rather than a way around this.
- **A range over HTTP is capped and cannot be paged through cheaply.** `-max-scan` bounds what one
  request answers with, and a client walks a large store by asking again from the last key it saw —
  which starts the gather over, since the engine has nowhere to resume from. The cap is a count and not
  a number of bytes, so a store of large values wants a smaller one than a store of small ones.

## Working on it

`AGENTS.md` has the notes for that: the invariants that must not be broken and
the test behind each, the chaos sweeps and what they found, the mistakes this
codebase has already made, what has been
measured and rejected so it is not tried again, and how to check a change before
pushing it.

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

That is one sample of each, which is enough to see a number and not enough to trust one. For that:

```bash
go run ./benchrun                   # ten passes of everything, then benchstat
go run ./benchrun -passes 5         # five of them
go run ./benchrun -bench ReadScale  # only what matches a regexp
```

It runs the whole suite start to finish, several times over, rather than repeating each benchmark ten
times where it stands. The difference is not pedantry: `go test -count=10` finishes one benchmark before
starting the next, so a machine that warms up over the session gives its early benchmarks a cold clock
and its later ones a hot one, and the drift lands as a bias inside each result. Alternating spreads every
benchmark's samples across the whole session, so the same drift lands as noise in all of them, where
benchstat can see it and say so. It keeps the raw samples, so two runs can be compared:

```bash
benchstat bench/old.txt bench/new.txt
```

For a `KeyValueStore`: reads and writes at 16 bytes, 1 KiB and 64 KiB, reading with and without copying,
the parallel read paths, reads across four orders of magnitude of key count and three of value size,
writes from one to eight goroutines, compaction, and index rebuilds.

For a `DB`: reading from the active log against reading from a frozen one, writing with logs rotating
under it against writing with a log large enough that they never do, merging, and opening with the hints
against opening without them.

And the cost of each sync policy, which is the one number worth knowing before choosing one.

## Fuzz Testing

```bash
go test -run xxx -fuzz FuzzKeyValueStore_Data
```

CI gives each target thirty seconds on every push, which catches a seed that has stopped passing but
finds little on its own: the corpus a long run builds up lives in the local build cache, not in the
repository, so CI starts from the seeds in the code every time. Finding anything new means running one
for minutes on a laptop. A failure writes the input that caused it into `testdata/fuzz`, where it
becomes a regression test and travels with the code.

Every target points at something that reads bytes it has no reason to trust.

`FuzzKeyValueStore_Data` feeds arbitrary bytes in through the `Data` slice, which is how a store backed
by a file or by shared memory is restored: no input may panic, hang, or make the store forget a key it
had already returned. `FuzzSegmentBytes` does the same for the half that reads a log without holding it
in memory, and holds the streaming indexer and the reader that fetches one record by offset to each
other: an offset the one accepted, the other has to be able to read, with the key it was indexed under.
`FuzzHint` feeds arbitrary bytes to the hint parser, where refusing is always allowed and accepting an
offset outside the log is not, since a hint is taken at its word. `FuzzKeyValueStore_WriteReadDelete`
fuzzes the write path.

The rest are replication, which is the part that reads bytes off a wire. `FuzzApply` hands a single
store arbitrary bytes as a batch; `FuzzDBApply` and `FuzzDBApplySnapshot` do the same to a `DB`
follower, where the thing that must never happen is a store claiming a position it does not hold the
records for. `FuzzDBSince` goes the other way and hands a leader arbitrary positions, which is what a
follower that has been tampered with sends: refusing is always allowed, but whatever does come back has
to be whole, verified records. `FuzzDBPosition` feeds the position parser itself.

Name a target exactly. `-fuzz FuzzApply` now matches `FuzzApplySnapshot` as well, and the go tool
refuses to run rather than pick one:

```bash
go test -run xxx -fuzz '^FuzzApply$' -fuzztime 60s
```
