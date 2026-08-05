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

Nothing in the library opens a socket. What crosses the gap is a `Position`, which marshals to twenty
bytes, and a run of records; carrying those over TCP, HTTP, a pipe or a file is yours to do. A record
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
a `DB` followed by another further down. `tcp_test.go` is the same thing over a real loopback socket,
with framing, a connection broken part way through and a reconnect — the only place that says any of
this survives a wire rather than a pipe.

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

**A follower must still always be able to take another snapshot.** A hold covers a follower that is
connected. One that was away while the merging happened, or whose hold was released, is stranded exactly
as before, and the loop that follows a `DB` has to be written with that in it. The one in `example/` is.

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
- **A read slows down as the index outgrows the cache**, by about 400 ns between a thousand keys and a
  million. Most of the quoted read figures are from small stores and are the best case. See
  "How it scales"; the short version is that it hurts a store of small values and barely touches one of
  large values, and that concurrent readers get most of it back.
- **A `KeyValueStore` holds every record in memory**, live or superseded, until compaction. A `DB` holds
  only the keys and its active log, which is what to reach for once a store outgrows memory.
- **No range or prefix queries.** The index is a hash map, so keys have no order. A radix tree that gave
  ordered traversal was measured and reverted: prefix queries went from a full scan to 214 ns, but point
  lookups cost 3 to 4.5x more, which was the wrong trade here. It is in the history at `9e3cf2c`.
- **One writer at a time, and a second one costs.** Writes take every shard of the lock, so they
  serialize with each other, and for a single `KeyValueStore` with compaction as well. Two goroutines
  writing do not merely fail to go faster, they halve the store's throughput — 114 ns a write becomes
  228 — and more of them change little after that. Write from one goroutine.
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
  semi-synchronous mode, no acknowledgement from a follower, and nothing waits for one.
- **There is no failover, and no fencing.** Which store is the leader is your decision and nobody
  else's. A follower promoted by hand is just a store you start writing to. Two of them written to at
  once diverge, and the divergence is *reported* rather than resolved — a follower will refuse to splice
  one log onto another, so nothing is corrupted, but writes acknowledged by the wrong leader are
  discovered to be worthless and thrown away. Nothing here carries a term, so nothing can tell a leader
  it has stopped being one. `AGENTS.md` has what fencing would take, and why it comes before consensus
  rather than after.
- **A follower is a whole copy.** There is no partial replication, no filtering by key, and no way to
  follow one part of a store. The unit is the log.
- **A replica costs the leader a copy.** Each batch is copied out of `Data` under the read lock before
  it is written to the connection, which is what keeps a slow follower from blocking writes. Ten
  followers catching up at once is ten copies.

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
