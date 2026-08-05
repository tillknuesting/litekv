# Notes for whoever works on this next

The README says what the store does and what it costs. This says what is easy to
break, what has already been tried, and how things are checked here. It is meant
to save you a day, not to introduce the code.

## Where things are

| file         | owns                                                                     |
| ------------ | ------------------------------------------------------------------------ |
| `kv.go`      | `KeyValueStore`: the record format, the in-memory store, the sharded lock |
| `file.go`    | durability for one store: `Log`, `Open`, `Attach`, sync policies          |
| `db.go`      | `DB`: segments, rotation, merging by size                                 |
| `segment.go` | the two kinds of segment, and reading a log without holding it in memory  |
| `hint.go`    | the index of a frozen log, written beside it                             |
| `bloom.go`   | the filter in front of that index, once a log is big enough to want one   |
| `replica.go` | `Position`, and shipping the log to a follower: `Since`, `Follow`, `Apply` |
| `dbreplica.go` | `DBPosition`, and shipping a `DB`'s records: `Snapshot`, `Since`, `Follow` |
| `fs.go`      | the one seam through which this package touches a disk                    |

`KeyValueStore` and `DB` are deliberately separate. The first is one log with
everything in memory and `Data` as a public byte slice people are expected to
hand around. The second is several logs with only the keys in memory. Do not
merge them: the whole point of the first is that it is simple and has no files
in it unless you ask for them.

## Verifying a change

Everything below has to pass before anything is pushed. It is not optional; most
of the bugs listed further down were found by one of them.

```bash
gofmt -l .            # must print nothing
go vet ./...
staticcheck ./...     # has earned its keep here, twice
go test -race ./...
GOMAXPROCS=1 go test ./...   # the lock degrades to one shard; that path is real
go run ./example      # it exercises every exported call
go test -run xxx -fuzz FuzzSegmentBytes -fuzztime 30s .
go test -run xxx -fuzz FuzzApply -fuzztime 30s .   # what arrives over a wire
```

`GOMAXPROCS=1` is not paranoia. The lock shards on `GOMAXPROCS`, so a one-core
machine takes a different path through it, and background merging stops being in
the background.

## Invariants. Break these and the store loses data

Each of these has a test behind it. If you change the code near one, read the
test first, and if you think the invariant is wrong, be sure before you decide
that.

**A record reaches `Data`, then the log, then the index.** In that order, in
`appendRecord`. The index is pointed at a record only once both have taken it,
so a log that fails leaves the store exactly as it was rather than half applied.

**A merge takes a contiguous run of logs.** Lookups go newest to oldest and stop
at the first answer, so the order logs are asked in is the only thing deciding
which version of a key wins. Merging across a gap puts records of different ages
in one log and there is no way to put them back in order.

**A tombstone may only be dropped by a merge that reaches the oldest log.**
Anything older that was left out of the run can still hold the value the
tombstone hides. Dropping it brings a deleted key back to life.
`TestDBTieredKeepsTombstones`.

**A merge renames over the oldest log it replaces, then removes the rest oldest
first.** At every point in between, what is on disk is the merged log plus the
newest few of its inputs, which are asked first and answer correctly, with
anything they do not hold falling through to the merged log.
`TestDBMergeInterrupted` stops the removals at each point and checks all of them.

**A hint is removed before the log it describes is replaced.** A hint is taken
at its word, so one left beside a different log is the only way a wrong answer
could survive everything else.

**A read that fails is not a log that ends.** Bytes that are not records, or a
log stopping mid-record, are a torn tail, and the answer to a torn tail is to
cut the log back to it. A disk that will not hand the bytes over is a different
thing, and answering it the same way deletes what could not be read. This was
real: one refused read per log truncated all thirteen logs to zero and reported
a healthy, empty store. `endOfLog` in `segment.go` is the whole fix.

**A follower's position is checked before a leader sends it anything.** An
offset alone cannot say which log it is an offset into, and two stores of the
same length holding different records would otherwise be spliced into one log
that decodes perfectly and answers wrongly. `Position` carries where its last
record starts and that record's checksum, and `batch` verifies both against its
own log or answers `ErrorDiverged`. `TestReplicaDiverged` covers the three ways
it happens: a follower with a history of its own, a leader that compacted, and a
follower ahead of its leader.

**A follower verifies every record before keeping any of it.** `applyWhole`
decodes and checksums the whole batch first, and a record it will not vouch for
stops it there. A leader is not a reason to trust the wire, and a record kept
without checking is one no later read can question. `FuzzApply` is the same
claim against arbitrary bytes.

**Every position a `DB` hands out names a record.** The check a position carries
needs a record to check against, so the start of a log — which names nothing —
cannot be checked, and a frozen log may have been merged and be a different file
behind the same name. `db.batch` goes out of its way for this: it crosses into
the next log rather than stopping at a boundary, and it stays at the end of a
log rather than stepping to the start of an empty one. The single exception is a
snapshot of a store whose active log is empty, which is refused if that log has
since frozen. `TestDBSnapshotOfAnEmptyStore` is the exception,
`TestDBTailCrossesFrozenLogs` is the rule.

**A `DB` follower writes the records before the position that claims them.**
Crashing in between means the same batch arrives again, which is the same
records in the same order and changes nothing it holds. The other order claims
records that were never written, and that is the one that loses data. It is an
ordering, so no call's result shows it: `TestDBFollowerWritesRecordsBeforeThePosition`
watches it through the seam in `fs.go`, on both the batch path and the snapshot
path, because a mutation that swapped the order in one of them went unnoticed
while only the other was watched.

**A `DB` snapshot freezes before it reads.** Everything it covers is then on the
disk and cannot change, and the position it reports is the end of the log it
just froze — not the start of the new one, which would name no record and be
unusable the moment that log filled. It holds `mergeMu`, not `db.mu`: writes and
rotation carry on, and only merging waits. Holding `db.mu` instead would stall
writes, because Go's RWMutex blocks new readers once a writer is waiting and
`db.Write` takes it for reading while rotation takes it for writing.
`TestDBSnapshotKeepsWritingCheck`.

**A follower that crashed says where it got to, and the leader takes it.** A
batch is one write, so losing power part way through leaves a record half on the
disk. Opening drops that tail and truncates the file, and the position that
comes back has to be one the leader will carry on from — a follower that had to
start again from empty after every crash would be no follower at all.
`TestReplicaSurvivesACrashMidBatch` tears a batch at the end of a catch-up and
`TestReplicaCrashWithMoreToCome` tears one in the middle of one; both assert
that the resync count is zero.

**Nothing marks a store read-only, and nothing needs to.** `Apply` takes the
position the batch was cut for and refuses when the store is somewhere else, so
a write of a follower's own — or a batch that arrived twice — is caught by the
position rather than by a mode flag. `TestReplicaWrongPosition`.

**`Position` is allowed to be slow, never wrong.** `lastRecord` is where the
last record starts, and every path that changes `Data` moves it. If one forgets,
`position` finds out — it checks the record there against the end of `Data` and
reads the log when it does not fit. That is deliberate: this is the shape of the
cached-search-order idea rejected below, and the fallback is what makes it safe
to keep. `TestPositionTracksTheLog` corrupts the shortcut on purpose after every
operation that moves the log.

**The record offsets live only in `decodeHeader`.** They were in three places
once and a format change left two of them reading the wrong bytes, compiling
perfectly while dropping the entire store on compaction.

**Lock order is `mergeMu` then `db.mu`, never the reverse.** And the logs a
merge will touch are chosen while `mergeMu` is held: choosing first and locking
after lets a background merge delete them in between.

**`freeze` opens the read handle before closing the store.** The other way
round, a failure in between leaves the active log closed and the store unable to
take another write.

**`Write` reports whether the record is stored, and nothing else.** Rotating a
full log is housekeeping that happens after the record is safe; a failure there
is remembered in `db.rotateErr` and reported by `Sync` and `Close`.

## The replication tests were checked by breaking the code

Tests that pass on the first run have said nothing yet. Each of these edits was
made to `replica.go` on purpose, the replication tests were run, and the edit
was reverted. All twelve are caught. If you change that file and want to know
whether the suite still has hold of it, this is the list to work through again.

| the code, broken                                              | caught by                             |
| ------------------------------------------------------------- | ------------------------------------- |
| Leader skips the checksum in the position check                | `TestReplicaDiverged`                 |
| Leader ignores where the follower says its log ends            | `TestReplicaDiverged`                 |
| Leader misses a follower that is ahead of it                   | `TestReplicaDiverged`                 |
| A torn tail is streamed to followers                           | `TestPositionIgnoresATornTail`        |
| `Position` gives up instead of reading the log                 | `TestPositionTracksTheLog`            |
| Batch cut off by one                                           | `TestReplicaBatchEndsOnARecord`       |
| Follower does not verify record checksums                      | `TestReplicaRejectsDamagedBatch`      |
| Follower does not check the batch continues its log            | `TestReplicaWrongPosition`            |
| Follower treats a partly arrived record as whole               | `TestReplicaTruncatedBatch`           |
| Follower drops the unapplied tail between reads                | `TestReplicaModel`                    |
| Follower keeps the first record for a key rather than the last | `TestReplicaSupersededAcrossBatches`  |
| Follower appends to the log but not to the index               | `TestReplicaCatchUp`                  |

Three of those are worth knowing about beyond the list.

**Two checks in `batch` look redundant and are not.** The checksum makes the
offset comparisons look like belt and braces, and for an intact log they are.
The case they exist for is a log with a torn tail: whole, correctly checksummed
records can lie *beyond* bytes that do not decode, because a crash tore a hole
in front of them rather than at the end. A position naming one of those parses,
ends where it says, and carries a checksum that genuinely matches — and only
`pos.Offset > here.Offset` catches it. Removing that check broke no test until
one was written for exactly that shape. Both subtests are in
`TestReplicaDiverged` and both say in their comments why they are contrived.

**A broken replication path makes the suite slow before it makes it fail.**
Several tests wait on a deadline for an asynchronous follower to arrive — which
is the only honest thing to do, since there is nothing to assert about how long
that takes — so a change that stops followers converging burns those deadlines
before anything reports. If the suite suddenly takes half a minute longer, read
the failures rather than the clock.

**`lastRecord` is the one thing no mutation can reach.** Failing to move it is
invisible by design: `position` falls back to reading the log, so the answer
stays right and only the speed goes. That is the trade, and it is why the
invariant above is phrased as slow-never-wrong rather than as a rule to follow.

## Traps this codebase has already sprung

- **Hard-coded header offsets.** Bit twice. Ask `decodeHeader`.
- **Timing assertions in tests.** `TestCompactionStall` once failed CI at 193 ms
  against 75 ms, which said nothing about the store. Measure and log; do not
  assert a latency.
- **Benchmarks that measure the allocator.** The original suite appended
  gigabyte values in a `b.N` loop without resetting, so it measured `append` and
  ran a small machine out of memory. It came back: `WriteDurability/memory` was
  still unbounded and still appending about a gigabyte a sample, which is why it
  read 158 ns alone and 190 ns after a benchmark that had built a large store,
  and why it was the noisiest number in the suite at ±12%. Bounded, it is 119 ns
  at ±1%, and agrees with `WriteUpdate/16` next door, which it never had. If a
  write benchmark has no `len(kvs.Data) > 1<<26` in it, that is a bug.
- **`HeapAlloc` deltas under-report.** They once claimed a 100k-entry map costs
  10.9 bytes a key. Count the structure, or use `TotalAlloc` over a preallocated
  build.
- **A benchmark that probes too few distinct keys measures nothing.** Deciding
  whether a Bloom filter beat the index took three attempts, and the first two
  gave opposite answers, both wrong. Probing one absent key said the filter won
  by a quarter; four thousand keys said it lost by 8%; probing as many keys as
  the store held said it won by 2.6x. What decides whether a map lookup goes to
  memory is not how big the map is but how much of it the workload touches, and
  a small probe set keeps the buckets in cache however large the map. If a
  benchmark is about cache, count the distinct keys it touches before believing
  it.
- **Benchmarks run back to back drift.** Two toolchains looked 6-8% apart until
  they were interleaved, at which point they were identical. That was the
  laptop warming up. Alternate the runs.
- **An option that documented the opposite of what it did.** `MergeTrigger: 1`
  said it turned merging off and turned it up: `pickMerge` takes any run of at
  least the trigger, so one took every pair of logs of a size and merged more
  eagerly than the default two did. Nothing caught it because every benchmark
  in `db_test.go` asks for `1 << 30` instead — the workaround was in the tests,
  which is the shape of a bug that survives. It was found by a replication test
  wondering why a log had vanished with merging supposedly off.
  `TestDBMergeTriggerBelowTwoDisablesMerging` now holds the option to what it
  says, and zero-means-default with negative-means-off matches `BloomMinKeys`
  next door.
- **A follower asking whether there is more, through the write lock.**
  `Changed` handed out its channel under `kvs.Lock`, which takes every shard, so
  a follower waiting on a store stood in the same queue as the writers and took
  a write from 105 ns to 165. It is an `atomic.Pointer` now, and 138. The write
  lock is not a general-purpose lock in this package; anything a follower calls
  often does not belong behind it. `BenchmarkWriteWithAWaiter` is the number.
- **A buffer sized to the maximum, allocated per call.** `Apply` reads into a
  buffer bounded by `BatchSize`, and allocating it at that size on the way in
  cost 2 MB and 79 µs for a batch holding one 46-byte record — a 200x
  regression on the version it replaced, and invisible to every test. It grows
  into what is arriving instead, and both ends keep their buffers in a
  `sync.Pool`, as `segment.go` does for the same reason. If a replication
  benchmark reports more than a few hundred bytes an op, a buffer is being
  allocated per call again.
- **`crc32` leaks its argument to the heap.** Handing it a stack array put that
  array on the heap and cost an allocation on every read and write. The header
  is folded a byte at a time for that reason; do not "simplify" it back.

## Already measured, already rejected

Do not spend a day rediscovering these. Numbers are from an Apple M4; what
matters is the ratios.

| tried                                     | result                                                  |
| ----------------------------------------- | ------------------------------------------------------- |
| Radix tree index, then adaptive (ART)      | 3-4.5x slower lookups; reverted. History at `9e3cf2c`   |
| B-tree index                               | 1.6-1.7x slower than the radix tree, so worse still     |
| 16 inline labels in the ART node           | 20 B/key cheaper, 70% slower lookups                    |
| `bytes.IndexByte` for that node's labels   | the call costs more than the vector compare saves        |
| `make`+`copy` versus `append` for a value  | identical                                               |
| Contiguous CRC on the write path           | ~9 ns cheaper, but moves a full pass over the value      |
|                                            | inside the write lock. Not worth it for large values     |
| Custom open-addressed hash index           | ties Go's map on speed at half the memory; not adopted   |
|                                            | because a map is zero code and already fast              |
| Caching the DB search order beside the     | not tried on purpose: it has to track active and frozen  |
| segments                                   | exactly, they change in four places, and the copy would   |
|                                            | be a fifth. searchOrder yields instead, which allocates   |
|                                            | nothing and cannot go stale                              |
| Bloom filters, on the grounds that a       | wrong, and adopted instead. No I/O is saved, but the      |
| frozen miss never touches the disk         | filter is 40x smaller than the index and stays in cache   |
|                                            | when it does not: a miss over 256k-key logs went 250 ns   |
|                                            | to 87. Below ~4k keys a log it costs ~8%, hence the       |
|                                            | threshold. See BenchmarkDB_BloomThreshold                 |
| Blaming the DB's parallel read collapse on | wrong. Removing it made a serial active read a third      |
| the per-read allocation                    | faster and moved the parallel table not at all. It was    |
|                                            | the two system calls a frozen read made; a page of        |
|                                            | read-ahead took 5.2 µs to 3.5                            |

The index is a Go map on purpose. If you are about to replace it, read the
"Limitations" section of the README first, then measure against the map before
writing anything.

## How measurement is done here

Every number in the README came from a benchmark or a test in this repository,
on a quiet machine, and the test that produced it is still there. If you change
something that moves one of those numbers, re-run it and change the number.

Use `go run ./benchrun` rather than `go test -bench . -count=10`. It runs the
whole suite ten times over instead of running each benchmark ten times where it
stands, which is what keeps the machine warming up over the session out of the
individual results — see the drift entry below. It prints the load average it
started under, and leaves the raw samples in `bench/` for benchstat to compare.

Not every number is equally solid, and the suite says which. Where a store is on
its way out of L2 — the middle rows of `ReadScale`, around 16k to 131k keys —
a row is tight within a session and moves 10 to 20% between them. Three
consecutive runs of untouched code gave 178, 156 and 186 ns for the same row.
Compare those only within a session, and do not go hunting for what changed
between two of them.

For anything about durability or ordering, use the seam in `fs.go`. It records
every open, write, sync, truncate, close, rename, remove, list and read in
order, and can be told to fail any of them, or to run out of room part way
through a write. `fs_test.go` has the harness. Orderings are most of what makes
a crash survivable and none of them show up in the result of a call.

## Open

- **One fuzz run failed and was never explained.** No reproducer was written,
  and it has not recurred in tens of millions of executions. If it returns it
  will leave the input in `testdata/fuzz`, and that is the thing to chase.
- **`os.Stat` and directory syncing** still go straight to the OS in one or two
  places. Everything else goes through `fs.go`.
- **The fuzz corpus lives in the local build cache**, not the repository. CI
  starts from the seeds in the code every time, so its thirty seconds a target
  is a smoke test. Real coverage comes from running one for minutes locally.

## What to build next, and what it needs

Replication is finished in both halves, for a single store and for a `DB`. What
is left is the operational apparatus around it, and none of it is small.

**A replication slot**, or something like one. A follower that falls behind far
enough for the log it was reading to be merged away has to take a whole new
snapshot, and nothing here holds a log open for it. That is also the narrow
window a follower resting at the end of a frozen log sits in. PostgreSQL's
answer is a slot, and it costs the leader unbounded disk when a follower dies
quietly, which is why it is a decision rather than an obvious improvement.

**Semi-synchronous replication.** Everything here is asynchronous: a write
returns as soon as the leader has it, and a leader that dies loses whatever its
followers had not received. The book is pointed about this. It needs an
acknowledgement from a follower, which needs the leader to know its followers,
which is the first piece of state the leader does not currently keep.

**Reads that are not stale.** `Position` is the right primitive for
read-your-writes and monotonic reads — take the leader's position after a write
and refuse a follower behind it — and none of it is built.

Failover is a different project again: it needs a way to agree on who the leader
is, which is consensus, and this is a storage engine.

Smaller: expiry, now that records carry a time; or a batch write, which needs a
commit marker in the format to be atomic across a crash.

## Conventions

Work happens on `main`. No branches, no pull requests; this is a single-author
repository and the round trip buys nothing. Verify before pushing, since there
is no review stage to catch anything.

Commit messages here say what changed and why, in prose, and give the numbers
when there are numbers. `git log` is where the reasoning for most decisions
lives.
