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
- **Benchmarks run back to back drift.** Two toolchains looked 6-8% apart until
  they were interleaved, at which point they were identical. That was the
  laptop warming up. Alternate the runs.
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

**Replication by shipping the log** is the natural one. The log is already an
ordered, checksummed, immutable stream of records with timestamps, so a follower
needs little more than "send me everything from offset N". The timestamp and
version fields were added with this in mind. It brings networking into a library
that has none, which is the thing to think about before starting.

Smaller: expiry, now that records carry a time; or a batch write, which needs a
commit marker in the format to be atomic across a crash.

## Conventions

Work happens on `main`. No branches, no pull requests; this is a single-author
repository and the round trip buys nothing. Verify before pushing, since there
is no review stage to catch anything.

Commit messages here say what changed and why, in prose, and give the numbers
when there are numbers. `git log` is where the reasoning for most decisions
lives.
