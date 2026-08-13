# Notes for whoever works on this next

The README says what the store does and what it costs. This says what is easy to
break, what has already been tried, and how things are checked here. It is meant
to save you a day, not to introduce the code.

## Where this is going

This is the storage engine. The database that sits on it exists and is a
repository of its own —
[litekvd](https://github.com/tillknuesting/litekvd) — which is what this section
spent a long time working towards and what the split finally made true: the
engine is a library anybody can take, and the server is somebody's dependency
rather than a directory in here. The engine is still where nearly all of the
code and all of the risk is, so what follows stays as written:

- **Read scaling and failover stop being theoretical.** For an embedded store
  the readers are in the same process and a replica buys little. For a server
  the readers are clients, and a replica behind a load balancer is the ordinary
  way to serve more of them — and a database that cannot survive losing a node
  is a hard sell.
- **Keep the engine free of a wire.** Nothing in this module opens a socket and
  nothing should; the daemon owns the protocol. This is not a style rule. It is
  what keeps the store embeddable, and it is also the thing that makes the
  daemon's tests worth anything — they exercise the exported API and nothing
  else, so a change that breaks a caller breaks them. If the daemon turns out to
  need something the engine does not export, that is a separate, deliberate
  commit here and a new tag, not a quiet widening while building something else.
  The split made this cheap to check: `go list -deps` on this module names no
  `net/*` package at all.
- **Format changes are cheapest now.** A batch commit marker: anything touching
  the record layout costs less before there is data anyone minds losing. The
  version byte exists for this, and has now carried three changes — the
  timestamp, the expiry, the sequence number — each costing compatibility work.
  Decisions in the daemon can wait; on-disk ones cannot.

## Where things are

| file         | owns                                                                     |
| ------------ | ------------------------------------------------------------------------ |
| `kv.go`      | `KeyValueStore`: the record format, the in-memory store, the sharded lock |
| `file.go`    | durability for one store: `Log`, `Open`, `Attach`, sync policies          |
| `db.go`      | `DB`: segments, rotation, merging by size                                 |
| `segment.go` | the two kinds of segment, and reading a log without holding it in memory  |
| `batch.go`   | write batches: the marker, and what makes one all or nothing             |
| `writer.go`  | one writer goroutine in front of many callers, and group commit          |
| `hint.go`    | the index of a frozen log, written beside it                             |
| `bloom.go`   | the filter in front of that index, once a log is big enough to want one   |
| `order.go`   | ranges and prefixes, over an index that has no order                     |
| `replica.go` | `Position`, shipping the log to a follower: `Since`, `Follow`, `Apply`, and `Reached` |
| `dbreplica.go` | `DBPosition`, shipping a `DB`'s records: `Snapshot`, `Since`, `Follow`, and `Reached` |
| `fs.go`      | the one seam through which this package touches a disk                    |
| `lock_flock.go` | the lock that makes one process the owner of a directory, where there is one |
| `lock_none.go`  | and where there is not: the platforms that open without it               |

And beside it:

| file            | owns                                                            |
| --------------- | --------------------------------------------------------------- |
| `tools/mutate/` | the mutation sweep, and `mutations.go` beside it is what it breaks |

The HTTP server, the replication endpoint and the `litekvd` binary used to be
`server/` and `cmd/` here. They are
[litekvd](https://github.com/tillknuesting/litekvd) now, which depends on this
module the way anybody else would.

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
go test -run xxx -fuzz '^FuzzApply$' -fuzztime 30s .     # what arrives over a wire
go test -run xxx -fuzz '^FuzzDBApply$' -fuzztime 30s .   # and into a DB
```

The `^...$` matters: `-fuzz FuzzApply` now matches `FuzzApplySnapshot` too, and
the go tool refuses to run rather than choosing.

Anything with a test behind it also gets mutation tested. `tools/mutate` is the
sweep — eight mutations, eight workers, and it prints each verdict as it lands
rather than at the end:

```bash
go run ./tools/mutate          # all of them
go run ./tools/mutate lock     # only those whose name matches
```

Eight is not many, and the reason is worth knowing rather than reading as
neglect: a hundred and nine of them went to the daemon with the code they broke.
What is left is the directory lock, which is the only engine work done since the
sweep was built. **Everything older in this file that says a mutation was caught
was checked by hand, before the tool existed, and those runs are written up
below per feature — thirteen for the numbering, thirteen for batches, seven for
the writer.** Adding to `mutations.go` when you change the engine is how that
stops being true.

Each mutation names a file relative to the repository root and the tool runs
`go vet` and then the tests for whatever package that file is in.
`suiteTimeout` is ten minutes, which is generous on purpose: the suite takes
forty-five seconds and eight of them at once on one machine is not eight times
as fast. Lowering it to make a sweep finish sooner is the worst thing that can
be done to this tool, because a test binary killed by the deadline exits
non-zero exactly like a failing one — every mutation would report caught and the
sweep would be testing nothing. A deadline that fires is reported as itself
unless the dump names a test it hung.

It used to be an ad-hoc script in a scratch directory, rewritten from memory
each time, which is how two of the traps below were discovered twice. Then it
was Python, which is how the repository came to have two languages in it for one
tool. The mutations themselves are in `tools/mutate/mutations.go`; adding one is
six lines and is part of writing the code, not a thing to come back to.

Being Go, it is checked by everything above — `gofmt`, `go vet` and `go build
./...` cover it, so a tool that no longer compiles is caught by the same command
that catches a store that no longer compiles, rather than by somebody running a
sweep three weeks later.

A change to replication or to anything the daemon leans on wants the daemon's
suite run too, in its own checkout, against this working tree:

```bash
cd ../litekvd && go mod edit -replace github.com/tillknuesting/litekv=../litekv \
  && go test -race ./... ; go mod edit -dropreplace github.com/tillknuesting/litekv
```

That is the only way a change here is checked against a real client before it is
tagged, and there is no single command that runs both suites. The daemon's own
`AGENTS.md` has what its two-process runs look like — a leader and a follower,
the follower killed and restarted mid-stream — including the two traps that
matter when comparing two stores: count what `ForEach` yields and not `Len`, and
stop both with a signal rather than a kill.

`GOMAXPROCS=1` is not paranoia. The lock shards on `GOMAXPROCS`, so a one-core
machine takes a different path through it, and background merging stops being in
the background.

## Invariants. Break these and the store loses data

Each of these has a test behind it. If you change the code near one, read the
test first, and if you think the invariant is wrong, be sure before you decide
that.

**A replaced leader hears about it from `Follow` as well as from `Since`.**
Streaming and polling are two ways of asking a store for records, and for a
while only one of them wrote down a newer term — so a leader with a follower
attached, which is the ordinary arrangement and the one a server uses, went on
taking writes after being superseded, and those writes are lost when it finds
out. `TestFollowFencesALeaderTheWaySinceDoes` runs both calls through the same
assertions for exactly that reason: the answer to "who is the leader now" must
not depend on which call was used to ask. It was found from the server side and
fixed here, which is the shape most engine bugs of this kind will have — the
caller notices, the invariant belongs to the store.

**A directory is locked before it is read, and unlocked after the last log is
closed.** Both ends matter and for the same reason. Two opens that each list the
directory and only then discover each other have already both decided which log
to carry on writing to, and they have decided the same one; a store that let go
of the lock before its last sync hands the directory over mid-sentence.
`TestTheLockIsTakenBeforeTheDirectoryIsRead` and
`TestTheLockIsReleasedAfterTheLastLogIsClosed` watch the order through the seam
rather than the result.

**An open that fails gives the lock back.** There is nobody to give it back
later — `OpenDB` returned no store to close — so a lock kept on a failure is a
directory this process has shut against its own next attempt, and the symptom
shows up at that next attempt pointing at nothing.
`TestAFailedOpenLetsGoOfTheLock` sweeps every operation an open makes and
reopens after each one.

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

**A log that will not be removed is emptied.** A file the store has forgotten is
a file the next open reads back, and there is nowhere in the ordering it can
safely land: a merge names its output after the *oldest* log of the run, so a
victim whose removal failed keeps a *higher* id and is asked first. That is
harmless while the leftover is the newest thing the merge covered — and stops
being harmless at the second merge, when the output takes the oldest id again
and climbs back over the leftover in age while the leftover's id stays where it
is. Nothing notices, because the list in memory is right and only a restart reads
the directory. `TestDBReadsNothingLeftBehindByAMerge` is that in six writes; the
randomised chaos run found it in four hundred.

So `mergeLocked` and `Reset` empty what they cannot remove, with `emptyLog`,
which truncates and syncs. Everything a merge would remove is already in the
merged log, and everything `Reset` would remove is about to be replaced by a
snapshot, so there is nothing in emptying either to regret.

**A merge renames over the oldest log it replaces, then removes the rest oldest
first — and stops at the first one it can neither remove nor empty.** At every point in
between, what is on disk is the merged log plus the newest few of its inputs,
which are asked first and answer correctly, with anything they do not hold
falling through to the merged log. `TestDBMergeInterrupted` stops the removals
at each point and checks all of them.

The stopping is the part that was missing. The loop ignored a refusal and
carried on, so a log that would not go was skipped and a newer one removed
instead, leaving an *older* input in front of the merged log — answering with
records the merge superseded, including a tombstone it dropped, which brings a
deleted key back on the next open. Nothing says so.

Emptying dissolves that: a log with nothing in it is not in front of anything, so
the removals carry on past one they could empty. Stopping is what is left for a
log that can be neither removed nor emptied, which is intact and older than the
logs after it. `TestDBMergeSurvivesARefusedRemoval` and
`TestDBMergeStopsWhenItCanNeitherRemoveNorEmpty`.

**A merge opens its output before renaming it into place.** Opening it
afterwards leaves a failure with the file already swapped over the oldest victim
while `db.frozen` still holds that victim's segment: an index describing a file
that no longer exists, whose offsets land on whatever the merge happened to put
there. Lookups come back with older values, or with a tombstone, and every
checksum passes because the records are real — just not the ones asked for.
`TestDBMergeFailureDoesNotStrandAnIndex`.

**A hint is removed before the log it describes is replaced.** A hint is taken
at its word, so one left beside a different log is the only way a wrong answer
could survive everything else.

**A read that fails is not a log that ends.** Bytes that are not records, or a
log stopping mid-record, are a torn tail, and the answer to a torn tail is to
cut the log back to it. A disk that will not hand the bytes over is a different
thing, and answering it the same way deletes what could not be read. This was
real: one refused read per log truncated all thirteen logs to zero and reported
a healthy, empty store. `endOfLog` in `segment.go` is the whole fix.

**An expired record is a tombstone until something may drop it.** It says there
is no value, and an older record for the same key may be sitting in an older
log, so dropping it early brings that older value back. `Compact` may drop it
outright — one log has nothing older anywhere — and a merge may only drop it
when the run reaches the oldest log, exactly as for a tombstone.
`TestDBTieredKeepsExpiredRecords` is `TestDBTieredKeepsTombstones` with an
expiry, and merging everything is not enough to test it: only a run that stops
short can tell the two rules apart.

**Whether a record survives compaction is `latestOffsets`' decision alone.** The
same test used to sit in `Compact`'s scan as well, and the two hid each other:
removing either one left the other doing the work, so a mutation of either
survived. One place, and the scan asks the map.

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

**The term heard of is written down, not only the term held.** A store keeps
two numbers: the term it is at, and the highest it has heard of anywhere. Being
fenced is the second being above the first, and both have to survive a restart —
the first version of this wrote only the term it was at, so a fenced leader came
back believing itself current and took writes again, which is the whole of what
fencing exists to stop. `TestFencingSurvivesARestart`.

That went in with a comment in the test saying "and it stays fenced across a
restart", followed by a `Close` and nothing else. A comment is not an assertion,
and a comment claiming the thing the commit is *for* is the worst place to put
one.

**One writer of the state file at a time.** `Promote`, `noteTerm` and `advance`
all write it, each reading its own snapshot of the three numbers in it, so two
of them interleaving would put one path's stale copy of a field on the disk
underneath the other path's fresh one. `stateMu` is held across the read and the
write.

**A Writer answers everybody it accepted.** A caller that got onto the queue is
written and answered, including by a `Close` on its way out: `Close` takes the
lock that guards the queue, closes it, and the goroutine drains what is left
before it goes. That is why `submit` waits on its own channel with nothing to
time it out — being left there would be the one unforgivable thing a queue can
do. `TestWriterCloseDrainsTheQueue` closes with a caller inside the store and
five more behind it.

**A group is a batch, so nobody is told a write failed while it survives.** The
records of a group go down behind one marker, which means a crash loses the
group or none of it. Written as several records instead, a torn write would keep
the first few while every caller in the group heard an error — records nobody
was told about, which is the worse half of at-least-once with none of the
comfort. `TestWriterGroupIsOneBatchInTheLog`.

**A frozen log's sorted keys are the only cache in this package, and only
because they cannot go stale.** A range needs the keys in order and the index is
a hash map, so `sortedKeys` sorts them the first time a log is asked and keeps
them. That is allowed here and nowhere else: a frozen log's index is built once
and never changes, and a merge does not edit a log — it writes a new one and
swaps the segment, which brings a new sort with it. The search-order cache in
the rejected table below was turned down for exactly the property this one has.
The log being written has no such cache and is filtered per query instead.

**A batch is whole before any of it is handed over.** The marker says how many
bytes of records follow it, and `scan` and `scanSegment` check all of them —
framing and checksums — before yielding the first. Yielding as they go and
letting the caller stop at a bad record would keep the records before it and
lose the ones after, which is half a batch, which is the one thing the marker
exists to prevent. The checksums are therefore taken in the walker rather than
left to the caller, which is a departure from everything else in this package
and the reason a batch's records are checksummed twice on the paths that check
them again. `TestBatchWithADamagedRecordIsDroppedWhole` and
`TestBatchDamagedInAFrozenLog`.

**Nothing outside the walkers knows a marker exists.** `scan` and `scanSegment`
yield the records of a batch and never the marker itself, so `ForEach`,
`Verify`, `Compact`, `latestOffsets`, `offsets`, `indexSegment` and `mergeInto`
were correct without changes — and, more to the point, cannot be made incorrect
by someone adding a seventh. A marker that reached one of them would be indexed
under an empty key. If you add a walk of a log, walk it with one of those two.

**A batch is one write to the log.** Ten records in ten writes would be ten
places for the disk to stop, and the marker makes that survivable rather than
free. `appendBatch` serializes the lot, patches the span into the marker, takes
the marker's checksum, and then writes once.
`TestBatchInAFileIsOneWrite` counts the writes; `TestBatchTornAtEveryLength`
cuts that write short at every length there is and holds the store to all of the
batch or none of it.

**A position never names a record inside a batch.** A leader cuts its stream at
the end of a record or the end of a whole batch — `unitAt` is the one place that
decides which — and takes a batch bigger than the wire's pieces in one go, as it
does a record bigger than them. A follower's `verifyRecords` stops its good
bytes at the marker until the whole batch has arrived. Both halves matter: the
leader not cutting is what keeps a follower from ever being offered half a
batch, and the follower's check is what keeps a connection that died mid-batch
from applying part of one. `TestBatchCrossesWhole` asserts it after every piece
of a stream cut into sixty-four byte pieces, not only at the end.

**A record's number is in the record, and only ever goes up.** Merging drops
records, so a store that counted what it holds would count fewer than the leader
wrote, and two replicas that merged at different times would give the same
record different numbers and answer the same question about a position
differently. So it travels with the record: a follower keeps the numbers on what
it is sent, a merge copies them through, and a promoted follower carries on from
the highest it holds. `observe` raises the counter and never lowers it, for the
reason a term is only ever raised — a number that was handed out names a place
in the stream, and handing it out again puts two records there. Gaps cost
nothing; nothing counts the numbers, everything compares them.
`TestFollowerKeepsTheLeadersNumbers`, `TestMergeKeepsTheHighestNumber`.

**A follower's log is not in number order.** A snapshot ships the newest version
of every key by asking the newest log first, so it arrives newest first and the
follower's log ends on a low number. Reading the number off the last record — as
the first version of this did — leaves a follower about to reuse most of the
stream, and the store that catches it is one whose snapshot is bigger than the
4 KiB the apply path reads at a time, since a smaller one arrives in a single
piece and the highest number is in it. The counter is raised over the highest
number in whatever arrived, never the last of them, and the same applies to
`Recover` and `RebuildIndex`.

**The number is handed out under the write lock.** It is what puts a record after
the one before it, so an atomic counter taken before the lock would let two
writers take numbers in one order and append in the other, leaving a position
that names the last record naming a number with a bigger one behind it.
`TestNumbersFollowTheRecordOrder` writes from eight goroutines and reads the log
back. The checksum follows the number, so a numbered store checksums the
serialized record under the lock rather than folding the fields before it — which
turns out to be *faster* (231 ns to 205 at 16-byte values, 261 to 219 at 1 KiB),
because one pass over contiguous bytes beats a fold of the header a byte at a
time. That is the "contiguous CRC" line in the rejected table, measured again and
much larger than the 9 ns recorded there. It is still not worth doing to the
unnumbered path on its own — that would move a pass over the value inside the
lock for nothing in return.

**A merged log records the highest number of its inputs, not of what it kept.**
A merge drops records, and the one it drops may be the newest of the lot, so
reading the merged file back reports a number below one already handed out. The
hint beside a log carries that number (hint version 2), and a hint from before it
is ignored, which costs the scan a hint exists to save and arrives at the same
answer. `TestMergeKeepsTheHighestNumber`.

**A follower's term and its applied position are one write.** They are one fact
from two sides — which leader, and how far through it — and `Reached` reads both
to tell a store that has been promoted, whose term is above the term it applied
at and whose own log is what its positions are offsets into, from one that is
following, whose term equals it and which judges by what it has applied.
Written separately, as `adoptTerm` then `setApplied` used to be, every fault
between them leaves a follower looking promoted, and a crash between them leaves
it looking promoted until the next batch arrives — at which point a leader's
position is judged against the follower's own logs, which are a different set of
files and answer whatever they answer. `advance` writes both, and saves the
follower a write per batch while it is at it.
`TestFollowerTermNeverOutrunsItsPosition` fails each disk operation of a
catch-up in turn and holds the two to each other after every one, in memory and
after a reopen.

**A term only ever goes up, and it is written down before it is believed.**
`Promote` raises it above the highest this store has heard of and persists it
before returning; `noteTerm` and `adoptTerm` do the same for a term heard from
somewhere else. A term that did not survive a restart would be no fence: the
store would come back believing itself current and take writes again.
`TestPromoteRaisesTheTerm` reopens to check exactly that.

**Every position carries its term.** It is not enough for `db.position()` to
set it — `db.batch` builds positions in four places and drops it in all of them
unless told, which is how the first version of this quietly did nothing.
`batch` also normalises an incoming position's term to its own once it has
checked it, because everything below compares positions for equality.
`TestFollowerAdoptsTheTermFromABatch` is the one that fails without it, and it
had to be written because a snapshot carries the term too and hides the batch
path entirely.

**A hold pins from a follower's log onwards, not just that log.** Holding one at
a time looks like enough and is not: a follower walks forward through the logs,
and the newest frozen ones are exactly what merging takes first, so it would be
reading into a run being rewritten as it went. `holdFloor` is the oldest held id
and `pickMerge` will not touch anything from there on.
`TestDBFollowIsNotStrandedByAMerge` streams against a store that is being written
and merged throughout; without the floor it is knocked off within a second.

**`Follow` adopts the hold rather than the caller releasing it.** The caller
cannot tell when `Follow` has taken its own, and letting go first leaves the log
the stream starts from unheld — for however long that goroutine takes to be
scheduled, which on one core is long enough to lose it every time. That is why
`Follow` takes the release as an argument and calls it after holding.

**A `DB` follower's position is never more durable than the records it claims.**
`writeApplied` syncs only under `SyncAlways`, which is the policy that has
already synced the records. Syncing it under `SyncNever` — which it did, until a
benchmark showed the cost and the reason for it — would leave a store that
survived losing power claiming records that did not survive it. That is the one
direction the ordering below exists to rule out, and having it hold in the
process and break at the disk would have been the worst of both.
`TestDBFollowerPositionIsNoMoreDurableThanTheRecords`.

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

**A rotation leaves the store writable however it fails.** Three things had to
be true for that, and only the first was. `freeze` opens the read handle before
closing the store, or a failure in between leaves the active log closed and
nothing to write to. `rotateLocked` opens the *new* log before ending the old
one, for exactly the same reason — it used to do it the other way round, so one
refused open closed the active log for the life of the process. And `freeze`
ignores a close that fails, because `closeNoSync` marks the store closed before
it touches the file: there is no carrying on with it either way, and refusing to
finish leaves the same store with no log at all. All three were found by failing
the leader's disk one operation at a time while a follower asked for a snapshot,
and `TestDBRotationFailureLeavesTheStoreWritable` holds the first two.

**`Write` reports whether the record is stored, and nothing else.** Rotating a
full log is housekeeping that happens after the record is safe; a failure there
is remembered in `db.rotateErr` and reported by `Sync` and `Close`.

## One process owns a directory

The failure this closes is the quiet one. Two processes with the same directory
open both write to the active log, both keep an index of where they put things,
and neither is wrong about anything it can see. The first news either gets is a
checksum that does not match, some minutes or days later, and by then there is
no way to say which records were lost or which of the two lost them. Nothing
checked before this; a typo in a unit file was enough.

**It is a lock on a descriptor, not a file that is created and deleted, and
that is the whole design.** The `O_CREAT|O_EXCL` version is portable and needs
no syscall this package did not already make, and it is wrong for a database:
after a crash the file is still there, so the store refuses to open until
somebody logs in and removes it. The crash this package is built to survive
becomes the crash that needs a human at three in the morning. Writing the pid in
and checking whether it is alive is the usual patch and it is worse — a race
against pid reuse, and meaningless in a container where everything is pid 1. A
lock the kernel holds is dropped when the process ends however it ends, and
`TestAnotherProcessIsKeptOutAndAKillLetsItIn` is that claim tested the only way
it can be: another process, killed outright, and the directory open immediately
afterwards.

**The build constraint is a list and not `unix`, and the list was measured.**
`syscall.Flock` is missing on solaris and aix, which the `unix` tag covers, so a
build there would fail. `LockFileEx` is not in the standard library on Windows —
it is in `golang.org/x/sys`, and this module has no dependencies and is not
taking its first one for a lock file. So `lock_flock.go` names the six platforms
that have `Flock` and `lock_none.go` takes the rest. Check by cross-compiling
before changing that list; the list is what `GOOS=solaris go build` says, not
what the `unix` tag suggests.

**`lock_none.go` opens without a lock rather than refusing to open.** Refusing
would take away platforms that work today to gain a guarantee those platforms
cannot give, which is a regression dressed as safety. It creates no file either:
a `LOCK` sitting in a directory locking nothing is a thing an operator would
reasonably read as protection. `lockingEnforced` is what the tests skip on, so a
platform without a lock reports the tests it did not run instead of passing
them.

**The lock file is never removed, and that is not laziness.** Removing it is how
one lock becomes two: B opens the file and takes the lock, A removes the file it
is holding, C creates a fresh one and locks that, and now B and C each hold a
lock on a different inode with the same name and each believes it is alone. It
survives `Close`, and three separate directory walks step over it without naming
it — `segmentIDs` takes only `.seg`, `removeStaleMerges` only `.merging` and
orphaned `.hint`, and the reset in `ApplySnapshot` only `.seg` and `.hint`.
`TestTheLockFileIsLeftBehindAndIgnored` is what holds that, because "it happens
to fall through three filters" is exactly the kind of thing a fourth filter
breaks.

**`fileSystem.Lock` is a seam and `lockFile` is under it.** A lock is taken on a
descriptor and `diskFile` deliberately has none, so the platform half calls
`os.OpenFile` directly — the one place besides `os.Stat` where this package
touches a disk without going through `disk`. The seam is one level up, at
`osDisk.Lock`, which is where the watcher gets in and why the ordering tests can
exist at all.

**Eight mutations cover it and all eight are caught.** The lock never taken
exclusively, taken after the directory is read, waited for rather than refused,
kept by an open that failed, released by `Close` before the logs or not at all,
the lock file counted as a log, and any failure to lock reported as contention.
Three of them are caught by tests that were already here — the chaos runs and
`TestShortReadWhileIndexing` reopen a directory after making an open fail,
which is exactly the invariant, written down years before there was a lock to
break.

A ninth was written and deleted rather than kept: dropping the explicit
`LOCK_UN` from `Unlock` and closing the descriptor instead. Closing releases the
lock by itself, so that version behaves identically — an equivalent mutant, and
one that would have sat in the survivor list forever implying a test somebody
forgot to write. The explicit call earns its place through the ordering, not
through the release.

**Adding the operation shifted every fault index.** `watchedDisk.record` counts
operations for `failNth` and `failFrom`, so a new operation at the front of
`OpenDB` moves the numbering that every chaos sweep indexes into. That was
harmless here because those sweeps run to `operations()` rather than to a
number somebody wrote down — but it is worth knowing before adding the next
operation, and it is the reason no test in this repository should ever hard-code
a fault ordinal.

## Replication over a real socket

It lives in the daemon repository now. It was `tcp_test.go` in this package,
where it was the only place the library was put on a wire and where the framing
beside it was a sketch; the sketch became that repository's `replica.go` and the
test moved with it. This package has no over-a-socket coverage any more and that
is the right way round — the daemon's version exercises the engine through its
exported API and nothing else, so a change that breaks a caller breaks it, which
a test living next to the unexported names could not say. The cost is that it
is a different `go test`, in a different checkout, against a tagged version:
see "Verifying a change" for the one-liner that points it at a working tree.

Why either version exists: everything else here moves records through a
`bytes.Buffer` or an `io.Pipe`, which says what the records are but not that the
arrangement works, since a pipe never returns a short read, never splits a write
across two calls, and never goes away in the middle of one.

It runs a leader and a follower over a real listener with a length in front of
every frame, a connection broken on purpose part way through, and a reconnect.
The framing is not part of the library and deliberately so — it is what the
package says is the caller's job — but writing it found three things no
in-process test had:

- **A leader must answer `ErrorDiverged` with a snapshot, not by hanging up.**
  Nothing holds a log open for a follower that is not connected, so a follower
  that was away long enough always comes back to a position that is gone. A
  leader that treats that as a failed connection leaves it stuck forever.
- **`Applied() == Position()` is the wrong way to ask whether a follower has
  caught up.** A follower that has read a log to its end rests there rather than
  stepping to the start of the log being written, so a caught-up pair reports
  the end of log 13 against the start of log 14. Ask the leader whether it has
  anything more instead.
- **`Len()` is the wrong way to compare two stores.** It counts tombstones, and
  a follower that came back by way of a snapshot has none, since a snapshot
  carries only live records. Both stores are right and the counts differ. Count
  what `ForEach` yields.

Two more came out of promoting it, and both are about counting snapshots, which
is the only way from outside to tell a follower that resumed from one that was
sent the whole store again — the records end up the same either way.

- **A snapshot count is not a constant when anything else is going on.** With
  merging on, a follower away for a moment can legitimately be sent the store
  again; and a snapshot of a store whose active log is empty at that instant
  points at the start of that log, which names no record, so writes that fill
  and freeze it before the stream reads anything make the leader refuse its own
  position and snapshot a second time. `TestReplicationOverHTTP` therefore has
  merging off and counts deltas around the reconnects rather than a total.
- **A follower's `Close` does not end the handler on the other side.** It ends
  a moment later, when the closed socket reaches it, and in that moment it is
  the most likely handler in the process to take a snapshot: `Follow` was
  blocked, the store moved on, and it comes back diverged. A test that counts
  what a leader did has to wait for the leader to be serving nobody first, which
  is what the `alone` helper is. The handler also checks whether the stream is
  still wanted before snapshotting, which narrows the window but cannot close
  it.

## Chaos: faults in the way of every operation

`chaos_test.go` puts a fault in the way of each disk operation in turn and
checks one thing — that a follower given a working disk afterwards ends up
holding what its leader holds. That is the only promise worth making about a
fault. Losing a batch, applying one twice, refusing a position, needing a whole
new snapshot: all allowed, some expected. Settling into a quiet disagreement is
not.

The sweeps are what find things, not the hand-picked cases. Five of them: every
operation failed once, every operation failed from there on, writes cut off part
way at eleven different lengths, the same against a leader instead of a
follower, and a randomised run where the leader keeps being written to while the
follower fails and restarts. The randomised one earns its keep — it found two
bugs the ordered sweeps could not, because they need a merge and a restart in
the same run.

Four bugs came out of it, and only one was in replication:

- a rotation that ended the old log before opening the new one, so one refused
  open closed the store to writes for the life of the process;
- a `freeze` that refused to finish when a close failed, leaving the same store
  with no log at all;
- a merge that opened its output after renaming it into place, stranding an
  index over a replaced file;
- a merge that carried on removing logs past a refusal, leaving an older input
  in front of the merged one.

All four are the same mistake: **do the thing that can fail before the thing
that cannot be undone.** The package already knew it — it is why `freeze` opens
its read handle first — and had it wrong in three other places.

The fifth was in replication, and is the one that took longest to see: `Reset`
removed the logs and then the position, so a refusal on the position left a
store that came back claiming a leader's records it had just deleted. It is the
same rule turned round. Delete the claim before the thing claimed.

## Fuzzing the replication paths

Everything that takes bytes from a wire has a target, and they are worth running
for minutes rather than the seconds CI gives them:

| target                 | what it feeds                                        |
| ---------------------- | ---------------------------------------------------- |
| `FuzzApply`            | arbitrary bytes as a batch, to a single store         |
| `FuzzDBApply`          | the same, to a `DB` follower, with a made-up position |
| `FuzzDBApplySnapshot`  | arbitrary bytes as a snapshot                         |
| `FuzzDBSince`          | arbitrary positions to a leader                       |
| `FuzzDBPosition`       | arbitrary bytes to the position parser                |

`FuzzReadFrame` went with the daemon, and it is recorded here because what it
asserts is about memory rather than about frames: a header claiming a gigabyte
with nothing behind it must cost what a header claiming nothing costs. The
reader grows into the bytes that arrive instead of allocating the length it was
told about, and a version that did the obvious thing shows up as a fuzzer taking
the machine down rather than as a failure. The same shape of mistake is
available in this package to anyone who allocates from a number a stranger sent.

`FuzzDBApply` and `FuzzDBApplySnapshot` reuse one store rather than opening one
per execution, and install `unsyncedDisk` from `fs_test.go`, which is the real
filesystem with the waiting taken out. Both matter more than they look: opening
a store per execution and syncing the position file took the target from eight
and a half thousand executions a second to fifty-eight. Nothing about what is
written or in what order changes, only whether the process waits for it, so the
paths explored are the same ones.

`FuzzDBSince` is the leader side, which is the half that faces a follower that
has been tampered with. What it asserts is not that a position is refused —
refusing is always allowed — but that whatever comes back is whole, verified
records, since a follower takes what it is given and this is the only place that
can tell.

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

And twelve more for the `DB` half, in `dbreplica.go`: a leader accepting a
position it cannot check, a frozen log skipping the handshake or checking the
offsets but not the checksum, a snapshot shipping tombstones or superseded
records or reading before it froze, stepping to the start of an empty log,
taking the newest following log rather than the next, a batch that does not give
up the record it has no room for, a follower that does not check where it is or
that writes the position before the records, a position that is never written or
is believed when damaged, a snapshot that does not empty the store first, and a
reset that leaves the logs or the position behind. All twenty-four are caught.

And nine for the freshness checks, against `fresh_test.go`: a promoted store
judging by the position it applied rather than its own, a position from a
replaced leader compared instead of refused, a term nobody has heard of treated
as anything but behind, offsets compared without the log they are in, a
single-store position believed rather than checked against the record it names,
a store that is behind not saying so, `Await` treating giving up as arriving,
`Apply` waking nothing when it applies, and the term written apart from the
position again. All nine are caught.

That last one is the reason `advance` exists, and it is caught by the fault
sweep rather than by any assertion about a call's result: splitting the write in
two breaks nothing a test can see until a fault lands between the halves.

And eight for carrying a stranded position forward, against `stranded_test.go`:
a log that dropped records crossed anyway, only the resume log asked whether it
dropped rather than every log after it, a merge that does not record that it
dropped, a merge that forgets what its inputs had dropped, a log read without a
hint trusted rather than assumed to have dropped, the checksum of a surviving
record not checked, a position with no number resumed, and records the follower
already holds sent again — which is the one that would put an old record in a
newer log on the follower and shadow a newer one. All eight are caught.

And eleven for ranges, against `order_test.go`: the lower bound not applied, an
upper bound that includes its own key, a frozen log starting its walk at the
beginning or walking past the upper bound, a store, a DB and a frozen log each
handing their keys back unsorted, the oldest log to hold a key answering for it,
a deleted or expired key yielded anyway, a prefix running to the end of the keys,
and a prefix of nothing but 0xff overflowing its last byte. All eleven are
caught, first time — the ordering check inside the `collect` helper is what does
most of it, since a range that is wrong is usually a range that is out of order.

And seven for the writer, against `writer_test.go`: the writer taking one caller
at a time, a caller in a group hearing about another write, the group not being
emptied between writes, a caller's batch flattened to its first record, `Close`
abandoning what it accepted, a write after `Close` taken anyway, and a delete
through the writer written as a write. All seven are caught. `Close` survived
first: the test waited for the answers, so it could not tell a `Close` that waits
from one that returns and leaves the writer to finish. It asserts the ordering
now — that `Close` has not returned while a caller it accepted cannot yet have
been written, and that everything is written by the time it has.

And thirteen for write batches, against `batch_test.go`: an incomplete batch
read as far as it goes, a span longer than the log believed, a damaged record
inside a batch condemning only itself (in memory and again in a frozen log), a
frozen log yielding a batch before checking it, a marker handed out as a record,
a span written before the records are counted, a marker keeping the checksum it
had before its span, a batch written a record at a time, a refused batch keeping
what it appended, a leader cutting wherever a record ends, a follower taking the
records of a batch that has not finished arriving, and a follower not checking
the records inside one. All thirteen are caught — five of them only after the
tests grew a damaged batch to go with the torn one, since tearing a write and
corrupting a byte are different faults and only the first was being made.

And thirteen for the numbering, against `sequence_test.go`: records not numbered
at all, every record taking the same number, a checksum that does not cover the
number, a counter allowed to go back down, a batch taken at its last number
rather than its highest, a new log starting again from one, a reopened store
forgetting what its frozen logs used, a merge recording the numbers it kept
rather than the ones it covered, a hint that drops the number, an empty log with
no place in the stream, a position numbered by its last record instead of by
what follows it, `Reached` treating equal numbers as behind, and a number that
does not cross the wire. All thirteen are caught — two of them only after the
tests were fixed, which is the point of running them:

- the counter going back down survived because the snapshot in the test fitted
  in the 4 KiB the apply path reads at a time, so it arrived in one piece with
  its highest number in it. Only a snapshot large enough to arrive in several
  makes a follower take a number lower than one it already has;
- the number not crossing the wire survived because `-run TestPositionBinary`
  does not match `TestDBPositionBinary`, and a `KeyValueStore`'s positions carry
  no number to lose. That is the filter trap below, for the third time.

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

- **Damage in a length field tests the framing, not the checksum.** A test that
  flipped a bit inside a record of a batch reported that a damaged batch was
  dropped whole — and it was, but by the framing check, because the bit landed
  in a key length and moved where every following record started. The mutation
  that removed the checksum inside the walker survived it. Damage the last byte
  of a value when the checksum is what is being tested; damage a length when the
  framing is.
- **A field added to a struct that is compared to its zero value.** `Position`
  gained `Seq`, and five places asking `pos == (Position{})` were really asking
  "does this name a record", which is `pos.Offset == 0`. Four were in the
  library and were changed with the field; the fifth was in a test, which then
  wrote one record instead of ten, took a snapshot of a store whose active log
  was empty, and failed only under `GOMAXPROCS=1` — where the timing that had
  been hiding it changed. Grep for the zero value of anything you add a field
  to, tests included, and ask what each comparison actually means.
- **Hard-coded header offsets.** Bit three times now. The third was a test doing
  pointer arithmetic with `headerSize` to find where a value sat, which was
  right until a second layout existed and `headerSize` became the largest of
  them rather than the one a plain `Write` uses. Ask `decodeHeader`, or ask
  `parseRecordAt` for the record and use what it hands back.
- **A count of logs is not a fixed number.** `TestTheLockFileIsLeftBehindAndIgnored`
  compares `Segments()` across a close and reopen, which is a real claim — the
  `LOCK` file must not be counted as one — and it was written with merging left
  on. Merging is a background thing, so the count is whatever the merges
  happened to have finished by, and the test passed alone and failed under the
  suite. It surfaced as a mutation verdict rather than as a flake: an unlock
  that only closed the file was reported as caught by that test, which cannot
  be right, since closing the descriptor releases the lock and the mutation
  changes nothing. Running it by hand passed; running the suite failed with
  "logs after reopening: 8, want 14". Set `MergeTrigger: 1` in any test that
  counts logs, and treat a catch you cannot explain as a broken test rather
  than as good news.
- **A test that waits where the code should refuse.** The mutation removing
  `LOCK_NB` — a lock waited for rather than refused — was caught by the suite
  hanging until the ten-minute deadline, which is a catch and a useless one:
  ten minutes for a verdict, and a report that could not tell it from a
  timeout set too low. Every test here that expects `ErrorLocked` goes through
  `openWithin`, which fails in fifteen seconds. If a mutation's correct
  behaviour is "answers quickly", the test has to say so; a test that blocks
  forever is not testing that.
- **A mutation script that does not run the test that would catch it.** Each
  script has a `-run` filter, and twice a new test was written whose name did
  not match it, so a mutation was reported as surviving when the test for it was
  simply never run. If a mutation survives and there is obviously a test for it,
  check the filter before touching the code.
- **A mutation whose patterns rot.** They are exact text, so renaming a function
  or moving a line turns a mutation into a silent SKIP. Read the SKIP lines: a
  suite reporting twelve caught and five skipped is a suite testing seven
  things. Merging two branches is the worst case for this — adding a field to a
  struct literal realigns every line in it, so `writes: writer,` became
  `writes:  writer,` and a mutation aimed at the write path stopped being aimed
  at anything.
- **A mutation that does not compile is a mutation that did not run.** Four in
  one sweep: replacing a channel with an undeclared name, and three that left a
  variable unused because the only line that read it was the line replaced. Each
  reported `SKIPPED (does not build)`, which is honest and easy to skim past.
  Mutate to something that still typechecks — `case started.IsZero():` instead
  of `case false:` — or add the `_ =` that keeps the variable read.
- **A mutation script that dies on a test's output.** One sweep stopped at
  mutation 30 of 46 with a `UnicodeDecodeError`: a failing fuzz test printed a
  corpus entry, the corpus entry was arbitrary bytes, and `subprocess.run(...,
  text=True)` raised rather than returning. Seventeen mutations were never tried
  and nothing said so — the tally line was the thing that failed to print. Pass
  `errors="replace"`, and count the result lines against the number of mutations
  defined rather than trusting that the run finished.
- **A mutation whose pattern matches two files.** The scripts pick a target by
  searching for the text to replace, and once `db.go` and `dbreplica.go` both
  had the same line, two mutations were silently edited into the wrong file and
  reported as surviving. They now refuse an ambiguous pattern rather than
  choosing. If a mutation "survives" and the fix looks obviously tested, check
  which file it landed in before believing it.
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

The clock has a seam too, in `kv.go`: `now` is where this package reads it and
the only place it does. Expiry is the one thing here whose answer changes
without anybody writing anything, so a test that cannot move the clock has to
sleep — slow when it passes, flaky when the machine is busy, and only ever able
to check the coarse case. `at` in `expiry_test.go` is the harness.

For anything about durability or ordering, use the seam in `fs.go`. It records
every open, write, sync, truncate, close, rename, remove, list and read in
order, and can be told to fail any of them, or to run out of room part way
through a write. `fs_test.go` has the harness. Orderings are most of what makes
a crash survivable and none of them show up in the result of a call.

## Open

- **The randomised chaos run found a real one, and it took a bisect to believe
  it.** A follower settled on a value two merges old, once every few dozen runs.
  The run is seeded — `rand.NewSource(11)` — so the only thing varying is when
  the background merges land, and the bisect said it appeared with the commit
  that merged two writes of the state file into one. That commit did not cause
  it: it changed how many disk operations an apply makes, which moved every
  fault index in the sweep, and the new indices reached a bug that had been
  there all along. A bisect over a fault-injection suite tells you when
  something became *reachable*, not when it was written.

  What made it findable was the record numbers. A check after every step — no
  log may hold a lower number for a key than an older log does — turned "the
  follower disagrees, eventually" into "log 24 is on the disk and not in the
  store, at step 68". That check is worth rebuilding if anything like it comes
  back.
- **Three fuzz runs have failed and none was explained.** Different targets,
  months apart: one of the original targets, once `FuzzDBSince` at the end of a
  long verification run, and once `FuzzApply` during the lock work. None wrote
  anything to `testdata/fuzz`, which a target that actually found an input
  always does, so none was the fuzzing finding a bug — something else about the
  run failed. The second did not recur in thirteen further runs, alone, in
  sequence, and under load; the third did not recur in five, including the same
  back-to-back pair that produced it, against a target that touches nothing the
  lock changed. If it returns, look for `testdata/fuzz` first: if there is
  nothing there, it is not the store.

  The third one also lost its own evidence: it was run as `go test … | tail -3`,
  which kept `FAIL` and threw away the reason. That is the mistake the bullet
  below this one is about, made again by somebody who had read it. Capture to a
  file and grep the file.
- **One suite run failed and the output was thrown away.** It happened in a
  verification chain that redirected `go test` to `/dev/null` and printed only
  the exit code, so there is nothing to go on. Thirty-seven captured runs since
  — thirty-one on all cores, six on one — have been clean. Redirecting the
  output of the thing you are checking is a way of learning nothing; capture it
  and grep, and mind that a pipeline's exit status is the last command's, so
  `go test ... | tail -1` succeeds however the tests went.
- **`os.Stat` and directory syncing** still go straight to the OS in one or two
  places. Everything else goes through `fs.go`.
- **The fuzz corpus lives in the local build cache**, not the repository. CI
  starts from the seeds in the code every time, so its thirty seconds a target
  is a smoke test. Real coverage comes from running one for minutes locally.

## The server left, and where it went

The HTTP server and the `litekvd` daemon are their own repository now:
[github.com/tillknuesting/litekvd](https://github.com/tillknuesting/litekvd).
Its `AGENTS.md` holds what used to be this section — what a handler test can and
cannot say, the five mutations that survive on purpose, the traps that belong to
the wire rather than to the log.

The split is what the first section of this file always said the arrangement
was: nothing in package `litekv` opens a socket. It is now true of the whole
repository and not only of the package, which is what makes this a toolbox
somebody can take the log out of.

Two things follow for anyone working here.

**The engine can no longer be tested through the server, and mostly never was.**
Everything below this line is held up by tests in this repository. If a change
here would break the daemon, the daemon's own suite is where that shows up, and
it is a separate `go test` in a separate checkout — there is no single command
that runs both. Tag a release here before expecting the daemon to see a change.

**The mutation sweep is in both repositories and they are not the same sweep.**
This one has the eight for the directory lock and runs the engine's suite at ten
minutes a mutation; the daemon's has a hundred and nine and runs its own at
ninety seconds. The runner is duplicated, deliberately — a shared tool would be
a third module for four hundred lines that change about once a year.

## What to build next, and what it needs

The six pieces that built the server are done and their notes went with it, to
the daemon's own `AGENTS.md`. What is left here is the engine's list, and one
item on it is the reason to read this section at all.

| what                        | state | why                                     |
| --------------------------- | ----- | --------------------------------------- |
| a lock file                 | done  | hours of work against silent corruption |
| `DB.Demote`, and a lease    | open  | the one real gap left; see below        |
| ranges that stream and page | open  | scaling, and nobody has hit it yet      |

**A way down, and a lease** is the gap worth naming clearly, because it is the
only one where the current behaviour loses acknowledged writes. A replaced
leader finds out it was replaced when something carrying a newer term asks it
for records, and until then it goes on taking writes that are lost the moment it
finds out. There is also no way down at all: `Promote` raises a term and nothing
lowers a store back to following, so a node that should hand over has to be
killed. The engine's half is `DB.Demote`; the daemon's half is a `/v1/demote`
route and a lease loop where a leader that cannot renew stops taking writes on
its own — the external-lease arrangement argued for under "Consensus, and why it
is not on that list". It bounds the window at the lease TTL less the clock skew
rather than closing it, which is a much smaller number than "until a follower
turns up" and is still a number.

**Ranges that stream and page** is the k-way merge described under range and
prefix queries below, plus a cursor a client can resume from. The hard half is
the cursor, not the merge: a resume has to stay correct across a merge that
moved the keys under it, which is the same class of problem as carrying a
stranded follower and took longer than it looked there too.

Below is what was on the list before either of those, kept because the reasoning
is still the reasoning — and because several entries end in "what is left of it,
if anyone wants it", which is the useful part.

Replication is finished in both halves, for a single store and for a `DB`.

Fencing is done: `Promote`, a term on every `DBPosition`, and `ErrorFenced` from
a store that has heard of a newer one. Expiry is done. Reads that are not stale
are done: `Reached` and `Await`, on both halves. The per-record sequence number
is done, which made positions totally ordered and settled the log boundary those
two were cautious about. What is left below is ordered as before.

One thing about `Reached` is still worth knowing before building on it: **a `DB`
cannot check a position, only compare it.** A follower holds none of the leader's
bytes, so what it compares is two numbers, and the term decides which of the
store's two positions it compares against — the applied one for a store that is
following, its own for one that follows nobody. Handing it a position cut by
something that was never this store's leader is a caller error nothing here can
catch, which is the difference from the single-store version, where the record
the position names is in `Data` to be looked at.

The number also makes **carrying a stranded position forward** cheaper than it
was when it was written up below. A merged log keeps its records' numbers, so a
position naming number 40,000 can be found in the log that replaced the one it
was cut against, where before there was nothing to look for. It is still not
free — something has to find it without scanning the log — but the fact the
scheme needed is now on the disk.

**The server's writer** is done: `Writer`, on both halves, one goroutine behind
a queue, storing everything waiting as one batch. The numbers are in the README
and one of them is worth knowing before reaching for it — in front of a store
with no file it is *slower*, 0.53 µs a write against 1.2, because what a queue
amortizes is the cost of a write to the log and there is no log. With one it is
2x under `SyncEvery` and 5.2x under `SyncAlways`, and the ratio grows with the
sync and with the number of callers, which is the shape group commit always has.

The leader-and-followers arrangement — the first caller waiting becomes the
writer, writes everybody's records and wakes them, rather than a goroutine of its
own — would take the uncontended handoff back out and is what RocksDB does. It
was not built: it costs a leadership handover to keep one caller from writing for
everybody indefinitely, and the case it wins is the case that should not be using
a queue at all. Worth doing if a `Writer` in front of an in-memory store ever
stops being a mistake.

**A batch write** is done: `Batch`, `WriteBatch` on both halves, and a marker
record opening a span. It cost no new record layout in the end — the marker is
an ordinary record with a type of its own — and the six scanners were left alone
because the framing went into `scan` and `scanSegment` rather than into each of
them. What it did cost is the first rule in this format where one record's
meaning depends on another, which is why the walkers now checksum inside a batch
and nothing else does.

What is left of it, if anyone wants it: a batch has to fit in memory twice over,
once in the caller's `Batch` and once in the records serialized from it, and a
reader holds one batch while checking it. Streaming a batch larger than memory
would need a commit record at the end rather than a marker at the front, which
is a different format and a worse one for everything else — recovery could no
longer tell how much to expect, only whether what it found was finished.

The format byte is the cheap part and the semantics are not. "Is this record
inside a batch that was never committed?" has to be answered by `Recover`,
`RebuildIndex`, `latestOffsets`, `indexSegment`, `mergeInto` and the replication
apply path — six scanners — and a follower has to buffer a batch rather than
apply half of one. The likely shape is a marker record opening a batch with the
byte span that follows, so recovery either has all of it or discards from the
marker on, and records outside a span stay ordinary writes needing no flag.
Estimate it as its own piece of work, not an afternoon.

**Range and prefix queries** are done, and without an ordered index in the end.
The keys are asked rather than kept in order: a frozen log sorts its keys the
first time one is wanted and keeps them, since that index never changes again,
and the log being written is filtered with only the matches sorted. A prefix
matching a hundred keys of a hundred thousand is 130 µs against the 45.6 ms of
walking everything, and the write path pays nothing.

What is left of it, if a range ever becomes a hot path: the answer is gathered
and then sorted rather than streamed, because every log has to be asked before
anything can be yielded in order. A k-way merge over the per-log sorted keys
would stream it and hold nothing, and is worth the code only when somebody is
ranging over most of a large store.

**Semi-synchronous replication** is done and it is not here. It needed all three
of the things that note said it did — a leader that knows its followers, a
follower that says how far it has got, and a write that waits — and all three
turned out to belong above the engine, so all three are in the daemon. The
engine gave up nothing to make it possible, which is the test that the layering
was right.

What is worth keeping on this side is the shape of the guarantee, because people
ask the engine for it: **a write cannot be taken back.** The record is in the log
before anything waits — there is nothing to replicate until it is written — so a
wait that runs out is reported and never undone. Say that to anyone who asks for
"synchronous replication" here; what is on offer is an acknowledgement that means
a failover will not lose the write, and nothing stronger. Building it any other
way means holding a record out of the log until a quorum has it, which is a
different database and is filed under consensus below.

**Carrying a stranded position forward** is done, and the mappings the earlier
note here proposed turned out not to be needed. The records carry numbers, so a
position that no longer names a log can be found by reading: `resumeAt` picks
the oldest log whose numbers reach the follower's, and `resumeIn` walks it to the
place just before the first record the follower has not got. That is a scan of
one log, paid once by a follower that has been away, against the whole store it
would otherwise be sent.

Two things about it are worth knowing before touching it.

**What is checked is weaker than what it replaces, on purpose.** If the record
the position names is still there, its checksum has to match. In a busy store it
usually is not there — something newer superseded it, and the merge dropped it —
and then the number is taken at its word. The term has already scoped the
position to one leader by then, which is what makes that defensible. The
alternative was refusing every resume in exactly the stores where snapshots hurt
most.

**A log that dropped records cannot be crossed.** A merge that reaches the
oldest log drops tombstones and expired records, and a follower carried across
one would never hear that a key was deleted: it holds an older value and nothing
in what follows would replace it. So the hint records whether a log dropped
anything — sticky, since a merge of a log that dropped covers its range too — and
a resume refuses when any log at or after the resume point has it. A log opened
without a hint is assumed to have dropped something, since the records cannot
say. `TestStrandedFollowerAcrossADroppedTombstone` is the case that must not be
resumed, and it is the one that makes this safe rather than clever.

### Consensus, and why it is not on that list

Automatic failover needs agreement on who the leader is, and there are three
ways to get it. They are not the same size and only one of them composes with
what is here.

**An external lease** — etcd, Consul, ZooKeeper — does the consensus elsewhere
and tells these nodes who is leading. Everything in `dbreplica.go` survives
untouched; the data path is unchanged. It costs an operational dependency, which
is a little ironic for a standalone database but is the pragmatic answer.

**Raft for leader election only**, implemented here, is the same shape without
the dependency: the record shipping stays the data path and consensus decides
only who runs it. This is what PostgreSQL with Patroni does.

**Raft for the data path is a different system, and it supersedes this one.**
The Raft log would hold the commands, every node would apply committed entries
to its local store, and the store's own replication would go unused — you would
not ship these logs at all. `dbreplica.go` and half of `replica.go` become dead
code, a write becomes a quorum round trip plus an fsync on a majority instead of
a local one, and three nodes is the floor, since a quorum of two is not a thing.
Anyone reaching for Raft should know it replaces this work rather than sitting
on top of it.

For one machine and perhaps a second, none of that fits: two-node Raft gives
nothing a single node did not have. Promotion by hand with fencing is the honest
arrangement at that size, which is why fencing is first on the list and
consensus is not on it.

## Two things called a batch

`Batch` and `WriteBatch` are several records stored together. `ReplicaOptions.
BatchSize`, `db.batch`, `kvs.batch` and the `batch` argument to `Apply` are a
run of records crossing a wire. They are unrelated, and both names are the right
one for their side: every key-value store calls the first a write batch, and the
second has been called a batch here since before the first existed.

Where a sentence could mean either, this repository says **a write batch** for
the records and **a batch of the log** for the wire. The code says `unitAt` for
the thing a wire may not cut through, which is a record or a write batch, and
that word is deliberate too.

## Conventions

Work happens on `main`. No branches, no pull requests; this is a single-author
repository and the round trip buys nothing. Verify before pushing, since there
is no review stage to catch anything.

Commit messages here say what changed and why, in prose, and give the numbers
when there are numbers. `git log` is where the reasoning for most decisions
lives.
