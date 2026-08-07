package litekv

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Replicating a DB is not replicating a KeyValueStore across several files.
//
// A KeyValueStore ships its log as bytes, which works because that log only
// ever grows: an offset in it means the same thing forever. A DB has no such
// thing. Its logs are merged in the background, and a merge renames its output
// over the oldest log it replaces — so the file called 0000000005.seg can
// become a different file, with different contents, at a different length,
// while a follower thinks it has read forty kilobytes of it. Merging also
// discards: superseded records always, tombstones when the run reaches the
// oldest log. The bytes are not a stream, and the history is not kept.
//
// So what crosses here is records, not bytes. The follower appends them to a
// store of its own and rotates and merges on its own schedule, and the two ends
// agree on every key while agreeing on nothing about their files. That is the
// logical replication log of the leader-follower chapter of Designing
// Data-Intensive Applications, chosen for the reason the book gives: it is not
// tied to how either end happens to store things.
//
// Because a merge destroys history, a leader cannot replay its writes from
// arbitrarily far back, and no protocol can make it. Replication is therefore a
// snapshot and then the tail after it, which is the same shape the book gives
// for setting up a new follower, and a follower that falls so far behind that
// its place has been merged away needs a new snapshot rather than a rewind.

// DBPosition is how far a follower has got through a leader's DB: which of its
// logs, and where in that log.
//
// The log part is a Position like any other and carries the same check — where
// its last record starts and that record's checksum — so a log that has been
// merged out from under a follower is caught rather than half read. The zero
// value is no position at all, which only a snapshot can fill.
type DBPosition struct {
	// Term is which leader this position came from. It rises by one every time
	// a store is promoted, and it is what stops a leader that has been replaced
	// from being followed or written to. See Promote.
	Term uint64

	// Segment is which of the leader's logs, by the id in its filename.
	Segment uint64

	// Log is where in that log, exactly as for a single store.
	Log Position
}

// dbPositionSize is what a DBPosition takes on the wire.
const dbPositionSize = 8 + 8 + positionSize

// MarshalBinary encodes the position in forty-four bytes, little-endian.
func (p DBPosition) MarshalBinary() ([]byte, error) {
	log, err := p.Log.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, dbPositionSize)
	binary.LittleEndian.PutUint64(buf[0:8], p.Term)
	binary.LittleEndian.PutUint64(buf[8:16], p.Segment)
	copy(buf[16:], log)
	return buf, nil
}

// UnmarshalBinary decodes a position encoded by MarshalBinary, checking it
// rather than believing it, as the single-log one does.
func (p *DBPosition) UnmarshalBinary(data []byte) error {
	if len(data) != dbPositionSize {
		return fmt.Errorf("litekv: db position is %d bytes, not %d", len(data), dbPositionSize)
	}

	var q DBPosition
	if err := q.Log.UnmarshalBinary(data[16:]); err != nil {
		return err
	}
	q.Term = binary.LittleEndian.Uint64(data[0:8])
	q.Segment = binary.LittleEndian.Uint64(data[8:16])

	*p = q
	return nil
}

// Position returns where in this store's stream of records a follower would
// have to be to be up to date with it: the active log, and the end of it.
func (db *DB) Position() DBPosition {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.position()
}

// position is Position with db.mu held.
func (db *DB) position() DBPosition {
	if db.active == nil {
		return DBPosition{}
	}
	return DBPosition{Term: db.term, Segment: db.active.segID, Log: db.active.kvs.Position()}
}

// Term is the leader generation this store is at. It starts at zero, rises by
// one on every Promote, and is written down beside the logs.
func (db *DB) Term() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.term
}

// Promote raises this store's term and returns the new one, which is what makes
// a replica into a leader.
//
// The term is the whole of fencing. Two stores taking writes at once cannot be
// reconciled — the position check will refuse to splice one log onto another, so
// nothing is corrupted, but writes acknowledged by the wrong leader are found to
// be worthless and thrown away. A checksum cannot tell you that a leader has no
// business being one; a term can, because it only ever goes up.
//
// What it does not do is decide who should be leader. That is consensus, and it
// is not here: something outside — a person, a script, a lease service — decides,
// and calling this is how the decision is written down. Raising it twice in two
// places at once puts two stores on the same term and gives the guarantee away,
// so whatever decides has to be the only thing deciding.
//
// A store that had been fenced starts taking writes again, since its term is now
// the highest it has seen.
func (db *DB) Promote() (uint64, error) {
	// One writer of the state file at a time, and it decides what to write with
	// the lock already held. Three paths write it — this, noteTerm and
	// advance — each reading its own snapshot of the three numbers, and two of
	// them interleaving would put one path's stale copy of a field on the disk
	// under the other path's fresh one.
	db.stateMu.Lock()
	defer db.stateMu.Unlock()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return 0, ErrorClosed
	}

	term := db.term + 1
	if db.seen >= term {
		term = db.seen + 1
	}
	applied := db.applied
	db.mu.Unlock()

	if err := writeReplicaState(db.dir, term, term, applied, db.opts.Sync == SyncAlways); err != nil {
		return 0, err
	}

	db.mu.Lock()
	db.term, db.seen = term, term
	db.mu.Unlock()

	return term, nil
}

// fenced reports whether this store has heard of a leader newer than itself, in
// which case it is not one and may not take writes. Callers must hold db.mu.
func (db *DB) isFenced() bool { return db.seen > db.term }

// noteTerm remembers the highest term this store has heard of. A store that
// hears of one above its own has been replaced and stops taking writes: it
// cannot tell that by itself, and the only place the news reaches it is a
// follower or a leader that has moved on.
func (db *DB) noteTerm(term uint64) error {
	db.stateMu.Lock()
	defer db.stateMu.Unlock()

	db.mu.Lock()
	if term <= db.seen {
		db.mu.Unlock()
		return nil
	}
	applied, mine := db.applied, db.term
	db.mu.Unlock()

	// The term heard of is what goes down, and it is what makes this a fence
	// rather than a note in memory: a store that forgot it on the way through a
	// restart would come back believing itself current and take writes again,
	// which is the whole of what fencing is for.
	if err := writeReplicaState(db.dir, mine, term, applied, db.opts.Sync == SyncAlways); err != nil {
		return err
	}

	db.mu.Lock()
	if term > db.seen {
		db.seen = term
	}
	db.mu.Unlock()
	return nil
}

// Snapshot writes the store's live records to w and returns the position they
// are current as of, along with the function that lets go of the log that
// position names. It is how a follower starts: apply the snapshot, then stream
// from the position it came with.
//
// The hold is the difference between that working and not. Shipping a snapshot
// takes as long as it takes, and merging carries on throughout, so a position
// handed back unheld can already have been merged away by the time the follower
// is ready to stream from it — and the snapshot taken in answer to that would go
// the same way. Held, the log stays where it is. Release it once the stream is
// running, or once you have given up on it; releasing twice is harmless.
//
// What crosses is one record per live key — the newest version of it — and
// nothing else. Superseded records do not go, and neither do tombstones or
// records whose expiry has passed, since a follower starting from nothing has
// no older value for either of them to hide.
//
// The snapshot is consistent without stopping the store. The active log is
// frozen first, so everything the snapshot covers is on the disk and can no
// longer change, and the position it reports is the start of a log with nothing
// in it yet: whatever is written from here on is the tail rather than part of
// the snapshot. Writes and rotation carry on throughout. Merging does not — a
// merge may remove a log, and this is reading them — so a snapshot of a large
// store holds merging off for as long as it takes.
func (db *DB) Snapshot(w io.Writer, opts ReplicaOptions) (DBPosition, func(), error) {
	// No merge may take a log away while it is being read out. This is also
	// the lock order the rest of the package uses: mergeMu, then db.mu.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	// Asked before the log is frozen, not after: a store that is not a leader
	// should not be quietly rotating on its way to saying so.
	if db.isFencedLocked() {
		return DBPosition{}, func() {}, ErrorFenced
	}

	at, frozen, err := db.freezeForSnapshot()
	if err != nil {
		return DBPosition{}, func() {}, err
	}

	// Taken here rather than by the caller afterwards, because here there is no
	// gap: this stretch already excludes merging, so the log the position names
	// cannot go between deciding on it and holding it.
	release := db.hold(at)

	// Buffered, or a store of small records is a write to w per key.
	writer := bufio.NewWriterSize(w, int(opts.batchSize()))

	// Newest log first, so the first version of a key that turns up is the one
	// that counts, exactly as a lookup does it. A key whose newest record is a
	// tombstone is marked and skipped rather than passed over, or an older
	// value would come back from a log further down.
	seen := make(map[string]bool)

	for _, seg := range frozen {
		var failed error

		seg.eachKey(func(key string, pos int64) bool {
			if seen[key] {
				return true
			}
			seen[key] = true

			record, raw, err := seg.recordAt(pos)
			if err != nil {
				failed = err
				return false
			}
			if record.Crc != checksumSerialized(raw) {
				failed = fmt.Errorf("log %d, record at offset %d: %w", seg.id(), pos, ErrorChecksumMismatch)
				return false
			}
			if record.Type != RecordTypeNormal || record.Expired() {
				return true
			}
			if _, err := writer.Write(raw); err != nil {
				failed = err
				return false
			}
			return true
		})

		if failed != nil {
			release()
			return DBPosition{}, func() {}, failed
		}
	}

	if err := writer.Flush(); err != nil {
		release()
		return DBPosition{}, func() {}, err
	}
	return at, release, nil
}

// freezeForSnapshot ends the active log and reports the position a snapshot
// taken now is current as of, along with the logs it has to read. Callers must
// hold db.mergeMu, which is what keeps those logs from being merged away
// afterwards.
//
// Logs frozen after this returns are not in the list and do not need to be:
// they hold records written after the position, which reach a follower as the
// tail rather than as part of the snapshot.
func (db *DB) freezeForSnapshot() (DBPosition, []*diskSegment, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return DBPosition{}, nil, ErrorClosed
	}
	if db.active == nil {
		return DBPosition{}, nil, ErrorClosed
	}

	// An empty active log is already the start of one, so there is nothing to
	// freeze and no empty file to leave behind. The position is then the start
	// of that log, which cannot be checked against a record because there is
	// none — see the note in batch. It is safe while the log is the one being
	// written, and a follower that has not used it by the time that log freezes
	// takes another snapshot.
	if db.active.size() == 0 {
		return db.position(), append([]*diskSegment(nil), db.frozen...), nil
	}

	// The end of the log about to be frozen, taken before it is: it names that
	// log's last record, so a follower handed it can be checked. Reporting the
	// start of the new log instead would name no record at all, and every
	// follower would be unverifiable from the moment that log filled.
	at := DBPosition{Term: db.term, Segment: db.active.segID, Log: db.active.kvs.Position()}

	if err := db.rotateLocked(); err != nil {
		return DBPosition{}, nil, err
	}
	db.notify()

	return at, append([]*diskSegment(nil), db.frozen...), nil
}

// Since writes the records after pos to w, at most one batch of them, and
// returns the position they leave a follower at. It returns pos unchanged when
// the follower is already up to date.
//
// It returns ErrorDiverged when pos names a log this store no longer has, or
// one whose contents are not what the position says they were — which is what a
// merge leaves behind, since it rewrites its output over the oldest log it
// replaces. There is nothing to carry on from, and the answer is a new
// Snapshot.
func (db *DB) Since(pos DBPosition, w io.Writer, opts ReplicaOptions) (DBPosition, error) {
	bufp := sendBuffers.Get().(*[]byte)
	defer func() { sendBuffers.Put(bufp) }()

	batch, next, err := db.batch(pos, opts.batchSize(), (*bufp)[:0])
	*bufp = batch

	if errors.Is(err, ErrorFenced) {
		// Written down before it is reported, so that a leader which has been
		// replaced stops taking writes rather than carrying on until somebody
		// notices the error.
		if noted := db.noteTerm(pos.Term); noted != nil {
			return pos, noted
		}
	}
	if err != nil {
		return pos, err
	}
	if len(batch) == 0 {
		return next, nil
	}

	if _, err := w.Write(batch); err != nil {
		return pos, err
	}
	return next, nil
}

// Hold keeps the log a position names, and every log after it, from being
// merged away, and returns the function that lets them go again. Releasing
// twice is harmless.
//
// From that log onwards, and not only that log: a follower walks forward
// through the logs, and the newest frozen ones are exactly what merging takes
// first, so pinning one at a time would leave it reading into a run that was
// being rewritten as it went. This is the thing PostgreSQL calls a replication
// slot, and it pins the same way.
//
// Without one, a follower is at the mercy of the merging that goes on
// underneath it. A follower that has fallen behind is reading a frozen log and
// a merge can take it; one that has caught up rests wherever it read last,
// which when the log being written is empty is the end of the last frozen log —
// and a merge can take that too. Either way the answer is ErrorDiverged and a
// snapshot of the whole store, which for an idle follower on a store that
// merges is a routine and expensive surprise.
//
// Follow takes one for the length of a connection, so a connected follower does
// not need to ask for it. Hold is for a leader answering with Since instead,
// where there is no connection for anything to hang off.
//
// What it costs is logs, and the cost is not small: everything written since
// the oldest follower's position stays on the disk, unmerged, and every lookup
// asks each of those logs in turn. A follower that goes quiet without releasing
// leaves the leader carrying them indefinitely. That is why this is a hold with
// a release rather than a list of followers the leader keeps: nothing here pins
// a disk for a follower that has gone away.
//
// Merge ignores holds. It is an explicit request to compact the whole store,
// and a follower reading one of those logs will have to take a new snapshot.
func (db *DB) Hold(pos DBPosition) (release func()) {
	// Wait for a merge that is already running. One that has chosen its victims
	// cannot be called off, so a hold taken while it ran would look like it had
	// been honoured and would not have been. This is also the lock order the
	// rest of the package uses: mergeMu, then db.mu.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	return db.hold(pos)
}

// isFencedLocked reports whether this store has been replaced, taking the lock
// itself, for the callers that are not already holding it.
func (db *DB) isFencedLocked() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.isFenced()
}

// hold is Hold with db.mergeMu already held, for the callers that are inside
// their own merge-free stretch and would deadlock taking it again.
func (db *DB) hold(pos DBPosition) func() {
	db.mu.Lock()

	if db.held == nil {
		db.held = make(map[uint64]int)
	}
	db.held[pos.Segment]++

	db.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			db.mu.Lock()
			defer db.mu.Unlock()

			if db.held[pos.Segment]--; db.held[pos.Segment] <= 0 {
				delete(db.held, pos.Segment)
			}

			// What was blocked may now be worth doing.
			db.mergeInBackground()
		})
	}
}

// Follow hands the records after pos to send, and goes on handing them over as
// they are written to this store, until until is closed or send reports an
// error. It returns the position it had got as far as.
//
// holding is the release returned by Snapshot, or nil when the position did not
// come from one. Follow takes a hold of its own before calling it, so the log
// the stream starts from is never unheld for an instant. The stream then moves
// its hold forward as it reads, which is what lets the leader merge everything
// behind it.
//
// send takes the position those records leave a follower at, as well as the
// records, and both have to reach the other end. A follower of a DB cannot work
// out where it is from its own log the way a follower of a single store can —
// its files have nothing to do with the leader's — so the position has to
// travel. How it travels is the caller's business: a length, the twenty-eight
// bytes of MarshalBinary, and the records will do.
//
// A slow follower does not slow this store down: each batch is copied out under
// the lock and handed over outside it.
func (db *DB) Follow(pos DBPosition, holding func(), send func(batch []byte, next DBPosition) error, until <-chan struct{}, opts ReplicaOptions) (DBPosition, error) {
	bufp := sendBuffers.Get().(*[]byte)
	defer func() { sendBuffers.Put(bufp) }()

	// The log being read is held for as long as this stream is on it, so
	// merging cannot take the follower's place away underneath it. The hold
	// moves as the stream does, and is taken on the new log before it is let go
	// on the old one.
	release := db.Hold(pos)
	defer func() { release() }()

	// The hold Snapshot took is handed over here rather than by the caller,
	// because the caller cannot tell when this one has been taken. Letting go
	// of the snapshot's hold first leaves a moment with nothing holding the log
	// the stream is about to start from — and on a machine with one core that
	// moment is however long it takes this goroutine to be scheduled, which is
	// long enough to lose it every time.
	if holding != nil {
		holding()
	}

	for {
		// Before asking, never after: a record written in between closes the
		// channel already in hand, so the wait ends at once.
		changed := db.Changed()

		for {
			batch, next, err := db.batch(pos, opts.batchSize(), (*bufp)[:0])
			*bufp = batch

			if err != nil {
				return pos, err
			}
			if next == pos {
				break
			}
			if err := send(batch, next); err != nil {
				return pos, err
			}

			if next.Segment != pos.Segment {
				moved := db.Hold(next)
				release()
				release = moved
			}
			pos = next
		}

		select {
		case <-changed:
		case <-until:
			return pos, nil
		}
	}
}

// batch copies the records after pos out of the logs that hold them, up to size
// bytes, and reports where they leave a follower that takes them all.
//
// A batch crosses from one log into the next rather than stopping at the
// boundary, and that is not for efficiency. A position at the very start of a
// log names no record, so there is nothing for the leader to check it against —
// and a frozen log may have been merged, keeping its name while becoming a
// different file entirely. Reading at least one record of a log before resting
// in it means every position in a frozen log names a record, and so can be
// checked. The cost is that a batch may overshoot its size by one record at
// each log it crosses.
//
// A follower that has caught up rests wherever it read last: after a record in
// the log being written, or at the end of the last frozen log when that log is
// empty. Either way it names a record. A merge that takes the frozen log it is
// resting at will strand it, and the answer to that is a fresh snapshot — there
// is nothing here holding a log open for a follower the way a replication slot
// would.
func (db *DB) batch(pos DBPosition, size int64, dst []byte) ([]byte, DBPosition, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed || db.active == nil {
		return dst, pos, ErrorClosed
	}

	// A follower that has heard of a newer leader is not this store's to serve.
	// This is the only way a leader learns it has been replaced — nothing else
	// tells it — so the term is remembered by the caller and this store stops
	// taking writes.
	if pos.Term > db.term {
		return dst, pos, ErrorFenced
	}

	// From here the term is this store's. It has been checked, and everything
	// below compares positions for equality, so a caller a term behind must not
	// be told it has something to catch up on that it has not.
	pos.Term = db.term

	// The one position that cannot be checked. A snapshot of a store whose
	// active log was empty has nowhere to point but the start of that log, and
	// if it fills and freezes before the follower asks for anything, there is no
	// record to say whether it is still the log it was. It is refused, and the
	// follower takes another snapshot exactly as if the log had been removed.
	// Every other position names a record, which is what batch goes out of its
	// way to arrange.
	if pos.Log.Offset == 0 && pos.Segment != db.active.segID {
		if seg := db.frozenSegment(pos.Segment); seg == nil || seg.bytes > 0 {
			return dst, pos, ErrorDiverged
		}
	}

	start := len(dst)
	next := pos
	resumed := false

	// stranded is what to do when the log a position names is gone, or has been
	// written over by a merge. The number the position carries may still find
	// the record it named, wherever that record lives now, which saves the
	// follower a whole snapshot. It is tried once, and only for the position
	// the follower actually asked with.
	stranded := func() bool {
		if resumed || next != pos {
			return false
		}
		repaired, ok := db.resumeAt(pos)
		if !ok {
			return false
		}
		resumed, next = true, repaired
		return true
	}

	for {
		// However full the batch already is, a log that has just been entered
		// gives up one record, or the position would rest at the start of it
		// where nothing can check it. Both batch methods promise that, so the
		// room asked for here may be nothing at all.
		room := size - int64(len(dst)-start)

		if next.Segment == db.active.segID {
			// The log being written is the end of the line.
			taken, log, err := db.active.kvs.batch(next.Log, room, dst)
			if err != nil {
				if errors.Is(err, ErrorDiverged) && stranded() {
					dst = dst[:start]
					continue
				}
				return dst[:start], pos, err
			}
			return taken, DBPosition{Term: db.term, Segment: next.Segment, Log: log}, nil
		}

		seg := db.frozenSegment(next.Segment)
		if seg == nil {
			if stranded() {
				continue
			}
			return dst[:start], pos, ErrorDiverged
		}

		taken, log, err := seg.batch(next.Log, room, dst)
		if err != nil {
			if errors.Is(err, ErrorDiverged) && stranded() {
				dst = dst[:start]
				continue
			}
			return dst[:start], pos, err
		}
		dst = taken
		next = DBPosition{Term: db.term, Segment: next.Segment, Log: log}

		if log.Offset < seg.bytes {
			return dst, next, nil
		}

		// This log is finished, so carry on into the next one — but only if
		// there is something in it to read. The end of this log names its last
		// record and can be checked later; the start of the next names nothing
		// at all.
		//
		// Waiting at the start of the log being written would be checkable by
		// nothing, and that log is certain to freeze eventually. Waiting at the
		// end of this one is checkable, and only a merge that takes this log
		// disturbs it. Both leave the same narrow window — a follower that
		// pauses across a rotation — but only one of them can tell afterwards
		// whether the log is still the log it was, and a spurious snapshot
		// costs less than a silent wrong answer.
		after, ok := db.segmentAfter(next.Segment)
		if !ok {
			return dst, next, nil
		}
		if after == db.active.segID && db.active.size() == 0 {
			return dst, next, nil
		}
		next = DBPosition{Term: db.term, Segment: after}
	}
}

// resumeAt finds where a position carries on when the log it named is gone.
//
// A follower that was away while merging happened is holding a position into a
// log that has been folded into another one, and until the records were numbered
// there was nothing to look for: an offset into a log that no longer exists says
// nothing about the log that replaced it. The number says plenty. What the
// follower needs is the records numbered from its position onwards, and a merge
// keeps the numbers on everything it carries across, so the place to carry on
// from can be found by reading.
//
// What comes back is a position naming a real record in a log this store still
// has, so everything downstream carries on as if the follower had never been
// away — and nothing numbered below the follower's position is ever sent, which
// matters more than it looks: an old record applied after a newer one would land
// in a newer log on the follower and shadow it.
//
// What is checked, and what is not. If the record the position names is still
// there, its checksum has to be the one the position carries, and a mismatch is
// a different history and is refused. If a merge dropped that record — because
// something newer superseded it, which is the ordinary fate of a record in a
// busy store — there is nothing left to check it against, and the number is
// taken at its word. The term has already scoped it to one leader by then, which
// is what makes a number worth taking at its word at all.
//
// It refuses in these cases, each costing the follower the snapshot it would
// have needed before any of this existed:
//
//   - A position with no number, from a store written before there were any.
//   - A number no log reaches, which is a follower ahead of this store.
//   - A log at or after the resume point that dropped records. A merge that
//     reaches the oldest log drops tombstones and expired records, and a
//     follower carried across one would never hear that a key was deleted — it
//     holds an older value for that key and nothing in what follows would
//     replace it. That is the one way this could go quietly wrong, so it is
//     checked before anything is sent.
//   - A resume point at the very start of a log, which names no record for the
//     position to be checked against later.
//
// Callers must hold db.mu.
func (db *DB) resumeAt(pos DBPosition) (DBPosition, bool) {
	// A position at the start of a log names no record, and one with no number
	// gives nothing to look for.
	if pos.Log.Seq == 0 || pos.Log.Offset == 0 {
		return DBPosition{}, false
	}

	// Oldest first: the first log whose numbers reach the follower's is the one
	// to carry on in, since a log's records are older than the logs after it.
	for i := len(db.frozen) - 1; i >= 0; i-- {
		seg := db.frozen[i]
		if seg.maxSeq < pos.Log.Seq {
			continue
		}

		// This log and every log after it, since the stream runs through all
		// of them.
		for _, later := range db.frozen[:i+1] {
			if later.dropped {
				return DBPosition{}, false
			}
		}

		log, ok, older := seg.resumeIn(pos.Log.Seq, pos.Log.Crc)
		if older {
			continue // this log ends before the follower does: try the next
		}
		if !ok {
			return DBPosition{}, false
		}
		return DBPosition{Term: db.term, Segment: seg.segID, Log: log}, true
	}

	// Or the log being written, which nothing has merged and which drops
	// nothing.
	if db.active != nil {
		if log, ok, _ := db.active.kvs.resumeIn(pos.Log.Seq, pos.Log.Crc); ok {
			return DBPosition{Term: db.term, Segment: db.active.segID, Log: log}, true
		}
	}

	return DBPosition{}, false
}

// resumeIn finds the place in this log just before the first record numbered
// want, and reports it as a position. See resumeAt for what that means and what
// is checked.
//
// It reads the log to find it. That is a scan of one log, paid once by a
// follower that has been away, against the whole store it would otherwise be
// sent — which is why there is no index from numbers to offsets. Build one in
// the hint if a store ever spends its time resuming.
func (d *diskSegment) resumeIn(want uint64, crc uint32) (found Position, ok, older bool) {
	var before Record
	var at, end int64
	var any bool

	older = true // until a record the follower has not got turns up

	d.scan(func(pos int64, raw []byte, r Record) bool {
		if r.Seq < want {
			before, at, end, any = r, pos, pos+int64(len(raw)), true
			return true
		}
		older = false

		// The first record the follower has not got. The one before it is what
		// the position names, and if that is the record the follower last took
		// then its checksum has to agree.
		if !any {
			// Everything this log holds is newer than the follower, so the
			// place to carry on is the start of it. That names no record and
			// could not be checked if it had come off a wire — but this was
			// worked out here, from a log this store is holding open, and what
			// came before it has been merged away rather than skipped.
			found, ok = Position{Seq: want}, true
			return false
		}
		if before.Seq == want-1 && before.Crc != crc {
			return false
		}
		found, ok = Position{Offset: end, Last: at, Crc: before.Crc, Seq: want}, true
		return false
	})

	return found, ok, older
}

// resumeIn is the same over the log being written, which is in memory.
func (kvs *KeyValueStore) resumeIn(want uint64, crc uint32) (found Position, ok, older bool) {
	kvs.RLock()
	defer kvs.RUnlock()

	var before Record
	var at, end int64
	var any bool

	older = true

	kvs.scan(func(pos, next int64, r Record) bool {
		if r.Seq < want {
			before, at, end, any = r, pos, next, true
			return true
		}
		older = false

		if !any {
			found, ok = Position{Seq: want}, true
			return false
		}
		if before.Seq == want-1 && before.Crc != crc {
			return false
		}
		found, ok = Position{Offset: end, Last: at, Crc: before.Crc, Seq: want}, true
		return false
	})

	return found, ok, older
}

// frozenSegment returns the frozen log with this id, or nil. Callers must hold
// db.mu.
func (db *DB) frozenSegment(id uint64) *diskSegment {
	for _, seg := range db.frozen {
		if seg.id() == id {
			return seg
		}
	}
	return nil
}

// segmentAfter returns the id of the log that follows this one, which is the
// smallest id larger than it. Ids only ever increase, so that is the next log
// by age — but they are not consecutive, since a merge takes the id of the
// oldest log it replaces and the rest of that run's ids go with it. Callers
// must hold db.mu.
func (db *DB) segmentAfter(id uint64) (uint64, bool) {
	after, found := uint64(0), false

	for _, seg := range db.frozen {
		if seg.id() > id && (!found || seg.id() < after) {
			after, found = seg.id(), true
		}
	}
	if db.active != nil && db.active.segID > id && (!found || db.active.segID < after) {
		after, found = db.active.segID, true
	}

	return after, found
}

// Changed returns a channel closed the next time this store changes: a write, a
// delete, or a rotation that ends the log a follower is reading. Follow waits on
// this; it is exported for a leader that sends its records somewhere this
// package cannot write to.
//
// A follower cannot wait on the active log's own channel instead, because
// rotation replaces that log with a different one and the channel goes with it.
func (db *DB) Changed() <-chan struct{} {
	for {
		if waiting := db.waiters.Load(); waiting != nil {
			return *waiting
		}

		fresh := make(chan struct{})
		if db.waiters.CompareAndSwap(nil, &fresh) {
			return fresh
		}
	}
}

// notify wakes whatever is waiting on Changed. Unlike the single-store one this
// is not called under a lock that excludes another caller, so the swap is what
// makes sure a channel is closed once.
func (db *DB) notify() {
	if waiting := db.waiters.Swap(nil); waiting != nil {
		close(*waiting)
	}
}

// batch copies the records after pos out of a frozen log, up to size bytes of
// them. It is the same cut Since makes over a store held in memory, made
// against a file instead.
func (d *diskSegment) batch(pos Position, size int64, dst []byte) ([]byte, Position, error) {
	if pos.Offset > d.bytes {
		return dst, pos, ErrorDiverged
	}

	// Where the follower says it has got to has to be a place this log has
	// actually been. A merge writes its output over the oldest log it replaces,
	// so a log keeps its name while becoming something else entirely, and this
	// is what tells the difference. An offset of zero names no record.
	if pos.Offset != 0 {
		record, raw, err := readRecordAt(d.file, d.bytes, pos.Last)
		if err != nil || pos.Last+int64(len(raw)) != pos.Offset || record.Crc != pos.Crc {
			return dst, pos, ErrorDiverged
		}
	}

	if pos.Offset == d.bytes {
		return dst, pos, nil
	}

	want := d.bytes - pos.Offset
	if want > size {
		want = size
	}
	if want < 0 {
		// A batch that is already over its size still takes one record, below.
		want = 0
	}

	start := len(dst)
	dst = append(dst, make([]byte, want)...)
	if _, err := io.ReadFull(io.NewSectionReader(d.file, pos.Offset, want), dst[start:]); err != nil {
		return dst[:start], pos, err
	}

	// Cut at a boundary a follower can rest on: the end of a record, or the end
	// of a whole write batch. What was read may stop inside either.
	next := pos
	for next.Offset-pos.Offset < want {
		end, record, at, ok := unitAt(dst[start:], next.Offset-pos.Offset)
		if !ok {
			break // the rest of this record, or of this batch, did not fit
		}
		next = Position{Offset: pos.Offset + end, Last: pos.Offset + at, Crc: record.Crc, Seq: after(record.Seq)}
	}

	// Not one whole unit fitted, which happens when a record — or a batch — is
	// larger than a batch of the log. It has to go anyway, or a log holding one
	// could never be replicated at all.
	if next == pos {
		raw, record, at, err := d.unit(pos.Offset)
		if err != nil {
			return dst[:start], pos, err
		}
		dst = append(dst[:start], raw...)
		return dst, Position{Offset: pos.Offset + int64(len(raw)), Last: pos.Offset + at, Crc: record.Crc, Seq: after(record.Seq)}, nil
	}

	return dst[:start+int(next.Offset-pos.Offset)], next, nil
}

// unit reads one record, or one whole write batch, out of a frozen log,
// starting at pos. It is what the cut above falls back on when not even one of
// them fitted in the room a batch had left.
//
// The batch is read whole, which is a batch's worth of memory — the same amount
// the store that wrote it needed to build it, so it is not a new bound.
func (d *diskSegment) unit(pos int64) (raw []byte, last Record, at int64, err error) {
	record, head, err := readRecordAt(d.file, d.bytes, pos)
	if err != nil {
		return nil, Record{}, 0, err
	}
	if record.Type != RecordTypeBatch {
		return head, record, 0, nil
	}

	span, ok := markerSpan(record)
	if !ok || span > d.bytes-(pos+int64(len(head))) {
		return nil, Record{}, 0, &CorruptAtError{Offset: pos}
	}

	raw = make([]byte, int64(len(head))+span)
	if _, err := d.file.ReadAt(raw, pos); err != nil {
		return nil, Record{}, 0, err
	}

	end, last, at, ok := unitAt(raw, 0)
	if !ok || end != int64(len(raw)) {
		return nil, Record{}, 0, &CorruptAtError{Offset: pos}
	}
	return raw, last, at, nil
}

// Applied is the leader position this store has taken records up to, for a
// store being used as a follower. It is the zero position for one that has
// never applied anything, which is a follower that needs a snapshot.
//
// Unlike a single store, a DB cannot work this out from its own log: its files
// have nothing to do with the leader's. It is written down beside the logs
// instead, and read back when the store is opened.
func (db *DB) Applied() DBPosition {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.applied
}

// ErrorSuperseded is returned by Reached for a position cut by a leader this
// store has since stopped following. Whether the record it names survived the
// handover cannot be told from here — a failover keeps what the new leader had,
// which is everything it had received and nothing it had not — and a position
// in the old leader's logs says nothing about the new one's. Take a fresh
// position from the leader there is now.
const ErrorSuperseded = Error("position is from a leader that has been replaced")

// Reached reports whether this store holds every record up to pos: nil if it
// does, ErrorStale if it is behind, and ErrorSuperseded if pos came from a
// leader this store has stopped following.
//
// It is the single-store Reached one level up, and what a replica behind a load
// balancer answers a read with. Write to the leader, take its Position, hand it
// back with the next read, and a replica that has not caught up refuses rather
// than answering out of an older copy of the store. Await is the same question
// with waiting, which is usually what a read wants: a client reading its own
// write is a few milliseconds ahead of the stream, not minutes.
//
// What is compared here is two positions in one leader's stream — its log ids,
// which rise as it rotates, and the offsets within a log. Nothing checks the
// record: a DB follower holds none of the leader's bytes, its own files having
// nothing to do with them, so a position is something the leader said rather
// than something this store can look up. Apply is where a position is checked
// against the records it names, and a position from somewhere that was never
// this store's leader is a caller error nothing here can catch.
//
// Which position it is compared against is the whole of the term handling. A
// store that is following judges by how far it has applied; one that follows
// nobody — because it never has, or because Promote raised it above the term it
// last applied at — is the leader those positions were cut by, and judges by its
// own. A term this store has not heard of is a leader it has heard nothing from,
// so it is behind by definition.
//
// The boundary between two logs is what the numbers are for. A position at the
// start of a log names no record, and the end of the log before it is the same
// point in the stream — which offsets and log ids cannot say, since neither
// position knows how long that log was, and a leader hands out the start of a
// log every time it rotates or is snapshotted. Both carry the same number, so
// both compare equal. A store holding records from before there were numbers
// falls back to comparing log ids and offsets, and is cautious at that one
// boundary: it reports ErrorStale while holding everything, until one more
// record crosses.
//
// This says nothing about whether the store should be read from at all. A fenced
// store still holds what it holds, and a replica is behind its leader by
// definition; being at a position is the only claim made here.
func (db *DB) Reached(pos DBPosition) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrorClosed
	}
	return db.reached(pos)
}

// reached is Reached with db.mu held.
func (db *DB) reached(pos DBPosition) error {
	// The zero position names nothing, and every store holds that much.
	if pos == (DBPosition{}) {
		return nil
	}
	if pos.Term > db.term {
		return ErrorStale
	}

	here := db.applied
	switch {
	case here == DBPosition{}, pos.Term > here.Term:
		// This store follows nobody at that term — it never has, or Promote
		// raised it past the term it last applied at — so the positions at
		// that term are the ones it cut itself, and its own log is what they
		// are offsets into.
		here = db.position()
	case pos.Term < here.Term:
		return ErrorSuperseded
	}

	// The numbers, when both ends have them. They are the only thing here that
	// compares across logs: the end of one log and the start of the next are
	// the same point in the stream and carry the same number, where their
	// offsets have nothing in common and their log ids differ by one.
	if here.Log.Seq != 0 && pos.Log.Seq != 0 {
		if here.Log.Seq < pos.Log.Seq {
			return ErrorStale
		}
		return nil
	}

	// A store written before records were numbered, or a position cut by one.
	// The log ids rise as a leader rotates and the offsets rise within a log,
	// which orders every pair of positions except the one across a boundary —
	// where this says stale while holding everything, until one more record
	// crosses. That is the cautious way round, and it is why the numbers exist.
	if here.Segment != pos.Segment {
		if here.Segment > pos.Segment {
			return nil
		}
		return ErrorStale
	}
	if here.Log.Offset < pos.Log.Offset {
		return ErrorStale
	}
	return nil
}

// Await is Reached with waiting: it returns nil once this store holds
// everything up to pos, ErrorStale if until is closed first, and anything else
// Reached reports at once, since a position from a leader that has been
// replaced is not one waiting will bring.
//
// A read that waits is a read that costs the client the replication lag rather
// than the truth. Give it a deadline — a context's Done channel is the usual
// one — since a follower that has fallen a long way behind, or is following
// nothing at all, will not arrive on this side of it.
func (db *DB) Await(pos DBPosition, until <-chan struct{}) error {
	for {
		// Taken before the position is asked about and not after, so that a
		// batch applied in between is a wake-up rather than a wait for the
		// next one. See Changed.
		changed := db.Changed()

		if err := db.Reached(pos); !errors.Is(err, ErrorStale) {
			return err
		}

		select {
		case <-changed:
		case <-until:
			return ErrorStale
		}
	}
}

// Apply appends one batch of a leader's records to this store and records that
// it has reached next. from is the position the batch was cut for; Apply
// reports ErrorPosition if this store has applied something else since.
//
// A batch is all or nothing here, which is the difference from the single-store
// Apply. There the follower's own log is the leader's log, so the position of
// half a batch is a fact about the bytes it holds; here the position is
// something the leader said, and it describes the whole batch or none of it. A
// batch that is damaged or ends part way through a record is refused entirely
// and the store is left where it was, ready for the same batch again.
//
// The records go down before the position that claims them. A crash in between
// leaves the store having applied records it does not admit to, and the same
// batch arrives again — the same records, in the same order, so what they say
// is unchanged and only the bytes are spent twice. The other order would claim
// records that were never written, which is the one that loses data.
func (db *DB) Apply(from, next DBPosition, r io.Reader, opts ReplicaOptions) (DBPosition, error) {
	batch, err := readBatch(r, opts.batchSize())
	if err != nil {
		return db.Applied(), err
	}

	index, good, last, damaged := verifyRecords(batch, 0)
	if damaged != nil {
		return db.Applied(), damaged
	}
	if good != int64(len(batch)) {
		// Whole records as far as it goes and then part of one, which for a
		// batch that has to arrive entire is a batch that did not.
		return db.Applied(), &CorruptAtError{Offset: good}
	}

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return db.applied, ErrorClosed
	}
	// A leader below the highest term this store has heard of has been
	// replaced, and its records are not to be taken.
	if next.Term < db.seen {
		here := db.applied
		db.mu.RUnlock()
		return here, ErrorFenced
	}
	if db.applied != from {
		here := db.applied
		db.mu.RUnlock()
		return here, ErrorPosition
	}
	active := db.active
	db.mu.RUnlock()

	if good > 0 {
		if err := active.take(batch, index, last); err != nil {
			return db.Applied(), err
		}
	}

	if err := db.advance(next.Term, next); err != nil {
		return db.Applied(), err
	}

	// Applying is how a follower's store changes, so it is one of the things
	// Changed exists to report: anything waiting to read its own write here,
	// and anything following this store in turn, is waiting on that channel.
	db.notify()

	// The records are stored; rotating is housekeeping, as it is after a write.
	db.rotateIfFull(active)
	return next, nil
}

// ApplySnapshot replaces everything this store holds with the records in r, and
// records that it is now at the position the snapshot was taken from. It is the
// other half of Snapshot, and what a follower does to start or to start again.
//
// The records are applied as they arrive rather than gathered up, so a snapshot
// of a store larger than memory costs no more than a small one — which is the
// whole reason DB exists and would be given away by reading it in one piece.
//
// The store is emptied first, and the position is written down last. A failure
// anywhere in between leaves a store holding some of a snapshot and admitting
// to no position at all, which is a follower that needs another one: the same
// place it started.
func (db *DB) ApplySnapshot(at DBPosition, r io.Reader, opts ReplicaOptions) error {
	db.mu.RLock()
	stale := at.Term < db.seen
	db.mu.RUnlock()

	if stale {
		return ErrorFenced
	}
	if err := db.Reset(); err != nil {
		return err
	}
	if err := db.applyStream(r, opts.batchSize()); err != nil {
		return err
	}
	if err := db.advance(at.Term, at); err != nil {
		return err
	}

	db.notify()
	return nil
}

// applyStream reads records from r and appends them to the log being written, a
// group at a time as they arrive. What is applied together is what arrived
// together, as for a single store.
func (db *DB) applyStream(r io.Reader, limit int64) error {
	bufp := applyBuffers.Get().(*[]byte)
	buf := (*bufp)[:0]
	defer func() {
		*bufp = buf
		applyBuffers.Put(bufp)
	}()

	for {
		if len(buf) == cap(buf) {
			buf = growBuffer(buf, limit)
		}

		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]

		if n > 0 {
			index, good, last, damaged := verifyRecords(buf, 0)
			if damaged != nil {
				return damaged
			}

			if good > 0 {
				db.mu.RLock()
				if db.closed {
					db.mu.RUnlock()
					return ErrorClosed
				}
				active := db.active
				db.mu.RUnlock()

				if err := active.take(buf[:good], index, last); err != nil {
					return err
				}
				db.rotateIfFull(active)
			}
			buf = append(buf[:0], buf[good:]...)
		}

		if err != nil {
			// A record that stops half way is a torn stream, not a record.
			if len(buf) > 0 {
				return &CorruptAtError{Offset: int64(len(buf))}
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// take appends verified records to the log being written. It is the write path
// without the writing: the records cross unchanged, keeping the timestamps the
// leader gave them, which going through Write would replace.
func (m *memSegment) take(batch []byte, index map[string]int64, last int64) error {
	m.kvs.Lock()
	defer m.kvs.Unlock()

	if state := m.kvs.state; state != nil && state.closed {
		return ErrorClosed
	}
	return m.kvs.takeRecords(batch, index, last)
}

// advance writes down which leader this store is following and how far through
// it the store has got, in that one write of the file beside the logs.
//
// The term goes with the position and not before it. They are one fact from two
// sides — which leader, and where in it — and anything reading both has to be
// given them together or be handed one leader's offsets under another leader's
// term. Reached is that reader: it tells a store that has been promoted from
// one that is following by whether its term has gone past the term it last
// applied at, and writing the two separately leaves a window where a follower
// looks promoted — a window a crash between the writes leaves open until the
// next batch arrives. One write closes it, and saves the follower a write.
//
// It waits for the disk exactly when the records did. The position must never
// be more durable than the records it claims: under SyncNever the records are
// with the operating system and no further, and a position synced past them
// would leave a store that survived losing power claiming records that did not.
// Losing the position instead costs a batch applied twice, which is the same
// records in the same order.
func (db *DB) advance(term uint64, pos DBPosition) error {
	// One writer of the state file at a time, and it decides what to write
	// with the lock already held, as Promote and noteTerm do.
	db.stateMu.Lock()
	defer db.stateMu.Unlock()

	db.mu.RLock()
	mine, seen := db.term, db.seen
	db.mu.RUnlock()

	// A follower takes the term of the leader it follows, so that a follower of
	// a promoted leader is fenced against the one it replaced. Raising its own
	// term costs it nothing: it is not taking writes either way, and Promote is
	// what makes it a leader. Terms only ever go up.
	if term > mine {
		mine = term
	}
	if term > seen {
		seen = term
	}

	if err := writeReplicaState(db.dir, mine, seen, pos, db.opts.Sync == SyncAlways); err != nil {
		return err
	}

	db.mu.Lock()
	db.term, db.seen, db.applied = mine, seen, pos
	db.mu.Unlock()
	return nil
}

// Reset empties the store: every log goes, and so does the record of how far
// through a leader it had got. It is what a follower does when a leader answers
// ErrorDiverged, since there is no offset the two agree on and the only way
// forward is a new snapshot.
//
// Everything the store held is gone.
func (db *DB) Reset() error {
	// A merge must not be running over logs that are about to be removed, and
	// this is the lock order the rest of the package uses.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrorClosed
	}

	// The record of where this store had got to goes first, and nothing else
	// happens if it cannot go.
	//
	// Removing the logs and then failing to remove this would leave a store
	// that comes back claiming a stretch of a leader it has just deleted — and
	// that is the one kind of wrong nothing downstream can notice. The leader is
	// asked for what comes after a position it recognises, answers correctly,
	// and the follower settles a whole snapshot short with every check passing.
	// It is the same rule as writing the records before the position that claims
	// them, turned round: delete the claim before the thing claimed.
	if err := disk.Remove(filepath.Join(db.dir, appliedFile)); err != nil {
		return err
	}
	db.applied = DBPosition{}

	err := db.closeSegments()
	db.active, db.frozen = nil, nil

	entries, readErr := disk.ReadDir(db.dir)
	if readErr != nil {
		return readErr
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(name, segmentSuffix) && !strings.HasSuffix(name, hintSuffix) {
			continue
		}

		path := filepath.Join(db.dir, name)
		rerr := disk.Remove(path)

		// A log that will not go is emptied, for the reason a merge empties
		// one: this store has forgotten it, and a forgotten file is read back
		// the next time the directory is. Here it would answer for keys the
		// snapshot about to arrive does not hold — which is every key the
		// leader has deleted since — and bring them back from the dead.
		if rerr != nil && strings.HasSuffix(name, segmentSuffix) {
			if eerr := emptyLog(path); eerr == nil {
				rerr = nil
			}
		}
		if err == nil {
			err = rerr
		}
	}

	db.notify()

	if serr := db.start(db.nextID); err == nil {
		err = serr
	}
	return err
}

// readBatch reads a whole batch into a buffer. The caller has framed it, so it
// is bounded by whatever the leader was asked for; the limit here only decides
// how much is taken in one read.
func readBatch(r io.Reader, limit int64) ([]byte, error) {
	bufp := applyBuffers.Get().(*[]byte)
	buf := (*bufp)[:0]
	defer func() {
		*bufp = buf
		applyBuffers.Put(bufp)
	}()

	for {
		if len(buf) == cap(buf) {
			buf = growBuffer(buf, limit)
		}

		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]

		if err != nil {
			if errors.Is(err, io.EOF) {
				return append([]byte(nil), buf...), nil
			}
			return nil, err
		}
	}
}

// The replication state, written beside the logs: the term this store is at and
// how far through a leader it has got. It is small and rewritten constantly, so
// it goes to one side and is renamed into place: one that exists is one that was
// finished.
//
// A damaged or missing one means no position and no term, which costs a snapshot
// and never a wrong answer — the same bargain a hint makes. Losing a term the
// safe way round means going back to zero, so a store that was fenced starts
// taking writes again; a term is a claim about the world, not a fact about the
// records, and there is nowhere else to recover it from.
const (
	appliedFile    = "replica"
	appliedMagic   = "LKVR"
	appliedVersion = 4

	// magic, version, the term, the highest term heard of, the position, and a
	// checksum over all of it.
	appliedSize = 4 + 1 + 8 + 8 + dbPositionSize + 4
)

func writeReplicaState(dir string, term, seen uint64, pos DBPosition, durable bool) error {
	encoded, err := pos.MarshalBinary()
	if err != nil {
		return err
	}

	var buf [appliedSize]byte
	copy(buf[0:4], appliedMagic)
	buf[4] = appliedVersion
	binary.LittleEndian.PutUint64(buf[5:13], term)
	binary.LittleEndian.PutUint64(buf[13:21], seen)
	copy(buf[21:21+dbPositionSize], encoded)
	binary.LittleEndian.PutUint32(buf[21+dbPositionSize:], crc32.ChecksumIEEE(buf[:21+dbPositionSize]))

	path := filepath.Join(dir, appliedFile)
	temp := path + mergeSuffix

	file, err := disk.Open(temp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	failed := func(err error) error {
		file.Close()
		disk.Remove(temp)
		return err
	}

	if _, err := file.Write(buf[:]); err != nil {
		return failed(err)
	}
	if durable {
		if err := file.Sync(); err != nil {
			return failed(err)
		}
	}
	if err := file.Close(); err != nil {
		disk.Remove(temp)
		return err
	}
	if err := disk.Rename(temp, path); err != nil {
		disk.Remove(temp)
		return err
	}

	if durable {
		syncDir(dir)
	}
	return nil
}

// readReplicaState reads that back, and reports nothing for a file that is not
// there or cannot be trusted.
func readReplicaState(dir string) (term, seen uint64, pos DBPosition) {
	raw, err := disk.ReadFile(filepath.Join(dir, appliedFile))
	if err != nil || len(raw) != appliedSize {
		return 0, 0, DBPosition{}
	}
	if string(raw[0:4]) != appliedMagic || raw[4] != appliedVersion {
		return 0, 0, DBPosition{}
	}
	if binary.LittleEndian.Uint32(raw[21+dbPositionSize:]) != crc32.ChecksumIEEE(raw[:21+dbPositionSize]) {
		return 0, 0, DBPosition{}
	}

	if err := pos.UnmarshalBinary(raw[21 : 21+dbPositionSize]); err != nil {
		return 0, 0, DBPosition{}
	}

	term = binary.LittleEndian.Uint64(raw[5:13])
	seen = binary.LittleEndian.Uint64(raw[13:21])
	if seen < term {
		seen = term
	}
	return term, seen, pos
}
