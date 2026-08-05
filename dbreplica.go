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
	// Segment is which of the leader's logs, by the id in its filename.
	Segment uint64

	// Log is where in that log, exactly as for a single store.
	Log Position
}

// dbPositionSize is what a DBPosition takes on the wire.
const dbPositionSize = 8 + positionSize

// MarshalBinary encodes the position in twenty-eight bytes, little-endian.
func (p DBPosition) MarshalBinary() ([]byte, error) {
	log, err := p.Log.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, dbPositionSize)
	binary.LittleEndian.PutUint64(buf[0:8], p.Segment)
	copy(buf[8:], log)
	return buf, nil
}

// UnmarshalBinary decodes a position encoded by MarshalBinary, checking it
// rather than believing it, as the single-log one does.
func (p *DBPosition) UnmarshalBinary(data []byte) error {
	if len(data) != dbPositionSize {
		return fmt.Errorf("litekv: db position is %d bytes, not %d", len(data), dbPositionSize)
	}

	var q DBPosition
	if err := q.Log.UnmarshalBinary(data[8:]); err != nil {
		return err
	}
	q.Segment = binary.LittleEndian.Uint64(data[0:8])

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
	return DBPosition{Segment: db.active.segID, Log: db.active.kvs.Position()}
}

// Snapshot writes the store's live records to w and returns the position they
// are current as of. It is how a follower starts: apply the snapshot, then
// stream from the position it came with.
//
// What crosses is one record per live key — the newest version of it — and
// nothing else. Superseded records do not go, and neither do tombstones, since
// a follower starting from nothing has no older value for one to hide.
//
// The snapshot is consistent without stopping the store. The active log is
// frozen first, so everything the snapshot covers is on the disk and can no
// longer change, and the position it reports is the start of a log with nothing
// in it yet: whatever is written from here on is the tail rather than part of
// the snapshot. Writes and rotation carry on throughout. Merging does not — a
// merge may remove a log, and this is reading them — so a snapshot of a large
// store holds merging off for as long as it takes.
func (db *DB) Snapshot(w io.Writer, opts ReplicaOptions) (DBPosition, error) {
	// No merge may take a log away while it is being read out. This is also
	// the lock order the rest of the package uses: mergeMu, then db.mu.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	at, frozen, err := db.freezeForSnapshot()
	if err != nil {
		return DBPosition{}, err
	}

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
			if record.Type != RecordTypeNormal {
				return true
			}
			if _, err := writer.Write(raw); err != nil {
				failed = err
				return false
			}
			return true
		})

		if failed != nil {
			return DBPosition{}, failed
		}
	}

	if err := writer.Flush(); err != nil {
		return DBPosition{}, err
	}
	return at, nil
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
	at := DBPosition{Segment: db.active.segID, Log: db.active.kvs.Position()}

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

// Follow hands the records after pos to send, and goes on handing them over as
// they are written to this store, until until is closed or send reports an
// error. It returns the position it had got as far as.
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
func (db *DB) Follow(pos DBPosition, send func(batch []byte, next DBPosition) error, until <-chan struct{}, opts ReplicaOptions) (DBPosition, error) {
	bufp := sendBuffers.Get().(*[]byte)
	defer func() { sendBuffers.Put(bufp) }()

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

	// The one position that cannot be checked. A snapshot of a store whose
	// active log was empty has nowhere to point but the start of that log, and
	// if it fills and freezes before the follower asks for anything, there is no
	// record to say whether it is still the log it was. It is refused, and the
	// follower takes another snapshot exactly as if the log had been removed.
	// Every other position names a record, which is what batch goes out of its
	// way to arrange.
	if pos.Log == (Position{}) && pos.Segment != db.active.segID {
		if seg := db.frozenSegment(pos.Segment); seg == nil || seg.bytes > 0 {
			return dst, pos, ErrorDiverged
		}
	}

	start := len(dst)
	next := pos

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
				return dst[:start], pos, err
			}
			return taken, DBPosition{Segment: next.Segment, Log: log}, nil
		}

		seg := db.frozenSegment(next.Segment)
		if seg == nil {
			return dst[:start], pos, ErrorDiverged
		}

		taken, log, err := seg.batch(next.Log, room, dst)
		if err != nil {
			return dst[:start], pos, err
		}
		dst = taken
		next = DBPosition{Segment: next.Segment, Log: log}

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
		next = DBPosition{Segment: after}
	}
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
	// is what tells the difference.
	if pos != (Position{}) {
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

	// Cut at a record boundary: a follower can only take whole records.
	next := pos
	for next.Offset-pos.Offset < want {
		record, end, err := parseRecordAt(dst[start:], next.Offset-pos.Offset)
		if err != nil {
			break // the rest of this record did not fit in the batch
		}
		next = Position{Offset: pos.Offset + end, Last: next.Offset, Crc: record.Crc}
	}

	// Not one whole record fitted, which happens when a record is larger than a
	// batch. It has to go anyway, or a log holding one could never be
	// replicated at all.
	if next == pos {
		record, raw, err := readRecordAt(d.file, d.bytes, pos.Offset)
		if err != nil {
			return dst[:start], pos, err
		}
		dst = append(dst[:start], raw...)
		return dst, Position{Offset: pos.Offset + int64(len(raw)), Last: pos.Offset, Crc: record.Crc}, nil
	}

	return dst[:start+int(next.Offset-pos.Offset)], next, nil
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

	if err := db.setApplied(next); err != nil {
		return db.Applied(), err
	}

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
	if err := db.Reset(); err != nil {
		return err
	}
	if err := db.applyStream(r, opts.batchSize()); err != nil {
		return err
	}
	return db.setApplied(at)
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

// setApplied writes down how far through the leader this store has got.
//
// It waits for the disk exactly when the records did. The position must never
// be more durable than the records it claims: under SyncNever the records are
// with the operating system and no further, and a position synced past them
// would leave a store that survived losing power claiming records that did not.
// Losing the position instead costs a batch applied twice, which is the same
// records in the same order.
func (db *DB) setApplied(pos DBPosition) error {
	if err := writeApplied(db.dir, pos, db.opts.Sync == SyncAlways); err != nil {
		return err
	}

	db.mu.Lock()
	db.applied = pos
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
		if rerr := disk.Remove(filepath.Join(db.dir, name)); err == nil {
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

// The record of how far through a leader a follower has got, written beside the
// logs. It is small and rewritten constantly, so it goes to one side and is
// renamed into place: one that exists is one that was finished.
//
// A damaged or missing one means no position at all, which costs a snapshot and
// never a wrong answer — the same bargain a hint makes.
const (
	appliedFile    = "replica"
	appliedMagic   = "LKVR"
	appliedVersion = 1

	// magic, version, the position, and a checksum over all of it.
	appliedSize = 4 + 1 + dbPositionSize + 4
)

func writeApplied(dir string, pos DBPosition, durable bool) error {
	encoded, err := pos.MarshalBinary()
	if err != nil {
		return err
	}

	var buf [appliedSize]byte
	copy(buf[0:4], appliedMagic)
	buf[4] = appliedVersion
	copy(buf[5:5+dbPositionSize], encoded)
	binary.LittleEndian.PutUint32(buf[5+dbPositionSize:], crc32.ChecksumIEEE(buf[:5+dbPositionSize]))

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

// readApplied reads that record back, and reports the zero position for one
// that is not there or cannot be trusted.
func readApplied(dir string) DBPosition {
	raw, err := disk.ReadFile(filepath.Join(dir, appliedFile))
	if err != nil || len(raw) != appliedSize {
		return DBPosition{}
	}
	if string(raw[0:4]) != appliedMagic || raw[4] != appliedVersion {
		return DBPosition{}
	}
	if binary.LittleEndian.Uint32(raw[5+dbPositionSize:]) != crc32.ChecksumIEEE(raw[:5+dbPositionSize]) {
		return DBPosition{}
	}

	var pos DBPosition
	if err := pos.UnmarshalBinary(raw[5 : 5+dbPositionSize]); err != nil {
		return DBPosition{}
	}
	return pos
}
