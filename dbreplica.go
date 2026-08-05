package litekv

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
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

// Follow writes the records after pos to w and goes on writing them as they are
// written to this store, until until is closed or w stops taking them.
//
// A slow follower does not slow this store down: each batch is copied out under
// the lock and written to w outside it.
func (db *DB) Follow(pos DBPosition, w io.Writer, until <-chan struct{}, opts ReplicaOptions) (DBPosition, error) {
	for {
		// Before asking, never after: a record written in between closes the
		// channel already in hand, so the wait ends at once.
		changed := db.Changed()

		for {
			next, err := db.Since(pos, w, opts)
			if err != nil {
				return pos, err
			}
			if next == pos {
				break
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
// there means every position a follower is ever handed names a record, and so
// can be checked. The cost is that a batch may overshoot its size by one record
// at each log it crosses.
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
		// there is something in it to read. Resting at the end of this log
		// names its last record and can be checked later; resting at the start
		// of the next names nothing at all, and a follower that paused there
		// while the log filled and froze could not be told whether it was still
		// the same log.
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
