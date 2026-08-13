package litekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Replication here is the log, sent somewhere else.
//
// A store's records are already an ordered, checksummed, append-only stream, so
// a follower holding the first N bytes of a leader's log and given the bytes
// after them holds the same store, record for record. That is the whole idea:
// Position says how far a follower has got, Since hands over what it is
// missing, Apply puts it in, and Changed says when there is more.
//
// Nothing here opens a socket. What crosses the gap is a Position, which
// marshals to twenty-eight bytes, and a run of records; carrying those over TCP,
// HTTP, a pipe or a file is the caller's business and no concern of a storage
// engine. example/ wires one over a connection.
//
// There is no framing to invent, because a record already carries its own
// lengths: a stream of them is its own framing, and a follower reads whatever
// has arrived and applies the whole ones. That is what lets Apply be one call
// for both shapes of transport. Over a reader that ends — an HTTP body, a
// file — it is one batch. Over a connection it runs until the connection does
// not, applying records as they land.
//
// The arrangement is the one the leader-follower chapter of Designing
// Data-Intensive Applications describes, and the one PostgreSQL and MySQL use:
// the follower names a position once, and the leader streams from there.
//
// A follower must not be written to. Its own writes would move it somewhere the
// leader's log does not go, which needs no rule to enforce: Apply checks that
// the batch continues the log the store actually holds, so the write is caught
// by the next batch rather than quietly kept.

// Position is how much of a leader's log a follower holds, and enough about the
// last record in it for the leader to tell whether the two are the same log.
//
// An offset on its own would not be enough. Two stores can both be a thousand
// bytes long and hold entirely different records, and sending the bytes after a
// thousand to the second of them splices one history onto the other and leaves
// a log that decodes perfectly and answers wrongly. So a position also says
// where its last record starts and what that record's checksum is, and a leader
// checks both against its own log before it sends anything. It is the check
// Raft makes with prevLogIndex and prevLogTerm, in the terms this format
// already has.
//
// The zero value is a follower holding nothing, which any leader can fill.
type Position struct {
	// Offset is how many bytes of the leader's log the follower holds, and so
	// where the next record goes.
	Offset int64

	// Last is where the last of those records starts.
	Last int64

	// Crc is that record's checksum.
	Crc uint32

	// Seq is the number the next record written here will carry, which is one
	// past the number of the record at Last. It is zero for a store that does
	// not number its records, which is every KeyValueStore not standing in for
	// a DB's log.
	//
	// It is what makes two positions comparable when their offsets are not.
	// Offsets are only comparable within one log, and a DB has many: the end of
	// one log and the start of the next are the same point in the stream and
	// have nothing in common to say so. Both carry the same number.
	Seq uint64
}

// ErrorDiverged is returned by Since when the position it was given is not a
// point in this store's log: the follower holds records this store never wrote,
// or holds more than it has. There is no offset to carry on from, so the
// follower has to empty itself with Reset and be sent the log from the start.
const ErrorDiverged = Error("follower's log is not part of this one")

// ErrorPosition is returned by Apply when the batch does not continue the log
// the store holds now. A batch that arrived twice, or arrived after something
// else wrote to the store, looks like this. Ask again with the position Apply
// reported.
const ErrorPosition = Error("batch does not continue this log")

// ErrorStale is returned by Reached and Await when this store has not got as
// far as the position it was given. It is not a fault in the store: it is a
// replica saying it does not yet hold a write that something else has already
// acknowledged, which is the honest answer and the only one worth having.
const ErrorStale = Error("store has not reached that position")

// positionSize is what a Position takes on the wire.
const positionSize = 28

// MarshalBinary encodes the position in twenty-eight bytes, little-endian, as records
// are. It is here so that the two ends of a connection do not have to invent an
// encoding for the one thing they both have to agree on.
func (p Position) MarshalBinary() ([]byte, error) {
	var buf [positionSize]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(p.Offset))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(p.Last))
	binary.LittleEndian.PutUint32(buf[16:20], p.Crc)
	binary.LittleEndian.PutUint64(buf[20:28], p.Seq)
	return buf[:], nil
}

// UnmarshalBinary decodes a position encoded by MarshalBinary. A position comes
// off a wire, so it is checked rather than believed: the offsets have to be
// positive and the last record has to start before the log ends.
func (p *Position) UnmarshalBinary(data []byte) error {
	if len(data) != positionSize {
		return fmt.Errorf("litekv: position is %d bytes, not %d", len(data), positionSize)
	}

	q := Position{
		Offset: int64(binary.LittleEndian.Uint64(data[0:8])),
		Last:   int64(binary.LittleEndian.Uint64(data[8:16])),
		Crc:    binary.LittleEndian.Uint32(data[16:20]),
		Seq:    binary.LittleEndian.Uint64(data[20:28]),
	}
	if q.Offset < 0 || q.Last < 0 || (q.Offset != 0 && q.Last >= q.Offset) {
		return fmt.Errorf("litekv: %w: offset %d, last record at %d", ErrorCorruptData, q.Offset, q.Last)
	}

	*p = q
	return nil
}

// Position returns how much of this store's log a follower would have to hold
// to be up to date with it.
//
// A store whose Data slice was replaced by hand reports its position once
// Recover or RebuildIndex has been called, the same as its index does.
func (kvs *KeyValueStore) Position() Position {
	kvs.RLock()
	defer kvs.RUnlock()

	return kvs.position()
}

// position is Position with at least a read lock held.
func (kvs *KeyValueStore) position() Position {
	// The usual case: lastRecord says where the newest record starts, so one
	// header decode confirms it and there is nothing to read.
	if pos, ok := kvs.positionAt(kvs.lastRecord); ok {
		return pos
	}

	// It does not fit, so either something changed Data without saying where
	// the last record moved to, or a crash tore the end off the log. Read the
	// records and find out, which for the second of those reports the log as
	// far as it is intact and leaves the torn tail out of it.
	var found Position
	kvs.scan(func(pos, next int64, r Record) bool {
		found = Position{Offset: next, Last: pos, Crc: r.Crc, Seq: after(r.Seq)}
		return true
	})

	// An empty log still has a place in the stream, and saying so is the whole
	// of what the number is for: the log a DB has just rotated into holds no
	// record to name, and the end of the log it rotated out of holds one. They
	// are the same point, and only the number says so.
	if found == (Position{}) && kvs.numbers {
		found.Seq = kvs.seq
	}
	return found
}

// after is the number the record following one numbered seq would take, and
// zero for a record that carries no number: a store that does not number them
// has no next number either, and saying one would put an unnumbered log ahead
// of the start of a numbered one.
func after(seq uint64) uint64 {
	if seq == 0 {
		return 0
	}
	return seq + 1
}

// positionAt reports the position of a log whose newest record starts at last,
// and whether that is what it is: a record has to decode there, and it has to
// end exactly where the data does.
func (kvs *KeyValueStore) positionAt(last int64) (Position, bool) {
	if last < 0 || last >= int64(len(kvs.Data)) {
		return Position{}, false
	}

	record, next, err := parseRecordAt(kvs.Data, last)
	if err != nil || next != int64(len(kvs.Data)) {
		return Position{}, false
	}
	return Position{Offset: next, Last: last, Crc: record.Crc, Seq: after(record.Seq)}, true
}

// Reached reports whether this store holds every record up to pos: nil if it
// does, ErrorStale if it is behind, and ErrorDiverged if pos names a record
// this log does not hold.
//
// It is what makes reads from a replica safe to promise anything about. Write
// to the leader, take its Position, and hand it back with the next read: a
// replica that has not caught up says so rather than answering out of an older
// copy of the store. That is read-your-writes, and asking with the position of
// the last read instead gives monotonic reads — a client that has seen a value
// never sees the store go backwards, however it is routed. Both are what a
// leader with asynchronous replicas otherwise takes away, and not seeing your
// own write a millisecond after making it is the way this design most often
// surprises people.
//
// The position is checked, not merely compared. A single store's log is the
// leader's log byte for byte, so the record pos names is here to be looked at,
// and looking at it is what tells a replica of that leader from a store that
// happens to be as long. What this does not say is anything about how fresh
// the store is in general: a position is not a clock, and reaching one says
// only that what it names is here.
func (kvs *KeyValueStore) Reached(pos Position) error {
	kvs.RLock()
	defer kvs.RUnlock()

	// A position at the start of the log names no record, and every store holds
	// that much of it.
	if pos.Offset == 0 {
		return nil
	}

	if here := kvs.position(); pos.Offset > here.Offset {
		return ErrorStale
	}

	// Far enough along, but along the same log? This is the check batch makes
	// before a leader sends anything, made against a position that arrived
	// from a client rather than from a follower.
	record, next, err := parseRecordAt(kvs.Data, pos.Last)
	if err != nil || next != pos.Offset || record.Crc != pos.Crc {
		return ErrorDiverged
	}
	return nil
}

// Await is Reached with waiting: it returns nil once this store holds
// everything up to pos, ErrorStale if until is closed first, and anything else
// Reached reports at once, since a position this log does not hold is not one
// waiting will bring.
//
// The channel is taken before the position is asked about and not after, which
// is what makes a record that arrives in between a wake-up rather than a wait
// for the next one. See Changed.
func (kvs *KeyValueStore) Await(pos Position, until <-chan struct{}) error {
	for {
		changed := kvs.Changed()

		if err := kvs.Reached(pos); !errors.Is(err, ErrorStale) {
			return err
		}

		select {
		case <-changed:
		case <-until:
			return ErrorStale
		}
	}
}

// defaultBatch is how much of the log crosses at once when nothing says
// otherwise. It bounds the copy a leader makes and the buffer a follower keeps,
// and since a stream costs no round trip per batch, it is a memory setting
// rather than a latency one.
const defaultBatch = 1 << 20

// ReplicaOptions configures how much of a log moves at a time. The zero value
// sends and buffers a megabyte.
type ReplicaOptions struct {
	// BatchSize is how much of the log a leader hands over at once, and how
	// much a follower buffers while reading. Zero means a megabyte.
	//
	// A record larger than this still crosses whole, since a log holding one
	// could otherwise never be replicated at all, and a follower holds one
	// however large it is.
	BatchSize int64
}

func (o ReplicaOptions) batchSize() int64 {
	if o.BatchSize <= 0 {
		return defaultBatch
	}
	return o.BatchSize
}

// Since writes the records after pos to w, at most one batch of them, and
// returns the position they leave a follower at once it has applied them all.
// It writes nothing and returns pos unchanged when the follower is already up
// to date, which is how a loop knows it has caught up.
//
// This is one batch and then a return, for a transport that answers a request:
// an HTTP handler, or anything where the follower asks each time. Follow is the
// same thing left running, and is what a connection wants.
//
// It returns ErrorDiverged if pos is not a point in this log, which means the
// follower cannot be caught up and has to empty itself with Reset and be sent
// the whole log from Position{}.
//
// The returned position is where the follower lands if the batch reaches it
// whole. What it actually holds is whatever its own Apply reports, which is
// less when the connection between them ends part way; it asks again from
// there.
func (kvs *KeyValueStore) Since(pos Position, w io.Writer, opts ReplicaOptions) (Position, error) {
	bufp := sendBuffers.Get().(*[]byte)
	defer func() { sendBuffers.Put(bufp) }()

	batch, next, err := kvs.batch(pos, opts.batchSize(), (*bufp)[:0])
	*bufp = batch // it may have grown to hold this one

	if err != nil {
		return pos, err
	}
	if len(batch) == 0 {
		return pos, nil
	}

	if _, err := w.Write(batch); err != nil {
		return pos, err
	}
	return next, nil
}

// Follow writes the records after pos to w, and goes on writing them as they
// are written to this store, until until is closed or w stops taking them. It
// returns the position it had sent as far as.
//
// This is replication as it is actually run: the follower names a position
// once, and the leader streams from there. Since is the same thing for a
// transport that asks each time, and costs a round trip a batch for it.
//
// A slow follower does not slow this store down. Each batch is copied out under
// the read lock and written to w outside it, so a writer that blocks holds up
// this stream and nothing else — which is what makes the replication
// asynchronous, and what makes a follower able to fall behind.
//
// A nil until never stops it, which is what a connection handler wants: it ends
// when the connection does, and w reports that.
func (kvs *KeyValueStore) Follow(pos Position, w io.Writer, until <-chan struct{}, opts ReplicaOptions) (Position, error) {
	for {
		// Before asking, never after. A record written in between closes the
		// channel already in hand, so the wait below ends at once rather than
		// missing what it was waiting for.
		changed := kvs.Changed()

		for {
			next, err := kvs.Since(pos, w, opts)
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

// sendBuffers holds the buffers a leader copies a batch into. One connection
// keeps one for as long as it is sending, rather than allocating a batch and
// throwing it away for every one. Nothing points into a buffer after Since has
// written it out, and a pool lets go of what it holds when the collector runs,
// so a buffer grown for one enormous record does not stay grown for good.
var sendBuffers = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, initialApply)
		return &buf
	},
}

// batch copies the records after pos out of the log into dst, up to size bytes
// of them, and reports where they leave a follower that takes them all.
//
// The bytes are copied rather than written to the caller's writer with the lock
// held, because that writer is usually a connection, and every write to this
// store would then be waiting for a network.
func (kvs *KeyValueStore) batch(pos Position, size int64, dst []byte) ([]byte, Position, error) {
	kvs.RLock()
	defer kvs.RUnlock()

	here := kvs.position()
	if pos == here {
		return dst, pos, nil
	}

	// Where the follower says it has got to has to be a place this log has
	// actually been: a record starting there, ending where the follower says its
	// log ends, with the checksum the follower holds. An offset of zero is the
	// start of the log and names no record, whatever else the position carries.
	if pos.Offset != 0 {
		if pos.Offset > here.Offset {
			return dst, pos, ErrorDiverged
		}
		record, next, err := parseRecordAt(kvs.Data, pos.Last)
		if err != nil || next != pos.Offset || record.Crc != pos.Crc {
			return dst, pos, ErrorDiverged
		}
	}

	// Cut at a boundary a follower can rest on: the end of a record, or the end
	// of a whole write batch, since half a batch is the one thing the marker
	// exists to stop anybody holding. One unit always goes, however large, or a
	// log holding a record — or a batch — bigger than a batch of the log could
	// never be replicated at all.
	next := pos
	for next.Offset < here.Offset {
		end, record, at, ok := unitAt(kvs.Data, next.Offset)
		if !ok {
			return dst, pos, &CorruptAtError{Offset: next.Offset}
		}
		if end-pos.Offset > size && next.Offset > pos.Offset {
			break
		}
		next = Position{Offset: end, Last: at, Crc: record.Crc, Seq: after(record.Seq)}
	}

	return append(dst, kvs.Data[pos.Offset:next.Offset]...), next, nil
}

// Apply reads records from a leader and appends them to the store, returning
// the position the store is at afterwards. from is the position the stream
// starts at, which is the position the follower asked with; Apply reports
// ErrorPosition if the store is somewhere else when the first records arrive.
//
// It reads until r ends, so it covers both shapes of transport. Over a reader
// that stops — an HTTP body, a file, a bounded read of a connection — it
// applies one batch and returns. Over a connection carrying a leader's Follow
// it runs until the connection does, applying records as they land, and returns
// how far it got when it fails.
//
// What is applied together is what arrived together: the records already in the
// buffer when a read returns go into the log in one write and are synced once,
// so a follower catching up pays for a batch and one keeping up pays for a
// record. Nothing waits for a buffer to fill, so a follower is never behind by
// something it has already received.
//
// Every record is checked against its own checksum before any of it is kept,
// and a stream that ends part way through a record keeps the whole records
// before it and reports the rest as damaged. Both are reported along with the
// position reached, so a caller that carries on from there loses nothing. A
// leader is not a reason to trust the wire in between.
func (kvs *KeyValueStore) Apply(from Position, r io.Reader, opts ReplicaOptions) (Position, error) {
	limit := opts.batchSize()

	// The buffer starts small and grows towards a batch as records arrive, so a
	// follower taking one record at a time costs one record rather than the
	// largest batch it might ever see. It grows past a batch only for a single
	// record larger than one, which has to fit somewhere.
	bufp := applyBuffers.Get().(*[]byte)
	buf := (*bufp)[:0]
	defer func() {
		*bufp = buf
		applyBuffers.Put(bufp)
	}()

	pos := from

	for {
		if len(buf) == cap(buf) {
			buf = growBuffer(buf, limit)
		}

		// One Read, blocking until something is there, and taking whatever came
		// with it. What arrives together is what the leader wrote together, so
		// the batching is the connection's rather than a timer's, and nothing
		// waits for a buffer to fill.
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]

		if n > 0 {
			next, used, applyErr := kvs.applyWhole(pos, buf)
			pos = next
			if applyErr != nil {
				return pos, applyErr
			}
			buf = append(buf[:0], buf[used:]...)
		}

		if err != nil {
			// A record that stops half way is a torn stream, not a record: for
			// a reader that ends, there is nothing more coming to complete it.
			if len(buf) > 0 {
				return pos, &CorruptAtError{Offset: pos.Offset}
			}
			if errors.Is(err, io.EOF) {
				return pos, nil
			}
			return pos, err
		}
	}
}

// initialApply is the buffer a follower starts with. A record is at least
// thirteen bytes and usually a great deal less than this, so one that is
// keeping up never grows past it.
const initialApply = 4 << 10

// applyBuffers holds those buffers between calls. A follower taking one batch
// per request would otherwise allocate one and throw it away every time, and a
// streaming one keeps its buffer for as long as its connection lasts either
// way. Nothing handed to a caller ever points into one: a record is copied into
// Data, and its key into the index, as it is applied.
var applyBuffers = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, initialApply)
		return &buf
	},
}

// growBuffer makes room to read into: twice what there is, up to a batch, and
// past a batch only when one record needs more than that.
func growBuffer(buf []byte, limit int64) []byte {
	size := max(cap(buf)*2, initialApply)
	if int64(size) > limit && int64(cap(buf)) < limit {
		size = int(limit)
	}

	grown := make([]byte, len(buf), size)
	copy(grown, buf)
	return grown
}

// applyWhole appends the whole records at the front of batch to the store, and
// reports how many bytes of it that was. A record that is only partly there is
// left for the next read; one that is not a record at all is an error.
func (kvs *KeyValueStore) applyWhole(from Position, batch []byte) (Position, int64, error) {
	index, good, last, damaged := verifyRecords(batch, from.Offset)

	kvs.Lock()
	defer kvs.Unlock()

	if state := kvs.state; state != nil && state.closed {
		return kvs.position(), 0, ErrorClosed
	}

	// The stream continues a log this store is no longer at: it arrived twice,
	// or arrived after something else wrote here. Either way none of it applies.
	if here := kvs.position(); here != from {
		return here, 0, ErrorPosition
	}
	if good == 0 {
		return from, 0, damaged
	}

	if err := kvs.takeRecords(batch[:good], index, last); err != nil {
		return from, 0, err
	}
	return kvs.position(), good, damaged
}

// verifyRecords walks the whole records at the front of batch, checking each
// against its own checksum, and reports how many bytes of it are good and where
// the last of those records starts. Offsets in the errors it returns are
// relative to at, which is where the batch sits in whatever it came from.
//
// The index it builds is where each key's newest record in this batch is,
// relative to the start of it. Building it here saves decoding the batch twice,
// and a key written repeatedly within one batch is inserted once.
func verifyRecords(batch []byte, at int64) (index map[string]int64, good, last int64, damaged error) {
	index = make(map[string]int64)

	for good < int64(len(batch)) {
		size, whole, ok := recordLen(batch[good:])
		if !ok {
			damaged = &CorruptAtError{Offset: at + good}
			break
		}
		if !whole {
			break // the rest of it has not arrived
		}

		record, next, err := parseRecordAt(batch, good)
		if err != nil || next != good+size {
			damaged = &CorruptAtError{Offset: at + good}
			break
		}
		if record.Crc != checksumSerialized(batch[good:next]) {
			damaged = fmt.Errorf("record at offset %d: %w", at+good, ErrorChecksumMismatch)
			break
		}

		// A write batch is one thing to a follower as well. Nothing in it
		// counts until all of it has arrived, so the good bytes stop at the
		// marker until then, and a record inside it that will not verify
		// condemns the batch rather than the records before it.
		if record.Type == RecordTypeBatch {
			from, to, ok := spanAt(record, next, int64(len(batch)))
			if !ok {
				if _, whole := markerSpan(record); whole {
					break // the rest of the batch has not arrived
				}
				damaged = &CorruptAtError{Offset: at + good}
				break
			}

			inside, bad := verifyInside(batch[from:to], at+from)
			if bad != nil {
				damaged = bad
				break
			}
			for key, off := range inside.index {
				index[key] = from + off
			}

			last = from + inside.last
			good = to
			continue
		}

		index[string(record.Key)] = good
		last = good
		good = next
	}

	return index, good, last, damaged
}

// verifyInside checks the records of one write batch, which have to be whole
// and intact together or not be taken at all.
func verifyInside(span []byte, at int64) (struct {
	index map[string]int64
	last  int64
}, error) {
	var checked struct {
		index map[string]int64
		last  int64
	}
	checked.index = make(map[string]int64)

	var pos int64
	for pos < int64(len(span)) {
		record, next, err := parseRecordAt(span, pos)
		if err != nil || record.Type == RecordTypeBatch {
			return checked, &CorruptAtError{Offset: at + pos}
		}
		if record.Crc != checksumSerialized(span[pos:next]) {
			return checked, fmt.Errorf("record at offset %d: %w", at+pos, ErrorChecksumMismatch)
		}

		checked.index[string(record.Key)] = pos
		checked.last = pos
		pos = next
	}

	return checked, nil
}

// takeRecords appends records that have already been verified, and points the
// index at them. last is where the last of them starts, and index says where
// each key's newest one is, both relative to the start of batch. Callers must
// hold the write lock.
//
// Data, then the log, then the index, as for a write of the store's own: the
// index points at the records only once both have taken them, so a failure
// leaves the store where it was rather than half caught up.
func (kvs *KeyValueStore) takeRecords(batch []byte, index map[string]int64, last int64) error {
	pos := int64(len(kvs.Data))
	kvs.Data = append(kvs.Data, batch...)

	if state := kvs.state; state != nil {
		if err := kvs.writeToLog(state, kvs.Data[pos:], pos); err != nil {
			kvs.Data = kvs.Data[:pos]
			return err
		}
	}

	if kvs.Index == nil {
		kvs.Index = make(map[string]int64, len(index))
	}
	for key, off := range index {
		kvs.Index[key] = pos + off
	}

	kvs.lastRecord = pos + last

	// The records came numbered by whoever wrote them, and a follower keeps
	// those numbers rather than making its own. What it takes from them is
	// where to carry on from if it is ever promoted, since numbers only go up
	// and reissuing one would put two records in the same place in the stream.
	//
	// The highest of them, not the last of them: a snapshot ships the newest
	// version of every key by asking the newest log first, so what arrives is
	// in no particular order and the last record of a batch is routinely not
	// its highest numbered. See TestFollowerKeepsTheLeadersNumbers.
	kvs.observe(highestSeqIn(batch))

	kvs.notify()
	return nil
}

// highestSeqIn is the largest number carried by the records at the front of
// buf, and zero if none of them carry one. The records have already been
// verified by the time this walks them, so a record that will not decode ends
// it rather than being an error.
func highestSeqIn(buf []byte) uint64 {
	var highest uint64

	for at := int64(0); at < int64(len(buf)); {
		record, next, err := parseRecordAt(buf, at)
		if err != nil {
			break
		}
		if record.Seq > highest {
			highest = record.Seq
		}
		at = next
	}
	return highest
}

// recordLen reports how many bytes the record at the front of buf takes and
// whether all of them are there, and whether it is a record at all.
//
// The distinction is the whole of what makes a stream different from a batch. A
// record that stops half way through a connection's buffer is on its way; the
// same bytes at the end of everything the leader will ever send are damage. The
// caller knows which, because it knows whether more is coming.
func recordLen(buf []byte) (size int64, whole, ok bool) {
	header, decoded := decodeHeader(buf)
	if !decoded {
		// Either not a record, or not all of its fixed part yet. The largest
		// header is the most it could still be waiting for.
		return 0, false, len(buf) < headerSize
	}

	size = header.size + int64(header.keyLength) + int64(header.valueLength)
	return size, int64(len(buf)) >= size, true
}

// Reset empties the store and its log. It is what a follower does when a leader
// answers ErrorDiverged: there is no offset the two logs agree on, so the only
// way forward is to start again from Position{} and be sent the whole of it.
//
// For a store opened by Open this is crash safe, since it goes through the same
// rename Rewrite does. Everything the store held is gone.
func (kvs *KeyValueStore) Reset() error {
	kvs.Lock()
	defer kvs.Unlock()

	kvs.Data = nil
	kvs.Index = make(map[string]int64)
	kvs.lastRecord = 0

	// The numbering is left where it is. Emptying a store does not un-hand-out
	// the numbers it has already given to records that are now somewhere else,
	// and a follower about to be sent a snapshot takes its numbering from the
	// records in it.
	kvs.notify()

	return kvs.rewrite()
}

// Changed returns a channel that is closed the next time the store's log
// changes: a write, a delete, a batch applied, or a compaction that moves every
// record. It is how a leader sends records as they happen rather than asking
// for them on a timer.
//
// Follow does this already, and is what to reach for. Changed is here for a
// leader that sends its records somewhere this package cannot write to.
//
// Take the channel before reading the position, never after. A record written
// in between closes the channel already in hand, so the wait ends at once
// instead of missing what it was waiting for:
//
//	for {
//		changed := leader.Changed()
//		pos, err = leader.Since(pos, w, litekv.ReplicaOptions{})
//		...
//		select {
//		case <-changed:
//		case <-done:
//			return
//		}
//	}
//
// The channel is closed, not sent on, so any number of followers may wait on
// the same one and all of them wake.
func (kvs *KeyValueStore) Changed() <-chan struct{} {
	for {
		if waiting := kvs.waiters.Load(); waiting != nil {
			return *waiting
		}

		fresh := make(chan struct{})
		if kvs.waiters.CompareAndSwap(nil, &fresh) {
			return fresh
		}
	}
}

// notify wakes whatever is waiting on Changed. A store nobody is following pays
// one atomic swap for it. Callers must hold the write lock, which is what keeps
// two of these from racing each other.
//
// A channel handed out after the swap and before the caller looks at the
// position is not a missed wake-up: the record is already in Data by then, so
// the position the caller reads includes it, which is why Changed has to be
// taken before the position and not after.
func (kvs *KeyValueStore) notify() {
	if waiting := kvs.waiters.Swap(nil); waiting != nil {
		close(*waiting)
	}
}
