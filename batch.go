package litekv

import (
	"encoding/binary"
	"time"
)

// A write batch is several records stored together or not at all.
//
// Everything else in this package is one record at a time, and one record is
// already atomic: it reaches the log whole or it does not decode, and recovery
// cuts the log back to the last one that did. Several records are not. A crash
// part way through writing them leaves some of them on the disk, and there is
// nothing in the log to say that the ones that made it were meant to arrive
// with the ones that did not — which is exactly what a caller moving a value
// from one key to another needs said.
//
// So a batch opens with a marker: a record of its own, holding no key, whose
// value is the number of bytes of records that follow it. Recovery either finds
// all of that span, intact, or discards the log from the marker on. Records
// outside a span are ordinary writes and carry no flag, so a store that never
// writes a batch has exactly the log it always had.
//
// The marker is the only record in this format that refers to another. That is
// the whole of the cost, and it is paid by the log walkers rather than by their
// callers: scan and scanSegment check a batch and then yield the records in it,
// so everything that reads a log — ForEach, Verify, Compact, latestOffsets,
// indexSegment, mergeInto — sees the records and never the marker, and cannot
// forget to.
//
// What this is not is a transaction. There are no reads in it, nothing is
// isolated from a concurrent writer, and there is nothing to roll back once it
// is written. It is one durable, atomic append of several records.

// Batch collects records to be written together. The zero value is an empty
// batch, ready to use, and one may be written more than once.
//
// The keys and values are not copied when they are added. They are read when
// the batch is written, so anything handed to it has to stay unchanged until
// then — unlike Write, which copies before it returns. The batch exists to save
// that copy, and a caller that wants one can make it.
type Batch struct {
	entries []Record
}

// Write adds a record storing value under key.
func (b *Batch) Write(key, value []byte) {
	b.entries = append(b.entries, Record{
		Type:        RecordTypeNormal,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	})
}

// WriteExpiring adds a record that stops counting once at has passed, on the
// same terms as KeyValueStore.WriteExpiring. A zero time never expires.
func (b *Batch) WriteExpiring(key, value []byte, at time.Time) {
	expires := int64(0)
	if !at.IsZero() {
		expires = at.UnixNano()
	}

	b.entries = append(b.entries, Record{
		Type:        RecordTypeNormal,
		Expires:     expires,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	})
}

// Delete adds a tombstone for key, which shadows whatever is older exactly as a
// delete of its own does.
func (b *Batch) Delete(key []byte) {
	b.entries = append(b.entries, Record{
		Type:      RecordTypeDeleted,
		Key:       key,
		KeyLength: uint32(len(key)),
	})
}

// Len is how many records the batch will write.
func (b *Batch) Len() int { return len(b.entries) }

// Reset empties the batch, keeping the memory it has already taken so that a
// caller writing batch after batch does not allocate for each one.
func (b *Batch) Reset() { b.entries = b.entries[:0] }

// WriteBatch stores every record in b, or none of them.
//
// A crash part way through leaves the store as it was: the records go down
// behind a marker saying how many bytes to expect, and recovery discards from
// that marker on unless all of them are there and intact. Nothing is visible
// until the whole batch is stored, since the index is pointed at the records
// only once the log has taken them, exactly as a single write does.
//
// Within the batch, later records win: writing a key twice leaves the second
// value, and deleting a key written earlier in the same batch leaves it
// deleted. An empty batch writes nothing and reports no error.
//
// It returns ErrorRecordTooLarge if any key or value does not fit in the uint32
// length fields, and writes nothing in that case.
func (kvs *KeyValueStore) WriteBatch(b *Batch) error {
	if b == nil || len(b.entries) == 0 {
		return nil
	}

	// Checked before the lock and before anything is serialized, so a batch
	// that cannot be written whole is refused rather than half appended.
	for _, entry := range b.entries {
		if uint64(len(entry.Key)) > maxFieldLen || uint64(len(entry.Value)) > maxFieldLen {
			return ErrorRecordTooLarge
		}
	}

	kvs.Lock()
	defer kvs.Unlock()

	return kvs.appendBatch(b.entries)
}

// markerSpan reports how many bytes of records a marker opens, and whether the
// record is a well-formed one.
//
// A marker holding a key, or a value that is not the eight bytes of a span, or
// a span of nothing, is not a marker this package wrote. Refusing it is the
// same answer as refusing a record that will not decode: the log ends there.
func markerSpan(r Record) (int64, bool) {
	if r.Type != RecordTypeBatch {
		return 0, false
	}
	if len(r.Key) != 0 || len(r.Value) != 8 {
		return 0, false
	}

	span := int64(binary.LittleEndian.Uint64(r.Value))
	if span <= 0 {
		return 0, false
	}
	return span, true
}

// spanAt reports the span of the batch opening at pos in data, where a record
// has already been decoded, along with where the records in it start and end.
// The bounds are checked against the data actually present, so a span claiming
// a gigabyte in a log of forty bytes is refused rather than believed.
func spanAt(record Record, next, size int64) (from, to int64, ok bool) {
	span, ok := markerSpan(record)
	if !ok {
		return 0, 0, false
	}
	if span > size-next {
		return 0, 0, false
	}
	return next, next + span, true
}

// unitAt reports how much of data starting at pos has to cross a wire together,
// and which record a position naming the end of it would name.
//
// For an ordinary record that is the record. For a marker it is the marker and
// every record in its span, because a follower holding half a batch is exactly
// what the marker exists to prevent, and the last record in it is the one the
// position names — never the marker, and never a record inside the batch.
//
// It reports false when the unit is not all there, which for a log is a torn
// tail and for a buffer off a wire is a batch that has not finished arriving.
func unitAt(data []byte, pos int64) (end int64, last Record, at int64, ok bool) {
	size := int64(len(data))

	record, next, err := parseRecordAt(data, pos)
	if err != nil {
		return 0, Record{}, 0, false
	}
	if record.Type != RecordTypeBatch {
		return next, record, pos, true
	}

	from, to, ok := spanAt(record, next, size)
	if !ok {
		return 0, Record{}, 0, false
	}

	// The last record of the batch, which is what the end of it is named by.
	for walk := from; walk < to; {
		inner, after, err := parseRecordAt(data, walk)
		if err != nil || after > to {
			return 0, Record{}, 0, false
		}
		last, at = inner, walk
		walk = after
	}
	if at == 0 && to == from {
		return 0, Record{}, 0, false // a span of nothing is not a batch
	}
	return to, last, at, true
}
