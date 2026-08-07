package litekv

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
)

// The index is a hash map, so the keys have no order, and everything else in
// this package is built on that: a lookup is one hash and one read, and it
// stays one however many keys there are. An ordered index instead of the map
// was measured and reverted — a radix tree cost three to four and a half times
// on point lookups, which is the wrong trade for a store whose whole shape is
// point lookups.
//
// So a range is answered by asking the keys rather than by keeping them in
// order, and the two halves of a DB answer differently:
//
//   - A frozen log's index never changes again. The keys are sorted once, the
//     first time anybody asks this log for a range, and kept — a cache that
//     cannot go stale, which is the difference between this and the search
//     order that AGENTS.md turned down. A range is then a binary search and a
//     walk.
//   - The log being written changes constantly, so there is nothing to keep.
//     Its keys are filtered against the range and only the matches are sorted,
//     which is cheap because the matches are usually few and because the log is
//     bounded: that is what rotation is for.
//
// Nothing is paid on the write path for any of this, and nothing is paid in
// memory by a store that never asks for a range.

// Range calls fn with every live key between from and to, in order, along with
// its value. from is included and to is not; a nil from starts at the first key
// and a nil to runs to the last.
//
// It visits the newest version of each key and nothing else: a key deleted or
// expired is not visited, and neither are the superseded records behind a key
// that was written twice. Return false from fn to stop early.
//
// The key and value are only valid until fn returns.
func (kvs *KeyValueStore) Range(from, to []byte, fn func(key, value []byte) bool) error {
	kvs.RLock()
	defer kvs.RUnlock()

	// Filtered first and sorted afterwards, so that a range over ten keys of a
	// million costs a walk of the keys and a sort of ten.
	matches := make([]string, 0, 16)
	for key := range kvs.Index {
		if within(key, from, to) {
			matches = append(matches, key)
		}
	}
	slices.Sort(matches)

	for _, key := range matches {
		record, err := kvs.recordFor(key)
		if err != nil {
			return err
		}
		if record.Type != RecordTypeNormal || record.Expired() {
			continue
		}
		if !fn(record.Key, record.Value) {
			return nil
		}
	}

	return nil
}

// Prefix calls fn with every live key beginning with prefix, in order. An empty
// prefix is every key.
func (kvs *KeyValueStore) Prefix(prefix []byte, fn func(key, value []byte) bool) error {
	return kvs.Range(prefix, prefixEnd(prefix), fn)
}

// recordFor is the record the index points at for a key, checked against its
// checksum. Callers must hold at least a read lock.
func (kvs *KeyValueStore) recordFor(key string) (Record, error) {
	pos, ok := kvs.Index[key]
	if !ok {
		return Record{}, ErrorKeyNotFound
	}

	record, next, err := parseRecordAt(kvs.Data, pos)
	if err != nil {
		return Record{}, err
	}
	if record.Crc != checksumSerialized(kvs.Data[pos:next]) {
		return Record{}, fmt.Errorf("record at offset %d: %w", pos, ErrorChecksumMismatch)
	}
	if string(record.Key) != key {
		return Record{}, ErrorKeyMismatch
	}
	return record, nil
}

// Range calls fn with every live key between from and to, in order, along with
// its value, on the same terms as KeyValueStore.Range.
//
// A key is answered by the newest log that holds it, so a value in the log
// being written shadows an older one, and a tombstone shadows both. What that
// costs here is that every log has to be asked before anything can be yielded
// in order — the answer is gathered and then sorted, rather than streamed — so
// a range over most of a large store holds most of its keys while it runs. A
// range over a few of them holds a few.
//
// The key and value are only valid until fn returns.
func (db *DB) Range(from, to []byte, fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrorClosed
	}

	// Where each key's newest record is, gathered newest log first so that the
	// first log to hold a key is the one that answers for it — including when
	// what it holds is a tombstone, which has to shadow the older logs rather
	// than let them answer.
	type located struct {
		key string
		seg readable
		pos int64
	}

	found := make(map[string]located)
	for seg := range db.searchOrder() {
		if err := keysIn(seg, from, to, func(key string, pos int64) bool {
			if _, seen := found[key]; !seen {
				found[key] = located{key: key, seg: seg, pos: pos}
			}
			return true
		}); err != nil {
			return err
		}
	}

	ordered := make([]located, 0, len(found))
	for _, at := range found {
		ordered = append(ordered, at)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].key < ordered[j].key })

	for _, at := range ordered {
		record, raw, err := at.seg.recordAt(at.pos)
		if err != nil {
			return err
		}
		if record.Crc != checksumSerialized(raw) {
			return fmt.Errorf("record at offset %d: %w", at.pos, ErrorChecksumMismatch)
		}
		if record.Type != RecordTypeNormal || record.Expired() {
			continue
		}
		if !fn(record.Key, record.Value) {
			return nil
		}
	}

	return nil
}

// Prefix calls fn with every live key beginning with prefix, in order. An empty
// prefix is every key.
func (db *DB) Prefix(prefix []byte, fn func(key, value []byte) bool) error {
	return db.Range(prefix, prefixEnd(prefix), fn)
}

// keysIn calls fn with the keys of one segment that fall in the range, and
// where their newest records sit. A frozen log answers from its sorted keys; the
// log being written is filtered as it stands.
func keysIn(seg readable, from, to []byte, fn func(key string, pos int64) bool) error {
	if frozen, ok := seg.(*diskSegment); ok {
		frozen.rangeKeys(from, to, fn)
		return nil
	}

	seg.eachKey(func(key string, pos int64) bool {
		if !within(key, from, to) {
			return true
		}
		return fn(key, pos)
	})
	return nil
}

// rangeKeys calls fn with this log's keys in the range, in order.
func (d *diskSegment) rangeKeys(from, to []byte, fn func(key string, pos int64) bool) {
	keys := d.sortedKeys()

	start := 0
	if len(from) > 0 {
		start = sort.SearchStrings(keys, string(from))
	}

	for _, key := range keys[start:] {
		if len(to) > 0 && key >= string(to) {
			return
		}
		if !fn(key, d.index[key]) {
			return
		}
	}
}

// sortedKeys is this log's keys in order, sorted the first time anybody asks
// and kept afterwards.
//
// Keeping it is safe here and would not be anywhere else in this package: a
// frozen log's index is built once and never changes again, so there is nothing
// this copy can fall behind. A merge does not edit a log, it writes a new one
// and swaps the segment, which brings a new sort with it.
func (d *diskSegment) sortedKeys() []string {
	d.ordered.Do(func() {
		keys := make([]string, 0, len(d.index))
		for key := range d.index {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		d.keys = keys
	})

	return d.keys
}

// within reports whether key falls in [from, to), with a nil bound meaning no
// bound on that side.
func within(key string, from, to []byte) bool {
	if len(from) > 0 && key < string(from) {
		return false
	}
	if len(to) > 0 && key >= string(to) {
		return false
	}
	return true
}

// prefixEnd is the first key that does not begin with prefix, or nil when there
// is none — which is a prefix of nothing but 0xff bytes, and an empty prefix,
// both of which run to the end.
func prefixEnd(prefix []byte) []byte {
	end := bytes.TrimRight(prefix, "\xff")
	if len(end) == 0 {
		return nil
	}

	end = append([]byte(nil), end...)
	end[len(end)-1]++
	return end
}
