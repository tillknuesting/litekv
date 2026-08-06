package litekv

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// FuzzHint feeds arbitrary bytes to the hint parser, which reads a file this
// package wrote but has no reason to trust: a damaged disk, an older version,
// or something else entirely can be sitting where a hint should be.
//
// Refusing is always allowed. What is not allowed is accepting a hint and
// handing back an offset that is not in the log, since a hint is taken at its
// word and every read afterwards trusts it.
func FuzzHint(f *testing.F) {
	index := map[string]int64{"alpha": 0, "beta": 40}

	dir := f.TempDir()
	segment := dir + "/0000000001" + segmentSuffix
	if err := writeHint(segment, 1024, 7, index); err != nil {
		f.Fatal(err)
	}
	written, err := disk.ReadFile(hintPath(segment))
	if err != nil {
		f.Fatal(err)
	}

	f.Add(written, int64(1024))
	f.Add(written, int64(0))
	f.Add([]byte{}, int64(0))
	f.Add([]byte("LKVH"), int64(10))
	f.Add(make([]byte, hintHeaderSize+4), int64(0))

	f.Fuzz(func(t *testing.T, data []byte, segmentSize int64) {
		got, _, ok := parseHint(data, segmentSize)
		if !ok {
			return
		}

		for key, pos := range got {
			if pos < 0 || pos+headerSizeV0 > segmentSize {
				t.Fatalf("accepted a hint putting %q at %d, outside a log of %d bytes", key, pos, segmentSize)
			}
		}
	})
}

// FuzzSegmentBytes feeds arbitrary bytes to the half of the package that reads
// a log without holding it in memory: the streaming indexer and the reader that
// fetches one record by offset.
//
// Nothing here may panic or hang, whatever the bytes claim their lengths are.
// What the indexer accepts, the reader has to agree with: an offset it indexed
// must decode, and the record there must hold the key it was indexed under.
func FuzzSegmentBytes(f *testing.F) {
	current := &KeyValueStore{}
	current.Write([]byte("alpha"), []byte("one"))
	current.Delete([]byte("alpha"))
	current.Write([]byte("beta"), []byte(""))
	f.Add(current.Data)

	var old []byte
	old = appendV0(old, RecordTypeNormal, []byte("old"), []byte("record"))
	f.Add(old)
	f.Add(append(append([]byte(nil), old...), current.Data...))

	f.Add([]byte{})
	f.Add(make([]byte, headerSizeV0))
	f.Add(make([]byte, headerSizeV1))
	f.Add(make([]byte, headerSizeV2))
	f.Add(make([]byte, headerSizeV4))

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bytes.NewReader(data)
		size := int64(len(data))

		index, good, _, err := indexSegment(reader, size)
		if err != nil {
			t.Fatalf("indexing bytes should refuse them, not fail: %v", err)
		}
		if good < 0 || good > size {
			t.Fatalf("indexed up to %d of %d bytes", good, size)
		}

		for key, pos := range index {
			record, raw, err := readRecordAt(reader, good, pos)
			if err != nil {
				t.Fatalf("indexed %q at %d and then could not read it: %v", key, pos, err)
			}
			if record.Crc != checksumSerialized(raw) {
				t.Fatalf("indexed %q at %d, where the record fails its checksum", key, pos)
			}
			if string(record.Key) != key {
				t.Fatalf("indexed %q at %d, where the record holds %q", key, pos, record.Key)
			}
		}

		// Walking the part that indexed cleanly must agree with it.
		seen := 0
		if err := scanSegment(reader, good, func(pos int64, raw []byte, r Record) bool {
			seen++
			return true
		}); err != nil {
			t.Fatalf("scanning the good part of the log failed: %v", err)
		}
		if seen < len(index) {
			t.Fatalf("the walk saw %d records for %d indexed keys", seen, len(index))
		}
	})
}

// FuzzDBPosition feeds arbitrary bytes to the parser for a position that came
// off a wire. Refusing is always allowed; what is not allowed is accepting one
// whose fields cannot describe a log, since everything downstream reads it as
// though they do.
func FuzzDBPosition(f *testing.F) {
	for _, pos := range []DBPosition{{}, {Segment: 3, Log: Position{Offset: 40, Last: 12, Crc: 99}}} {
		encoded, err := pos.MarshalBinary()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Add([]byte{})
	f.Add(make([]byte, dbPositionSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		var pos DBPosition
		if err := pos.UnmarshalBinary(data); err != nil {
			return
		}

		if pos.Log.Offset < 0 || pos.Log.Last < 0 {
			t.Fatalf("accepted a position with a negative offset: %+v", pos)
		}
		if pos.Log.Offset != 0 && pos.Log.Last >= pos.Log.Offset {
			t.Fatalf("accepted a position whose last record starts at or past the end: %+v", pos)
		}

		// And it survives the round trip it was built for.
		again, err := pos.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var back DBPosition
		if err := back.UnmarshalBinary(again); err != nil {
			t.Fatalf("a position this package accepted does not re-parse: %v", err)
		}
		if back != pos {
			t.Fatalf("a position came back as %+v, want %+v", back, pos)
		}
	})
}

// FuzzDBApply feeds arbitrary bytes to a DB follower as though a leader had
// sent them. A batch is all or nothing here, so the store must either take the
// whole thing and say so, or take none of it and stay exactly where it was.
// What must never happen is a store that claims a position it does not hold the
// records for, since nothing afterwards can find that out.
func FuzzDBApply(f *testing.F) {
	installUnsynced(f)

	leader, err := OpenDB(f.TempDir(), smallSegments(4096))
	if err != nil {
		f.Fatal(err)
	}
	leader.Write([]byte("alpha"), []byte("one"))
	leader.Write([]byte("beta"), []byte("two"))
	leader.Delete([]byte("alpha"))

	var wire bytes.Buffer
	if _, release, err := leader.Snapshot(&wire, ReplicaOptions{}); err != nil {
		f.Fatal(err)
	} else {
		release()
	}
	leader.Close()

	whole := wire.Bytes()
	f.Add(whole, uint64(1), int64(0))
	f.Add(whole[:len(whole)/2], uint64(1), int64(0))
	f.Add([]byte{}, uint64(0), int64(0))
	f.Add([]byte("not a record at all"), uint64(7), int64(40))
	f.Add(make([]byte, headerSizeV1), uint64(1), int64(22))

	// One store, emptied between executions rather than opened again: opening a
	// DB is a directory read and a handful of files, and doing it per execution
	// took the fuzzer from thousands a second to tens.
	db, err := OpenDB(f.TempDir(), smallSegments(4096))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { db.Close() })

	var one sync.Mutex

	f.Fuzz(func(t *testing.T, batch []byte, segment uint64, offset int64) {
		if offset < 0 {
			offset = -offset
		}

		one.Lock()
		defer one.Unlock()

		if err := db.Reset(); err != nil {
			t.Fatal(err)
		}

		// A position that could plausibly have come from a leader: the fields
		// have to be consistent with each other or Apply is not the thing being
		// tested.
		next := DBPosition{Segment: segment, Log: Position{Offset: offset + 1, Last: offset, Crc: 1}}

		got, err := db.Apply(DBPosition{}, next, bytes.NewReader(batch), ReplicaOptions{})

		if err != nil {
			if got != (DBPosition{}) {
				t.Fatalf("a refused batch reported %+v, want the position it was at", got)
			}
			if applied := db.Applied(); applied != (DBPosition{}) {
				t.Fatalf("a refused batch left the store claiming %+v", applied)
			}
			if db.Len() != 0 {
				t.Fatalf("a refused batch left %d keys behind", db.Len())
			}
			return
		}

		if got != next || db.Applied() != next {
			t.Fatalf("an applied batch left the store at %+v, want %+v", db.Applied(), next)
		}

		// Whatever it took, the store still answers for it.
		if err := db.ForEach(func(key, value []byte) bool {
			if _, err := db.Read(key); err != nil {
				t.Fatalf("%q was applied but reads as %v", key, err)
			}
			return true
		}); err != nil {
			t.Fatalf("the store is not readable after applying %d bytes: %v", len(batch), err)
		}
	})
}

// FuzzDBApplySnapshot feeds arbitrary bytes to a follower as though they were a
// leader's snapshot. Half a snapshot with a position on it would be a store
// missing keys and saying it was up to date, which nothing afterwards could
// notice, so the position must only be claimed once the whole thing is in.
func FuzzDBApplySnapshot(f *testing.F) {
	installUnsynced(f)

	leader, err := OpenDB(f.TempDir(), smallSegments(4096))
	if err != nil {
		f.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}

	var wire bytes.Buffer
	if _, release, err := leader.Snapshot(&wire, ReplicaOptions{}); err != nil {
		f.Fatal(err)
	} else {
		release()
	}
	leader.Close()

	whole := wire.Bytes()
	f.Add(whole)
	f.Add(whole[:len(whole)-3])
	f.Add([]byte{})
	f.Add([]byte("rubbish"))

	// One store, emptied by ApplySnapshot itself, rather than a new one per
	// execution: see the note in FuzzDBApply.
	db, err := OpenDB(f.TempDir(), smallSegments(4096))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { db.Close() })

	var one sync.Mutex

	f.Fuzz(func(t *testing.T, snapshot []byte) {
		one.Lock()
		defer one.Unlock()

		at := DBPosition{Segment: 4, Log: Position{Offset: 40, Last: 12, Crc: 7}}

		if err := db.ApplySnapshot(at, bytes.NewReader(snapshot), ReplicaOptions{}); err != nil {
			if applied := db.Applied(); applied != (DBPosition{}) {
				t.Fatalf("a refused snapshot left the store claiming %+v", applied)
			}
			return
		}

		if applied := db.Applied(); applied != at {
			t.Fatalf("an applied snapshot left the store at %+v, want %+v", applied, at)
		}
		if err := db.ForEach(func(key, value []byte) bool { return true }); err != nil {
			t.Fatalf("the store is not readable after a snapshot of %d bytes: %v", len(snapshot), err)
		}
	})
}

// FuzzDBSince feeds arbitrary positions to a leader, which is what a follower
// that has been tampered with, or that is following an entirely different
// store, would send. Refusing is always allowed. What is not allowed is a panic,
// or handing back bytes that are not whole records — a follower takes what it is
// given, and this is the only place that can tell.
func FuzzDBSince(f *testing.F) {
	installUnsynced(f)

	leader, err := OpenDB(f.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 256, MergeTrigger: 1})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { leader.Close() })

	for i := 0; i < 60; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			f.Fatal(err)
		}
	}

	at := leader.Position()
	f.Add(at.Segment, at.Log.Offset, at.Log.Last, at.Log.Crc, int64(1<<20))
	f.Add(uint64(1), int64(0), int64(0), uint32(0), int64(64))
	f.Add(uint64(0), int64(-1), int64(-1), uint32(0), int64(0))

	f.Fuzz(func(t *testing.T, segment uint64, offset, last int64, crc uint32, size int64) {
		pos := DBPosition{Segment: segment, Log: Position{Offset: offset, Last: last, Crc: crc}}

		var wire bytes.Buffer
		next, err := leader.Since(pos, &wire, ReplicaOptions{BatchSize: size})
		if err != nil {
			return
		}

		// Whatever came back has to be whole records, or the follower applying
		// them would stop part way and blame the wire.
		batch := wire.Bytes()
		var walked int64
		for walked < int64(len(batch)) {
			record, end, err := parseRecordAt(batch, walked)
			if err != nil {
				t.Fatalf("position %+v gave back %d bytes that stop being records at %d", pos, len(batch), walked)
			}
			if record.Crc != checksumSerialized(batch[walked:end]) {
				t.Fatalf("position %+v gave back a record at %d that does not verify", pos, walked)
			}
			walked = end
		}

		if len(batch) > 0 && next == pos {
			t.Fatalf("position %+v gave back %d bytes but did not move", pos, len(batch))
		}
	})
}
