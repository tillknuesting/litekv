package litekv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

// replicaOf brings a store up to date with a DB the way a follower would: the
// snapshot first, then the tail from the position it came with.
//
// The store standing in for a follower here is a KeyValueStore, which works
// because a snapshot and a tail are both runs of records and that is all Apply
// wants. It is not what a real follower would be — a DB's worth of records may
// not fit in memory, which is the whole reason DB exists — but it is enough to
// check that the leader hands out the right records in the right order, which
// is what these tests are about.
func replicaOf(t *testing.T, db *DB, opts ReplicaOptions) (*KeyValueStore, DBPosition) {
	t.Helper()

	var wire bytes.Buffer

	at, err := db.Snapshot(&wire, opts)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	follower := &KeyValueStore{}
	if _, err := follower.Apply(Position{}, &wire, opts); err != nil {
		t.Fatalf("applying the snapshot: %v", err)
	}

	return follower, tailInto(t, db, follower, at, opts)
}

// tailInto streams whatever the leader has after pos into the follower, and
// reports where that leaves it.
func tailInto(t *testing.T, db *DB, follower *KeyValueStore, pos DBPosition, opts ReplicaOptions) DBPosition {
	t.Helper()

	for {
		var wire bytes.Buffer

		next, err := db.Since(pos, &wire, opts)
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			return pos
		}
		pos = next

		if wire.Len() == 0 {
			continue // a log ended; the next one starts where it left off
		}
		if _, err := follower.Apply(follower.Position(), &wire, opts); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
}

// sameContents holds a follower to what the DB answers for every key either of
// them has. It is what sameStore cannot be here: the two hold the same records
// but lay them out completely differently, so there are no bytes to compare.
func sameContents(t *testing.T, db *DB, follower *KeyValueStore, absent []string) {
	t.Helper()

	live := 0
	err := db.ForEach(func(key, value []byte) bool {
		live++

		got, err := follower.Read(key)
		if err != nil {
			t.Errorf("%q: the leader has '%s', the follower says %v", key, value, err)
			return true
		}
		if !bytes.Equal(got, value) {
			t.Errorf("%q: the leader has '%s', the follower '%s'", key, value, got)
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEach: %v", err)
	}

	// The other direction: a follower holding keys the leader does not is just
	// as wrong, and comparing one way round would never notice.
	for key := range follower.Index {
		if _, err := follower.Read([]byte(key)); errors.Is(err, ErrorKeyDeleted) {
			continue
		}
		if _, err := db.Read([]byte(key)); err != nil {
			t.Errorf("%q is on the follower but the leader says %v", key, err)
		}
	}

	for _, key := range absent {
		if _, err := follower.Read([]byte(key)); err == nil {
			t.Errorf("%q was never written but the follower has it", key)
		}
	}

	if err := follower.Verify(); err != nil {
		t.Errorf("the follower holds a record that does not verify: %v", err)
	}
	t.Logf("%d live keys, %d bytes on the follower", live, follower.Size())
}

// TestDBSnapshotIsTheLiveRecords checks the shape of a snapshot: the newest
// version of every live key and nothing else. Superseded records must not go,
// and neither must tombstones, since a follower starting from nothing has no
// older value for one to hide.
func TestDBSnapshotIsTheLiveRecords(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(256))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Every key written several times over, so most records are superseded.
	for round := 0; round < 10; round++ {
		for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
			if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%02d", key, round))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Delete([]byte("gamma")); err != nil {
		t.Fatal(err)
	}

	follower, _ := replicaOf(t, db, ReplicaOptions{})
	sameContents(t, db, follower, []string{"never written"})

	// Three live keys, one record each, and no tombstone for the fourth.
	if got := len(follower.Index); got != 3 {
		t.Errorf("the snapshot carried %d keys, want 3", got)
	}
	records := 0
	if err := follower.ForEach(func(_, _ []byte, deleted bool) bool {
		records++
		if deleted {
			t.Error("a tombstone crossed in a snapshot")
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if records != 3 {
		t.Errorf("the snapshot carried %d records for 3 live keys", records)
	}

	if _, err := follower.Read([]byte("gamma")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("a deleted key reads as '%v' on a fresh follower, want %v", err, ErrorKeyNotFound)
	}
}

// TestDBSnapshotKeepsWritingCheck checks the claim the snapshot makes about
// itself: that it can be taken while the store is being written to, and that
// what it holds is the store as it was at the position it reports, with
// everything after that coming down the tail.
func TestDBSnapshotKeepsWritingCheck(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(512))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("before")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	at, err := db.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Written after the snapshot was taken, so none of it may be in it.
	for i := 0; i < 100; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("after")); err != nil {
			t.Fatal(err)
		}
	}

	follower := &KeyValueStore{}
	if _, err := follower.Apply(Position{}, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		got, err := follower.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if string(got) != "before" {
			t.Fatalf("%s is %q in the snapshot: it caught a write made after it", key, got)
		}
	}

	// And the tail brings the rest.
	tailInto(t, db, follower, at, ReplicaOptions{})
	sameContents(t, db, follower, nil)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		got, err := follower.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s after the tail: %v", key, err)
		}
		if string(got) != "after" {
			t.Fatalf("%s is %q after the tail, want 'after'", key, got)
		}
	}
}

// TestDBTailCrossesRotations checks that the tail follows the store from one
// log into the next. A DB rotates whenever the active log fills, so a follower
// that could only read one log would stop at the first rotation.
func TestDBTailCrossesRotations(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{
		Sync:         SyncNever,
		SegmentSize:  256,
		MergeTrigger: 1, // merging off, so nothing is taken away underneath
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Something to snapshot, so that the position it comes with names a record.
	for i := 0; i < 20; i++ {
		if err := db.Write([]byte(fmt.Sprintf("early-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	follower, pos := replicaOf(t, db, ReplicaOptions{})

	before := db.Segments()
	for i := 0; i < 200; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if db.Segments() <= before+2 {
		t.Fatalf("%d logs after 200 writes, want several more than the %d there were", db.Segments(), before)
	}

	pos = tailInto(t, db, follower, pos, ReplicaOptions{})
	sameContents(t, db, follower, []string{"key-999"})

	// Caught up means the leader has nothing more to give, not that the two are
	// on the same log: a follower that has read a log to its end stays there
	// rather than stepping to the start of an empty one, because the end of a
	// log names a record and the start of one names nothing.
	if again := tailInto(t, db, follower, pos, ReplicaOptions{}); again != pos {
		t.Errorf("a caught-up follower moved from %+v to %+v", pos, again)
	}

	// And it keeps up from there, across another rotation.
	for i := 200; i < 300; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	tailInto(t, db, follower, pos, ReplicaOptions{})
	sameContents(t, db, follower, nil)
}

// TestDBTailCrossesFrozenLogs walks a follower through several logs that are
// all frozen, a record at a time. It is the case a batch size big enough to
// swallow a whole log never reaches: the batch is over its size the moment it
// crosses into the next log, and the log it crosses into still has to give up a
// record, because a position resting at the start of one cannot be checked.
func TestDBTailCrossesFrozenLogs(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 256, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Write([]byte("first"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	follower, pos := replicaOf(t, db, ReplicaOptions{})

	// Enough to fill several logs, so that the follower has frozen logs ahead
	// of it rather than the one being written.
	for i := 0; i < 120; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if db.Segments() < 5 {
		t.Fatalf("%d logs, want several frozen ones ahead of the follower", db.Segments())
	}

	// One byte a batch: every call takes exactly the one record it must, and
	// every log boundary is crossed with the batch already past its size.
	crossings := 0
	for {
		var wire bytes.Buffer

		next, err := db.Since(pos, &wire, ReplicaOptions{BatchSize: 1})
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			break
		}
		if next.Segment != pos.Segment {
			crossings++
		}
		pos = next

		if _, err := follower.Apply(follower.Position(), &wire, ReplicaOptions{}); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	if crossings < 3 {
		t.Errorf("the follower crossed %d log boundaries, want several", crossings)
	}
	sameContents(t, db, follower, []string{"key-999"})
}

// TestDBTailDivergesOnAMerge checks the thing that makes a DB different. A
// merge writes its output over the oldest log it replaces, so a log keeps its
// name while becoming something else entirely, and a follower reading it must
// be told rather than handed the new bytes as though they carried on.
func TestDBTailDivergesOnAMerge(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 256, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The same handful of keys over and over, so that most records are dead by
	// the time a merge runs and the merged log is nothing like its inputs. A
	// store of write-once keys merges into something that begins with the same
	// records it began with before, and a follower reading it carries on
	// perfectly correctly — merging only strands a follower when it throws
	// records away in front of where that follower is.
	for round := 0; round < 40; round++ {
		for _, key := range []string{"alpha", "beta", "gamma"} {
			if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%02d", key, round))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if db.Segments() < 4 {
		t.Fatalf("%d logs, want several to merge", db.Segments())
	}

	// A position one record into the oldest frozen log, which is where a
	// follower that had fallen behind would be. It names a record, as every
	// position a follower is handed does.
	db.mu.RLock()
	oldest := db.frozen[len(db.frozen)-1]
	first, raw, err := readRecordAt(oldest.file, oldest.bytes, 0)
	db.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	stale := DBPosition{
		Segment: oldest.id(),
		Log:     Position{Offset: int64(len(raw)), Last: 0, Crc: first.Crc},
	}

	// Prove it is a position the leader would have served a moment ago.
	if _, err := db.Since(stale, io.Discard, ReplicaOptions{}); err != nil {
		t.Fatalf("the position was not usable before the merge: %v", err)
	}

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Since(stale, io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
		t.Fatalf("a position in a merged log got '%v', want %v", err, ErrorDiverged)
	}

	// And the way back is a new snapshot, not a rewind.
	follower, _ := replicaOf(t, db, ReplicaOptions{})
	sameContents(t, db, follower, nil)
}

// TestDBSnapshotOfAnEmptyStore covers the one position this design cannot
// check. A snapshot of a store with nothing in its active log has nowhere to
// point but the start of that log, which names no record; if the log fills and
// freezes before the follower asks for anything, the leader has no way to say
// whether it is still the same log and refuses rather than guess.
func TestDBSnapshotOfAnEmptyStore(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 256, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wire bytes.Buffer
	at, err := db.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if wire.Len() != 0 {
		t.Errorf("a snapshot of an empty store carried %d bytes", wire.Len())
	}

	// Used straight away it is fine, because that log is still the one being
	// written and a log being written cannot be merged.
	if err := db.Write([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	follower := &KeyValueStore{}
	next := tailInto(t, db, follower, at, ReplicaOptions{})
	sameContents(t, db, follower, nil)

	// Left unused while that log fills and freezes, it cannot be checked, and
	// the answer is to say so rather than to hand over a log that may be a
	// different one.
	for i := 0; i < 100; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Since(at, io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
		t.Fatalf("a snapshot position left in a frozen log got '%v', want %v", err, ErrorDiverged)
	}

	// A follower that kept up has a position naming a record, and carries on.
	if _, err := db.Since(next, io.Discard, ReplicaOptions{}); err != nil {
		t.Fatalf("a position that names a record stopped working: %v", err)
	}
	tailInto(t, db, follower, next, ReplicaOptions{})
	sameContents(t, db, follower, nil)
}

// TestDBPositionBinary checks the twenty-eight bytes a position crosses as, and
// that one arriving from somewhere else is checked rather than believed.
func TestDBPositionBinary(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(256))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range []DBPosition{{}, db.Position(), {Segment: 9}} {
		encoded, err := want.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != dbPositionSize {
			t.Errorf("a position encoded to %d bytes, want %d", len(encoded), dbPositionSize)
		}

		var got DBPosition
		if err := got.UnmarshalBinary(encoded); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("a position came back as %+v, want %+v", got, want)
		}
	}

	for _, bad := range [][]byte{nil, make([]byte, dbPositionSize-1), make([]byte, dbPositionSize+1)} {
		var got DBPosition
		if err := got.UnmarshalBinary(bad); err == nil {
			t.Errorf("%d bytes were accepted as a position", len(bad))
		}
	}

	// A log part that does not describe a log is refused whatever the segment.
	nonsense := make([]byte, dbPositionSize)
	nonsense[8] = 40  // an offset of 40
	nonsense[16] = 40 // with its last record starting at the end of it
	var got DBPosition
	if err := got.UnmarshalBinary(nonsense); err == nil {
		t.Errorf("a position whose log part is impossible was accepted as %+v", got)
	}
}

// TestDBReplicaModel runs a random history against a DB and keeps a follower up
// with it as it goes, checking the answers after every round. Merging is on and
// the segments are tiny, so logs are rotated and merged out from under the
// follower constantly and it has to keep noticing and taking a new snapshot.
func TestDBReplicaModel(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	random := rand.New(rand.NewSource(3))

	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	live := map[string]string{}

	follower := &KeyValueStore{}
	pos := DBPosition{}
	snapshots, tails := 0, 0

	for step := 0; step < 1500; step++ {
		key := keys[random.Intn(len(keys))]

		switch n := random.Intn(100); {
		case n < 65:
			value := fmt.Sprintf("value-%d", step)
			if err := db.Write([]byte(key), []byte(value)); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			live[key] = value

		case n < 80:
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			delete(live, key)

		case n < 97:
			opts := ReplicaOptions{BatchSize: int64(1 + random.Intn(400))}

			// The follower's loop as it would really be written: carry on from
			// where it is, and when the leader says that place is gone, empty
			// itself and take a new snapshot.
			var wire bytes.Buffer

			_, err := db.Since(pos, &wire, opts)
			if pos == (DBPosition{}) || errors.Is(err, ErrorDiverged) {
				wire.Reset()
				if pos, err = db.Snapshot(&wire, opts); err != nil {
					t.Fatalf("step %d: Snapshot: %v", step, err)
				}
				if err := follower.Reset(); err != nil {
					t.Fatal(err)
				}
				if _, err := follower.Apply(Position{}, &wire, opts); err != nil {
					t.Fatalf("step %d: applying a snapshot: %v", step, err)
				}
				snapshots++
			} else if err != nil {
				t.Fatalf("step %d: Since: %v", step, err)
			}

			pos = tailInto(t, db, follower, pos, opts)
			tails++

			for key, want := range live {
				got, err := follower.Read([]byte(key))
				if err != nil {
					t.Fatalf("step %d: %q: the leader has %q, the follower says %v", step, key, want, err)
				}
				if string(got) != want {
					t.Fatalf("step %d: %q: the leader has %q, the follower %q", step, key, want, got)
				}
			}
			for _, key := range keys {
				if _, ok := live[key]; ok {
					continue
				}
				if _, err := follower.Read([]byte(key)); err == nil {
					t.Fatalf("step %d: %q is deleted but the follower still has it", step, key)
				}
			}

		default:
			// A stretch with nobody replicating, and then a merge: this is how
			// a follower falls far enough behind for the log it was reading to
			// be rewritten under it, which is the case the whole snapshot path
			// exists for.
			for i := 0; i < 150; i++ {
				value := fmt.Sprintf("burst-%d-%d", step, i)
				key := keys[random.Intn(len(keys))]
				if err := db.Write([]byte(key), []byte(value)); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				live[key] = value
			}
			if err := db.Merge(); err != nil {
				t.Fatalf("step %d: Merge: %v", step, err)
			}
		}
	}

	t.Logf("%d rounds of catching up, %d of them starting from a new snapshot", tails, snapshots)

	if snapshots < 2 {
		t.Errorf("only %d snapshots, so recovery from a merge was barely exercised", snapshots)
	}
	if tails-snapshots < 20 {
		t.Errorf("only %d rounds carried on from where the follower was", tails-snapshots)
	}
}
