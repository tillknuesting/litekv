package litekv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

	at, releaseAt, err := db.Snapshot(&wire, opts)

	defer releaseAt()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	follower := &KeyValueStore{}
	if _, err := follower.Apply(Position{}, &wire, opts); err != nil {
		t.Fatalf("applying the snapshot: %v", err)
	}

	return follower, tailInto(t, db, follower, at, opts)
}

// syncInto brings a store standing in for a follower up to date, taking a
// snapshot when it has nowhere to carry on from, and reports whether it had to.
//
// It handles ErrorDiverged wherever it turns up rather than probing for it
// first: a merge can land between a probe and the tail that follows it, and a
// follower that treats that as a failure is a follower that has not been
// written properly.
func syncInto(t *testing.T, db *DB, follower *KeyValueStore, pos DBPosition, opts ReplicaOptions) (DBPosition, bool) {
	t.Helper()

	resynced := false

	for {
		var wire bytes.Buffer

		next, err := db.Since(pos, &wire, opts)
		if pos == (DBPosition{}) || errors.Is(err, ErrorDiverged) {
			wire.Reset()

			at, releaseAt, err := db.Snapshot(&wire, opts)

			defer releaseAt()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if err := follower.Reset(); err != nil {
				t.Fatal(err)
			}
			if _, err := follower.Apply(Position{}, &wire, opts); err != nil {
				t.Fatalf("applying a snapshot: %v", err)
			}
			pos, resynced = at, true
			continue
		}
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			return pos, resynced
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

// tailInto streams whatever the leader has after pos into the follower, and
// reports where that leaves it. It is for stores with merging off, where a
// position cannot be taken away underneath it; syncInto is the one to use
// otherwise.
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
	at, releaseAt, err := db.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseAt()

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
	at, releaseAt, err := db.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseAt()

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

			var resynced bool
			pos, resynced = syncInto(t, db, follower, pos, opts)
			if resynced {
				snapshots++
			}
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

// followDB brings a follower DB up to date with a leader the way a real one
// would: a snapshot when it has nowhere to carry on from, and the tail after it.
// It reports whether it had to start again from a snapshot.
func followDB(t *testing.T, leader, follower *DB, opts ReplicaOptions) (resynced bool) {
	t.Helper()

	pos := follower.Applied()

	for {
		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, opts)
		if pos == (DBPosition{}) || errors.Is(err, ErrorDiverged) {
			wire.Reset()

			at, releaseAt, err := leader.Snapshot(&wire, opts)

			defer releaseAt()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if err := follower.ApplySnapshot(at, &wire, opts); err != nil {
				t.Fatalf("ApplySnapshot: %v", err)
			}
			pos, resynced = at, true
			continue
		}
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			return resynced
		}

		got, err := follower.Apply(pos, next, &wire, opts)
		if err != nil {
			t.Fatalf("Apply(%+v -> %+v): %v", pos, next, err)
		}
		if got != next {
			t.Fatalf("the follower reached %+v, the leader sent as far as %+v", got, next)
		}
		pos = next
	}
}

// sameStores holds two DBs to the same answers for every key either of them
// has. There are no bytes to compare: the two hold the same records and lay
// them out completely differently, which is the whole point of shipping records
// rather than a log.
func sameStores(t *testing.T, leader, follower *DB, absent []string) {
	t.Helper()

	live := 0
	if err := leader.ForEach(func(key, value []byte) bool {
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
	}); err != nil {
		t.Fatalf("ForEach on the leader: %v", err)
	}

	mirrored := 0
	if err := follower.ForEach(func(key, value []byte) bool {
		mirrored++

		got, err := leader.Read(key)
		if err != nil {
			t.Errorf("%q is live on the follower but the leader says %v", key, err)
			return true
		}
		if !bytes.Equal(got, value) {
			t.Errorf("%q is '%s' on the follower and '%s' on the leader", key, value, got)
		}
		return true
	}); err != nil {
		t.Fatalf("ForEach on the follower: %v", err)
	}

	if live != mirrored {
		t.Errorf("the leader has %d live keys, the follower %d", live, mirrored)
	}
	for _, key := range absent {
		if _, err := follower.Read([]byte(key)); err == nil {
			t.Errorf("%q was never written but the follower has it", key)
		}
	}
}

// TestDBFollower is a DB replicated into a DB, which is what all of this is for:
// the follower keeps only the keys in memory, as the leader does, and lays its
// records out however its own rotations and merges decide.
func TestDBFollower(t *testing.T) {
	// Merging off, so that "it did not need another snapshot" is a fact about
	// replication rather than about whether a background merge happened to run
	// first. A follower resting at the end of a frozen log is stranded by a
	// merge that takes it, which is real and deliberate — TestDBFollowerModel
	// and TestDBTailDivergesOnAMerge are where that belongs.
	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 300, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	for i := 0; i < 300; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("updated")); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.Delete([]byte("key-100")); err != nil {
		t.Fatal(err)
	}

	if resynced := followDB(t, leader, follower, ReplicaOptions{}); !resynced {
		t.Error("a follower with nothing did not start from a snapshot")
	}
	sameStores(t, leader, follower, []string{"key-100", "never written"})

	// The two disagree about their files, which is what logical replication
	// buys and what makes them impossible to compare byte for byte.
	if leader.Segments() == follower.Segments() {
		t.Logf("both stores happen to be spread over %d logs", leader.Segments())
	}

	// And it keeps up from there without another snapshot. Few enough writes
	// that the log being written does not fill: a follower that pauses for a
	// whole log's worth may find that log merged away underneath it, which is
	// the case TestDBFollowerModel covers and this one is not about.
	for i := 300; i < 306; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if resynced := followDB(t, leader, follower, ReplicaOptions{}); resynced {
		t.Error("a follower that was up to date started again from a snapshot")
	}
	sameStores(t, leader, follower, []string{"key-100"})
}

// TestDBFollowerKeepsWhatTheLeaderWrote checks that the records cross unchanged
// rather than being written again at this end. A follower that re-wrote them
// would give every record its own timestamp and quietly disagree with the
// leader about when anything happened.
func TestDBFollowerKeepsWhatTheLeaderWrote(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(512))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), smallSegments(512))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	if err := leader.Write([]byte("early"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	// A gap wide enough that a clock read at this end could not be mistaken for
	// one read at the other.
	time.Sleep(20 * time.Millisecond)

	if err := leader.Write([]byte("late"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	for _, key := range []string{"early", "late"} {
		want := recordTime(t, leader, key)
		got := recordTime(t, follower, key)

		if got != want {
			t.Errorf("%q was written at %d on the leader and %d on the follower", key, want, got)
		}
	}
}

// recordTime is when the newest record for a key was written, straight out of
// whichever log holds it.
func recordTime(t *testing.T, db *DB, key string) int64 {
	t.Helper()

	db.mu.RLock()
	defer db.mu.RUnlock()

	for seg := range db.searchOrder() {
		var found int64
		seg.eachKey(func(k string, pos int64) bool {
			if k != key {
				return true
			}
			record, _, err := seg.recordAt(pos)
			if err != nil {
				t.Fatal(err)
			}
			found = record.Timestamp
			return false
		})
		if found != 0 {
			return found
		}
	}

	t.Fatalf("%q is in no log of this store", key)
	return 0
}

// TestDBFollowerSurvivesRestart checks the thing a DB follower needs that a
// single store's does not: somewhere durable to keep how far through the leader
// it has got. Its own logs cannot say, because they are not the leader's.
func TestDBFollowerSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Merging off: this is about the position surviving a close, not about
	// what a merge does to a follower resting on a frozen log. With merging on,
	// whether the last step needs a snapshot depends on when a background merge
	// runs, which on one core is when the writes stop.
	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 400, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	followDB(t, leader, follower, ReplicaOptions{})

	was := follower.Applied()
	if was == (DBPosition{}) {
		t.Fatal("the follower reports no position after catching up")
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 400, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Applied(); got != was {
		t.Fatalf("a reopened follower is at %+v, want %+v", got, was)
	}
	sameStores(t, leader, reopened, nil)

	// And it carries on rather than starting again. Few enough writes that the
	// log being written does not fill, so no merge can take the position away
	// and the answer does not depend on when a background merge happens to run.
	for i := 200; i < 206; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if resynced := followDB(t, leader, reopened, ReplicaOptions{}); resynced {
		t.Error("a follower that had only been closed started again from a snapshot")
	}
	sameStores(t, leader, reopened, nil)
}

// TestDBFollowerBatchIsAllOrNothing checks the difference from the single-store
// Apply. There a half-applied batch is a fact about the follower's own log and
// its position says so; here the position is something the leader said about
// the whole batch, so half of one must leave the store exactly as it was.
func TestDBFollowerBatchIsAllOrNothing(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	followDB(t, leader, follower, ReplicaOptions{})

	before := follower.Applied()
	keys := follower.Len()

	for i := 20; i < 30; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	next, err := leader.Since(before, &wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	batch := wire.Bytes()

	damaged := []struct {
		name string
		give []byte
		want error
	}{
		// A batch that stops part way through a record and one whose record is
		// damaged are different things and say so differently: the first is a
		// batch that did not arrive, the second is one that did and cannot be
		// trusted.
		{"a batch cut short", batch[:len(batch)-3], ErrorCorruptData},
		{"a batch with a record damaged in it", flipLast(batch), ErrorChecksumMismatch},
	}

	for _, test := range damaged {
		t.Run(test.name, func(t *testing.T) {
			pos, err := follower.Apply(before, next, bytes.NewReader(test.give), ReplicaOptions{})
			if !errors.Is(err, test.want) {
				t.Fatalf("a damaged batch applied with '%v', want %v", err, test.want)
			}
			if pos != before {
				t.Errorf("a refused batch moved the follower to %+v, want %+v", pos, before)
			}
			if got := follower.Applied(); got != before {
				t.Errorf("a refused batch left the follower at %+v, want %+v", got, before)
			}
			if got := follower.Len(); got != keys {
				t.Errorf("a refused batch left %d keys behind, want %d", got, keys)
			}
		})
	}

	// And the whole batch still applies afterwards.
	if _, err := follower.Apply(before, next, bytes.NewReader(batch), ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	sameStores(t, leader, follower, nil)
}

// flipLast damages the last record of a batch, leaving its framing intact so
// that only the checksum can tell.
func flipLast(batch []byte) []byte {
	damaged := append([]byte(nil), batch...)
	damaged[len(damaged)-1] ^= 0xff
	return damaged
}

// TestDBFollowerRefusesTheWrongBatch checks that a batch is only applied to the
// store it was cut for. A batch that arrived twice describes a stretch of the
// leader this follower has already taken.
func TestDBFollowerRefusesTheWrongBatch(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	if err := leader.Write([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	before := follower.Applied()
	if err := leader.Write([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	next, err := leader.Since(before, &wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	batch := wire.Bytes()

	if _, err := follower.Apply(before, next, bytes.NewReader(batch), ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	keys := follower.Len()

	pos, err := follower.Apply(before, next, bytes.NewReader(batch), ReplicaOptions{})
	if !errors.Is(err, ErrorPosition) {
		t.Fatalf("a batch that arrived twice applied with '%v', want %v", err, ErrorPosition)
	}
	if pos != next {
		t.Errorf("the refusal reported %+v, want the position the follower is at, %+v", pos, next)
	}
	if got := follower.Len(); got != keys {
		t.Errorf("the batch was applied anyway: %d keys, want %d", got, keys)
	}
}

// TestDBFollowerReset checks that a follower told to start again does so on the
// disk: the logs go, and so does the record of where it had got to.
func TestDBFollowerReset(t *testing.T) {
	dir := t.TempDir()

	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 400})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	followDB(t, leader, follower, ReplicaOptions{})

	if follower.Len() == 0 || follower.Applied() == (DBPosition{}) {
		t.Fatal("the follower is empty before the reset")
	}

	if err := follower.Reset(); err != nil {
		t.Fatal(err)
	}

	if got := follower.Len(); got != 0 {
		t.Errorf("a reset follower holds %d keys", got)
	}
	if got := follower.Applied(); got != (DBPosition{}) {
		t.Errorf("a reset follower is at %+v, want nothing", got)
	}
	if got := follower.Segments(); got != 1 {
		t.Errorf("a reset follower has %d logs, want the one it writes to", got)
	}

	// Nothing of the old store is left on the disk, and reopening finds nothing.
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 400})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Len(); got != 0 {
		t.Errorf("a reopened reset follower holds %d keys", got)
	}
	if got := reopened.Applied(); got != (DBPosition{}) {
		t.Errorf("a reopened reset follower is at %+v, want nothing", got)
	}

	// And it can take the whole store again.
	followDB(t, leader, reopened, ReplicaOptions{})
	sameStores(t, leader, reopened, nil)
}

// TestDBFollowerStreams drives a follower from a leader's Follow over a pipe,
// which is what a connection would be. The position travels with the records,
// since a DB follower cannot work out where it is from its own logs.
func TestDBFollowerStreams(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 400})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	// A snapshot to start from, taken before anything is streamed.
	var snapshot bytes.Buffer
	at, releaseAt, err := leader.Snapshot(&snapshot, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseAt()
	if err := follower.ApplySnapshot(at, &snapshot, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	const records = 400

	done := make(chan struct{})
	failed := make(chan error, 1)
	var resnapshots atomic.Int64

	go func() {
		// The transport: hand the records and the position they lead to
		// straight to the follower. Over a connection this is where they would
		// be framed and written instead.
		send := func(batch []byte, next DBPosition) error {
			from := follower.Applied()

			got, err := follower.Apply(from, next, bytes.NewReader(batch), ReplicaOptions{})
			if err != nil {
				return err
			}
			if got != next {
				return fmt.Errorf("the follower reached %+v, the leader sent to %+v", got, next)
			}
			return nil
		}

		// A stream can be stranded, by a merge taking the log it was resting
		// at while it waited, and a follower that cannot start again is not a
		// follower. This is the loop a real one runs.
		pos := at
		for {
			if _, err := leader.Follow(pos, nil, send, done, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
				failed <- err
				return
			}
			resnapshots.Add(1)

			var fresh bytes.Buffer

			resumeAt, releaseResume, err := leader.Snapshot(&fresh, ReplicaOptions{})
			if err != nil {
				failed <- err
				return
			}
			defer releaseResume()
			if err := follower.ApplySnapshot(resumeAt, &fresh, ReplicaOptions{}); err != nil {
				failed <- err
				return
			}
			pos = resumeAt
		}
	}()

	for i := 0; i < records; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// Asynchronous, so it arrives when it arrives.
	deadline := time.Now().Add(20 * time.Second)
	for follower.Len() < leader.Len() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	close(done)
	if err := <-failed; err != nil {
		t.Fatalf("the stream ended with %v", err)
	}
	if n := resnapshots.Load(); n > 0 {
		t.Logf("the stream was stranded and started again %d times", n)
	}

	sameStores(t, leader, follower, []string{"key-999"})
}

// TestDBFollowerModel runs a random history against a leader DB and keeps a
// follower DB up with it, checking every key after every round. Both stores
// rotate and merge on their own schedules and neither knows anything about the
// other's files, which is the whole claim logical replication makes.
func TestDBFollowerModel(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	// A different segment size, so the two lay their records out differently
	// and nothing can accidentally depend on them agreeing.
	follower, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 900})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	random := rand.New(rand.NewSource(5))

	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	live := map[string]string{}
	rounds, resyncs := 0, 0

	for step := 0; step < 1200; step++ {
		key := keys[random.Intn(len(keys))]

		switch n := random.Intn(100); {
		case n < 65:
			value := fmt.Sprintf("value-%d", step)
			if err := leader.Write([]byte(key), []byte(value)); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			live[key] = value

		case n < 80:
			if err := leader.Delete([]byte(key)); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			delete(live, key)

		case n < 97:
			opts := ReplicaOptions{BatchSize: int64(1 + random.Intn(400))}
			if followDB(t, leader, follower, opts) {
				resyncs++
			}
			rounds++

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
			// A stretch with nobody replicating, then a merge: this is how a
			// follower falls far enough behind for the log it was reading to be
			// rewritten under it.
			for i := 0; i < 150; i++ {
				value := fmt.Sprintf("burst-%d-%d", step, i)
				burst := keys[random.Intn(len(keys))]
				if err := leader.Write([]byte(burst), []byte(value)); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				live[burst] = value
			}
			if err := leader.Merge(); err != nil {
				t.Fatalf("step %d: Merge: %v", step, err)
			}
		}
	}

	followDB(t, leader, follower, ReplicaOptions{})
	sameStores(t, leader, follower, []string{"never written"})

	t.Logf("%d rounds of catching up, %d of them starting from a snapshot; "+
		"the leader is over %d logs and the follower over %d",
		rounds, resyncs, leader.Segments(), follower.Segments())

	if resyncs < 2 {
		t.Errorf("only %d snapshots, so recovery from a merge was barely exercised", resyncs)
	}
	if rounds-resyncs < 20 {
		t.Errorf("only %d rounds carried on from where the follower was", rounds-resyncs)
	}
}

// TestDBFollowerWritesRecordsBeforeThePosition checks the order, which is the
// whole of what makes a crash here survivable and is invisible in the result of
// any call. The records have to reach the disk before the file that claims
// them: crashing in between means applying a batch twice, which is the same
// records in the same order and changes nothing, while the other order means
// claiming records that were never written.
func TestDBFollowerWritesRecordsBeforeThePosition(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	watcher := &watchedDisk{}
	watcher.install(t)

	follower, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	var wire bytes.Buffer
	at, releaseAt, err := leader.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseAt()

	watcher.reset()
	if err := follower.ApplySnapshot(at, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	recordsBeforePosition(t, watcher, "applying a snapshot")

	// And again for a batch, which is the other path that writes records down
	// and then says it holds them.
	for i := 20; i < 40; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var tail bytes.Buffer
	next, err := leader.Since(at, &tail, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if next == at {
		t.Fatal("the leader had nothing to send, so there is no batch to watch")
	}

	watcher.reset()
	if _, err := follower.Apply(at, next, &tail, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	recordsBeforePosition(t, watcher, "applying a batch")
}

// recordsBeforePosition checks that the records reached a log before the file
// claiming them was written, and that the file was renamed into place rather
// than written where it stands.
func recordsBeforePosition(t *testing.T, watcher *watchedDisk, what string) {
	t.Helper()

	records, position := -1, -1
	for i, op := range watcher.order() {
		if strings.HasPrefix(op, "write:") && strings.HasSuffix(op, segmentSuffix) {
			records = i
		}
		if position < 0 && strings.HasPrefix(op, "write:"+appliedFile) {
			position = i
		}
	}

	if records < 0 {
		t.Fatalf("%s: no records reached a log at all", what)
	}
	if position < 0 {
		t.Fatalf("%s: the position was never written: %v", what, watcher.order())
	}
	if records > position {
		t.Errorf("%s: the position was written before the records it claims:\n%v", what, watcher.order())
	}
	if got := watcher.count("rename", appliedFile); got != 1 {
		t.Errorf("%s: the position was renamed into place %d times, want 1", what, got)
	}
}

// TestDBFollowerIgnoresADamagedPosition checks that the record of how far a
// follower has got is checked rather than believed. It is the only thing saying
// which of a leader's records this store already holds, and one that has been
// damaged would have it carry on from somewhere it has never been.
func TestDBFollowerIgnoresADamagedPosition(t *testing.T) {
	dir := t.TempDir()

	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(dir, smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	followDB(t, leader, follower, ReplicaOptions{})

	if follower.Applied() == (DBPosition{}) {
		t.Fatal("the follower reports no position after catching up")
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, appliedFile)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	damaged := []struct {
		name string
		give []byte
	}{
		{"a byte flipped in the position", flipLast(good)},
		{"the wrong magic", append([]byte("XXXX"), good[4:]...)},
		{"a version this build does not know", append(append([]byte{}, good[:4]...), append([]byte{99}, good[5:]...)...)},
		{"cut short", good[:len(good)-1]},
		{"nothing at all", nil},
	}

	for _, test := range damaged {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(path, test.give, 0o644); err != nil {
				t.Fatal(err)
			}

			reopened, err := OpenDB(dir, smallSegments(4096))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()

			if got := reopened.Applied(); got != (DBPosition{}) {
				t.Errorf("a damaged position was believed: %+v", got)
			}

			// Which costs a snapshot and nothing else.
			if resynced := followDB(t, leader, reopened, ReplicaOptions{}); !resynced {
				t.Error("a follower with no position did not take a snapshot")
			}
			sameStores(t, leader, reopened, nil)
		})
	}
}

// TestDBFollowerRefusesATruncatedSnapshot checks that a snapshot which stops
// part way through a record is not taken for a whole one. Half a snapshot with
// a position on it would be a store missing keys and saying it was up to date,
// which nothing afterwards could notice.
func TestDBFollowerRefusesATruncatedSnapshot(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	for i := 0; i < 40; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	at, releaseAt, err := leader.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseAt()
	whole := wire.Bytes()

	follower, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	err = follower.ApplySnapshot(at, bytes.NewReader(whole[:len(whole)-5]), ReplicaOptions{})

	var corrupt *CorruptAtError
	if !errors.As(err, &corrupt) {
		t.Fatalf("half a snapshot applied with '%v', want a *CorruptAtError", err)
	}
	if got := follower.Applied(); got != (DBPosition{}) {
		t.Errorf("a refused snapshot left the follower claiming %+v", got)
	}

	// Which leaves it needing another one, and able to take it.
	if resynced := followDB(t, leader, follower, ReplicaOptions{}); !resynced {
		t.Error("a follower with a refused snapshot did not take another")
	}
	sameStores(t, leader, follower, nil)
}

// TestDBFollowerAppliesTwiceAfterACrash is the at-least-once claim, run rather
// than asserted in a comment. The records reach the disk before the position
// that claims them, so a crash in between leaves a follower holding records it
// does not admit to — and the same batch arrives again. What must come out of
// that is the same store, since it is the same records in the same order.
func TestDBFollowerAppliesTwiceAfterACrash(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("first")); err != nil {
			t.Fatal(err)
		}
	}

	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	follower, err := OpenDB(dir, smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followDB(t, leader, follower, ReplicaOptions{})
	before := follower.Applied()

	// A batch that supersedes half of what is there, so applying it twice would
	// show up as the wrong value rather than merely as wasted bytes.
	for i := 0; i < 10; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("second")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	next, err := leader.Since(before, &wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	batch := wire.Bytes()

	// The machine goes down between the records and the position: the records
	// are written, the file that claims them is not.
	watcher.fail["rename:"+appliedFile] = errDiskFailed

	if _, err := follower.Apply(before, next, bytes.NewReader(batch), ReplicaOptions{}); !errors.Is(err, errDiskFailed) {
		t.Fatalf("a position that could not be written reported '%v', want the disk's error", err)
	}
	if got := follower.Applied(); got != before {
		t.Fatalf("the follower claims %+v after failing to write it, want %+v", got, before)
	}

	// It is holding records it does not admit to, which is the safe direction.
	if value, ok := liveValue(t, follower, "key-00"); !ok || value != "second" {
		t.Fatalf("key-00 is %q on the follower, want the records that were written", value)
	}

	// So the leader sends the same batch again, and it applies.
	delete(watcher.fail, "rename:"+appliedFile)

	got, err := follower.Apply(before, next, bytes.NewReader(batch), ReplicaOptions{})
	if err != nil {
		t.Fatalf("the same batch again: %v", err)
	}
	if got != next {
		t.Fatalf("the follower reached %+v, want %+v", got, next)
	}

	sameStores(t, leader, follower, nil)

	// And it survives being reopened, which is the only thing that proves the
	// position on the disk agrees with the records beside it.
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDB(dir, smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Applied(); got != next {
		t.Errorf("a reopened follower is at %+v, want %+v", got, next)
	}
	sameStores(t, leader, reopened, nil)
}

// TestDBFollowerSurvivesADisk checks that a follower whose disk refuses says so
// and stays where it was, whichever of the writes it refuses. None of these may
// leave the store claiming records it does not hold, which is the one direction
// that cannot be recovered from.
func TestDBFollowerSurvivesADisk(t *testing.T) {
	failures := []struct {
		name string
		op   func(active uint64) string
	}{
		{"the log refuses the records", func(active uint64) string {
			return fmt.Sprintf("write:%010d%s", active, segmentSuffix)
		}},
		{"the position cannot be written", func(uint64) string { return "write:" + appliedFile + mergeSuffix }},
		{"the position cannot be renamed into place", func(uint64) string { return "rename:" + appliedFile }},
	}

	for _, test := range failures {
		t.Run(test.name, func(t *testing.T) {
			leader, err := OpenDB(t.TempDir(), smallSegments(4096))
			if err != nil {
				t.Fatal(err)
			}
			defer leader.Close()

			for i := 0; i < 20; i++ {
				if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}

			watcher := &watchedDisk{}
			watcher.install(t)

			follower, err := OpenDB(t.TempDir(), smallSegments(4096))
			if err != nil {
				t.Fatal(err)
			}
			defer follower.Close()

			followDB(t, leader, follower, ReplicaOptions{})
			before := follower.Applied()
			keys := follower.Len()

			if err := leader.Write([]byte("after"), []byte("value")); err != nil {
				t.Fatal(err)
			}

			var wire bytes.Buffer
			next, err := leader.Since(before, &wire, ReplicaOptions{})
			if err != nil {
				t.Fatal(err)
			}

			watcher.fail[test.op(follower.Position().Segment)] = errDiskFailed

			if _, err := follower.Apply(before, next, &wire, ReplicaOptions{}); !errors.Is(err, errDiskFailed) {
				t.Fatalf("a refusing disk reported '%v', want the disk's error", err)
			}
			if got := follower.Applied(); got != before {
				t.Fatalf("the follower claims %+v after a failed write, want %+v", got, before)
			}

			// Once the disk works again, so does the follower.
			watcher.fail = map[string]error{}

			if resynced := followDB(t, leader, follower, ReplicaOptions{}); resynced {
				t.Error("a follower that had merely failed a write took a whole new snapshot")
			}
			sameStores(t, leader, follower, nil)

			if follower.Len() < keys {
				t.Errorf("the follower ended with %d keys, fewer than the %d it had", follower.Len(), keys)
			}
		})
	}
}

// TestDBSnapshotReportsADisk checks that a snapshot whose disk refuses part way
// through says so rather than handing over the part it managed. A follower that
// took half a snapshot and a position would be missing keys and claiming to be
// up to date.
func TestDBSnapshotReportsADisk(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(256))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 60; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// One read per frozen log and then no more, so the snapshot fails part way
	// through reading the records out.
	db.mu.RLock()
	oldest := filepath.Base(db.frozen[len(db.frozen)-1].path)
	db.mu.RUnlock()
	watcher.refuseReads(oldest, 0)

	var wire bytes.Buffer
	if _, _, err := db.Snapshot(&wire, ReplicaOptions{}); !errors.Is(err, errDiskFailed) {
		t.Fatalf("a snapshot over a refusing disk reported '%v', want the disk's error", err)
	}

	// And the store is untouched by having been asked: it still answers, and it
	// can still be snapshotted once the disk works.
	watcher.calm()

	follower, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followDB(t, db, follower, ReplicaOptions{})
	sameStores(t, db, follower, nil)
}

// BenchmarkDBSnapshot is what setting up a follower costs the leader: reading
// the live record of every key out of the logs that hold it. The bytes per
// second are the useful number, since a connection slower than this is what
// decides how long a follower takes to start and one faster is not.
func BenchmarkDBSnapshot(b *testing.B) {
	value := make([]byte, 512)

	db, err := OpenDB(b.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 1 << 20, MergeTrigger: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const keys = 20000
	for i := 0; i < keys; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%08d", i)), value); err != nil {
			b.Fatal(err)
		}
	}

	var sized bytes.Buffer

	_, release, err := db.Snapshot(&sized, ReplicaOptions{})
	if err != nil {
		b.Fatal(err)
	}
	release()

	b.SetBytes(int64(sized.Len()))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, release, err := db.Snapshot(io.Discard, ReplicaOptions{})
		if err != nil {
			b.Fatal(err)
		}
		release()
	}

	b.StopTimer()
	b.ReportMetric(float64(keys), "keys")
}

// BenchmarkDBFollowerApply is a follower taking records: what one costs beyond
// the connection that carried it. The leader is left out on purpose — any wire
// is slower than this.
func BenchmarkDBFollowerApply(b *testing.B) {
	for _, size := range []int{16, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			leader, err := OpenDB(b.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 4 << 20, MergeTrigger: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer leader.Close()

			value := make([]byte, size)
			for i := 0; i < 4096; i++ {
				if err := leader.Write([]byte(fmt.Sprintf("key-%08d", i)), value); err != nil {
					b.Fatal(err)
				}
			}

			var wire bytes.Buffer

			at, release, err := leader.Snapshot(&wire, ReplicaOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer release()

			snapshot := wire.Bytes()

			follower, err := OpenDB(b.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 4 << 20, MergeTrigger: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer follower.Close()

			b.SetBytes(int64(len(snapshot)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := follower.ApplySnapshot(at, bytes.NewReader(snapshot), ReplicaOptions{}); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(len(snapshot))/4096, "B/record")
		})
	}
}

// TestDBFollowerPositionIsNoMoreDurableThanTheRecords checks that the file
// claiming a leader's records waits for the disk exactly when those records
// did. Syncing it under a policy that does not sync the records would leave a
// store that survived losing power claiming records that did not survive it,
// which is the one direction the ordering exists to rule out.
func TestDBFollowerPositionIsNoMoreDurableThanTheRecords(t *testing.T) {
	policies := []struct {
		name  string
		sync  SyncPolicy
		syncs int
	}{
		{"always", SyncAlways, 1},
		{"never", SyncNever, 0},
		{"every", SyncEvery, 0},
	}

	for _, policy := range policies {
		t.Run(policy.name, func(t *testing.T) {
			leader, err := OpenDB(t.TempDir(), smallSegments(4096))
			if err != nil {
				t.Fatal(err)
			}
			defer leader.Close()

			for i := 0; i < 10; i++ {
				if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}

			watcher := &watchedDisk{}
			watcher.install(t)

			follower, err := OpenDB(t.TempDir(), DBOptions{
				Sync: policy.sync, SegmentSize: 4096, MergeTrigger: 1,
				Interval: time.Hour, // the timer must not fire during the test
			})
			if err != nil {
				t.Fatal(err)
			}
			defer follower.Close()

			watcher.reset()
			followDB(t, leader, follower, ReplicaOptions{})

			if got := watcher.count("sync", appliedFile+mergeSuffix); got != policy.syncs {
				t.Errorf("under %s the position was synced %d times, want %d:\n%v",
					policy.name, got, policy.syncs, watcher.order())
			}

			// It is written and renamed into place whatever the policy: what
			// changes is only whether the process waits for the disk.
			if got := watcher.count("rename", appliedFile); got == 0 {
				t.Errorf("under %s the position was never renamed into place", policy.name)
			}
			sameStores(t, leader, follower, nil)
		})
	}
}

// TestDBFollowerResetDropsThePositionFirst checks the order Reset works in,
// which is the order that writing the records before the position works in,
// turned round: delete the claim before the thing claimed.
//
// Removing the logs and then failing to remove the position leaves a store that
// comes back claiming a stretch of a leader whose records it has just deleted.
// Nothing downstream can notice — the leader is asked for what comes after a
// position it recognises, answers correctly, and the follower settles a whole
// snapshot short with every check passing. It was found by a chaos run that
// restarted a follower often enough to hit it.
func TestDBFollowerResetDropsThePositionFirst(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	for i := 0; i < 60; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	follower, err := OpenDB(dir, smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	if follower.Applied() == (DBPosition{}) {
		t.Fatal("the follower has no position to lose")
	}

	// The disk refuses to let go of the position.
	watcher.fail["remove:"+appliedFile] = errDiskFailed

	if err := follower.Reset(); !errors.Is(err, errDiskFailed) {
		t.Fatalf("a reset that could not drop the position reported '%v'", err)
	}

	// Nothing else may have happened: the store still holds what it held, and
	// still says so, which is a store that can be reset again later.
	if follower.Len() == 0 {
		t.Error("a refused reset emptied the store anyway")
	}
	if follower.Applied() == (DBPosition{}) {
		t.Error("a refused reset forgot the position in memory while leaving it on the disk")
	}
	sameStores(t, leader, follower, nil)

	// And once the disk lets go, the reset takes everything with it — including
	// on the disk, which is the half a restart would otherwise resurrect.
	watcher.calm()
	watcher.fail = map[string]error{}

	if err := follower.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Applied(); got != (DBPosition{}) {
		t.Fatalf("a reopened store claims %+v after a reset", got)
	}
	if got := reopened.Len(); got != 0 {
		t.Fatalf("a reopened store holds %d keys after a reset", got)
	}

	// Which means it asks for a snapshot rather than a tail, and ends up whole.
	if resynced := followDB(t, leader, reopened, ReplicaOptions{}); !resynced {
		t.Error("a reset follower carried on from a position instead of taking a snapshot")
	}
	sameStores(t, leader, reopened, nil)
}

// holdFrozen picks a frozen log, counting back from the oldest, and holds it —
// checking that there are enough logs and taking the hold under the same lock,
// writing more through more() until there are.
//
// Every part of that matters. Picking a log and then holding it is a race: a
// merge can take it in the gap, and the position built from it would name
// contents that are already gone. Counting the logs and then picking is the
// same race one step earlier, since merging is running the whole time and can
// collapse the store to a single log between the two. Snapshot does it this way
// for the same reason.
//
// A negative index counts down from the newest instead of up from the oldest.
func holdFrozen(t *testing.T, db *DB, fromOldest int, more func()) (DBPosition, func()) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for {
		pos, release, ok := tryHoldFrozen(db, fromOldest)
		if ok {
			return pos, release
		}
		if time.Now().After(deadline) {
			t.Fatalf("never had a log at %d from the oldest to hold", fromOldest)
		}
		more()
	}
}

// tryHoldFrozen is one attempt, with the lock held throughout.
func tryHoldFrozen(db *DB, fromOldest int) (DBPosition, func(), bool) {
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	db.mu.RLock()

	want := len(db.frozen) - 1 - fromOldest
	if fromOldest < 0 {
		want = 0
	}
	if want < 0 || want >= len(db.frozen) {
		db.mu.RUnlock()
		return DBPosition{}, nil, false
	}

	seg := db.frozen[want]
	record, raw, err := readRecordAt(seg.file, seg.bytes, 0)
	id := seg.id()
	db.mu.RUnlock()

	if err != nil {
		return DBPosition{}, nil, false
	}

	pos := DBPosition{Segment: id, Log: Position{Offset: int64(len(raw)), Last: 0, Crc: record.Crc}}
	return pos, db.hold(pos), true
}

// TestDBHoldKeepsALogFromMerging checks the thing a hold is for: the log a
// follower is reading stays where it is, and the position naming it goes on
// working, however much merging the store does around it.
func TestDBHoldKeepsALogFromMerging(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The same few keys over and over, so merging has plenty to do.
	round := 0
	write := func(int) {
		for _, key := range []string{"alpha", "beta", "gamma"} {
			if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%03d", key, round))); err != nil {
				t.Fatal(err)
			}
		}
		round++
	}
	for round < 40 {
		write(0)
	}

	// Not the oldest log, but one with an older log beneath it: a hold has to
	// pin the log it names as well as the ones after it, and the oldest has
	// nothing to be merged with, so holding that one would not tell the
	// difference.
	held, release := holdFrozen(t, db, 1, func() { write(0) })

	// Enough writing and merging to have taken that log several times over.
	for i := 0; i < 200; i++ {
		write(0)
	}

	for attempt := 0; attempt < 50; attempt++ {
		if _, err := db.Since(held, io.Discard, ReplicaOptions{}); err != nil {
			t.Fatalf("a held position stopped working: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	// And a follower reading from it gets the whole store, which is the point
	// of the position still being good.
	follower := &KeyValueStore{}
	if _, err := follower.Apply(Position{}, bytes.NewReader(nil), ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	tailInto(t, db, follower, held, ReplicaOptions{})

	for _, key := range []string{"alpha", "beta", "gamma"} {
		want := fmt.Sprintf("%s-%03d", key, round-1)
		got, err := follower.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if string(got) != want {
			t.Errorf("%s reads as %q on the follower, want %q", key, got, want)
		}
	}

	// Letting go lets the merging that was blocked happen.
	before := db.Segments()
	release()
	release() // releasing twice is harmless

	deadline := time.Now().Add(10 * time.Second)
	for db.Segments() >= before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := db.Segments(); got >= before {
		t.Errorf("%d logs after the hold was released, want fewer than the %d it was blocking at", got, before)
	}
}

// TestDBHoldIsCounted checks that two followers on one log both have to let go
// before merging comes back, which is what a leader with more than one of them
// needs.
func TestDBHoldIsCounted(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	write := func(from, to int) {
		t.Helper()
		for round := from; round < to; round++ {
			for _, key := range []string{"alpha", "beta"} {
				if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%03d", key, round))); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	settle := func() int {
		deadline := time.Now().Add(5 * time.Second)
		for {
			was := db.Segments()
			time.Sleep(20 * time.Millisecond)
			if db.Segments() == was || time.Now().After(deadline) {
				return was
			}
		}
	}

	write(0, 60)

	// Held before anything is allowed to settle, and picked inside the same
	// lock: picking a log and then holding it races the merging that is already
	// running.
	at, first := holdFrozen(t, db, 0, func() { write(0, 1) })
	second := db.Hold(at)

	write(60, 200)
	held := settle()

	first()
	write(200, 260)
	stillHeld := settle()

	if _, err := db.Since(at, io.Discard, ReplicaOptions{}); err != nil {
		t.Errorf("the held log was merged while one of its two holders still had it: %v", err)
	}
	if stillHeld < held {
		t.Errorf("merging resumed at %d logs while one of two holders still had it, from %d", stillHeld, held)
	}

	second()

	deadline := time.Now().Add(10 * time.Second)
	for db.Segments() >= stillHeld && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := db.Segments(); got >= stillHeld {
		t.Errorf("%d logs after both holders let go, want fewer than the %d they were holding at", got, stillHeld)
	}
}

// TestDBFollowIsNotStrandedByAMerge is the case the hold exists for, end to
// end: a stream running against a store that is being written to and merged
// throughout, which without a hold is knocked off its position sooner or later
// and has to take a whole new snapshot.
func TestDBFollowIsNotStrandedByAMerge(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 400})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 600})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	// Something in the store before the snapshot, so the position it comes with
	// names a record. A snapshot of a store whose active log is empty has
	// nowhere to point but the start of that log, and that position stops being
	// checkable the moment the log freezes — which on one core is before this
	// goroutine is next scheduled.
	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("early-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	at, releaseAt, err := leader.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.ApplySnapshot(at, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	failed := make(chan error, 1)

	// The snapshot's hold goes to Follow, which takes one of its own before
	// letting this one go. Releasing it here instead leaves a moment with the
	// log the stream starts from unheld — which on one core is however long the
	// goroutine takes to be scheduled, and is lost every time.
	go func() {
		_, err := leader.Follow(at, releaseAt, func(batch []byte, next DBPosition) error {
			_, err := follower.Apply(follower.Applied(), next, bytes.NewReader(batch), ReplicaOptions{})
			return err
		}, done, ReplicaOptions{})
		failed <- err
	}()

	// Write in bursts with pauses between them, so the stream spends time
	// caught up and waiting — which is exactly when it is resting on a log that
	// merging would otherwise take.
	live := map[string]string{}
	for burst := 0; burst < 40; burst++ {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%02d", i)
			value := fmt.Sprintf("value-%d-%d", burst, i)
			if err := leader.Write([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			live[key] = value
		}
		time.Sleep(2 * time.Millisecond)
	}

	deadline := time.Now().Add(20 * time.Second)
	for follower.Applied() != leader.Position() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	close(done)

	if err := <-failed; err != nil {
		t.Fatalf("the stream was stranded: %v", err)
	}

	for key, want := range live {
		got, err := follower.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if string(got) != want {
			t.Errorf("%s is %q on the follower, want %q", key, got, want)
		}
	}
	// Nothing is held now, so merging is free to catch up. Waiting for it to
	// settle is the only honest way to ask: merges run in the background and
	// there is nothing to assert about when.
	deadline = time.Now().Add(20 * time.Second)
	for {
		was := leader.Segments()
		time.Sleep(20 * time.Millisecond)
		if leader.Segments() == was || time.Now().After(deadline) {
			break
		}
	}

	leader.mu.RLock()
	stillHeld := len(leader.held)
	leader.mu.RUnlock()

	logs := leader.Segments()
	t.Logf("the leader settled at %d logs after 800 writes, and the stream was never knocked off", logs)

	if stillHeld != 0 {
		t.Errorf("%d logs are still held after the stream ended: %v", stillHeld, leader.held)
	}
	if logs > 20 {
		t.Errorf("%d logs once nothing is held, which is not a store that merged", logs)
	}
}

// TestDBHoldPinsFromTheOldestHolder checks that two followers at different
// places pin from the older of them, not the newer. Taking the newest would
// leave the log the slower follower is still reading free to be merged, which
// is the reader a hold exists for.
func TestDBHoldPinsFromTheOldestHolder(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	write := func(rounds int) {
		t.Helper()
		for round := 0; round < rounds; round++ {
			for _, key := range []string{"alpha", "beta"} {
				if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%03d", key, round))); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	settle := func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			was := db.Segments()
			time.Sleep(20 * time.Millisecond)
			if db.Segments() == was || time.Now().After(deadline) {
				return
			}
		}
	}

	usable := func(pos DBPosition) bool {
		_, err := db.Since(pos, io.Discard, ReplicaOptions{})
		return err == nil
	}

	// The slow follower goes first, and is held before anything is allowed to
	// settle: picking a log and then waiting is a race with the merging that is
	// already running, and there may be only one log left by the time it stops.
	write(60)

	behindAt, slow := holdFrozen(t, db, 0, func() { write(1) })
	behind := behindAt.Segment
	defer slow()

	// Now logs pile up behind the hold rather than being merged away, so there
	// is something newer to be the second follower.
	write(200)
	settle()

	aheadAt, quick := holdFrozen(t, db, -1, func() { write(1) }) // the newest
	ahead := aheadAt.Segment
	if ahead == behind {
		t.Fatalf("no newer log to hold: %d is the one already held", ahead)
	}

	write(400)
	settle()

	// With the floor at the older of the two, both positions still work. Taking
	// the newer as the floor would leave everything older than it — the slow
	// follower's log among them — free to be merged, and that log would keep
	// its id while becoming a different file.
	if !usable(behindAt) {
		t.Errorf("the position in log %d stopped working while the follower behind was reading it", behind)
	}
	if !usable(aheadAt) {
		t.Errorf("the position in log %d stopped working while a follower was reading it", ahead)
	}

	// And the follower that is ahead letting go changes nothing, because the
	// floor was never it.
	quick()
	write(500)
	settle()

	if !usable(behindAt) {
		t.Errorf("the position in log %d stopped working after only the follower ahead of it let go", behind)
	}
}
