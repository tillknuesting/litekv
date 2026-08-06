package litekv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

// Fencing is the answer to two stores both taking writes, which is the one
// thing replication here cannot repair. The position check refuses to splice
// one log onto another, so nothing is corrupted — but a checksum cannot tell
// you that a leader has no business being one, and writes acknowledged by the
// wrong leader are found to be worthless and thrown away. A term can tell you,
// because it only ever goes up.

// TestPromoteRaisesTheTerm checks the ordinary path: a store starts at nothing,
// promotion raises it, and it survives being reopened.
func TestPromoteRaisesTheTerm(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}

	if got := db.Term(); got != 0 {
		t.Errorf("a fresh store is at term %d, want 0", got)
	}

	for want := uint64(1); want <= 3; want++ {
		got, err := db.Promote()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("promotion gave term %d, want %d", got, want)
		}
		if db.Term() != want {
			t.Errorf("the store reports term %d, want %d", db.Term(), want)
		}
	}

	// A term that did not survive a restart would be no fence at all: the store
	// would come back believing itself current and take writes again.
	if err := db.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Term(); got != 3 {
		t.Errorf("a reopened store is at term %d, want 3", got)
	}
	if got := reopened.Position().Term; got != 3 {
		t.Errorf("its positions carry term %d, want 3", got)
	}
}

// TestFencedLeaderStopsTakingWrites is the point of the whole thing: a store
// that hears of a newer leader stops being one, and cannot find that out any
// other way.
func TestFencedLeaderStopsTakingWrites(t *testing.T) {
	old, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	if _, err := old.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := old.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	// A follower that has been round a newer leader asks the old one for
	// records. That is the only moment the old one can learn it was replaced.
	ahead := old.Position()
	ahead.Term += 5

	if _, err := old.Since(ahead, io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorFenced) {
		t.Fatalf("a leader asked by a newer follower reported '%v', want %v", err, ErrorFenced)
	}

	// And from that moment it is not a leader.
	if err := old.Write([]byte("key"), []byte("again")); !errors.Is(err, ErrorFenced) {
		t.Errorf("a fenced store took a write, reporting '%v'", err)
	}
	if err := old.Delete([]byte("key")); !errors.Is(err, ErrorFenced) {
		t.Errorf("a fenced store took a delete, reporting '%v'", err)
	}
	if _, _, err := old.Snapshot(io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorFenced) {
		t.Errorf("a fenced store served a snapshot, reporting '%v'", err)
	}

	// Reads carry on. A fenced store is not broken, it is not in charge.
	if got, err := old.Read([]byte("key")); err != nil || string(got) != "value" {
		t.Errorf("a fenced store stopped answering: %q, '%v'", got, err)
	}

	// Promoting past what it heard makes it a leader again.
	term, err := old.Promote()
	if err != nil {
		t.Fatal(err)
	}
	if term <= 5 {
		t.Errorf("promotion gave term %d, want one above the %d it had heard of", term, 5)
	}
	if err := old.Write([]byte("key"), []byte("again")); err != nil {
		t.Errorf("a promoted store still refuses writes: %v", err)
	}
}

// TestFencedFollowerRefusesAnOldLeader is the other half: a follower that has
// been round a newer leader will not take records from the one it replaced.
func TestFencedFollowerRefusesAnOldLeader(t *testing.T) {
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

	if _, err := leader.Promote(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	followDB(t, leader, follower, ReplicaOptions{})
	sameStores(t, leader, follower, nil)

	// The follower goes round a newer leader, which is what a promotion
	// elsewhere looks like from here.
	if got := follower.Term(); got != 1 {
		t.Fatalf("the follower is at term %d, want the leader's 1", got)
	}

	var wire bytes.Buffer
	at, release, err := leader.Snapshot(&wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	release()

	stale := at
	stale.Term = 0 // as an older leader would have sent it

	if err := follower.ApplySnapshot(stale, bytes.NewReader(wire.Bytes()), ReplicaOptions{}); !errors.Is(err, ErrorFenced) {
		t.Errorf("a follower took a snapshot from an older term, reporting '%v'", err)
	}
	if err := follower.ApplySnapshot(at, bytes.NewReader(wire.Bytes()), ReplicaOptions{}); err != nil {
		t.Errorf("a follower refused a snapshot from its own term: %v", err)
	}

	// And a batch from an older term goes the same way, with nothing applied.
	before := follower.Applied()
	if err := leader.Write([]byte("late"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	wire.Reset()
	next, err := leader.Since(before, &wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}

	staleNext := next
	staleNext.Term = 0

	if _, err := follower.Apply(before, staleNext, bytes.NewReader(wire.Bytes()), ReplicaOptions{}); !errors.Is(err, ErrorFenced) {
		t.Errorf("a follower took a batch from an older term, reporting '%v'", err)
	}
	if got := follower.Applied(); got != before {
		t.Errorf("a refused batch moved the follower to %+v", got)
	}
}

// TestPromotedFollowerFencesTheOldLeader is the failover it is all for, run end
// to end: a replica is promoted, the old leader hears about it through the one
// follower still asking, and stops.
func TestPromotedFollowerFencesTheOldLeader(t *testing.T) {
	old, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	replica, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()

	if _, err := old.Promote(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := old.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	followDB(t, old, replica, ReplicaOptions{})

	// Somebody outside decides the old leader is gone, and promotes.
	term, err := replica.Promote()
	if err != nil {
		t.Fatal(err)
	}
	if term != 2 {
		t.Fatalf("the promoted replica is at term %d, want 2", term)
	}
	if err := replica.Write([]byte("after"), []byte("promotion")); err != nil {
		t.Fatalf("the promoted replica will not take writes: %v", err)
	}

	// The old leader is still up and still thinks it is in charge, which is the
	// whole problem. It goes on taking writes until it hears otherwise.
	if err := old.Write([]byte("doomed"), []byte("value")); err != nil {
		t.Fatalf("the old leader stopped on its own, which it cannot do: %v", err)
	}

	// It hears the moment anything with the newer term asks it for records.
	if _, err := old.Since(replica.Position(), io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorFenced) {
		t.Fatalf("the old leader answered a newer term with '%v', want %v", err, ErrorFenced)
	}
	if err := old.Write([]byte("doomed"), []byte("again")); !errors.Is(err, ErrorFenced) {
		t.Errorf("the old leader took a write after being fenced: '%v'", err)
	}

	// And it stays fenced across a restart, which is what makes it a fence
	// rather than a note in memory.
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFollowerAdoptsTheTermFromABatch checks the half of adoption a snapshot
// hides. A follower that starts from a snapshot takes the term with it, so the
// batch path is never the thing that raised it — unless the leader is promoted
// after the follower is already caught up, which is what a leader doing its own
// failover dance looks like from here.
func TestFollowerAdoptsTheTermFromABatch(t *testing.T) {
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

	if _, err := leader.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Write([]byte("first"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if resynced := followDB(t, leader, follower, ReplicaOptions{}); !resynced {
		t.Fatal("a fresh follower did not take a snapshot")
	}
	if got := follower.Term(); got != 1 {
		t.Fatalf("after the snapshot the follower is at term %d, want 1", got)
	}

	// Promoted again with the follower already caught up, so what carries the
	// new term across is a batch and nothing else.
	if _, err := leader.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Write([]byte("second"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if resynced := followDB(t, leader, follower, ReplicaOptions{}); resynced {
		t.Fatal("the follower took another snapshot, so a batch was not what carried the term")
	}
	if got := follower.Term(); got != 2 {
		t.Errorf("after a batch from term 2 the follower is at term %d", got)
	}
	sameStores(t, leader, follower, nil)

	// And it survives being reopened, which is what makes the follower proof
	// against the leader it has just replaced coming back.
	if got := follower.Position().Term; got != 2 {
		t.Errorf("the follower's positions carry term %d, want 2", got)
	}
}
