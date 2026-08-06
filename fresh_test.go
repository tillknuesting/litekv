package litekv

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// A replica is behind its leader — that is what asynchronous replication is —
// and the person who notices is the client that wrote to the leader a
// millisecond ago and read from a replica. Reached is what a replica answers
// that with: the client carries the leader's position and gets told the store
// is behind rather than getting an older answer that looks current.
//
// These tests are about which position is compared against and what happens at
// the edges of that, since the comparison itself is three lines.

func TestReachedRefusesAStoreThatIsBehind(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	// An empty store has reached the position of an empty store, and nothing
	// else. The zero position is what a client that has never written carries.
	if err := follower.Reached(Position{}); err != nil {
		t.Errorf("an empty store has not reached the empty position: %v", err)
	}

	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	after := leader.Position()
	if err := leader.Reached(after); err != nil {
		t.Errorf("a leader has not reached its own position: %v", err)
	}
	if err := follower.Reached(after); !errors.Is(err, ErrorStale) {
		t.Errorf("a follower holding nothing reported '%v', want %v", err, ErrorStale)
	}

	catchUp(t, leader, follower)

	if err := follower.Reached(after); err != nil {
		t.Errorf("a caught-up follower has not reached %+v: %v", after, err)
	}

	// A position past the end of the log is being behind however far past it
	// is, including on the leader: a leader that lost its tail to a crash is
	// behind a client that was told the write had landed.
	beyond := Position{Offset: after.Offset + 100, Last: after.Offset, Crc: after.Crc}
	if err := leader.Reached(beyond); !errors.Is(err, ErrorStale) {
		t.Errorf("a position past the end of the log reported '%v', want %v", err, ErrorStale)
	}
}

// TestReachedChecksTheRecordAndNotOnlyTheLength is the reason this is not a
// comparison of two integers. Two stores of the same length can hold entirely
// different records, and a client's position is not a follower's: nothing
// checked it on the way in.
func TestReachedChecksTheRecordAndNotOnlyTheLength(t *testing.T) {
	one := &KeyValueStore{}
	other := &KeyValueStore{}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		if err := one.Write(key, []byte("mine")); err != nil {
			t.Fatal(err)
		}
		if err := other.Write(key, []byte("thin")); err != nil {
			t.Fatal(err)
		}
	}

	mine, theirs := one.Position(), other.Position()
	if mine.Offset != theirs.Offset {
		t.Fatalf("the two logs are %d and %d bytes; the test needs them equal", mine.Offset, theirs.Offset)
	}
	if mine == theirs {
		t.Fatal("two stores holding different records reported the same position")
	}

	if err := other.Reached(mine); !errors.Is(err, ErrorDiverged) {
		t.Errorf("a store as long as the leader but holding other records reported '%v', want %v", err, ErrorDiverged)
	}

	// A position naming a record this log holds, but claiming it ends
	// somewhere else, is the same answer: the check is the whole record.
	bent := mine
	bent.Offset--
	if err := one.Reached(bent); !errors.Is(err, ErrorDiverged) {
		t.Errorf("a position ending mid-record reported '%v', want %v", err, ErrorDiverged)
	}
}

func TestAwaitReturnsWhenTheRecordsArrive(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	if err := leader.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	after := leader.Position()

	done := make(chan error, 1)
	go func() { done <- follower.Await(after, nil) }()

	// Nothing arrives for a moment, which is the ordinary case: the client is
	// ahead of the stream rather than the stream being broken. Returning here
	// would not be a slow test, it would be a wrong answer — the records are
	// not in this store and nothing has claimed otherwise.
	time.Sleep(time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Await returned '%v' before the records arrived", err)
	default:
	}

	catchUp(t, leader, follower)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Await reported '%v' once the records had arrived", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Await did not return once the records had arrived")
	}
}

func TestAwaitGivesUpWhenTold(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	if err := leader.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	// A deadline that has already passed, since a read that waits has to be
	// able to stop waiting.
	until := make(chan struct{})
	close(until)

	if err := follower.Await(leader.Position(), until); !errors.Is(err, ErrorStale) {
		t.Errorf("Await gave up with '%v', want %v", err, ErrorStale)
	}

	// A position beyond the end of the log is only being behind, however far
	// beyond it is, so that one waits until it is told not to.
	if err := follower.Await(Position{Offset: 1 << 20, Last: 0, Crc: 1}, until); !errors.Is(err, ErrorStale) {
		t.Errorf("a position past the end of the log reported '%v', want %v", err, ErrorStale)
	}

	// A position this log does not hold is refused at once instead, since
	// waiting will not bring one, and nil for until is safe for that.
	if err := leader.Await(Position{Offset: leader.Position().Offset, Last: 0, Crc: 1}, nil); !errors.Is(err, ErrorDiverged) {
		t.Errorf("a position this log does not hold reported '%v', want %v", err, ErrorDiverged)
	}
}

// The DB half. There is no record to check here — a follower holds none of the
// leader's bytes — so what these are about is which of the two positions a
// store keeps gets compared.

func TestDBReachedRefusesAReplicaThatIsBehind(t *testing.T) {
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

	for i := 0; i < 50; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	after := leader.Position()
	if err := leader.Reached(after); err != nil {
		t.Errorf("a leader has not reached its own position: %v", err)
	}
	if err := follower.Reached(after); !errors.Is(err, ErrorStale) {
		t.Errorf("a follower that has applied nothing reported '%v', want %v", err, ErrorStale)
	}

	followDB(t, leader, follower, ReplicaOptions{})

	if err := follower.Reached(after); err != nil {
		t.Errorf("a caught-up follower has not reached %+v: %v", after, err)
	}

	// A write the follower has not been sent yet is the case this exists for.
	if err := leader.Write([]byte("late"), []byte("record")); err != nil {
		t.Fatal(err)
	}
	late := leader.Position()

	if err := follower.Reached(late); !errors.Is(err, ErrorStale) {
		t.Errorf("a follower a record behind reported '%v', want %v", err, ErrorStale)
	}
	if got, err := follower.Read([]byte("late")); err == nil {
		t.Errorf("the follower answered 'late' with %q before the record reached it", got)
	}

	followDB(t, leader, follower, ReplicaOptions{})

	if err := follower.Reached(late); err != nil {
		t.Errorf("the follower has not reached %+v after catching up: %v", late, err)
	}
}

// TestDBReachedOrdersTheWholeStream walks a position through the leader's logs.
// Every mark it passed it has reached, in whatever log that mark was in, and
// anything ahead of it it has not — which is the whole claim, and the one that
// offsets alone cannot make across a rotation.
func TestDBReachedOrdersTheWholeStream(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	var marks []DBPosition
	for i := 0; i < 40; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
		marks = append(marks, leader.Position())
	}

	last := marks[len(marks)-1]
	if last.Segment == marks[0].Segment {
		t.Fatal("the leader never rotated; the test needs more than one log")
	}

	// The numbers only ever go up, however many logs they cross.
	for i, mark := range marks {
		if mark.Log.Seq == 0 {
			t.Fatalf("the position after write %d carries no number: %+v", i, mark)
		}
		if i > 0 && mark.Log.Seq <= marks[i-1].Log.Seq {
			t.Fatalf("write %d took the number back from %d to %d", i, marks[i-1].Log.Seq, mark.Log.Seq)
		}
		if err := leader.Reached(mark); err != nil {
			t.Fatalf("the leader has not reached its own position after write %d (%+v): %v", i, mark, err)
		}
	}

	// One record further on than it has written is ahead of it, in whichever
	// log that record would land in.
	ahead := last
	ahead.Log.Seq++
	if err := leader.Reached(ahead); !errors.Is(err, ErrorStale) {
		t.Errorf("a position one record ahead reported '%v', want %v", err, ErrorStale)
	}
}

// TestDBReachedFallsBackToLogsAndOffsets is the same question asked with a
// position from before records were numbered — a client holding one across the
// upgrade, or a store whose logs predate it. There is nothing to compare but
// the log id and the offset, which is what this did before the numbers and is
// exact everywhere except at a log boundary.
func TestDBReachedFallsBackToLogsAndOffsets(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	var marks []DBPosition
	for i := 0; i < 40; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}

		mark := leader.Position()
		mark.Log.Seq = 0 // as a position cut before there were numbers
		marks = append(marks, mark)
	}

	last := marks[len(marks)-1]
	if last.Segment == marks[0].Segment {
		t.Fatal("the leader never rotated; the test needs more than one log")
	}

	for i, mark := range marks {
		if err := leader.Reached(mark); err != nil {
			t.Fatalf("the leader has not reached %+v, its own position after write %d: %v", mark, i, err)
		}
	}

	// One byte further on in the log it is writing, and one whole log further
	// on, are both ahead of it.
	ahead := last
	ahead.Log.Offset++
	if err := leader.Reached(ahead); !errors.Is(err, ErrorStale) {
		t.Errorf("a position one byte ahead reported '%v', want %v", err, ErrorStale)
	}

	ahead = last
	ahead.Segment++
	ahead.Log.Offset = 0
	if err := leader.Reached(ahead); !errors.Is(err, ErrorStale) {
		t.Errorf("a position in the next log reported '%v', want %v", err, ErrorStale)
	}
}

// TestDBReachedAfterAPromotion is why a store cannot simply judge by the
// position it has applied. A promoted replica keeps that position — it is where
// the leader it followed got to — while its own log carries on from there under
// a new term, and the writes a client makes to it now are named by positions of
// its own.
func TestDBReachedAfterAPromotion(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	replica, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()

	if _, err := leader.Promote(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	followDB(t, leader, replica, ReplicaOptions{})

	// A record after the snapshot, so that the leader's position names one
	// rather than being the start of the log the snapshot left empty. See
	// TestDBReachedAtTheStartOfALog.
	if err := leader.Write([]byte("last"), []byte("word")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, replica, ReplicaOptions{})

	// A position from the leader it followed, which it holds every record of.
	old := leader.Position()
	if err := replica.Reached(old); err != nil {
		t.Errorf("the replica has not reached the position it caught up to: %v", err)
	}

	if _, err := replica.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := replica.Write([]byte("mine"), []byte("now")); err != nil {
		t.Fatal(err)
	}

	mine := replica.Position()
	if mine.Term <= old.Term {
		t.Fatalf("the promoted store is at term %d, the leader it replaced at %d", mine.Term, old.Term)
	}
	if err := replica.Reached(mine); err != nil {
		t.Errorf("a promoted store has not reached the position it just handed out: %v", err)
	}

	// The position it applied at is still a position it has reached, since its
	// own log carries on from there rather than replacing it.
	if err := replica.Reached(old); err != nil {
		t.Errorf("a promoted store no longer admits to %+v: %v", old, err)
	}
}

// TestDBReachedRefusesAPositionFromAReplacedLeader is the one case that cannot
// be answered. The position names a record in a log this store never had, under
// a leader it has stopped following, and whether that record survived the
// handover is not a thing a follower of the new one can work out.
func TestDBReachedRefusesAPositionFromAReplacedLeader(t *testing.T) {
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
	if _, err := leader.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	if got := follower.Applied().Term; got != 2 {
		t.Fatalf("the follower applied at term %d, want 2", got)
	}

	stale := follower.Applied()
	stale.Term = 1

	if err := follower.Reached(stale); !errors.Is(err, ErrorSuperseded) {
		t.Errorf("a position from a replaced leader reported '%v', want %v", err, ErrorSuperseded)
	}

	// Nor is it something waiting will fix, so Await says so rather than
	// holding the read until its deadline.
	if err := follower.Await(stale, nil); !errors.Is(err, ErrorSuperseded) {
		t.Errorf("Await on a superseded position reported '%v', want %v", err, ErrorSuperseded)
	}

	// A term above anything it has heard of is the other direction, and that
	// one is only being behind: the news may yet arrive.
	ahead := follower.Applied()
	ahead.Term = 9
	if err := follower.Reached(ahead); !errors.Is(err, ErrorStale) {
		t.Errorf("a position from a leader it has not heard of reported '%v', want %v", err, ErrorStale)
	}
}

// TestDBReachedAtTheStartOfALog is the question the numbers were added for. A
// position at the start of a log names no record, and the end of the log before
// it is the same point in the stream: a leader whose active log is empty hands
// out the first, and a follower holding every record it ever wrote rests at the
// second, because that is the position that can be checked.
//
// Nothing about the offsets or the log ids says those are the same place. The
// numbers do, and a follower that holds everything says so instead of saying it
// is behind.
func TestDBReachedAtTheStartOfALog(t *testing.T) {
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

	// A snapshot freezes the active log, so the leader's position is the start
	// of the empty one it left behind, and the follower's is the end of the one
	// it froze. The two hold exactly the same records.
	followDB(t, leader, follower, ReplicaOptions{})
	sameStores(t, leader, follower, nil)

	at := leader.Position()
	if at.Log.Offset != 0 {
		t.Fatalf("the leader's active log is not empty; the test needs the boundary: %+v", at)
	}
	if follower.Applied().Segment >= at.Segment {
		t.Fatalf("the follower stepped into the empty log after all: %+v", follower.Applied())
	}
	if at.Log.Seq == 0 {
		t.Fatal("the leader's position carries no number; nothing below is being tested")
	}

	// Different logs, different offsets, same place in the stream.
	if got := follower.Applied().Log.Seq; got != at.Log.Seq {
		t.Errorf("the follower is at number %d and the leader at %d, holding the same records", got, at.Log.Seq)
	}
	if err := follower.Reached(at); err != nil {
		t.Errorf("a follower holding every record reported '%v' at the boundary", err)
	}

	// And it is still an ordering: one more record on the leader, not yet sent,
	// is ahead of the follower.
	if err := leader.Write([]byte("one"), []byte("more")); err != nil {
		t.Fatal(err)
	}
	ahead := leader.Position()
	if err := follower.Reached(ahead); !errors.Is(err, ErrorStale) {
		t.Errorf("a follower a record behind reported '%v', want %v", err, ErrorStale)
	}

	followDB(t, leader, follower, ReplicaOptions{})

	if err := follower.Reached(ahead); err != nil {
		t.Errorf("the follower has not reached %+v after catching up: %v", ahead, err)
	}
}

func TestDBAwaitReturnsWhenTheBatchArrives(t *testing.T) {
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

	if err := leader.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	if err := leader.Write([]byte("key"), []byte("newer")); err != nil {
		t.Fatal(err)
	}
	after := leader.Position()

	done := make(chan error, 1)
	go func() { done <- follower.Await(after, nil) }()

	// The waiter has to be woken by the applying, which is the thing a
	// follower's store does that no write of its own does.
	time.Sleep(time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Await returned '%v' before the batch arrived", err)
	default:
	}

	followDB(t, leader, follower, ReplicaOptions{})

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Await reported '%v' once the batch had arrived", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Await did not return once the batch had arrived")
	}

	if got, err := follower.Read([]byte("key")); err != nil || string(got) != "newer" {
		t.Errorf("the follower answered %q, '%v' after Await returned, want 'newer'", got, err)
	}

	// A closed store is not one to wait on either.
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	if err := follower.Await(leader.Position(), nil); !errors.Is(err, ErrorClosed) {
		t.Errorf("Await on a closed store reported '%v', want %v", err, ErrorClosed)
	}
}

// TestFollowerTermNeverOutrunsItsPosition holds the invariant Reached rests on:
// a store's term is above the term it applied at only when it has been
// promoted. That is what tells a leader from a follower here, and it is why the
// term and the position are one write rather than two — written separately,
// every fault between them leaves a follower looking promoted, and it stays
// looking promoted until the next batch arrives.
//
// So this fails each disk operation a catch-up makes, one run per operation,
// and checks the two never come apart: in memory, and after the reopen that
// stands in for the process coming back.
func TestFollowerTermNeverOutrunsItsPosition(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	leader, err := OpenDB(t.TempDir(), smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	// A leader on a term above nothing, so that catching up moves the
	// follower's term as well as its position.
	if _, err := leader.Promote(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// A clean run first, to learn how many operations there are to fail.
	clean := t.TempDir()
	watcher.reset()
	watcher.inject(clean, 0, 0, 0)

	measured, err := OpenDB(clean, smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	if err := chaosFollow(leader, measured, ReplicaOptions{BatchSize: 256}); err != nil {
		t.Fatalf("a clean run failed: %v", err)
	}
	total := watcher.operations()
	if err := measured.Close(); err != nil {
		t.Fatal(err)
	}
	if total < 10 {
		t.Fatalf("only %d operations to fail, which is not a sweep", total)
	}

	together := func(t *testing.T, db *DB, when string) {
		t.Helper()

		if term, applied := db.Term(), db.Applied(); term != applied.Term {
			t.Errorf("%s: the follower is at term %d having applied at term %d", when, term, applied.Term)
		}
	}

	faults := 0

	for n := 1; n <= total; n++ {
		dir := t.TempDir()

		watcher.calm()
		watcher.reset()
		watcher.inject(dir, n, 0, 0)

		follower, err := OpenDB(dir, smallSegments(1024))
		if err != nil {
			watcher.calm()
			if follower, err = OpenDB(dir, smallSegments(1024)); err != nil {
				t.Fatalf("operation %d: the store could not be opened even once the disk worked: %v", n, err)
			}
		}
		if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
			faults++
		}

		watcher.calm()
		together(t, follower, fmt.Sprintf("operation %d", n))
		follower.Close()

		reopened, err := OpenDB(dir, smallSegments(1024))
		if err != nil {
			t.Fatalf("operation %d: reopening after the fault: %v", n, err)
		}
		together(t, reopened, fmt.Sprintf("operation %d, reopened", n))

		// Which is the answer that matters: a follower that looked promoted
		// would judge the leader's position against its own logs, which are a
		// different set of files and would answer whatever they answered.
		if err := reopened.Reached(leader.Position()); err != nil && !errors.Is(err, ErrorStale) {
			t.Errorf("operation %d: Reached reported '%v'", n, err)
		}
		reopened.Close()
	}

	t.Logf("failed each of %d operations in turn; %d of them stopped the follower", total, faults)

	if faults == 0 {
		t.Error("no injected fault ever reached the follower, so nothing was tested")
	}
}
