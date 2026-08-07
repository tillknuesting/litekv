package litekv

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// A follower that goes away while merging happens comes back holding a position
// into a log that is no longer there. Before the records were numbered there was
// nothing to do about that: an offset into a log that has been folded into
// another one says nothing about the log that replaced it, so the answer was
// ErrorDiverged and a snapshot of the whole store.
//
// The numbers survive a merge, so the record the position named can be looked
// for. These are about when that works and, more importantly, when it must not
// be tried.

// strandedLeader is a store whose logs a follower can be left behind by: enough
// records to fill several, with a key written throughout so that there is
// something to disagree about.
func strandedLeader(t *testing.T, dir string, opts DBOptions) (*DB, map[string]string) {
	t.Helper()

	db, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	live := map[string]string{}
	for round := 0; round < 20; round++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key-%02d", i)
			value := fmt.Sprintf("value-%02d-%d", round, i)
			if err := db.Write([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			live[key] = value
		}
	}

	return db, live
}

// follow brings a follower up to date and reports whether it needed a snapshot
// to do it, which is the whole question here.
func follow(t *testing.T, leader, follower *DB, opts ReplicaOptions) (snapshots int) {
	t.Helper()

	pos := follower.Applied()

	for rounds := 0; ; rounds++ {
		if rounds > 200 {
			t.Fatal("the follower is going round in circles")
		}

		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, opts)
		if pos == (DBPosition{}) || errors.Is(err, ErrorDiverged) {
			wire.Reset()

			at, release, err := leader.Snapshot(&wire, opts)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if err := follower.ApplySnapshot(at, &wire, opts); err != nil {
				release()
				t.Fatalf("ApplySnapshot: %v", err)
			}
			release()

			snapshots++
			pos = at
			continue
		}
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			return snapshots
		}

		got, err := follower.Apply(pos, next, &wire, opts)
		if err != nil {
			t.Fatalf("Apply(%+v -> %+v): %v", pos, next, err)
		}
		if got != next {
			t.Fatalf("the follower reached %+v, the leader sent to %+v", got, next)
		}
		pos = next
	}
}

// mergeShortOfTheOldest folds every frozen log but the oldest into one, which
// is the shape of a merge that has not reached the bottom of the store: nothing
// is dropped, because something older could still hold what a tombstone hides.
// Merge, the exported one, always reaches the oldest log and so always drops.
func mergeShortOfTheOldest(t *testing.T, db *DB, include uint64) {
	t.Helper()

	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	db.mu.RLock()
	frozen := append([]*diskSegment(nil), db.frozen...)
	db.mu.RUnlock()

	victims := frozen[:len(frozen)-1] // newest first, stopping short of the oldest
	if len(victims) < 2 {
		t.Fatalf("only %d logs to merge, want at least two short of the oldest", len(victims))
	}

	held := false
	for _, seg := range victims {
		held = held || seg.id() == include
	}
	if !held {
		t.Fatalf("log %d is not in the run about to be merged", include)
	}

	if err := db.mergeLocked(victims, false); err != nil {
		t.Fatal(err)
	}
}

// TestStrandedFollowerCarriesOn is the one this was built for. The follower gets
// part way through, the leader merges the logs it was reading out from under it,
// and it carries on from the number it had rather than being sent everything
// again.
func TestStrandedFollowerCarriesOn(t *testing.T) {
	// Merging by hand, so that the test says when a follower is stranded rather
	// than hoping the background gets there.
	opts := smallSegments(300)

	leader, live := strandedLeader(t, t.TempDir(), opts)
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	// Caught up to begin with, by a snapshot as any new follower is.
	if got := follow(t, leader, follower, ReplicaOptions{}); got != 1 {
		t.Fatalf("a new follower took %d snapshots, want 1", got)
	}

	// It goes away. The leader carries on writing, fills more logs, and folds
	// the ones the follower was resting on into an older one.
	for round := 20; round < 30; round++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key-%02d", i)
			value := fmt.Sprintf("value-%02d-%d", round, i)
			if err := leader.Write([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			live[key] = value
		}
	}

	stranded := follower.Applied()
	if seg := leader.frozenSegment(stranded.Segment); seg == nil {
		t.Fatal("the follower's log is already gone; the test has not started yet")
	}

	mergeShortOfTheOldest(t, leader, stranded.Segment)

	// The log it was resting in is gone, or is a different file under the same
	// name. Either way its offset means nothing now.
	leader.mu.RLock()
	seg := leader.frozenSegment(stranded.Segment)
	gone := seg == nil || seg.bytes != stranded.Log.Offset
	leader.mu.RUnlock()

	if !gone {
		t.Fatal("the merge left the follower's log alone; the test has not started yet")
	}

	// And it carries on without a snapshot.
	if got := follow(t, leader, follower, ReplicaOptions{}); got != 0 {
		t.Errorf("a stranded follower took %d snapshots, want none", got)
	}

	sameStores(t, leader, follower, nil)
	for key, value := range live {
		if got, err := follower.Read([]byte(key)); err != nil || string(got) != value {
			t.Errorf("%s = %q, '%v', want %q", key, got, err, value)
		}
	}
}

// TestStrandedFollowerAcrossADroppedTombstone is the case that must not be
// resumed. A merge that reaches the oldest log drops tombstones, and a follower
// carried across one would never hear that a key was deleted — it holds an older
// value for that key and nothing in what follows would replace it. The whole
// store goes instead.
func TestStrandedFollowerAcrossADroppedTombstone(t *testing.T) {
	opts := smallSegments(300)

	leader, _ := strandedLeader(t, t.TempDir(), opts)
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	if got := follow(t, leader, follower, ReplicaOptions{}); got != 1 {
		t.Fatalf("a new follower took %d snapshots, want 1", got)
	}

	// A key deleted while the follower is away. The follower holds a value for
	// it; if it is carried across the merge that drops the tombstone, it keeps
	// that value for ever.
	if err := leader.Delete([]byte("key-00")); err != nil {
		t.Fatal(err)
	}
	for round := 20; round < 30; round++ {
		for i := 1; i < 5; i++ {
			if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d-%d", round, i))); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Merge everything, which reaches the oldest log and so drops the tombstone.
	if err := leader.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Read([]byte("key-00")); !errors.Is(err, ErrorKeyNotFound) {
		t.Fatalf("the tombstone was not dropped, so this tests nothing: %v", err)
	}

	leader.mu.RLock()
	dropped := false
	for _, seg := range leader.frozen {
		dropped = dropped || seg.dropped
	}
	leader.mu.RUnlock()

	if !dropped {
		t.Fatal("no log admits to having dropped anything; the test has not started yet")
	}

	// So the follower is sent the whole store rather than carried across it.
	if got := follow(t, leader, follower, ReplicaOptions{}); got != 1 {
		t.Errorf("a follower behind a dropped tombstone took %d snapshots, want 1", got)
	}

	// Which is the point: the deletion reaches it.
	if _, err := follower.Read([]byte("key-00")); err == nil {
		t.Error("the follower still holds a key the leader deleted")
	}
	sameStores(t, leader, follower, nil)
}

// TestStrandedPositionIsCheckedNotGuessed holds the resume to the same standard
// as the offset check it stands in for: the record has to be there, with the
// number claimed and the checksum carried.
func TestStrandedPositionIsCheckedNotGuessed(t *testing.T) {
	opts := smallSegments(300)

	leader, _ := strandedLeader(t, t.TempDir(), opts)
	defer leader.Close()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	// A position naming a record the store does hold, which is what a stranded
	// follower's looks like once its own log has been merged away.
	var sound DBPosition
	for _, seg := range leader.frozen {
		seg.scan(func(pos int64, raw []byte, r Record) bool {
			if r.Seq == 0 {
				return true
			}
			sound = DBPosition{
				Term:    leader.term,
				Segment: 999, // a log that never existed
				Log:     Position{Offset: pos + int64(len(raw)), Last: pos, Crc: r.Crc, Seq: r.Seq + 1},
			}
			return false
		})
		if sound.Log.Seq != 0 {
			break
		}
	}
	if sound.Log.Seq == 0 {
		t.Fatal("no numbered record to build a position from")
	}

	if _, ok := leader.resumeAt(sound); !ok {
		t.Error("a position naming a record the store holds was not resumed")
	}

	for _, test := range []struct {
		name  string
		bend  func(DBPosition) DBPosition
		count bool
	}{
		{"a checksum from somewhere else", func(p DBPosition) DBPosition { p.Log.Crc ^= 1; return p }, false},
		{"a number no record carries", func(p DBPosition) DBPosition { p.Log.Seq += 1 << 40; return p }, false},
		{"no number at all", func(p DBPosition) DBPosition { p.Log.Seq = 0; return p }, false},
		{"the start of a log", func(p DBPosition) DBPosition { p.Log.Offset = 0; return p }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, ok := leader.resumeAt(test.bend(sound)); ok {
				t.Errorf("%s was resumed", test.name)
			}
		})
	}
}
