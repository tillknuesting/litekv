package litekv

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Expiry is the one thing in this package whose answer changes without anybody
// writing anything, so these tests move the clock rather than sleep. A test
// that sleeps to wait for an expiry is slow when it passes and flaky when the
// machine is busy, and it can only ever check the coarse case.

// at moves the package clock for the duration of a test, and hands back a
// function to move it further.
func at(t *testing.T, start time.Time) func(time.Duration) {
	t.Helper()

	previous := now
	current := start
	now = func() time.Time { return current }
	t.Cleanup(func() { now = previous })

	return func(d time.Duration) { current = current.Add(d) }
}

// TestExpiryOnlyCostsTheRecordsThatUseIt checks the reason expiry is a record
// version rather than a field on every record: a store that never sets one
// holds exactly the bytes it always did.
func TestExpiryOnlyCostsTheRecordsThatUseIt(t *testing.T) {
	plain := &KeyValueStore{}
	if err := plain.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	expiring := &KeyValueStore{}
	if err := expiring.WriteExpiring([]byte("key"), []byte("value"), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if got, want := plain.Size(), int64(headerSizeV1+len("key")+len("value")); got != want {
		t.Errorf("a record with no expiry takes %d bytes, want %d", got, want)
	}
	if got, want := expiring.Size(), int64(headerSizeV2+len("key")+len("value")); got != want {
		t.Errorf("a record with an expiry takes %d bytes, want %d", got, want)
	}

	// And a zero time asks for what Write asks for, down to the layout.
	zero := &KeyValueStore{}
	if err := zero.WriteExpiring([]byte("key"), []byte("value"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got, want := zero.Size(), plain.Size(); got != want {
		t.Errorf("WriteExpiring with no time took %d bytes, want the %d a plain write takes", got, want)
	}

	// Both layouts verify, which is the part a checksum folded over the wrong
	// fields would get wrong.
	for _, store := range []*KeyValueStore{plain, expiring, zero} {
		if err := store.Verify(); err != nil {
			t.Errorf("Verify: %v", err)
		}
	}
}

// TestExpiryHidesAKeyWhenItsTimeComes is the whole feature: a key answers until
// the moment it is due and not afterwards.
func TestExpiryHidesAKeyWhenItsTimeComes(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

	kvs := &KeyValueStore{}
	if err := kvs.WriteExpiring([]byte("temporary"), []byte("value"), start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Write([]byte("permanent"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if got, err := kvs.Read([]byte("temporary")); err != nil || string(got) != "value" {
		t.Fatalf("before its time the key reads as %q, '%v'", got, err)
	}

	// A second before, and then the moment itself: expiry is not in the future
	// once the clock has reached it.
	advance(time.Minute - time.Nanosecond)
	if _, err := kvs.Read([]byte("temporary")); err != nil {
		t.Fatalf("a nanosecond before its time the key reads as '%v'", err)
	}

	advance(time.Nanosecond)
	if _, err := kvs.Read([]byte("temporary")); !errors.Is(err, ErrorKeyExpired) {
		t.Fatalf("at its time the key reads as '%v', want %v", err, ErrorKeyExpired)
	}

	// View and Modified answer the same way, and the permanent key is untouched.
	if err := kvs.View([]byte("temporary"), func([]byte) error { return nil }); !errors.Is(err, ErrorKeyExpired) {
		t.Errorf("View on an expired key gave '%v', want %v", err, ErrorKeyExpired)
	}
	if _, err := kvs.Modified([]byte("temporary")); !errors.Is(err, ErrorKeyExpired) {
		t.Errorf("Modified on an expired key gave '%v', want %v", err, ErrorKeyExpired)
	}
	if got, err := kvs.Read([]byte("permanent")); err != nil || string(got) != "value" {
		t.Errorf("a key with no expiry went with one that had: %q, '%v'", got, err)
	}

	// Writing over it brings it back, since the newest record is what counts.
	if err := kvs.Write([]byte("temporary"), []byte("again")); err != nil {
		t.Fatal(err)
	}
	if got, err := kvs.Read([]byte("temporary")); err != nil || string(got) != "again" {
		t.Errorf("after rewriting, the key reads as %q, '%v'", got, err)
	}
}

// TestExpiryIsAnInstantNotADuration checks that the expiry survives being
// written down and read back, which a duration measured from the reader's clock
// would not.
func TestExpiryIsAnInstantNotADuration(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

	due := start.Add(time.Hour)
	path := filepath.Join(t.TempDir(), "store.kv")

	store, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteExpiring([]byte("key"), []byte("value"), due); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopened half an hour later, it has half an hour left.
	advance(30 * time.Minute)

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.Read([]byte("key")); err != nil {
		t.Fatalf("half way to its time the key reads as '%v'", err)
	}

	record, _, err := parseRecordAt(reopened.Data, reopened.Index["key"])
	if err != nil {
		t.Fatal(err)
	}
	if got := record.ExpiresAt(); !got.Equal(due) {
		t.Errorf("the record is due at %v, want %v", got, due)
	}
	if record.Version != recordV2 {
		t.Errorf("a record with an expiry came back as version %d, want %d", record.Version, recordV2)
	}

	advance(30 * time.Minute)
	if _, err := reopened.Read([]byte("key")); !errors.Is(err, ErrorKeyExpired) {
		t.Errorf("after its time the key reads as '%v', want %v", err, ErrorKeyExpired)
	}
}

// TestCompactionReclaimsExpiredRecords checks that an expired record is dropped
// by compaction the way a tombstone is. Dropping it outright is safe for a
// single store, since there is nothing older anywhere for it to be hiding.
func TestCompactionReclaimsExpiredRecords(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

	kvs := &KeyValueStore{}

	// The same key twice, so there is an older record for the expired one to be
	// hiding, and compaction has to drop both rather than uncover it.
	if err := kvs.Write([]byte("temporary"), []byte("older")); err != nil {
		t.Fatal(err)
	}
	if err := kvs.WriteExpiring([]byte("temporary"), []byte("newer"), start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Write([]byte("permanent"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	advance(time.Hour)

	before := kvs.Size()
	if err := kvs.Compact(); err != nil {
		t.Fatal(err)
	}

	if kvs.Size() >= before {
		t.Errorf("compaction reclaimed nothing: %d bytes, was %d", kvs.Size(), before)
	}
	if _, err := kvs.Read([]byte("temporary")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("after compaction the expired key reads as '%v', want %v", err, ErrorKeyNotFound)
	}
	if got, err := kvs.Read([]byte("permanent")); err != nil || string(got) != "value" {
		t.Errorf("compaction lost a key that had not expired: %q, '%v'", got, err)
	}

	records := 0
	if err := kvs.ForEach(func(key, value []byte, deleted bool) bool {
		records++
		if string(key) == "temporary" {
			t.Errorf("an expired record survived compaction, holding %q", value)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if records != 1 {
		t.Errorf("%d records after compaction, want the one that had not expired", records)
	}
}

// TestDBMergeKeepsAnExpiredRecordUntilItReachesTheOldestLog is the tombstone
// rule, for expiry. An expired record says there is no value; dropping it while
// an older log still holds one brings that older value back.
func TestDBMergeKeepsAnExpiredRecordUntilItReachesTheOldestLog(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 256, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// An old value, then enough writing to freeze it away, then an expiring
	// record for the same key in a much newer log.
	if err := db.Write([]byte("key"), []byte("older")); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if err := db.Write(fmt.Appendf(nil, "filler-%02d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.WriteExpiring([]byte("key"), []byte("newer"), start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for i := 40; i < 80; i++ {
		if err := db.Write(fmt.Appendf(nil, "filler-%02d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	advance(time.Hour)

	if _, err := db.Read([]byte("key")); !errors.Is(err, ErrorKeyExpired) {
		t.Fatalf("after its time the key reads as '%v', want %v", err, ErrorKeyExpired)
	}

	// A merge that reaches the oldest log may drop it, and that is what Merge
	// does. The older value must go with it rather than be uncovered.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	got, err := db.Read([]byte("key"))
	if !errors.Is(err, ErrorKeyNotFound) && !errors.Is(err, ErrorKeyExpired) {
		t.Fatalf("after merging, the key reads as %q, '%v'", got, err)
	}
	if errors.Is(err, ErrorKeyNotFound) {
		return // dropped outright, which is what a merge over the whole store may do
	}
}

// TestDBExpiryAcrossReplication checks that an expiry crosses to a follower and
// means the same moment on both. It is stored as an instant for exactly this,
// and a duration would have started again when it arrived.
func TestDBExpiryAcrossReplication(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

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

	if err := leader.Write([]byte("permanent"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := leader.WriteExpiring([]byte("temporary"), []byte("value"), start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	followDB(t, leader, follower, ReplicaOptions{})

	if got, err := follower.Read([]byte("temporary")); err != nil || string(got) != "value" {
		t.Fatalf("before its time the follower reads %q, '%v'", got, err)
	}

	// Half an hour on: neither end has reached it.
	advance(30 * time.Minute)
	if _, err := follower.Read([]byte("temporary")); err != nil {
		t.Errorf("half way to its time the follower reads '%v'", err)
	}

	// And past it: both ends agree, without either being told.
	advance(31 * time.Minute)

	leaderErr := func() error { _, err := leader.Read([]byte("temporary")); return err }()
	followerErr := func() error { _, err := follower.Read([]byte("temporary")); return err }()

	if !errors.Is(leaderErr, ErrorKeyExpired) {
		t.Errorf("the leader reads the expired key as '%v'", leaderErr)
	}
	if !errors.Is(followerErr, ErrorKeyExpired) {
		t.Errorf("the follower reads the expired key as '%v'", followerErr)
	}

	// A snapshot taken now carries the live keys and not the expired one, the
	// same as it leaves out tombstones.
	fresh, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()

	followDB(t, leader, fresh, ReplicaOptions{})

	if _, err := fresh.Read([]byte("temporary")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("a snapshot carried an expired record: '%v'", err)
	}
	if got, err := fresh.Read([]byte("permanent")); err != nil || string(got) != "value" {
		t.Errorf("a snapshot lost a live key: %q, '%v'", got, err)
	}
}

// TestDBTieredKeepsExpiredRecords is TestDBTieredKeepsTombstones for expiry,
// and it is the same rule: a merge that stops short of the oldest log may not
// drop an expired record, because a log it left out still holds the value that
// record is hiding.
func TestDBTieredKeepsExpiredRecords(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

	// Merging is left to the test, so the runs are known.
	db, err := OpenDB(t.TempDir(), smallSegments(150))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The oldest log holds the value.
	if err := db.Write([]byte("doomed"), []byte("the old value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Write([]byte("filler"), fmt.Appendf(nil, "%150s", "x")); err != nil {
		t.Fatal(err)
	}

	// A newer log holds one that expires, and enough after it to make a run.
	if err := db.WriteExpiring([]byte("doomed"), []byte("the new value"), start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		if err := db.Write(fmt.Appendf(nil, "other%d", i), fmt.Appendf(nil, "%150s", "y")); err != nil {
			t.Fatal(err)
		}
	}

	advance(time.Hour)

	db.mu.RLock()
	frozen := len(db.frozen)
	db.mu.RUnlock()
	if frozen < 4 {
		t.Fatalf("only %d frozen logs; the test needs several", frozen)
	}

	// Merge a run that stops short of the oldest log, which is where the value
	// still lives.
	db.mu.RLock()
	victims := append([]*diskSegment(nil), db.frozen[:frozen-1]...)
	db.mu.RUnlock()

	db.mergeMu.Lock()
	err = db.mergeLocked(victims, false) // false: an older log remains
	db.mergeMu.Unlock()
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	// The expired record has to have survived, or the old value comes back.
	if _, err := db.Read([]byte("doomed")); !errors.Is(err, ErrorKeyExpired) {
		got, _ := db.Read([]byte("doomed"))
		t.Fatalf("after a partial merge the key reads as %q, '%v', want %v", got, err, ErrorKeyExpired)
	}

	// And nothing yields it as live.
	if err := db.ForEach(func(key, value []byte) bool {
		if string(key) == "doomed" {
			t.Errorf("ForEach yielded an expired key, holding %q", value)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
}

// TestDBForEachSkipsExpired checks the thing a caller walking a store relies on:
// what it is handed is what a read would have answered.
func TestDBForEachSkipsExpired(t *testing.T) {
	start := time.Unix(1700000000, 0)
	advance := at(t, start)

	db, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Write([]byte("permanent"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteExpiring([]byte("temporary"), []byte("value"), start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	seen := func() []string {
		var keys []string
		if err := db.ForEach(func(key, value []byte) bool {
			keys = append(keys, string(key))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return keys
	}

	if got := seen(); len(got) != 2 {
		t.Fatalf("before its time ForEach yielded %v, want both keys", got)
	}

	advance(time.Hour)

	got := seen()
	if len(got) != 1 || got[0] != "permanent" {
		t.Errorf("after its time ForEach yielded %v, want only the permanent key", got)
	}
}
