package litekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// A batch is several records stored together or not at all. One record is
// already atomic — it decodes whole or it does not, and recovery cuts the log
// back to the last one that did — and these tests are about the "or not at
// all" for several, which is the marker's whole job.

// markersIn counts the batch markers in a store's log, so that a test can say
// the marker is there as well as saying the records are.
func markersIn(t *testing.T, kvs *KeyValueStore) int {
	t.Helper()

	markers := 0
	for at := int64(0); at < int64(len(kvs.Data)); {
		record, next, err := parseRecordAt(kvs.Data, at)
		if err != nil {
			t.Fatalf("the log will not decode at %d: %v", at, err)
		}
		if record.Type == RecordTypeBatch {
			markers++
		}
		at = next
	}
	return markers
}

func TestBatchStoresEverythingInIt(t *testing.T) {
	kvs := &KeyValueStore{}

	var b Batch
	if b.Len() != 0 {
		t.Errorf("a fresh batch holds %d records", b.Len())
	}

	// An empty batch is not an error, it is nothing to do.
	if err := kvs.WriteBatch(&b); err != nil {
		t.Errorf("an empty batch reported '%v'", err)
	}
	if len(kvs.Data) != 0 {
		t.Errorf("an empty batch wrote %d bytes", len(kvs.Data))
	}

	b.Write([]byte("from"), []byte("emptied"))
	b.Write([]byte("to"), []byte("filled"))
	b.WriteExpiring([]byte("session"), []byte("token"), time.Now().Add(time.Hour))
	b.Delete([]byte("gone"))

	if b.Len() != 4 {
		t.Errorf("the batch holds %d records, want 4", b.Len())
	}
	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}

	if got := markersIn(t, kvs); got != 1 {
		t.Errorf("the log holds %d markers, want 1", got)
	}

	for key, want := range map[string]string{"from": "emptied", "to": "filled", "session": "token"} {
		got, err := kvs.Read([]byte(key))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, '%v', want %q", key, got, err, want)
		}
	}
	if _, err := kvs.Read([]byte("gone")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("a key deleted in a batch reads as '%v', want %v", err, ErrorKeyDeleted)
	}

	// The marker is not a record anybody wrote, so nothing that walks the store
	// hands it out.
	seen := 0
	if err := kvs.ForEach(func(key, value []byte, deleted bool) bool {
		if len(key) == 0 {
			t.Errorf("a walk of the store yielded a record with no key")
		}
		seen++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 4 {
		t.Errorf("a walk of the store yielded %d records, want 4", seen)
	}
	if got := len(kvs.Index); got != 4 {
		t.Errorf("the store indexes %d keys, want 4", got)
	}

	if err := kvs.Verify(); err != nil {
		t.Errorf("a store holding a batch does not verify: %v", err)
	}

	// And the index can be worked out again from the records alone.
	kvs.Index = nil
	if err := kvs.RebuildIndex(); err != nil {
		t.Fatal(err)
	}
	if got, err := kvs.Read([]byte("to")); err != nil || string(got) != "filled" {
		t.Errorf("after rebuilding the index, to = %q, '%v'", got, err)
	}
}

// TestBatchLastWriteWins holds the order inside a batch, which is the order the
// records are in and nothing cleverer.
func TestBatchLastWriteWins(t *testing.T) {
	kvs := &KeyValueStore{}

	var b Batch
	b.Write([]byte("key"), []byte("first"))
	b.Write([]byte("key"), []byte("second"))
	b.Write([]byte("doomed"), []byte("value"))
	b.Delete([]byte("doomed"))

	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}

	if got, err := kvs.Read([]byte("key")); err != nil || string(got) != "second" {
		t.Errorf("key = %q, '%v', want 'second'", got, err)
	}
	if _, err := kvs.Read([]byte("doomed")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("doomed reads as '%v', want %v", err, ErrorKeyDeleted)
	}

	// Reset keeps the batch usable, and empties it.
	b.Reset()
	if b.Len() != 0 {
		t.Errorf("a reset batch holds %d records", b.Len())
	}
	b.Write([]byte("key"), []byte("third"))
	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}
	if got, err := kvs.Read([]byte("key")); err != nil || string(got) != "third" {
		t.Errorf("key = %q, '%v', want 'third'", got, err)
	}
	if got := markersIn(t, kvs); got != 2 {
		t.Errorf("two batches left %d markers", got)
	}
}

func TestBatchSurvivesReopening(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/store.kv"

	kvs, err := Open(path, Options{Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}

	var b Batch
	for i := 0; i < 5; i++ {
		b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}
	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Write([]byte("after"), []byte("the batch")); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		if got, err := reopened.Read([]byte(key)); err != nil || string(got) != "value" {
			t.Errorf("%s = %q, '%v' after reopening", key, got, err)
		}
	}
	if got, err := reopened.Read([]byte("after")); err != nil || string(got) != "the batch" {
		t.Errorf("after = %q, '%v'", got, err)
	}
	if got := markersIn(t, reopened); got != 1 {
		t.Errorf("the reopened log holds %d markers, want 1", got)
	}
}

// TestBatchTornAtEveryLength is the whole point. The disk takes the batch's
// write and stops part way through it, at every length there is, and the store
// that comes back afterwards holds all of the batch or none of it — never the
// three records of five that happened to fit.
func TestBatchTornAtEveryLength(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	// One clean run, to learn how long the batch's write is.
	clean := t.TempDir()
	kvs, err := Open(clean+"/store.kv", Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	if err := kvs.Write([]byte("before"), []byte("the batch")); err != nil {
		t.Fatal(err)
	}

	before := int64(len(kvs.Data))
	batch := func(b *Batch) {
		for i := 0; i < 5; i++ {
			b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		}
	}

	var b Batch
	batch(&b)
	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}
	whole := int64(len(kvs.Data)) - before
	if err := kvs.Close(); err != nil {
		t.Fatal(err)
	}
	if whole < 100 {
		t.Fatalf("the batch's write is %d bytes, which is not much of a sweep", whole)
	}

	torn := 0
	for cut := int64(1); cut < whole; cut++ {
		dir := t.TempDir()

		watcher.calm()
		store, err := Open(dir+"/store.kv", Options{Sync: SyncNever})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Write([]byte("before"), []byte("the batch")); err != nil {
			t.Fatal(err)
		}

		// The disk takes this much of the batch's write and no more, which is
		// what losing power in the middle of one looks like. The count starts
		// from here, so it is the batch's own length that is being cut.
		watcher.inject(dir, 0, 0, cut)

		var b Batch
		batch(&b)
		if err := store.WriteBatch(&b); err == nil {
			t.Fatalf("cut at %d: a write the disk cut short reported no error", cut)
		}
		store.Close()
		watcher.calm()

		reopened, err := Open(dir+"/store.kv", Options{Sync: SyncNever})
		if err != nil {
			t.Fatalf("cut at %d: reopening: %v", cut, err)
		}

		// Whatever was there before the batch is still there. The batch itself
		// is all present or all absent, and never in between.
		if got, err := reopened.Read([]byte("before")); err != nil || string(got) != "the batch" {
			t.Errorf("cut at %d: the record before the batch reads %q, '%v'", cut, got, err)
		}

		held := 0
		for i := 0; i < 5; i++ {
			if _, err := reopened.Read([]byte(fmt.Sprintf("key-%d", i))); err == nil {
				held++
			}
		}
		if held != 0 && held != 5 {
			t.Errorf("cut at %d: the store came back holding %d of the batch's 5 records", cut, held)
		}
		if held == 0 {
			torn++
		}

		if err := reopened.Verify(); err != nil {
			t.Errorf("cut at %d: the store does not verify: %v", cut, err)
		}
		reopened.Close()
	}

	t.Logf("cut the batch's write at each of %d lengths; %d of them lost it entirely", whole-1, torn)

	if torn == 0 {
		t.Error("no cut ever lost the batch, so nothing was tested")
	}
}

// TestBatchInADBIsOneLog checks the thing rotation could take away: a batch has
// to be in one log, because half of it in a frozen log is exactly what no
// recovery can put right.
func TestBatchInADBIsOneLog(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A batch far larger than the log it goes into.
	var b Batch
	for i := 0; i < 40; i++ {
		b.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value"))
	}
	if err := db.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}

	if got := db.Segments(); got != 2 {
		t.Errorf("the store is in %d logs, want the batch in one and an empty one after it", got)
	}

	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if got, err := db.Read([]byte(key)); err != nil || string(got) != "value" {
			t.Fatalf("%s = %q, '%v'", key, got, err)
		}
	}

	// And it is still one batch after the log it is in has been frozen, merged
	// and read back.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if got, err := db.Read([]byte(key)); err != nil || string(got) != "value" {
			t.Errorf("after merging, %s = %q, '%v'", key, got, err)
		}
	}
}

// TestMergeDropsTheMarkers is what a merge is entitled to do with a batch: the
// records are already durable, and the file the merge writes is renamed into
// place whole, so the atomicity the marker was carrying has been provided by
// something else by then.
func TestMergeDropsTheMarkers(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(1<<20))
	if err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 3; round++ {
		var b Batch
		for i := 0; i < 5; i++ {
			b.Write([]byte(fmt.Sprintf("key-%d-%d", round, i)), []byte("value"))
		}
		if err := db.WriteBatch(&b); err != nil {
			t.Fatal(err)
		}

		db.mu.Lock()
		err = db.rotateLocked()
		db.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	// Every log the merge left, read the long way.
	db.mu.RLock()
	frozen := append([]*diskSegment(nil), db.frozen...)
	db.mu.RUnlock()

	for _, seg := range frozen {
		if err := scanSegment(seg.file, seg.bytes, func(pos int64, raw []byte, r Record) bool {
			if r.Type == RecordTypeBatch {
				t.Errorf("log %d still holds a marker at %d", seg.id(), pos)
			}
			return true
		}); err != nil {
			t.Fatalf("scanning log %d: %v", seg.id(), err)
		}
	}

	for round := 0; round < 3; round++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key-%d-%d", round, i)
			if got, err := db.Read([]byte(key)); err != nil || string(got) != "value" {
				t.Errorf("%s = %q, '%v' after merging", key, got, err)
			}
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// And the store still opens, which is where a marker left beside records it
	// no longer describes would be found out.
	reopened, err := OpenDB(dir, smallSegments(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got, err := reopened.Read([]byte("key-2-4")); err != nil || string(got) != "value" {
		t.Errorf("after reopening, key-2-4 = %q, '%v'", got, err)
	}
}

// TestBatchRefusedByTheLogLeavesNothing is the other half of all or nothing: a
// disk that will not take the write at all. What must not be left behind is the
// records in memory, the marker, or the numbers they were given.
func TestBatchRefusedByTheLogLeavesNothing(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()
	db, err := OpenDB(dir, smallSegments(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Write([]byte("before"), []byte("the batch")); err != nil {
		t.Fatal(err)
	}

	db.mu.RLock()
	active := db.active.kvs
	db.mu.RUnlock()

	was := active.Size()
	number := active.highestSeq()

	// Every write from here on is refused.
	watcher.inject(dir, 0, 1, 0)

	var b Batch
	for i := 0; i < 5; i++ {
		b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}
	if err := db.WriteBatch(&b); err == nil {
		t.Fatal("a batch the disk refused reported no error")
	}
	watcher.calm()

	if got := active.Size(); got != was {
		t.Errorf("a refused batch left %d bytes behind", got-was)
	}
	if got := active.highestSeq(); got != number {
		t.Errorf("a refused batch spent numbers: %d, want %d", got, number)
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, err := db.Read([]byte(key)); err == nil {
			t.Errorf("%s is readable after the batch was refused", key)
		}
	}
	if got, err := db.Read([]byte("before")); err != nil || string(got) != "the batch" {
		t.Errorf("the record before the batch reads %q, '%v'", got, err)
	}

	// And the store still takes writes, with the numbering where it was.
	if err := db.Write([]byte("after"), []byte("the refusal")); err != nil {
		t.Fatal(err)
	}
	if got := active.highestSeq(); got != number+1 {
		t.Errorf("the write after the refused batch took number %d, want %d", got, number+1)
	}
}

func TestBatchInAFileIsOneWrite(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()
	kvs, err := Open(dir+"/store.kv", Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer kvs.Close()

	watcher.reset()

	var b Batch
	for i := 0; i < 10; i++ {
		b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}
	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}

	// One write for the whole batch, not one per record. A batch spread over
	// eleven writes would be eleven chances for the disk to stop half way, and
	// the marker only makes that survivable, not free.
	if got := watcher.count("write", "store.kv"); got != 1 {
		t.Errorf("a batch of ten records took %d writes, want 1", got)
	}
}

func TestBatchOnAClosedStore(t *testing.T) {
	dir := t.TempDir()

	kvs, err := Open(dir+"/store.kv", Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	if err := kvs.Close(); err != nil {
		t.Fatal(err)
	}

	var b Batch
	b.Write([]byte("key"), []byte("value"))

	if err := kvs.WriteBatch(&b); !errors.Is(err, ErrorClosed) {
		t.Errorf("a batch written to a closed store reported '%v', want %v", err, ErrorClosed)
	}

	db, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteBatch(&b); !errors.Is(err, ErrorClosed) {
		t.Errorf("a batch written to a closed DB reported '%v', want %v", err, ErrorClosed)
	}

	_ = os.Remove(dir)
}

// intact walks every log of a store and fails if any of them holds a batch
// that is not all there. A partial batch is exactly what a follower must never
// be left holding, and it is invisible to a read: the records that did arrive
// answer perfectly well on their own.
//
// The count of batches comes from a raw walk rather than from scan, which hides
// markers on purpose — counting them with the thing that hides them is how the
// first version of this reported that nothing was being tested.
func intact(t *testing.T, db *DB, what string) (batches int) {
	t.Helper()

	db.mu.RLock()
	frozen := append([]*diskSegment(nil), db.frozen...)
	active := db.active
	db.mu.RUnlock()

	for _, seg := range frozen {
		// scanSegment refuses a batch that is not whole, so reaching the end of
		// the log is the assertion.
		var walked int64
		if err := scanSegment(seg.file, seg.bytes, func(pos int64, raw []byte, r Record) bool {
			walked = pos + int64(len(raw))
			return true
		}); err != nil {
			t.Fatalf("%s: log %d does not walk: %v", what, seg.id(), err)
		}
		if walked != seg.bytes {
			t.Fatalf("%s: log %d walks to %d of %d bytes", what, seg.id(), walked, seg.bytes)
		}

		for at := int64(0); at < seg.bytes; {
			record, raw, err := readRecordAt(seg.file, seg.bytes, at)
			if err != nil {
				t.Fatalf("%s: log %d will not read at %d: %v", what, seg.id(), at, err)
			}
			if record.Type == RecordTypeBatch {
				batches++
			}
			at += int64(len(raw))
		}
	}

	active.kvs.RLock()
	err := active.kvs.scan(func(pos, next int64, r Record) bool { return true })
	end := active.kvs.position().Offset
	size := int64(len(active.kvs.Data))
	data := append([]byte(nil), active.kvs.Data...)
	active.kvs.RUnlock()

	if err != nil {
		t.Fatalf("%s: the active log does not walk: %v", what, err)
	}
	if end != size {
		t.Fatalf("%s: the active log walks to %d of %d bytes", what, end, size)
	}

	for at := int64(0); at < int64(len(data)); {
		record, next, err := parseRecordAt(data, at)
		if err != nil {
			t.Fatalf("%s: the active log will not decode at %d: %v", what, at, err)
		}
		if record.Type == RecordTypeBatch {
			batches++
		}
		at = next
	}

	return batches
}

// TestBatchCrossesWhole holds the rule on the wire: a leader cuts its stream at
// the end of a batch and never inside one, so a follower never holds part of
// one however small the pieces it is given are.
func TestBatchCrossesWhole(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(700))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), smallSegments(500))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	// Caught up first, and only then given the batches. A follower that starts
	// from nothing is filled by a snapshot, which carries the live records and
	// no markers at all, and would test none of this.
	if err := leader.Write([]byte("first"), []byte("record")); err != nil {
		t.Fatal(err)
	}
	followDB(t, leader, follower, ReplicaOptions{})

	for round := 0; round < 12; round++ {
		var b Batch
		for i := 0; i < 6; i++ {
			b.Write([]byte(fmt.Sprintf("key-%02d-%d", round, i)), []byte("value"))
		}
		if err := leader.WriteBatch(&b); err != nil {
			t.Fatal(err)
		}
		if err := leader.Write([]byte(fmt.Sprintf("single-%02d", round)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// Pieces far smaller than a batch, so the leader is forced to decide the
	// question on almost every round.
	opts := ReplicaOptions{BatchSize: 64}

	pos := follower.Applied()
	for step := 0; ; step++ {
		if step > 500 {
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
				t.Fatalf("ApplySnapshot: %v", err)
			}
			release()
			pos = at
			continue
		}
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			break
		}

		got, err := follower.Apply(pos, next, &wire, opts)
		if err != nil {
			t.Fatalf("Apply(%+v -> %+v): %v", pos, next, err)
		}
		if got != next {
			t.Fatalf("the follower reached %+v, the leader sent to %+v", got, next)
		}
		pos = next

		// After every piece, not only at the end: a follower stopped here by a
		// connection dying must not be holding half a batch.
		intact(t, follower, fmt.Sprintf("after step %d", step))
	}

	sameStores(t, leader, follower, nil)

	if got := intact(t, follower, "caught up"); got == 0 {
		t.Error("the follower holds no batches at all, so nothing was tested")
	}
}

// TestBatchLargerThanTheWireCrossesAnyway is the rule that keeps a store
// replicable at all: a batch bigger than the pieces the wire is cut into still
// goes, exactly as a record bigger than them does.
func TestBatchLargerThanTheWireCrossesAnyway(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		name := "from the active log"
		if frozen {
			name = "from a frozen log"
		}

		t.Run(name, func(t *testing.T) {
			leader, err := OpenDB(t.TempDir(), smallSegments(1<<20))
			if err != nil {
				t.Fatal(err)
			}
			defer leader.Close()

			follower, err := OpenDB(t.TempDir(), smallSegments(1<<20))
			if err != nil {
				t.Fatal(err)
			}
			defer follower.Close()

			// Something to snapshot, so the follower starts from a position
			// that names a record rather than the start of an empty log.
			if err := leader.Write([]byte("first"), []byte("record")); err != nil {
				t.Fatal(err)
			}
			followDB(t, leader, follower, ReplicaOptions{})

			var b Batch
			value := make([]byte, 512)
			for i := 0; i < 20; i++ {
				b.Write([]byte(fmt.Sprintf("key-%02d", i)), value)
			}
			if err := leader.WriteBatch(&b); err != nil {
				t.Fatal(err)
			}

			if frozen {
				leader.mu.Lock()
				err = leader.rotateLocked()
				leader.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				if err := leader.Write([]byte("after"), []byte("the rotation")); err != nil {
					t.Fatal(err)
				}
			}

			// A wire cut into pieces smaller than one record of the batch.
			followDB(t, leader, follower, ReplicaOptions{BatchSize: 64})

			sameStores(t, leader, follower, nil)
			if got := intact(t, follower, "after a large batch"); got == 0 {
				t.Error("the batch did not cross as a batch")
			}
		})
	}
}

// TestBatchOverAKeyValueStoreWire is the same claim for the single-store half,
// where the follower's log is the leader's log byte for byte.
func TestBatchOverAKeyValueStoreWire(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	for round := 0; round < 8; round++ {
		var b Batch
		for i := 0; i < 5; i++ {
			b.Write([]byte(fmt.Sprintf("key-%d-%d", round, i)), []byte("value"))
		}
		if err := leader.WriteBatch(&b); err != nil {
			t.Fatal(err)
		}
	}

	pos := follower.Position()
	for step := 0; ; step++ {
		if step > 500 {
			t.Fatal("the follower is going round in circles")
		}

		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, ReplicaOptions{BatchSize: 48})
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			break
		}

		if pos, err = follower.Apply(pos, &wire, ReplicaOptions{BatchSize: 48}); err != nil {
			t.Fatalf("Apply: %v", err)
		}

		// The follower's log is the leader's log, so what it holds has to walk
		// from end to end at every step.
		follower.RLock()
		err = follower.scan(func(int64, int64, Record) bool { return true })
		end := follower.position().Offset
		size := int64(len(follower.Data))
		follower.RUnlock()

		if err != nil {
			t.Fatalf("step %d: the follower's log does not walk: %v", step, err)
		}
		if end != size {
			t.Fatalf("step %d: the follower's log walks to %d of %d bytes", step, end, size)
		}
	}

	sameStore(t, leader, follower)
}

// A batch that is torn is one a crash cut short. A batch that is damaged is a
// different thing — the bytes are all there and one of them is wrong — and it
// has to be answered the same way, because a record the marker vouched for that
// no longer checksums makes the batch as untrustworthy as a missing one.

// damageInside flips a bit in the middle record of the first batch in data, and
// reports where the marker that opens it starts.
func damageInside(t *testing.T, data []byte) int64 {
	t.Helper()

	for at := int64(0); at < int64(len(data)); {
		record, next, err := parseRecordAt(data, at)
		if err != nil {
			t.Fatalf("the log will not decode at %d: %v", at, err)
		}
		if record.Type != RecordTypeBatch {
			at = next
			continue
		}

		span, ok := markerSpan(record)
		if !ok {
			t.Fatalf("the marker at %d holds no span", at)
		}

		// The second record of the batch, so there is one either side of the
		// damage and the test can tell "dropped the batch" from "stopped here".
		_, second, err := parseRecordAt(data, next)
		if err != nil {
			t.Fatalf("the first record of the batch will not decode: %v", err)
		}
		if second >= next+span {
			t.Fatal("the batch holds one record; the test needs more")
		}

		inner, third, err := parseRecordAt(data, second)
		if err != nil {
			t.Fatalf("the second record of the batch will not decode: %v", err)
		}
		if len(inner.Value) == 0 {
			t.Fatal("the record to damage holds no value; the test needs one")
		}

		// The last byte of its value, which leaves every length in the record
		// alone: damage in a length field is caught by the framing before any
		// checksum is looked at, and this is about the checksum.
		data[third-1] ^= 0x40
		return at
	}

	t.Fatal("no batch in the log")
	return 0
}

func TestBatchWithADamagedRecordIsDroppedWhole(t *testing.T) {
	kvs := &KeyValueStore{}

	if err := kvs.Write([]byte("before"), []byte("the batch")); err != nil {
		t.Fatal(err)
	}

	var b Batch
	for i := 0; i < 4; i++ {
		b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}
	if err := kvs.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Write([]byte("after"), []byte("the batch")); err != nil {
		t.Fatal(err)
	}

	marker := damageInside(t, kvs.Data)

	// Loaded from those bytes, as a store that had been saved and put back is.
	loaded := &KeyValueStore{Data: append([]byte(nil), kvs.Data...)}

	discarded, err := loaded.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if discarded == 0 {
		t.Fatal("a damaged batch was recovered as if it were sound")
	}
	if got := int64(len(loaded.Data)); got != marker {
		t.Errorf("the log came back %d bytes long, want %d — the marker's offset", got, marker)
	}

	if got, err := loaded.Read([]byte("before")); err != nil || string(got) != "the batch" {
		t.Errorf("the record before the batch reads %q, '%v'", got, err)
	}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, err := loaded.Read([]byte(key)); err == nil {
			t.Errorf("%s survived a batch with a damaged record in it", key)
		}
	}
	// The record after the batch goes too. There is no way past a hole in an
	// append-only log, which is why the damage is answered at the marker.
	if _, err := loaded.Read([]byte("after")); err == nil {
		t.Error("a record beyond the damaged batch survived")
	}
}

func TestBatchDamagedInAFrozenLog(t *testing.T) {
	for _, lying := range []bool{false, true} {
		name := "a damaged record"
		if lying {
			name = "a span that is a lie"
		}

		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			db, err := OpenDB(dir, smallSegments(1<<20))
			if err != nil {
				t.Fatal(err)
			}

			if err := db.Write([]byte("before"), []byte("the batch")); err != nil {
				t.Fatal(err)
			}

			var b Batch
			for i := 0; i < 4; i++ {
				b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
			}
			if err := db.WriteBatch(&b); err != nil {
				t.Fatal(err)
			}

			db.mu.Lock()
			err = db.rotateLocked()
			db.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			path := db.path(1)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			// The hint would let the log be opened without reading it, and this
			// is about what happens when it is read.
			if err := os.Remove(hintPath(path)); err != nil {
				t.Fatal(err)
			}

			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			var marker int64
			if lying {
				marker = markerOf(t, raw)
				_, next, err := parseRecordAt(raw, marker)
				if err != nil {
					t.Fatal(err)
				}
				// A span nobody could hold, checksummed so that the marker
				// itself is beyond reproach. Nothing may try to read it.
				binary.LittleEndian.PutUint64(raw[next-8:next], 1<<60)
				binary.LittleEndian.PutUint32(raw[marker:marker+4], checksumSerialized(raw[marker:next]))
			} else {
				marker = damageInside(t, raw)
			}
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatal(err)
			}

			reopened, err := OpenDB(dir, smallSegments(1<<20))
			if err != nil {
				t.Fatalf("reopening: %v", err)
			}
			defer reopened.Close()

			if got, err := reopened.Read([]byte("before")); err != nil || string(got) != "the batch" {
				t.Errorf("the record before the batch reads %q, '%v'", got, err)
			}
			for i := 0; i < 4; i++ {
				key := fmt.Sprintf("key-%d", i)
				if _, err := reopened.Read([]byte(key)); err == nil {
					t.Errorf("%s survived a batch that could not be trusted", key)
				}
			}

			// And the log on the disk has been cut back to the marker, so the
			// bytes nobody would vouch for are not there to be read again.
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != marker {
				t.Errorf("the log is %d bytes, want %d — the marker's offset", info.Size(), marker)
			}
		})
	}
}

// markerOf is where the first batch in data opens.
func markerOf(t *testing.T, data []byte) int64 {
	t.Helper()

	for at := int64(0); at < int64(len(data)); {
		record, next, err := parseRecordAt(data, at)
		if err != nil {
			t.Fatalf("the log will not decode at %d: %v", at, err)
		}
		if record.Type == RecordTypeBatch {
			return at
		}
		at = next
	}

	t.Fatal("no batch in the log")
	return 0
}

// TestBatchDamagedOnTheWire is the follower's side of the same rule. A leader
// is not a reason to trust the wire, and a batch is checked as one thing: a
// record inside it that will not vouch for itself refuses the whole batch
// rather than the records after it.
func TestBatchDamagedOnTheWire(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	if err := leader.Write([]byte("before"), []byte("the batch")); err != nil {
		t.Fatal(err)
	}
	catchUp(t, leader, follower)

	var b Batch
	for i := 0; i < 4; i++ {
		b.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
	}
	if err := leader.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}

	pos := follower.Position()

	var wire bytes.Buffer
	if _, err := leader.Since(pos, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	sent := wire.Bytes()
	damageInside(t, sent)

	was := len(follower.Data)

	got, err := follower.Apply(pos, bytes.NewReader(sent), ReplicaOptions{})
	if !errors.Is(err, ErrorChecksumMismatch) {
		t.Errorf("a batch with a damaged record in it applied with '%v', want %v", err, ErrorChecksumMismatch)
	}
	if got != pos {
		t.Errorf("the follower moved to %+v, want to stay at %+v", got, pos)
	}
	if len(follower.Data) != was {
		t.Errorf("the follower kept %d bytes of a batch it refused", len(follower.Data)-was)
	}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, err := follower.Read([]byte(key)); err == nil {
			t.Errorf("%s was applied from a batch that was refused", key)
		}
	}
}
