package litekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// smallSegments rotates every few records and never merges on its own, so that
// tests decide when merging happens.
func smallSegments(size int64) DBOptions {
	return DBOptions{Sync: SyncNever, SegmentSize: size, MergeTrigger: 1 << 30}
}

// liveValue reports what the DB says about a key, treating a deleted key and one
// that was never written as the same: a merge drops tombstones, so which of the
// two a caller sees depends on whether one has run.
func liveValue(t *testing.T, db *DB, key string) (string, bool) {
	t.Helper()

	value, err := db.Read([]byte(key))
	switch {
	case err == nil:
		return string(value), true
	case errors.Is(err, ErrorKeyNotFound), errors.Is(err, ErrorKeyDeleted):
		return "", false
	default:
		t.Fatalf("key %q: %v", key, err)
		return "", false
	}
}

func TestDBBasics(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := db.Segments(); got != 1 {
		t.Errorf("a new DB has %d segments, want 1", got)
	}

	for i := 0; i < 100; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("value%03d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if got := db.Segments(); got < 5 {
		t.Errorf("100 records into 200-byte segments made %d of them, want several", got)
	}

	// Every key is findable wherever it landed.
	for i := 0; i < 100; i++ {
		value, ok := liveValue(t, db, fmt.Sprintf("key%03d", i))
		if !ok || value != fmt.Sprintf("value%03d", i) {
			t.Fatalf("key%03d: got '%s' (%v)", i, value, ok)
		}
	}
	if got := db.Len(); got != 100 {
		t.Errorf("Len is %d, want 100", got)
	}

	// A rewrite in a newer segment shadows the old one.
	if err := db.Write([]byte("key000"), []byte("rewritten")); err != nil {
		t.Fatal(err)
	}
	if value, _ := liveValue(t, db, "key000"); value != "rewritten" {
		t.Errorf("key000: got '%s', want 'rewritten'", value)
	}

	// So does a delete.
	if err := db.Delete([]byte("key001")); err != nil {
		t.Fatal(err)
	}
	if _, ok := liveValue(t, db, "key001"); ok {
		t.Error("key001 survived being deleted")
	}
	if _, err := db.Read([]byte("key001")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("expected '%v' before a merge, got '%v'", ErrorKeyDeleted, err)
	}

	// View sees the same thing as Read.
	var viewed string
	if err := db.View([]byte("key000"), func(value []byte) error {
		viewed = string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if viewed != "rewritten" {
		t.Errorf("View saw '%s', want 'rewritten'", viewed)
	}

	// ForEach covers the live keys once each.
	seen := map[string]string{}
	if err := db.ForEach(func(key, value []byte) bool {
		if _, twice := seen[string(key)]; twice {
			t.Errorf("ForEach gave key %q twice", key)
		}
		seen[string(key)] = string(value)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 99 { // 100 written, one deleted
		t.Errorf("ForEach saw %d live keys, want 99", len(seen))
	}
	if seen["key000"] != "rewritten" {
		t.Errorf("ForEach: key000 is '%s', want 'rewritten'", seen["key000"])
	}
}

func TestDBMerge(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(dir, smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Rewrite the same few keys over and over, so most records are superseded.
	for round := 0; round < 40; round++ {
		for _, key := range []string{"a", "b", "c", "d"} {
			if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%02d", key, round))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Delete([]byte("d")); err != nil {
		t.Fatal(err)
	}
	// A key written after its delete has to come back.
	if err := db.Write([]byte("c"), []byte("c-final")); err != nil {
		t.Fatal(err)
	}

	before := db.Segments()
	sizeBefore := dirSize(t, dir)
	if before < 4 {
		t.Fatalf("only %d segments; the test needs several to merge", before)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	if after := db.Segments(); after >= before {
		t.Errorf("merging left %d segments, was %d", after, before)
	}
	if sizeAfter := dirSize(t, dir); sizeAfter >= sizeBefore {
		t.Errorf("merging left %d bytes on disk, was %d", sizeAfter, sizeBefore)
	}

	for key, want := range map[string]string{"a": "a-39", "b": "b-39", "c": "c-final"} {
		if value, ok := liveValue(t, db, key); !ok || value != want {
			t.Errorf("%s: got '%s' (%v), want '%s'", key, value, ok, want)
		}
	}
	if _, ok := liveValue(t, db, "d"); ok {
		t.Error("the deleted key came back after merging")
	}

	// Merging twice is not a problem, and changes nothing.
	if err := db.Merge(); err != nil {
		t.Fatalf("second Merge: %v", err)
	}
	if value, ok := liveValue(t, db, "c"); !ok || value != "c-final" {
		t.Errorf("c after a second merge: got '%s' (%v)", value, ok)
	}
}

// TestDBMergeInterrupted is the reason the merge renames over the oldest log
// and only then removes the rest, oldest first. Whatever a crash leaves behind,
// what is on disk still has to answer the same way.
func TestDBMergeInterrupted(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(150))
	if err != nil {
		t.Fatal(err)
	}

	// A history with enough shape to catch a wrong answer: values that get
	// rewritten, a key deleted part way through, and one written again after
	// its delete.
	want := map[string]string{}
	for round := 0; round < 30; round++ {
		for _, key := range []string{"a", "b", "c"} {
			value := fmt.Sprintf("%s-%02d", key, round)
			if err := db.Write([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			want[key] = value
		}
		if round == 10 {
			if err := db.Delete([]byte("gone")); err != nil {
				t.Fatal(err)
			}
			delete(want, "gone")
		}
		if round == 5 {
			if err := db.Write([]byte("gone"), []byte("written before the delete")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Keep a copy of every log as it stood before the merge.
	before := snapshotDir(t, dir)

	db, err = OpenDB(dir, smallSegments(150))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	after := snapshotDir(t, dir)

	// The logs the merge removed, oldest first.
	var removed []string
	for name := range before {
		if _, kept := after[name]; !kept {
			removed = append(removed, name)
		}
	}
	sort.Strings(removed)
	if len(removed) < 2 {
		t.Fatalf("the merge removed %d logs; the test needs at least two", len(removed))
	}

	// A crash can stop the removals at any point, leaving the merged log plus
	// the logs it had not got to yet. Every one of those states has to read
	// the same as the finished merge.
	for stopped := 0; stopped <= len(removed); stopped++ {
		t.Run(fmt.Sprintf("removed %d of %d", stopped, len(removed)), func(t *testing.T) {
			crashed := t.TempDir()
			for name, content := range after {
				writeInto(t, crashed, name, content)
			}
			// Put back the ones the crash never got to.
			for _, name := range removed[stopped:] {
				writeInto(t, crashed, name, before[name])
			}

			db, err := OpenDB(crashed, smallSegments(150))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			for key, value := range want {
				if got, ok := liveValue(t, db, key); !ok || got != value {
					t.Errorf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
				}
			}
			if _, ok := liveValue(t, db, "gone"); ok {
				t.Error("the deleted key came back")
			}
		})
	}
}

func TestDBReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(300))
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%03d", i%50)
		value := fmt.Sprintf("value%03d", i)
		if err := db.Write([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		want[key] = value
	}
	for _, key := range []string{"key000", "key001"} {
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
		delete(want, key)
	}

	segments := db.Segments()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, smallSegments(300))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Segments(); got != segments {
		t.Errorf("reopened with %d segments, want %d", got, segments)
	}
	for key, value := range want {
		if got, ok := liveValue(t, reopened, key); !ok || got != value {
			t.Errorf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
	for _, key := range []string{"key000", "key001"} {
		if _, ok := liveValue(t, reopened, key); ok {
			t.Errorf("%s came back after reopening", key)
		}
	}

	// And writing carries on into a new active segment.
	if err := reopened.Write([]byte("after"), []byte("reopening")); err != nil {
		t.Fatal(err)
	}
	if value, ok := liveValue(t, reopened, "after"); !ok || value != "reopening" {
		t.Errorf("after: got '%s' (%v)", value, ok)
	}
}

func TestDBOpenIgnoresStrayFiles(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	db.Write([]byte("a"), []byte("1"))
	db.Close()

	// An interrupted merge leaves one of these behind, and something else may
	// keep its own files in the directory.
	writeInto(t, dir, "0000000001.seg.merging", []byte("half written rubbish"))
	writeInto(t, dir, "notes.txt", []byte("nothing to do with us"))

	reopened, err := OpenDB(dir, smallSegments(200))
	if err != nil {
		t.Fatalf("Open with stray files: %v", err)
	}
	defer reopened.Close()

	if value, ok := liveValue(t, reopened, "a"); !ok || value != "1" {
		t.Errorf("a: got '%s' (%v), want '1'", value, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, "0000000001.seg.merging")); !os.IsNotExist(err) {
		t.Error("the half built merge was left behind")
	}
	if _, err := os.Stat(filepath.Join(dir, "notes.txt")); err != nil {
		t.Error("a file that is none of our business was removed")
	}
}

// TestDBMergeDoesNotBlock is the point of merging in the background: reads and
// writes carry on against the old logs while it runs.
func TestDBMergeDoesNotBlock(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 400, MergeTrigger: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers, which keep rotating segments and so keep merges coming.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("key%d-%d", i, n%20)
				if err := db.Write([]byte(key), []byte(fmt.Sprintf("value%d", n))); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(i)
	}

	// Readers of a key that is written once and must never stop being readable,
	// whatever the merges are doing underneath.
	if err := db.Write([]byte("constant"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				value, err := db.Read([]byte("constant"))
				if err != nil || string(value) != "value" {
					t.Errorf("constant: got '%s' (%v)", value, err)
					return
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	if value, ok := liveValue(t, db, "constant"); !ok || value != "value" {
		t.Errorf("constant: got '%s' (%v) at the end", value, ok)
	}
}

func TestDBClosed(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	db.Write([]byte("a"), []byte("1"))

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("closing twice: %v", err)
	}

	if err := db.Write([]byte("b"), []byte("2")); !errors.Is(err, ErrorClosed) {
		t.Errorf("Write: expected '%v', got '%v'", ErrorClosed, err)
	}
	if err := db.Delete([]byte("a")); !errors.Is(err, ErrorClosed) {
		t.Errorf("Delete: expected '%v', got '%v'", ErrorClosed, err)
	}
	// A DB keeps the values of its frozen logs on the disk, so once the files
	// are shut it cannot answer at all. A KeyValueStore, whose records are in
	// memory, still can.
	if _, err := db.Read([]byte("a")); !errors.Is(err, ErrorClosed) {
		t.Errorf("Read after Close: expected '%v', got '%v'", ErrorClosed, err)
	}
	if err := db.Merge(); !errors.Is(err, ErrorClosed) {
		t.Errorf("Merge: expected '%v', got '%v'", ErrorClosed, err)
	}
}

// TestDBModel runs a long random mix against a map that says what the answers
// should be, with segments small enough that rotation and merging happen
// constantly underneath.
func TestDBModel(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 250, MergeTrigger: 3})
	if err != nil {
		t.Fatal(err)
	}

	live := map[string]string{}
	random := rand.New(rand.NewSource(7))

	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	check := func(step string) {
		t.Helper()
		for key, want := range live {
			if got, ok := liveValue(t, db, key); !ok || got != want {
				t.Fatalf("%s: key %q: got '%s' (%v), want '%s'", step, key, got, ok, want)
			}
		}
		for _, key := range keys {
			if _, ok := live[key]; ok {
				continue
			}
			if _, ok := liveValue(t, db, key); ok {
				t.Fatalf("%s: key %q reads as live but should not", step, key)
			}
		}
	}

	for step := 0; step < 2000; step++ {
		key := keys[random.Intn(len(keys))]

		switch n := random.Intn(100); {
		case n < 60:
			value := fmt.Sprintf("value-%d", step)
			if err := db.Write([]byte(key), []byte(value)); err != nil {
				t.Fatalf("step %d: Write: %v", step, err)
			}
			live[key] = value

		case n < 80:
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatalf("step %d: Delete: %v", step, err)
			}
			delete(live, key)

		case n < 90:
			got, ok := liveValue(t, db, key)
			want, isLive := live[key]
			if ok != isLive || (ok && got != want) {
				t.Fatalf("step %d: key %q: got '%s' (%v), want '%s' (%v)", step, key, got, ok, want, isLive)
			}

		case n < 95:
			seen := map[string]string{}
			if err := db.ForEach(func(key, value []byte) bool {
				seen[string(key)] = string(value)
				return true
			}); err != nil {
				t.Fatalf("step %d: ForEach: %v", step, err)
			}
			if len(seen) != len(live) {
				t.Fatalf("step %d: ForEach saw %d keys, want %d", step, len(seen), len(live))
			}
			for key, want := range live {
				if seen[key] != want {
					t.Fatalf("step %d: ForEach: key %q is '%s', want '%s'", step, key, seen[key], want)
				}
			}

		case n < 98:
			if err := db.Merge(); err != nil {
				t.Fatalf("step %d: Merge: %v", step, err)
			}

		default: // close and reopen, which every record has to survive
			if err := db.Close(); err != nil {
				t.Fatalf("step %d: Close: %v", step, err)
			}
			db, err = OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 250, MergeTrigger: 3})
			if err != nil {
				t.Fatalf("step %d: reopen: %v", step, err)
			}
		}

		if step%100 == 0 {
			check(fmt.Sprintf("step %d", step))
		}
	}

	check("final")

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func nanotime() int64 { return time.Now().UnixNano() }

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	var total int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func snapshotDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	files := map[string][]byte{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), segmentSuffix) {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = content
	}
	return files
}

func writeInto(t *testing.T, dir, name string, content []byte) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDBDefaults(t *testing.T) {
	// The zero value: 4 MiB segments, merging once four have piled up, and a
	// sync on every write.
	db, err := OpenDB(t.TempDir(), DBOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// Nothing near 4 MiB, so it is all still one segment.
	if got := db.Segments(); got != 1 {
		t.Errorf("%d segments for 20 small records, want 1", got)
	}
	if value, ok := liveValue(t, db, "key19"); !ok || value != "value" {
		t.Errorf("key19: got '%s' (%v)", value, ok)
	}
	if err := db.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

func TestDBSync(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncEvery, Interval: time.Hour, SegmentSize: 200})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 40; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Syncing a closed DB is a no-op rather than an error.
	if err := db.Sync(); err != nil {
		t.Errorf("Sync after Close: %v", err)
	}
}

func TestDBForEachStopsAndRefusesWhenClosed(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(150))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	seen := 0
	if err := db.ForEach(func(key, value []byte) bool {
		seen++
		return seen < 3
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if seen != 3 {
		t.Errorf("ForEach ran on for %d keys after being told to stop at 3", seen)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A closed DB cannot read: the values it did not have in memory are behind
	// files it has shut.
	if err := db.ForEach(func(key, value []byte) bool { return true }); !errors.Is(err, ErrorClosed) {
		t.Errorf("ForEach on a closed DB: expected '%v', got '%v'", ErrorClosed, err)
	}
	if err := db.View([]byte("key00"), func([]byte) error { return nil }); !errors.Is(err, ErrorClosed) {
		t.Errorf("View on a closed DB: expected '%v', got '%v'", ErrorClosed, err)
	}
	// Len only looks at the indexes, which are still in memory.
	if db.Len() != 30 {
		t.Errorf("a closed DB reports %d keys, want 30", db.Len())
	}
}

func TestDBViewMissingKey(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(150))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("key00")); err != nil {
		t.Fatal(err)
	}

	called := false
	err = db.View([]byte("missing"), func([]byte) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyNotFound, err)
	}

	err = db.View([]byte("key00"), func([]byte) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
	if called {
		t.Error("View called fn for a key it could not find")
	}

	// An error from fn comes back as it is.
	sentinel := errors.New("sentinel")
	if err := db.View([]byte("key01"), func([]byte) error { return sentinel }); err != sentinel {
		t.Errorf("expected the error from fn, got '%v'", err)
	}
}

func TestOpenDBErrors(t *testing.T) {
	dir := t.TempDir()
	notADir := filepath.Join(dir, "file")
	writeInto(t, dir, "file", []byte("in the way"))

	if _, err := OpenDB(notADir, DBOptions{}); err == nil {
		t.Error("opening a DB where a file is in the way should fail")
	}

	// A segment that is not readable as a store.
	bad := t.TempDir()
	writeInto(t, bad, "0000000001.seg", []byte("not records"))
	db, err := OpenDB(bad, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatalf("a damaged segment should recover, not fail: %v", err)
	}
	defer db.Close()
	if got := db.Len(); got != 0 {
		t.Errorf("a segment of rubbish yielded %d keys", got)
	}
}

// TestCompactionStall measures what the segments are for. Compacting a single
// log holds the write lock for the whole rewrite, so a write landing in the
// middle of one waits for the whole store to be copied, and the wait grows with
// the store. A DB merges in the background, so a write waits for nothing but
// the record it is writing, however much is already stored.
func TestCompactionStall(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a few MB")
	}

	value := make([]byte, 128)

	worst := func(records int, write func(i int) error) time.Duration {
		var longest time.Duration
		for i := 0; i < records; i++ {
			start := time.Now()
			if err := write(i); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(start); took > longest {
				longest = took
			}
		}
		return longest
	}

	for _, records := range []int{10_000, 40_000} {
		// Half the writes are rewrites, so compaction has records to drop, and
		// the live set grows with the run: that is what makes compacting the
		// single log cost more the more it holds.
		key := func(i int) []byte { return []byte(fmt.Sprintf("key%06d", i%(records/2))) }

		single, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncNever})
		if err != nil {
			t.Fatal(err)
		}

		singleWorst := worst(records, func(i int) error {
			if err := single.Write(key(i), value); err != nil {
				return err
			}
			// Compact at the same points in both, so the comparison is fair.
			if i%2000 == 1999 {
				return single.Compact()
			}
			return nil
		})
		single.Close()

		db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 256 << 10, MergeTrigger: 2})
		if err != nil {
			t.Fatal(err)
		}

		dbWorst := worst(records, func(i int) error { return db.Write(key(i), value) })
		db.Close()

		t.Logf("%6d records: worst write is %8v with one log and compaction, %8v with segments",
			records, singleWorst.Round(time.Microsecond), dbWorst.Round(time.Microsecond))

		// Merging in the background only keeps out of a write's way if there is
		// a core for it to run on. On one, it is the same work in the same
		// place and the wait is whatever the scheduler decides, so there is
		// nothing to hold it to.
		if runtime.GOMAXPROCS(0) < 2 {
			continue
		}
		if dbWorst > singleWorst {
			t.Errorf("%d records: segments stalled a write for %v, worse than compacting one log at %v",
				records, dbWorst, singleWorst)
		}
	}
}

// TestDBMemory is the point of keeping the frozen logs on the disk: what has to
// fit in memory is the keys, not the values.
func TestDBMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("writes about 16 MB")
	}

	const (
		records   = 16_000
		valueSize = 1024
	)
	value := make([]byte, valueSize)
	key := func(i int) []byte { return []byte(fmt.Sprintf("key%06d", i)) }

	held := func(build func() (func() error, error)) uint64 {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		closer, err := build()
		if err != nil {
			t.Fatal(err)
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		if err := closer(); err != nil {
			t.Fatal(err)
		}
		return after.HeapAlloc - before.HeapAlloc
	}

	// One log: every record stays in memory as well as on the disk.
	single := held(func() (func() error, error) {
		kvs, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncNever})
		if err != nil {
			return nil, err
		}
		for i := 0; i < records; i++ {
			if err := kvs.Write(key(i), value); err != nil {
				return nil, err
			}
		}
		return kvs.Close, nil
	})

	// Segments: only the active one is in memory, plus every index.
	segmented := held(func() (func() error, error) {
		db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 1 << 20, MergeTrigger: 1 << 30})
		if err != nil {
			return nil, err
		}
		for i := 0; i < records; i++ {
			if err := db.Write(key(i), value); err != nil {
				return nil, err
			}
		}
		return db.Close, nil
	})

	stored := int64(records) * (valueSize + 9 + headerSize)
	t.Logf("%d records holding %d MiB: one log keeps %d MiB in memory, segments keep %d MiB",
		records, stored>>20, single>>20, segmented>>20)

	if segmented >= single {
		t.Errorf("segments held %d bytes, no better than one log at %d", segmented, single)
	}
}

// TestDBReadCost is the other side of that trade: a value in a frozen log costs
// a read from the file rather than a look at memory.
func TestDBReadCost(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a few MB")
	}

	const records = 4000
	value := make([]byte, 512)
	key := func(i int) []byte { return []byte(fmt.Sprintf("key%06d", i)) }

	dir := t.TempDir()
	db, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 256 << 10, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < records; i++ {
		if err := db.Write(key(i), value); err != nil {
			t.Fatal(err)
		}
	}

	single, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	for i := 0; i < records; i++ {
		if err := single.Write(key(i), value); err != nil {
			t.Fatal(err)
		}
	}

	time := func(read func(i int) error) float64 {
		start := nanotime()
		for round := 0; round < 5; round++ {
			for i := 0; i < records; i++ {
				if err := read(i); err != nil {
					t.Fatal(err)
				}
			}
		}
		return float64(nanotime()-start) / float64(records*5)
	}

	fromDisk := time(func(i int) error {
		_, err := db.Read(key(i))
		return err
	})
	fromMemory := time(func(i int) error {
		_, err := single.Read(key(i))
		return err
	})

	t.Logf("read of a 512-byte value: %.0f ns from a frozen log on disk, %.0f ns from memory",
		fromDisk, fromMemory)
}

// TestDBTieredKeepsTombstones is the rule that makes a partial merge safe. A
// merge that does not reach the oldest log must carry its tombstones into the
// merged log: an older log left out of it can still hold the value one hides,
// and dropping it would bring a deleted key back.
func TestDBTieredKeepsTombstones(t *testing.T) {
	dir := t.TempDir()

	// Merging is left to the test, so the runs are known.
	db, err := OpenDB(dir, smallSegments(150))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The oldest log holds the value.
	if err := db.Write([]byte("doomed"), []byte("the old value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Write([]byte("filler"), []byte(strings.Repeat("x", 150))); err != nil {
		t.Fatal(err)
	}

	// Newer logs hold the delete, and enough after it to make a run.
	if err := db.Delete([]byte("doomed")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := db.Write([]byte(fmt.Sprintf("other%d", i)), []byte(strings.Repeat("y", 150))); err != nil {
			t.Fatal(err)
		}
	}

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

	// The delete has to have survived, or the old value comes back.
	if _, err := db.Read([]byte("doomed")); !errors.Is(err, ErrorKeyDeleted) {
		value, _ := db.Read([]byte("doomed"))
		t.Errorf("after a partial merge: expected '%v', got '%v' (value '%s')", ErrorKeyDeleted, err, value)
	}

	// And once a merge does reach the oldest log, both can go.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Read([]byte("doomed")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("after a full merge: expected '%v', got '%v'", ErrorKeyNotFound, err)
	}
}

// TestDBTiers checks that merging only combines logs of a size, so the store
// settles at a few logs rather than rewriting itself into one every time.
func TestDBTiers(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 32 << 10, MergeTrigger: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value := make([]byte, 256)
	for i := 0; i < 4000; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%06d", i)), value); err != nil {
			t.Fatal(err)
		}
	}

	// Everything the background merges were going to do.
	for {
		db.mergeMu.Lock()
		db.mu.RLock()
		victims, drop, ok := db.pickMerge()
		db.mu.RUnlock()
		if !ok {
			db.mergeMu.Unlock()
			break
		}
		err := db.mergeLocked(victims, drop)
		db.mergeMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}

	db.mu.RLock()
	var sizes []int64
	tiers := map[int]int{}
	for _, seg := range db.frozen {
		sizes = append(sizes, seg.bytes)
		tiers[sizeTier(seg.bytes, 32<<10)]++
	}
	db.mu.RUnlock()

	t.Logf("%d frozen logs of sizes %v, in %d size classes", len(sizes), sizes, len(tiers))

	// With the trigger at two, no size class may hold two logs once merging has
	// settled: they would have been merged.
	for tier, count := range tiers {
		if count >= 2 {
			t.Errorf("size class %d still holds %d logs", tier, count)
		}
	}
	// And the whole store is a handful of logs, not one per rotation.
	if len(sizes) > 6 {
		t.Errorf("%d frozen logs is more than a tiered store should settle at", len(sizes))
	}
}

// appendV0 encodes a record in the original layout: a 13-byte header with the
// type where the version now sits, and no timestamp. It is how a store written
// before either existed looks on disk.
func appendV0(dst []byte, recordType RecordType, key, value []byte) []byte {
	start := len(dst)

	var header [headerSizeV0]byte
	header[4] = byte(recordType)
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(value)))

	dst = append(dst, header[:]...)
	dst = append(dst, key...)
	dst = append(dst, value...)

	// The checksum covers everything after itself, whatever the layout.
	binary.LittleEndian.PutUint32(dst[start:start+4], checksumSerialized(dst[start:]))
	return dst
}

// TestDBReadsTheOldFormat runs old records through the half of the library that
// never holds them in memory: indexed off the disk, read back a record at a
// time, hinted, and merged together with records in the current layout.
func TestDBReadsTheOldFormat(t *testing.T) {
	dir := t.TempDir()

	// A segment as an older version of this library would have left it.
	var old []byte
	old = appendV0(old, RecordTypeNormal, []byte("alpha"), []byte("from the old format"))
	old = appendV0(old, RecordTypeNormal, []byte("beta"), []byte("also old"))
	old = appendV0(old, RecordTypeNormal, []byte("doomed"), []byte("about to go"))
	old = appendV0(old, RecordTypeDeleted, []byte("doomed"), nil)
	old = appendV0(old, RecordTypeNormal, []byte("alpha"), []byte("rewritten, still old"))
	writeInto(t, dir, "0000000001"+segmentSuffix, old)

	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatalf("OpenDB over an old segment: %v", err)
	}

	// Indexed by reading the file, since there is no hint for it yet.
	for key, want := range map[string]string{"alpha": "rewritten, still old", "beta": "also old"} {
		if got, ok := liveValue(t, db, key); !ok || got != want {
			t.Errorf("%s: got '%s' (%v), want '%s'", key, got, ok, want)
		}
	}
	if _, ok := liveValue(t, db, "doomed"); ok {
		t.Error("a key deleted in the old format read as live")
	}

	// Old records have no timestamp and do not pretend otherwise.
	db.mu.RLock()
	frozenOrActive := db.searchOrder()
	db.mu.RUnlock()
	for _, seg := range frozenOrActive {
		seg.eachKey(func(key string, pos int64) bool {
			record, _, err := seg.recordAt(pos)
			if err != nil {
				t.Errorf("record for %q: %v", key, err)
				return false
			}
			if record.Version == recordV0 && !record.Written().IsZero() {
				t.Errorf("old record for %q claims a time", key)
			}
			return true
		})
	}

	// New records land beside the old ones, in the current layout.
	for i := 0; i < 40; i++ {
		if err := db.Write([]byte(fmt.Sprintf("new%02d", i)), []byte("in the current format")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Write([]byte("alpha"), []byte("rewritten in the new format")); err != nil {
		t.Fatal(err)
	}

	// A merge has to carry both layouts across.
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	want := map[string]string{
		"alpha": "rewritten in the new format",
		"beta":  "also old",
		"new00": "in the current format",
		"new39": "in the current format",
	}
	for key, value := range want {
		if got, ok := liveValue(t, db, key); !ok || got != value {
			t.Errorf("after merging, %s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
	if _, ok := liveValue(t, db, "doomed"); ok {
		t.Error("the deleted key came back through the merge")
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// And again from the hints written along the way.
	reopened, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	for key, value := range want {
		if got, ok := liveValue(t, reopened, key); !ok || got != value {
			t.Errorf("after reopening, %s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

// TestScanSegmentMixedLayouts walks a file holding both layouts, which is what
// a merge and a rebuild each have to do.
func TestScanSegmentMixedLayouts(t *testing.T) {
	var data []byte
	data = appendV0(data, RecordTypeNormal, []byte("old1"), []byte("a"))

	current := &KeyValueStore{}
	current.Write([]byte("new1"), []byte(strings.Repeat("b", 5000))) // past the scan buffer
	data = append(data, current.Data...)

	data = appendV0(data, RecordTypeNormal, []byte("old2"), []byte("c"))
	data = appendV0(data, RecordTypeDeleted, []byte("old1"), nil)

	path := filepath.Join(t.TempDir(), "mixed"+segmentSuffix)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var versions []uint8
	var keys []string
	err = scanSegment(file, int64(len(data)), func(pos int64, raw []byte, r Record) bool {
		if r.Crc != checksumSerialized(raw) {
			t.Errorf("record at %d fails its checksum", pos)
		}
		versions = append(versions, r.Version)
		keys = append(keys, string(r.Key))
		return true
	})
	if err != nil {
		t.Fatalf("scanSegment: %v", err)
	}

	if strings.Join(keys, ",") != "old1,new1,old2,old1" {
		t.Errorf("scanned %v, want [old1 new1 old2 old1]", keys)
	}
	if versions[0] != recordV0 || versions[1] != recordV1 || versions[2] != recordV0 {
		t.Errorf("scanned versions %v", versions)
	}

	// And a record fetched by offset agrees with the walk.
	index, good, err := indexSegment(file, int64(len(data)))
	if err != nil {
		t.Fatalf("indexSegment: %v", err)
	}
	if good != int64(len(data)) {
		t.Errorf("indexed %d of %d bytes", good, len(data))
	}
	record, _, err := readRecordAt(file, int64(len(data)), index["new1"])
	if err != nil {
		t.Fatalf("readRecordAt: %v", err)
	}
	if len(record.Value) != 5000 || record.Version != recordV1 {
		t.Errorf("new1 read back as version %d with %d bytes", record.Version, len(record.Value))
	}
}

// TestDBRotationFailureLeavesStoreUsable checks that a rotation that cannot
// finish does not take the store with it. Freezing hands the records from the
// store that was writing them to a handle that reads them, and if it let go of
// the first before it had the second, a failure there would leave the active
// log closed and every later write refused.
func TestDBRotationFailureLeavesStoreUsable(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Write([]byte("before"), []byte("the trouble")); err != nil {
		t.Fatal(err)
	}

	// Take away the permission freezing needs to open the log for reading.
	db.mu.RLock()
	active := filepath.Join(dir, fmt.Sprintf("%010d%s", db.active.segID, segmentSuffix))
	db.mu.RUnlock()

	if err := os.Chmod(active, 0o000); err != nil {
		t.Skipf("cannot make the log unreadable: %v", err)
	}
	defer os.Chmod(active, 0o644)

	// Enough to want a rotation, which cannot be finished.
	for i := 0; i < 20; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte(strings.Repeat("x", 40))); err != nil {
			t.Fatalf("the write itself failed: %v", err)
		}
	}

	// The records went in regardless, and the store still takes writes.
	if value, ok := liveValue(t, db, "before"); !ok || value != "the trouble" {
		t.Errorf("before: got '%s' (%v) after a failed rotation", value, ok)
	}
	if value, ok := liveValue(t, db, "key19"); !ok || value != strings.Repeat("x", 40) {
		t.Errorf("key19: got '%s' (%v) after a failed rotation", value, ok)
	}
	if err := db.Write([]byte("after"), []byte("the trouble")); err != nil {
		t.Errorf("the store stopped taking writes after a failed rotation: %v", err)
	}

	// But the failure is not swallowed: Sync says what went wrong.
	if err := db.Sync(); err == nil {
		t.Error("Sync did not report the rotation that failed")
	}
	// And having been reported once, it is not repeated.
	if err := db.Sync(); err != nil {
		t.Errorf("Sync reported the same rotation failure twice: %v", err)
	}
}

// TestHintCoversAnOldFormatLog checks that a hint is accepted for a log whose
// last record is in the older, shorter layout. The bound on where a record may
// start is the smallest a record can be, and using the larger one quietly
// rejected every such hint.
func TestHintCoversAnOldFormatLog(t *testing.T) {
	dir := t.TempDir()

	// A log ending in an old-format record, which is nine bytes shorter than
	// the current layout's header.
	var old []byte
	old = appendV0(old, RecordTypeNormal, []byte("alpha"), []byte("one"))
	old = appendV0(old, RecordTypeNormal, []byte("omega"), []byte("t"))
	writeInto(t, dir, "0000000001"+segmentSuffix, old)

	// A second log, so the first is frozen rather than the active one. Only a
	// frozen log gets a hint; the active one is read into memory anyway.
	var newer []byte
	newer = appendV0(newer, RecordTypeNormal, []byte("later"), []byte("record"))
	writeInto(t, dir, "0000000002"+segmentSuffix, newer)

	// Opening reads it the long way and writes a hint.
	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	hints, _ := hintFiles(t, dir)
	if len(hints) == 0 {
		t.Fatal("no hint was written for the old-format log")
	}

	// Damage a record so that reading the log the long way would stop early.
	// If the hint is taken, the store opens whole regardless.
	file, err := os.OpenFile(filepath.Join(dir, "0000000001"+segmentSuffix), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'!'}, headerSizeV0+1); err != nil {
		t.Fatal(err)
	}
	file.Close()

	reopened, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Len(); got != 3 {
		t.Errorf("%d keys after reopening, want 3: the hint was refused", got)
	}
	if value, ok := liveValue(t, reopened, "omega"); !ok || value != "t" {
		t.Errorf("omega: got '%s' (%v), want 't'", value, ok)
	}
}

func TestDBSyncCoversEveryLog(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 200, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 60; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte(strings.Repeat("v", 30))); err != nil {
			t.Fatal(err)
		}
	}
	if db.Segments() < 3 {
		t.Fatalf("%d segments; the test wants several frozen ones", db.Segments())
	}

	// Under SyncNever nothing has been synced yet, so this has to reach the
	// frozen logs as well as the active one.
	if err := db.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
