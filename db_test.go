package litekv

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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

// readable reports what the DB says about a key, treating a deleted key and one
// that was never written as the same: a merge drops tombstones, so which of the
// two a caller sees depends on whether one has run.
func readable(t *testing.T, db *DB, key string) (string, bool) {
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
		value, ok := readable(t, db, fmt.Sprintf("key%03d", i))
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
	if value, _ := readable(t, db, "key000"); value != "rewritten" {
		t.Errorf("key000: got '%s', want 'rewritten'", value)
	}

	// So does a delete.
	if err := db.Delete([]byte("key001")); err != nil {
		t.Fatal(err)
	}
	if _, ok := readable(t, db, "key001"); ok {
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
		if value, ok := readable(t, db, key); !ok || value != want {
			t.Errorf("%s: got '%s' (%v), want '%s'", key, value, ok, want)
		}
	}
	if _, ok := readable(t, db, "d"); ok {
		t.Error("the deleted key came back after merging")
	}

	// Merging twice is not a problem, and changes nothing.
	if err := db.Merge(); err != nil {
		t.Fatalf("second Merge: %v", err)
	}
	if value, ok := readable(t, db, "c"); !ok || value != "c-final" {
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
				if got, ok := readable(t, db, key); !ok || got != value {
					t.Errorf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
				}
			}
			if _, ok := readable(t, db, "gone"); ok {
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
		if got, ok := readable(t, reopened, key); !ok || got != value {
			t.Errorf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
	for _, key := range []string{"key000", "key001"} {
		if _, ok := readable(t, reopened, key); ok {
			t.Errorf("%s came back after reopening", key)
		}
	}

	// And writing carries on into a new active segment.
	if err := reopened.Write([]byte("after"), []byte("reopening")); err != nil {
		t.Fatal(err)
	}
	if value, ok := readable(t, reopened, "after"); !ok || value != "reopening" {
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

	if value, ok := readable(t, reopened, "a"); !ok || value != "1" {
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

	if value, ok := readable(t, db, "constant"); !ok || value != "value" {
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
	// Reads keep working, as they do on a closed store.
	if value, err := db.Read([]byte("a")); err != nil || string(value) != "1" {
		t.Errorf("Read after Close: got '%s' (%v), want '1'", value, err)
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
			if got, ok := readable(t, db, key); !ok || got != want {
				t.Fatalf("%s: key %q: got '%s' (%v), want '%s'", step, key, got, ok, want)
			}
		}
		for _, key := range keys {
			if _, ok := live[key]; ok {
				continue
			}
			if _, ok := readable(t, db, key); ok {
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
			got, ok := readable(t, db, key)
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
	if value, ok := readable(t, db, "key19"); !ok || value != "value" {
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
	// Reading a closed DB still works; only writing is refused.
	live := 0
	if err := db.ForEach(func(key, value []byte) bool { live++; return true }); err != nil {
		t.Errorf("ForEach on a closed DB: %v", err)
	}
	if live != 30 {
		t.Errorf("ForEach on a closed DB saw %d keys, want 30", live)
	}
	if err := db.View([]byte("key00"), func([]byte) error { return nil }); err != nil {
		t.Errorf("View on a closed DB: %v", err)
	}
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

		if dbWorst > singleWorst {
			t.Errorf("%d records: segments stalled a write for %v, worse than compacting one log at %v",
				records, dbWorst, singleWorst)
		}
	}
}
