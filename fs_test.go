package litekv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// watchedDisk is a real filesystem with a notebook. It records every open,
// write, sync, truncate and close in the order they happen, so that a test can
// check the orderings this package promises rather than only their results, and
// it can be told to fail an operation to see what happens then.
type watchedDisk struct {
	mu   sync.Mutex
	ops  []diskOp
	fail map[string]error // operation "sync:0000000001.seg" to the error to give

	// writeLimit stops a file taking more than so many bytes, which is what a
	// disk filling up part way through a record looks like from here. The write
	// that crosses the line takes what fits and reports the rest as lost.
	writeLimit map[string]int64
	written    map[string]int64

	// readsAllowed lets a file be read so many times before it starts
	// refusing, which is how to fail the second read of a record rather than
	// the first.
	readsAllowed map[string]int
	reads        map[string]int
}

type diskOp struct {
	what string // open, write, sync, truncate, close
	name string // the file it happened to, without the directory
}

func (w *watchedDisk) record(what, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.ops = append(w.ops, diskOp{what: what, name: filepath.Base(name)})
	return w.fail[what+":"+filepath.Base(name)]
}

// install puts the watcher in place for the duration of a test.
func (w *watchedDisk) install(t *testing.T) {
	t.Helper()

	if w.fail == nil {
		w.fail = map[string]error{}
	}
	if w.writeLimit == nil {
		w.writeLimit = map[string]int64{}
	}
	if w.readsAllowed == nil {
		w.readsAllowed = map[string]int{}
	}
	w.written = map[string]int64{}
	w.reads = map[string]int{}

	previous := disk
	disk = w
	t.Cleanup(func() { disk = previous })
}

// The watcher is a real disk that writes down what it is asked, and refuses
// what the test told it to refuse.

func (w *watchedDisk) Open(name string, flag int, perm os.FileMode) (diskFile, error) {
	if err := w.record("open", name); err != nil {
		return nil, err
	}
	file, err := osDisk{}.Open(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &watchedFile{inner: file, disk: w, name: name}, nil
}

func (w *watchedDisk) Remove(name string) error {
	if err := w.record("remove", name); err != nil {
		return err
	}
	return osDisk{}.Remove(name)
}

func (w *watchedDisk) Rename(from, to string) error {
	if err := w.record("rename", to); err != nil {
		return err
	}
	return osDisk{}.Rename(from, to)
}

func (w *watchedDisk) ReadDir(name string) ([]os.DirEntry, error) {
	if err := w.record("readdir", name); err != nil {
		return nil, err
	}
	return osDisk{}.ReadDir(name)
}

func (w *watchedDisk) ReadFile(name string) ([]byte, error) {
	if err := w.record("readfile", name); err != nil {
		return nil, err
	}
	return osDisk{}.ReadFile(name)
}

func (w *watchedDisk) MkdirAll(name string, perm os.FileMode) error {
	if err := w.record("mkdirall", name); err != nil {
		return err
	}
	return osDisk{}.MkdirAll(name, perm)
}

// allowWrite reports how many of these bytes the file may take before it has
// had all it is going to.
func (w *watchedDisk) allowWrite(name string, n int) (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	limit, capped := w.writeLimit[filepath.Base(name)]
	if !capped {
		return n, true
	}

	already := w.written[filepath.Base(name)]
	room := limit - already
	if room < 0 {
		room = 0
	}
	if int64(n) <= room {
		w.written[filepath.Base(name)] = already + int64(n)
		return n, true
	}

	w.written[filepath.Base(name)] = limit
	return int(room), false
}

// allowRead reports whether the file has any reads left in it.
func (w *watchedDisk) allowRead(name string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	allowed, capped := w.readsAllowed[filepath.Base(name)]
	if !capped {
		return true
	}

	w.reads[filepath.Base(name)]++
	return w.reads[filepath.Base(name)] <= allowed
}

// since returns what happened to the files matching suffix, in order.
func (w *watchedDisk) since(suffix string) []diskOp {
	w.mu.Lock()
	defer w.mu.Unlock()

	var found []diskOp
	for _, op := range w.ops {
		if strings.HasSuffix(op.name, suffix) {
			found = append(found, op)
		}
	}
	return found
}

// count returns how many times what happened to name.
func (w *watchedDisk) count(what, name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := 0
	for _, op := range w.ops {
		if op.what == what && op.name == name {
			n++
		}
	}
	return n
}

// order returns the sequence of operations as "what:name" strings.
func (w *watchedDisk) order() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	var out []string
	for _, op := range w.ops {
		out = append(out, op.what+":"+op.name)
	}
	return out
}

func (w *watchedDisk) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.ops = nil
}

type watchedFile struct {
	inner diskFile
	disk  *watchedDisk
	name  string
}

func (f *watchedFile) ReadAt(p []byte, off int64) (int, error) {
	if !f.disk.allowRead(f.name) {
		return 0, errDiskFailed
	}
	return f.inner.ReadAt(p, off)
}
func (f *watchedFile) Stat() (os.FileInfo, error) { return f.inner.Stat() }

func (f *watchedFile) Sync() error {
	if err := f.disk.record("sync", f.name); err != nil {
		return err
	}
	return f.inner.Sync()
}

func (f *watchedFile) Close() error {
	if err := f.disk.record("close", f.name); err != nil {
		return err
	}
	return f.inner.Close()
}

func (f *watchedFile) Truncate(size int64) error {
	if err := f.disk.record("truncate", f.name); err != nil {
		return err
	}
	return f.inner.Truncate(size)
}

func (f *watchedFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.disk.record("write", f.name); err != nil {
		return 0, err
	}

	allowed, whole := f.disk.allowWrite(f.name, len(p))
	n, err := f.inner.WriteAt(p[:allowed], off)
	if err == nil && !whole {
		return n, errDiskFull
	}
	return n, err
}

func (f *watchedFile) Write(p []byte) (int, error) {
	if err := f.disk.record("write", f.name); err != nil {
		return 0, err
	}

	allowed, whole := f.disk.allowWrite(f.name, len(p))
	n, err := f.inner.Write(p[:allowed])
	if err == nil && !whole {
		return n, errDiskFull
	}
	return n, err
}

// errDiskFull is what a disk that has run out says.
var errDiskFull = errors.New("no space left on device")

// errDiskFailed is a read that could not be served, which is not the same as a
// file that ends.
var errDiskFailed = errors.New("input/output error")

// TestSyncPolicyReachesTheDisk checks that each policy syncs when it says it
// does, which is the whole of what a policy is and is otherwise invisible.
func TestSyncPolicyReachesTheDisk(t *testing.T) {
	tests := []struct {
		name   string
		opts   Options
		writes int
		want   int
	}{
		{"always syncs every write", Options{Sync: SyncAlways}, 5, 5},
		{"never syncs on its own", Options{Sync: SyncNever}, 5, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			watcher := &watchedDisk{}
			watcher.install(t)

			path := filepath.Join(t.TempDir(), "kv")
			kvs, err := Open(path, test.opts)
			if err != nil {
				t.Fatal(err)
			}
			watcher.reset() // opening syncs nothing we are asking about

			for i := 0; i < test.writes; i++ {
				if err := kvs.Write([]byte{byte(i)}, []byte("value")); err != nil {
					t.Fatal(err)
				}
			}

			if got := watcher.count("sync", "kv"); got != test.want {
				t.Errorf("%d syncs for %d writes, want %d", got, test.writes, test.want)
			}

			// And Close syncs whatever the policy left.
			if err := kvs.Close(); err != nil {
				t.Fatal(err)
			}
			if got := watcher.count("sync", "kv"); got != test.want+1 {
				t.Errorf("%d syncs after Close, want %d", got, test.want+1)
			}
		})
	}

	t.Run("every syncs on its timer", func(t *testing.T) {
		watcher := &watchedDisk{}
		watcher.install(t)

		kvs, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncEvery, Interval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		defer kvs.Close()

		if err := kvs.Write([]byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		watcher.reset()

		deadline := time.Now().Add(2 * time.Second)
		for watcher.count("sync", "kv") == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if watcher.count("sync", "kv") == 0 {
			t.Error("the timer never reached the disk")
		}
	})
}

// TestDBSyncReachesEveryLog is the one that could not be observed before: a
// frozen log is never written again, but under SyncNever it was never synced
// either, and a sync means all of it.
func TestDBSyncReachesEveryLog(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 200, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 60; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte(strings.Repeat("v", 30))); err != nil {
			t.Fatal(err)
		}
	}

	segments := db.Segments()
	if segments < 3 {
		t.Fatalf("%d segments; the test wants several", segments)
	}

	watcher.reset()
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	synced := map[string]bool{}
	for _, op := range watcher.since(segmentSuffix) {
		if op.what == "sync" {
			synced[op.name] = true
		}
	}
	if len(synced) != segments {
		t.Errorf("Sync reached %d logs of %d: %v", len(synced), segments, synced)
	}

	// Closing syncs every log too, whether or not Sync was called first.
	watcher.reset()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	closedWithoutSync := []string{}
	syncedBeforeClose := map[string]bool{}
	for _, op := range watcher.since(segmentSuffix) {
		switch op.what {
		case "sync":
			syncedBeforeClose[op.name] = true
		case "close":
			if !syncedBeforeClose[op.name] {
				closedWithoutSync = append(closedWithoutSync, op.name)
			}
		}
	}
	if len(closedWithoutSync) != 0 {
		t.Errorf("closed without syncing first: %v", closedWithoutSync)
	}
}

// TestMergeOrdersItsWrites checks the orderings a merge depends on for a crash
// to be harmless: the new log is synced before it is renamed into place, and
// the hint for the log being replaced is gone before the replacement happens.
func TestMergeOrdersItsWrites(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()
	db, err := OpenDB(dir, smallSegments(300))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for round := 0; round < 30; round++ {
		for _, key := range []string{"a", "b", "c"} {
			if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%02d", key, round))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if db.Segments() < 3 {
		t.Fatalf("%d segments; the test wants several", db.Segments())
	}

	watcher.reset()
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	// The merge builds one file and syncs it before anything is renamed.
	var wroteMerge, syncedMerge bool
	for _, op := range watcher.order() {
		switch {
		case strings.HasPrefix(op, "write:") && strings.HasSuffix(op, mergeSuffix):
			wroteMerge = true
		case strings.HasPrefix(op, "sync:") && strings.HasSuffix(op, mergeSuffix):
			if !wroteMerge {
				t.Error("the merged log was synced before it was written")
			}
			syncedMerge = true
		}
	}
	if !wroteMerge || !syncedMerge {
		t.Errorf("the merge did not write and sync a file of its own: %v", watcher.order())
	}
}

// TestSyncFailureIsReported checks that a disk refusing to sync is not
// swallowed on the way out.
func TestSyncFailureIsReported(t *testing.T) {
	broken := errors.New("the disk said no")

	watcher := &watchedDisk{fail: map[string]error{"sync:kv": broken}}
	watcher.install(t)

	kvs, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	if err := kvs.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("the write itself should not have failed: %v", err)
	}
	if err := kvs.Sync(); !errors.Is(err, broken) {
		t.Errorf("Sync: expected '%v', got '%v'", broken, err)
	}
	if err := kvs.Close(); !errors.Is(err, broken) {
		t.Errorf("Close: expected '%v', got '%v'", broken, err)
	}
}

// TestWriteFailureUnderSyncAlways checks that a policy which cannot keep its
// promise says so, rather than reporting a write as stored when it is not
// durable.
func TestWriteFailureUnderSyncAlways(t *testing.T) {
	broken := errors.New("the disk said no")

	watcher := &watchedDisk{fail: map[string]error{"sync:kv": broken}}
	watcher.install(t)

	kvs, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer kvs.Close()

	if err := kvs.Write([]byte("k"), []byte("v")); !errors.Is(err, broken) {
		t.Errorf("under SyncAlways a write that cannot be synced should fail: got '%v'", err)
	}
}

// filledDB writes enough to freeze several logs, and returns what it stored.
func filledDB(t *testing.T, dir string, opts DBOptions) (*DB, map[string]string) {
	t.Helper()

	db, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{}
	for round := 0; round < 30; round++ {
		for _, key := range []string{"a", "b", "c"} {
			value := fmt.Sprintf("%s-%02d", key, round)
			if err := db.Write([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			want[key] = value
		}
	}

	if db.Segments() < 3 {
		t.Fatalf("%d segments; the test wants several", db.Segments())
	}
	return db, want
}

func TestOpenDBReportsDiskFailures(t *testing.T) {
	broken := errors.New("the disk said no")

	tests := []struct {
		name string
		fail string
	}{
		{"cannot make the directory", "mkdirall"},
		{"cannot list the directory", "readdir"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")

			watcher := &watchedDisk{fail: map[string]error{test.fail + ":" + filepath.Base(dir): broken}}
			watcher.install(t)

			if _, err := OpenDB(dir, DBOptions{}); !errors.Is(err, broken) {
				t.Errorf("expected '%v', got '%v'", broken, err)
			}
		})
	}
}

// TestMergeRenameFailureLeavesTheStoreAlone checks that a merge which cannot
// put its result into place changes nothing: the logs it was going to replace
// are still there and still answer.
func TestMergeRenameFailureLeavesTheStoreAlone(t *testing.T) {
	broken := errors.New("the disk said no")

	dir := t.TempDir()
	watcher := &watchedDisk{}
	watcher.install(t)

	db, want := filledDB(t, dir, smallSegments(300))
	defer db.Close()

	segmentsBefore := db.Segments()

	// The merged log is renamed over the oldest of the logs it replaces.
	db.mu.RLock()
	oldest := filepath.Base(db.path(db.frozen[len(db.frozen)-1].id()))
	db.mu.RUnlock()

	watcher.mu.Lock()
	watcher.fail["rename:"+oldest] = broken
	watcher.mu.Unlock()

	if err := db.Merge(); !errors.Is(err, broken) {
		t.Errorf("Merge: expected '%v', got '%v'", broken, err)
	}

	if got := db.Segments(); got != segmentsBefore {
		t.Errorf("%d segments after a failed merge, was %d", got, segmentsBefore)
	}
	for key, value := range want {
		if got, ok := liveValue(t, db, key); !ok || got != value {
			t.Errorf("%s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}

	// The half-built log did not survive to confuse the next open.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), mergeSuffix) {
			t.Errorf("a failed merge left %q behind", entry.Name())
		}
	}

	// And once the disk recovers, merging works.
	watcher.mu.Lock()
	delete(watcher.fail, "rename:"+oldest)
	watcher.mu.Unlock()

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge after the disk recovered: %v", err)
	}
	for key, value := range want {
		if got, ok := liveValue(t, db, key); !ok || got != value {
			t.Errorf("after merging, %s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

// TestMergeRemoveFailureIsTheCrashCase checks the state a merge leaves when it
// cannot remove the logs it has replaced. That is the same state a crash
// between the removals leaves, which the ordering is designed to survive: the
// store answers the same, and so does a store opened from what is on disk.
func TestMergeRemoveFailureIsTheCrashCase(t *testing.T) {
	dir := t.TempDir()
	watcher := &watchedDisk{}
	watcher.install(t)

	db, want := filledDB(t, dir, smallSegments(300))

	// Refuse to remove any log at all.
	db.mu.RLock()
	for _, seg := range db.frozen {
		watcher.fail["remove:"+filepath.Base(db.path(seg.id()))] = errors.New("cannot remove")
	}
	db.mu.RUnlock()

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// The merge itself succeeded, so the store answers from the merged log.
	for key, value := range want {
		if got, ok := liveValue(t, db, key); !ok || got != value {
			t.Errorf("%s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// And the leftovers on disk read the same, which is the whole point of
	// renaming over the oldest log and removing the rest in order.
	watcher.mu.Lock()
	watcher.fail = map[string]error{}
	watcher.mu.Unlock()

	reopened, err := OpenDB(dir, smallSegments(300))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	for key, value := range want {
		if got, ok := liveValue(t, reopened, key); !ok || got != value {
			t.Errorf("after reopening, %s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

// TestRewriteRenameFailureKeepsTheOldFile is the same promise for a single
// store: a rewrite that cannot land leaves what was there.
func TestRewriteRenameFailureKeepsTheOldFile(t *testing.T) {
	broken := errors.New("the disk said no")

	path := filepath.Join(t.TempDir(), "kv")
	watcher := &watchedDisk{fail: map[string]error{"rename:kv": broken}}
	watcher.install(t)

	kvs, err := Open(path, Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer kvs.Close()

	for i := 0; i < 20; i++ {
		if err := kvs.Write([]byte("k"), []byte(fmt.Sprintf("value%02d", i))); err != nil {
			t.Fatal(err)
		}
	}

	if err := kvs.Compact(); !errors.Is(err, broken) {
		t.Errorf("Compact: expected '%v', got '%v'", broken, err)
	}

	// The store still answers, and the file it had is still there.
	if value, err := kvs.Read([]byte("k")); err != nil || string(value) != "value19" {
		t.Errorf("k: got '%s' (%v), want 'value19'", value, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the original file is gone: %v", err)
	}
}

// TestHintFailuresAreHarmless checks that a hint which cannot be written or
// read costs nothing but the time it would have saved.
func TestHintFailuresAreHarmless(t *testing.T) {
	for _, failing := range []string{"write", "rename", "readfile"} {
		t.Run("cannot "+failing+" a hint", func(t *testing.T) {
			dir := t.TempDir()
			watcher := &watchedDisk{}
			watcher.install(t)

			db, want := filledDB(t, dir, smallSegments(300))

			// Refuse the hints from here on, whichever way they are touched.
			watcher.mu.Lock()
			db.mu.RLock()
			for _, seg := range db.frozen {
				name := filepath.Base(hintPath(db.path(seg.id())))
				watcher.fail[failing+":"+name] = errors.New("no hints for you")
				watcher.fail[failing+":"+name+mergeSuffix] = errors.New("no hints for you")
			}
			db.mu.RUnlock()
			watcher.mu.Unlock()

			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			// Opening falls back to reading the logs, and is right either way.
			reopened, err := OpenDB(dir, smallSegments(300))
			if err != nil {
				t.Fatalf("Open with unusable hints: %v", err)
			}
			defer reopened.Close()

			for key, value := range want {
				if got, ok := liveValue(t, reopened, key); !ok || got != value {
					t.Errorf("%s: got '%s' (%v), want '%s'", key, got, ok, value)
				}
			}
		})
	}
}

// bigDB fills a store with enough data that the merged log and its hint both
// run past the buffer they are written through, so a write can fail part way
// rather than only at the flush.
func bigDB(t *testing.T, dir string) (*DB, map[string]string) {
	t.Helper()

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 64 << 10, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}

	value := strings.Repeat("v", 200)
	want := map[string]string{}
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("key%06d", i)
		if err := db.Write([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		want[key] = value
	}

	if db.Segments() < 4 {
		t.Fatalf("%d segments; the test wants several", db.Segments())
	}
	return db, want
}

// TestMergePartialWrite is a disk that fills up in the middle of a merge, so
// the write fails after some of the records are down rather than before any of
// them are.
func TestMergePartialWrite(t *testing.T) {
	dir := t.TempDir()
	watcher := &watchedDisk{}
	watcher.install(t)

	db, want := bigDB(t, dir)
	defer db.Close()

	segmentsBefore := db.Segments()

	// The merge writes into a file beside the oldest log. Let it get a little
	// way in and then run out of room.
	db.mu.RLock()
	temp := filepath.Base(db.path(db.frozen[len(db.frozen)-1].id())) + mergeSuffix
	db.mu.RUnlock()

	watcher.mu.Lock()
	watcher.writeLimit[temp] = 100 << 10 // less than the merge needs
	watcher.mu.Unlock()

	if err := db.Merge(); err == nil {
		t.Error("a merge that ran out of room reported success")
	}

	// Nothing was replaced, and the half-written file did not survive.
	if got := db.Segments(); got != segmentsBefore {
		t.Errorf("%d segments after a failed merge, was %d", got, segmentsBefore)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), mergeSuffix) {
			t.Errorf("a merge that ran out of room left %q behind", entry.Name())
		}
	}

	for key, value := range want {
		if got, ok := liveValue(t, db, key); !ok || got != value {
			t.Fatalf("%s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}

	// With room again, the merge goes through and the store is unchanged by
	// any of it.
	watcher.mu.Lock()
	delete(watcher.writeLimit, temp)
	watcher.mu.Unlock()

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge after the disk recovered: %v", err)
	}
	for key, value := range want {
		if got, ok := liveValue(t, db, key); !ok || got != value {
			t.Fatalf("after merging, %s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

// TestHintPartialWrite is the same for a hint, which is written the same way
// and matters less: a hint that did not finish is one the next open reads the
// log instead of.
func TestHintPartialWrite(t *testing.T) {
	dir := t.TempDir()
	watcher := &watchedDisk{}
	watcher.install(t)

	db, want := bigDB(t, dir)

	// Every hint from here on runs out of room part way through.
	watcher.mu.Lock()
	db.mu.RLock()
	for _, seg := range db.frozen {
		watcher.writeLimit[filepath.Base(hintPath(db.path(seg.id())))+mergeSuffix] = 4 << 10
	}
	db.mu.RUnlock()
	watcher.mu.Unlock()

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// No half-written hint was left where a reader would find it.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), mergeSuffix) {
			t.Errorf("a hint that ran out of room left %q behind", entry.Name())
		}
	}

	watcher.mu.Lock()
	watcher.writeLimit = map[string]int64{}
	watcher.mu.Unlock()

	reopened, err := OpenDB(dir, DBOptions{Sync: SyncNever, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatalf("Open after hints that could not be written: %v", err)
	}
	defer reopened.Close()

	for key, value := range want {
		if got, ok := liveValue(t, reopened, key); !ok || got != value {
			t.Fatalf("%s: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

// TestShortReadIsReported checks that a disk giving back less than it was asked
// for is an error rather than a wrong answer. This is the claim that a
// half-read record cannot be mistaken for a whole one.
func TestShortReadIsReported(t *testing.T) {
	// A record is read in two goes: its header, then the rest of it. Failing
	// the first and failing the second are different paths through the reader.
	for _, allowed := range []int{0, 1} {
		t.Run(fmt.Sprintf("after %d reads", allowed), func(t *testing.T) {
			dir := t.TempDir()
			watcher := &watchedDisk{}
			watcher.install(t)

			db, err := OpenDB(dir, smallSegments(300))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			for i := 0; i < 60; i++ {
				if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte(strings.Repeat("v", 40))); err != nil {
					t.Fatal(err)
				}
			}
			if db.Segments() < 2 {
				t.Fatalf("%d segments; the test needs a frozen one", db.Segments())
			}

			// Find a key that lives in a frozen log, so reading it goes to the
			// disk rather than to memory.
			db.mu.RLock()
			frozen := db.frozen[0]
			var key string
			frozen.eachKey(func(k string, _ int64) bool { key = k; return false })
			name := filepath.Base(db.path(frozen.id()))
			db.mu.RUnlock()

			if value, err := db.Read([]byte(key)); err != nil {
				t.Fatalf("%s should be readable before the disk misbehaves: %v", key, err)
			} else if len(value) == 0 {
				t.Fatalf("%s read back empty", key)
			}

			watcher.mu.Lock()
			watcher.readsAllowed[name] = allowed
			watcher.reads[name] = 0
			watcher.mu.Unlock()

			// A short read has to be an error, not a value built from whatever
			// came back.
			value, err := db.Read([]byte(key))
			if err == nil {
				t.Errorf("a short read gave back '%s' instead of an error", value)
			}
			if value != nil {
				t.Errorf("a failed read returned %d bytes as well as its error", len(value))
			}
		})
	}
}

// TestShortReadWhileIndexing checks the same for the streaming reader, which is
// what opening a store without a hint uses.
func TestShortReadWhileIndexing(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(300))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%02d", i)), []byte(strings.Repeat("v", 40))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Without hints, opening has to read the logs.
	hints, _ := hintFiles(t, dir)
	for _, name := range hints {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	watcher := &watchedDisk{}
	watcher.install(t)

	_, segments := hintFiles(t, dir)
	watcher.mu.Lock()
	for _, name := range segments {
		watcher.readsAllowed[name] = 0
	}
	watcher.mu.Unlock()

	sizesBefore := map[string]int64{}
	for _, name := range segments {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		sizesBefore[name] = info.Size()
	}

	// A disk that will not be read is a store that will not open, rather than
	// one that opens with some of its keys missing.
	if _, err := OpenDB(dir, smallSegments(300)); err == nil {
		t.Error("opening a store off an unreadable disk reported success")
	}

	// And nothing was thrown away on the strength of a read that failed. This
	// is the point: a torn tail is answered by cutting the log back to it, and
	// a read that cannot be served must not look like one.
	for _, name := range segments {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != sizesBefore[name] {
			t.Errorf("%s is %d bytes after a failed open, was %d", name, info.Size(), sizesBefore[name])
		}
	}

	// With the disk back, everything is still there.
	watcher.mu.Lock()
	watcher.readsAllowed = map[string]int{}
	watcher.mu.Unlock()

	reopened, err := OpenDB(dir, smallSegments(300))
	if err != nil {
		t.Fatalf("reopen after the disk recovered: %v", err)
	}
	defer reopened.Close()

	if got := reopened.Len(); got != 60 {
		t.Errorf("%d keys after the disk recovered, want 60", got)
	}
}
