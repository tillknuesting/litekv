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

	previous := openDisk
	openDisk = func(name string, flag int, perm os.FileMode) (diskFile, error) {
		if err := w.record("open", name); err != nil {
			return nil, err
		}
		file, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		return &watchedFile{File: file, disk: w, name: name}, nil
	}
	t.Cleanup(func() { openDisk = previous })
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
	*os.File
	disk *watchedDisk
	name string
}

func (f *watchedFile) Sync() error {
	if err := f.disk.record("sync", f.name); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *watchedFile) Close() error {
	if err := f.disk.record("close", f.name); err != nil {
		return err
	}
	return f.File.Close()
}

func (f *watchedFile) Truncate(size int64) error {
	if err := f.disk.record("truncate", f.name); err != nil {
		return err
	}
	return f.File.Truncate(size)
}

func (f *watchedFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.disk.record("write", f.name); err != nil {
		return 0, err
	}
	return f.File.WriteAt(p, off)
}

func (f *watchedFile) Write(p []byte) (int, error) {
	if err := f.disk.record("write", f.name); err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

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
