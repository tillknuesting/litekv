package litekv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestDBReadsNothingLeftBehindByAMerge is the bug the randomised chaos run
// found, in the smallest shape that produces it.
//
// A merge renames its output over the oldest log it replaces and then removes
// the rest, and a removal that fails leaves a file the store in memory has
// already forgotten. The claim was that this is harmless: what is on the disk is
// the merged log plus the newest few of its inputs, and asking those first
// answers correctly.
//
// It stops being harmless at the *second* merge. The output takes the oldest id
// again, so it climbs back over the leftover in age while the leftover's id
// stays where it is — and nothing reads the ids again until the store is
// reopened, at which point the leftover is asked first and answers with records
// two merges old. Every checksum passes; the records are real, just not the ones
// asked for.
func TestDBReadsNothingLeftBehindByAMerge(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()
	opts := DBOptions{Sync: SyncNever, SegmentSize: 120, MergeTrigger: 1 << 30}

	db, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Three logs, with an old value for the key in the middle one.
	if err := db.Write([]byte("key"), []byte("oldest")); err != nil {
		t.Fatal(err)
	}
	rotate(t, db)
	if err := db.Write([]byte("key"), []byte("middle")); err != nil {
		t.Fatal(err)
	}
	rotate(t, db)
	if err := db.Write([]byte("other"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	rotate(t, db)

	// The first merge cannot remove log 2, so its file stays behind.
	watcher.fail = map[string]error{"remove:0000000002.seg": errDiskFailed}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	watcher.fail = map[string]error{}

	// The file is still there and it is empty, which is what makes what follows
	// safe: a log the store has forgotten cannot answer for anything.
	leftover := filepath.Join(dir, fmt.Sprintf("%010d%s", 2, segmentSuffix))
	if info, err := os.Stat(leftover); err != nil {
		t.Fatalf("log 2 is gone, so the merge did not have to leave it behind: %v", err)
	} else if info.Size() != 0 {
		t.Errorf("log 2 was left holding %d bytes", info.Size())
	}

	// The newest value for the key arrives after that, in a later log.
	if err := db.Write([]byte("key"), []byte("newest")); err != nil {
		t.Fatal(err)
	}
	rotate(t, db)

	// The second merge folds that log into the output as well — which is named
	// after the oldest log, and so is older than the leftover.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	if got, err := db.Read([]byte("key")); err != nil || string(got) != "newest" {
		t.Fatalf("before reopening, key = %q, '%v', want 'newest'", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got, err := reopened.Read([]byte("key")); err != nil || string(got) != "newest" {
		t.Errorf("after reopening, key = %q, '%v', want 'newest'", got, err)
	}
}

func rotate(t *testing.T, db *DB) {
	t.Helper()

	db.mu.Lock()
	err := db.rotateLocked()
	db.mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}
}
