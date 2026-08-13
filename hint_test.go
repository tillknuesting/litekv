package litekv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hintFiles lists the hints in a directory, and the logs beside them.
func hintFiles(t *testing.T, dir string) (hints, segments []string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		switch {
		case strings.HasSuffix(entry.Name(), hintSuffix):
			hints = append(hints, entry.Name())
		case strings.HasSuffix(entry.Name(), segmentSuffix):
			segments = append(segments, entry.Name())
		}
	}
	return hints, segments
}

// fillSegments writes enough to freeze several logs and returns what was stored.
func fillSegments(t *testing.T, db *DB, records int) map[string]string {
	t.Helper()

	want := map[string]string{}
	for i := range records {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := db.Write([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		want[key] = value
	}
	return want
}

func TestHintWrittenAndUsed(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	want := fillSegments(t, db, 200)
	frozen := db.Segments() - 1
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Every frozen log has a hint; the active one does not need one, since it
	// is read into memory anyway.
	hints, segments := hintFiles(t, dir)
	if len(hints) != frozen {
		t.Errorf("%d hints for %d frozen logs (%d logs in all)", len(hints), frozen, len(segments))
	}

	reopened, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	for key, value := range want {
		if got, ok := liveValue(t, reopened, key); !ok || got != value {
			t.Fatalf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

// TestHintSkipsTheScan shows a hint being taken at its word: a log whose
// records have been damaged still opens whole, because with a hint the records
// are not read at startup. Verify is what finds such damage.
func TestHintSkipsTheScan(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	want := fillSegments(t, db, 200)
	keys := db.Len()
	db.Close()

	// Damage a record in the oldest log, without changing its length.
	_, segments := hintFiles(t, dir)
	oldest := filepath.Join(dir, segments[0])
	file, err := os.OpenFile(oldest, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'!'}, headerSize+2); err != nil {
		t.Fatal(err)
	}
	file.Close()

	withHint, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	if got := withHint.Len(); got != keys {
		t.Errorf("with a hint: %d keys, want %d", got, keys)
	}
	// The damage is still there to be found, by the read that wants it.
	damaged := 0
	for key := range want {
		if _, err := withHint.Read([]byte(key)); errors.Is(err, ErrorChecksumMismatch) {
			damaged++
		}
	}
	if damaged == 0 {
		t.Error("the damaged record read back as if it were fine")
	}
	withHint.Close()

	// Without the hint, opening reads the log and stops at the damage, which
	// costs the records after it.
	for _, name := range mustHints(t, dir) {
		os.Remove(filepath.Join(dir, name))
	}

	withoutHint, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer withoutHint.Close()

	if got := withoutHint.Len(); got >= keys {
		t.Errorf("without a hint: %d keys, expected fewer than %d", got, keys)
	}
}

func mustHints(t *testing.T, dir string) []string {
	t.Helper()
	hints, _ := hintFiles(t, dir)
	if len(hints) == 0 {
		t.Fatal("no hints to remove")
	}
	return hints
}

func TestHintRejected(t *testing.T) {
	tests := []struct {
		name   string
		damage func(t *testing.T, dir, hint string)
	}{
		{
			name: "damaged",
			damage: func(t *testing.T, dir, hint string) {
				path := filepath.Join(dir, hint)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[len(data)/2]++
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "truncated",
			damage: func(t *testing.T, dir, hint string) {
				path := filepath.Join(dir, hint)
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, info.Size()/2); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "describing a log of another size",
			damage: func(t *testing.T, dir, hint string) {
				// Append to the log it describes, so its recorded size is wrong.
				segment := filepath.Join(dir, strings.TrimSuffix(hint, hintSuffix)+segmentSuffix)
				file, err := os.OpenFile(segment, os.O_APPEND|os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(make([]byte, 64))
				file.Close()
			},
		},
		{
			name: "not a hint at all",
			damage: func(t *testing.T, dir, hint string) {
				if err := os.WriteFile(filepath.Join(dir, hint), []byte("nonsense"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()

			db, err := OpenDB(dir, smallSegments(400))
			if err != nil {
				t.Fatal(err)
			}
			want := fillSegments(t, db, 150)
			db.Close()

			test.damage(t, dir, mustHints(t, dir)[0])

			// A hint that cannot be trusted is ignored, and the log read instead.
			reopened, err := OpenDB(dir, smallSegments(400))
			if err != nil {
				t.Fatalf("Open with a %s hint: %v", test.name, err)
			}
			defer reopened.Close()

			for key, value := range want {
				if got, ok := liveValue(t, reopened, key); !ok || got != value {
					t.Fatalf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
				}
			}
		})
	}
}

func TestHintWrittenAfterAScan(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, db, 150)
	db.Close()

	// A store from before hints existed has none.
	for _, name := range mustHints(t, dir) {
		os.Remove(filepath.Join(dir, name))
	}

	reopened, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	frozen := reopened.Segments() - 1
	reopened.Close()

	// Having read the logs the long way, it wrote down what it learned.
	hints, _ := hintFiles(t, dir)
	if len(hints) != frozen {
		t.Errorf("%d hints after a scan, want %d", len(hints), frozen)
	}
}

func TestHintFollowsTheMerge(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Rewrites, so merging has something to drop.
	want := map[string]string{}
	for round := range 30 {
		for i := range 10 {
			key := fmt.Sprintf("key%02d", i)
			value := fmt.Sprintf("value-%02d-%02d", i, round)
			if err := db.Write([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			want[key] = value
		}
	}

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	// One hint per frozen log, and no hint left over from a log that is gone.
	hints, segments := hintFiles(t, dir)
	if len(hints) != db.Segments()-1 {
		t.Errorf("%d hints for %d frozen logs", len(hints), db.Segments()-1)
	}
	for _, hint := range hints {
		segment := strings.TrimSuffix(hint, hintSuffix) + segmentSuffix
		found := false
		for _, name := range segments {
			if name == segment {
				found = true
			}
		}
		if !found {
			t.Errorf("hint %q has no log", hint)
		}
	}

	// And the hint the merge wrote is the one that gets used.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	for key, value := range want {
		if got, ok := liveValue(t, reopened, key); !ok || got != value {
			t.Errorf("key %q: got '%s' (%v), want '%s'", key, got, ok, value)
		}
	}
}

func TestHintOrphanRemoved(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, db, 100)
	db.Close()

	// A hint whose log is no longer there.
	writeInto(t, dir, "0000009999"+hintSuffix, []byte("left behind"))

	reopened, err := OpenDB(dir, smallSegments(400))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := os.Stat(filepath.Join(dir, "0000009999"+hintSuffix)); !os.IsNotExist(err) {
		t.Error("a hint with no log was left behind")
	}
}

// TestHintOpenCost is why hints exist.
func TestHintOpenCost(t *testing.T) {
	if testing.Short() {
		t.Skip("writes about 64 MB")
	}

	dir := t.TempDir()
	db, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 4 << 20, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}

	value := make([]byte, 1024)
	for i := range 64_000 {
		if err := db.Write(fmt.Appendf(nil, "key%08d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	keys := db.Len()
	db.Close()

	var onDisk, hintBytes int64
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		info, _ := entry.Info()
		if strings.HasSuffix(entry.Name(), hintSuffix) {
			hintBytes += info.Size()
		} else {
			onDisk += info.Size()
		}
	}

	open := func() time.Duration {
		start := time.Now()
		reopened, err := OpenDB(dir, DBOptions{Sync: SyncNever, MergeTrigger: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
		took := time.Since(start)
		if got := reopened.Len(); got != keys {
			t.Errorf("reopened with %d keys, want %d", got, keys)
		}
		reopened.Close()
		return took
	}

	withHints := open()

	for _, name := range mustHints(t, dir) {
		os.Remove(filepath.Join(dir, name))
	}
	withoutHints := open()

	t.Logf("%d keys in %d MiB: opening took %v with hints, %v without",
		keys, onDisk>>20, withHints.Round(time.Millisecond), withoutHints.Round(time.Millisecond))
	t.Logf("the hints are %d KiB against %d MiB of log, %.1f%% of it",
		hintBytes>>10, onDisk>>20, 100*float64(hintBytes)/float64(onDisk))
}
