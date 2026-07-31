package litekv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// memLog is a Log that keeps the records in memory, counts syncs, and can be
// told to start failing.
type memLog struct {
	mu      sync.Mutex
	data    []byte
	syncs   int
	failing bool
}

var errLogFull = errors.New("log is full")

func (m *memLog) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failing {
		return 0, errLogFull
	}
	if int64(len(m.data)) < off {
		return 0, fmt.Errorf("write at %d leaves a hole after %d", off, len(m.data))
	}
	m.data = append(m.data[:off], p...)
	return len(p), nil
}

func (m *memLog) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int64(len(m.data)) > size {
		m.data = m.data[:size]
	}
	return nil
}

func (m *memLog) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.syncs++
	return nil
}

func (m *memLog) contents() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]byte(nil), m.data...)
}

func (m *memLog) syncCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.syncs
}

func TestOpenRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv")

	kvs, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 50; i++ {
		if err := kvs.Write([]byte(fmt.Sprintf("key%02d", i)), []byte(fmt.Sprintf("value%02d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := kvs.Write([]byte("key00"), []byte("updated")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := kvs.Delete([]byte("key01")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The file holds exactly what the store holds.
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(onDisk) != string(kvs.Data) {
		t.Errorf("file holds %d bytes, Data holds %d", len(onDisk), len(kvs.Data))
	}

	if err := kvs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got := len(reopened.Index); got != 50 {
		t.Errorf("reopened with %d keys, want 50", got)
	}
	if value, err := reopened.Read([]byte("key00")); err != nil || string(value) != "updated" {
		t.Errorf("key00: got '%s' (err %v), want 'updated'", value, err)
	}
	if _, err := reopened.Read([]byte("key01")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("key01: expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
	if value, err := reopened.Read([]byte("key49")); err != nil || string(value) != "value49" {
		t.Errorf("key49: got '%s' (err %v), want 'value49'", value, err)
	}
	if err := reopened.Verify(); err != nil {
		t.Errorf("Verify: %v", err)
	}
}

// TestDataSliceIsTheFormat is the promise that the byte slice is the whole
// store: what one half writes, the other reads.
func TestDataSliceIsTheFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv")

	// Built in memory, written out by hand, opened as a file.
	memory := &KeyValueStore{}
	memory.Write([]byte("a"), []byte("1"))
	memory.Write([]byte("b"), []byte("2"))

	if err := os.WriteFile(path, memory.Data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	opened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if value, err := opened.Read([]byte("b")); err != nil || string(value) != "2" {
		t.Errorf("b: got '%s' (err %v), want '2'", value, err)
	}

	if err := opened.Write([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Written by the file store, read back by hand into a plain store.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	byHand := &KeyValueStore{Data: raw}
	discarded, err := byHand.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if discarded != 0 {
		t.Errorf("Recover discarded %d bytes of an intact store", discarded)
	}
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if value, err := byHand.Read([]byte(key)); err != nil || string(value) != want {
			t.Errorf("%s: got '%s' (err %v), want '%s'", key, value, err, want)
		}
	}
}

func TestOpenRecoversTornTail(t *testing.T) {
	tests := []struct {
		name   string
		damage func(t *testing.T, path string, size int64)
	}{
		{
			name: "record cut short by a crash",
			damage: func(t *testing.T, path string, size int64) {
				if err := os.Truncate(path, size-3); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "record whose bytes did not all land",
			damage: func(t *testing.T, path string, size int64) {
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				// Flip a byte inside the last record's value.
				if _, err := file.WriteAt([]byte{'X'}, size-2); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "kv")

			kvs, err := Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			kvs.Write([]byte("keep1"), []byte("value1"))
			kvs.Write([]byte("keep2"), []byte("value2"))
			goodSize := int64(len(kvs.Data))
			kvs.Write([]byte("torn"), []byte("never acknowledged"))
			fullSize := int64(len(kvs.Data))
			kvs.Close()

			test.damage(t, path, fullSize)

			reopened, err := Open(path, Options{})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()

			if int64(len(reopened.Data)) != goodSize {
				t.Errorf("recovered %d bytes, want %d", len(reopened.Data), goodSize)
			}
			if _, err := reopened.Read([]byte("torn")); !errors.Is(err, ErrorKeyNotFound) {
				t.Errorf("the torn record survived: %v", err)
			}
			for _, key := range []string{"keep1", "keep2"} {
				if _, err := reopened.Read([]byte(key)); err != nil {
					t.Errorf("%s was lost: %v", key, err)
				}
			}
			if err := reopened.Verify(); err != nil {
				t.Errorf("Verify after recovery: %v", err)
			}

			// The file was truncated too, so the damage is gone for good.
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != goodSize {
				t.Errorf("file is %d bytes after recovery, want %d", info.Size(), goodSize)
			}

			// And the store keeps working from there.
			if err := reopened.Write([]byte("after"), []byte("recovery")); err != nil {
				t.Fatalf("Write after recovery: %v", err)
			}
			if value, err := reopened.Read([]byte("after")); err != nil || string(value) != "recovery" {
				t.Errorf("after: got '%s' (err %v)", value, err)
			}
		})
	}
}

func TestRecoverReportsDiscarded(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("1"))
	good := len(kvs.Data)
	kvs.Write([]byte("b"), []byte("2"))

	// Damage the second record.
	kvs.Data[len(kvs.Data)-1]++

	discarded, err := kvs.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if want := int64(len(kvs.Data)+int(discarded)) - int64(good); discarded != want {
		t.Errorf("discarded %d bytes, want %d", discarded, want)
	}
	if len(kvs.Data) != good {
		t.Errorf("kept %d bytes, want %d", len(kvs.Data), good)
	}
	if _, err := kvs.Read([]byte("b")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("the damaged record survived: %v", err)
	}
}

// TestWriteFailureLeavesStoreUnchanged is the reason the index is pointed at a
// record only after the log has taken it.
func TestWriteFailureLeavesStoreUnchanged(t *testing.T) {
	log := &memLog{}
	kvs := &KeyValueStore{}
	if err := kvs.Attach(log, Options{Sync: SyncNever}); err != nil {
		t.Fatal(err)
	}

	if err := kvs.Write([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	before := string(kvs.Data)
	keys := len(kvs.Index)

	log.failing = true
	err := kvs.Write([]byte("b"), []byte("2"))
	if !errors.Is(err, errLogFull) {
		t.Errorf("expected the log's error, got '%v'", err)
	}

	if string(kvs.Data) != before {
		t.Errorf("a failed write left %d bytes in Data, want %d", len(kvs.Data), len(before))
	}
	if len(kvs.Index) != keys {
		t.Errorf("a failed write left %d keys indexed, want %d", len(kvs.Index), keys)
	}
	if _, err := kvs.Read([]byte("b")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("a failed write became readable: %v", err)
	}

	// The store recovers once the log does.
	log.failing = false
	if err := kvs.Write([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Write after the log recovered: %v", err)
	}
	if string(log.contents()) != string(kvs.Data) {
		t.Error("the log and Data disagree after the failure")
	}
}

func TestSyncPolicies(t *testing.T) {
	t.Run("always syncs every write", func(t *testing.T) {
		log := &memLog{}
		kvs := &KeyValueStore{}
		kvs.Attach(log, Options{Sync: SyncAlways})

		for i := 0; i < 5; i++ {
			kvs.Write([]byte{byte(i)}, []byte("v"))
		}
		if got := log.syncCount(); got != 5 {
			t.Errorf("%d syncs for 5 writes, want 5", got)
		}
	})

	t.Run("never syncs on its own", func(t *testing.T) {
		log := &memLog{}
		kvs := &KeyValueStore{}
		kvs.Attach(log, Options{Sync: SyncNever})

		for i := 0; i < 5; i++ {
			kvs.Write([]byte{byte(i)}, []byte("v"))
		}
		if got := log.syncCount(); got != 0 {
			t.Errorf("%d syncs under SyncNever, want 0", got)
		}

		// But asking still works.
		if err := kvs.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if got := log.syncCount(); got != 1 {
			t.Errorf("%d syncs after calling Sync, want 1", got)
		}
	})

	t.Run("every syncs on its timer", func(t *testing.T) {
		log := &memLog{}
		kvs := &KeyValueStore{}
		kvs.Attach(log, Options{Sync: SyncEvery, Interval: 10 * time.Millisecond})

		kvs.Write([]byte("a"), []byte("1"))

		deadline := time.Now().Add(2 * time.Second)
		for log.syncCount() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if log.syncCount() == 0 {
			t.Error("the timer never synced")
		}

		if err := kvs.Detach(); err != nil {
			t.Fatalf("Detach: %v", err)
		}

		// Detaching stops the timer and leaves the data in memory.
		settled := log.syncCount()
		time.Sleep(40 * time.Millisecond)
		if log.syncCount() != settled {
			t.Error("the timer kept running after Detach")
		}
		if value, err := kvs.Read([]byte("a")); err != nil || string(value) != "1" {
			t.Errorf("a: got '%s' (err %v) after Detach", value, err)
		}
		if err := kvs.Write([]byte("b"), []byte("2")); err != nil {
			t.Errorf("Write after Detach: %v", err)
		}
		if string(log.contents()) == string(kvs.Data) {
			t.Error("a detached store is still writing to the log")
		}
	})
}

func TestCloseRejectsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv")
	kvs, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	kvs.Write([]byte("a"), []byte("1"))

	if err := kvs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := kvs.Close(); err != nil {
		t.Errorf("closing twice: %v", err)
	}

	if err := kvs.Write([]byte("b"), []byte("2")); !errors.Is(err, ErrorClosed) {
		t.Errorf("expected '%v', got '%v'", ErrorClosed, err)
	}
	if err := kvs.Delete([]byte("a")); !errors.Is(err, ErrorClosed) {
		t.Errorf("expected '%v', got '%v'", ErrorClosed, err)
	}

	// Reads still work: the data is in memory either way.
	if value, err := kvs.Read([]byte("a")); err != nil || string(value) != "1" {
		t.Errorf("a: got '%s' (err %v) after Close", value, err)
	}
}

func TestCompactRewritesTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv")
	kvs, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		kvs.Write([]byte("hot"), []byte(fmt.Sprintf("value%03d", i)))
	}
	kvs.Write([]byte("cold"), []byte("kept"))
	kvs.Write([]byte("gone"), []byte("deleted"))
	kvs.Delete([]byte("gone"))

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := kvs.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Errorf("the file did not shrink: %d bytes before, %d after", before.Size(), after.Size())
	}
	if after.Size() != int64(len(kvs.Data)) {
		t.Errorf("the file is %d bytes, Data is %d", after.Size(), len(kvs.Data))
	}

	// Writes still land in the new file.
	if err := kvs.Write([]byte("after"), []byte("compaction")); err != nil {
		t.Fatalf("Write after Compact: %v", err)
	}
	if err := kvs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	for key, want := range map[string]string{"hot": "value199", "cold": "kept", "after": "compaction"} {
		if value, err := reopened.Read([]byte(key)); err != nil || string(value) != want {
			t.Errorf("%s: got '%s' (err %v), want '%s'", key, value, err, want)
		}
	}
	if _, err := reopened.Read([]byte("gone")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("a compacted-away key came back: %v", err)
	}
}

func TestRewriteSeedsAnEmptyLog(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("1"))
	kvs.Write([]byte("b"), []byte("2"))

	log := &memLog{}
	if err := kvs.Attach(log, Options{Sync: SyncNever}); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Rewrite(); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	if string(log.contents()) != string(kvs.Data) {
		t.Error("Rewrite did not put the store's data in the log")
	}

	kvs.Write([]byte("c"), []byte("3"))
	if string(log.contents()) != string(kvs.Data) {
		t.Error("the log and Data diverged after Rewrite")
	}
}

func TestAttachTwice(t *testing.T) {
	kvs := &KeyValueStore{}
	if err := kvs.Attach(&memLog{}, Options{Sync: SyncNever}); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Attach(&memLog{}, Options{Sync: SyncNever}); !errors.Is(err, ErrorAttached) {
		t.Errorf("expected '%v', got '%v'", ErrorAttached, err)
	}
}

func TestInMemoryStoreNeedsNothing(t *testing.T) {
	// The zero value still touches none of this.
	kvs := &KeyValueStore{}
	if err := kvs.Write([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Sync(); err != nil {
		t.Errorf("Sync on an in-memory store: %v", err)
	}
	if err := kvs.Close(); err != nil {
		t.Errorf("Close on an in-memory store: %v", err)
	}
	if err := kvs.Rewrite(); err != nil {
		t.Errorf("Rewrite on an in-memory store: %v", err)
	}
	// Closing an in-memory store does not stop it.
	if err := kvs.Write([]byte("b"), []byte("2")); err != nil {
		t.Errorf("Write after Close on an in-memory store: %v", err)
	}
}

func TestFileStoreConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv")
	kvs, err := Open(path, Options{Sync: SyncEvery, Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			for j := 0; j < 50; j++ {
				if err := kvs.Write(key, []byte(fmt.Sprintf("value%d", j))); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
				kvs.Read(key)
				kvs.Sync()
			}
		}(i)
	}
	wg.Wait()

	if err := kvs.Verify(); err != nil {
		t.Errorf("Verify: %v", err)
	}
	if err := kvs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk) != string(kvs.Data) {
		t.Errorf("the file holds %d bytes, Data holds %d", len(onDisk), len(kvs.Data))
	}
}

func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		path := filepath.Join(t.TempDir(), "kv")
		kvs, err := Open(path, Options{Sync: SyncEvery, Interval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		kvs.Write([]byte("a"), []byte("1"))
		if err := kvs.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Give any leaked goroutine a chance to still be running.
	time.Sleep(20 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutines went from %d to %d", before, after)
	}
}

// BenchmarkWriteDurability is the price of each sync policy, which is the whole
// reason the choice exists. The in-memory case is the floor.
func BenchmarkWriteDurability(b *testing.B) {
	value := make([]byte, 128)

	policies := []struct {
		name string
		opts Options
	}{
		{"SyncAlways", Options{Sync: SyncAlways}},
		{"SyncEvery/100ms", Options{Sync: SyncEvery, Interval: 100 * time.Millisecond}},
		{"SyncNever", Options{Sync: SyncNever}},
	}

	for _, policy := range policies {
		b.Run(policy.name, func(b *testing.B) {
			kvs, err := Open(filepath.Join(b.TempDir(), "kv"), policy.opts)
			if err != nil {
				b.Fatal(err)
			}
			defer kvs.Close()

			key := []byte("key")
			b.SetBytes(int64(len(value)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kvs.Write(key, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("memory", func(b *testing.B) {
		kvs := &KeyValueStore{}
		key := []byte("key")
		b.SetBytes(int64(len(value)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kvs.Write(key, value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestUnclosedStoreSurvives is the answer to "what if we never get to Close".
// A record is written to the log as it happens, so the operating system has it
// the moment Write returns, whether or not anything is ever synced or closed.
// Killing the process cannot take it back: only losing power can, which is what
// the sync policy is about.
//
// Opening the same file a second time while the first store is still open and
// unclosed stands in for a process that died without cleaning up.
func TestUnclosedStoreSurvives(t *testing.T) {
	for _, policy := range []struct {
		name string
		opts Options
	}{
		{"SyncNever", Options{Sync: SyncNever}},
		{"SyncEvery/1h", Options{Sync: SyncEvery, Interval: time.Hour}}, // never fires
	} {
		t.Run(policy.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "kv")

			doomed, err := Open(path, policy.opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i++ {
				if err := doomed.Write([]byte(fmt.Sprintf("key%02d", i)), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}
			// No Sync. No Close. The process is gone.

			survivor, err := Open(path, Options{})
			if err != nil {
				t.Fatalf("Open after the crash: %v", err)
			}
			defer survivor.Close()

			if got := len(survivor.Index); got != 20 {
				t.Errorf("recovered %d keys, want 20", got)
			}
			if value, err := survivor.Read([]byte("key19")); err != nil || string(value) != "value" {
				t.Errorf("key19: got '%s' (err %v)", value, err)
			}
			if err := survivor.Verify(); err != nil {
				t.Errorf("Verify: %v", err)
			}
		})
	}
}
