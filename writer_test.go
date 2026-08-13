package litekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A Writer exists because two goroutines writing to a store halve its
// throughput, and a server has a goroutine per request. What it does about that
// is put one writer in front of the store and let everybody else queue, and
// what it gets for free by doing so is group commit: everything waiting when
// the writer wakes is stored as one batch, so it is one write to the log and
// one wait for the disk however many callers are behind it.

// heldStore stands in for a store that is slow to write, so that a test can say
// exactly who is waiting when the writer wakes rather than hoping.
type heldStore struct {
	mu      sync.Mutex
	groups  [][]string // the keys of each batch it was given, in order
	entered chan struct{}
	release chan struct{}
	fail    error
}

func (h *heldStore) WriteBatch(b *Batch) error {
	if h.entered != nil {
		h.entered <- struct{}{}
	}
	if h.release != nil {
		<-h.release
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	keys := make([]string, 0, len(b.entries))
	for _, entry := range b.entries {
		keys = append(keys, string(entry.Key))
	}
	h.groups = append(h.groups, keys)

	return h.fail
}

func (h *heldStore) written() [][]string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([][]string(nil), h.groups...)
}

// queued waits until the writer's queue holds n callers, so that a test can act
// on a queue it knows the shape of. It is not a timing assertion: the callers
// are already blocked in their own goroutines and the only question is when the
// runtime gets to them.
func queued(t *testing.T, w *Writer, n int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for len(w.queue) < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d callers reached the queue", len(w.queue), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWriterStoresEverythingConcurrently(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	w := db.Writer(WriterOptions{})

	const writers, each = 8, 100

	var wg sync.WaitGroup
	for id := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := range each {
				key := fmt.Appendf(nil, "key-%d-%03d", id, i)
				if err := w.Write(key, []byte("value")); err != nil {
					t.Errorf("write: %v", err)
					return
				}

				// The slices are the caller's again the moment this returns,
				// exactly as they are after a write of the store's own.
				key[0] = 'K'
			}
		}(id)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for id := range writers {
		for i := range each {
			key := fmt.Sprintf("key-%d-%03d", id, i)
			if got, err := db.Read([]byte(key)); err != nil || string(got) != "value" {
				t.Fatalf("%s = %q, '%v'", key, got, err)
			}
		}
	}
	if got := db.Len(); got != writers*each {
		t.Errorf("the store holds %d keys, want %d", got, writers*each)
	}
}

// TestWriterStoresWhatIsWaitingAsOne is the claim that makes this worth having.
// The store is held while one caller is being written, everybody else piles up
// behind it, and what they cost between them is a single batch.
func TestWriterStoresWhatIsWaitingAsOne(t *testing.T) {
	held := &heldStore{entered: make(chan struct{}), release: make(chan struct{})}
	w := newWriter(held, WriterOptions{})

	done := make(chan error, 10)

	// The first caller reaches the store, and is held inside it.
	go func() { done <- w.Write([]byte("first"), []byte("value")) }()
	<-held.entered

	// Nine more, which cannot be taken off the queue while the writer is held.
	for i := range 9 {
		go func(i int) { done <- w.Write(fmt.Appendf(nil, "key-%d", i), []byte("value")) }(i)
	}
	queued(t, w, 9)

	// Let the first one finish. The writer wakes to nine waiting callers and
	// takes all of them, which is the whole of group commit.
	held.release <- struct{}{}
	<-held.entered
	held.release <- struct{}{}

	for i := range 10 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("write: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 10 callers were answered", i)
		}
	}

	groups := held.written()
	if len(groups) != 2 {
		t.Fatalf("ten callers cost %d batches, want 2: %v", len(groups), groups)
	}
	if len(groups[0]) != 1 || groups[0][0] != "first" {
		t.Errorf("the first batch held %v, want just the first caller", groups[0])
	}
	if len(groups[1]) != 9 {
		t.Errorf("the nine waiting callers cost a batch of %d, want 9: %v", len(groups[1]), groups[1])
	}

	// Nothing is held any more, so Close can drain what is left of nothing.
	close(held.release)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWriterGroupIsOneBatchInTheLog is the same claim seen from the log: a
// group is a batch, so a crash loses the group or none of it, and the records
// of one caller are never split across two of them.
func TestWriterGroupIsOneBatchInTheLog(t *testing.T) {
	kvs := &KeyValueStore{}
	w := kvs.Writer(WriterOptions{})

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var b Batch
			b.Write(fmt.Appendf(nil, "key-%02d-a", i), []byte("value"))
			b.Write(fmt.Appendf(nil, "key-%02d-b", i), []byte("value"))

			if err := w.WriteBatch(&b); err != nil {
				t.Errorf("WriteBatch: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// However the groups fell out, every one of them is a batch, and a caller's
	// two records are inside the same one.
	kvs.RLock()
	defer kvs.RUnlock()

	spans := map[string]int{}
	group := 0
	for at := int64(0); at < int64(len(kvs.Data)); {
		record, next, err := parseRecordAt(kvs.Data, at)
		if err != nil {
			t.Fatalf("the log will not decode at %d: %v", at, err)
		}
		if record.Type == RecordTypeBatch {
			group++
		} else {
			spans[string(record.Key)] = group
		}
		at = next
	}

	if group == 0 {
		t.Fatal("the writer wrote no batches at all")
	}
	for i := range 20 {
		a := spans[fmt.Sprintf("key-%02d-a", i)]
		b := spans[fmt.Sprintf("key-%02d-b", i)]
		if a == 0 || a != b {
			t.Errorf("caller %d landed in batches %d and %d", i, a, b)
		}
	}
}

func TestWriterAnswersEveryoneInAFailedGroup(t *testing.T) {
	failed := errors.New("the disk said no")
	held := &heldStore{entered: make(chan struct{}, 8), release: make(chan struct{}), fail: failed}
	w := newWriter(held, WriterOptions{})
	defer w.Close()

	done := make(chan error, 5)
	for i := range 5 {
		go func(i int) { done <- w.Write(fmt.Appendf(nil, "key-%d", i), []byte("value")) }(i)
	}

	// However they fall into groups, every group goes through and every caller
	// hears the same answer.
	close(held.release)

	for i := range 5 {
		select {
		case err := <-done:
			if !errors.Is(err, failed) {
				t.Errorf("a caller in a failed group reported '%v', want %v", err, failed)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 5 callers were answered", i)
		}
	}
}

// TestWriterCloseDrainsTheQueue holds the one thing a queue must not do, which
// is lose what it accepted. A caller waiting when Close is called is written and
// answered; a caller arriving afterwards is turned away rather than left.
func TestWriterCloseDrainsTheQueue(t *testing.T) {
	// Room in entered for the groups nobody is waiting to hear about: the
	// writer must not be held up by the test's own bookkeeping.
	held := &heldStore{entered: make(chan struct{}, 8), release: make(chan struct{})}
	w := newWriter(held, WriterOptions{})

	done := make(chan error, 6)

	// One caller held inside the store, and five more queued behind it, so that
	// Close arrives with records in the writer's hand and records on the queue.
	go func() { done <- w.Write([]byte("held"), []byte("value")) }()
	<-held.entered

	for i := range 5 {
		go func(i int) { done <- w.Write(fmt.Appendf(nil, "key-%d", i), []byte("value")) }(i)
	}
	queued(t, w, 5)

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()

	// Close cannot have finished: six callers have been accepted and the store
	// has not let go of the first of them, so none of them can have been
	// written. Returning here would not be Close being quick, it would be Close
	// abandoning records it took responsibility for.
	time.Sleep(10 * time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close returned ('%v') while the writer was still holding six callers", err)
	default:
	}

	close(held.release)

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned")
	}

	// And by the time it returned, all six were written — not merely on their
	// way. Nothing below waits, which is the point of asking here.
	stored := 0
	for _, group := range held.written() {
		stored += len(group)
	}
	if stored != 6 {
		t.Errorf("Close returned with %d of the 6 records it had accepted written", stored)
	}

	for i := range 6 {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("a caller queued before Close reported '%v'", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 6 callers queued before Close were answered", i)
		}
	}

	// And afterwards it takes nothing.
	if err := w.Write([]byte("late"), []byte("value")); !errors.Is(err, ErrorClosed) {
		t.Errorf("a write after Close reported '%v', want %v", err, ErrorClosed)
	}
	var b Batch
	b.Write([]byte("late"), []byte("value"))
	if err := w.WriteBatch(&b); !errors.Is(err, ErrorClosed) {
		t.Errorf("a batch after Close reported '%v', want %v", err, ErrorClosed)
	}
	if err := w.Delete([]byte("late")); !errors.Is(err, ErrorClosed) {
		t.Errorf("a delete after Close reported '%v', want %v", err, ErrorClosed)
	}

	// Closing twice is harmless.
	if err := w.Close(); err != nil {
		t.Errorf("closing twice reported '%v'", err)
	}
}

func TestWriterWritesEveryKindOfRecord(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	w := db.Writer(WriterOptions{Queue: 4})
	defer w.Close()

	if err := w.Write([]byte("plain"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteExpiring([]byte("later"), []byte("value"), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteExpiring([]byte("gone"), []byte("value"), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete([]byte("plain")); err != nil {
		t.Fatal(err)
	}

	var b Batch
	b.Write([]byte("one"), []byte("value"))
	b.Delete([]byte("one"))
	if err := w.WriteBatch(&b); err != nil {
		t.Fatal(err)
	}

	// An empty batch is nothing to do, and must not leave a caller waiting.
	var empty Batch
	if err := w.WriteBatch(&empty); err != nil {
		t.Errorf("an empty batch reported '%v'", err)
	}

	if _, err := db.Read([]byte("plain")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("plain reads as '%v', want %v", err, ErrorKeyDeleted)
	}
	if got, err := db.Read([]byte("later")); err != nil || string(got) != "value" {
		t.Errorf("later = %q, '%v'", got, err)
	}
	if _, err := db.Read([]byte("gone")); !errors.Is(err, ErrorKeyExpired) {
		t.Errorf("gone reads as '%v', want %v", err, ErrorKeyExpired)
	}
	if _, err := db.Read([]byte("one")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("one reads as '%v', want %v", err, ErrorKeyDeleted)
	}
}

// TestWriterOnAFencedStore checks that a Writer hides nothing the store would
// have said: it is a queue in front of the same call, not a different one.
func TestWriterOnAFencedStore(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	w := db.Writer(WriterOptions{})
	defer w.Close()

	if err := w.Write([]byte("before"), []byte("the fence")); err != nil {
		t.Fatal(err)
	}

	ahead := db.Position()
	ahead.Term += 5
	if _, err := db.Since(ahead, nil, ReplicaOptions{}); !errors.Is(err, ErrorFenced) {
		t.Fatalf("the store was not fenced: %v", err)
	}

	if err := w.Write([]byte("after"), []byte("the fence")); !errors.Is(err, ErrorFenced) {
		t.Errorf("a write to a fenced store through a Writer reported '%v', want %v", err, ErrorFenced)
	}
}

// BenchmarkWriterInMemory is the contention on its own: a store with no file
// under it, so there is no system call and no disk in the measurement, only the
// write lock and what it costs several goroutines to take turns on it.
func BenchmarkWriterInMemory(b *testing.B) {
	for _, through := range []string{"direct", "queued"} {
		b.Run(through, func(b *testing.B) {
			kvs := &KeyValueStore{}
			w := kvs.Writer(WriterOptions{})
			defer w.Close()

			value := make([]byte, 128)

			b.SetBytes(int64(len(value)))
			b.ReportAllocs()
			b.ResetTimer()

			var counter atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				key := make([]byte, 16)
				for pb.Next() {
					// Bounded, or this measures the allocator: see the note in
					// AGENTS.md about write benchmarks.
					if kvs.Size() > 1<<26 {
						b.StopTimer()
						kvs.Lock()
						kvs.Data, kvs.Index, kvs.lastRecord = kvs.Data[:0], nil, 0
						kvs.Unlock()
						b.StartTimer()
					}

					binary.LittleEndian.PutUint64(key, counter.Add(1))

					var err error
					if through == "direct" {
						err = kvs.Write(key, value)
					} else {
						err = w.Write(key, value)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkWriterParallel is the number this exists for: several goroutines
// writing at once, through the store's own Write and through a Writer.
//
// SyncNever is the lock contention on its own. SyncAlways is what a server
// actually runs, and there the queue is amortizing a wait for the disk across
// everybody who is waiting — which is why the two rows are so far apart.
func BenchmarkWriterParallel(b *testing.B) {
	for _, policy := range []struct {
		name string
		sync SyncPolicy
	}{
		{"never", SyncNever},
		{"every", SyncEvery},
		{"always", SyncAlways},
	} {
		for _, through := range []string{"direct", "queued"} {
			b.Run(policy.name+"/"+through, func(b *testing.B) {
				db, err := OpenDB(b.TempDir(), DBOptions{
					Sync: policy.sync, SegmentSize: 1 << 30, MergeTrigger: 1 << 30,
				})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				w := db.Writer(WriterOptions{})
				defer w.Close()

				value := make([]byte, 128)

				b.SetBytes(int64(len(value)))
				b.ReportAllocs()
				b.ResetTimer()

				var counter atomic.Uint64
				b.RunParallel(func(pb *testing.PB) {
					key := make([]byte, 16)
					for pb.Next() {
						binary.LittleEndian.PutUint64(key, counter.Add(1))

						var err error
						if through == "direct" {
							err = db.Write(key, value)
						} else {
							err = w.Write(key, value)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
