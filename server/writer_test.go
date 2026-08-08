package server

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

// A handler per request is a goroutine per request, and a write takes every
// shard of the store's lock. Two goroutines writing do not merely fail to go
// faster — they halve the store's throughput, because the second spends its
// time waiting for the first — so an HTTP server is the worst caller a store of
// this shape can have. litekv.Writer was written for it before there was one:
// the callers hand their records to a queue, one goroutine writes, and
// everything waiting when it wakes goes down as a single batch and a single
// sync.
//
// These are about the queue being in the path and staying there. What it is
// worth is BenchmarkWriteThroughTheHandler, at the bottom.

func TestConcurrentWritesAreAllStored(t *testing.T) {
	s, _ := newServer(t, Options{})

	const clients, each = 16, 32

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < each; i++ {
				key := fmt.Sprintf("client-%02d-key-%02d", c, i)

				rec := httptest.NewRecorder()
				s.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v1/keys/"+key,
					strings.NewReader(key+"-value")))

				if rec.Code != http.StatusNoContent {
					t.Errorf("PUT %s: %d", key, rec.Code)
					return
				}
			}
		}()
	}
	wg.Wait()

	for c := 0; c < clients; c++ {
		for i := 0; i < each; i++ {
			key := fmt.Sprintf("client-%02d-key-%02d", c, i)

			body := wants(t, do(t, s, http.MethodGet, "/v1/keys/"+key, nil), http.StatusOK)
			if string(body) != key+"-value" {
				t.Fatalf("%s = %q", key, body)
			}
		}
	}
}

// TestClosingTheServerStopsWritesAndNotReads is the one that says the queue is
// in the path rather than being a field nobody reads.
//
// A closed Server is not a closed store. The writer has stopped, so nothing new
// can be stored — but the store is still open, still holds everything, and
// still answers. Writes going straight to the store would carry on being
// accepted here, which is the whole difference this asserts.
func TestClosingTheServerStopsWritesAndNotReads(t *testing.T) {
	s, db := newServer(t, Options{})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/before", strings.NewReader("v")), http.StatusNoContent)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	wants(t, do(t, s, http.MethodPut, "/v1/keys/after", strings.NewReader("v")), http.StatusServiceUnavailable)
	wants(t, do(t, s, http.MethodDelete, "/v1/keys/before", nil), http.StatusServiceUnavailable)

	if body := wants(t, do(t, s, http.MethodGet, "/v1/keys/before", nil), http.StatusOK); string(body) != "v" {
		t.Errorf("a closed server read %q, want %q", body, "v")
	}

	// And the store really is still open, which is what makes the shutdown
	// order in cmd/litekvd the order it is: the writer goes before the store,
	// never after.
	if err := db.Write([]byte("direct"), []byte("v")); err != nil {
		t.Errorf("the store was closed along with the server: %v", err)
	}

	// Closing twice is harmless, which matters because the binary both defers
	// it and calls it on the way down.
	if err := s.Close(); err != nil {
		t.Errorf("closing twice: %v", err)
	}
}

// TestNoGoroutineLeak holds New to the bargain it makes: it starts a goroutine
// the caller did not ask for, so it has to hand back something that stops it.
func TestNoGoroutineLeak(t *testing.T) {
	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	before := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		s := New(db, Options{Logger: quiet()})

		wants(t, do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusNoContent)

		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Give any that leaked a chance to still be running.
	time.Sleep(20 * time.Millisecond)

	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutines went from %d to %d over twenty servers", before, after)
	}
}

// nothing stores nothing. It is the floor of the benchmark below: what a
// request costs before the store has been asked to do anything at all.
type nothing struct{}

func (nothing) Write(key, value []byte) error                       { return nil }
func (nothing) WriteExpiring(key, value []byte, at time.Time) error { return nil }
func (nothing) Delete(key []byte) error                             { return nil }
func (nothing) WriteBatch(b *litekv.Batch) error                    { return nil }

// BenchmarkWriteThroughTheHandler is the number this piece exists for: the same
// handler, the same store, several requests at once, with the queue in the way
// and without it.
//
// It drives the handler with recorders rather than over a socket on purpose.
// The socket is real cost and it is the same cost either way; putting it in the
// measurement would bury the thing being measured under it. What is left is a
// handler goroutine per request contending for a store, which is the shape the
// queue is for.
//
// SyncNever is the lock contention on its own. SyncAlways is what a server that
// means it runs, and there the queue is amortizing one wait for the disk across
// everybody waiting, which is why the rows are so far apart.
//
// The "nothing" row is the floor and is there so the other two can be read. A
// benchmark that builds a request and a recorder per iteration is measuring
// that too, and without a row saying how much of the number it is, the ratio
// between the other two looks smaller than it is.
func BenchmarkWriteThroughTheHandler(b *testing.B) {
	for _, policy := range []struct {
		name string
		sync litekv.SyncPolicy
	}{
		{"never", litekv.SyncNever},
		{"every", litekv.SyncEvery},
		{"always", litekv.SyncAlways},
	} {
		for _, through := range []string{"nothing", "direct", "queued"} {
			b.Run(policy.name+"/"+through, func(b *testing.B) {
				db, err := litekv.OpenDB(b.TempDir(), litekv.DBOptions{
					Sync: policy.sync, SegmentSize: 1 << 30, MergeTrigger: 1 << 30,
				})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				s := New(db, Options{Logger: quiet()})
				defer s.Close()

				// The only difference between the rows. All three go through
				// the same handler, the same routing and the same body reading.
				switch through {
				case "nothing":
					s.writes = nothing{}
				case "direct":
					s.writes = db
				}

				value := strings.Repeat("x", 128)

				b.SetBytes(int64(len(value)))
				b.ReportAllocs()
				b.ResetTimer()

				var counter atomic.Uint64
				b.RunParallel(func(pb *testing.PB) {
					key := make([]byte, 8)
					for pb.Next() {
						binary.LittleEndian.PutUint64(key, counter.Add(1))

						rec := httptest.NewRecorder()
						s.ServeHTTP(rec, httptest.NewRequest(http.MethodPut,
							fmt.Sprintf("/v1/keys/%x", key), strings.NewReader(value)))

						if rec.Code != http.StatusNoContent {
							b.Fatalf("PUT: %d", rec.Code)
						}
					}
				})
			})
		}
	}
}
