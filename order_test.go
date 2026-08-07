package litekv

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A range is the one question this store's index cannot answer directly: it is
// a hash map, so the keys have no order. What these hold is that asking the
// keys gives the same answer an ordered index would — every live key in the
// range, in order, the newest version of each, across however many logs a DB
// has spread them over.

// collect runs a range over a store and returns what it yielded, as "key=value".
func collect(t *testing.T, run func(fn func(key, value []byte) bool) error) []string {
	t.Helper()

	var got, keys []string
	if err := run(func(key, value []byte) bool {
		keys = append(keys, string(key))
		got = append(got, fmt.Sprintf("%s=%s", key, value))
		return true
	}); err != nil {
		t.Fatalf("range: %v", err)
	}

	// In order, which is the whole claim, so it is checked here rather than in
	// every test that calls this. The keys are what is ordered, not the pairs:
	// "user:2" comes before "user:20" while "user:2=v" does not.
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("out of order: %q before %q in %v", keys[i-1], keys[i], keys)
		}
	}
	return got
}

func same(t *testing.T, got []string, want ...string) {
	t.Helper()

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRangeOverAKeyValueStore(t *testing.T) {
	kvs := &KeyValueStore{}

	for _, key := range []string{"b", "d", "a", "c", "e"} {
		if err := kvs.Write([]byte(key), []byte("v-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	// A key written twice is visited once, at its newest value.
	if err := kvs.Write([]byte("c"), []byte("v-c2")); err != nil {
		t.Fatal(err)
	}
	// A deleted key and an expired one are not visited at all.
	if err := kvs.Delete([]byte("d")); err != nil {
		t.Fatal(err)
	}
	if err := kvs.WriteExpiring([]byte("e"), []byte("v-e"), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range(nil, nil, fn) }),
		"a=v-a", "b=v-b", "c=v-c2")

	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range([]byte("b"), []byte("c"), fn) }),
		"b=v-b")

	// from is included and to is not.
	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range([]byte("a"), []byte("c"), fn) }),
		"a=v-a", "b=v-b")

	// A bound of nil on either side runs to the end of the keys.
	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range([]byte("b"), nil, fn) }),
		"b=v-b", "c=v-c2")
	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range(nil, []byte("b"), fn) }),
		"a=v-a")

	// An empty range yields nothing, and neither does a backwards one.
	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range([]byte("x"), nil, fn) }))
	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Range([]byte("c"), []byte("a"), fn) }))
}

func TestPrefixBounds(t *testing.T) {
	kvs := &KeyValueStore{}

	for _, key := range []string{"user:1", "user:2", "user:20", "users", "user", "usg", "a"} {
		if err := kvs.Write([]byte(key), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Prefix([]byte("user:"), fn) }),
		"user:1=v", "user:2=v", "user:20=v")

	// The prefix itself counts as beginning with itself, and "users" does too.
	same(t, collect(t, func(fn func(k, v []byte) bool) error { return kvs.Prefix([]byte("user"), fn) }),
		"user=v", "user:1=v", "user:2=v", "user:20=v", "users=v")

	// An empty prefix is every key.
	if got := collect(t, func(fn func(k, v []byte) bool) error { return kvs.Prefix(nil, fn) }); len(got) != 7 {
		t.Errorf("an empty prefix yielded %d keys, want 7: %v", len(got), got)
	}

	// A prefix of nothing but 0xff has no key after it, which is the case that
	// makes prefixEnd return nil rather than overflowing a byte.
	high := &KeyValueStore{}
	for _, key := range []string{"\xff", "\xff\xff", "\xff\x00", "\xfe"} {
		if err := high.Write([]byte(key), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if got := collect(t, func(fn func(k, v []byte) bool) error { return high.Prefix([]byte("\xff"), fn) }); len(got) != 3 {
		t.Errorf("a 0xff prefix yielded %d keys, want 3: %v", len(got), got)
	}
}

// TestRangeAcrossADBsLogs is the part a single store cannot test: the keys are
// spread over several logs, each with an index of its own, and the newest log
// to hold a key is the one that answers for it.
func TestRangeAcrossADBsLogs(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Written out of order and spread over several logs by rotation.
	for i := 30; i >= 0; i-- {
		if err := db.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("first-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if db.Segments() < 3 {
		t.Fatalf("the store is in %d logs; the test needs several", db.Segments())
	}

	// Updates and a delete in a newer log, which have to win over the older one.
	if err := db.Write([]byte("key-05"), []byte("second-05")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("key-06")); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteExpiring([]byte("key-07"), []byte("gone"), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	got := collect(t, func(fn func(k, v []byte) bool) error { return db.Range([]byte("key-03"), []byte("key-09"), fn) })
	same(t, got, "key-03=first-03", "key-04=first-04", "key-05=second-05", "key-08=first-08")

	// Everything, in order, and the count agrees with the store's own idea of
	// how many keys it holds once the deleted and expired ones are taken off.
	all := collect(t, func(fn func(k, v []byte) bool) error { return db.Range(nil, nil, fn) })
	if len(all) != 29 {
		t.Errorf("a range over everything yielded %d keys, want 29", len(all))
	}

	// And the same answers once the logs have been merged into one.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	merged := collect(t, func(fn func(k, v []byte) bool) error { return db.Range([]byte("key-03"), []byte("key-09"), fn) })
	same(t, merged, got...)
}

// TestRangeStopsWhenTold checks the early return, which is what makes a range
// over a large store affordable to a caller who wants the first few of them.
func TestRangeStopsWhenTold(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 40; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	if err := db.Range(nil, nil, func(key, value []byte) bool {
		got = append(got, string(key))
		return len(got) < 3
	}); err != nil {
		t.Fatal(err)
	}

	same(t, got, "key-00", "key-01", "key-02")
}

// TestRangeSeesTheSortedKeysOnce holds the one cached thing here: a frozen
// log's keys are sorted the first time a range asks and kept afterwards, which
// is only safe because that index never changes again.
func TestRangeSeesTheSortedKeysOnce(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	db.mu.RLock()
	frozen := append([]*diskSegment(nil), db.frozen...)
	db.mu.RUnlock()

	if len(frozen) == 0 {
		t.Fatal("the store never rotated; the test needs a frozen log")
	}
	for _, seg := range frozen {
		if seg.keys != nil {
			t.Errorf("log %d sorted its keys before anybody asked for a range", seg.id())
		}
	}

	collect(t, func(fn func(k, v []byte) bool) error { return db.Range(nil, nil, fn) })

	for _, seg := range frozen {
		if len(seg.keys) != len(seg.index) {
			t.Errorf("log %d kept %d sorted keys for an index of %d", seg.id(), len(seg.keys), len(seg.index))
		}
		for i := 1; i < len(seg.keys); i++ {
			if seg.keys[i-1] >= seg.keys[i] {
				t.Fatalf("log %d kept its keys out of order at %d", seg.id(), i)
			}
		}
	}

	// A second range over the same logs answers the same, from the kept keys.
	first := collect(t, func(fn func(k, v []byte) bool) error { return db.Range([]byte("key-05"), []byte("key-08"), fn) })
	second := collect(t, func(fn func(k, v []byte) bool) error { return db.Range([]byte("key-05"), []byte("key-08"), fn) })
	same(t, second, first...)
	same(t, first, "key-05=value", "key-06=value", "key-07=value")
}

func TestRangeOnAClosedDB(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(4096))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Range(nil, nil, func(key, value []byte) bool { return true }); err != ErrorClosed {
		t.Errorf("a range over a closed store reported '%v', want %v", err, ErrorClosed)
	}
}

// BenchmarkRange is what a range costs against what it replaces, which for a
// store without one is walking every record.
func BenchmarkRange(b *testing.B) {
	db, err := OpenDB(b.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 1 << 20, MergeTrigger: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// A hundred thousand keys over a good few logs, and a prefix matching a
	// hundred of them: the shape a server answering ?prefix= actually has.
	value := make([]byte, 64)
	for i := 0; i < 100_000; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key-%06d", i)), value); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("prefix of 100", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			found := 0
			if err := db.Prefix([]byte("key-0001"), func(key, value []byte) bool {
				found++
				return true
			}); err != nil {
				b.Fatal(err)
			}
			if found != 100 {
				b.Fatalf("found %d keys, want 100", found)
			}
		}
	})

	b.Run("the same by walking everything", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			found := 0
			if err := db.ForEach(func(key, value []byte) bool {
				if strings.HasPrefix(string(key), "key-0001") {
					found++
				}
				return true
			}); err != nil {
				b.Fatal(err)
			}
			if found != 100 {
				b.Fatalf("found %d keys, want 100", found)
			}
		}
	})
}
