package litekv

import (
	"fmt"
	"testing"
)

// TestBloomHasNoFalseNegatives is the only property that matters for
// correctness. A filter that says "probably" when it should say "no" costs a
// wasted index lookup; one that says "no" when the key is there costs the
// record, because the caller takes that as ErrorKeyNotFound and stops looking.
func TestBloomHasNoFalseNegatives(t *testing.T) {
	for _, count := range []int{1, 2, 100, 10_000, 200_000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			index := make(map[string]int64, count)
			for i := 0; i < count; i++ {
				index[fmt.Sprintf("key:%016d", i)] = int64(i)
			}

			filter := newBloom(index)
			for key := range index {
				if !filter.mayContain([]byte(key)) {
					t.Fatalf("filter denies %q, which it was built from", key)
				}
			}
		})
	}
}

// TestBloomEmpty checks the degenerate case, since a log can be indexed before
// anything is in it.
func TestBloomEmpty(t *testing.T) {
	filter := newBloom(map[string]int64{})
	if filter.mayContain([]byte("anything")) {
		// Not a correctness failure, but an empty filter that says yes to
		// everything is useless, and would mean the sizing is wrong.
		t.Error("an empty filter claims to hold a key")
	}
}

// TestBloomThreshold checks that a log gets a filter when it is worth one and
// not before, and that the option can force either.
func TestBloomThreshold(t *testing.T) {
	index := make(map[string]int64, 100)
	for i := 0; i < 100; i++ {
		index[fmt.Sprintf("key:%016d", i)] = int64(i)
	}

	tests := []struct {
		name string
		min  int
		want bool
	}{
		{"below the threshold", 1000, false},
		{"exactly at it", 100, true},
		{"above it", 10, true},
		{"disabled", -1, false},
		{"disabled below it too", -1, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := maybeBloom(index, test.min) != nil; got != test.want {
				t.Errorf("maybeBloom over %d keys with a minimum of %d: filter=%t, want %t",
					len(index), test.min, got, test.want)
			}
		})
	}
}

// TestBloomMinKeysOption checks that the option reaches the segments, since a
// threshold nothing consults would be worse than none: the default would look
// right in a test and do nothing in a store.
func TestBloomMinKeysOption(t *testing.T) {
	tests := []struct {
		name string
		min  int
		want bool
	}{
		{"forced on", 1, true},
		{"turned off", -1, false},
		{"default leaves small logs alone", 0, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := OpenDB(t.TempDir(), DBOptions{
				Sync: SyncNever, SegmentSize: 400, MergeTrigger: 1 << 30, BloomMinKeys: test.min,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			// Small logs, so the default declines to filter them and the
			// explicit settings are the only thing that can differ.
			for i := 0; i < 200; i++ {
				if err := db.Write([]byte(fmt.Sprintf("key%04d", i)), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}

			db.mu.RLock()
			defer db.mu.RUnlock()

			if len(db.frozen) == 0 {
				t.Fatal("no frozen log to inspect")
			}
			for _, seg := range db.frozen {
				if got := seg.filter != nil; got != test.want {
					t.Errorf("log %d over %d keys: filter=%t, want %t", seg.id(), len(seg.index), got, test.want)
				}
			}
		})
	}
}

// TestDBWithFiltersAnswersTheSame is the test that matters most, and the reason
// it is differential rather than a list of expected values.
//
// A filter is only ever allowed to make a lookup faster. If it can also make
// one wrong, the way it goes wrong is the worst kind: a key that is present is
// reported missing, silently, and only for the keys whose bits happen to
// collide. Nothing about the store looks broken.
//
// So the same history runs against a store with filters forced on for every log
// and one with them off, and every answer has to match — the live keys, the
// rewritten ones, the deleted ones, and a pile of keys that were never written,
// which is where a filter that is too eager would show up.
func TestDBWithFiltersAnswersTheSame(t *testing.T) {
	build := func(t *testing.T, min int) *DB {
		t.Helper()

		db, err := OpenDB(t.TempDir(), DBOptions{
			Sync: SyncNever, SegmentSize: 2000, MergeTrigger: 1 << 30, BloomMinKeys: min,
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 600; i++ {
			if err := db.Write([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("value%04d", i))); err != nil {
				t.Fatal(err)
			}
		}
		// Rewrites, so a newer log shadows an older one.
		for i := 0; i < 600; i += 3 {
			if err := db.Write([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("rewritten%04d", i))); err != nil {
				t.Fatal(err)
			}
		}
		// Deletes, so tombstones have to be found through the filter too.
		for i := 1; i < 600; i += 7 {
			if err := db.Delete([]byte(fmt.Sprintf("key%04d", i))); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}

	// Every key written, every key deleted, and a wide band that never existed.
	probes := make([]string, 0, 2000)
	for i := 0; i < 600; i++ {
		probes = append(probes, fmt.Sprintf("key%04d", i))
	}
	for i := 0; i < 1400; i++ {
		probes = append(probes, fmt.Sprintf("absent%04d", i))
	}

	answers := func(t *testing.T, db *DB) []string {
		t.Helper()

		out := make([]string, len(probes))
		for i, key := range probes {
			value, err := db.Read([]byte(key))
			switch {
			case err != nil:
				out[i] = "err:" + err.Error()
			default:
				out[i] = "ok:" + string(value)
			}
		}
		return out
	}

	filtered := build(t, 1) // a filter on every log, however small
	defer filtered.Close()
	plain := build(t, -1) // none at all
	defer plain.Close()

	// The premise of the comparison: one really does have filters and the other
	// really does not. Without this the test could pass by testing nothing.
	filtered.mu.RLock()
	withFilters := 0
	for _, seg := range filtered.frozen {
		if seg.filter != nil {
			withFilters++
		}
	}
	total := len(filtered.frozen)
	filtered.mu.RUnlock()

	if total == 0 || withFilters != total {
		t.Fatalf("%d of %d logs have a filter; the comparison needs all of them to", withFilters, total)
	}

	compare := func(t *testing.T, stage string) {
		t.Helper()

		want := answers(t, plain)
		got := answers(t, filtered)
		for i := range probes {
			if got[i] != want[i] {
				t.Errorf("%s: key %q reads %q with filters and %q without", stage, probes[i], got[i], want[i])
			}
		}
	}

	compare(t, "as written")

	// A merge rebuilds the index, so it rebuilds the filter. One built from a
	// stale index would deny keys the merged log holds.
	if err := filtered.Merge(); err != nil {
		t.Fatal(err)
	}
	if err := plain.Merge(); err != nil {
		t.Fatal(err)
	}
	compare(t, "after merging")
}

// TestBloomSurvivesReopen checks the path that builds a filter from a hint file
// rather than from a log just written, which is a different constructor and the
// one every restart takes.
func TestBloomSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	opts := DBOptions{Sync: SyncNever, SegmentSize: 2000, MergeTrigger: 1 << 30, BloomMinKeys: 1}

	db, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		if err := db.Write([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("value%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	reopened.mu.RLock()
	filtered := 0
	for _, seg := range reopened.frozen {
		if seg.filter != nil {
			filtered++
		}
	}
	total := len(reopened.frozen)
	reopened.mu.RUnlock()

	if total == 0 || filtered != total {
		t.Fatalf("after reopening, %d of %d logs have a filter", filtered, total)
	}

	for i := 0; i < 400; i++ {
		key := fmt.Sprintf("key%04d", i)
		value, err := reopened.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s after reopening: %v", key, err)
		}
		if string(value) != fmt.Sprintf("value%04d", i) {
			t.Fatalf("%s reads %q", key, value)
		}
	}
}

// TestBloomFalsePositiveRate measures what the filter actually costs in wasted
// lookups. It is not a correctness test and it does not assert a tight bound,
// since the rate is statistical and this repository does not assert on numbers
// that vary; it fails only if the rate is far enough off that the sizing must
// be wrong.
func TestBloomFalsePositiveRate(t *testing.T) {
	const count = 100_000

	index := make(map[string]int64, count)
	for i := 0; i < count; i++ {
		index[fmt.Sprintf("key:%016d", i)] = int64(i)
	}
	filter := newBloom(index)

	positives := 0
	for i := 0; i < count; i++ {
		if filter.mayContain([]byte(fmt.Sprintf("absent:%016d", i))) {
			positives++
		}
	}

	rate := float64(positives) / count
	t.Logf("false positive rate %.2f%% at %d bits a key over %d probes, filter %d KiB against an index of about %d KiB",
		rate*100, bloomBits, bloomProbes, filter.bytes()/1024, count*59/1024)

	if rate > 0.10 {
		t.Errorf("false positive rate %.2f%% is far above the ~1%% the sizing intends", rate*100)
	}
}
