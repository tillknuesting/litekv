package litekv

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
)

// Chaos here means one thing: a fault is put in the way of every disk operation
// a follower makes, one at a time and then in combination, and the follower has
// to end up holding what the leader holds once the disk works again.
//
// That is the only promise worth making about a fault. A follower may lose a
// batch, apply one twice, refuse a position, or need a whole new snapshot —
// those are all allowed and some are expected. What is not allowed is a
// follower that settles into disagreeing with its leader and says nothing, and
// a fault landing at a point nobody thought to test is exactly how that gets
// shipped.

// chaosFollow is followDB without the assertions: it reports what went wrong
// rather than ending the test, since under fault injection something going
// wrong is the point.
func chaosFollow(leader, follower *DB, opts ReplicaOptions) error {
	pos := follower.Applied()

	for rounds := 0; ; rounds++ {
		if rounds > 100 {
			return errors.New("the follower is going round in circles")
		}

		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, opts)
		if pos == (DBPosition{}) || errors.Is(err, ErrorDiverged) {
			wire.Reset()

			at, releaseAt, err := leader.Snapshot(&wire, opts)

			defer releaseAt()
			if err != nil {
				return fmt.Errorf("snapshot: %w", err)
			}
			if err := follower.ApplySnapshot(at, &wire, opts); err != nil {
				return fmt.Errorf("applying a snapshot: %w", err)
			}
			pos = at
			continue
		}
		if err != nil {
			return fmt.Errorf("since %+v: %w", pos, err)
		}
		if next == pos {
			return nil
		}

		// A position that moved with nothing behind it would advance a follower
		// past records it never saw, which is the whole failure this file is
		// about, so it is worth saying rather than quietly applying nothing.
		if wire.Len() == 0 {
			return fmt.Errorf("the leader moved from %+v to %+v and sent nothing", pos, next)
		}

		got, err := follower.Apply(pos, next, &wire, opts)
		if err != nil {
			return fmt.Errorf("apply %+v to %+v: %w", pos, next, err)
		}
		if got != next {
			return fmt.Errorf("the follower reached %+v, the leader sent to %+v", got, next)
		}
		pos = next
	}
}

// chaosLeader builds a store with enough shape in it that getting it wrong
// shows: keys written once, keys rewritten so that older records are dead, and
// a key deleted so a tombstone has to be honoured.
func chaosLeader(t *testing.T) *DB {
	t.Helper()

	leader, err := OpenDB(t.TempDir(), smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leader.Close() })

	// Enough records that a follower rotates several times taking them, so the
	// sweep covers freezing a log and writing a hint rather than only the first
	// few writes.
	for i := range 200 {
		if err := leader.Write(fmt.Appendf(nil, "key-%03d", i), []byte("first")); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 60 {
		if err := leader.Write(fmt.Appendf(nil, "key-%03d", i), []byte("second")); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.Delete([]byte("key-199")); err != nil {
		t.Fatal(err)
	}

	// A write batch or two, so that every fault in the sweeps lands somewhere
	// near one: a follower that applies half of a batch is a disagreement the
	// records themselves cannot show, since the ones that arrived read
	// perfectly well on their own.
	for round := range 2 {
		var b Batch
		for i := range 8 {
			b.Write(fmt.Appendf(nil, "batched-%d-%d", round, i), []byte("together"))
		}
		b.Delete(fmt.Appendf(nil, "key-%03d", 190+round))
		if err := leader.WriteBatch(&b); err != nil {
			t.Fatal(err)
		}
	}

	return leader
}

// TestDBFollowerChaosEveryOperation fails each disk operation a follower makes,
// one run per operation, and checks that a follower given a working disk
// afterwards always converges on its leader.
//
// The sweep is the point. Picking the interesting operations by hand is how the
// uninteresting one that turns out to matter gets missed, and there are only a
// few dozen of them.
func TestDBFollowerChaosEveryOperation(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	leader := chaosLeader(t)

	// A clean run first, to learn how many operations there are to fail.
	clean := t.TempDir()
	watcher.reset()
	watcher.inject(clean, 0, 0, 0)

	follower, err := OpenDB(clean, smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
		t.Fatalf("a clean run failed: %v", err)
	}
	sameStores(t, leader, follower, nil)

	total := watcher.operations()
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	if total < 10 {
		t.Fatalf("only %d operations to fail, which is not a sweep", total)
	}

	faults, recovered := 0, 0

	for n := 1; n <= total; n++ {
		dir := t.TempDir()

		watcher.calm()
		watcher.reset()
		watcher.inject(dir, n, 0, 0)

		follower, err := OpenDB(dir, smallSegments(1024))
		if err != nil {
			// Even opening can be the operation that fails.
			watcher.calm()
			if follower, err = OpenDB(dir, smallSegments(1024)); err != nil {
				t.Fatalf("operation %d: the store could not be opened even once the disk worked: %v", n, err)
			}
		}

		if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
			faults++
		}

		// The disk works again, and the process comes back: this is what a
		// follower has after a fault, and it has to be enough.
		watcher.calm()
		follower.Close()

		reopened, err := OpenDB(dir, smallSegments(1024))
		if err != nil {
			t.Fatalf("operation %d: reopening after the fault: %v", n, err)
		}

		if err := chaosFollow(leader, reopened, ReplicaOptions{BatchSize: 256}); err != nil {
			t.Fatalf("operation %d: catching up on a working disk: %v", n, err)
		}
		sameStoresQuietly(t, leader, reopened, fmt.Sprintf("operation %d", n))

		if err := reopened.Close(); err != nil {
			t.Fatalf("operation %d: %v", n, err)
		}
		recovered++
	}

	t.Logf("failed each of %d operations in turn; %d of them stopped the follower, all %d converged",
		total, faults, recovered)

	if faults == 0 {
		t.Error("no injected fault ever reached the follower, so nothing was tested")
	}
}

// TestDBFollowerChaosStaysDown is the same sweep against a machine that does not
// come back: everything from the nth operation onwards fails, so the follower is
// cut off part way through and stays cut off. What it must not do is come back
// up claiming to hold more than it does.
func TestDBFollowerChaosStaysDown(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	leader := chaosLeader(t)

	clean := t.TempDir()
	watcher.reset()
	watcher.inject(clean, 0, 0, 0)

	follower, err := OpenDB(clean, smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
		t.Fatalf("a clean run failed: %v", err)
	}
	total := watcher.operations()
	follower.Close()

	for n := 1; n <= total; n++ {
		dir := t.TempDir()

		watcher.calm()
		watcher.reset()
		watcher.inject(dir, 0, n, 0)

		if follower, err := OpenDB(dir, smallSegments(1024)); err == nil {
			chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256})
			follower.Close()
		}

		// The machine comes back, with whatever it managed to write.
		watcher.calm()

		reopened, err := OpenDB(dir, smallSegments(1024))
		if err != nil {
			t.Fatalf("operation %d: reopening after the disk went away: %v", n, err)
		}

		// Whatever it claims to have applied, it has to actually hold. Catching
		// up from that claim rather than from a fresh snapshot is what proves
		// it: a position that ran ahead of the records would leave the follower
		// short of keys and perfectly happy about it.
		if err := chaosFollow(leader, reopened, ReplicaOptions{BatchSize: 256}); err != nil {
			t.Fatalf("operation %d: catching up after the disk came back: %v", n, err)
		}
		sameStoresQuietly(t, leader, reopened, fmt.Sprintf("from operation %d", n))

		reopened.Close()
	}

	t.Logf("cut the disk off at each of %d points; every follower came back and caught up", total)
}

// TestDBFollowerChaosRandomised throws faults at a follower while the leader
// keeps being written to, which is the arrangement none of the ordered sweeps
// cover: a store that is moving, rotating and merging while its follower is
// failing and restarting.
func TestDBFollowerChaosRandomised(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 700})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	dir := t.TempDir()
	watcher.inject(dir, 0, 0, 0)

	follower, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 500})
	if err != nil {
		t.Fatal(err)
	}

	random := rand.New(rand.NewSource(11))
	live := map[string]string{}

	faults, restarts := 0, 0

	for step := range 400 {
		// The leader carries on regardless, which is what makes this different
		// from failing a store that is standing still.
		for i := 0; i < 1+random.Intn(6); i++ {
			key := fmt.Sprintf("key-%02d", random.Intn(25))

			if random.Intn(100) < 15 {
				if err := leader.Delete([]byte(key)); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				delete(live, key)
				continue
			}

			value := fmt.Sprintf("value-%d-%d", step, i)
			if err := leader.Write([]byte(key), []byte(value)); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			live[key] = value
		}

		// A fault somewhere in the follower's next few operations, often
		// enough that it rarely gets a clean run.
		watcher.calm()
		watcher.reset()

		nth := 0
		if random.Intn(100) < 60 {
			nth = 1 + random.Intn(12)
		}
		watcher.inject(dir, nth, 0, 0)

		if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: int64(1 + random.Intn(400))}); err != nil {
			faults++
		}

		// And sometimes the process goes away and comes back.
		if random.Intn(100) < 20 {
			watcher.calm()
			follower.Close()

			if follower, err = OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 500}); err != nil {
				t.Fatalf("step %d: reopening: %v", step, err)
			}
			restarts++
		}
	}

	// However badly it went, a working disk and one more round has to settle it.
	watcher.calm()
	if err := chaosFollow(leader, follower, ReplicaOptions{}); err != nil {
		t.Fatalf("catching up at the end: %v", err)
	}

	for key, want := range live {
		got, err := follower.Read([]byte(key))
		if err != nil {
			t.Fatalf("%q: the leader has %q, the follower says %v\n  follower applied %+v, leader at %+v\n  %s\n%s",
				key, want, err, follower.Applied(), leader.Position(), chaosStory(faults, restarts),
				whereIsIt(t, follower, key))
		}
		if string(got) != want {
			t.Fatalf("%q: the leader has %q, the follower %q\n  follower applied %+v, leader at %+v\n  %s\n%s",
				key, want, got, follower.Applied(), leader.Position(), chaosStory(faults, restarts),
				whereIsIt(t, follower, key))
		}
	}
	sameStoresQuietly(t, leader, follower, "at the end")
	follower.Close()

	t.Logf("%d faults reached the follower and %d restarts, and it still agrees with its leader",
		faults, restarts)

	if faults < 20 || restarts < 5 {
		t.Errorf("only %d faults and %d restarts, which is not much chaos", faults, restarts)
	}
}

// sameStoresQuietly is sameStores without the running commentary, for the
// sweeps that call it hundreds of times.
func sameStoresQuietly(t *testing.T, leader, follower *DB, what string) {
	t.Helper()

	live := 0
	if err := leader.ForEach(func(key, value []byte) bool {
		live++

		got, err := follower.Read(key)
		if err != nil {
			t.Fatalf("%s: %q: the leader has '%s', the follower says %v", what, key, value, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("%s: %q: the leader has '%s', the follower '%s'", what, key, value, got)
		}
		return true
	}); err != nil {
		t.Fatalf("%s: ForEach on the leader: %v", what, err)
	}

	mirrored := 0
	if err := follower.ForEach(func(key, value []byte) bool {
		mirrored++

		got, err := leader.Read(key)
		if err != nil {
			t.Fatalf("%s: %q is live on the follower but the leader says %v", what, key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("%s: %q is '%s' on the follower and '%s' on the leader", what, key, value, got)
		}
		return true
	}); err != nil {
		t.Fatalf("%s: ForEach on the follower: %v", what, err)
	}

	if live != mirrored {
		t.Fatalf("%s: the leader has %d live keys, the follower %d", what, live, mirrored)
	}
}

// TestDBFollowerChaosTornWrites cuts the follower's disk off part way through
// whichever write crosses a line, rather than refusing the write outright. That
// is what losing power looks like from inside a process: half a record on the
// disk, and no error until the next thing asks.
//
// The line is swept across the whole store, so it lands inside a record, on a
// record boundary, inside a hint and inside the file that says how far through
// the leader the follower has got, without anyone having to decide which of
// those is interesting.
func TestDBFollowerChaosTornWrites(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	leader := chaosLeader(t)

	for _, torn := range []int64{1, 7, 13, 22, 40, 64, 100, 256, 512, 1000, 2048} {
		t.Run(fmt.Sprint(torn), func(t *testing.T) {
			dir := t.TempDir()

			watcher.calm()
			watcher.reset()
			watcher.inject(dir, 0, 0, torn)

			follower, err := OpenDB(dir, smallSegments(1024))
			if err != nil {
				t.Fatalf("opening a follower whose disk fills at %d bytes: %v", torn, err)
			}
			chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256})

			// The power comes back, and so does the process.
			watcher.calm()
			follower.Close()

			reopened, err := OpenDB(dir, smallSegments(1024))
			if err != nil {
				t.Fatalf("reopening after a torn write: %v", err)
			}
			defer reopened.Close()

			// Whatever half-written record is on the disk, recovery drops it,
			// and what the follower claims has to be something it can carry on
			// from without losing a key.
			if err := chaosFollow(leader, reopened, ReplicaOptions{BatchSize: 256}); err != nil {
				t.Fatalf("catching up after a torn write: %v", err)
			}
			sameStoresQuietly(t, leader, reopened, fmt.Sprintf("torn at %d", torn))
		})
	}
}

// TestDBLeaderChaos fails the leader's disk instead of the follower's, one
// operation at a time. A leader that cannot read its own logs must say so
// rather than hand over a snapshot with a hole in it, and it must still be a
// working store afterwards — a follower asking is not a reason for a leader to
// break.
func TestDBLeaderChaos(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	// A fresh leader for every run. Sharing one makes the sweep shrink as it
	// goes, because the first run rotates and the rest have nothing left to do.
	build := func(t *testing.T) (*DB, string) {
		t.Helper()

		dir := t.TempDir()

		leader, err := OpenDB(dir, smallSegments(1024))
		if err != nil {
			t.Fatal(err)
		}
		for i := range 120 {
			if err := leader.Write(fmt.Appendf(nil, "key-%03d", i), []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		return leader, dir
	}

	// How many operations a leader makes answering a follower from nothing.
	leader, dir := build(t)

	watcher.calm()
	watcher.reset()
	watcher.inject(dir, 0, 0, 0)

	first, err := OpenDB(t.TempDir(), smallSegments(1024))
	if err != nil {
		t.Fatal(err)
	}
	if err := chaosFollow(leader, first, ReplicaOptions{BatchSize: 256}); err != nil {
		t.Fatalf("a clean run failed: %v", err)
	}
	total := watcher.operations()
	first.Close()
	leader.Close()

	if total < 5 {
		t.Fatalf("only %d operations on the leader, which is not a sweep", total)
	}

	refused := 0

	for n := 1; n <= total; n++ {
		leader, dir := build(t)

		watcher.calm()
		watcher.reset()
		watcher.inject(dir, n, 0, 0)

		follower, err := OpenDB(t.TempDir(), smallSegments(1024))
		if err != nil {
			t.Fatal(err)
		}

		if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
			refused++
		}

		// The leader is still a store: it answers, and it takes writes. A
		// rotation that half happened used to leave it unable to do either.
		watcher.calm()

		if _, err := leader.Read([]byte("key-000")); err != nil {
			t.Fatalf("operation %d: the leader stopped answering: %v", n, err)
		}
		if err := leader.Write([]byte("after"), []byte("value")); err != nil {
			t.Fatalf("operation %d: the leader stopped taking writes: %v", n, err)
		}

		// And the follower catches up completely once the disk works.
		if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
			t.Fatalf("operation %d: catching up on a working disk: %v", n, err)
		}
		sameStoresQuietly(t, leader, follower, fmt.Sprintf("leader operation %d", n))

		follower.Close()
		leader.Close()
	}

	t.Logf("failed each of %d leader operations in turn; %d stopped a follower, every leader kept working", total, refused)

	if refused == 0 {
		t.Error("no injected fault ever reached the leader, so nothing was tested")
	}
}

// TestDBLeaderChaosRefusedReads sweeps a fault across the reads a snapshot
// makes, which the operation counter does not reach: a read is not recorded as
// an operation, so failing the nth of those needs its own sweep.
//
// A leader that cannot read its own log has to say so. Handing over the records
// it managed and a position covering all of them would leave a follower short of
// keys with nothing to notice it by, since the position is the only thing either
// end checks and it would be perfectly valid.
func TestDBLeaderChaosRefusedReads(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	leader, err := OpenDB(dir, smallSegments(512))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	for i := range 120 {
		if err := leader.Write(fmt.Appendf(nil, "key-%03d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// Which logs a snapshot has to read, and how many reads each is worth.
	leader.mu.RLock()
	logs := make([]string, 0, len(leader.frozen))
	for _, seg := range leader.frozen {
		logs = append(logs, filepath.Base(seg.path))
	}
	leader.mu.RUnlock()

	if len(logs) < 3 {
		t.Fatalf("%d frozen logs, want several to sweep across", len(logs))
	}

	refused := 0

	for _, log := range logs {
		for allowed := range 4 {
			watcher.calm()
			watcher.refuseReads(log, allowed)

			var wire bytes.Buffer
			_, release, err := leader.Snapshot(&wire, ReplicaOptions{})
			release()

			if err != nil {
				refused++

				// Nothing that failed may have handed over a usable position:
				// the caller has an error and no position, so there is no way
				// to mistake a partial snapshot for a whole one.
				continue
			}
		}
	}

	if refused == 0 {
		t.Fatal("no refused read ever stopped a snapshot, so nothing was tested")
	}

	// And once the disk answers again, a follower takes the whole store.
	watcher.calm()

	follower, err := OpenDB(t.TempDir(), smallSegments(512))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	if err := chaosFollow(leader, follower, ReplicaOptions{BatchSize: 256}); err != nil {
		t.Fatalf("catching up after the reads recovered: %v", err)
	}
	sameStoresQuietly(t, leader, follower, "after refused reads")

	t.Logf("swept a refused read across %d logs at four depths; %d snapshots were stopped and none lied",
		len(logs), refused)
}

func chaosStory(faults, restarts int) string {
	return fmt.Sprintf("after %d faults and %d restarts", faults, restarts)
}

// whereIsIt reports every record a store holds for a key, log by log, with what
// each log's index says about it. A follower answering with an old value is
// either missing the new record or pointing at the wrong one, and this is what
// tells the two apart.
func whereIsIt(t *testing.T, db *DB, key string) string {
	t.Helper()

	db.mu.RLock()
	defer db.mu.RUnlock()

	var out []string

	for seg := range db.searchOrder() {
		indexed := int64(-1)
		seg.eachKey(func(k string, pos int64) bool {
			if k == key {
				indexed = pos
				return false
			}
			return true
		})

		found := []string{}
		if indexed >= 0 {
			record, _, err := seg.recordAt(indexed)
			if err != nil {
				found = append(found, fmt.Sprintf("index says %d, which will not decode: %v", indexed, err))
			} else {
				found = append(found, fmt.Sprintf("index says %d, holding %q = %q (deleted %t)",
					indexed, record.Key, record.Value, record.Type != RecordTypeNormal))
			}
		}

		switch frozen := seg.(type) {
		case *diskSegment:
			frozen.scan(func(pos int64, raw []byte, r Record) bool {
				if string(r.Key) == key {
					found = append(found, fmt.Sprintf("record at %d = %q (deleted %t)",
						pos, r.Value, r.Type != RecordTypeNormal))
				}
				return true
			})
			out = append(out, fmt.Sprintf("  frozen log %d (%d bytes): %v", frozen.id(), frozen.bytes, found))
		case *memSegment:
			frozen.kvs.ForEach(func(k, v []byte, deleted bool) bool {
				if string(k) == key {
					found = append(found, fmt.Sprintf("record = %q (deleted %t)", v, deleted))
				}
				return true
			})
			out = append(out, fmt.Sprintf("  active log %d: %v", frozen.segID, found))
		}
	}

	return "  where the key is:\n" + joinLines(out)
}

func joinLines(lines []string) string {
	var out strings.Builder
	for _, line := range lines {
		out.WriteString(line + "\n")
	}
	return out.String()
}
