package litekv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// walkPosition works out where a log ends the slow way, by reading every record
// in it. Position has to agree with this however it arrived at its answer, and
// a test that used Position to check Position would check nothing.
func walkPosition(kvs *KeyValueStore) Position {
	var pos Position

	var at int64
	for at < int64(len(kvs.Data)) {
		record, next, err := parseRecordAt(kvs.Data, at)
		if err != nil {
			break
		}
		pos = Position{Offset: next, Last: at, Crc: record.Crc}
		at = next
	}
	return pos
}

// catchUp sends the leader everything the follower is missing, a batch at a
// time, and reports how many batches that took.
func catchUp(t *testing.T, leader, follower *KeyValueStore) int {
	t.Helper()

	pos := follower.Position()

	for batches := 0; ; batches++ {
		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, ReplicaOptions{})
		if err != nil {
			t.Fatalf("Since(%+v): %v", pos, err)
		}
		if next == pos {
			return batches
		}

		got, err := follower.Apply(pos, &wire, ReplicaOptions{})
		if err != nil {
			t.Fatalf("Apply(%+v): %v", pos, err)
		}
		if got != next {
			t.Fatalf("the follower reached %+v, the leader sent as far as %+v", got, next)
		}
		pos = got
	}
}

// sameStore checks that the follower holds the leader's log, byte for byte, and
// answers as it does.
func sameStore(t *testing.T, leader, follower *KeyValueStore) {
	t.Helper()

	if !bytes.Equal(leader.Data, follower.Data) {
		t.Fatalf("the follower holds %d bytes, the leader %d", len(follower.Data), len(leader.Data))
	}
	if got, want := follower.Position(), leader.Position(); got != want {
		t.Errorf("the follower is at %+v, the leader at %+v", got, want)
	}
	if len(follower.Index) != len(leader.Index) {
		t.Errorf("the follower indexes %d keys, the leader %d", len(follower.Index), len(leader.Index))
	}

	for key := range leader.Index {
		want, wantErr := leader.Read([]byte(key))
		got, gotErr := follower.Read([]byte(key))

		if !errors.Is(gotErr, wantErr) && !errors.Is(wantErr, gotErr) {
			t.Errorf("%q reads as '%v' on the follower and '%v' on the leader", key, gotErr, wantErr)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%q is %q on the follower and %q on the leader", key, got, want)
		}
	}
}

// TestReplicaCatchUp is the whole feature in one test: a follower that holds
// nothing is given a leader's log and ends up holding the same store, and one
// that is already caught up is only sent what happened since.
func TestReplicaCatchUp(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	for i := 0; i < 200; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Updates and a delete, so the log holds more than one record per key and
	// the follower has to end up with the same view of which one wins.
	if err := leader.Write([]byte("key-000"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err := leader.Delete([]byte("key-001")); err != nil {
		t.Fatal(err)
	}

	if batches := catchUp(t, leader, follower); batches == 0 {
		t.Fatal("an empty follower was told it had nothing to catch up on")
	}
	sameStore(t, leader, follower)

	if _, err := follower.Read([]byte("key-001")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("a deleted key reads as '%v' on the follower, want %v", err, ErrorKeyDeleted)
	}

	// Caught up, so there is nothing to send.
	if batches := catchUp(t, leader, follower); batches != 0 {
		t.Errorf("a follower that was up to date took %d batches to say so", batches)
	}

	// And from here only what has happened since crosses.
	before := int64(len(follower.Data))
	if err := leader.Write([]byte("late"), []byte("record")); err != nil {
		t.Fatal(err)
	}

	pos := follower.Position()

	var wire bytes.Buffer
	if _, err := leader.Since(pos, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, want := int64(wire.Len()), int64(len(leader.Data))-before; got != want {
		t.Errorf("catching up on one record sent %d bytes, want %d", got, want)
	}
	if _, err := follower.Apply(pos, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	sameStore(t, leader, follower)
}

// TestPositionTracksTheLog checks Position against a full read of the log after
// everything that moves a log's end, and checks it again with the shortcut
// deliberately wrong, since the shortcut is an optimisation and the answer must
// not depend on it.
func TestPositionTracksTheLog(t *testing.T) {
	steps := []struct {
		name string
		do   func(t *testing.T, kvs *KeyValueStore)
	}{
		{"nothing at all", func(*testing.T, *KeyValueStore) {}},
		{"one write", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.Write([]byte("a"), []byte("1")); err != nil {
				t.Fatal(err)
			}
		}},
		{"a delete", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.Delete([]byte("a")); err != nil {
				t.Fatal(err)
			}
		}},
		{"an update", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.Write([]byte("a"), []byte("2")); err != nil {
				t.Fatal(err)
			}
		}},
		{"compaction", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.Compact(); err != nil {
				t.Fatal(err)
			}
		}},
		{"rebuilding the index", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.RebuildIndex(); err != nil {
				t.Fatal(err)
			}
		}},
		{"recovery", func(t *testing.T, kvs *KeyValueStore) {
			if _, err := kvs.Recover(); err != nil {
				t.Fatal(err)
			}
		}},
		{"a reset", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.Reset(); err != nil {
				t.Fatal(err)
			}
		}},
		{"a write after the reset", func(t *testing.T, kvs *KeyValueStore) {
			if err := kvs.Write([]byte("b"), []byte("3")); err != nil {
				t.Fatal(err)
			}
		}},
	}

	kvs := &KeyValueStore{}
	for _, step := range steps {
		step.do(t, kvs)

		want := walkPosition(kvs)
		if got := kvs.Position(); got != want {
			t.Fatalf("after %s the store is at %+v, the log says %+v", step.name, got, want)
		}

		// The shortcut is where the last record starts. Point it somewhere else
		// and the answer has to come out the same, the long way.
		kvs.lastRecord = 7
		if got := kvs.Position(); got != want {
			t.Fatalf("after %s, with the shortcut wrong, the store is at %+v, want %+v", step.name, got, want)
		}
		kvs.lastRecord = want.Last
	}
}

// TestPositionIgnoresATornTail checks that a store whose log was cut off part
// way through a record reports the end of the last whole record rather than the
// end of the bytes, so that a follower is never sent half of one.
func TestPositionIgnoresATornTail(t *testing.T) {
	kvs := &KeyValueStore{}
	for i := 0; i < 3; i++ {
		if err := kvs.Write([]byte{byte('a' + i)}, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	whole := kvs.Position()

	// All of another record's header and most of its value, which is what a
	// write interrupted by losing power leaves behind.
	kvs.Lock()
	kvs.Data = append(kvs.Data, kvs.Data[whole.Last:whole.Offset-3]...)
	kvs.Unlock()

	if got := kvs.Position(); got != whole {
		t.Errorf("a torn tail moved the position to %+v, want %+v", got, whole)
	}

	// And nothing beyond the last whole record is sent.
	var wire bytes.Buffer
	if _, err := kvs.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	if int64(wire.Len()) != whole.Offset {
		t.Errorf("a store with a torn tail sent %d bytes, want %d", wire.Len(), whole.Offset)
	}
}

// TestReplicaDiverged covers the three ways a follower can be somewhere the
// leader's log has never been. None of them may be answered by streaming from
// the follower's offset, which would splice two histories into one log that
// decodes perfectly and answers wrongly.
func TestReplicaDiverged(t *testing.T) {
	t.Run("a follower with a history of its own", func(t *testing.T) {
		leader := &KeyValueStore{}
		follower := &KeyValueStore{}

		if err := leader.Write([]byte("shared"), []byte("from the leader")); err != nil {
			t.Fatal(err)
		}
		if err := follower.Write([]byte("shared"), []byte("of its own")); err != nil {
			t.Fatal(err)
		}

		if _, err := leader.Since(follower.Position(), io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
			t.Fatalf("a follower with its own records got '%v', want %v", err, ErrorDiverged)
		}

		// Emptying it is the way back, and the only one.
		if err := follower.Reset(); err != nil {
			t.Fatal(err)
		}
		if got := follower.Position(); got != (Position{}) {
			t.Fatalf("a reset store is at %+v, want the zero position", got)
		}
		catchUp(t, leader, follower)
		sameStore(t, leader, follower)
	})

	t.Run("a leader that compacted", func(t *testing.T) {
		leader := &KeyValueStore{}
		follower := &KeyValueStore{}

		for i := 0; i < 20; i++ {
			if err := leader.Write([]byte("one key"), []byte(fmt.Sprintf("version %d", i))); err != nil {
				t.Fatal(err)
			}
		}
		catchUp(t, leader, follower)

		// Nineteen of those twenty records go, so every offset the follower
		// holds is now somewhere else or nowhere.
		if err := leader.Compact(); err != nil {
			t.Fatal(err)
		}

		if _, err := leader.Since(follower.Position(), io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
			t.Fatalf("a follower of a compacted leader got '%v', want %v", err, ErrorDiverged)
		}

		if err := follower.Reset(); err != nil {
			t.Fatal(err)
		}
		catchUp(t, leader, follower)
		sameStore(t, leader, follower)
	})

	t.Run("a follower that is ahead", func(t *testing.T) {
		leader := &KeyValueStore{}
		follower := &KeyValueStore{}

		if err := leader.Write([]byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		catchUp(t, leader, follower)

		// A write of its own, which is what a follower must never take.
		if err := follower.Write([]byte("b"), []byte("2")); err != nil {
			t.Fatal(err)
		}

		if _, err := leader.Since(follower.Position(), io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
			t.Fatalf("a follower ahead of its leader got '%v', want %v", err, ErrorDiverged)
		}
	})

	t.Run("a position that names a record the leader never wrote", func(t *testing.T) {
		leader := &KeyValueStore{}
		if err := leader.Write([]byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}

		// The right offsets, the wrong record.
		bad := leader.Position()
		bad.Crc++

		if _, err := leader.Since(bad, io.Discard, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
			t.Fatalf("a position with the wrong checksum got '%v', want %v", err, ErrorDiverged)
		}
	})
}

// TestReplicaRejectsDamagedBatch checks that a follower verifies what it is
// sent rather than trusting it. A leader is not a reason to trust the wire in
// between, and a record kept without checking is one no later read can question.
func TestReplicaRejectsDamagedBatch(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 5; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	if _, err := leader.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	batch := wire.Bytes()

	// Damage the value of the third record, which is where the follower has to
	// stop: without a marker to resynchronise on there is no next record to
	// carry on from.
	third, _, err := offsetOfRecord(batch, 2)
	if err != nil {
		t.Fatal(err)
	}
	batch[int(third)+headerSizeV1+len("key-2")] ^= 0xff

	follower := &KeyValueStore{}
	pos, err := follower.Apply(Position{}, bytes.NewReader(batch), ReplicaOptions{})
	if !errors.Is(err, ErrorChecksumMismatch) {
		t.Fatalf("a damaged batch applied with '%v', want %v", err, ErrorChecksumMismatch)
	}
	if pos.Offset != third {
		t.Errorf("the follower stopped at %d, want the start of the damaged record at %d", pos.Offset, third)
	}
	if got := walkPosition(follower); got != pos {
		t.Errorf("Apply reported %+v, the log says %+v", pos, got)
	}
	if err := follower.Verify(); err != nil {
		t.Errorf("the follower kept something that does not verify: %v", err)
	}
	if _, err := follower.Read([]byte("key-2")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("the damaged record was kept: %v", err)
	}
}

// TestReplicaTruncatedBatch checks that a batch that ends part way through a
// record keeps the whole records before it, reports where it got to, and can be
// carried on from. A connection that drops is exactly this.
func TestReplicaTruncatedBatch(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 5; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	if _, err := leader.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	batch := wire.Bytes()

	fourth, _, err := offsetOfRecord(batch, 3)
	if err != nil {
		t.Fatal(err)
	}

	follower := &KeyValueStore{}
	pos, err := follower.Apply(Position{}, bytes.NewReader(batch[:fourth+6]), ReplicaOptions{})

	var corrupt *CorruptAtError
	if !errors.As(err, &corrupt) {
		t.Fatalf("half a record applied with '%v', want a *CorruptAtError", err)
	}
	if pos.Offset != fourth {
		t.Errorf("the follower stopped at %d, want %d", pos.Offset, fourth)
	}
	if err := follower.Verify(); err != nil {
		t.Errorf("the follower kept something that does not verify: %v", err)
	}

	// Asking again from where it got to finishes the job.
	catchUp(t, leader, follower)
	sameStore(t, leader, follower)
}

// TestReplicaWrongPosition checks that a batch is only applied to the log it
// was cut for. A batch that arrives twice, or arrives after something else has
// written, describes a log the store is no longer holding.
func TestReplicaWrongPosition(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 3; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	var wire bytes.Buffer
	next, err := leader.Since(Position{}, &wire, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	batch := wire.Bytes()

	follower := &KeyValueStore{}
	if _, err := follower.Apply(Position{}, bytes.NewReader(batch), ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	// The same batch again. It applied once, and applying it twice would put
	// every record in the log a second time.
	pos, err := follower.Apply(Position{}, bytes.NewReader(batch), ReplicaOptions{})
	if !errors.Is(err, ErrorPosition) {
		t.Fatalf("a batch that arrived twice applied with '%v', want %v", err, ErrorPosition)
	}
	if pos != next {
		t.Errorf("the refusal reported %+v, want the position the store is at, %+v", pos, next)
	}
	if got := walkPosition(follower); got != next {
		t.Errorf("the batch was applied anyway: the log is at %+v", got)
	}

	// A write of its own moves the store somewhere no batch was cut for, which
	// is what keeps a follower from quietly being written to.
	if err := follower.Write([]byte("local"), []byte("write")); err != nil {
		t.Fatal(err)
	}
	if err := leader.Write([]byte("key-9"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	wire.Reset()
	if _, err := leader.Since(next, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := follower.Apply(next, &wire, ReplicaOptions{}); !errors.Is(err, ErrorPosition) {
		t.Fatalf("a batch applied over a local write with '%v', want %v", err, ErrorPosition)
	}
}

// TestReplicaBatchIsBounded checks that a large log crosses in bounded pieces
// and that every piece ends on a record boundary, and that a record larger than
// a batch still goes: a log holding one could otherwise never be replicated.
func TestReplicaBatchIsBounded(t *testing.T) {
	// Far smaller than the default, which keeps the test quick and checks that
	// the size is a setting rather than a constant with a knob beside it.
	opts := ReplicaOptions{BatchSize: 8 << 10}

	leader := &KeyValueStore{}

	value := bytes.Repeat([]byte("x"), 512)
	for i := 0; leader.Size() < 3*opts.BatchSize; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%06d", i)), value); err != nil {
			t.Fatal(err)
		}
	}

	follower := &KeyValueStore{}
	pos := follower.Position()
	batches := 0

	for {
		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, opts)
		if err != nil {
			t.Fatal(err)
		}
		if next == pos {
			break
		}
		batches++

		if int64(wire.Len()) > opts.BatchSize {
			t.Fatalf("batch %d was %d bytes, more than the %d it may be", batches, wire.Len(), opts.BatchSize)
		}
		if pos, err = follower.Apply(pos, &wire, opts); err != nil {
			t.Fatalf("batch %d: %v", batches, err)
		}
	}

	if batches < 3 {
		t.Errorf("%d batches for %d bytes, expected at least 3", batches, leader.Size())
	}
	sameStore(t, leader, follower)

	// One record larger than a whole batch.
	if err := leader.Write([]byte("huge"), bytes.Repeat([]byte("y"), int(opts.BatchSize)+1)); err != nil {
		t.Fatal(err)
	}

	for {
		var wire bytes.Buffer

		next, err := leader.Since(pos, &wire, opts)
		if err != nil {
			t.Fatal(err)
		}
		if next == pos {
			break
		}
		if pos, err = follower.Apply(pos, &wire, opts); err != nil {
			t.Fatal(err)
		}
	}
	sameStore(t, leader, follower)
}

// TestReplicaFollowerKeepsAFile checks that a follower with a log of its own is
// a store like any other afterwards: closed, reopened, and still where it was.
func TestReplicaFollowerKeepsAFile(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 50; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(t.TempDir(), "follower.kv")

	follower, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	catchUp(t, leader, follower)
	sameStore(t, leader, follower)

	was := follower.Position()
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Position(); got != was {
		t.Errorf("a reopened follower is at %+v, want %+v", got, was)
	}
	sameStore(t, leader, reopened)

	// And it carries on from there rather than starting again.
	if err := leader.Write([]byte("after"), []byte("the restart")); err != nil {
		t.Fatal(err)
	}
	catchUp(t, leader, reopened)
	sameStore(t, leader, reopened)
}

// TestReplicaAppliesABatchAtOnce checks that a batch reaches the disk in one
// write and one sync rather than one of each per record. A follower under
// SyncAlways would otherwise be paying for the leader's whole history a record
// at a time.
func TestReplicaAppliesABatchAtOnce(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 50; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%02d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	watcher := &watchedDisk{}
	watcher.install(t)

	follower, err := Open(filepath.Join(t.TempDir(), "follower.kv"), Options{Sync: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	watcher.reset()
	if batches := catchUp(t, leader, follower); batches != 1 {
		t.Fatalf("50 records took %d batches, want 1", batches)
	}

	if got := watcher.count("write", "follower.kv"); got != 1 {
		t.Errorf("a batch of 50 records took %d writes, want 1", got)
	}
	if got := watcher.count("sync", "follower.kv"); got != 1 {
		t.Errorf("a batch of 50 records took %d syncs, want 1", got)
	}
}

// TestReplicaLogFailureLeavesFollowerAlone checks that a batch the follower's
// own log refuses is not half applied: the same invariant a write of its own
// has, since Apply appends through the same path.
func TestReplicaLogFailureLeavesFollowerAlone(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 5; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	log := &memLog{}
	follower := &KeyValueStore{}
	if err := follower.Attach(log, Options{Sync: SyncNever}); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	if _, err := leader.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	log.failing = true
	pos, err := follower.Apply(Position{}, &wire, ReplicaOptions{})
	if !errors.Is(err, errLogFull) {
		t.Fatalf("a batch the log refused applied with '%v', want the log's error", err)
	}
	if pos != (Position{}) {
		t.Errorf("a refused batch moved the store to %+v", pos)
	}
	if len(follower.Data) != 0 || len(follower.Index) != 0 {
		t.Errorf("a refused batch left %d bytes and %d keys behind", len(follower.Data), len(follower.Index))
	}

	// And it works once the log does.
	log.failing = false
	catchUp(t, leader, follower)
	sameStore(t, leader, follower)

	if !bytes.Equal(log.contents(), follower.Data) {
		t.Error("the follower's log and its Data disagree")
	}
}

// TestReplicaApplyOnAClosedStore checks that a closed follower refuses a batch
// rather than taking it into a log nothing is writing.
func TestReplicaApplyOnAClosedStore(t *testing.T) {
	leader := &KeyValueStore{}
	if err := leader.Write([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	follower, err := Open(filepath.Join(t.TempDir(), "follower.kv"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	if _, err := leader.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := follower.Apply(Position{}, &wire, ReplicaOptions{}); !errors.Is(err, ErrorClosed) {
		t.Fatalf("a closed follower took a batch with '%v', want %v", err, ErrorClosed)
	}
}

// TestReplicaFollowsAsItHappens is replication as it would actually be run: a
// follower waiting on Changed and taking records as they are written, rather
// than asking on a timer.
func TestReplicaFollowsAsItHappens(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	const records = 500

	// A pipe standing in for a connection. It is unbuffered, so what the
	// follower applies together is exactly what the leader wrote together,
	// which is the batching a real socket does as well.
	reader, writer := io.Pipe()
	done := make(chan struct{})

	var streaming sync.WaitGroup
	streaming.Add(2)

	// The leader, which writes records as they land and does not stop.
	go func() {
		defer streaming.Done()

		_, err := leader.Follow(Position{}, writer, done, ReplicaOptions{})
		writer.CloseWithError(err)
	}()

	// The follower, which applies them as they arrive and stops when the
	// connection does.
	go func() {
		defer streaming.Done()

		if _, err := follower.Apply(Position{}, reader, ReplicaOptions{}); err != nil {
			t.Errorf("Apply: %v", err)
		}
	}()

	for i := 0; i < records; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// The follower is asynchronous, so it arrives when it arrives. This is the
	// replication lag every asynchronous replica has, in miniature, and there
	// is nothing to assert about how long it takes.
	deadline := time.Now().Add(10 * time.Second)
	for follower.Size() < leader.Size() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	close(done)
	streaming.Wait()

	sameStore(t, leader, follower)
}

// TestReplicaStreamArrivesWithoutAsking is what makes a stream a stream: a
// record reaches the follower because the leader wrote one, not because the
// follower asked again. A follower that only received on request would sit here
// holding nothing.
func TestReplicaStreamArrivesWithoutAsking(t *testing.T) {
	leader := &KeyValueStore{}
	follower := &KeyValueStore{}

	reader, writer := io.Pipe()
	done := make(chan struct{})

	var streaming sync.WaitGroup
	streaming.Add(2)

	go func() {
		defer streaming.Done()

		_, err := leader.Follow(Position{}, writer, done, ReplicaOptions{})
		writer.CloseWithError(err)
	}()
	go func() {
		defer streaming.Done()

		if _, err := follower.Apply(Position{}, reader, ReplicaOptions{}); err != nil {
			t.Errorf("Apply: %v", err)
		}
	}()

	// One record at a time, with nothing sent from the follower in between.
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := leader.Write(key, []byte("value")); err != nil {
			t.Fatal(err)
		}

		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := follower.Read(key); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never reached the follower", key)
			}
			time.Sleep(time.Millisecond)
		}

		// And it arrived as a record, not as bytes: the follower knows where it
		// is without being told.
		if got, want := follower.Position(), leader.Position(); got != want {
			t.Errorf("after %s the follower is at %+v, the leader at %+v", key, got, want)
		}
	}

	close(done)
	streaming.Wait()
}

// TestReplicaFollowChecksThePositionFirst checks that a stream refuses to start
// from a position the leader's log has never been at, rather than beginning to
// send and finding out later.
func TestReplicaFollowChecksThePositionFirst(t *testing.T) {
	leader := &KeyValueStore{}
	if err := leader.Write([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	bad := leader.Position()
	bad.Crc++

	if _, err := leader.Follow(bad, io.Discard, nil, ReplicaOptions{}); !errors.Is(err, ErrorDiverged) {
		t.Fatalf("a stream from a diverged position got '%v', want %v", err, ErrorDiverged)
	}
}

// TestReplicaFollowStopsWhenTold checks that a stream ends on its channel and
// gives back the position it had sent as far as, so that a leader-side handler
// can shut down without leaving a goroutine behind.
func TestReplicaFollowStopsWhenTold(t *testing.T) {
	leader := &KeyValueStore{}
	for i := 0; i < 10; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	var sent Position
	var err error

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		sent, err = leader.Follow(Position{}, io.Discard, done, ReplicaOptions{})
	}()

	// It sends what is there and then waits, so closing the channel is the only
	// thing that ends it.
	close(done)

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("a stream did not stop when its channel closed")
	}

	if err != nil {
		t.Fatalf("a stream that was stopped reported %v", err)
	}
	if want := leader.Position(); sent != want && sent != (Position{}) {
		t.Errorf("a stopped stream had sent as far as %+v, want %+v or nothing", sent, want)
	}
}

// TestChangedWakesEveryWaiter checks that the channel is closed rather than
// sent on, so that any number of followers wake from the same write.
func TestChangedWakesEveryWaiter(t *testing.T) {
	kvs := &KeyValueStore{}

	var waiting sync.WaitGroup
	for i := 0; i < 8; i++ {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			<-kvs.Changed()
		}()
	}

	// Whether a waiter has reached the channel yet or not, the write either
	// wakes it or leaves a closed channel behind for it to find.
	woken := make(chan struct{})
	go func() {
		waiting.Wait()
		close(woken)
	}()

	for {
		if err := kvs.Write([]byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-woken:
			return
		case <-time.After(time.Millisecond):
		}
	}
}

// TestChangedCoversEveryChangeToTheLog checks that a follower is woken by
// anything that moves the log, not only by a write. Compaction moves every
// record, and a follower asleep through one would wait for a write that may
// never come.
func TestChangedCoversEveryChangeToTheLog(t *testing.T) {
	changes := []struct {
		name string
		do   func(kvs *KeyValueStore) error
	}{
		{"a write", func(kvs *KeyValueStore) error { return kvs.Write([]byte("a"), []byte("1")) }},
		{"a delete", func(kvs *KeyValueStore) error { return kvs.Delete([]byte("a")) }},
		{"compaction", (*KeyValueStore).Compact},
		{"a reset", (*KeyValueStore).Reset},
		{"recovery", func(kvs *KeyValueStore) error { _, err := kvs.Recover(); return err }},
		{"rebuilding the index", (*KeyValueStore).RebuildIndex},
		{"a batch applied", func(kvs *KeyValueStore) error {
			leader := &KeyValueStore{}
			if err := leader.Write([]byte("z"), []byte("9")); err != nil {
				return err
			}
			var wire bytes.Buffer
			if _, err := leader.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
				return err
			}
			_, err := kvs.Apply(kvs.Position(), &wire, ReplicaOptions{})
			return err
		}},
	}

	for _, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			kvs := &KeyValueStore{}
			changed := kvs.Changed()

			if err := change.do(kvs); err != nil {
				t.Fatal(err)
			}

			select {
			case <-changed:
			default:
				t.Errorf("%s did not wake a follower", change.name)
			}
		})
	}
}

// TestPositionBinary checks the twenty bytes a position crosses a connection
// as, and that one arriving from somewhere else is checked rather than
// believed.
func TestPositionBinary(t *testing.T) {
	kvs := &KeyValueStore{}
	for i := 0; i < 3; i++ {
		if err := kvs.Write([]byte{byte('a' + i)}, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range []Position{{}, kvs.Position()} {
		encoded, err := want.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != positionSize {
			t.Errorf("a position encoded to %d bytes, want %d", len(encoded), positionSize)
		}

		var got Position
		if err := got.UnmarshalBinary(encoded); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("a position came back as %+v, want %+v", got, want)
		}
	}

	bad := []struct {
		name string
		data []byte
	}{
		{"nothing at all", nil},
		{"one byte short", make([]byte, positionSize-1)},
		{"one byte long", make([]byte, positionSize+1)},
		{"a negative offset", mustMarshal(t, Position{Offset: -1})},
		{"a negative last record", mustMarshal(t, Position{Offset: 40, Last: -1})},
		{"a last record at the end of the log", mustMarshal(t, Position{Offset: 40, Last: 40})},
		{"a last record past the end of the log", mustMarshal(t, Position{Offset: 40, Last: 41})},
	}

	for _, test := range bad {
		t.Run(test.name, func(t *testing.T) {
			var got Position
			if err := got.UnmarshalBinary(test.data); err == nil {
				t.Errorf("%s was accepted as %+v", test.name, got)
			}
		})
	}
}

func mustMarshal(t *testing.T, pos Position) []byte {
	t.Helper()

	encoded, err := pos.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// offsetOfRecord returns where the nth record of a log starts and where it
// ends, for tests that want to damage one in particular.
func offsetOfRecord(data []byte, n int) (int64, int64, error) {
	var at int64
	for i := 0; ; i++ {
		_, next, err := parseRecordAt(data, at)
		if err != nil {
			return 0, 0, err
		}
		if i == n {
			return at, next, nil
		}
		at = next
	}
}

// BenchmarkReplicaSteady is a follower keeping up: one record written, and the
// round of Since and Apply that carries it across. It is the cost replication
// adds to a store that is already being written, with the connection taken out
// of it, so what it measures is this package and not a network.
func BenchmarkReplicaSteady(b *testing.B) {
	for _, size := range []int{16, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			key := []byte("key:0000000000000000")
			value := make([]byte, size)

			leader := &KeyValueStore{}
			follower := &KeyValueStore{}
			pos := Position{}

			var wire bytes.Buffer

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := leader.Write(key, value); err != nil {
					b.Fatal(err)
				}

				wire.Reset()
				if _, err := leader.Since(pos, &wire, ReplicaOptions{}); err != nil {
					b.Fatal(err)
				}
				var err error
				if pos, err = follower.Apply(pos, &wire, ReplicaOptions{}); err != nil {
					b.Fatal(err)
				}

				// Both logs would otherwise grow for the whole run, and what
				// would be measured is append.
				if len(leader.Data) > 1<<26 {
					b.StopTimer()
					if err := leader.Reset(); err != nil {
						b.Fatal(err)
					}
					if err := follower.Reset(); err != nil {
						b.Fatal(err)
					}
					pos = Position{}
					b.StartTimer()
				}
			}
		})
	}
}

// BenchmarkWriteWithAWaiter is what following a store costs the store. Changed
// takes the write lock to hand out its channel, so a follower asking whether
// there is more to send stands in the same queue the writers do, and this is the
// worst case for that: a waiter that wakes on every single record and goes
// straight back to asking.
func BenchmarkWriteWithAWaiter(b *testing.B) {
	for _, waiting := range []bool{false, true} {
		name := "alone"
		if waiting {
			name = "followed"
		}

		b.Run(name, func(b *testing.B) {
			kvs := &KeyValueStore{}
			key := []byte("key:0000000000000000")
			value := make([]byte, 16)

			stop := make(chan struct{})
			var waiter sync.WaitGroup

			if waiting {
				waiter.Add(1)
				go func() {
					defer waiter.Done()

					for {
						changed := kvs.Changed()
						select {
						case <-changed:
						case <-stop:
							return
						}
					}
				}()
			}

			b.SetBytes(16)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				kvs.Write(key, value)
				if len(kvs.Data) > 1<<26 {
					kvs.Data = kvs.Data[:0]
				}
			}

			b.StopTimer()
			close(stop)
			waiter.Wait()
		})
	}
}

// BenchmarkReplicaCatchUp is a follower that holds nothing being handed a whole
// log, which is what setting one up costs and what a follower that has diverged
// pays to come back. The bytes per second are the useful number: a connection
// slower than this is what decides how long it takes, and one faster is not.
func BenchmarkReplicaCatchUp(b *testing.B) {
	value := make([]byte, 1024)

	leader := &KeyValueStore{}
	for i := 0; leader.Size() < 8<<20; i++ {
		if err := leader.Write([]byte(fmt.Sprintf("key-%08d", i)), value); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(leader.Size())
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		follower := &KeyValueStore{}
		pos := Position{}

		var wire bytes.Buffer
		for {
			wire.Reset()

			next, err := leader.Since(pos, &wire, ReplicaOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if next == pos {
				break
			}
			if pos, err = follower.Apply(pos, &wire, ReplicaOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// FuzzApply feeds arbitrary bytes to a follower as though a leader had sent
// them. What arrives over a connection is not a log this package wrote, and a
// follower that keeps a record without checking it has no later chance to.
//
// Refusing is always allowed. What is not allowed is a store that afterwards
// fails to verify, or reports a position its own log does not agree with, since
// everything that follows is built on both.
func FuzzApply(f *testing.F) {
	leader := &KeyValueStore{}
	leader.Write([]byte("alpha"), []byte("one"))
	leader.Write([]byte("beta"), []byte("two"))
	leader.Delete([]byte("alpha"))

	var wire bytes.Buffer
	if _, err := leader.Since(Position{}, &wire, ReplicaOptions{}); err != nil {
		f.Fatal(err)
	}
	whole := wire.Bytes()

	f.Add(whole, true)
	f.Add(whole[:10], true)
	f.Add([]byte{}, false)
	f.Add(make([]byte, headerSizeV1), false)
	f.Add([]byte("not a record at all"), false)

	f.Fuzz(func(t *testing.T, batch []byte, seeded bool) {
		follower := &KeyValueStore{}
		if seeded {
			if err := follower.Write([]byte("already"), []byte("here")); err != nil {
				t.Fatal(err)
			}
		}

		before := follower.Position()
		after, _ := follower.Apply(before, bytes.NewReader(batch), ReplicaOptions{})

		if err := follower.Verify(); err != nil {
			t.Fatalf("a store that took %d bytes no longer verifies: %v", len(batch), err)
		}
		if got := walkPosition(follower); got != after {
			t.Fatalf("Apply reported %+v, the log says %+v", after, got)
		}
		if got := follower.Position(); got != after {
			t.Fatalf("the store is at %+v, Apply reported %+v", got, after)
		}
		if after.Offset < before.Offset {
			t.Fatalf("the log went backwards, from %+v to %+v", before, after)
		}
	})
}
