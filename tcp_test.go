package litekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Everything else in this package moves records through a bytes.Buffer or an
// io.Pipe, which is enough to check what the records are but not enough to
// check that the library can be put on a wire at all. A pipe never returns a
// short read, never splits a write across two calls, and never goes away in the
// middle of one.
//
// So this is the same replication over a real TCP connection on the loopback
// interface, with a length in front of every frame, reads that have to be
// filled with io.ReadFull, and a connection that is broken on purpose part way
// through and reconnected. Nothing here is part of the library — the framing
// below is exactly what the library says is the caller's job — but it is the
// only place that says the arrangement works outside a test's imagination.

// The frames. A kind, the position the payload leads to, and a length in front
// of the payload, which is the least a transport can get away with: a record
// stream is self-framing but a reader still has to know where one batch stops
// and the next begins.
const (
	frameSnapshot = 'S'
	frameBatch    = 'B'

	frameHeader = 1 + dbPositionSize + 8
)

func writeFrame(w io.Writer, kind byte, at DBPosition, payload []byte) error {
	encoded, err := at.MarshalBinary()
	if err != nil {
		return err
	}

	var header [frameHeader]byte
	header[0] = kind
	copy(header[1:1+dbPositionSize], encoded)
	binary.LittleEndian.PutUint64(header[1+dbPositionSize:], uint64(len(payload)))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readFrame(r io.Reader) (byte, DBPosition, []byte, error) {
	var header [frameHeader]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, DBPosition{}, nil, err
	}

	var at DBPosition
	if err := at.UnmarshalBinary(header[1 : 1+dbPositionSize]); err != nil {
		return 0, DBPosition{}, nil, err
	}

	length := binary.LittleEndian.Uint64(header[1+dbPositionSize:])
	if length > 1<<30 {
		return 0, DBPosition{}, nil, fmt.Errorf("a frame claiming %d bytes", length)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, DBPosition{}, nil, err
	}
	return header[0], at, payload, nil
}

// serveReplication is the leader's side of a connection: read where the
// follower has got to, send it a snapshot if it has got nowhere, and then
// stream. It is what a real one would be, minus the logging.
func serveReplication(conn net.Conn, leader *DB, stop <-chan struct{}) error {
	defer conn.Close()

	var asked [dbPositionSize]byte
	if _, err := io.ReadFull(conn, asked[:]); err != nil {
		return err
	}

	var from DBPosition
	if err := from.UnmarshalBinary(asked[:]); err != nil {
		return err
	}

	send := func(batch []byte, next DBPosition) error {
		return writeFrame(conn, frameBatch, next, batch)
	}

	// A follower with nowhere to carry on from gets the whole store, and so
	// does one whose place has been merged away while it was gone. Nothing
	// holds a log open for a follower that is not connected, so this is not an
	// unusual path — it is what happens to any follower that was away long
	// enough, and a leader that answered it by dropping the connection would
	// leave that follower stuck forever.
	for {
		var holding func()

		if from == (DBPosition{}) {
			var snapshot bytes.Buffer

			// The hold that comes back with the snapshot goes to Follow, which
			// takes one of its own before letting it go: released here instead,
			// there would be a moment with the log the stream starts from
			// unheld.
			at, release, err := leader.Snapshot(&snapshot, ReplicaOptions{})
			if err != nil {
				return err
			}
			if err := writeFrame(conn, frameSnapshot, at, snapshot.Bytes()); err != nil {
				release()
				return err
			}
			from, holding = at, release
		}

		_, err := leader.Follow(from, holding, send, stop, ReplicaOptions{})
		if !errors.Is(err, ErrorDiverged) {
			return err
		}
		from = DBPosition{} // start again, with a snapshot
	}
}

// followOverTCP is the other side: say where it has got to, then take what
// arrives until the connection ends.
func followOverTCP(conn net.Conn, follower *DB) error {
	defer conn.Close()

	asked, err := follower.Applied().MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := conn.Write(asked); err != nil {
		return err
	}

	for {
		kind, at, payload, err := readFrame(conn)
		if err != nil {
			return err // the connection ended, which is not a failure
		}

		switch kind {
		case frameSnapshot:
			if err := follower.ApplySnapshot(at, bytes.NewReader(payload), ReplicaOptions{}); err != nil {
				return err
			}
		case frameBatch:
			from := follower.Applied()
			if _, err := follower.Apply(from, at, bytes.NewReader(payload), ReplicaOptions{}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("a frame of kind %q", kind)
		}
	}
}

// TestDBReplicationOverTCP runs a leader and a follower over a loopback socket,
// with values big enough that a batch does not fit in one segment and the reads
// on the other side have to be filled rather than taken as they come.
func TestDBReplicationOverTCP(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 96 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	// Something to snapshot, so the position it comes with names a record.
	value := bytes.Repeat([]byte("x"), 4<<10)
	live := map[string]string{}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		want := fmt.Sprintf("%s-first", key)
		if err := leader.Write([]byte(key), append(append([]byte(nil), want...), value...)); err != nil {
			t.Fatal(err)
		}
		live[key] = want
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	defer listener.Close()

	stop := make(chan struct{})
	var serving sync.WaitGroup

	serving.Add(1)
	go func() {
		defer serving.Done()

		for {
			conn, err := listener.Accept()
			if err != nil {
				return // the listener closed
			}
			serving.Add(1)
			go func() {
				defer serving.Done()

				if err := serveReplication(conn, leader, stop); err != nil {
					t.Logf("the leader's side of a connection ended with: %v", err)
				}
			}()
		}
	}()

	// A follower's whole life: dial, hand over where it is, take what comes.
	dial := func() net.Conn {
		t.Helper()

		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dialling the leader: %v", err)
		}
		return conn
	}

	first := dial()
	streamed := make(chan error, 1)
	go func() { streamed <- followOverTCP(first, follower) }()

	// Written while the stream is running, so the snapshot and the tail are
	// both exercised over the wire.
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("key-%03d", i)
		want := fmt.Sprintf("%s-first", key)
		if err := leader.Write([]byte(key), append(append([]byte(nil), want...), value...)); err != nil {
			t.Fatal(err)
		}
		live[key] = want
	}

	waitForPositions(t, follower, leader, "the follower to catch up over the wire")
	sameLive(t, leader, follower, live, "after the first connection")

	// The connection breaks in the middle of things, which is what a network
	// does. Nothing about that may cost the follower what it has.
	first.Close()
	if err := <-streamed; err != nil && !errors.Is(err, io.EOF) &&
		!errors.Is(err, net.ErrClosed) && !isReset(err) {
		t.Fatalf("the stream ended with an unexpected error: %v", err)
	}

	was := follower.Applied()
	if was == (DBPosition{}) {
		t.Fatal("the follower lost its position when the connection broke")
	}

	for i := 200; i < 320; i++ {
		key := fmt.Sprintf("key-%03d", i)
		want := fmt.Sprintf("%s-second", key)
		if err := leader.Write([]byte(key), append(append([]byte(nil), want...), value...)); err != nil {
			t.Fatal(err)
		}
		live[key] = want
	}
	if err := leader.Delete([]byte("key-000")); err != nil {
		t.Fatal(err)
	}
	delete(live, "key-000")

	// And it reconnects from where it was, without taking the store again.
	second := dial()
	resumed := make(chan error, 1)
	go func() {
		err := followOverTCP(second, follower)
		if err != nil {
			t.Logf("the follower's side of the second connection ended with: %v", err)
		}
		resumed <- err
	}()

	waitForPositions(t, follower, leader, "the follower to catch up after reconnecting")
	sameLive(t, leader, follower, live, "after reconnecting")

	if _, err := follower.Read([]byte("key-000")); !errors.Is(err, ErrorKeyDeleted) &&
		!errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("a key deleted while the connection was down reads as '%v'", err)
	}

	second.Close()
	<-resumed

	close(stop)
	listener.Close()
	serving.Wait()

	t.Logf("%d keys across a socket, the leader over %d logs and the follower over %d",
		len(live), leader.Segments(), follower.Segments())
}

// isReset is the connection being closed from the other end, which is a normal
// way for a stream to stop and not something to fail a test over.
func isReset(err error) bool {
	return err != nil && (errors.Is(err, io.ErrUnexpectedEOF) ||
		bytes.Contains([]byte(err.Error()), []byte("reset by peer")) ||
		bytes.Contains([]byte(err.Error()), []byte("broken pipe")) ||
		bytes.Contains([]byte(err.Error()), []byte("closed")))
}

// waitForPositions waits until the leader has nothing left to send the
// follower, and says where both of them were if that never happens.
//
// Comparing the two positions is the obvious check and the wrong one. A
// follower that has read a log to its end rests there rather than stepping to
// the start of the log being written, so a caught-up follower reports the end
// of log 13 while the leader reports the start of log 14. Asking the leader
// whether it has anything more is the question that was meant.
func waitForPositions(t *testing.T, follower, leader *DB, what string) {
	t.Helper()

	caughtUp := func() bool {
		pos := follower.Applied()
		next, err := leader.Since(pos, io.Discard, ReplicaOptions{})
		return err == nil && next == pos
	}

	deadline := time.Now().Add(30 * time.Second)
	for !caughtUp() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: the follower is at %+v, the leader at %+v",
				what, follower.Applied(), leader.Position())
		}
		time.Sleep(time.Millisecond)
	}
}

// sameLive holds a follower to the values the leader was given.
func sameLive(t *testing.T, leader, follower *DB, live map[string]string, when string) {
	t.Helper()

	for key, want := range live {
		got, err := follower.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s: %q: the leader has %q, the follower says %v", when, key, want, err)
		}
		if !bytes.HasPrefix(got, []byte(want)) {
			t.Fatalf("%s: %q starts %q on the follower, want %q", when, key, got[:min(len(got), 24)], want)
		}
	}

	// Live keys, not Len: Len counts tombstones, and a follower that came back
	// by way of a snapshot has none, since a snapshot carries only live records.
	// Both stores are right and the counts differ.
	count := func(db *DB) int {
		n := 0
		if err := db.ForEach(func(key, value []byte) bool { n++; return true }); err != nil {
			t.Fatalf("%s: ForEach: %v", when, err)
		}
		return n
	}

	if l, f := count(leader), count(follower); l != f {
		t.Errorf("%s: the leader has %d live keys, the follower %d", when, l, f)
	}
}
