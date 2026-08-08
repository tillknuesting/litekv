package server

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

// This was tcp_test.go in the engine's own package, where it was the only place
// the library was put on a wire: everything else there moves records through a
// bytes.Buffer or an io.Pipe, which says what the records are but not that the
// arrangement works, since a pipe never returns a short read, never splits a
// write across two calls and never goes away in the middle of one.
//
// It lives here now because here it is a test of the real thing rather than of
// a sketch beside it. The framing, the leader's loop and the follower are the
// ones that ship, and they reach the engine through its exported API and
// nothing else, so a change that breaks a caller breaks this. What it gave up
// by moving is a bare socket, and it gave up nothing: httptest.NewServer is a
// real listener, a real client and a real connection, and a chunked body
// arrives unchunked as a byte stream exactly as a socket's would.
//
// The three things the original found the hard way each have a test here: a
// leader answering divergence with a snapshot rather than by hanging up, a
// follower that keeps its position when a connection breaks, and the two wrong
// ways to ask whether two stores agree — Applied() == Position(), and Len().

// leader is a store, the handler over it, the listener under that, and a count
// of the frames that have gone out.
type leader struct {
	db     *litekv.DB
	api    *Server
	srv    *httptest.Server
	frames *frameCounter

	live atomic.Int64
}

// serving opens a store and serves it, with the closes registered in the order
// they have to happen: the handler stops the streams, then the listener waits
// for what is left, then the store closes. httptest.Server.Close blocks until
// every request has finished, and a replication stream is a request that would
// never finish on its own.
func serving(t *testing.T, opts litekv.DBOptions) *leader {
	t.Helper()

	db, err := litekv.OpenDB(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}

	up := &leader{db: db, frames: &frameCounter{}}

	up.api = New(db, Options{Logger: quiet()})
	up.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up.live.Add(1)
		defer up.live.Add(-1)

		up.api.ServeHTTP(up.frames.wrap(w), r)
	}))

	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(up.srv.Close)
	t.Cleanup(func() { _ = up.api.Close() })

	return up
}

// alone waits until this leader is serving nobody.
//
// A follower's Close ends its side of a connection, and the handler on this
// side goes away a moment later, when the closed socket reaches it. Anything
// that counts what the leader did has to wait for that moment or it is counting
// against a handler that is still running — and a handler that has been left
// behind by a follower is exactly the one that goes looking for a snapshot,
// because Follow was blocked and the store moved on underneath it.
func (l *leader) alone(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for l.live.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the leader is still serving %d requests", l.live.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// followingAt points a Follower at a leader and stops it when the test ends.
func followingAt(t *testing.T, db *litekv.DB, at string) *Follower {
	t.Helper()

	f, err := Follow(db, at, FollowerOptions{
		Logger: quiet(),
		// Short enough that a test which breaks a connection on purpose does
		// not spend a second of its life waiting for the reconnect.
		MinBackoff: time.Millisecond,
		MaxBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	return f
}

// TestReplicationOverHTTP is the promoted test: a leader and a follower over a
// real listener, with values big enough that a batch does not fit in one
// segment and the reads on the other side have to be filled rather than taken
// as they come, a connection broken on purpose part way through, and a follower
// stopped and started again.
//
// Merging is off on the leader, which is what lets this count snapshots and
// mean it. With merging on, a follower that is away for a moment can have its
// position taken out from under it and be sent the whole store again — that is
// correct, and it is the next test — so "the reconnect took no snapshot" would
// be an assertion about how lucky the merging was. Rotation is still on, so the
// batches still cross logs.
func TestReplicationOverHTTP(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever,
		SegmentSize: 64 << 10, MergeTrigger: 1 << 30})

	// Something to snapshot, so the position it comes with names a record.
	value := bytes.Repeat([]byte("x"), 4<<10)
	live := map[string]string{}

	write := func(from, to int, suffix string) {
		t.Helper()

		for i := from; i < to; i++ {
			key := fmt.Sprintf("key-%03d", i)
			want := key + suffix

			if err := up.db.Write([]byte(key), append([]byte(want), value...)); err != nil {
				t.Fatal(err)
			}
			live[key] = want
		}
	}

	write(0, 100, "-first")

	follower, err := litekv.OpenDB(t.TempDir(),
		litekv.DBOptions{Sync: litekv.SyncNever, SegmentSize: 96 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	following := followingAt(t, follower, up.srv.URL)

	// Written while the stream is running, so the snapshot and the tail are
	// both exercised over the wire.
	write(100, 200, "-first")

	waitForPositions(t, follower, up.db, "the follower to catch up over the wire")
	sameLive(t, up.db, follower, live, "after the first connection")

	// At least one, and not exactly one, and the difference is worth knowing.
	// A snapshot of a store whose active log happens to be empty at that instant
	// has nowhere to point but the start of that log, which names no record; if
	// the writes going on alongside fill and freeze it before the stream reads
	// anything, the leader refuses that position and snapshots again. It is the
	// one position in the format that cannot be checked, it is documented as
	// such, and how often it happens is a fact about the timing rather than
	// about this code. What the counting below is for is the reconnects, where
	// a snapshot is never right.
	took := up.frames.snapshots()
	if took < 1 {
		t.Error("a follower starting from nothing was not sent a snapshot")
	}

	// The connection breaks in the middle of things, which is what a network
	// does, and the follower is expected to notice and come back by itself.
	// Nothing about that may cost it what it has.
	before := follower.Applied()
	up.srv.CloseClientConnections()

	write(200, 260, "-second")

	waitForPositions(t, follower, up.db, "the follower to reconnect by itself")
	sameLive(t, up.db, follower, live, "after a broken connection")

	if got := up.frames.snapshots(); got != took {
		t.Errorf("a broken connection cost the follower %d snapshots", got-took)
	}
	if now := follower.Applied(); now == before {
		t.Error("the follower did not move after reconnecting")
	}

	// And now it is stopped rather than broken, which is a process going down.
	if err := following.Close(); err != nil {
		t.Fatal(err)
	}
	up.alone(t)

	if follower.Applied() == (litekv.DBPosition{}) {
		t.Fatal("the follower lost its position when it was stopped")
	}

	write(260, 320, "-second")

	if err := up.db.Delete([]byte("key-000")); err != nil {
		t.Fatal(err)
	}
	delete(live, "key-000")

	// A new one over the same store carries on from where the last one was,
	// without taking the store again.
	followingAt(t, follower, up.srv.URL)

	waitForPositions(t, follower, up.db, "the follower to catch up after starting again")
	sameLive(t, up.db, follower, live, "after starting again")

	if got := up.frames.snapshots(); got != took {
		t.Errorf("a follower that was stopped and started again took %d snapshots", got-took)
	}

	if _, err := follower.Read([]byte("key-000")); !errors.Is(err, litekv.ErrorKeyDeleted) &&
		!errors.Is(err, litekv.ErrorKeyNotFound) {
		t.Errorf("a key deleted while the follower was stopped reads as '%v'", err)
	}

	t.Logf("%d keys over HTTP in %d frames, %d of them snapshots, the leader over %d logs "+
		"and the follower over %d", len(live), up.frames.batches()+up.frames.snapshots(),
		up.frames.snapshots(), up.db.Segments(), follower.Segments())
}

// TestAFollowerKeepsUpWhileTheLeaderMerges is the leader doing everything a
// leader does — rotating, freezing, merging in the background — while a
// follower streams from it over a connection that stays open throughout.
//
// This is where the hold matters. Follow moves its hold forward as it reads, so
// merging can take everything behind the stream and nothing the stream is on;
// what it does not do is protect the log a stream is about to start from, which
// is why the hold that comes back with a snapshot has to be handed to Follow
// rather than released by the caller. It counts no snapshots — with merging on,
// how many there are is a fact about the merging — and holds the two stores to
// agreeing at the end, which is the only promise worth making here.
func TestAFollowerKeepsUpWhileTheLeaderMerges(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever, SegmentSize: 4 << 10})

	value := bytes.Repeat([]byte("m"), 400)
	live := map[string]string{}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if err := up.db.Write([]byte(key), append([]byte(key+"-v"), value...)); err != nil {
			t.Fatal(err)
		}
		live[key] = key + "-v"
	}

	follower, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever, SegmentSize: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followingAt(t, follower, up.srv.URL)

	// Enough rotations under the stream that the background merging has plenty
	// to take, and a few keys rewritten so it has something to discard.
	for i := 50; i < 400; i++ {
		key := fmt.Sprintf("key-%03d", i%200)
		want := fmt.Sprintf("%s-%d", key, i)

		if err := up.db.Write([]byte(key), append([]byte(want), value...)); err != nil {
			t.Fatal(err)
		}
		live[key] = want
	}

	waitForPositions(t, follower, up.db, "a follower to keep up with a leader that is merging")
	sameLive(t, up.db, follower, live, "while the leader merged")

	t.Logf("%d keys with the leader merging underneath: %d snapshots and %d batches, "+
		"the leader over %d logs and the follower over %d",
		len(live), up.frames.snapshots(), up.frames.batches(), up.db.Segments(), follower.Segments())
}

// TestALeaderAnswersDivergenceWithASnapshot is the first of the three, and the
// one that costs a follower everything when it is wrong.
//
// Nothing holds a log open for a follower that is not connected, so a follower
// that was away while the leader merged comes back to a position that is gone.
// The leader's answer to that has to be a snapshot. A leader that treated it as
// a failed connection would leave the follower asking for the same dead
// position for the rest of its life, and reconnecting would never help.
func TestALeaderAnswersDivergenceWithASnapshot(t *testing.T) {
	// Small logs so a few records rotate one, and merging left to this test
	// rather than to the background, so the merge lands where it is wanted.
	opts := litekv.DBOptions{Sync: litekv.SyncNever, SegmentSize: 1 << 10, MergeTrigger: 1 << 30}

	up := serving(t, opts)

	value := bytes.Repeat([]byte("y"), 200)
	live := map[string]string{}

	write := func(from, to int) {
		t.Helper()

		for i := from; i < to; i++ {
			key := fmt.Sprintf("key-%03d", i)
			want := key + "-v"

			if err := up.db.Write([]byte(key), append([]byte(want), value...)); err != nil {
				t.Fatal(err)
			}
			live[key] = want
		}
	}

	write(0, 40)

	follower, err := litekv.OpenDB(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	following := followingAt(t, follower, up.srv.URL)
	waitForPositions(t, follower, up.db, "the follower to catch up before it goes away")

	if err := following.Close(); err != nil {
		t.Fatal(err)
	}
	up.alone(t)

	stranded := follower.Applied()

	// While it is away: more logs, so its position is in a frozen one, and a
	// deleted key, so the merge that follows drops a tombstone. A log that
	// dropped records cannot be crossed — a follower carried over one would
	// never hear that the key was deleted — so this is a divergence that
	// carrying the position forward is not allowed to repair.
	write(40, 90)

	if err := up.db.Delete([]byte("key-000")); err != nil {
		t.Fatal(err)
	}
	delete(live, "key-000")

	write(90, 120)

	if err := up.db.Merge(); err != nil {
		t.Fatal(err)
	}

	// The position it is about to ask with really is gone, or this test would
	// pass with the leader doing nothing interesting at all.
	if _, err := up.db.Since(stranded, io.Discard, litekv.ReplicaOptions{}); !errors.Is(err, litekv.ErrorDiverged) {
		t.Fatalf("the merge left the follower's position usable: %v", err)
	}

	took := up.frames.snapshots()
	followingAt(t, follower, up.srv.URL)

	waitForPositions(t, follower, up.db, "a stranded follower to be carried by a snapshot")
	sameLive(t, up.db, follower, live, "after being stranded")

	if got := up.frames.snapshots() - took; got < 1 {
		t.Error("a stranded follower was not sent a snapshot")
	}
	if _, err := follower.Read([]byte("key-000")); !errors.Is(err, litekv.ErrorKeyDeleted) &&
		!errors.Is(err, litekv.ErrorKeyNotFound) {
		t.Errorf("the key deleted before the merge reads as '%v' on the follower", err)
	}
}

// TestTheStreamEndsWhenStreamsAreClosed holds CloseStreams to its job. A stream
// is a request that never finishes on its own, and http.Server.Shutdown waits
// for every request rather than cancelling any of them, so without this a
// leader with one follower attached spends its whole shutdown timeout going
// down.
func TestTheStreamEndsWhenStreamsAreClosed(t *testing.T) {
	s, db := newServer(t, Options{})

	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	ended := make(chan struct{})

	go func() {
		defer close(ended)
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, replicaPath, nil))
	}()

	// Long enough that the handler is in Follow rather than on its way there,
	// so this is a stream being ended and not one being refused.
	time.Sleep(20 * time.Millisecond)
	s.CloseStreams()

	select {
	case <-ended:
	case <-time.After(30 * time.Second):
		t.Fatal("the stream did not end when the streams were closed")
	}

	if rec.Code != http.StatusOK {
		t.Errorf("the stream that was ended answered %d", rec.Code)
	}

	// And a new one is refused rather than handed out and taken away again.
	wants(t, do(t, s, http.MethodGet, replicaPath, nil), http.StatusServiceUnavailable)

	// Closing twice is harmless, which matters because Close calls it too.
	s.CloseStreams()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestAPositionThatIsNotOneIsRefused. A position on the wire is opaque, so
// there is nothing for a client to get subtly wrong — only wholly wrong, which
// is a 400 rather than a stream that dies without saying why.
func TestAPositionThatIsNotOneIsRefused(t *testing.T) {
	s, _ := newServer(t, Options{})

	right := marshal(t, litekv.DBPosition{})
	long := marshal(t, litekv.DBPosition{})

	// The right number of bytes and not a position: the offsets have to be
	// positive and the last record has to start before the log ends, which the
	// store checks rather than believes.
	nonsense := marshal(t, litekv.DBPosition{Log: litekv.Position{Offset: 1, Last: 99}})

	for _, from := range []string{
		"not-a-position!",
		"//A=", // base64, and not the URL alphabet
		base64.RawURLEncoding.EncodeToString(right[:10]),
		base64.RawURLEncoding.EncodeToString(append(long, 0)),
		base64.RawURLEncoding.EncodeToString(nonsense),
	} {
		target := replicaPath + "?from=" + url.QueryEscape(from)

		body := wants(t, do(t, s, http.MethodGet, target, nil), http.StatusBadRequest)
		if !strings.Contains(string(body), "position") {
			t.Errorf("from=%q was refused with %q", from, body)
		}
	}

	// And no parameter at all is a follower with nowhere to carry on from,
	// which is the one thing that is not an error.
	if _, err := positionOf(""); err != nil {
		t.Errorf("a missing from: %v", err)
	}
}

func marshal(t *testing.T, pos litekv.DBPosition) []byte {
	t.Helper()

	encoded, err := pos.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// TestAFollowerWithANewerTermFencesTheLeader. A leader learns it has been
// replaced from something asking it for records at a term above its own, and
// this endpoint is now one of the places that can happen. It has to answer 409
// with the term on it, and — the part that is easy to leave out — it has to
// write the news down, or the leader carries on taking writes until somebody
// notices the errors.
func TestAFollowerWithANewerTermFencesTheLeader(t *testing.T) {
	s, db := newServer(t, Options{})

	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	ahead := db.Position()
	ahead.Term += 3

	from, err := positionParam(ahead)
	if err != nil {
		t.Fatal(err)
	}

	resp := do(t, s, http.MethodGet, replicaPath+"?from="+from, nil)
	wants(t, resp, http.StatusConflict)

	if resp.Header.Get(headerTerm) == "" {
		t.Errorf("a fenced leader answered without %s", headerTerm)
	}

	if err := db.Write([]byte("after"), []byte("v")); !errors.Is(err, litekv.ErrorFenced) {
		t.Errorf("the leader went on taking writes after hearing of a newer term: %v", err)
	}
}

// TestAFencedFollowerBacksOffRatherThanSpinning. There is nowhere for a
// follower to go when the leader it is pointed at has been replaced — it cannot
// know who the new one is, and that is what roles and consensus are for — so it
// keeps asking, in case somebody promotes something, but at the longest
// interval rather than as fast as it can.
func TestAFencedFollowerBacksOffRatherThanSpinning(t *testing.T) {
	var asked struct {
		sync.Mutex
		n int
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Lock()
		asked.n++
		asked.Unlock()

		w.Header().Set(headerTerm, "7")
		writeError(w, http.StatusConflict, litekv.ErrorFenced.Error())
	}))
	defer srv.Close()

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f, err := Follow(db, srv.URL, FollowerOptions{Logger: quiet(),
		MinBackoff: time.Microsecond, MaxBackoff: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	asked.Lock()
	n := asked.n
	asked.Unlock()

	// A handful at the hundred-millisecond wait, against the thousands a
	// microsecond wait would give if a refusal were treated as an ordinary
	// connection ending.
	if n == 0 {
		t.Error("a fenced follower gave up rather than backing off")
	}
	if n > 25 {
		t.Errorf("a fenced follower asked %d times in 200ms", n)
	}
	t.Logf("a refused follower asked %d times in 200ms", n)
}

// TestAnUnreachableLeaderIsNotAnError. A leader that is not there yet is the
// ordinary state of affairs when two nodes are started at once, and it is the
// same state of affairs as one that has gone away: something to reconnect to.
func TestAnUnreachableLeaderIsNotAnError(t *testing.T) {
	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Port 1 on loopback, which nothing is listening on.
	f, err := Follow(db, "http://127.0.0.1:1", FollowerOptions{Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("pointing a follower at a leader that is down: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// An address that could never be dialled is a different thing, and is the
	// one that is worth failing on the way in.
	for _, bad := range []string{"", "127.0.0.1:8080", "ftp://host", "http://", "://"} {
		if f, err := Follow(db, bad, FollowerOptions{Logger: quiet()}); err == nil {
			_ = f.Close()
			t.Errorf("%q was accepted as a leader address", bad)
		}
	}
}

// TestAFrameRoundTrips is the framing on its own, with no store under it.
func TestAFrameRoundTrips(t *testing.T) {
	at := litekv.DBPosition{Term: 3, Segment: 12,
		Log: litekv.Position{Offset: 400, Last: 380, Crc: 0xdeadbeef, Seq: 91}}

	for _, payload := range [][]byte{nil, {}, []byte("one record's worth"),
		bytes.Repeat([]byte("z"), 200<<10)} {
		var wire bytes.Buffer
		if err := writeFrame(&wire, frameBatch, at, payload); err != nil {
			t.Fatal(err)
		}

		kind, back, got, err := readFrame(&wire, defaultMaxFrame)
		if err != nil {
			t.Fatalf("%d bytes: %v", len(payload), err)
		}
		if kind != frameBatch || back != at {
			t.Errorf("%d bytes came back as %q at %+v", len(payload), kind, back)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("%d bytes came back as %d", len(payload), len(got))
		}
	}
}

// TestAFrameLengthIsBounded. The length is the other end's word for it, and the
// other end is whatever answered a URL somebody put on a command line.
func TestAFrameLengthIsBounded(t *testing.T) {
	header := make([]byte, frameHeader)
	header[0] = frameBatch
	copy(header[1:], marshal(t, litekv.DBPosition{}))

	binary.LittleEndian.PutUint64(header[1+dbPositionSize:], 1<<40)

	// Which error it is, and not merely that there was one. Take the bound away
	// and this frame still fails — as a torn one, because the reader goes
	// looking for a terabyte and the connection ends — so "some error came back"
	// is a test the missing bound passes. errFrameTooLarge exists to be asked
	// for here.
	_, _, _, err := readFrame(bytes.NewReader(header), defaultMaxFrame)
	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("a frame claiming a terabyte reported '%v', want it refused on the bound", err)
	}

	// A length within the bound and not behind it is a torn frame, which is
	// what a connection that went away mid-payload leaves. It has to be an
	// error rather than a short payload taken as whole, and it has to cost the
	// three bytes that arrived rather than the half a gigabyte claimed.
	binary.LittleEndian.PutUint64(header[1+dbPositionSize:], 1<<29)

	_, _, _, err = readFrame(bytes.NewReader(append(header, 'a', 'b', 'c')), defaultMaxFrame)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("a frame claiming half a gigabyte with three bytes behind it: %v", err)
	}
}

// FuzzReadFrame feeds arbitrary bytes to the one thing here that reads from a
// stranger. It must never panic, and it must never allocate on a length it has
// been told about rather than sent — which is what the bound and the growing
// are for, and which a fuzzer running the machine out of memory would report as
// having been removed.
func FuzzReadFrame(f *testing.F) {
	var seed bytes.Buffer
	_ = writeFrame(&seed, frameBatch, litekv.DBPosition{Term: 1, Segment: 2,
		Log: litekv.Position{Offset: 8, Last: 0, Crc: 7, Seq: 1}}, []byte("hello"))

	f.Add(seed.Bytes())
	f.Add([]byte{})
	f.Add(make([]byte, frameHeader))

	huge := make([]byte, frameHeader)
	huge[0] = frameSnapshot
	binary.LittleEndian.PutUint64(huge[1+dbPositionSize:], 1<<62)
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		kind, at, payload, err := readFrame(bytes.NewReader(data), 1<<20)
		if err != nil {
			return
		}
		if len(payload) > len(data) {
			t.Fatalf("a %d byte input yielded a %d byte payload", len(data), len(payload))
		}

		// What came back has to re-encode to the bytes it was read from, which
		// is the only claim the reader makes about a frame it has not handed to
		// a store.
		var again bytes.Buffer
		if err := writeFrame(&again, kind, at, payload); err != nil {
			t.Fatal(err)
		}
		if again.Len() > len(data) || !bytes.Equal(again.Bytes(), data[:again.Len()]) {
			t.Fatal("a frame did not re-encode to the bytes it was read from")
		}
	})
}

// waitForPositions waits until the leader has nothing left to send the
// follower, and says where both of them were if that never happens.
//
// Comparing the two positions is the obvious check and the wrong one. A
// follower that has read a log to its end rests there rather than stepping to
// the start of the log being written, so a caught-up follower reports the end
// of log 13 while the leader reports the start of log 14. Asking the leader
// whether it has anything more is the question that was meant.
func waitForPositions(t *testing.T, follower, leader *litekv.DB, what string) {
	t.Helper()

	caughtUp := func() bool {
		pos := follower.Applied()
		next, err := leader.Since(pos, io.Discard, litekv.ReplicaOptions{})
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
func sameLive(t *testing.T, leader, follower *litekv.DB, live map[string]string, when string) {
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
	count := func(db *litekv.DB) int {
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

// frameCounter watches what a leader writes and counts the frames, which is the
// only way from outside to tell a follower that resumed from where it was from
// one that was sent the whole store again. Both end up holding the same
// records, so nothing about the data can say which happened, and that is
// exactly the difference a broken connection must not make.
//
// It knows that writeFrame puts the header in one Write and the payload in the
// next, and it counts the payload down so that a batch which happens to be
// header-sized and to start with an 'S' is not counted as a snapshot. The
// counting-down is per response and not across them: a frame whose header goes
// out and whose payload does not is what a connection breaking mid-frame looks
// like, and it must not leave the next connection's frames uncounted.
type frameCounter struct {
	mu    sync.Mutex
	snap  int
	batch int
}

func (c *frameCounter) wrap(w http.ResponseWriter) http.ResponseWriter {
	return &countedWriter{ResponseWriter: w, counter: c}
}

func (c *frameCounter) saw(kind byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch kind {
	case frameSnapshot:
		c.snap++
	case frameBatch:
		c.batch++
	}
}

func (c *frameCounter) snapshots() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.snap
}

func (c *frameCounter) batches() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.batch
}

// countedWriter is one response, and net/http calls Write on it from one
// goroutine, so the frame accounting needs no lock of its own.
type countedWriter struct {
	http.ResponseWriter
	counter *frameCounter
	left    int64
}

func (w *countedWriter) Write(p []byte) (int, error) {
	switch {
	case w.left > 0:
		w.left -= int64(len(p))
	case len(p) == frameHeader:
		w.counter.saw(p[0])
		w.left = int64(binary.LittleEndian.Uint64(p[1+dbPositionSize:]))
	}
	return w.ResponseWriter.Write(p)
}

func (w *countedWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// The tests below were written because the mutations for them survived the
// first sweep of this file. Each one is a promise the code makes that nothing
// was asking it to keep.

// TestAFrameClaimCostsNothing. A claim is not bytes. The bound refuses a
// hostile number, but a number under the bound and far over what actually
// arrived would still be a way to make this process ask for memory on a
// stranger's word, which is why the payload is grown into as the bytes come.
//
// Measured rather than asserted structurally, because "it does not allocate the
// claim" is a statement about allocation and nothing else says it. The mutation
// that allocates at the claimed length grows TotalAlloc by the claim, or dies
// trying.
func TestAFrameClaimCostsNothing(t *testing.T) {
	header := make([]byte, frameHeader)
	header[0] = frameBatch
	copy(header[1:], marshal(t, litekv.DBPosition{}))

	// Under the bound, so the length check lets it through, and far over the
	// three bytes behind it.
	const claim = 1 << 40
	binary.LittleEndian.PutUint64(header[1+dbPositionSize:], claim)

	body := bytes.NewReader(append(header, 'a', 'b', 'c'))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	_, _, _, err := readFrame(body, claim*2)

	runtime.ReadMemStats(&after)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("a frame claiming a terabyte with three bytes behind it: %v", err)
	}

	// A megabyte of headroom over the 64 KiB the reader grows in. A terabyte is
	// six orders of magnitude the other side of it.
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Errorf("reading a frame that claimed %d bytes and sent 3 allocated %d", claim, grew)
	}
}

// TestClosingTheServerEndsTheStreams. CloseStreams has its own test next door;
// this is about Close calling it. They are not the same promise — Close is what
// cmd/litekvd calls, and a Close that stopped the writer and left a stream
// running would hang the shutdown it was added to bound.
func TestClosingTheServerEndsTheStreams(t *testing.T) {
	s, db := newServer(t, Options{})

	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	ended := make(chan struct{})

	go func() {
		defer close(ended)
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, replicaPath, nil))
	}()

	// Long enough that the handler is inside Follow rather than on its way
	// there, so this is a stream being ended rather than one being refused.
	time.Sleep(20 * time.Millisecond)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ended:
	case <-time.After(30 * time.Second):
		t.Fatal("a stream outlived the Close of the server serving it")
	}
}

// TestAFollowerIgnoresWhatANonOkAnswerSays. A leader that answered with a status
// has not sent a stream, and what is in the body of a refusal is not records.
//
// The sharp version of this: the fake leader answers 500 and then sends a
// perfectly good frame holding a perfectly good batch. A follower that reads a
// body without looking at the status would apply it — and it would be applying
// records from a server that had just said it was broken.
func TestAFollowerIgnoresWhatANonOkAnswerSays(t *testing.T) {
	source, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	if err := source.Write([]byte("poison"), []byte("applied from a 500")); err != nil {
		t.Fatal(err)
	}

	// A real snapshot frame, so that nothing but the status stands between this
	// and being applied.
	var payload bytes.Buffer
	at, release, err := source.Snapshot(&payload, litekv.ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	release()

	var frame bytes.Buffer
	if err := writeFrame(&frame, frameSnapshot, at, payload.Bytes()); err != nil {
		t.Fatal(err)
	}

	var asked atomic.Int64
	lying := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		w.Header().Set("Content-Type", contentTypeStream)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(frame.Bytes())
	}))
	defer lying.Close()

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f := followingAt(t, db, lying.URL)

	// Long enough to have asked several times and applied it if it were going to.
	for deadline := time.Now().Add(30 * time.Second); asked.Load() < 3; {
		if time.Now().After(deadline) {
			t.Fatalf("the follower asked %d times", asked.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if value, err := db.Read([]byte("poison")); err == nil {
		t.Errorf("a follower applied %q out of the body of a 500", value)
	}
	if applied := db.Applied(); applied != (litekv.DBPosition{}) {
		t.Errorf("a follower moved to %+v on the strength of a 500", applied)
	}
}

// TestCloseWaitsForTheFollower. Close waits for the goroutine rather than
// merely asking it to stop, and that is the whole of its contract: a batch being
// applied is a write to the store, and a Close that returned first would leave
// the caller free to close the store underneath it. cmd/litekvd closes the
// follower and then the store on the strength of this.
func TestCloseWaitsForTheFollower(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever, SegmentSize: 32 << 10})

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f := followingAt(t, db, up.srv.URL)

	// Written throughout, so the follower is busy rather than idle when it is
	// stopped: a goroutine that outlives Close has something to do.
	writing := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := 0; ; i++ {
			select {
			case <-writing:
				return
			default:
			}
			if err := up.db.Write([]byte(fmt.Sprintf("k-%05d", i)),
				bytes.Repeat([]byte("v"), 512)); err != nil {
				return
			}
		}
	}()

	// Caught up enough to be applying rather than waiting for a first snapshot.
	for deadline := time.Now().Add(30 * time.Second); db.Applied() == (litekv.DBPosition{}); {
		if time.Now().After(deadline) {
			t.Fatal("the follower never applied anything")
		}
		time.Sleep(time.Millisecond)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Whatever it had reached when Close returned is where it stays. The leader
	// is still being written to for a moment longer, so a goroutine that was
	// still running would have more to apply and would apply it.
	stopped := db.Applied()

	time.Sleep(200 * time.Millisecond)
	close(writing)
	<-done

	if now := db.Applied(); now != stopped {
		t.Errorf("Close returned at %+v and the follower carried on to %+v", stopped, now)
	}
}

// TestTheBackoffGrows. A leader that is down is not a reason to ask as fast as
// the machine can, and the wait doubling is the only thing between one follower
// and a leader that comes back up to a spin.
//
// Counted rather than timed. Asserting a latency in a test is a way to fail on a
// busy machine and says nothing about the store — see AGENTS.md — but counting
// how many times a fixed window was used is robust: with the wait doubling from
// a millisecond to twenty, a fifth of a second is a handful of attempts, and
// without it, it is two hundred.
func TestTheBackoffGrows(t *testing.T) {
	var asked atomic.Int64

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		// A 5xx is a leader having a bad day rather than one refusing this
		// follower, so it takes the ordinary growing backoff and not the
		// straight-to-the-longest one a 4xx takes.
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer down.Close()

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f := followingAt(t, db, down.URL) // 1ms doubling to 20ms
	time.Sleep(200 * time.Millisecond)

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Half of each wait is jittered, so the arithmetic is not exact: from 1ms
	// doubling to 20ms, two hundred milliseconds is somewhere around fifteen
	// attempts. Forty is well clear of that and nowhere near the two hundred a
	// wait that never grew would manage.
	if n := asked.Load(); n > 40 {
		t.Errorf("a follower asked a leader that was down %d times in 200ms; the backoff is not growing", n)
	} else {
		t.Logf("%d attempts in 200ms", n)
	}
}

// TestOneSmallRecordArrivesAtOnce is about flushing, and it exists because the
// mutation that stops the frames being flushed was caught by the big tests in
// one sweep and survived the next. That is worse than not being caught: those
// tests write hundreds of four-kilobyte values, which fill net/http's buffer and
// push the frames out whatever this code does, so whether they notice is a fact
// about how much they happened to write.
//
// One small record cannot fill anything. Either it is flushed or it sits in a
// buffer on the leader until something else pushes it out, and a follower that
// has caught up and is waiting is exactly the case where nothing will.
func TestOneSmallRecordArrivesAtOnce(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever, MergeTrigger: 1 << 30})

	if err := up.db.Write([]byte("first"), []byte("something to snapshot")); err != nil {
		t.Fatal(err)
	}

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	followingAt(t, db, up.srv.URL)
	waitForPositions(t, db, up.db, "the follower to take the snapshot and settle")

	// Now it is idle and caught up, which is the state that matters: nothing
	// else is coming that could push a buffered frame out.
	if err := up.db.Write([]byte("small"), []byte("nine bytes")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		if value, err := db.Read([]byte("small")); err == nil {
			if string(value) != "nine bytes" {
				t.Fatalf("the record arrived as %q", value)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a single small record never reached an idle follower; it is sitting in a buffer")
		}
		time.Sleep(time.Millisecond)
	}
}
