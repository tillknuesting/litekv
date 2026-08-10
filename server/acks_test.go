package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

// Semi-synchronous replication is one promise — a 204 means a failover will not
// lose this write — and it can be broken in two directions. It can answer 204
// when no follower has the record, which is the promise being a lie. Or it can
// answer 202 when one does, which is a server that says its writes are unsafe
// when they are not, and which nobody will leave switched on.

// TestReachesComparesBySequence is the comparison everything else rests on. It
// is by the sequence number because that is the only field of a position that
// survives a merge and only ever goes up; offsets and log numbers do neither.
func TestReachesComparesBySequence(t *testing.T) {
	at := func(term, seq uint64) litekv.DBPosition {
		return litekv.DBPosition{Term: term, Segment: 3,
			Log: litekv.Position{Offset: 100, Seq: seq}}
	}

	for _, test := range []struct {
		what        string
		acked, want litekv.DBPosition
		reaches     bool
	}{
		{"the same place", at(1, 40), at(1, 40), true},
		{"further on", at(1, 41), at(1, 40), true},
		{"behind", at(1, 39), at(1, 40), false},
		// The zero position names nothing, so everything has reached it — which
		// is what an empty leader's position looks like, and a write on it is
		// replicated the moment anybody is attached.
		{"a follower on a term, against nothing to reach", at(1, 0), litekv.DBPosition{}, true},
		{"nothing against nothing", litekv.DBPosition{}, litekv.DBPosition{}, true},
		{"nothing, against a leader that has written", litekv.DBPosition{}, at(1, 40), false},

		// A position from another leader says nothing about this history. Its
		// numbers are that leader's numbers, and a follower on an older term has
		// not got this write however large its sequence is.
		{"an older term, further on", at(1, 9000), at(2, 40), false},
		{"a newer term", at(3, 1), at(2, 40), true},
	} {
		t.Run(test.what, func(t *testing.T) {
			if got := reaches(test.acked, test.want); got != test.reaches {
				t.Errorf("reaches(%+v, %+v) = %v", test.acked, test.want, got)
			}
		})
	}
}

// TestAWriteWaitsForAFollower is the promise itself, end to end: a leader told
// to wait for one follower answers 204 only once a follower has the record, and
// says how many did.
func TestAWriteWaitsForAFollower(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})
	up.api.opts.WaitFor = 1
	up.api.opts.WaitTimeout = 15 * time.Second

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	followingAt(t, db, up.srv.URL)

	// Waited for, so that the follower is attached before the write rather than
	// racing it: what is being tested is the wait, not the attach.
	waitForFollowers(t, up.api, 1)

	started := time.Now()
	resp := asking(t, up.api, http.MethodPut, "/v1/keys/held", "v", nil)
	wants(t, resp, http.StatusNoContent)

	// Promptly, and that is a separate claim from correctly. A wait that is
	// never woken and only ever times out returns the same count and the same
	// 204 — fifteen seconds later — so without a bound on how long this took,
	// the waking is not tested at all.
	if took := time.Since(started); took > 5*time.Second {
		t.Errorf("a write waited %v for a follower that was already attached", took)
	}

	if got := resp.Header.Get(headerReplicated); got != "1" {
		t.Errorf("%s is %q, want 1", headerReplicated, got)
	}

	// And the follower really does hold it, which is the whole of what the 204
	// was claiming.
	if value, err := db.Read([]byte("held")); err != nil || string(value) != "v" {
		t.Errorf("the follower the write waited for has %q, '%v'", value, err)
	}
}

// TestAWriteWithNoFollowerSaysSo. A leader asked to wait for a follower it does
// not have cannot make one appear, and it cannot unwrite the record either — it
// is in the log before anything waits. So it says what happened: 202, and a
// count of zero.
//
// This is the case an operator has to be able to see. A server that answered 204
// here would be promising a failover it cannot survive.
func TestAWriteWithNoFollowerSaysSo(t *testing.T) {
	s, db := newServer(t, Options{WaitFor: 1, WaitTimeout: 50 * time.Millisecond})

	started := time.Now()
	resp := asking(t, s, http.MethodPut, "/v1/keys/alone", "v", nil)
	wants(t, resp, http.StatusAccepted)

	if took := time.Since(started); took < 40*time.Millisecond {
		t.Errorf("a write with nobody to wait for gave up after %v", took)
	}
	if got := resp.Header.Get(headerReplicated); got != "0" {
		t.Errorf("%s is %q, want 0", headerReplicated, got)
	}

	// Stored, and that is the honest part: a client that reads 202 as a failure
	// and retries writes the record twice.
	if value, err := db.Read([]byte("alone")); err != nil || string(value) != "v" {
		t.Errorf("a write that answered 202 is not in the store: %q, '%v'", value, err)
	}

	// Every write route, because a batch that skipped the wait would be the way
	// round it.
	wants(t, asking(t, s, http.MethodDelete, "/v1/keys/alone", "", nil), http.StatusAccepted)
	wants(t, asking(t, s, http.MethodPost, "/v1/batch",
		`{"op":"write","key":"b","value":"v"}`+"\n", nil), http.StatusAccepted)
}

// TestWaitingForNobodyIsTheOldBehaviour. WaitFor zero is asynchronous
// replication, which is what this has always done, and it must not have grown a
// five-second wait by default.
func TestWaitingForNobodyIsTheOldBehaviour(t *testing.T) {
	s, _ := newServer(t, Options{})

	started := time.Now()
	resp := asking(t, s, http.MethodPut, "/v1/keys/k", "v", nil)
	wants(t, resp, http.StatusNoContent)

	if took := time.Since(started); took > time.Second {
		t.Errorf("a write with WaitFor unset took %v", took)
	}

	// And it says nothing about replication, because it did not wait for any.
	if got := resp.Header.Get(headerReplicated); got != "" {
		t.Errorf("%s is %q on a server that waits for nobody", headerReplicated, got)
	}
}

// TestAnAckFromAStrangerIsRefused. An acknowledgement is a claim, and the only
// thing that makes it worth anything is that this leader is the one sending that
// follower records. Taking one from anybody would let whatever can reach this
// route satisfy a semi-synchronous write by asserting it had the data — which is
// the guarantee, given away to a caller that guessed a URL.
func TestAnAckFromAStrangerIsRefused(t *testing.T) {
	s, db := newServer(t, Options{WaitFor: 1, WaitTimeout: 50 * time.Millisecond})

	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	at, err := positionParam(db.Position())
	if err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(ackBody{ID: "a-follower-that-is-not-streaming", At: at})
	if err != nil {
		t.Fatal(err)
	}

	wants(t, asking(t, s, http.MethodPost, ackPath, string(body), nil), http.StatusConflict)

	// And the write still says nobody has it.
	resp := asking(t, s, http.MethodPut, "/v1/keys/k", "v2", nil)
	wants(t, resp, http.StatusAccepted)

	if got := resp.Header.Get(headerReplicated); got != "0" {
		t.Errorf("%s is %q after an ack from a stranger", headerReplicated, got)
	}
}

// TestAFollowerThatWentAwayStopsCounting. WaitFor is a number about now. A
// follower whose stream has ended is not going to acknowledge anything, so
// keeping its last position would make every write look replicated for as long
// as the leader ran.
func TestAFollowerThatWentAwayStopsCounting(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})
	up.api.opts.WaitFor = 1
	up.api.opts.WaitTimeout = 50 * time.Millisecond

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f := followingAt(t, db, up.srv.URL)
	waitForFollowers(t, up.api, 1)

	wants(t, asking(t, up.api, http.MethodPut, "/v1/keys/while-here", "v", nil),
		http.StatusNoContent)

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	up.alone(t)
	waitForFollowers(t, up.api, 0)

	resp := asking(t, up.api, http.MethodPut, "/v1/keys/after-it-left", "v", nil)
	wants(t, resp, http.StatusAccepted)

	if got := resp.Header.Get(headerReplicated); got != "0" {
		t.Errorf("%s is %q after the only follower left", headerReplicated, got)
	}
}

// TestAnAckOutOfOrderDoesNotGoBackwards. Acknowledgements are their own requests
// on their own connection, so two of them can arrive in the wrong order or the
// same one twice. A leader that took the newest it was told rather than the
// furthest would move a follower backwards and un-replicate a write that had
// already been acknowledged.
func TestAnAckOutOfOrderDoesNotGoBackwards(t *testing.T) {
	f := newFollowers()

	at := func(seq uint64) litekv.DBPosition {
		return litekv.DBPosition{Term: 1, Log: litekv.Position{Seq: seq}}
	}

	f.attach("one", at(0))

	if !f.ack("one", at(50)) {
		t.Fatal("an ack from an attached follower was refused")
	}
	if got, _ := f.count(at(50)); got != 1 {
		t.Fatalf("after acking 50, %d followers have reached 50", got)
	}

	// Older, and it must not take effect.
	if !f.ack("one", at(20)) {
		t.Fatal("an out-of-order ack was refused rather than ignored")
	}
	if got, _ := f.count(at(50)); got != 1 {
		t.Errorf("an ack of 20 undid an ack of 50: %d followers have reached 50", got)
	}
}

// waitForFollowers waits until a leader has the number of attached followers
// given, so that a test asserting something about the wait is not racing the
// attach.
func waitForFollowers(t *testing.T, s *Server, want int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		got, _ := s.followers.count(litekv.DBPosition{})
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the leader has %d followers attached, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStatusAndMetricsCountFollowers. Somebody running a semi-synchronous leader
// has to be able to see whether the followers it is waiting for are there, and a
// counter of requests answered cannot say: a stream is answered when it ends.
func TestStatusAndMetricsCountFollowers(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	if err := up.db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	acked := func() string {
		t.Helper()

		for _, line := range strings.Split(string(wants(t,
			do(t, up.api, http.MethodGet, metricsPath, nil), http.StatusOK)), "\n") {
			if strings.HasPrefix(line, "litekv_replication_followers ") {
				return strings.TrimPrefix(line, "litekv_replication_followers ")
			}
		}
		t.Fatal("there is no litekv_replication_followers in /metrics")
		return ""
	}

	if got := acked(); got != "0" {
		t.Errorf("a leader with no followers reports %s", got)
	}

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	followingAt(t, db, up.srv.URL)
	waitForFollowers(t, up.api, 1)

	if got := acked(); got != strconv.Itoa(1) {
		t.Errorf("a leader with one follower reports %s", got)
	}
}

// BenchmarkSemiSynchronousWrite is what waiting costs, with a real follower on
// the other end of a real listener.
//
// Measured here and not with curl, which is what the first attempt did: two
// hundred writes took 1507ms waiting and 1513ms not, because a curl process
// costs about seven milliseconds to start and that is the whole number. A
// measurement whose two arms agree to within half a percent is measuring the
// harness.
func BenchmarkSemiSynchronousWrite(b *testing.B) {
	for _, waitFor := range []int{0, 1} {
		name := "asynchronous"
		if waitFor > 0 {
			name = "waiting for one follower"
		}

		b.Run(name, func(b *testing.B) {
			db, err := litekv.OpenDB(b.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			s := New(db, Options{Logger: quiet(), WaitFor: waitFor,
				WaitTimeout: 30 * time.Second})
			defer s.Close()

			wire := httptest.NewServer(s)
			defer wire.Close()

			follower, err := litekv.OpenDB(b.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
			if err != nil {
				b.Fatal(err)
			}
			defer follower.Close()

			f, err := Follow(follower, wire.URL, FollowerOptions{Logger: quiet(),
				MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond})
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			// Attached before the timer starts, or the first write pays for the
			// whole connection.
			for deadline := time.Now().Add(30 * time.Second); ; {
				if got, _ := s.followers.count(litekv.DBPosition{}); got == 1 {
					break
				}
				if time.Now().After(deadline) {
					b.Fatal("the follower never attached")
				}
				time.Sleep(time.Millisecond)
			}

			value := strings.Repeat("x", 128)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; b.Loop(); i++ {
				rec := httptest.NewRecorder()
				s.ServeHTTP(rec, httptest.NewRequest(http.MethodPut,
					fmt.Sprintf("/v1/keys/k-%d", i), strings.NewReader(value)))

				if rec.Code != http.StatusNoContent {
					b.Fatalf("PUT: %d (%s)", rec.Code, rec.Header().Get(headerReplicated))
				}
			}
		})
	}
}
