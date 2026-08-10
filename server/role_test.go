package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

// Two kinds of promise here and they fail differently.
//
// A replica refusing writes is about a store that would otherwise take them.
// Fencing does not cover it — a following store holds its leader's term, so
// ErrorFenced never fires — and nothing in the engine can, because the thing
// that makes this node a replica is a goroutine up here. Get it wrong and the
// two stores diverge silently: no checksum is wrong, nothing errors, and the
// records simply disagree for ever.
//
// Reads that are not stale are the opposite: a client asking to be refused. The
// failure is answering it anyway, with data that is correct and older than what
// the client has already been told.

// asking sends one request with headers, since do takes none.
func asking(t *testing.T, s *Server, method, target string, body string, header map[string]string) *http.Response {
	t.Helper()

	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	var req *http.Request
	if reader != nil {
		req = httptest.NewRequest(method, target, reader)
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec.Result()
}

// TestAReplicaRefusesEveryWrite is the one that stops the divergence. Every
// route that stores something has to refuse, not just the obvious one: a batch
// aimed at a replica is the same mistake as a PUT, and a longer one.
func TestAReplicaRefusesEveryWrite(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	if err := up.db.Write([]byte("from-the-leader"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	s, db := newServer(t, Options{})
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}

	waitForPositions(t, db, up.db, "the replica to catch up")

	for _, write := range []struct {
		what           string
		method, target string
		body           string
	}{
		{"a put", http.MethodPut, "/v1/keys/mine", "v"},
		{"a delete", http.MethodDelete, "/v1/keys/from-the-leader", ""},
		{"a batch", http.MethodPost, "/v1/batch", `{"op":"write","key":"mine","value":"v"}` + "\n"},
	} {
		t.Run(write.what, func(t *testing.T) {
			resp := asking(t, s, write.method, write.target, write.body, nil)
			wants(t, resp, http.StatusConflict)

			// And it says where to go, or a client that guessed wrong has
			// nothing to do but guess again.
			if got := resp.Header.Get(headerLeader); got != up.srv.URL {
				t.Errorf("%s is %q, want %q", headerLeader, got, up.srv.URL)
			}
		})
	}

	// Nothing of the client's reached the store, and the leader's record is
	// still the leader's.
	if _, err := db.Read([]byte("mine")); err == nil {
		t.Error("a replica stored a write it answered 409 to")
	}
	if _, err := db.Read([]byte("from-the-leader")); err != nil {
		t.Errorf("a refused delete removed the leader's record anyway: %v", err)
	}

	// Reading is the whole point of a replica and is never refused.
	wants(t, do(t, s, http.MethodGet, "/v1/keys/from-the-leader", nil), http.StatusOK)
	wants(t, do(t, s, http.MethodGet, "/v1/keys", nil), http.StatusOK)
}

// TestPromoteStopsFollowingBeforeItRaisesTheTerm. The order is the whole of the
// promotion: a term raised while records are still arriving is a store that has
// fenced its own leader and then applies another of its batches.
func TestPromoteStopsFollowingBeforeItRaisesTheTerm(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	if err := up.db.Write([]byte("before"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	s, db := newServer(t, Options{})
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitForPositions(t, db, up.db, "the replica to catch up")

	was := db.Term()

	resp := asking(t, s, http.MethodPost, "/v1/promote", "", nil)
	body := wants(t, resp, http.StatusOK)

	var promoted struct {
		Term uint64 `json:"term"`
	}
	if err := json.Unmarshal(body, &promoted); err != nil {
		t.Fatalf("the answer is not a term: %v: %s", err, body)
	}
	if promoted.Term <= was {
		t.Errorf("promoted to term %d from %d", promoted.Term, was)
	}

	// It leads now: it takes writes, and it does not say it is following.
	wants(t, do(t, s, http.MethodPut, "/v1/keys/mine", strings.NewReader("v")), http.StatusNoContent)

	if leader, replica := s.following(); replica {
		t.Errorf("a promoted node still says it follows %q", leader)
	}

	// And it has stopped taking the old leader's records. The old leader is
	// written to and nothing arrives.
	if err := up.db.Write([]byte("after-the-promotion"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if _, err := db.Read([]byte("after-the-promotion")); err == nil {
		t.Error("a promoted node is still applying its old leader's records")
	}
}

func TestStatusSaysWhichOfTheTwo(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	if err := up.db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	read := func(s *Server) statusBody {
		t.Helper()

		var body statusBody
		if err := json.Unmarshal(wants(t, do(t, s, http.MethodGet, "/v1/status", nil),
			http.StatusOK), &body); err != nil {
			t.Fatalf("the status is not the status shape: %v", err)
		}
		return body
	}

	// The leader.
	if got := read(up.api); got.Role != "leader" || got.Leader != "" {
		t.Errorf("a leader reports %+v", got)
	}
	if got := read(up.api); got.Position == "" {
		t.Error("a leader reports no position")
	}

	// The replica.
	s, db := newServer(t, Options{})
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitForPositions(t, db, up.db, "the replica to catch up")

	got := read(s)
	if got.Role != "replica" {
		t.Errorf("a replica reports the role %q", got.Role)
	}
	if got.Leader != up.srv.URL {
		t.Errorf("a replica reports the leader %q, want %q", got.Leader, up.srv.URL)
	}
	if got.Applied == "" {
		t.Error("a replica that has applied records reports no applied position")
	}
	if got.Keys == 0 {
		t.Error("a replica that has applied records reports no keys")
	}

	// A leader has applied nothing, so it says nothing rather than saying zero.
	if leader := read(up.api); leader.Applied != "" {
		t.Errorf("a leader reports an applied position of %q", leader.Applied)
	}
}

// TestAWriteHandsBackAPositionAReadCanBeHeldTo is the read-your-writes
// arrangement end to end, and it is the reason the other two exist.
//
// A client writes to the leader, is handed a position, and sends it with the
// reads it makes afterwards. A replica that has not got there says so rather
// than answering with what it has — which is the difference between a stale
// read and a wrong one, and the whole point of putting a replica behind a load
// balancer.
func TestAWriteHandsBackAPositionAReadCanBeHeldTo(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	// Written through the leader's own handler, which is where the position
	// comes from.
	resp := asking(t, up.api, http.MethodPut, "/v1/keys/fresh", "just written", nil)
	wants(t, resp, http.StatusNoContent)

	at := resp.Header.Get(headerPosition)
	if at == "" {
		t.Fatal("a write handed back no position")
	}

	// A store that has never heard of the leader cannot have reached it.
	behind, _ := newServer(t, Options{})

	stale := asking(t, behind, http.MethodGet, "/v1/keys/fresh", "", map[string]string{headerAfter: at})
	wants(t, stale, http.StatusPreconditionFailed)

	// And it says where it is, so a client can decide whether to wait here or
	// go elsewhere.
	if stale.Header.Get(headerPosition) == "" {
		t.Error("a refused read did not say where the store had got to")
	}

	// With a wait and nothing arriving, the wait runs out rather than the
	// answer being wrong.
	started := time.Now()
	waited := asking(t, behind, http.MethodGet, "/v1/keys/fresh", "",
		map[string]string{headerAfter: at, headerWait: "150ms"})
	wants(t, waited, http.StatusGatewayTimeout)

	if took := time.Since(started); took < 100*time.Millisecond {
		t.Errorf("a 150ms wait gave up after %v", took)
	}

	// The leader itself has reached it, so the same header is no obstacle.
	if got := wants(t, asking(t, up.api, http.MethodGet, "/v1/keys/fresh", "",
		map[string]string{headerAfter: at}), http.StatusOK); string(got) != "just written" {
		t.Errorf("the leader answered %q", got)
	}

	// And a replica does once it has caught up — through the wait, which is the
	// case the wait is for: the client asks the moment after writing and the
	// records are still on their way.
	s, db := newServer(t, Options{})
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	_ = db

	if got := wants(t, asking(t, s, http.MethodGet, "/v1/keys/fresh", "",
		map[string]string{headerAfter: at, headerWait: "30s"}), http.StatusOK); string(got) != "just written" {
		t.Errorf("a replica that waited answered %q", got)
	}
}

// TestAPositionHeaderThatIsNotOne. The same opaque cookie as the stream's from
// parameter, and the same answer when it is not one.
func TestAPositionHeaderThatIsNotOne(t *testing.T) {
	s, _ := newServer(t, Options{})

	// A position this store has reached, so that the wait is what each case
	// below turns on. With a position that does not decode, every one of them
	// would be refused before the wait was looked at — which is what the first
	// version of this test did, and it passed for the wrong reason.
	sound, err := positionParam(litekv.DBPosition{})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		what   string
		header map[string]string
		want   int
	}{
		{"a position that is not base64", map[string]string{headerAfter: "!!!!"}, http.StatusBadRequest},
		{"a position of the wrong length", map[string]string{headerAfter: "AAAA"}, http.StatusBadRequest},
		{"a wait that is not a duration",
			map[string]string{headerAfter: sound, headerWait: "soon"}, http.StatusBadRequest},
		{"a negative wait",
			map[string]string{headerAfter: sound, headerWait: "-1s"}, http.StatusBadRequest},
		{"a sound position and no wait", map[string]string{headerAfter: sound}, http.StatusNotFound},
	} {
		t.Run(test.what, func(t *testing.T) {
			wants(t, asking(t, s, http.MethodGet, "/v1/keys/k", "", test.header), test.want)
		})
	}

	// No header at all is no precondition, which is the ordinary read.
	wants(t, do(t, s, http.MethodGet, "/v1/keys/k", nil), http.StatusNotFound)
}

// TestFollowingTwiceIsRefused. A second Follower on one store is two things
// applying two leaders' records into one log, which is the divergence this
// whole file is about, arranged locally.
func TestFollowingTwiceIsRefused(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	s, _ := newServer(t, Options{})
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet()}); err == nil {
		t.Error("a server followed two leaders at once")
	}

	// An address that could never be dialled is refused before anything starts,
	// and leaves the server a leader rather than a replica of nothing.
	fresh, _ := newServer(t, Options{})
	if err := fresh.Follow("not a url at all", FollowerOptions{Logger: quiet()}); err == nil {
		t.Error("a server followed an address that is not one")
	}
	if _, replica := fresh.following(); replica {
		t.Error("a server that failed to start following says it is a replica")
	}
	wants(t, do(t, fresh, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusNoContent)
}

// TestARangeIsHeldToTheAfterHeaderToo. A client that will not read a stale value
// will not read a stale range either, and a route that quietly ignored the
// header would be the one place the promise did not hold.
func TestARangeIsHeldToTheAfterHeaderToo(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	written := asking(t, up.api, http.MethodPut, "/v1/keys/user:1", "ada", nil)
	wants(t, written, http.StatusNoContent)

	at := written.Header.Get(headerPosition)
	if at == "" {
		t.Fatal("a write handed back no position")
	}

	// The leader has reached it and answers.
	wants(t, asking(t, up.api, http.MethodGet, "/v1/keys?prefix=user:", "",
		map[string]string{headerAfter: at}), http.StatusOK)

	// A store that has never heard of it does not.
	behind, _ := newServer(t, Options{})
	wants(t, asking(t, behind, http.MethodGet, "/v1/keys?prefix=user:", "",
		map[string]string{headerAfter: at}), http.StatusPreconditionFailed)

	// And without the header it answers what it has, which is nothing.
	empty := wants(t, do(t, behind, http.MethodGet, "/v1/keys?prefix=user:", nil), http.StatusOK)
	if len(empty) != 0 {
		t.Errorf("an empty store answered a range with %q", empty)
	}
}

// TestClosingTheServerStopsTheFollower. The follower writes to the store without
// going through the queue, so it is the one thing Close has to stop before the
// caller is free to close the store underneath it. cmd/litekvd closes the
// server and then the store on the strength of this.
func TestClosingTheServerStopsTheFollower(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})

	if err := up.db.Write([]byte("before"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	s, db := newServer(t, Options{})
	if err := s.Follow(up.srv.URL, FollowerOptions{Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitForPositions(t, db, up.db, "the replica to catch up")

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Whatever it had when Close returned is where it stays. The leader carries
	// on being written to; a follower still running would apply it.
	stopped := db.Applied()

	for i := 0; i < 20; i++ {
		if err := up.db.Write([]byte(fmt.Sprintf("after-%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	if now := db.Applied(); now != stopped {
		t.Errorf("Close returned at %+v and the follower carried on to %+v", stopped, now)
	}
	if _, err := db.Read([]byte("after-00")); err == nil {
		t.Error("a follower kept applying after the server that owns it was closed")
	}

	// And the node is a leader again, which is what stopping the following
	// means: it is nobody's replica now.
	if leader, replica := s.following(); replica {
		t.Errorf("a closed server still says it follows %q", leader)
	}
}

// TestAFencedLeaderSaysSo. A store that has been replaced still calls itself a
// leader, still serves reads, and still reports a term — and every write it
// takes is refused. Nothing about it looks wrong from outside, which is why the
// status and the metrics have to say it outright.
func TestAFencedLeaderSaysSo(t *testing.T) {
	s, db := newServer(t, Options{})

	read := func() statusBody {
		t.Helper()

		var body statusBody
		if err := json.Unmarshal(wants(t, do(t, s, http.MethodGet, "/v1/status", nil),
			http.StatusOK), &body); err != nil {
			t.Fatalf("the status is not the status shape: %v", err)
		}
		return body
	}

	if read().Fenced {
		t.Error("a store nobody has replaced reports itself fenced")
	}

	gauge := func() string {
		t.Helper()

		for _, line := range strings.Split(string(wants(t,
			do(t, s, http.MethodGet, metricsPath, nil), http.StatusOK)), "\n") {
			if strings.HasPrefix(line, "litekv_fenced ") {
				return strings.TrimPrefix(line, "litekv_fenced ")
			}
		}
		t.Fatal("there is no litekv_fenced in /metrics")
		return ""
	}

	if got := gauge(); got != "0" {
		t.Errorf("litekv_fenced is %s on a store nobody has replaced", got)
	}

	// Replaced: something on a newer term asks it for records, which is the only
	// way this news ever reaches a leader.
	if _, err := db.Since(litekv.DBPosition{Term: db.Term() + 1}, io.Discard,
		litekv.ReplicaOptions{}); !errors.Is(err, litekv.ErrorFenced) {
		t.Fatalf("asking on a newer term reported '%v', want fenced", err)
	}

	after := read()
	if !after.Fenced {
		t.Error("a fenced store does not say so in its status")
	}
	if after.Role != "leader" {
		t.Errorf("a fenced store reports the role %q; it still calls itself a leader, "+
			"which is exactly why fenced has to be its own field", after.Role)
	}
	if got := gauge(); got != "1" {
		t.Errorf("litekv_fenced is %s on a store that has been replaced", got)
	}

	// And the behaviour the flag is describing: writes refused, reads served.
	wants(t, do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusConflict)
	wants(t, do(t, s, http.MethodGet, "/v1/keys", nil), http.StatusOK)
}
