package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/tillknuesting/litekv"
)

// What a leader knows about the nodes following it, and what a write can be
// made to wait for.
//
// Replication here has always been asynchronous: a write returns as soon as the
// leader has it, so a leader that dies loses whatever its followers had not
// received. Semi-synchronous replication is the answer to exactly that, and it
// needs three things this server did not have — a leader that knows who is
// following it, a follower that says how far it has got, and a write that waits.
//
// # The one thing this cannot do
//
// The record is in the leader's log before any of this waits. It has to be:
// there is nothing to replicate until it is written, and nothing here can
// unwrite it. So a wait that runs out is *reported* and never undone — the
// answer is 202 rather than 204 and Litekv-Replicated says how many followers
// had it — and a client that reads 202 as failure and retries will write the
// record twice.
//
// That is the honest shape of semi-synchronous replication and not a shortcut.
// What it buys is that a 204 means a failover will not lose this write. What it
// does not buy is a write that can be taken back.

const (
	// headerReplicated is how many followers had the write when the leader
	// answered. It is on every write once WaitFor is set, including the ones
	// that did not reach it.
	headerReplicated = "Litekv-Replicated"

	// ackPath is where a follower says how far it has got. A route of its own
	// rather than something coming back up the stream, because a stream's
	// response body only goes one way and the alternative — a request body the
	// follower writes to while reading the response — is full-duplex HTTP/1.1,
	// which is the thing proxies break. Riding one listener was the reason for
	// choosing HTTP; a scheme a proxy mangles gives that back.
	ackPath = "/v1/replica/ack"

	// defaultWaitTimeout is how long a write waits for its followers before
	// giving up and saying so.
	defaultWaitTimeout = 5 * time.Second
)

// followers is the registry: who is streaming, and how far each has said it has
// got.
//
// Keyed by an id the follower makes up, because the leader has nothing else to
// key on — two followers behind one NAT share an address, and a follower that
// reconnects has a new port. The id is opaque and this places no meaning on it
// beyond telling one follower from another.
type followers struct {
	mu sync.Mutex
	at map[string]litekv.DBPosition

	// changed is closed and replaced every time a follower moves, which is how
	// a waiting write finds out without polling. The same arrangement as the
	// engine's Changed, and for the same reason.
	changed chan struct{}
}

func newFollowers() *followers {
	return &followers{at: map[string]litekv.DBPosition{}, changed: make(chan struct{})}
}

// attach adds a follower that has just opened a stream, at whatever position it
// asked from.
func (f *followers) attach(id string, from litekv.DBPosition) {
	if id == "" {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Not overwritten if it is already there with something newer: a follower
	// that reconnects while its old handler is still shutting down would
	// otherwise take itself backwards.
	if was, ok := f.at[id]; ok && reaches(was, from) {
		return
	}
	f.at[id] = from
	f.wake()
}

// detach forgets a follower whose stream has ended.
//
// Forgotten rather than kept with its last position, and that is the point of
// it: a write waits for followers that have the record, and a follower that is
// not connected is not going to acknowledge anything. Counting one that left an
// hour ago would make WaitFor a number about the past.
func (f *followers) detach(id string) {
	if id == "" {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.at, id)
	f.wake()
}

// ack records how far a follower says it has got.
//
// A follower this leader is not streaming to is ignored rather than added. An
// ack is a claim, and the only thing that makes it worth anything is that this
// leader is the one sending that follower records; taking one from a stranger
// would let anything that can reach this route satisfy a semi-synchronous
// write by asserting it had the data.
func (f *followers) ack(id string, at litekv.DBPosition) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	was, streaming := f.at[id]
	if !streaming {
		return false
	}
	if reaches(was, at) {
		return true // an ack that arrived out of order, which a retry can do
	}

	f.at[id] = at
	f.wake()
	return true
}

// wake tells everything waiting that something moved. Callers hold f.mu.
func (f *followers) wake() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// count is how many followers have reached want, and the channel that closes
// when any of them moves.
func (f *followers) count(want litekv.DBPosition) (int, <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()

	got := 0
	for _, at := range f.at {
		if reaches(at, want) {
			got++
		}
	}
	return got, f.changed
}

// await waits until need followers have reached want, and returns how many did.
//
// It returns early on the request's own context, because a client that has gone
// away is not owed a wait — the record is stored either way, and holding the
// handler open only holds a goroutine.
func (f *followers) await(ctx context.Context, want litekv.DBPosition, need int,
	wait time.Duration) int {

	timeout := time.NewTimer(wait)
	defer timeout.Stop()

	for {
		got, changed := f.count(want)
		if got >= need {
			return got
		}

		select {
		case <-changed:
		case <-timeout.C:
			got, _ := f.count(want)
			return got
		case <-ctx.Done():
			got, _ := f.count(want)
			return got
		}
	}
}

// reaches reports whether a follower at acked holds everything up to want.
//
// By the sequence number, which is what makes two positions comparable at all:
// every record carries one, they only go up, and they survive a merge — which
// offsets and log numbers do not. The term is checked first because a position
// from a different leader is not on this history and its numbers say nothing
// about it.
func reaches(acked, want litekv.DBPosition) bool {
	if acked.Term != want.Term {
		return acked.Term > want.Term
	}
	return acked.Log.Seq >= want.Log.Seq
}

// ackBody is what a follower posts.
type ackBody struct {
	ID string `json:"id"`
	At string `json:"at"`
}

// acknowledge answers POST /v1/replica/ack.
func (s *Server) acknowledge(w http.ResponseWriter, r *http.Request) {
	var said ackBody

	// Small and fixed, so the limit is small and fixed. A body over it is a
	// client that is not a follower.
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&said); err != nil {
		s.fail(w, r, badRequest("an acknowledgement is {\"id\":…,\"at\":…}"))
		return
	}

	at, err := positionOf(said.At)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	if !s.followers.ack(said.ID, at) {
		// Not an error the follower can act on, and not a 404 either: the
		// follower is right that it exists, and this leader is right that it is
		// not streaming to it. It says so and the follower carries on.
		writeError(w, http.StatusConflict, "this leader is not streaming to that follower")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// attached is how many followers this leader could wait for.
func (s *Server) attached() int {
	s.followers.mu.Lock()
	defer s.followers.mu.Unlock()

	return len(s.followers.at)
}
