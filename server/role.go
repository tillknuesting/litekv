package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/tillknuesting/litekv"
)

// Which of the two this node is, and what a client can ask about it.
//
// The store has no notion of a role. It has a term, and it refuses writes when
// it has heard of a newer one — that is fencing, and it is the whole of what the
// engine knows. A role is a fact about this process: whether something here is
// pulling another node's records into this store. Nothing in the engine can
// answer that, because the thing doing the pulling is up here.
//
// It matters because the two are not interchangeable. A store that is following
// will take a write perfectly happily, put it in its own log, and go on applying
// the leader's records around it — and the two histories never reconcile. The
// leader's position marches on over records this store does not have, its own
// record sits under a key the leader will overwrite from somewhere else, and no
// checksum anywhere is wrong. That is the failure this file exists to prevent,
// and until it existed the only thing standing in the way was a warning in a
// log.

const (
	// headerLeader comes back with a write a replica refused, so that a client
	// that guessed wrong is told where to go rather than left to find out.
	headerLeader = "Litekv-Leader"

	// headerPosition comes back with every write: where this store had got to
	// once the record was stored. It is a cookie for headerAfter below.
	headerPosition = "Litekv-Position"

	// headerAfter is a client saying "not from a store older than this". It
	// carries a position it was given by headerPosition.
	headerAfter = "Litekv-After"

	// headerWait says how long it may take. Without it a store that has not got
	// there says so at once.
	headerWait = "Litekv-Wait"
)

// errFollowing is a write aimed at a node that is following somebody. It is a
// 409 rather than a 403: the request was allowed, this is not the node to send
// it to, and which node is has changed and may change again.
var errFollowing = errors.New("this store is following a leader and does not take writes")

// errNotReached is a read from a store that has not got as far as the client
// has already seen. Separate from litekv.ErrorStale so that the status can be a
// 412 — a precondition the client set, and did not get — rather than something
// the store thinks is wrong.
var errNotReached = errors.New("this store has not reached that position")

// errWaited is the same thing after waiting for it. A 504: the client asked for
// time and the time ran out, which says nothing about whether it ever will.
var errWaited = errors.New("this store did not reach that position in time")

// role is what this process is doing, guarded on its own because promotion
// changes it while requests are in flight.
type role struct {
	mu     sync.RWMutex
	leader string       // where this node follows, empty if it leads
	stop   func() error // stops the follower, nil if there is none
}

// following reports where this node is following, and whether it is.
func (s *Server) following() (string, bool) {
	s.role.mu.RLock()
	defer s.role.mu.RUnlock()

	return s.role.leader, s.role.leader != ""
}

// Follow points this server's store at a leader and marks it a replica: it
// stops taking writes, says where they should go instead, and reports itself as
// one in the status.
//
// This rather than the package-level Follow, for anything with a Server in
// front of it. The two have to agree about the role — a Follower started behind
// the Server's back leaves it answering writes it should be refusing — and this
// is what keeps them from disagreeing. Promote stops it, and so does Close.
func (s *Server) Follow(leader string, opts FollowerOptions) error {
	s.role.mu.Lock()
	defer s.role.mu.Unlock()

	if s.role.stop != nil {
		return errors.New("this server is already following a leader")
	}
	if opts.Logger == nil {
		opts.Logger = s.log
	}

	f, err := Follow(s.db, leader, opts)
	if err != nil {
		return err
	}

	s.role.leader, s.role.stop = leader, f.Close
	return nil
}

// stopFollowing ends the following, if there is any, and returns this node to
// being a leader. It is what Promote and Close both need.
func (s *Server) stopFollowing() error {
	s.role.mu.Lock()
	stop := s.role.stop
	s.role.leader, s.role.stop = "", nil
	s.role.mu.Unlock()

	// Outside the lock: Close waits for the goroutine, and that goroutine is
	// applying records to the store. Holding the role lock across it would put
	// it in front of every request the store is serving meanwhile.
	if stop == nil {
		return nil
	}
	return stop()
}

// Promote makes this node a leader: it stops following, and raises the store's
// term so that the node it was following is fenced the next time it hears from
// anything on the new one.
//
// The order is the point. Following stops first, because a term raised while
// records are still arriving is a store that fences its own leader and then
// applies another of its batches. Nothing is corrupted by that — the position
// check refuses a batch that does not continue this log — but it is a promotion
// that half happened, and the half is not one anybody can reason about.
//
// What this does not do is decide that this node should be the leader. That is
// consensus, and it is not here: something outside decides, and this is how the
// decision is written down. Raising the term in two places at once puts two
// stores on the same term and gives the guarantee away.
func (s *Server) Promote() (uint64, error) {
	if err := s.stopFollowing(); err != nil {
		return 0, err
	}
	return s.db.Promote()
}

// promote answers POST /v1/promote.
func (s *Server) promote(w http.ResponseWriter, r *http.Request) {
	term, err := s.Promote()
	if err != nil {
		s.fail(w, r, err)
		return
	}

	s.log.Warn("promoted", "term", term)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		Term uint64 `json:"term"`
	}{term})
}

// statusBody is what GET /v1/status answers with.
//
// The positions are the opaque base64url a client hands back, not their fields:
// what a client can do with a position is give it to headerAfter, and taking one
// apart out here would be a second place that has to know the format.
type statusBody struct {
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	Leader   string `json:"leader,omitempty"`
	Position string `json:"position"`
	Applied  string `json:"applied,omitempty"`
	Segments int    `json:"segments"`
	Keys     int    `json:"keys"`
}

// status answers GET /v1/status.
//
// Keys is Len, which counts tombstones, and Segments counts logs. Neither is a
// number to compare between two stores — a follower that came back by way of a
// snapshot holds no tombstones, and both stores are right while the counts
// differ. They are here to be watched over time on one node.
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	leader, replica := s.following()

	body := statusBody{Role: "leader", Term: s.db.Term(), Leader: leader,
		Segments: s.db.Segments(), Keys: s.db.Len()}
	if replica {
		body.Role = "replica"
	}

	var err error
	if body.Position, err = positionParam(s.db.Position()); err != nil {
		s.fail(w, r, err)
		return
	}
	if applied := s.db.Applied(); applied != (litekv.DBPosition{}) {
		if body.Applied, err = positionParam(applied); err != nil {
			s.fail(w, r, err)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

// mayWrite refuses a write aimed at a replica, and answers it if it did.
//
// Checked here and not left to the store, because the store would take it. A
// following store is not a fenced one: it holds its leader's term, so
// ErrorFenced never fires, and the record goes into its log beside the records
// it is pulling in. Fencing is about two leaders; this is about a node that is
// not one at all.
func (s *Server) mayWrite(w http.ResponseWriter, r *http.Request) bool {
	leader, replica := s.following()
	if !replica {
		return true
	}

	w.Header().Set(headerLeader, leader)
	s.fail(w, r, errFollowing)
	return false
}

// wrote puts the position on a write that succeeded, waits for as many
// followers as Options.WaitFor asks for, and returns the status to answer with.
//
// The position is the store's and not the record's: at or after the write, never
// before it, which is all headerAfter needs. Asking for the record's own would
// mean the write path handing one back through the queue, and a position that is
// merely at-or-after is the same answer to the only question anybody asks of it.
//
// 204 means the write is stored and as replicated as it was asked to be. 202
// means it is stored and it is not — the record exists either way, and a client
// that reads 202 as a failure and retries will write it twice. That is what a
// semi-synchronous write can honestly say; see acks.go for why it cannot say
// more.
func (s *Server) wrote(w http.ResponseWriter, r *http.Request) int {
	at := s.db.Position()

	if encoded, err := positionParam(at); err == nil {
		w.Header().Set(headerPosition, encoded)
	}

	need := s.opts.WaitFor
	if need <= 0 {
		return http.StatusNoContent
	}

	got := s.followers.await(r.Context(), at, need, s.opts.waitTimeout())
	w.Header().Set(headerReplicated, strconv.Itoa(got))

	if got >= need {
		return http.StatusNoContent
	}

	s.log.Warn("a write was not replicated as far as asked",
		"followers", got, "wanted", need)
	return http.StatusAccepted
}

// notStale holds a read to headerAfter, if the client sent one.
//
// This is the read-your-writes arrangement, and it only means anything on a
// replica: a client writes to the leader, is handed a position, and sends it
// with the reads it makes afterwards to whichever node a load balancer picks. A
// replica that has not got there says so rather than answering with what it has,
// which is the whole difference between a stale read and a wrong one.
func (s *Server) notStale(w http.ResponseWriter, r *http.Request) bool {
	raw := r.Header.Get(headerAfter)
	if raw == "" {
		return true
	}

	pos, err := positionOf(raw)
	if err != nil {
		s.fail(w, r, err)
		return false
	}

	wait, err := waitFor(r)
	if err != nil {
		s.fail(w, r, err)
		return false
	}

	if wait == 0 {
		if err := s.db.Reached(pos); err != nil {
			s.staleness(w, r, err, errNotReached)
			return false
		}
		return true
	}

	// The request's own context is in the channel too, so a client that hangs
	// up is not waited for on its behalf.
	timer := time.NewTimer(wait)
	defer timer.Stop()

	until := make(chan struct{})
	done := make(chan struct{})
	defer close(done)

	go func() {
		defer close(until)

		select {
		case <-timer.C:
		case <-r.Context().Done():
		case <-done:
		}
	}()

	if err := s.db.Await(pos, until); err != nil {
		s.staleness(w, r, err, errWaited)
		return false
	}
	return true
}

// staleness answers a read that was refused on its position. Anything that is
// not the store being behind — a position from a leader that has been replaced,
// a closed store — is that thing and not this.
func (s *Server) staleness(w http.ResponseWriter, r *http.Request, err, behind error) {
	if errors.Is(err, litekv.ErrorStale) {
		// What this store does hold, so a client can decide whether to ask
		// again here or go somewhere else.
		if at, err := positionParam(s.db.Position()); err == nil {
			w.Header().Set(headerPosition, at)
		}
		s.fail(w, r, behind)
		return
	}
	s.fail(w, r, err)
}

// waitFor reads headerWait. No header is no waiting.
func waitFor(r *http.Request) (time.Duration, error) {
	raw := r.Header.Get(headerWait)
	if raw == "" {
		return 0, nil
	}

	wait, err := time.ParseDuration(raw)
	if err != nil {
		return 0, badRequest("%s must be a duration, such as 2s", headerWait)
	}
	if wait < 0 {
		return 0, badRequest("%s cannot be negative", headerWait)
	}
	return wait, nil
}
