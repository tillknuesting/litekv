// Package server puts an HTTP API in front of a litekv.DB.
//
// Nothing in package litekv opens a socket, and nothing in it should: the
// engine has no idea what a request is, and keeping it that way is what lets
// the same store be embedded in a program and served to a network. This package
// is the other half of that bargain. It owns the protocol — the routes, the
// framing, what a key looks like in a URL, which error is which status — and it
// reaches the store through the same exported API any other caller would.
//
// A Server is an http.Handler and nothing more. It does not listen, it does not
// open the store, and it does not close it; cmd/litekvd does all three, and a
// test drives the handler through httptest without a socket anywhere.
//
// # What a request looks like
//
// A value is the body, raw. There is no JSON envelope around a caller's bytes
// and nothing is base64 on the way through, because a key-value store's whole
// job is to hand back what it was given.
//
// A key is percent-encoded in the path. Go's ServeMux unescapes a path segment
// by segment and a %2F is deliberately not a separator, so any byte a key can
// hold survives the trip, including slashes, control bytes and things that are
// not UTF-8 at all. TestKeyOfAnyBytes pins that rather than trusting the
// documentation for it.
//
// With one exception: the empty key, which the store holds happily and which no
// path can name, since a path wildcard does not match an empty segment and
// /v1/keys/ is therefore not a route. It is not out of reach — a batch line
// writes it and a range hands it back — but the single-key routes cannot get
// at it.
package server

import (
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tillknuesting/litekv"
)

// Options configures a Server. The zero value is usable.
type Options struct {
	// MaxValue is the largest value a write may carry, in bytes. Zero means
	// 16 MiB.
	//
	// It bounds what one request can make the server hold, which matters more
	// here than anywhere in the library: a caller of db.Write already had the
	// bytes in its own memory before it called, and a caller over a socket has
	// not spent anything yet.
	MaxValue int64

	// MaxBatch is the largest body POST /v1/batch will take, in bytes. Zero
	// means 32 MiB.
	//
	// Separate from MaxValue because a batch is many values and because it is
	// JSON: base64 costs a third on any key or value that is not text. It is
	// not derived from MaxValue for the same reason — what MaxValue bounds is
	// one record and what this bounds is one request, and it is the request
	// that decides how much a client can make the server hold at once.
	//
	// MaxValue is not applied to the records inside a batch. This is the limit
	// on that route, and two limits on one request would mean two numbers to
	// keep in step and a batch that refused what a PUT accepts. The engine's
	// own bound on a key or a value still applies, and reports
	// ErrorRecordTooLarge, which is the same 413.
	MaxBatch int64

	// MaxScan is the most pairs GET /v1/keys will answer with, and the most a
	// client's own ?limit= may ask for. Zero means 1000.
	//
	// A range holds the store's read lock while it gathers, so an unbounded one
	// is a way for a client to stand in front of the writes; this is the cap
	// that client cannot raise. It counts pairs and not bytes, so a store of
	// large values wants a smaller number here than a store of small ones.
	MaxScan int

	// Queue is how many writes may be waiting to be stored before another
	// handler blocks on the way in. Zero means the Writer's own default.
	//
	// It bounds a group as well as a wait: everything queued goes down as one
	// batch, so a deeper queue is a larger batch under load and more memory
	// held while it is written.
	Queue int

	// Token, if set, is a shared secret every request must carry as
	// `Authorization: Bearer <token>`. Empty means no authentication at all,
	// which is what listening on loopback is for.
	//
	// It guards every route but /health, replication included — that one ships
	// the whole database to whoever asks, so it is the last thing to leave
	// open. /health is exempt because a load balancer probing it is not a
	// client and has no business holding the secret.
	//
	// This is a shared secret and nothing more. There are no users, no scopes
	// and no read-only credential: anything that can read can also write.
	Token string

	// Logger is where a request that failed for a reason the client is not told
	// gets written down, and where every request is written at Debug. Nil means
	// slog.Default().
	Logger *slog.Logger
}

const defaultMaxValue = 16 << 20

// Server serves a litekv.DB over HTTP. It is safe for concurrent use, which is
// what an http.Handler has to be.
type Server struct {
	db   *litekv.DB
	opts Options
	log  *slog.Logger
	mux  *http.ServeMux

	// writes is where a handler puts a record, and it is a litekv.Writer in
	// front of the store rather than the store itself. See the type.
	writes writes

	// writer is that Writer, kept so Close can stop it. It is the same object;
	// the two fields exist because the benchmark next door swaps the interface
	// for the store to measure what the queue is worth.
	writer *litekv.Writer

	// streams is closed when this server stops serving replication streams.
	// See CloseStreams, which is the only thing that closes it.
	streams    chan struct{}
	endStreams sync.Once

	// role is whether this node is following somebody, and the follower doing
	// it. See role.go: the store cannot answer that question, because the thing
	// pulling the records is up here.
	role role

	// metrics is what this server knows about itself. See ops.go.
	metrics *metrics
}

// writes is the calls a handler makes to store something. A *litekv.DB
// satisfies it and so does a *litekv.Writer in front of one, which is the whole
// reason it is an interface: BenchmarkWriteThroughTheHandler measures the same
// handler both ways.
//
// WriteBatch is in it so that POST /v1/batch goes through the queue like
// everything else. A batch arriving straight at the store would take every
// shard of its lock, which is exactly what the queue is here to stop, and it
// would do it for longer than a single write does.
type writes interface {
	Write(key, value []byte) error
	WriteExpiring(key, value []byte, at time.Time) error
	Delete(key []byte) error
	WriteBatch(b *litekv.Batch) error
}

// New returns a Server handing requests to db.
//
// It does not take ownership of db: the caller opened it and the caller closes
// it. A request that arrives after the store is closed is answered 503 rather
// than crashing, since the store reports ErrorClosed instead of panicking.
//
// It does start a goroutine — the writer's — so a Server has to be closed even
// though the store it serves is somebody else's.
func New(db *litekv.DB, opts Options) *Server {
	if opts.MaxValue <= 0 {
		opts.MaxValue = defaultMaxValue
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = defaultMaxBatch
	}
	if opts.MaxScan <= 0 {
		opts.MaxScan = defaultMaxScan
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	// A handler per request means a goroutine per request, and writes take
	// every shard of the store's lock: two of them do not merely fail to go
	// faster, they halve the store's throughput. The queue in front is what
	// stops that, and what turns everything waiting into one write to the log
	// and one sync. writer.go was written for exactly this caller.
	writer := db.Writer(litekv.WriterOptions{Queue: opts.Queue})

	s := &Server{
		db:      db,
		opts:    opts,
		log:     opts.Logger,
		mux:     http.NewServeMux(),
		writes:  writer,
		writer:  writer,
		streams: make(chan struct{}),
		metrics: newMetrics(),
	}

	// GET also matches HEAD, which is what makes asking for a value's size
	// without fetching it free. The mux answers a path it knows with a method
	// it does not with 405 and an Allow header of its own accord.
	s.handle("GET /v1/keys/{key}", s.readKey)
	s.handle("PUT /v1/keys/{key}", s.writeKey)
	s.handle("DELETE /v1/keys/{key}", s.deleteKey)

	// Several at once, and ranges. /v1/keys is the exact path and does not
	// collide with /v1/keys/{key} above it: a pattern without a trailing slash
	// matches that path and nothing under it, and a wildcard will not match an
	// empty segment, so /v1/keys/ is still nothing at all.
	// TestScanDoesNotCollideWithOneKey holds all three of those.
	s.handle("POST /v1/batch", s.writeBatch)
	s.handle("GET /v1/keys", s.scanKeys)

	// Replication rides this listener rather than one of its own. One port to
	// open, one thing to shut down, one place for authentication to go when
	// there is any, and it goes through whatever proxy or load balancer a read
	// replica is already behind — a second raw TCP listener would have needed
	// every one of those again.
	s.handle("GET "+replicaPath, s.streamReplica)

	// Which of the two this node is, and the one call that changes it.
	s.handle("GET /v1/status", s.status)
	s.handle("POST /v1/promote", s.promote)

	// What somebody running this asks it. See ops.go for why /health is the one
	// route the token does not cover.
	s.handle("GET "+healthPath, s.health)
	s.handle("GET "+metricsPath, s.exposition)

	return s
}

// handle registers one route, counted and timed under the pattern it is
// registered with. Everything goes through here: a route registered straight on
// the mux is a route missing from /metrics, and nothing would say so.
func (s *Server) handle(pattern string, h http.HandlerFunc) {
	route := pattern
	if _, path, found := strings.Cut(pattern, " "); found {
		route = path
	}
	s.mux.HandleFunc(pattern, s.instrument(route, h))
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Before the mux, so that a route this server does not have is refused on
	// the token rather than telling a stranger which routes exist.
	if !s.allowed(r) {
		unauthorized(w)
		return
	}

	s.mux.ServeHTTP(w, r)
}

// CloseStreams ends every replication stream this server is serving, and
// answers a request for a new one with 503. It touches neither the writer nor
// the store, so reads and writes carry on. Calling it twice is harmless, and
// Close calls it.
//
// It exists because a stream is a request that never finishes on its own, and
// http.Server.Shutdown waits for one. Shutdown closes the listeners and then
// waits for every connection to go idle; it does not cancel a request's
// context, so a leader with one follower attached would spend its whole
// shutdown timeout waiting for a handler that was never going to return. Hand
// this to (*http.Server).RegisterOnShutdown and it goes down in the time the
// ordinary requests take.
//
// Ending a stream abruptly costs the follower nothing: it reconnects, which is
// what it does about any connection ending, and it comes back at the position
// it had reached.
func (s *Server) CloseStreams() {
	s.endStreams.Do(func() { close(s.streams) })
}

// Close ends the replication streams, then stops the writer once everything
// queued has been stored and answers the handlers waiting on it. Closing twice
// is harmless.
//
// The order matters on the way down and there are three things in it. Stop
// taking requests first, so nothing new arrives; then close this, which lets
// the handlers still in flight be answered; then close the store. Closing the
// store first turns writes that were a moment from being acknowledged into
// ErrorClosed, for no reason other than the order they were shut down in.
//
// A closed Server still answers reads, because the store is still open and
// still holds everything. A stream is the one thing it stops answering, for the
// reason CloseStreams gives: it is a connection meant to stay open, not a read.
//
// It does not close the store. That is the caller's, and it goes last.
func (s *Server) Close() error {
	s.CloseStreams()

	// Before the writer, and for the same reason the writer goes before the
	// store: the follower writes to the store too, and it is the one thing here
	// that does so without going through the queue.
	if err := s.stopFollowing(); err != nil {
		s.log.Error("stopping the follower", "err", err)
	}
	return s.writer.Close()
}
