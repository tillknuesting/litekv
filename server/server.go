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
// With one exception: the empty key, which the store holds happily and which
// has no spelling here. A path wildcard does not match an empty segment, so
// /v1/keys/ is not a route.
package server

import (
	"log/slog"
	"net/http"
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

	// Queue is how many writes may be waiting to be stored before another
	// handler blocks on the way in. Zero means the Writer's own default.
	//
	// It bounds a group as well as a wait: everything queued goes down as one
	// batch, so a deeper queue is a larger batch under load and more memory
	// held while it is written.
	Queue int

	// Logger is where a request that failed for a reason the client is not told
	// gets written down. Nil means slog.Default().
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
}

// writes is the three calls a handler makes to store something. A *litekv.DB
// satisfies it and so does a *litekv.Writer in front of one, which is the whole
// reason it is an interface: BenchmarkWriteThroughTheHandler measures the same
// handler both ways.
type writes interface {
	Write(key, value []byte) error
	WriteExpiring(key, value []byte, at time.Time) error
	Delete(key []byte) error
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
		db:     db,
		opts:   opts,
		log:    opts.Logger,
		mux:    http.NewServeMux(),
		writes: writer,
		writer: writer,
	}

	// GET also matches HEAD, which is what makes asking for a value's size
	// without fetching it free. The mux answers a path it knows with a method
	// it does not with 405 and an Allow header of its own accord.
	s.mux.HandleFunc("GET /v1/keys/{key}", s.readKey)
	s.mux.HandleFunc("PUT /v1/keys/{key}", s.writeKey)
	s.mux.HandleFunc("DELETE /v1/keys/{key}", s.deleteKey)

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close stops the writer once everything queued has been stored, and answers
// the handlers waiting on it. Closing twice is harmless.
//
// The order matters on the way down and there are three things in it. Stop
// taking requests first, so nothing new arrives; then close this, which lets
// the handlers still in flight be answered; then close the store. Closing the
// store first turns writes that were a moment from being acknowledged into
// ErrorClosed, for no reason other than the order they were shut down in.
//
// It does not close the store. That is the caller's, and it goes last.
func (s *Server) Close() error {
	return s.writer.Close()
}
