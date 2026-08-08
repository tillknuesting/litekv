package server

import (
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	// headerExpires asks for a record that stops answering once the instant it
	// names has passed. It is an instant and not a duration for the same reason
	// the store's expiry is one: a duration has to be resolved against
	// somebody's clock, and the only clock a client and a server agree on is
	// the one they both write down. A client thinking in TTLs subtracts.
	headerExpires = "Litekv-Expires"

	// contentTypeValue is what a value is, as far as HTTP is concerned. The
	// store does not know what is in it and neither does this.
	contentTypeValue = "application/octet-stream"
)

// readKey answers GET and HEAD for one key.
func (s *Server) readKey(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))

	// Before the read and not after: a client that said "not from a store older
	// than this" is asking to be refused rather than answered with what is here.
	if !s.notStale(w, r) {
		return
	}

	// Read and not View. View hands out the stored bytes without copying them
	// and holds the store's read lock until the callback returns — and the
	// callback here writes to a socket, so a client that stops reading would
	// decide how long the store keeps that lock. A rotation or a merge finishing
	// needs the write lock, so that client would be deciding when the store may
	// rotate. A copy per read is what it costs not to let it.
	value, err := s.db.Read(key)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	w.Header().Set("Content-Type", contentTypeValue)
	w.Header().Set("Content-Length", strconv.Itoa(len(value)))
	w.WriteHeader(http.StatusOK)

	if r.Method == http.MethodHead {
		// net/http would discard the body anyway. Not writing it saves copying
		// a value onto a socket that will throw it away, which is the whole
		// point of asking with HEAD.
		return
	}

	if _, err := w.Write(value); err != nil {
		// The status is long gone and there is nothing to tell the client. This
		// is a client that went away mid-value, which is ordinary.
		s.log.Debug("value not delivered", "path", r.URL.Path, "err", err)
	}
}

// writeKey stores the body under one key.
func (s *Server) writeKey(w http.ResponseWriter, r *http.Request) {
	if !s.mayWrite(w, r) {
		return
	}

	key := []byte(r.PathValue("key"))

	expires, err := expiryOf(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// Refused on the declared length before anything is read, so a client
	// announcing a gigabyte is turned away at the header rather than after the
	// server has taken MaxValue of it. The reader below is still needed: a
	// chunked body declares nothing.
	if r.ContentLength > s.opts.MaxValue {
		writeError(w, http.StatusRequestEntityTooLarge, tooLargeFor(s.opts.MaxValue))
		return
	}

	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.opts.MaxValue))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	if expires.IsZero() {
		err = s.writes.Write(key, value)
	} else {
		err = s.writes.WriteExpiring(key, value, expires)
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}

	s.wrote(w)
	w.WriteHeader(http.StatusNoContent)
}

// deleteKey writes a tombstone for one key.
//
// Deleting a key that was never there is not an error and does not answer 404.
// The store appends a tombstone either way — it cannot know whether an older
// log holds the key without looking, and a delete does not look — so the honest
// answer is that the deletion is stored, which is what 204 says.
func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request) {
	if !s.mayWrite(w, r) {
		return
	}

	if err := s.writes.Delete([]byte(r.PathValue("key"))); err != nil {
		s.fail(w, r, err)
		return
	}

	s.wrote(w)
	w.WriteHeader(http.StatusNoContent)
}

// expiryOf reads headerExpires. A request without one gets the zero time, which
// is a record that never expires.
//
// The failure is a clientError so that it goes through statusOf with every
// other 400 this package answers. What it says is the client's mistake and
// nothing about the server, which is what makes it safe to hand back verbatim.
func expiryOf(r *http.Request) (time.Time, error) {
	raw := r.Header.Get(headerExpires)
	if raw == "" {
		return time.Time{}, nil
	}

	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, badRequest("%s must be an RFC 3339 time", headerExpires)
	}
	return at, nil
}
