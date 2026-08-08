package server

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/tillknuesting/litekv"
)

// Several records at once, stored all of them or none of them.
//
// The body is newline-delimited JSON — the encoding in ndjson.go — one
// operation to a line:
//
//	{"op":"write","key":"a","value":"1"}
//	{"op":"write","key_b64":"_w","value":"","expires":"2030-01-01T00:00:00Z"}
//	{"op":"delete","key":"gone"}
//
// All or nothing means two things here and the route has to provide both. The
// engine provides the second: litekv.WriteBatch puts the records down behind a
// marker and recovery discards from that marker on unless every one of them is
// there. This file provides the first: the whole body is parsed and understood
// before any of it is handed to the store, so a line the server cannot read is a
// request it refuses rather than a request it performs half of. A parser that
// stored as it went would make the marker pointless — the batch would be atomic
// on the disk and torn on the wire.
//
// It goes through the writer's queue like every other write on this server, and
// for the same reason: a handler per request is a goroutine per request, and a
// write takes every shard of the store's lock. A batch arriving straight at the
// store is the same contention a PUT would be, only longer.

// defaultMaxBatch is how much of a body POST /v1/batch will take.
//
// Larger than defaultMaxValue on purpose, and the ratio is the reason: base64
// costs a third, so a batch carrying one value of the largest size a PUT will
// take needs about 1.34x that before its own JSON is counted. Twice leaves
// room for the envelope and for a few of them. An operator who moves -max-value
// should look at this one as well; the two are not derived from each other,
// because a batch bounds a request and a value bounds a record and only one of
// those is what the memory is proportional to.
const defaultMaxBatch = 32 << 20

// writeBatch stores every operation in the body, or none of them.
//
// It answers 204 and no body, which is what a PUT and a DELETE answer, and for
// the same reason: there is nothing to say that the status has not said. In
// particular there is no count — a batch of ten writes stores ten records
// whether or not any key in it was already there, and a number that is always
// the number of lines sent is a number nobody can learn anything from.
func (s *Server) writeBatch(w http.ResponseWriter, r *http.Request) {
	if !s.mayWrite(w, r) {
		return
	}

	// Refused on the declared length before anything is read, exactly as a PUT
	// is: a client announcing a gigabyte should be turned away at the header
	// rather than after the server has taken MaxBatch of it and parsed it. The
	// reader below is still needed, because a chunked body declares nothing.
	if r.ContentLength > s.opts.MaxBatch {
		writeError(w, http.StatusRequestEntityTooLarge,
			tooLargeFor(s.opts.MaxBatch))
		return
	}

	batch, err := parseBatch(http.MaxBytesReader(w, r.Body, s.opts.MaxBatch), s.opts.MaxBatch)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// The keys and values in batch are still alive here, and unchanged, which
	// is what litekv.Batch requires of whatever built it: it did not copy them
	// when they were added and it reads them now. See fromTheWire.
	if err := s.writes.WriteBatch(batch); err != nil {
		s.fail(w, r, err)
		return
	}

	s.wrote(w)
	w.WriteHeader(http.StatusNoContent)
}

// parseBatch reads a whole NDJSON body into a litekv.Batch, or fails without
// having built one.
//
// max bounds the body and also bounds one line of it, which is the same number
// for a good reason: a body is at most max bytes, so a line cannot honestly be
// longer, and giving a line its own smaller limit would refuse a legitimate
// batch of one large value for a reason nothing in the API mentions.
//
// A blank line is skipped rather than refused. Every NDJSON body ends with a
// newline and there is nothing wrong with a client that separates its records
// with two.
func parseBatch(body io.Reader, max int64) (*litekv.Batch, error) {
	var batch litekv.Batch

	lines := bufio.NewScanner(body)

	// The starting buffer has to be no larger than the limit or the limit does
	// nothing: a bufio.Scanner checks its maximum only when it has to grow, so
	// one that starts out larger than the maximum never reaches the check.
	lines.Buffer(make([]byte, 0, min(64<<10, max)), int(max))

	for n := 1; lines.Scan(); n++ {
		line := bytes.TrimSpace(lines.Bytes())
		if len(line) == 0 {
			continue
		}

		if err := addTo(&batch, line); err != nil {
			// A body cut short in the middle of a line arrives as a line that
			// is not JSON, and blaming the line would be blaming the wrong
			// thing: the reader is what failed, and "the body is too long" is
			// the answer a client can act on. Whatever it has to say comes
			// first, and it is already there — a Scanner sets its error on the
			// same call that hands back the last partial token.
			if lines.Err() != nil {
				break
			}
			return nil, badRequest("line %d: %v", n, err)
		}
	}

	if err := lines.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			// The refusal MaxBytesReader makes, from the other side of it: one
			// line as long as the whole budget. Reported as the same error so
			// that a client is told about one limit rather than two, and so
			// that a caller of this function without a MaxBytesReader in front
			// of it — a test, a fuzz target — is still bounded.
			return nil, &http.MaxBytesError{Limit: max}
		}
		return nil, err
	}

	return &batch, nil
}

// addTo reads one line and adds what it says to the batch.
//
// Nothing here is shared with the next line. Every key and every value is its
// own allocation, which is what a litekv.Batch needs of anything handed to it
// and is the one thing in this file that is not obvious from reading it: the
// batch holds the slices rather than copying them, so a decode buffer reused
// across the lines would store the last line's bytes under all of the keys and
// would do it without an error anywhere.
func addTo(batch *litekv.Batch, line []byte) error {
	op, err := decodePair(line)
	if err != nil {
		return err
	}

	key, err := fromTheWire(op.Key, op.KeyB64, "key")
	if err != nil {
		return err
	}

	switch op.Op {
	case opWrite:
		value, err := fromTheWire(op.Value, op.ValueB64, "value")
		if err != nil {
			return err
		}

		if op.Expires == "" {
			batch.Write(key, value)
			return nil
		}

		// The same format and the same meaning as the Litekv-Expires header on
		// a PUT: an instant, not a duration, because a duration has to be
		// resolved against somebody's clock.
		at, err := time.Parse(time.RFC3339Nano, op.Expires)
		if err != nil {
			return errors.New("expires must be an RFC 3339 time")
		}
		batch.WriteExpiring(key, value, at)
		return nil

	case opDelete:
		// A delete carrying a value or an expiry is refused rather than having
		// them dropped. Both are a client that thinks a delete does something
		// it does not, and storing the tombstone anyway would confirm it.
		if op.Value != nil || op.ValueB64 != nil {
			return errors.New("a delete carries no value")
		}
		if op.Expires != "" {
			return errors.New("a delete does not expire")
		}

		batch.Delete(key)
		return nil
	}

	// Including the empty one, which is a line with no op at all. There is no
	// default operation: a client that leaves it out has left out the only
	// field saying whether this line writes something or removes it.
	return errors.New(`op must be "write" or "delete"`)
}
