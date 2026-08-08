package server

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/tillknuesting/litekv"
)

// The leader's half of replication, on the same listener as everything else.
//
// One port and one shutdown, one place for authentication to go when there is
// any, and it goes through whatever load balancer a read replica is already
// behind — a second raw TCP listener would have needed all of that again. What
// it costs is a few bytes of chunked framing per batch, which for the megabyte
// a batch defaults to is not a number worth writing down.
//
// The framing is the one tcp_test.go arrived at over a bare socket, carried
// here unchanged: a kind byte, the position the payload leads to, a length, and
// the payload. A record stream is self-framing, but a reader still has to know
// where one batch stops and the next begins, and a snapshot has to be told
// apart from a batch because the two are applied by different calls.
//
// HTTP gives none of that for free inside a single response body. It gives
// chunking, which the client library unchunks before this code sees it, so the
// body arrives as a byte stream exactly as a socket would.

const (
	frameSnapshot = 'S'
	frameBatch    = 'B'

	// replicaPath is the route, and is also what a Follower appends to the
	// address it is pointed at.
	replicaPath = "/v1/replica/stream"

	// contentTypeStream is what the body is. Not octet-stream: a client that
	// gets one of these by accident should find out from the type rather than
	// from the first byte that fails to parse.
	contentTypeStream = "application/vnd.litekv.replica-stream"

	// defaultMaxFrame bounds what one frame may claim, and so what a follower
	// will hold for one. A snapshot frame is the whole live store, so this is
	// also the largest store that can be replicated over this endpoint.
	defaultMaxFrame = 1 << 30
)

// dbPositionSize is what a litekv.DBPosition takes on the wire.
//
// The engine has a constant for it and does not export it, and writing
// forty-four here would be a number to get wrong the next time a field is added
// to a position — which has happened twice already, once for the term and once
// for the sequence number. Asking MarshalBinary cannot go stale.
var dbPositionSize = func() int {
	encoded, err := litekv.DBPosition{}.MarshalBinary()
	if err != nil {
		panic("server: a zero litekv.DBPosition does not marshal: " + err.Error())
	}
	return len(encoded)
}()

// frameHeader is a kind, the position the payload leads to, and its length.
var frameHeader = 1 + dbPositionSize + 8

// errNoFlusher is a ResponseWriter that cannot be flushed, which every one
// net/http hands a handler can be. It is a 500 because it is this server's
// problem and not the client's, and it exists so the type assertion has
// somewhere to fail to rather than a panic in a goroutine.
var errNoFlusher = errors.New("this response cannot be streamed")

// errFrameTooLarge is a frame claiming more than this follower will take.
//
// It is a sentinel rather than a sentence because it is the one refusal here
// that has to be told apart from a torn frame: both end the stream, and only
// one of them means the other end asked for memory it had no business asking
// for. A test that checks "some error" cannot tell them apart, and the mutation
// that removes the bound entirely leaves a torn frame behind, which looks
// exactly like a connection that went away.
var errFrameTooLarge = errors.New("frame is longer than this follower will take")

// writeFrame sends one frame. The header goes in a single Write, which is what
// lets a test count frames by watching the writes.
func writeFrame(w io.Writer, kind byte, at litekv.DBPosition, payload []byte) error {
	encoded, err := at.MarshalBinary()
	if err != nil {
		return err
	}

	header := make([]byte, frameHeader)
	header[0] = kind
	copy(header[1:1+dbPositionSize], encoded)
	binary.LittleEndian.PutUint64(header[1+dbPositionSize:], uint64(len(payload)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// readFrame takes one frame off r, refusing a payload longer than most.
//
// Two things about the length, and both are about a reader that has to survive
// whatever is on the other end of a socket. It is bounded, because a leader
// this follower has never authenticated is a leader that can name any number.
// And the payload is grown into as the bytes arrive rather than allocated at
// the size claimed: a header claiming a gigabyte with nothing behind it must
// cost what a header claiming nothing costs, or the claim itself is a way to
// make this process ask for a gigabyte per connection.
func readFrame(r io.Reader, most int64) (byte, litekv.DBPosition, []byte, error) {
	header := make([]byte, frameHeader)
	if _, err := io.ReadFull(r, header); err != nil {
		// io.EOF here is a stream that ended between frames, which is the
		// ordinary way one ends. Anything shorter is a torn header.
		return 0, litekv.DBPosition{}, nil, err
	}

	var at litekv.DBPosition
	if err := at.UnmarshalBinary(header[1 : 1+dbPositionSize]); err != nil {
		return 0, litekv.DBPosition{}, nil, err
	}

	length := binary.LittleEndian.Uint64(header[1+dbPositionSize:])
	if most < 0 || length > uint64(most) {
		return 0, litekv.DBPosition{}, nil,
			fmt.Errorf("%w: %d bytes, over the %d allowed", errFrameTooLarge, length, most)
	}

	const chunk = 64 << 10

	payload := make([]byte, 0, min(length, chunk))
	for uint64(len(payload)) < length {
		room := int(min(length-uint64(len(payload)), chunk))

		payload = slices.Grow(payload, room)
		if _, err := io.ReadFull(r, payload[len(payload):len(payload)+room]); err != nil {
			if errors.Is(err, io.EOF) {
				// The header promised more than arrived, which is a torn frame
				// and not a stream that ended tidily.
				err = io.ErrUnexpectedEOF
			}
			return 0, litekv.DBPosition{}, nil, err
		}
		payload = payload[:len(payload)+room]
	}
	return header[0], at, payload, nil
}

// positionOf decodes the from parameter of a stream request.
//
// A position on the wire is base64url of MarshalBinary, without padding, so it
// carries in a query string with nothing escaped. It is deliberately opaque: a
// follower hands back the bytes it was given and has no business taking them
// apart, which is what lets the position gain a field — as it has twice —
// without anything on the client side knowing. It is a cookie, not a structure.
//
// No parameter at all is the zero position, which is a follower with nowhere to
// carry on from and means a snapshot.
func positionOf(raw string) (litekv.DBPosition, error) {
	if raw == "" {
		return litekv.DBPosition{}, nil
	}

	encoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return litekv.DBPosition{}, fmt.Errorf("%w: %s", errBadPosition, err)
	}

	var pos litekv.DBPosition
	if err := pos.UnmarshalBinary(encoded); err != nil {
		return litekv.DBPosition{}, fmt.Errorf("%w: %s", errBadPosition, err)
	}
	return pos, nil
}

// positionParam is the other direction, for a follower saying where it is.
func positionParam(pos litekv.DBPosition) (string, error) {
	encoded, err := pos.MarshalBinary()
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// streamReplica answers GET /v1/replica/stream, which is a leader's side of a
// connection: read where the follower has got to, send it a snapshot if it has
// got nowhere, and then stream until the connection ends.
//
// The response is a 200 that never finishes on its own. Everything that can be
// answered with a status is answered before the first byte of the body, because
// after that there is no status left to send and the only thing a failure can
// do is end the stream.
func (s *Server) streamReplica(w http.ResponseWriter, r *http.Request) {
	from, err := positionOf(r.URL.Query().Get("from"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// Every frame has to leave as it is cut. net/http buffers, and Follow does
	// not return until there is more to send, so a batch left in that buffer is
	// a follower that has not caught up and nothing that would push it out.
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.fail(w, r, errNoFlusher)
		return
	}

	select {
	case <-s.streams:
		// A server on its way down. A stream is not a read: it is a connection
		// meant to stay open, and handing out a new one now would be handing
		// out something about to be taken away.
		s.fail(w, r, litekv.ErrorClosed)
		return
	default:
	}

	// A follower carrying a newer term is how a leader finds out it has been
	// replaced, and it is the only way the news reaches one. Since writes that
	// down and Follow does not, so it is asked here — it costs nothing, because
	// a store that refuses on the term refuses before it reads a record. The
	// asymmetry is in the engine; this stands in for it until it is not.
	if from.Term > s.db.Term() {
		if _, noted := s.db.Since(from, io.Discard, litekv.ReplicaOptions{}); noted != nil {
			s.log.Warn("a follower reported a newer term and it could not be written down",
				"term", from.Term, "err", noted)
		}
		s.fail(w, r, litekv.ErrorFenced)
		return
	}

	// The first snapshot is taken before the status is written, so that a store
	// which refuses one — a fenced leader, a store on its way down, a disk that
	// said no — answers with a status a client can act on rather than with a
	// stream that dies for reasons it cannot see. Every later snapshot happens
	// mid-body, where ending the stream is all there is to do.
	var (
		body    bytes.Buffer
		holding func()
	)
	if from == (litekv.DBPosition{}) {
		at, release, err := s.db.Snapshot(&body, litekv.ReplicaOptions{})
		if err != nil {
			s.fail(w, r, err)
			return
		}
		from, holding = at, release
	}

	w.Header().Set("Content-Type", contentTypeStream)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	// Flushed with nothing behind it, so the follower's request returns as soon
	// as it is connected rather than when the leader next has something to say.
	// A leader with an idle store may have nothing for hours.
	flusher.Flush()

	// The stream ends when the client goes away or when this server is asked to
	// stop serving streams. Follow takes one channel, so the two are merged.
	// The third is this handler returning for any other reason, which net/http
	// would signal by cancelling the request anyway; a test driving the handler
	// with a recorder is the caller that would not.
	served := make(chan struct{})
	defer close(served)

	until := make(chan struct{})
	go func() {
		defer close(until)

		select {
		case <-r.Context().Done():
		case <-s.streams:
		case <-served:
		}
	}()

	send := func(batch []byte, next litekv.DBPosition) error {
		if err := writeFrame(w, frameBatch, next, batch); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	for {
		if holding != nil {
			err := writeFrame(w, frameSnapshot, from, body.Bytes())

			// A snapshot is the whole live store, and this handler lives as
			// long as the connection does. Letting go of it here rather than
			// keeping the buffer around costs one allocation on the rare
			// occasion a second snapshot is needed.
			body = bytes.Buffer{}

			if err != nil {
				holding()
				s.log.Debug("a snapshot did not reach a follower", "err", err)
				return
			}
			flusher.Flush()
		}

		// The hold that came back with the snapshot goes to Follow, which takes
		// one of its own before letting it go. Released here instead there
		// would be a moment with the log the stream starts from unheld — and on
		// a machine with one core that moment is however long it takes the
		// other goroutine to be scheduled, which is long enough to lose it
		// every time.
		_, err := s.db.Follow(from, holding, send, until, litekv.ReplicaOptions{})
		holding = nil

		// A leader answers divergence with a snapshot rather than by hanging
		// up. Nothing holds a log open for a follower that is not connected, so
		// a follower that was away long enough always comes back to a position
		// that is gone; this is not an unusual path but the ordinary fate of
		// one that missed a merge. A leader that treated it as a failed
		// connection would leave that follower asking for the same dead
		// position forever, and no amount of reconnecting would fix it.
		if !errors.Is(err, litekv.ErrorDiverged) {
			if err != nil {
				s.log.Debug("a replication stream ended", "err", err)
			}
			return
		}

		// Asked before the snapshot and not only at the top of the loop: Follow
		// blocks for as long as the store is quiet, so this is the first moment
		// since the stream started at which the client may have gone. Taking a
		// whole-store snapshot for a follower that is no longer there is the
		// most expensive way to find that out.
		select {
		case <-until:
			return
		default:
		}

		at, release, err := s.db.Snapshot(&body, litekv.ReplicaOptions{})
		if err != nil {
			s.log.Debug("a stranded follower could not be sent a snapshot", "err", err)
			return
		}
		from, holding = at, release
	}
}
