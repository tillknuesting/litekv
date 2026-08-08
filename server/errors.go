package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/tillknuesting/litekv"
)

// The store's errors are a small closed set and every one of them means
// something a status code already means. Mapping them in one place rather than
// in each handler is not tidiness: it is the only way the mapping stays the
// same across routes, and a client that learns what a 404 means from one route
// is right about the others.

// headerTerm carries the store's term on a refusal that turns on it, so a
// client that was talking to a leader can tell "it has been replaced" from
// "something went wrong".
const headerTerm = "Litekv-Term"

// errBadPosition is a from parameter that is not a position at all: not
// base64url, or the wrong number of bytes, or bytes the store refuses to
// decode. The store never saw it, so it is not a store error, but it goes
// through the same mapping so that every route answers a request it could not
// read in the same way.
var errBadPosition = errors.New("from is not a position")

// errorBody is what a failed request answers with. One field, because a client
// that wants to branch on the failure branches on the status; the string is for
// whoever is reading the terminal.
type errorBody struct {
	Error string `json:"error"`
}

// clientError is a request this package refused before the store ever saw it: a
// line of a batch that is not JSON, a field spelled two ways at once, a limit
// over the one this server will answer with.
//
// It exists so that a route finding out twelve lines into a body can hand the
// reason back up and let statusOf decide the status, rather than answering 400
// in its own words from wherever it happened to be. Every 400 this package
// answers is one of these, including the expiry header, which could have
// answered on the spot and does not — one way of saying it is easier to keep
// right than two.
//
// The message reaches the client verbatim. That is the point of it, and it is
// why nothing from the store is ever wrapped in one: a store error can name a
// path on the server's disk, and those go through the other branch.
type clientError struct{ error }

// badRequest is a mistake the client made, phrased for the client.
func badRequest(format string, args ...any) error {
	return clientError{fmt.Errorf(format, args...)}
}

// tooLargeFor is what a body over a limit is told, in one place because it is
// said from three: a declared length on a PUT, a declared length on a batch, and
// a MaxBytesReader that found out while reading. Which limit was hit is the
// number, since the routes have one each.
func tooLargeFor(limit int64) string {
	return fmt.Sprintf("the body exceeds the %d byte limit", limit)
}

// fail answers a request that could not be served.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, message := statusOf(err)

	if errors.Is(err, litekv.ErrorFenced) {
		w.Header().Set(headerTerm, strconv.FormatUint(s.db.Term(), 10))
	}

	if status == http.StatusInternalServerError {
		// The client is told the status and nothing else. An error from the
		// store can name a path on the server's disk or an offset in a log, and
		// a stranger has no business with either; it goes to the log, which is
		// where somebody who may see it will look.
		s.log.Error("request failed",
			"method", r.Method, "path", r.URL.Path, "err", err)
		message = "internal error"
	}

	writeError(w, status, message)
}

// statusOf maps a store error onto a status and the sentence a client is told.
// An error it does not know is a 500 with no message, and fail fills that in.
func statusOf(err error) (int, string) {
	switch {
	// All three mean the same thing to a client: there is no value under that
	// key. The store tells them apart because it knows whether the key was
	// asked to go, told to go by itself, or was never there, and none of those
	// change what a caller does next.
	case errors.Is(err, litekv.ErrorKeyNotFound),
		errors.Is(err, litekv.ErrorKeyDeleted),
		errors.Is(err, litekv.ErrorKeyExpired):
		return http.StatusNotFound, err.Error()

	// Not 403: the request was allowed, the store is no longer the one to send
	// it to. A conflict is what that is.
	case errors.Is(err, litekv.ErrorFenced):
		return http.StatusConflict, err.Error()

	case errors.Is(err, litekv.ErrorRecordTooLarge):
		return http.StatusRequestEntityTooLarge, err.Error()

	// A closed store is not a broken one. It is a server on its way down, and
	// 503 is the status that tells a client to try the next one.
	case errors.Is(err, litekv.ErrorClosed):
		return http.StatusServiceUnavailable, err.Error()

	// A write aimed at a node that is following somebody. Not 403: the request
	// was allowed, this is not the node for it, and which node is can change.
	case errors.Is(err, errFollowing):
		return http.StatusConflict, err.Error()

	// A read the client asked not to be answered from a store this far behind.
	// A precondition it set and did not get, which is what 412 is.
	case errors.Is(err, errNotReached):
		return http.StatusPreconditionFailed, err.Error()

	// The same, after waiting for it. The wait ran out; nothing here says it
	// never would have.
	case errors.Is(err, errWaited):
		return http.StatusGatewayTimeout, err.Error()

	// A position from a leader that has been replaced. The client is holding a
	// cookie from a history this store is not on.
	case errors.Is(err, litekv.ErrorSuperseded):
		return http.StatusConflict, err.Error()

	// A follower asked to carry on from something this server could not read.
	// Nothing was wrong on this side, and repeating the request will go the
	// same way, so it is the client's 400 and not a 500.
	case errors.Is(err, errBadPosition):
		return http.StatusBadRequest, err.Error()
	}

	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return http.StatusRequestEntityTooLarge, tooLargeFor(tooBig.Limit)
	}

	// Last, so that a store error wrapped on its way out of a parse is still
	// the store error it was rather than a 400. Nothing here wraps one.
	var bad clientError
	if errors.As(err, &bad) {
		return http.StatusBadRequest, bad.Error()
	}

	return http.StatusInternalServerError, ""
}

// writeError sends a JSON body under status. It is also the only way a handler
// answers a request the store never saw — a malformed header, a body that is
// too long — since those have no store error to map.
func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)

	// Nothing to do about a failed write here: the status is already on its way
	// and the connection is the client's problem.
	_ = json.NewEncoder(w).Encode(errorBody{Error: message})
}
