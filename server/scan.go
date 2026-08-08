package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tillknuesting/litekv"
)

// Ranges and prefixes, as newline-delimited JSON of the pairs that matched.
//
//	GET /v1/keys?prefix=user:
//	GET /v1/keys?from=a&to=m&limit=100
//
// The query carries raw bytes exactly as the path does. r.URL.Query()
// percent-decodes each value, so a bound holding a slash, a space, a control
// byte or a sequence that is not UTF-8 at all arrives as the bytes the client
// meant — TestBoundOfAnyBytes puts that through a real socket, the way
// TestKeyOfAnyBytes does for the path, because it is a claim about Go's URL
// parser rather than about this package.
//
// # Why there is a limit, and what it does not protect
//
// A range is not a stream. The engine gathers every matching key from every log
// before it can yield the first one in order, and it holds the store's read
// lock for all of it, so an unbounded range in front of the writes is a real
// way to hurt this server: rotation and merging want the write lock, and they
// wait behind whoever is scanning. -max-scan is the cap a client cannot raise
// and ?limit= is the client's own, and the cap is what stops one request
// parking a walk of the whole store there.
//
// It is worth being exact about what a limit buys, because it is less than it
// looks. Stopping the callback early does not stop the gather: the keys have all
// been collected and sorted by the time the first pair is handed over. What it
// stops is the reading of the records — the value copies, and for a frozen log
// the system calls that fetch them — which is most of the cost of a large answer
// but not the walk that found it. A range that has to be cheap has to be
// narrow; a limit makes a wide range cheaper, not cheap.
//
// # And why the answer is built before it is sent
//
// Nothing is written to the socket inside the callback, which costs holding the
// answer in memory and is not negotiable. The callback runs under the store's
// read lock, so a client that stopped reading would be deciding how long that
// lock is held, and therefore when the store is allowed to rotate or finish a
// merge. keys.go makes the same trade for the same reason, with Read instead of
// View. The framing is still NDJSON and a client can still consume it a line at
// a time; what it does not get is the server holding a lock open for it while
// it does.

// defaultMaxScan is how many pairs one range will answer with unless -max-scan
// says otherwise. It is a count and not a size: the pairs are whatever the store
// holds, so a thousand of them is a thousand values, and a store of large values
// wants a smaller number here than a store of small ones.
const defaultMaxScan = 1000

// contentTypeScan is what a body of newline-delimited JSON is. The x- is what
// everybody uses; there is no registered type for it.
const contentTypeScan = "application/x-ndjson"

// scanKeys answers a range or a prefix.
//
// A range that matched nothing is 200 and an empty body, not 404. There is no
// key here to be missing — the client asked which keys are in a range and the
// answer is none of them, which is an answer. 404 on this route would mean the
// route does not exist.
//
// HEAD comes along with GET, as it does next door, and gives the length of the
// answer without the answer. It is not the saving it is on one key: the range
// still runs and the body is still built, since measuring it is building it.
func (s *Server) scanKeys(w http.ResponseWriter, r *http.Request) {
	want, err := scanned(r, s.opts.MaxScan)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// Built whole and sent afterwards. See the note at the top of the file: the
	// callback runs under the store's read lock and a socket does not belong
	// under it. What this bounds the memory with is the limit, which is the
	// other half of what the limit is for.
	var body bytes.Buffer
	out := json.NewEncoder(&body)

	// failed cannot happen — a pair is two strings and a bytes.Buffer does not
	// fail a write — but it is carried out of the callback rather than dropped,
	// because "cannot happen" is a claim about today's encoding rule.
	var (
		sent   int
		failed error
	)

	err = want.over(s.db, func(key, value []byte) bool {
		if failed = encodePair(out, key, value); failed != nil {
			return false
		}
		sent++
		return sent < want.limit
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if failed != nil {
		s.fail(w, r, failed)
		return
	}

	w.Header().Set("Content-Type", contentTypeScan)
	w.Header().Set("Content-Length", strconv.Itoa(body.Len()))
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(body.Bytes()); err != nil {
		// The status went out long ago and there is nothing to tell the client.
		// A client that went away mid-answer is ordinary.
		s.log.Debug("range not delivered", "path", r.URL.Path, "err", err)
	}
}

// scan is the range a query asked for.
type scan struct {
	// byPrefix says which pair of fields below is the real one. It is a flag
	// rather than a nil check because an empty prefix is a legitimate prefix —
	// it is every key, which is what the engine says it is — and is not the
	// same request as no prefix at all.
	byPrefix bool
	prefix   []byte

	from, to []byte

	// limit is the client's, capped by the server's, and is at least one.
	limit int
}

// over runs the range this scan names.
//
// Prefix rather than a from and a to computed here: the engine knows what the
// end of a prefix is, including the cases that have no end — a prefix of 0xff
// bytes, and the empty prefix — and working it out again on this side would be
// a second copy of that answer to keep right.
func (want scan) over(db *litekv.DB, fn func(key, value []byte) bool) error {
	if want.byPrefix {
		return db.Prefix(want.prefix, fn)
	}
	return db.Range(want.from, want.to, fn)
}

// scanned reads the query.
//
// What each of the odd requests does, since none of them is obvious and all of
// them are somebody's typo one day:
//
//   - No parameters at all is every key, capped by max. from and to are both
//     unbounded, which is what the engine means by a nil bound, and it is the
//     same request as an empty prefix.
//   - ?prefix= with nothing after it is every key, because the engine says an
//     empty prefix is every key and this route does not get to disagree with it.
//   - prefix together with from or to is refused. They are two ways of naming
//     one range, not two ranges to intersect, and honouring one while ignoring
//     the other would answer a question the client did not ask.
//   - ?from= or ?to= with nothing after it is no bound on that side, which is
//     what an empty bound is to the engine as well.
//   - A from after its to matches nothing, which is 200 and an empty body. The
//     range is empty; there is nothing wrong with the request.
//   - ?limit= with nothing after it is refused rather than treated as absent. A
//     client that built a query string wrongly should hear about it.
//   - A limit over max is refused rather than quietly lowered to it. This is
//     the one that matters: a client can only tell that an answer was cut short
//     by counting the lines against the limit it asked for, and a server that
//     silently substitutes its own takes that away.
func scanned(r *http.Request, max int) (scan, error) {
	q := r.URL.Query()

	want := scan{limit: max}

	if q.Has("prefix") && (q.Has("from") || q.Has("to")) {
		return scan{}, badRequest("prefix and from/to are two ways of naming one range; send one of them")
	}

	if q.Has("limit") {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 {
			return scan{}, badRequest("limit must be a positive whole number")
		}
		if n > max {
			return scan{}, badRequest("limit %d is over the %d this server will answer with", n, max)
		}
		want.limit = n
	}

	if q.Has("prefix") {
		want.byPrefix, want.prefix = true, []byte(q.Get("prefix"))
		return want, nil
	}

	want.from, want.to = []byte(q.Get("from")), []byte(q.Get("to"))
	return want, nil
}
