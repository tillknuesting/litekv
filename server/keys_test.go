package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

func TestKeyRoundTrip(t *testing.T) {
	s, _ := newServer(t, Options{})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/greeting", strings.NewReader("hello")), http.StatusNoContent)

	resp := do(t, s, http.MethodGet, "/v1/keys/greeting", nil)
	body := wants(t, resp, http.StatusOK)

	if string(body) != "hello" {
		t.Errorf("read %q, want %q", body, "hello")
	}
	if got := resp.Header.Get("Content-Type"); got != contentTypeValue {
		t.Errorf("Content-Type %q, want %q", got, contentTypeValue)
	}
	if got := resp.Header.Get("Content-Length"); got != "5" {
		t.Errorf("Content-Length %q, want 5", got)
	}

	wants(t, do(t, s, http.MethodPut, "/v1/keys/greeting", strings.NewReader("hello again")), http.StatusNoContent)
	if body := wants(t, do(t, s, http.MethodGet, "/v1/keys/greeting", nil), http.StatusOK); string(body) != "hello again" {
		t.Errorf("after writing again, read %q", body)
	}

	wants(t, do(t, s, http.MethodDelete, "/v1/keys/greeting", nil), http.StatusNoContent)
	wants(t, do(t, s, http.MethodGet, "/v1/keys/greeting", nil), http.StatusNotFound)
}

// TestKeyOfAnyBytes is the one that had to go over a real socket. A key is
// arbitrary bytes and a URL is not, and the claim being made — that
// percent-encoding a path segment carries any of them, including the slash that
// would otherwise split it in two — is a claim about Go's request parser and its
// mux, not about this package. A recorder would be handed a request some other
// code already built, which is exactly the step in question.
func TestKeyOfAnyBytes(t *testing.T) {
	s, _ := newServer(t, Options{})

	wire := httptest.NewServer(s)
	defer wire.Close()

	for _, key := range []string{
		"plain",
		"with a space",
		"a/b/c",             // the one that would be three segments unescaped
		"trailing/",         //
		"/leading",          //
		"100%",              // a percent, which is what the escaping is made of
		"question?mark",     // a query string, if nothing escapes it
		"hash#fragment",     //
		"dot.dot..dots",     // path cleaning, if anything cleans
		"\x00zero",          // bytes no text protocol expects
		"\xff\xfe not utf8", // and bytes that are not text at all
		"ümlaut",
		"键",
	} {
		t.Run(fmt.Sprintf("%q", key), func(t *testing.T) {
			target := wire.URL + "/v1/keys/" + url.PathEscape(key)
			value := "value for " + key

			put, err := http.NewRequest(http.MethodPut, target, strings.NewReader(value))
			if err != nil {
				t.Fatalf("building the request: %v", err)
			}
			resp, err := wire.Client().Do(put)
			if err != nil {
				t.Fatalf("PUT: %v", err)
			}
			wants(t, resp, http.StatusNoContent)

			resp, err = wire.Client().Get(target)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			if got := wants(t, resp, http.StatusOK); string(got) != value {
				t.Errorf("read %q, want %q", got, value)
			}

			// And the store holds the bytes the caller meant, not the ones the
			// URL was spelled with. Reading it back over the same encoding
			// would agree with itself however wrong both ends were.
			if _, err := s.db.Read([]byte(key)); err != nil {
				t.Errorf("the store does not hold %q: %v", key, err)
			}
		})
	}
}

// TestTheEmptyKeyHasNoPathSpelling pins the one key the single-key routes
// cannot reach: a path wildcard will not match an empty segment, so /v1/keys/
// is not a route. The store is happy to hold it — this asserts that too, so
// that the day it stops being true, the reason for the gap goes with it.
//
// It is not unreachable over HTTP. A batch line writes it and a range hands it
// back; TestBatchOfTheEmptyKey is that. This is about the path and only the
// path.
func TestTheEmptyKeyHasNoPathSpelling(t *testing.T) {
	s, db := newServer(t, Options{})

	if err := db.Write(nil, []byte("stored under nothing")); err != nil {
		t.Fatalf("the store refused an empty key, so this gap is not the path's: %v", err)
	}
	if _, err := db.Read(nil); err != nil {
		t.Fatalf("the store did not keep an empty key: %v", err)
	}

	wants(t, do(t, s, http.MethodGet, "/v1/keys/", nil), http.StatusNotFound)
	wants(t, do(t, s, http.MethodPut, "/v1/keys/", strings.NewReader("v")), http.StatusNotFound)
}

func TestValueOfZeroBytes(t *testing.T) {
	s, _ := newServer(t, Options{})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/empty", strings.NewReader("")), http.StatusNoContent)

	resp := do(t, s, http.MethodGet, "/v1/keys/empty", nil)
	body := wants(t, resp, http.StatusOK)

	if len(body) != 0 {
		t.Errorf("read %q, want nothing", body)
	}
	// An empty value and a missing key are different things and the length is
	// what says so.
	if got := resp.Header.Get("Content-Length"); got != "0" {
		t.Errorf("Content-Length %q, want 0", got)
	}
}

func TestHeadDoesNotSendTheValue(t *testing.T) {
	s, _ := newServer(t, Options{})

	value := strings.Repeat("x", 4096)
	wants(t, do(t, s, http.MethodPut, "/v1/keys/big", strings.NewReader(value)), http.StatusNoContent)

	resp := do(t, s, http.MethodHead, "/v1/keys/big", nil)
	body := wants(t, resp, http.StatusOK)

	if len(body) != 0 {
		t.Errorf("HEAD answered %d bytes of body", len(body))
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(value)) {
		t.Errorf("Content-Length %q, want %d", got, len(value))
	}

	wants(t, do(t, s, http.MethodHead, "/v1/keys/missing", nil), http.StatusNotFound)
}

// TestMissingKeyIsFourOhFour also pins the shape of a failure, since every
// route answers with the same one.
func TestMissingKeyIsFourOhFour(t *testing.T) {
	s, _ := newServer(t, Options{})

	resp := do(t, s, http.MethodGet, "/v1/keys/never-written", nil)
	body := wants(t, resp, http.StatusNotFound)

	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type %q, want application/json", got)
	}

	var failure errorBody
	if err := json.Unmarshal(body, &failure); err != nil {
		t.Fatalf("the body is not the error shape: %v: %s", err, body)
	}
	if failure.Error == "" {
		t.Error("a failure with nothing said about it")
	}
}

// TestDeletingWhatWasNeverThere is not pedantry. DELETE is meant to be
// idempotent and the store cannot answer the question anyway — a delete appends
// a tombstone without looking for what it hides, so a 404 here would be a lie
// dressed as a check.
func TestDeletingWhatWasNeverThere(t *testing.T) {
	s, _ := newServer(t, Options{})

	wants(t, do(t, s, http.MethodDelete, "/v1/keys/never-written", nil), http.StatusNoContent)
	wants(t, do(t, s, http.MethodDelete, "/v1/keys/never-written", nil), http.StatusNoContent)
}

func TestExpiringWrite(t *testing.T) {
	s, _ := newServer(t, Options{})

	put := func(key, expires string) *http.Response {
		t.Helper()

		req := httptest.NewRequest(http.MethodPut, "/v1/keys/"+key, strings.NewReader("v"))
		req.Header.Set(headerExpires, expires)

		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		return rec.Result()
	}

	wants(t, put("gone", time.Now().Add(-time.Hour).Format(time.RFC3339Nano)), http.StatusNoContent)
	wants(t, do(t, s, http.MethodGet, "/v1/keys/gone", nil), http.StatusNotFound)

	wants(t, put("staying", time.Now().Add(time.Hour).Format(time.RFC3339Nano)), http.StatusNoContent)
	wants(t, do(t, s, http.MethodGet, "/v1/keys/staying", nil), http.StatusOK)

	// A header that is not a time is the client's mistake, and it is told so
	// rather than having the record stored without the expiry it asked for.
	wants(t, put("nonsense", "in about an hour"), http.StatusBadRequest)
	wants(t, do(t, s, http.MethodGet, "/v1/keys/nonsense", nil), http.StatusNotFound)

	// A duration is not a time. Saying so is the point of the header being an
	// instant.
	wants(t, put("duration", "1h"), http.StatusBadRequest)
}

// TestValueTooLarge covers both ways a body can be too big, because they are
// caught in different places: a declared length is refused before anything is
// read, and a chunked body has no declared length and is caught while reading.
func TestValueTooLarge(t *testing.T) {
	s, _ := newServer(t, Options{MaxValue: 64})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/big", strings.NewReader(strings.Repeat("x", 65))),
		http.StatusRequestEntityTooLarge)

	// Exactly the limit is not too large.
	wants(t, do(t, s, http.MethodPut, "/v1/keys/edge", strings.NewReader(strings.Repeat("x", 64))),
		http.StatusNoContent)

	wire := httptest.NewServer(s)
	defer wire.Close()

	// An io.Reader the client cannot measure goes out chunked, so the server
	// learns how long it is by reading it.
	unmeasured := io.LimitReader(neverEnding{}, 65)
	req, err := http.NewRequest(http.MethodPut, wire.URL+"/v1/keys/chunked", unmeasured)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := wire.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	wants(t, resp, http.StatusRequestEntityTooLarge)

	if _, err := s.db.Read([]byte("chunked")); err == nil {
		t.Error("a value over the limit was stored anyway")
	}
}

// neverEnding is a body of unknown length, which is what makes a request
// chunked.
type neverEnding struct{}

func (neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// TestABodyAlreadyRefusedIsNotRead is the other half of the size limit and the
// half worth having. Reading a body the server has already decided to refuse is
// how a limit stops being one: a client that declares a gigabyte gets to make
// the server spend MaxValue of memory and the time to read it before being told
// no. The declared length is checked first for exactly that reason, and nothing
// but a test that watches the body can tell whether it still is.
func TestABodyAlreadyRefusedIsNotRead(t *testing.T) {
	s, _ := newServer(t, Options{MaxValue: 64})

	body := &counting{}
	req := httptest.NewRequest(http.MethodPut, "/v1/keys/declared", body)
	req.ContentLength = 1 << 30

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	wants(t, rec.Result(), http.StatusRequestEntityTooLarge)

	if body.read != 0 {
		t.Errorf("%d bytes of a refused body were read", body.read)
	}
}

// counting is a body that says how much of it was taken.
type counting struct{ read int }

func (c *counting) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	c.read += len(p)
	return len(p), nil
}

func TestMethodAndRoute(t *testing.T) {
	s, _ := newServer(t, Options{})

	resp := do(t, s, http.MethodPost, "/v1/keys/anything", strings.NewReader("v"))
	wants(t, resp, http.StatusMethodNotAllowed)

	// The mux says which methods there are, which is worth having: a client
	// that guessed POST finds out what to use without reading anything.
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodPut) {
		t.Errorf("Allow is %q, want PUT in it", allow)
	}

	wants(t, do(t, s, http.MethodGet, "/v1/nothing-here", nil), http.StatusNotFound)
	wants(t, do(t, s, http.MethodGet, "/", nil), http.StatusNotFound)
}

// TestClosedStoreIsUnavailable is the shutdown case. A store closed under a
// running handler answers ErrorClosed rather than crashing, and a client is
// told to go somewhere else rather than told the server is broken.
func TestClosedStoreIsUnavailable(t *testing.T) {
	s, db := newServer(t, Options{})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusNoContent)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	wants(t, do(t, s, http.MethodGet, "/v1/keys/k", nil), http.StatusServiceUnavailable)
	wants(t, do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusServiceUnavailable)
	wants(t, do(t, s, http.MethodDelete, "/v1/keys/k", nil), http.StatusServiceUnavailable)
}

// TestFencedStoreRefusesAWrite goes the long way round because there is no
// short one: a store cannot fence itself. It has to hear of a newer term, and
// the only thing that carries a term is a follower asking it for records.
func TestFencedStoreRefusesAWrite(t *testing.T) {
	s, replaced := newServer(t, Options{})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusNoContent)

	// Somebody following a leader newer than this one asks it to catch them up.
	// That request is refused, and refusing it is how this store finds out it
	// has been replaced.
	from := litekv.DBPosition{Term: replaced.Term() + 1}
	if _, err := replaced.Since(from, io.Discard, litekv.ReplicaOptions{}); !errors.Is(err, litekv.ErrorFenced) {
		t.Fatalf("asking on a newer term reported '%v', want fenced", err)
	}

	resp := do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v"))
	wants(t, resp, http.StatusConflict)

	// The term is on the answer, so a client can tell a store that has been
	// replaced from one that is merely unhappy.
	if got := resp.Header.Get(headerTerm); got != strconv.FormatUint(replaced.Term(), 10) {
		t.Errorf("%s is %q, want %d", headerTerm, got, replaced.Term())
	}

	wants(t, do(t, s, http.MethodDelete, "/v1/keys/k", nil), http.StatusConflict)

	// Reading is not writing. A fenced store's records are still records, and
	// refusing to serve them would take a replica out of service for a reason
	// that has nothing to do with reading. What it holds may be behind, which is
	// what the term on the answer is for.
	if got := wants(t, do(t, s, http.MethodGet, "/v1/keys/k", nil), http.StatusOK); string(got) != "v" {
		t.Errorf("a fenced store read %q, want %q", got, "v")
	}
}

// TestStatusOfEveryError is the mapping itself, held to the table rather than
// to whichever errors happen to be easy to provoke through a request.
func TestStatusOfEveryError(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
	}{
		{litekv.ErrorKeyNotFound, http.StatusNotFound},
		{litekv.ErrorKeyDeleted, http.StatusNotFound},
		{litekv.ErrorKeyExpired, http.StatusNotFound},
		{litekv.ErrorFenced, http.StatusConflict},
		{litekv.ErrorRecordTooLarge, http.StatusRequestEntityTooLarge},
		{litekv.ErrorClosed, http.StatusServiceUnavailable},
		{&http.MaxBytesError{Limit: 16}, http.StatusRequestEntityTooLarge},
		{badRequest("line 3: not a JSON object"), http.StatusBadRequest},
		{fmt.Errorf("wrapped: %w", badRequest("a limit over the maximum")), http.StatusBadRequest},
		{litekv.ErrorChecksumMismatch, http.StatusInternalServerError},
		{litekv.ErrorCorruptData, http.StatusInternalServerError},
		{fmt.Errorf("wrapped: %w", litekv.ErrorKeyNotFound), http.StatusNotFound},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			got, message := statusOf(test.err)
			if got != test.want {
				t.Errorf("%d, want %d", got, test.want)
			}
			// A 500 says nothing on purpose: fail fills in a sentence that
			// gives no part of the server away.
			if (message == "") != (test.want == http.StatusInternalServerError) {
				t.Errorf("the message is %q under a %d", message, got)
			}
		})
	}
}

// TestFiveHundredSaysNothing holds the other half of that: an error the mapping
// does not know reaches the client as a status and a bland sentence, with
// whatever the store said about its own insides left in the log.
func TestFiveHundredSaysNothing(t *testing.T) {
	s, _ := newServer(t, Options{})

	rec := httptest.NewRecorder()
	s.fail(rec, httptest.NewRequest(http.MethodGet, "/v1/keys/k", nil),
		fmt.Errorf("reading /var/lib/litekv/000013.log at offset 4096: %w", litekv.ErrorCorruptData))

	resp := rec.Result()
	body := wants(t, resp, http.StatusInternalServerError)

	if bytes.Contains(body, []byte("litekv")) || bytes.Contains(body, []byte("4096")) {
		t.Errorf("the answer gives the server away: %s", body)
	}

	// Saying nothing is not the same as saying nothing at all. statusOf leaves
	// the message empty for an error it does not know, and fail is what puts a
	// sentence there; without one the client gets {"error":""}, which reads
	// like a bug in the client.
	var failure errorBody
	if err := json.Unmarshal(body, &failure); err != nil {
		t.Fatalf("the body is not the error shape: %v: %s", err, body)
	}
	if failure.Error == "" {
		t.Error("a 500 with nothing said about it at all")
	}
}
