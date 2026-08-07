package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tillknuesting/litekv"
)

// The tests in this package are of the protocol and not of the store. What the
// store does with a record is settled by the several thousand lines of tests
// next door; what is open here is whether a request says the same thing to the
// store that the caller meant, and whether the answer says the same thing back.
//
// Two ways of asking, and both are used deliberately. httptest.NewRecorder
// drives the handler directly, which is enough for anything about statuses and
// bodies. httptest.NewServer puts a real HTTP client, a real parser and a real
// socket in between, which is the only way to find out whether a key survives
// being spelled in a URL — a recorder is handed a request somebody already
// built, and the question is exactly whether it can be built.

// quiet keeps the log of a request that failed on purpose out of the test
// output. Anything a test wants to assert about is in the response.
func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newServer opens a store in a temporary directory and returns a handler over
// it, and the store, for the tests that need to reach behind the API.
func newServer(t *testing.T, opts Options) (*Server, *litekv.DB) {
	t.Helper()

	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if opts.Logger == nil {
		opts.Logger = quiet()
	}
	return New(db, opts), db
}

// do runs one request against the handler and returns what it answered.
func do(t *testing.T, s *Server, method, target string, body io.Reader) *http.Response {
	t.Helper()

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(method, target, body))
	return rec.Result()
}

// wants fails unless the response has the status given, saying what came back
// instead — which for a failure is a sentence about why.
func wants(t *testing.T, resp *http.Response, status int) []byte {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != status {
		t.Fatalf("%d, want %d: %s", resp.StatusCode, status, body)
	}
	return body
}
