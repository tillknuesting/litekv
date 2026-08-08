package server

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// stored is one pair off the wire, decoded back into the bytes it names. The
// tests compare these rather than the JSON, since a test asserting on the JSON
// would agree with whatever the encoding happened to do.
type stored struct{ key, value string }

// scanFor reads a whole NDJSON answer. It fails the test on anything but a 200
// and on a line that does not decode, which is most of what the range route can
// get wrong about its own framing.
func scanFor(t *testing.T, s *Server, target string) []stored {
	t.Helper()

	resp := do(t, s, http.MethodGet, target, nil)
	body := wants(t, resp, http.StatusOK)

	if got := resp.Header.Get("Content-Type"); got != contentTypeScan {
		t.Errorf("Content-Type %q, want %q", got, contentTypeScan)
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(body)) {
		t.Errorf("Content-Length %q against %d bytes of body", got, len(body))
	}

	return pairsIn(t, body)
}

func pairsIn(t *testing.T, body []byte) []stored {
	t.Helper()

	var got []stored
	for lines := bufio.NewScanner(bytes.NewReader(body)); lines.Scan(); {
		line, err := decodePair(lines.Bytes())
		if err != nil {
			t.Fatalf("a line of the answer does not decode: %v: %s", err, lines.Bytes())
		}
		if line.Op != "" || line.Expires != "" {
			t.Errorf("an answer carried op or expires: %s", lines.Bytes())
		}

		key, err := fromTheWire(line.Key, line.KeyB64, "key")
		if err != nil {
			t.Fatalf("the key of a line: %v", err)
		}
		value, err := fromTheWire(line.Value, line.ValueB64, "value")
		if err != nil {
			t.Fatalf("the value of a line: %v", err)
		}
		got = append(got, stored{string(key), string(value)})
	}
	return got
}

// fill writes key=value for each key given, through the handler.
func fill(t *testing.T, s *Server, keys ...string) {
	t.Helper()

	for _, key := range keys {
		wants(t, do(t, s, http.MethodPut, "/v1/keys/"+url.PathEscape(key),
			strings.NewReader("v:"+key)), http.StatusNoContent)
	}
}

func keysOf(pairs []stored) []string {
	keys := make([]string, 0, len(pairs))
	for _, p := range pairs {
		keys = append(keys, p.key)
	}
	return keys
}

func same(t *testing.T, got []stored, want ...string) {
	t.Helper()

	if strings.Join(keysOf(got), ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", keysOf(got), want)
	}
	for _, p := range got {
		if p.value != "v:"+p.key {
			t.Errorf("%s carried %q", p.key, p.value)
		}
	}
}

func TestPrefixScan(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "apple", "user:1", "user:2", "user:10", "userz", "zebra")

	// In order, and only the ones that match. "userz" is the one that catches a
	// prefix implemented as "everything from here on".
	same(t, scanFor(t, s, "/v1/keys?prefix=user:"), "user:1", "user:10", "user:2")
}

func TestRangeScan(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "a", "b", "c", "d", "e")

	// from is included and to is not.
	same(t, scanFor(t, s, "/v1/keys?from=b&to=d"), "b", "c")
	same(t, scanFor(t, s, "/v1/keys?from=c"), "c", "d", "e")
	same(t, scanFor(t, s, "/v1/keys?to=c"), "a", "b")

	// A bound with nothing after it is no bound on that side, which is what an
	// empty bound is to the engine as well.
	same(t, scanFor(t, s, "/v1/keys?from=&to="), "a", "b", "c", "d", "e")

	// A range with nothing in it is 200 and nothing, not 404. There is no key
	// here to be missing.
	if got := scanFor(t, s, "/v1/keys?from=x&to=z"); len(got) != 0 {
		t.Errorf("an empty range gave %v", keysOf(got))
	}
	// Including one whose ends are the wrong way round, which is a range that
	// holds nothing rather than a request that is wrong.
	if got := scanFor(t, s, "/v1/keys?from=d&to=b"); len(got) != 0 {
		t.Errorf("a backwards range gave %v", keysOf(got))
	}
}

// TestScanOfEverything is the two ways of asking for all of it, which are the
// same request: no parameters at all, and a prefix with nothing after it. The
// engine says an empty prefix is every key and this route does not get to
// disagree with it.
func TestScanOfEverything(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "a", "b", "c")

	same(t, scanFor(t, s, "/v1/keys"), "a", "b", "c")
	same(t, scanFor(t, s, "/v1/keys?prefix="), "a", "b", "c")
}

// TestScanSkipsWhatIsNotThere. The engine visits live keys only; this is that
// seen through the route, since a range that answered with tombstones would be
// a different API from the one that answers 404 for them next door.
func TestScanSkipsWhatIsNotThere(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "kept", "deleted", "expired")

	wants(t, do(t, s, http.MethodDelete, "/v1/keys/deleted", nil), http.StatusNoContent)

	expiring := httptest.NewRequest(http.MethodPut, "/v1/keys/expired", strings.NewReader("v:expired"))
	expiring.Header.Set(headerExpires, time.Now().Add(-time.Hour).Format(time.RFC3339Nano))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, expiring)
	wants(t, rec.Result(), http.StatusNoContent)

	same(t, scanFor(t, s, "/v1/keys"), "kept")
}

// TestScanLimit is the client's own cap: the first N in key order, which is
// what makes a limit worth having rather than an arbitrary subset.
func TestScanLimit(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "a", "b", "c", "d", "e")

	same(t, scanFor(t, s, "/v1/keys?limit=2"), "a", "b")
	same(t, scanFor(t, s, "/v1/keys?limit=1"), "a")
	same(t, scanFor(t, s, "/v1/keys?limit=5"), "a", "b", "c", "d", "e")

	// A limit larger than the answer is not an error and does not pad it.
	same(t, scanFor(t, s, "/v1/keys?from=a&to=c&limit=100"), "a", "b")

	// Carrying on from the last key of a page: from is inclusive, so the next
	// page starts at the byte after it.
	same(t, scanFor(t, s, "/v1/keys?from=b%00&limit=2"), "c", "d")
}

// TestScanMaximum is the cap the client cannot raise, and the one thing on this
// route that is about the server rather than about the answer: a range holds
// the store's read lock while it gathers, so an unbounded one is a way to stand
// in front of the writes.
func TestScanMaximum(t *testing.T) {
	s, _ := newServer(t, Options{MaxScan: 3})
	fill(t, s, "a", "b", "c", "d", "e")

	// No limit named is the server's.
	same(t, scanFor(t, s, "/v1/keys"), "a", "b", "c")

	// Exactly the maximum is not over it.
	same(t, scanFor(t, s, "/v1/keys?limit=3"), "a", "b", "c")

	// And over it is refused rather than quietly lowered, because counting the
	// lines against the limit it asked for is the only way a client can tell
	// that an answer was cut short.
	body := wants(t, do(t, s, http.MethodGet, "/v1/keys?limit=4", nil), http.StatusBadRequest)
	if !bytes.Contains(body, []byte("3")) {
		t.Errorf("the refusal does not say what the maximum is: %s", body)
	}
}

// TestScanRefusesWhatItCannotRead is the rest of the query, and every one of
// these is somebody's typo one day.
func TestScanRefusesWhatItCannotRead(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "a", "b")

	for _, test := range []struct {
		name   string
		target string
	}{
		{"prefix and from", "/v1/keys?prefix=a&from=a"},
		{"prefix and to", "/v1/keys?prefix=a&to=b"},
		{"prefix and both", "/v1/keys?prefix=a&from=a&to=b"},
		{"an empty prefix with a bound", "/v1/keys?prefix=&from=a"},
		{"a limit of zero", "/v1/keys?limit=0"},
		{"a negative limit", "/v1/keys?limit=-1"},
		{"a limit that is not a number", "/v1/keys?limit=lots"},
		{"a limit with nothing after it", "/v1/keys?limit="},
		{"a fractional limit", "/v1/keys?limit=1.5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wants(t, do(t, s, http.MethodGet, test.target, nil), http.StatusBadRequest)
		})
	}
}

// TestScanEncodesBytesThatAreNotText is the encoding rule on the way out. The
// records are written through the path route, which has no JSON in it, so the
// bytes being asserted on are the store's rather than something this test and
// the encoder agreed about.
func TestScanEncodesBytesThatAreNotText(t *testing.T) {
	s, db := newServer(t, Options{})

	if err := db.Write([]byte("bin:\xff"), []byte{0x00, 0xfe, 0xff}); err != nil {
		t.Fatal(err)
	}
	if err := db.Write([]byte("bin:text"), []byte("ordinary")); err != nil {
		t.Fatal(err)
	}

	resp := do(t, s, http.MethodGet, "/v1/keys?prefix=bin:", nil)
	body := wants(t, resp, http.StatusOK)

	// The answer is text, whatever the store holds.
	if !isText(body) {
		t.Errorf("the answer is not valid UTF-8: %q", body)
	}
	if bytes.ContainsRune(body, '�') {
		t.Errorf("a replacement character reached the wire: %s", body)
	}

	got := pairsIn(t, body)
	if len(got) != 2 {
		t.Fatalf("got %d pairs", len(got))
	}
	// In key order, which puts 0xff after 't'.
	if got[0].key != "bin:text" || got[0].value != "ordinary" {
		t.Errorf("the text came back as %q = %q", got[0].key, got[0].value)
	}
	if got[1].key != "bin:\xff" || got[1].value != "\x00\xfe\xff" {
		t.Errorf("the bytes came back as %q = %q", got[1].key, got[1].value)
	}

	// And the text pair is spelled as text rather than base64, which is the
	// half of the rule a base64-everything encoder would also pass the above.
	if !bytes.Contains(body, []byte(`{"key":"bin:text","value":"ordinary"}`)) {
		t.Errorf("a text pair was not sent as text: %s", body)
	}
}

func isText(b []byte) bool {
	for _, line := range bytes.Split(b, []byte("\n")) {
		if _, err := decodePair(line); len(line) > 0 && err != nil {
			return false
		}
	}
	return true
}

// TestBoundOfAnyBytes is TestKeyOfAnyBytes for the query string. A bound is
// arbitrary bytes in the same way a key is, and whether a query carries them is
// a claim about Go's URL parser rather than about this package — so it goes
// through a real client, a real socket and a real parser, and a recorder would
// be handed a request some other code already built.
func TestBoundOfAnyBytes(t *testing.T) {
	s, _ := newServer(t, Options{})

	wire := httptest.NewServer(s)
	defer wire.Close()

	for _, prefix := range []string{
		"plain:",
		"with a space:",
		"a/b/c:",
		"100%:",
		"question?mark:",
		"hash#fragment:",
		"plus+and&amp:", // the two characters a query string is made of
		"\x00zero:",
		"\xff\xfe not utf8:",
		"ümlaut:",
		"键:",
	} {
		t.Run(fmt.Sprintf("%q", prefix), func(t *testing.T) {
			key, value := prefix+"key", "value for "+prefix

			put, err := http.NewRequest(http.MethodPut,
				wire.URL+"/v1/keys/"+url.PathEscape(key), strings.NewReader(value))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := wire.Client().Do(put)
			if err != nil {
				t.Fatalf("PUT: %v", err)
			}
			wants(t, resp, http.StatusNoContent)

			query := url.Values{"prefix": []string{prefix}}
			resp, err = wire.Client().Get(wire.URL + "/v1/keys?" + query.Encode())
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			got := pairsIn(t, wants(t, resp, http.StatusOK))

			if len(got) != 1 {
				t.Fatalf("%d pairs for prefix %q", len(got), prefix)
			}
			if got[0].key != key || got[0].value != value {
				t.Errorf("got %q = %q, want %q = %q", got[0].key, got[0].value, key, value)
			}
		})
	}
}

// TestScanDoesNotCollideWithOneKey is the route registration itself. /v1/keys
// and /v1/keys/{key} are two patterns one segment apart and the mux has to keep
// them apart; the third case is the one that would move if it did not, since
// /v1/keys/ is the empty key's non-spelling and the reason it has none.
func TestScanDoesNotCollideWithOneKey(t *testing.T) {
	s, _ := newServer(t, Options{})
	fill(t, s, "k")

	// The exact path is the range.
	same(t, scanFor(t, s, "/v1/keys"), "k")

	// One more segment is one key, and the raw body rather than JSON.
	if body := wants(t, do(t, s, http.MethodGet, "/v1/keys/k", nil), http.StatusOK); string(body) != "v:k" {
		t.Errorf("/v1/keys/k answered %q", body)
	}

	// And a trailing slash is still nothing at all, rather than a range or a
	// redirect to one.
	wants(t, do(t, s, http.MethodGet, "/v1/keys/", nil), http.StatusNotFound)

	// A method the range does not have is the mux's answer, not a 404.
	resp := do(t, s, http.MethodDelete, "/v1/keys", nil)
	wants(t, resp, http.StatusMethodNotAllowed)
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodGet) {
		t.Errorf("Allow is %q, want GET in it", allow)
	}
}

// TestScanOfAClosedStore. A range over a closed store is a server on its way
// down, and the answer is 503 with nothing written — which is only possible
// because the answer is built before any of it is sent.
func TestScanOfAClosedStore(t *testing.T) {
	s, db := newServer(t, Options{})
	fill(t, s, "a")

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	resp := do(t, s, http.MethodGet, "/v1/keys", nil)
	body := wants(t, resp, http.StatusServiceUnavailable)

	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("a failed range answered as %q", got)
	}
	if bytes.Contains(body, []byte(`"key"`)) {
		t.Errorf("a failed range sent part of an answer: %s", body)
	}
}
