package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

// post drives one request at POST /v1/batch. It is here rather than in
// server_test.go because the body of a batch is a string of lines and every
// test in this file builds one.
func post(t *testing.T, s *Server, body string) *http.Response {
	t.Helper()
	return do(t, s, http.MethodPost, "/v1/batch", strings.NewReader(body))
}

// lines joins operations into a body, ending with the newline every NDJSON body
// ends with.
func lines(ops ...string) string { return strings.Join(ops, "\n") + "\n" }

func TestBatchStoresEveryOperation(t *testing.T) {
	s, db := newServer(t, Options{})

	// Something for the batch to delete and something for it to overwrite.
	wants(t, do(t, s, http.MethodPut, "/v1/keys/old", strings.NewReader("v")), http.StatusNoContent)
	wants(t, do(t, s, http.MethodPut, "/v1/keys/kept", strings.NewReader("before")), http.StatusNoContent)

	wants(t, post(t, s, lines(
		`{"op":"write","key":"one","value":"1"}`,
		`{"op":"write","key":"two","value":"2"}`,
		`{"op":"write","key":"kept","value":"after"}`,
		`{"op":"delete","key":"old"}`,
		`{"op":"write","key":"empty","value":""}`,
		`{"op":"write","key":"novalue"}`,
	)), http.StatusNoContent)

	for key, want := range map[string]string{
		"one": "1", "two": "2", "kept": "after", "empty": "", "novalue": "",
	} {
		value, err := db.Read([]byte(key))
		if err != nil {
			t.Errorf("%s: %v", key, err)
			continue
		}
		if string(value) != want {
			t.Errorf("%s = %q, want %q", key, value, want)
		}
	}

	if _, err := db.Read([]byte("old")); err == nil {
		t.Error("the delete in the batch did not happen")
	}
}

// TestBatchLaterRecordsWin is the engine's rule seen through the route, and it
// is worth pinning here because a parser that reordered the lines — or a client
// that assumed it did — would break it silently.
func TestBatchLaterRecordsWin(t *testing.T) {
	s, db := newServer(t, Options{})

	wants(t, post(t, s, lines(
		`{"op":"write","key":"k","value":"first"}`,
		`{"op":"write","key":"k","value":"second"}`,
		`{"op":"write","key":"gone","value":"here"}`,
		`{"op":"delete","key":"gone"}`,
	)), http.StatusNoContent)

	value, err := db.Read([]byte("k"))
	if err != nil || string(value) != "second" {
		t.Errorf("k = %q, %v; want second", value, err)
	}
	if _, err := db.Read([]byte("gone")); err == nil {
		t.Error("a key written and then deleted in one batch survived")
	}
}

// TestBatchIsAllOrNothing is the reason the whole body is parsed before any of
// it is stored. The engine makes a batch atomic on the disk; that is worth
// nothing if the parser has already written the lines it understood.
func TestBatchIsAllOrNothing(t *testing.T) {
	s, db := newServer(t, Options{})

	body := wants(t, post(t, s, lines(
		`{"op":"write","key":"first","value":"1"}`,
		`{"op":"write","key":"second","value":"2"}`,
		`{"op":"write","key":"third","value":`,
	)), http.StatusBadRequest)

	// The line is named, because a body of a thousand operations with one
	// mistake in it is otherwise a search.
	if !bytes.Contains(body, []byte("line 3")) {
		t.Errorf("the failure does not say which line: %s", body)
	}

	for _, key := range []string{"first", "second", "third"} {
		if _, err := db.Read([]byte(key)); err == nil {
			t.Errorf("%s was stored by a batch that was refused", key)
		}
	}
}

// TestBatchRefusesWhatItCannotRead is the table of client mistakes. Every one of
// them is 400 and every one of them stores nothing, which the check after the
// loop is for: a refusal that stored something would pass the status assertion
// on its own.
func TestBatchRefusesWhatItCannotRead(t *testing.T) {
	s, db := newServer(t, Options{})

	for _, test := range []struct {
		name string
		body string
	}{
		{"no op", `{"key":"k","value":"v"}`},
		{"an op nobody has", `{"op":"upsert","key":"k","value":"v"}`},
		{"op of the wrong type", `{"op":1,"key":"k"}`},
		{"not JSON", `nonsense`},
		{"half an object", `{"op":"write"`},
		{"an unknown field", `{"op":"write","key":"k","ttl":60}`},
		{"two objects on a line", `{"op":"write","key":"a"} {"op":"write","key":"b"}`},
		{"key twice", `{"op":"write","key":"k","key_b64":"aw","value":"v"}`},
		{"value twice", `{"op":"write","key":"k","value":"v","value_b64":"dg"}`},
		{"base64 with padding", `{"op":"write","key_b64":"YQ==","value":"v"}`},
		{"not base64 at all", `{"op":"write","key_b64":"~~~~","value":"v"}`},
		{"bytes that are not text", "{\"op\":\"write\",\"key\":\"\xff\",\"value\":\"v\"}"},
		{"an expiry that is not a time", `{"op":"write","key":"k","value":"v","expires":"in an hour"}`},
		{"an expiry that is a duration", `{"op":"write","key":"k","value":"v","expires":"1h"}`},
		{"a delete with a value", `{"op":"delete","key":"k","value":"v"}`},
		{"a delete with an empty value", `{"op":"delete","key":"k","value":""}`},
		{"a delete with a base64 value", `{"op":"delete","key":"k","value_b64":"dg"}`},
		{"a delete that expires", `{"op":"delete","key":"k","expires":"2030-01-01T00:00:00Z"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			wants(t, post(t, s, lines(test.body)), http.StatusBadRequest)
		})
	}

	if n := db.Len(); n != 0 {
		t.Errorf("%d records were stored by batches that were all refused", n)
	}
}

// TestBatchOfBytesThatAreNotText is the encoding rule doing the job it exists
// for: a key and a value the JSON string form cannot hold, stored and then read
// back through the path route, which has no JSON in it at all and therefore
// cannot agree with a mistake made here.
func TestBatchOfBytesThatAreNotText(t *testing.T) {
	s, db := newServer(t, Options{})

	// 0xff 0xfe as a key, and a value of every byte there is.
	every := make([]byte, 256)
	for i := range every {
		every[i] = byte(i)
	}

	var body bytes.Buffer
	if err := encodePair(json.NewEncoder(&body), []byte{0xff, 0xfe}, every); err != nil {
		t.Fatal(err)
	}

	// encodePair does not write an op, since an answer has no use for one. The
	// client has to, which is the asymmetry the pair type is shaped around.
	var op pair
	if err := json.Unmarshal(bytes.TrimSpace(body.Bytes()), &op); err != nil {
		t.Fatal(err)
	}
	op.Op = opWrite
	written, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(written, []byte("key_b64")) || !bytes.Contains(written, []byte("value_b64")) {
		t.Fatalf("the encoding did not reach for base64: %s", written)
	}

	wants(t, post(t, s, string(written)+"\n"), http.StatusNoContent)

	value, err := db.Read([]byte{0xff, 0xfe})
	if err != nil {
		t.Fatalf("the store does not hold the key the batch meant: %v", err)
	}
	if !bytes.Equal(value, every) {
		t.Errorf("the value came back %d bytes long and different", len(value))
	}
}

// TestBatchExpiry is the expires field, which means exactly what the
// Litekv-Expires header means on a PUT.
func TestBatchExpiry(t *testing.T) {
	s, _ := newServer(t, Options{})

	wants(t, post(t, s, lines(
		fmt.Sprintf(`{"op":"write","key":"gone","value":"v","expires":%q}`,
			time.Now().Add(-time.Hour).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"op":"write","key":"staying","value":"v","expires":%q}`,
			time.Now().Add(time.Hour).Format(time.RFC3339Nano)),
	)), http.StatusNoContent)

	wants(t, do(t, s, http.MethodGet, "/v1/keys/gone", nil), http.StatusNotFound)
	wants(t, do(t, s, http.MethodGet, "/v1/keys/staying", nil), http.StatusOK)
}

// TestEveryLineKeepsItsOwnBytes is the trap this route is built around.
// litekv.Batch does not copy the keys and values it is given — it reads them
// when the batch is written — so every line's bytes have to be that line's own
// and have to still be there at the end of the body. A decode buffer shared
// across the lines would put the last line's bytes under all of the keys, and
// nothing would report an error.
//
// The lengths shrink deliberately: a shared buffer that was not cleared would
// leave the tail of a longer earlier value behind a shorter later one, which is
// the shape that survives a test using values of one length.
func TestEveryLineKeepsItsOwnBytes(t *testing.T) {
	s, db := newServer(t, Options{})

	const count = 200

	var body strings.Builder
	for i := 0; i < count; i++ {
		fmt.Fprintf(&body, "{\"op\":\"write\",\"key\":\"key-%03d\",\"value\":%q}\n",
			i, strings.Repeat(fmt.Sprintf("%d", i%10), count-i))
	}

	wants(t, post(t, s, body.String()), http.StatusNoContent)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%03d", i)
		want := strings.Repeat(fmt.Sprintf("%d", i%10), count-i)

		value, err := db.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if string(value) != want {
			t.Fatalf("%s is %d bytes of %q, want %d of %q",
				key, len(value), firstByte(value), len(want), want[:1])
		}
	}
}

func firstByte(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b[:1])
}

// TestEmptyBatchStoresNothing. An empty batch writes nothing and reports no
// error in the engine, and there is no reason for this route to be stricter
// than that: a client that had nothing to send sent nothing.
func TestEmptyBatchStoresNothing(t *testing.T) {
	s, db := newServer(t, Options{})

	wants(t, post(t, s, ""), http.StatusNoContent)
	wants(t, post(t, s, "\n\n\n"), http.StatusNoContent)
	wants(t, post(t, s, "   \n\t\n"), http.StatusNoContent)

	if n := db.Len(); n != 0 {
		t.Errorf("an empty batch stored %d records", n)
	}

	// A blank line between two operations is not a mistake either, and neither
	// is a body that does not end in a newline.
	wants(t, post(t, s, "{\"op\":\"write\",\"key\":\"a\",\"value\":\"1\"}\n\n{\"op\":\"write\",\"key\":\"b\",\"value\":\"2\"}"),
		http.StatusNoContent)

	for _, key := range []string{"a", "b"} {
		if _, err := db.Read([]byte(key)); err != nil {
			t.Errorf("%s: %v", key, err)
		}
	}
}

// TestBatchOfTheEmptyKey is the key the path routes cannot spell. A batch can:
// an absent key field is the empty key, exactly as an absent value is the empty
// value, and the range route can read it back.
func TestBatchOfTheEmptyKey(t *testing.T) {
	s, db := newServer(t, Options{})

	wants(t, post(t, s, lines(`{"op":"write","key":"","value":"named"}`)), http.StatusNoContent)
	if value, err := db.Read(nil); err != nil || string(value) != "named" {
		t.Fatalf("the empty key is %q, %v", value, err)
	}

	wants(t, post(t, s, lines(`{"op":"write","value":"absent"}`)), http.StatusNoContent)
	if value, err := db.Read(nil); err != nil || string(value) != "absent" {
		t.Fatalf("an absent key is not the empty key: %q, %v", value, err)
	}

	got := scanFor(t, s, "/v1/keys")
	if len(got) != 1 || got[0].key != "" || got[0].value != "absent" {
		t.Errorf("a range over the empty key gave %+v", got)
	}

	wants(t, post(t, s, lines(`{"op":"delete"}`)), http.StatusNoContent)
	if _, err := db.Read(nil); err == nil {
		t.Error("the empty key survived a delete of it")
	}
}

// TestBatchGoesThroughTheQueue is the same job TestClosingTheServerStopsWrites-
// AndNotReads does for a PUT, and it is the only way from outside the package to
// see that this route reaches the store through the writer rather than around
// it: a closed Server has stopped the writer and still has an open store, so a
// batch that went straight to the store would be accepted here.
func TestBatchGoesThroughTheQueue(t *testing.T) {
	s, db := newServer(t, Options{})

	wants(t, post(t, s, lines(`{"op":"write","key":"before","value":"v"}`)), http.StatusNoContent)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	wants(t, post(t, s, lines(`{"op":"write","key":"after","value":"v"}`)), http.StatusServiceUnavailable)

	if _, err := db.Read([]byte("after")); err == nil {
		t.Error("a batch reached the store after the writer had stopped")
	}
	if _, err := db.Read([]byte("before")); err != nil {
		t.Errorf("the batch from before the close is gone: %v", err)
	}
}

// TestBatchTooLarge covers both ways a body can be too big, which are caught in
// different places: a declared length is refused before anything is read, and a
// chunked body has no declared length and is caught while reading it.
func TestBatchTooLarge(t *testing.T) {
	s, db := newServer(t, Options{MaxBatch: 256})

	one := `{"op":"write","key":"k","value":"` + strings.Repeat("x", 300) + `"}`
	wants(t, post(t, s, lines(one)), http.StatusRequestEntityTooLarge)

	// A body already refused on its declared length is not read.
	body := &counting{}
	req := httptest.NewRequest(http.MethodPost, "/v1/batch", body)
	req.ContentLength = 1 << 30

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	wants(t, rec.Result(), http.StatusRequestEntityTooLarge)

	if body.read != 0 {
		t.Errorf("%d bytes of a refused batch were read", body.read)
	}

	// And the same over a socket, where the length is not declared at all.
	wire := httptest.NewServer(s)
	defer wire.Close()

	req, err := http.NewRequest(http.MethodPost, wire.URL+"/v1/batch", io.LimitReader(neverEnding{}, 300))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := wire.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	wants(t, resp, http.StatusRequestEntityTooLarge)

	if n := db.Len(); n != 0 {
		t.Errorf("a batch over the limit stored %d records", n)
	}

	// Under the limit is not over it.
	wants(t, post(t, s, lines(`{"op":"write","key":"k","value":"small"}`)), http.StatusNoContent)
}

// TestBatchBodyIsBoundedAcrossItsLines is the half of the size limit that the
// per-line bound cannot make, and the only thing that tests the MaxBytesReader
// rather than the scanner's own maximum. Every line here is well under the
// limit and the body is five times it: without a reader counting the whole
// thing, a million short lines would be a body nobody bounded.
//
// The length is not declared, which is what a chunked request looks like and
// what leaves the reader as the only thing that can refuse it.
func TestBatchBodyIsBoundedAcrossItsLines(t *testing.T) {
	s, db := newServer(t, Options{MaxBatch: 256})

	var body strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&body, "{\"op\":\"write\",\"key\":\"k%02d\",\"value\":\"v\"}\n", i)
	}
	if body.Len() < 5*256 {
		t.Fatalf("the body is %d bytes, which is not enough over the limit to be a test", body.Len())
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/batch", strings.NewReader(body.String()))
	req.ContentLength = -1

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	wants(t, rec.Result(), http.StatusRequestEntityTooLarge)

	if n := db.Len(); n != 0 {
		t.Errorf("%d records of an unbounded body were stored", n)
	}
}

// TestParseBatchBoundsOneLine is the half of the size limit that
// http.MaxBytesReader cannot make: a body arrives in lines and a line is what
// this holds in memory, so a caller with no reader in front of it is still
// bounded. It is reported as the same refusal the reader makes, so a client
// hears about one limit rather than two.
func TestParseBatchBoundsOneLine(t *testing.T) {
	long := `{"op":"write","key":"k","value":"` + strings.Repeat("x", 500) + `"}`

	_, err := parseBatch(strings.NewReader(long), 128)
	if err == nil {
		t.Fatal("a line five times the limit was read")
	}

	var tooBig *http.MaxBytesError
	if !errors.As(err, &tooBig) {
		t.Fatalf("a line over the limit reported %v, want the same error a body over it does", err)
	}
	if tooBig.Limit != 128 {
		t.Errorf("the refusal names %d as the limit", tooBig.Limit)
	}
	if status, _ := statusOf(err); status != http.StatusRequestEntityTooLarge {
		t.Errorf("a line over the limit is a %d, want %d", status, http.StatusRequestEntityTooLarge)
	}

	// The same line under a limit that fits it is not a problem.
	batch, err := parseBatch(strings.NewReader(long), 1024)
	if err != nil || batch.Len() != 1 {
		t.Errorf("under the limit: %d records, %v", batch.Len(), err)
	}
}

// TestBatchMethodAndRoute. /v1/batch is one route with one method on it, and
// the mux says which when a client guesses.
func TestBatchMethodAndRoute(t *testing.T) {
	s, _ := newServer(t, Options{})

	resp := do(t, s, http.MethodPut, "/v1/batch", strings.NewReader(""))
	wants(t, resp, http.StatusMethodNotAllowed)

	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodPost) {
		t.Errorf("Allow is %q, want POST in it", allow)
	}

	wants(t, do(t, s, http.MethodGet, "/v1/batch", nil), http.StatusMethodNotAllowed)
	wants(t, do(t, s, http.MethodPost, "/v1/batch/", strings.NewReader("")), http.StatusNotFound)
}

// TestBatchOfAClosedStore. A store closed under the handler is a server on its
// way down, and the batch is refused rather than crashing.
func TestBatchOfAClosedStore(t *testing.T) {
	s, db := newServer(t, Options{})

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	wants(t, post(t, s, lines(`{"op":"write","key":"k","value":"v"}`)), http.StatusServiceUnavailable)
}

// counted wraps whatever a Server writes through and says how many batches
// reached it. It is what the fuzz target asserts on, since "no partial batch"
// is really "the store was not asked at all unless the whole body parsed".
type counted struct {
	writes
	batches int
}

func (c *counted) WriteBatch(b *litekv.Batch) error {
	c.batches++
	return c.writes.WriteBatch(b)
}

// FuzzBatchBody puts arbitrary bytes through the parser as a body.
//
// Two things must hold for every input and neither is about the input being
// sensible. Nothing may panic — a parser reachable from a socket is reachable
// by anybody. And nothing may be stored unless the whole body was understood:
// the answer is 204 and the store was asked exactly once, or the answer is a
// refusal and the store was not asked at all. A parser that stored as it went
// would fail the second on the first input holding one good line and one bad
// one, and the fuzzer finds that in seconds.
func FuzzBatchBody(f *testing.F) {
	for _, seed := range []string{
		"",
		"\n",
		`{"op":"write","key":"a","value":"1"}`,
		"{\"op\":\"write\",\"key\":\"a\",\"value\":\"1\"}\n{\"op\":\"delete\",\"key\":\"a\"}\n",
		"{\"op\":\"write\",\"key\":\"a\",\"value\":\"1\"}\n{\"op\":\"nonsense\"}\n",
		`{"op":"write","key_b64":"_w","value_b64":"AAE"}`,
		`{"op":"write","key":"a","key_b64":"YQ"}`,
		`{"op":"write","key":"a","value":"v","expires":"2030-01-01T00:00:00Z"}`,
		"{\"op\":\"write\",\"key\":\"\xff\"}",
		"{",
		"null",
		"[]",
	} {
		f.Add([]byte(seed))
	}

	// One store for the whole run, and one that does as little as possible
	// besides taking records: a fuzz target that spends its time rotating and
	// merging is a fuzz target running a hundredth of the inputs. The engine's
	// own targets are where rotation and merging get fuzzed.
	db, err := litekv.OpenDB(f.TempDir(), litekv.DBOptions{
		Sync: litekv.SyncNever, SegmentSize: 1 << 30, MergeTrigger: 1 << 30,
	})
	if err != nil {
		f.Fatal(err)
	}
	defer db.Close()

	s := New(db, Options{Logger: quiet()})
	defer s.Close()

	spy := &counted{writes: s.writes}
	s.writes = spy

	f.Fuzz(func(t *testing.T, body []byte) {
		before := spy.batches

		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/batch", bytes.NewReader(body)))

		asked := spy.batches - before

		switch rec.Code {
		case http.StatusNoContent:
			if asked != 1 {
				t.Fatalf("a batch that was accepted reached the store %d times", asked)
			}
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			if asked != 0 {
				t.Fatalf("a batch that was refused with %d reached the store anyway", rec.Code)
			}
		default:
			t.Fatalf("answered %d: %s", rec.Code, rec.Body.Bytes())
		}
	})
}
