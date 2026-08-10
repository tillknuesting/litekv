package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tillknuesting/litekv"
)

func TestHealthSaysWhetherItCanServe(t *testing.T) {
	s, db := newServer(t, Options{})

	if body := wants(t, do(t, s, http.MethodGet, healthPath, nil), http.StatusOK); string(body) != "ok\n" {
		t.Errorf("a healthy server said %q", body)
	}

	// A store on its way down is not healthy, and saying so is the whole job:
	// a load balancer that goes on sending here is the reason to have the route.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wants(t, do(t, s, http.MethodGet, healthPath, nil), http.StatusServiceUnavailable)
}

// TestMetricsCountWhatWasServed. The numbers are per route and not per path,
// which is the only thing about this that could go badly wrong: a label taken
// from the URL would be one series per key, and /metrics would grow with the
// store until it was the largest thing this server sends anybody.
func TestMetricsCountWhatWasServed(t *testing.T) {
	s, _ := newServer(t, Options{})

	for i := 0; i < 3; i++ {
		wants(t, do(t, s, http.MethodPut, fmt.Sprintf("/v1/keys/k-%d", i),
			strings.NewReader("v")), http.StatusNoContent)
	}
	wants(t, do(t, s, http.MethodGet, "/v1/keys/k-0", nil), http.StatusOK)
	wants(t, do(t, s, http.MethodGet, "/v1/keys/missing", nil), http.StatusNotFound)

	body := string(wants(t, do(t, s, http.MethodGet, metricsPath, nil), http.StatusOK))

	for _, want := range []string{
		`litekv_requests_total{route="/v1/keys/{key}",method="PUT",status="204"} 3`,
		`litekv_requests_total{route="/v1/keys/{key}",method="GET",status="200"} 1`,
		`litekv_requests_total{route="/v1/keys/{key}",method="GET",status="404"} 1`,
		`litekv_request_duration_seconds_count{route="/v1/keys/{key}"} 5`,
		`litekv_request_duration_seconds_bucket{route="/v1/keys/{key}",le="+Inf"} 5`,
		// A real bucket and not only the catch-all. Every one of these requests
		// is served out of memory in well under five seconds, so a scheme that
		// counted a request into just one bucket — or into none — shows up here
		// and nowhere else: the buckets are cumulative, so the second largest
		// has to hold all five too.
		`litekv_request_duration_seconds_bucket{route="/v1/keys/{key}",le="5"} 5`,
		"litekv_role{role=\"leader\",leader=\"\"} 1",
		"litekv_store_keys 3",
		"litekv_term 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not have:\n  %s", want)
		}
	}

	// Three keys were written and one was missed, and none of them is a label.
	for _, never := range []string{"k-0", "k-1", "missing"} {
		if strings.Contains(body, never) {
			t.Errorf("/metrics has a series per key: %q is in it", never)
		}
	}
}

// TestEveryRouteIsCounted holds the registration to going through handle. A
// route added straight to the mux is a route missing from /metrics, and nothing
// anywhere would say so.
func TestEveryRouteIsCounted(t *testing.T) {
	s, _ := newServer(t, Options{})

	// One request to each, whatever it answers: what is being asserted is that
	// the route appears in the metrics at all.
	for _, ask := range []struct{ method, target string }{
		{http.MethodGet, "/v1/keys/k"},
		{http.MethodPut, "/v1/keys/k"},
		{http.MethodDelete, "/v1/keys/k"},
		{http.MethodGet, "/v1/keys"},
		{http.MethodPost, "/v1/batch"},
		{http.MethodGet, "/v1/status"},
		{http.MethodGet, healthPath},
	} {
		do(t, s, ask.method, ask.target, strings.NewReader(""))
	}

	body := string(wants(t, do(t, s, http.MethodGet, metricsPath, nil), http.StatusOK))

	for _, route := range []string{
		"/v1/keys/{key}", "/v1/keys", "/v1/batch", "/v1/status", healthPath,
	} {
		if !strings.Contains(body, fmt.Sprintf("route=%q", route)) {
			t.Errorf("%s is not counted", route)
		}
	}
}

// TestTheTokenCoversEverythingButHealth. The exemption is deliberate and the
// coverage is the point: replication is behind it too, and that is the route
// that hands over the whole database.
func TestTheTokenCoversEverythingButHealth(t *testing.T) {
	s, _ := newServer(t, Options{Token: "the-secret"})

	for _, ask := range []struct{ method, target string }{
		{http.MethodGet, "/v1/keys/k"},
		{http.MethodPut, "/v1/keys/k"},
		{http.MethodDelete, "/v1/keys/k"},
		{http.MethodGet, "/v1/keys"},
		{http.MethodPost, "/v1/batch"},
		{http.MethodGet, "/v1/status"},
		{http.MethodPost, "/v1/promote"},
		{http.MethodGet, replicaPath},
		{http.MethodGet, metricsPath},
		{http.MethodGet, "/v1/a-route-that-does-not-exist"},
	} {
		t.Run(ask.method+" "+ask.target, func(t *testing.T) {
			// On a context, because one of these is the replication stream and
			// a stream that is wrongly let through does not answer — it starts
			// streaming and never returns. Without this the failure is the
			// suite hanging until its timeout rather than this line saying
			// which route was open, which is the difference between a test that
			// reports and a test that stops the machine.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req := httptest.NewRequest(ask.method, ask.target, nil).WithContext(ctx)
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)

			resp := rec.Result()
			wants(t, resp, http.StatusUnauthorized)

			if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
				t.Errorf("WWW-Authenticate is %q", got)
			}
		})
	}

	// Health is the exemption, so that a load balancer probing this node does
	// not have to hold the secret that opens the database.
	wants(t, do(t, s, http.MethodGet, healthPath, nil), http.StatusOK)

	// And with the token, the ordinary answers come back.
	with := map[string]string{"Authorization": "Bearer the-secret"}
	wants(t, asking(t, s, http.MethodPut, "/v1/keys/k", "v", with), http.StatusNoContent)
	wants(t, asking(t, s, http.MethodGet, "/v1/keys/k", "", with), http.StatusOK)

	for _, wrong := range []string{
		"", "Bearer", "Bearer ", "Bearer the-secre", "Bearer the-secrets",
		"the-secret", "Basic the-secret", "bearer the-secret",
	} {
		t.Run("wrong: "+wrong, func(t *testing.T) {
			wants(t, asking(t, s, http.MethodGet, "/v1/keys/k", "",
				map[string]string{"Authorization": wrong}), http.StatusUnauthorized)
		})
	}
}

// TestNoTokenLetsEverythingThrough is the other half: the zero value is a server
// with no authentication, which is what listening on loopback is for. Worth a
// test because an empty configured token compared against an empty header would
// let a request through on a server that meant to require one.
func TestNoTokenLetsEverythingThrough(t *testing.T) {
	s, _ := newServer(t, Options{})

	wants(t, do(t, s, http.MethodPut, "/v1/keys/k", strings.NewReader("v")), http.StatusNoContent)
	wants(t, do(t, s, http.MethodGet, healthPath, nil), http.StatusOK)
}

// TestAFollowerCarriesTheToken. A leader with a token is a leader whose
// replication route is behind it, so a follower that did not present one would
// spend its life reconnecting to a 401 — and it would do it at the long backoff,
// which is exactly quiet enough not to be noticed.
func TestAFollowerCarriesTheToken(t *testing.T) {
	up := serving(t, litekv.DBOptions{Sync: litekv.SyncNever})
	up.api.opts.Token = "the-secret"

	if err := up.db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Without it, nothing arrives.
	without, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer without.Close()

	blocked := followingAt(t, without, up.srv.URL)
	time.Sleep(150 * time.Millisecond)

	if _, err := without.Read([]byte("k")); err == nil {
		t.Error("a follower with no token got the leader's records")
	}
	if err := blocked.Close(); err != nil {
		t.Fatal(err)
	}

	// With it, they do.
	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	f, err := Follow(db, up.srv.URL, FollowerOptions{Token: "the-secret", Logger: quiet(),
		MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	waitForPositions(t, db, up.db, "a follower with the token to catch up")

	if value, err := db.Read([]byte("k")); err != nil || string(value) != "v" {
		t.Errorf("a follower with the token read %q, '%v'", value, err)
	}
}

// TestAStreamTakesItsWriteDeadlineOff is the one route where a server-wide
// WriteTimeout is wrong: a stream is a response meant to still be being written
// next week, and a deadline on it is a follower disconnected on a timer.
//
// Driven over a real listener with a deadline short enough to fire, because the
// thing being tested is what net/http does with it, not what this package
// intends.
func TestAStreamTakesItsWriteDeadlineOff(t *testing.T) {
	db, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	s := New(db, Options{Logger: quiet()})

	// Counted, because "the record arrived" is not the assertion. A stream cut
	// by a deadline is a stream the follower reconnects to, and it gets
	// everything either way — so the only thing that tells a stream which kept
	// its deadline from one which cleared it is how many times the follower had
	// to come back.
	var streams atomic.Int64

	wire := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == replicaPath {
			streams.Add(1)
		}
		s.ServeHTTP(w, r)
	}))
	wire.Config.WriteTimeout = 150 * time.Millisecond
	wire.Start()

	// Registered in the order they have to run in reverse, which is what
	// t.Cleanup gives and what a defer does not: a defer runs before every
	// cleanup, so `defer wire.Close()` waits on a replication stream that the
	// follower's own cleanup has not been reached to stop yet, and the test
	// hangs until the ten-minute panic. serving() next door has this same
	// ordering for the same reason.
	//
	//	follower stops, then the streams end, then the listener waits for what
	//	is left, then the store closes.
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(wire.Close)
	t.Cleanup(func() { _ = s.Close() })

	follower, err := litekv.OpenDB(t.TempDir(), litekv.DBOptions{Sync: litekv.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followingAt(t, follower, wire.URL)
	waitForPositions(t, follower, db, "the follower to catch up")

	// Idle for longer than any deadline this server would impose — the
	// listener's is 150ms — and comfortably past a second, so that a stream
	// which cleared its deadline to a moment from now rather than to never is
	// caught as well as one that did not clear it at all.
	time.Sleep(1200 * time.Millisecond)

	if err := db.Write([]byte("after-the-deadline"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := follower.Read([]byte("after-the-deadline")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a record written after the write timeout never reached the follower")
		}
		time.Sleep(time.Millisecond)
	}

	// One connection, from beginning to end. More than one means the deadline
	// cut the stream and the follower papered over it by reconnecting.
	if opened := streams.Load(); opened != 1 {
		t.Errorf("the follower opened %d streams; the first one was cut by a write deadline", opened)
	}
}
