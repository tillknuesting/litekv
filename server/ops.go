package server

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tillknuesting/litekv"
)

// What somebody running this needs from it: whether it is alive, what it is
// doing, and a way to keep strangers out of it.
//
// All three are the same shape — they wrap the handlers rather than living
// inside them — because a route that had to remember to count itself is a route
// that will forget. The counting, the logging and the token check happen in one
// place each, and a route added later gets them without knowing they exist.

const (
	healthPath  = "/health"
	metricsPath = "/metrics"
)

// instrument counts a route, times it, and writes down what it answered.
//
// route is the mux pattern and not the path, and that is the whole reason it is
// passed in rather than read off the request: a label taken from r.URL.Path
// would be one metric series per key, which is a metrics endpoint that grows
// with the store and eventually is the largest thing this server sends anybody.
// (r.Pattern would do it, but the mux sets it on the request it hands the
// handler, not on the one a wrapper outside the mux is holding.)
func (s *Server) instrument(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		counted := &answer{ResponseWriter: w, status: http.StatusOK}

		h(counted, r)

		took := time.Since(started)
		s.metrics.observe(route, r.Method, counted.status, took)

		// At Debug, so that turning request logging on is a level and not a
		// flag. A server logging every request at Info is a server whose log
		// nobody reads, and the failures that matter already log themselves in
		// fail.
		s.log.Debug("served", "method", r.Method, "route", route,
			"status", counted.status, "bytes", counted.written,
			"seconds", took.Seconds())
	}
}

// answer remembers what a handler said, since a ResponseWriter will not tell.
type answer struct {
	http.ResponseWriter
	status  int
	written int64
	wrote   bool
}

func (a *answer) WriteHeader(status int) {
	if !a.wrote {
		a.status, a.wrote = status, true
	}
	a.ResponseWriter.WriteHeader(status)
}

func (a *answer) Write(p []byte) (int, error) {
	a.wrote = true // an implicit 200, which is what net/http would send

	n, err := a.ResponseWriter.Write(p)
	a.written += int64(n)
	return n, err
}

// Flush keeps the replication stream streaming. A ResponseWriter wrapped in
// something that is not a Flusher is a stream that buffers until it ends, which
// is a follower that never catches up — and the type assertion in
// streamReplica would fail long before that and answer 500.
func (a *answer) Flush() {
	if f, ok := a.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap is how http.ResponseController reaches the real writer, which
// streamReplica needs to take its write deadline off.
func (a *answer) Unwrap() http.ResponseWriter { return a.ResponseWriter }

// metrics is every number this server keeps about itself.
//
// Hand-rolled rather than a client library because the module has no
// dependencies and this is a hundred lines. If it ever needs exemplars, or
// native histograms, or anything else that is genuinely hard, take the
// dependency then.
type metrics struct {
	mu     sync.Mutex
	served map[served]uint64
	timing map[string]*timing
}

// served is one counter's labels. Bounded by construction: the routes are
// fixed, the methods are the ones the mux allows, and the statuses are the ones
// statusOf can return.
type served struct {
	route, method string
	status        int
}

// timing is one route's latency, in the buckets below.
type timing struct {
	counts [len(buckets) + 1]uint64
	sum    float64
}

// buckets are seconds, from a read served out of memory to a range that had to
// wait on a disk. The last one is +Inf and is not in the list.
var buckets = [...]float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5,
}

func newMetrics() *metrics {
	return &metrics{served: map[served]uint64{}, timing: map[string]*timing{}}
}

func (m *metrics) observe(route, method string, status int, took time.Duration) {
	seconds := took.Seconds()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.served[served{route, method, status}]++

	t := m.timing[route]
	if t == nil {
		t = &timing{}
		m.timing[route] = t
	}
	t.sum += seconds

	// Cumulative, which is what a Prometheus histogram bucket means: every
	// bucket counts what fell at or below it.
	for i, at := range buckets {
		if seconds <= at {
			t.counts[i]++
		}
	}
	t.counts[len(buckets)]++ // +Inf, which everything falls into
}

// health answers whether this server can serve, and nothing else.
//
// It asks the store the cheapest question that touches its state — has it
// reached the position every store has reached — which is a lock and a
// comparison, no disk. A health check that read a key would be a health check
// that says a store is unhealthy because a merge is holding the write lock.
//
// It is outside the token on purpose. A load balancer probing this is not a
// client, it has no business holding the secret that opens the database, and a
// health check nobody can reach without credentials is a node that gets taken
// out of rotation for the wrong reason.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Reached(litekv.DBPosition{}); err != nil {
		s.fail(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// exposition answers GET /metrics in the Prometheus text format.
func (s *Server) exposition(w http.ResponseWriter, r *http.Request) {
	var out strings.Builder

	s.metrics.mu.Lock()

	out.WriteString("# HELP litekv_requests_total Requests answered, by route, method and status.\n")
	out.WriteString("# TYPE litekv_requests_total counter\n")

	// Sorted, so that two scrapes of an unchanged server are the same bytes and
	// a diff of them says something.
	keys := make([]served, 0, len(s.metrics.served))
	for k := range s.metrics.served {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].route != keys[j].route {
			return keys[i].route < keys[j].route
		}
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].status < keys[j].status
	})

	for _, k := range keys {
		fmt.Fprintf(&out, "litekv_requests_total{route=%q,method=%q,status=\"%d\"} %d\n",
			k.route, k.method, k.status, s.metrics.served[k])
	}

	out.WriteString("# HELP litekv_request_duration_seconds How long a route took to answer.\n")
	out.WriteString("# TYPE litekv_request_duration_seconds histogram\n")

	routes := make([]string, 0, len(s.metrics.timing))
	for route := range s.metrics.timing {
		routes = append(routes, route)
	}
	sort.Strings(routes)

	for _, route := range routes {
		t := s.metrics.timing[route]
		for i, at := range buckets {
			fmt.Fprintf(&out, "litekv_request_duration_seconds_bucket{route=%q,le=%q} %d\n",
				route, strconv.FormatFloat(at, 'g', -1, 64), t.counts[i])
		}
		fmt.Fprintf(&out, "litekv_request_duration_seconds_bucket{route=%q,le=\"+Inf\"} %d\n",
			route, t.counts[len(buckets)])
		fmt.Fprintf(&out, "litekv_request_duration_seconds_sum{route=%q} %g\n", route, t.sum)
		fmt.Fprintf(&out, "litekv_request_duration_seconds_count{route=%q} %d\n",
			route, t.counts[len(buckets)])
	}

	s.metrics.mu.Unlock()

	// The store's own numbers, asked after the lock above is let go: they take
	// the store's lock, and holding two is how a deadlock gets written.
	leader, replica := s.following()
	role := "leader"
	if replica {
		role = "replica"
	}

	out.WriteString("# HELP litekv_role Which of the two this node is; 1 for the role it is.\n")
	out.WriteString("# TYPE litekv_role gauge\n")
	fmt.Fprintf(&out, "litekv_role{role=%q,leader=%q} 1\n", role, leader)

	for _, g := range []struct {
		name, help string
		value      int
	}{
		{"litekv_term", "The term this store is on.", int(s.db.Term())},
		{"litekv_store_keys", "Keys the store holds, tombstones included.", s.db.Len()},
		{"litekv_store_segments", "Logs the store is spread across.", s.db.Segments()},
	} {
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n",
			g.name, g.help, g.name, g.name, g.value)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(out.String()))
}

// allowed reports whether a request carries the token, if there is one to carry.
//
// Constant time, because a comparison that stops at the first wrong byte tells a
// caller how much of the token it has right, and a few thousand requests turn
// that into the whole of it.
//
// Everything is behind it except the health check, replication very much
// included: that route ships the entire database to whoever asks, and it would
// be the strangest thing on this server to leave open.
func (s *Server) allowed(r *http.Request) bool {
	if s.opts.Token == "" {
		return true
	}
	if r.URL.Path == healthPath {
		return true
	}

	const prefix = "Bearer "
	given := r.Header.Get("Authorization")
	if !strings.HasPrefix(given, prefix) {
		return false
	}

	return subtle.ConstantTimeCompare(
		[]byte(strings.TrimPrefix(given, prefix)), []byte(s.opts.Token)) == 1
}

// unauthorized is the answer to a request without the token. It says what kind
// of credential it wants and nothing about the one it was given.
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="litekv"`)
	writeError(w, http.StatusUnauthorized, "a bearer token is required")
}
