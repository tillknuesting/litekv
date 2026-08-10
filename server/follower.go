package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tillknuesting/litekv"
)

// The follower's half: dial a leader, apply what arrives, and dial again when
// the connection ends.
//
// Reconnecting is the whole of it. A connection ending is not a failure here —
// it is a leader restarting, a load balancer recycling, a laptop closing, or
// this server being asked to stop — and a follower that treated it as one would
// need somebody to restart it. What it must never do is lose where it had got
// to, and it does not have to try: the position is written down beside the
// follower's own logs by Apply, so a follower that comes back reads it out of
// the store and carries on. Nothing here keeps a copy that could go stale.

// FollowerOptions configures a Follower. The zero value is usable.
type FollowerOptions struct {
	// Token is the bearer token this follower presents to the leader, for a
	// leader started with one. Empty sends no Authorization header.
	//
	// The replication route is behind the token like everything else, and it is
	// the route that matters most: it hands over the whole database.
	Token string

	// Client is the HTTP client to dial with. Nil means one of this package's
	// own.
	//
	// It must have no Timeout: a replication stream is a connection meant to
	// stay open, and a Client.Timeout would end it on a schedule and call the
	// reconnect that follows progress.
	Client *http.Client

	// Logger is where a connection that ended, and why, is written down. Nil
	// means slog.Default().
	Logger *slog.Logger

	// MinBackoff and MaxBackoff bound the wait between attempts. Zero means
	// 100ms and 5s.
	MinBackoff, MaxBackoff time.Duration

	// MaxFrame is the largest frame this follower will take, in bytes. Zero
	// means 1 GiB.
	//
	// A snapshot arrives as one frame holding the leader's whole live store, so
	// this is the largest store that can be followed as much as it is a bound
	// on a hostile leader.
	MaxFrame int64

	// BatchSize is handed to the store as litekv.ReplicaOptions.BatchSize.
	// Zero means the engine's own default.
	BatchSize int64
}

// Follower follows a leader over HTTP and applies what it sends. It is started
// by Follow and stopped by Close.
type Follower struct {
	db     *litekv.DB
	leader *url.URL
	client *http.Client
	log    *slog.Logger
	opts   FollowerOptions

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// Follow starts following the leader at the given address and applying its
// records to db. leader is the base address of the leader's HTTP API — its
// scheme, host and any path prefix a proxy puts in front of it — and the
// replication route is added to it.
//
//	f, err := server.Follow(db, "http://10.0.0.2:8080", server.FollowerOptions{})
//	defer f.Close()
//
// It returns as soon as the goroutine is running, without waiting to find out
// whether the leader is there: a leader that is down is a leader to reconnect
// to, which is the same thing this does for the rest of its life. Only an
// address that could never be dialled is an error.
//
// It does not take ownership of db, and it does not close it. What it does do
// is write to it, so it has to be stopped before the store is closed.
//
// Nothing here makes db read-only. A store that is following and is also being
// written to through this package's handlers will diverge from its leader —
// its own records go into its own log while the leader's position marches on —
// and there is nothing in this piece that stops it. Roles are the next piece of
// work; until then, do not write to a node that is following.
func Follow(db *litekv.DB, leader string, opts FollowerOptions) (*Follower, error) {
	base, err := url.Parse(leader)
	if err != nil {
		return nil, fmt.Errorf("leader address %q: %w", leader, err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("leader address %q: want an http or https URL", leader)
	}
	if base.Host == "" {
		return nil, fmt.Errorf("leader address %q: no host in it", leader)
	}

	if opts.Client == nil {
		opts.Client = &http.Client{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MinBackoff <= 0 {
		opts.MinBackoff = 100 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 5 * time.Second
	}
	if opts.MaxBackoff < opts.MinBackoff {
		opts.MaxBackoff = opts.MinBackoff
	}
	if opts.MaxFrame <= 0 {
		opts.MaxFrame = defaultMaxFrame
	}

	ctx, cancel := context.WithCancel(context.Background())

	f := &Follower{
		db:     db,
		leader: base.JoinPath(replicaPath),
		client: opts.Client,
		log:    opts.Logger,
		opts:   opts,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go f.run(ctx)
	return f, nil
}

// Close stops following and waits for the goroutine to finish. Closing twice is
// harmless.
//
// It waits rather than merely asking, and that is the point of it: a batch
// being applied when the stop arrives is a write to the store, and returning
// before it finished would leave the caller free to close the store underneath
// it. Waiting also means the position is written down by the time this returns,
// so a follower stopped mid-stream comes back where it left off rather than
// asking for the whole store again.
func (f *Follower) Close() error {
	f.once.Do(func() {
		f.cancel()
		<-f.done
	})
	return nil
}

// run dials, streams, and dials again for as long as the context is alive.
func (f *Follower) run(ctx context.Context) {
	defer close(f.done)

	wait := f.opts.MinBackoff

	for {
		started := time.Now()
		err := f.stream(ctx)

		if ctx.Err() != nil {
			f.log.Debug("stopped following", "leader", f.leader.String(),
				"at", f.db.Applied())
			return
		}

		switch {
		// A connection that stayed up longer than the longest wait was a
		// working connection, whatever ended it, so the next attempt starts
		// from the shortest wait again. Without this a leader that restarts
		// once a day would be reconnected to at whatever the backoff had
		// climbed to during its last outage.
		case time.Since(started) >= f.opts.MaxBackoff:
			wait = f.opts.MinBackoff

		// A refusal that asking again cannot change: a leader that has been
		// replaced, or a position it will not take. Somebody has to repoint or
		// promote something, so this keeps trying — the somebody may be along
		// shortly — but at the longest interval rather than climbing to it.
		case hopeless(err):
			wait = f.opts.MaxBackoff
			f.log.Warn("the leader refused this follower", "leader", f.leader.String(), "err", err)

		default:
			if wait *= 2; wait > f.opts.MaxBackoff {
				wait = f.opts.MaxBackoff
			}
		}

		if err != nil && !hopeless(err) {
			f.log.Debug("the stream ended", "leader", f.leader.String(), "err", err)
		}

		// Half of the wait is fixed and half is jittered, so several followers
		// that lost the same leader do not all come back at the same instant,
		// and none of them comes back after no wait at all.
		delay := wait/2 + rand.N(wait/2+1)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// stream is one connection: say where this store has got to, then apply what
// arrives until the connection ends.
func (f *Follower) stream(ctx context.Context) error {
	// Asked of the store every time rather than remembered, so that a batch
	// that failed to apply cannot leave this claiming a position the store does
	// not hold.
	at, err := positionParam(f.db.Applied())
	if err != nil {
		return err
	}

	target := *f.leader
	target.RawQuery = url.Values{"from": {at}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", contentTypeStream)
	if f.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+f.opts.Token)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// Drained a little before closing, so an ordinary short answer can go
		// back in the connection pool. A stream that ended early is not worth
		// draining and the limit is what stops this reading one.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		says, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &refusedError{
			status: resp.StatusCode,
			term:   resp.Header.Get(headerTerm),
			says:   strings.TrimSpace(string(says)),
		}
	}

	opts := litekv.ReplicaOptions{BatchSize: f.opts.BatchSize}

	for {
		kind, next, payload, err := readFrame(resp.Body, f.opts.MaxFrame)
		if err != nil {
			return err // the connection ended, which is not a failure
		}

		switch kind {
		case frameSnapshot:
			if err := f.db.ApplySnapshot(next, bytes.NewReader(payload), opts); err != nil {
				return err
			}

		case frameBatch:
			// From where the store says it is, not from where the last frame
			// left it. They are the same until something else has written to
			// this store, and when they are not, Apply refusing is the only
			// thing between that and a quiet divergence.
			from := f.db.Applied()
			if _, err := f.db.Apply(from, next, bytes.NewReader(payload), opts); err != nil {
				return err
			}

		default:
			return fmt.Errorf("a frame of kind %q", kind)
		}
	}
}

// refusedError is a leader that answered with a status rather than a stream.
type refusedError struct {
	status int
	term   string
	says   string
}

func (e *refusedError) Error() string {
	if e.term != "" {
		return fmt.Sprintf("the leader answered %d at term %s: %s", e.status, e.term, e.says)
	}
	return fmt.Sprintf("the leader answered %d: %s", e.status, e.says)
}

// hopeless reports whether asking the same leader again straight away could
// possibly go differently.
//
// A 4xx says the request was the problem, and this follower will build the same
// request next time: a leader that has been replaced (409), or a position it
// will not decode (400). ErrorFenced from this store's own Apply is the mirror
// of it — this store has heard of a newer leader and will refuse everything
// the one it is pointed at sends.
//
// Neither is a reason to give up. Both are a reason for somebody to promote
// something or to repoint this process, and that somebody may be along shortly,
// so the retries carry on at the longest interval instead of the shortest.
func hopeless(err error) bool {
	var refused *refusedError
	if errors.As(err, &refused) {
		return refused.status >= 400 && refused.status < 500
	}
	return errors.Is(err, litekv.ErrorFenced)
}
