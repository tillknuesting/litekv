// Command litekvd serves a litekv store over HTTP.
//
// It opens one store in one directory and hands it to package server. Nothing
// else may have that directory open while it runs, and the store enforces that:
// a second litekvd on the same -dir fails to start, saying the directory is
// open in another process, rather than writing over the first one's log. The
// lock goes when the process does, however it goes, so a machine that lost
// power comes back and starts normally. See the Limitations in the README for
// the platforms that cannot take it.
//
//	litekvd -dir /var/lib/litekv -addr 127.0.0.1:8080
//
// With -leader it also follows another litekvd, taking its records over the
// replication route on that node's ordinary listener:
//
//	litekvd -dir /var/lib/replica -addr 127.0.0.1:8081 -leader http://127.0.0.1:8080
//
// A node started that way is a replica: it refuses writes with 409 and a
// Litekv-Leader header saying where they should go, and reports itself as one
// at /v1/status. POST /v1/promote stops it following and raises its term, which
// is how it becomes a leader — and which is a decision something outside this
// process has to make, since raising the term in two places at once puts two
// stores on the same term and gives the guarantee away.
//
// It listens on loopback unless told otherwise, because there is no
// authentication and no TLS in front of it yet. Put it behind a proxy or on a
// private network before giving it an address a stranger can reach.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tillknuesting/litekv"
	"github.com/tillknuesting/litekv/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "litekvd:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dir     = flag.String("dir", "", "directory holding the store (required)")
		addr    = flag.String("addr", "127.0.0.1:8080", "address to listen on")
		syncing = flag.String("sync", "always",
			"durability: always, every, or never. always is the store's own default and the only one that\n"+
				"survives losing the machine; every trades a window of writes for throughput")
		interval     = flag.Duration("sync-interval", 0, "how often to sync under -sync every (0 for one second)")
		segmentSize  = flag.Int64("segment-size", 0, "bytes before a log is frozen and a new one started (0 for 4 MiB)")
		mergeTrigger = flag.Int("merge-trigger", 0, "logs of a size before they are merged (0 for two, below two turns merging off)")
		maxValue     = flag.Int64("max-value", 0, "largest value a write may carry (0 for 16 MiB)")
		maxBatch     = flag.Int64("max-batch", 0, "largest body POST /v1/batch will take (0 for 32 MiB)")
		maxScan      = flag.Int("max-scan", 0,
			"most pairs a range will answer with, and the most a client's own ?limit= may ask for (0 for 1000).\n"+
				"A range holds the store's read lock while it gathers, so this is what stops one client\n"+
				"parking a walk of the whole store in front of the writes")
		queue  = flag.Int("queue", 0, "writes that may be waiting to be stored before a handler blocks (0 for 1024)")
		leader = flag.String("leader", "",
			"base URL of a leader to follow, such as http://10.0.0.2:8080. Its records are applied to this\n"+
				"store as they are written. Empty follows nobody, which is what a leader does")
		waitFor = flag.Int("wait-for", 0,
			"how many followers must have a write before it is acknowledged (0 for none, which is\n"+
				"asynchronous replication). This is semi-synchronous replication: it is what stops a\n"+
				"failover losing acknowledged writes, and what it cannot do is take a write back — the\n"+
				"record is in the log before anything waits, so a wait that runs out answers 202 rather\n"+
				"than 204 and says how many followers had it")
		waitTimeout = flag.Duration("wait-timeout", 0,
			"how long a write waits for -wait-for followers before answering 202 (0 for five seconds)")
		spoolDir = flag.String("spool-dir", "",
			"where a snapshot on its way to a follower is written before it is sent (empty for the\n"+
				"system temporary directory). It is spooled rather than held in memory, so this needs\n"+
				"room for about the live records, transiently, per follower taking one — and on most\n"+
				"Linux systems /tmp is a tmpfs, which would put it back in memory. -dir is usually right")
		heartbeat = flag.Duration("heartbeat", 0,
			"how often a leader with nothing to send tells its followers it is still there (0 for 10s).\n"+
				"It is what stands between a follower and a connection that was blackholed rather than\n"+
				"closed, which the OS keepalive notices about fifteen minutes later; negative turns it off")
		idle = flag.Duration("idle", 0,
			"how long this node waits to hear anything from its -leader before reconnecting (0 for 30s).\n"+
				"A few heartbeats rather than one, since dropping a working connection over a late beat\n"+
				"costs a reconnect and gains nothing")
		tokenFile = flag.String("token-file", "",
			"file holding a shared secret every request must carry as `Authorization: Bearer <token>`.\n"+
				"A file and not a flag value: an argument is visible in ps to every process on the machine.\n"+
				"The same token authenticates this node to -leader. Empty means no authentication at all")
		readTimeout = flag.Duration("read-timeout", 60*time.Second,
			"how long a request has to arrive, headers and body. Raise it if a large -max-batch has to\n"+
				"cross a slow link; 0 turns it off")
		idleTimeout  = flag.Duration("idle-timeout", 120*time.Second, "how long an idle keep-alive connection is held open")
		writeTimeout = flag.Duration("write-timeout", 60*time.Second,
			"how long a response has to be written. Replication streams are exempt and set no deadline,\n"+
				"since a stream is a response meant to still be going next week; 0 turns it off")
		shutdown = flag.Duration("shutdown-timeout", 10*time.Second, "how long requests in flight get once the server is asked to stop")
	)
	flag.Parse()

	if *dir == "" {
		flag.Usage()
		return errors.New("-dir is required")
	}

	policy, err := syncPolicy(*syncing)
	if err != nil {
		return err
	}

	token, err := tokenFrom(*tokenFile)
	if err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	db, err := litekv.OpenDB(*dir, litekv.DBOptions{
		Sync:         policy,
		Interval:     *interval,
		SegmentSize:  *segmentSize,
		MergeTrigger: *mergeTrigger,
	})
	if err != nil {
		return fmt.Errorf("opening %s: %w", *dir, err)
	}

	// Closed last and always, including on the paths below that return an error
	// with the store already open. A store closed twice reports ErrorClosed the
	// second time, which is why the ordinary path can close it too.
	defer func() {
		if err := db.Close(); err != nil && !errors.Is(err, litekv.ErrorClosed) {
			log.Error("closing the store", "err", err)
		}
	}()

	// Listening before serving, so that a port already taken is an error from
	// run and not a line in a log after main has said everything is fine.
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", *addr, err)
	}

	api := server.New(db, server.Options{
		MaxValue:    *maxValue,
		MaxBatch:    *maxBatch,
		MaxScan:     *maxScan,
		Queue:       *queue,
		Token:       token,
		WaitFor:     *waitFor,
		WaitTimeout: *waitTimeout,
		SpoolDir:    *spoolDir,
		Heartbeat:   *heartbeat,
		Logger:      log,
	})
	defer api.Close()

	// Through the Server rather than beside it, so that the two agree about
	// what this node is: a Follower started behind the Server's back leaves it
	// answering writes it should be refusing, and a store that takes a write
	// while it is following diverges from its leader for good. api.Close stops
	// it, in the right order, which is why there is no second defer here.
	if *leader != "" {
		if err := api.Follow(*leader, server.FollowerOptions{
			Token: token, Idle: *idle, Logger: log}); err != nil {
			return err
		}
	}

	srv := &http.Server{
		Handler: api,

		// Without these a client that opens a connection and sends a byte an
		// hour holds a handler for as long as it likes, and enough of them are
		// the whole server. ReadHeaderTimeout is the one that stops that, and
		// it is short because a request line and its headers are small however
		// slow the link.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       *readTimeout,
		IdleTimeout:       *idleTimeout,

		// WriteTimeout is a bound on how long a response may take to write, and
		// a replication stream is a response meant to be still being written
		// next week. Rather than go without the timeout because of that one
		// route, the route takes its own deadline off — see streamReplica.
		WriteTimeout: *writeTimeout,
	}

	// A replication stream is a request that never finishes on its own, and
	// Shutdown waits for every request rather than cancelling any of them. Left
	// to itself it would spend the whole of -shutdown-timeout waiting for a
	// handler that had no intention of returning, on every leader with a
	// follower attached.
	srv.RegisterOnShutdown(api.CloseStreams)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serving := make(chan error, 1)
	go func() { serving <- srv.Serve(listener) }()

	log.Info("serving", "addr", listener.Addr().String(), "dir", *dir,
		"sync", *syncing, "keys", db.Len(), "logs", db.Segments(), "leader", *leader,
		"authenticated", token != "")

	if token == "" && !strings.HasPrefix(*addr, "127.0.0.1:") && !strings.HasPrefix(*addr, "localhost:") {
		log.Warn("listening off loopback with no -token-file; anything that can reach this address "+
			"can read and write the whole store", "addr", listener.Addr().String())
	}

	select {
	case err := <-serving:
		return err

	case <-ctx.Done():
		log.Info("stopping")

		// Three steps here and four things, in this order, and the order is the
		// whole of it. The requests in flight get their time — the streams among
		// them are ended at once by the hook above, since they would otherwise
		// take all of it; then api.Close stops the follower, which is the other
		// thing writing to the store, and then the writer, which answers the
		// handlers waiting on it; then the store closes. Any other order answers
		// a request that was already accepted with a 503 for a store the client
		// had no reason to think was going away, drops a write that was a moment
		// from being acknowledged, or closes the store under a batch the
		// follower was in the middle of applying.
		timeout, cancel := context.WithTimeout(context.Background(), *shutdown)
		defer cancel()

		if err := srv.Shutdown(timeout); err != nil {
			log.Error("some requests did not finish", "err", err)
		}
		if err := api.Close(); err != nil {
			log.Error("closing the server", "err", err)
		}
		return db.Close()
	}
}

// tokenFrom reads the shared secret out of a file.
//
// A file rather than a flag value because an argument is visible in ps to every
// process on the machine, and a secret that every process can read is not one.
// Trailing whitespace is trimmed, since a token in a file almost always arrived
// with a newline on the end and a token that fails for that reason is an
// afternoon nobody gets back.
func tokenFrom(path string) (string, error) {
	if path == "" {
		return "", nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading the token: %w", err)
	}

	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("the token in %s is empty", path)
	}
	return token, nil
}

func syncPolicy(name string) (litekv.SyncPolicy, error) {
	switch name {
	case "always":
		return litekv.SyncAlways, nil
	case "every":
		return litekv.SyncEvery, nil
	case "never":
		return litekv.SyncNever, nil
	}
	return 0, fmt.Errorf("-sync %q: want always, every, or never", name)
}
