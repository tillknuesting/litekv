// Command litekvd serves a litekv store over HTTP.
//
// It opens one store in one directory and hands it to package server. Nothing
// else may have that directory open while it runs: the store is not
// multi-process safe and nothing checks, so a second litekvd on the same -dir
// will quietly write over the first one's log.
//
//	litekvd -dir /var/lib/litekv -addr 127.0.0.1:8080
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
		queue    = flag.Int("queue", 0, "writes that may be waiting to be stored before a handler blocks (0 for 1024)")
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
		MaxValue: *maxValue,
		MaxBatch: *maxBatch,
		MaxScan:  *maxScan,
		Queue:    *queue,
		Logger:   log,
	})
	defer api.Close()

	srv := &http.Server{Handler: api}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serving := make(chan error, 1)
	go func() { serving <- srv.Serve(listener) }()

	log.Info("serving", "addr", listener.Addr().String(), "dir", *dir,
		"sync", *syncing, "keys", db.Len(), "logs", db.Segments())

	select {
	case err := <-serving:
		return err

	case <-ctx.Done():
		log.Info("stopping")

		// Three things, in this order, and the order is the whole of it. The
		// requests in flight get their time; then the writer stores what is
		// queued and answers the handlers waiting on it; then the store closes.
		// Any other order answers a request that was already accepted with a
		// 503 for a store the client had no reason to think was going away, or
		// drops a write that was a moment from being acknowledged.
		timeout, cancel := context.WithTimeout(context.Background(), *shutdown)
		defer cancel()

		if err := srv.Shutdown(timeout); err != nil {
			log.Error("some requests did not finish", "err", err)
		}
		if err := api.Close(); err != nil {
			log.Error("closing the writer", "err", err)
		}
		return db.Close()
	}
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
