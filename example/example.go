// Command example walks through what litekv does: the store on its own, the
// same store keeping a file, and the same store mirroring its writes to a log
// of your own.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tillknuesting/litekv"
)

func main() {
	// log.Fatalln calls os.Exit, which skips deferred calls, so each section
	// returns its error rather than exiting from under its own defer.
	for _, section := range []struct {
		name string
		run  func() error
	}{
		{"in memory", inMemory},
		{"backed by a file", backedByFile},
		{"mirrored to your own log", ownLog},
		{"split across segments", segments},
	} {
		fmt.Printf("\n== %s ==\n", section.name)
		if err := section.run(); err != nil {
			log.Fatalln(err)
		}
	}
}

// inMemory is the store on its own: no files, no options, nothing to close.
func inMemory() error {
	kvs := &litekv.KeyValueStore{}

	if err := kvs.Write([]byte("foo"), []byte("bar")); err != nil {
		return err
	}

	// Read hands back a copy, so it is yours to keep or modify.
	value, err := kvs.Read([]byte("foo"))
	if err != nil {
		return err
	}
	fmt.Println("foo =", string(value))

	// View hands back the stored bytes instead, which saves the copy. They are
	// only valid until the callback returns, and the store is locked for
	// reading while it runs, so do not hold on to them or call back into it.
	if err := kvs.View([]byte("foo"), func(value []byte) error {
		fmt.Println("foo, without copying =", string(value))
		return nil
	}); err != nil {
		return err
	}

	// An update appends a new record and repoints the index at it.
	if err := kvs.Write([]byte("foo"), []byte("newValue")); err != nil {
		return err
	}

	// A delete appends a tombstone, which reads as a deleted key rather than a
	// missing one. The two are worth telling apart.
	if err := kvs.Delete([]byte("foo")); err != nil {
		return err
	}
	if _, err := kvs.Read([]byte("foo")); errors.Is(err, litekv.ErrorKeyDeleted) {
		fmt.Println("foo was deleted")
	} else if errors.Is(err, litekv.ErrorKeyNotFound) {
		fmt.Println("foo was never written")
	} else if err != nil {
		return err
	}

	if err := kvs.Write([]byte("foo2"), []byte("bar2")); err != nil {
		return err
	}

	// The index can be saved and loaded, which is cheaper than rebuilding it
	// from the records. Put the data in place before loading an index for it.
	saved, err := kvs.SaveIndex()
	if err != nil {
		return err
	}
	if err := kvs.LoadIndex(saved); err != nil {
		return err
	}

	// Or rebuild it from the records themselves, if it was lost. A
	// *litekv.CorruptAtError here means the data has a damaged tail; the index
	// still covers everything before that offset.
	if err := kvs.RebuildIndex(); err != nil {
		return err
	}

	// Verify checks every record against its checksum.
	if err := kvs.Verify(); err != nil {
		return err
	}

	// Every version of every key is still there until compaction.
	fmt.Println("all records before compaction:")
	if err := kvs.PrintAllKeyValuePairs(); err != nil {
		return err
	}

	if err := kvs.Compact(); err != nil {
		return err
	}

	fmt.Println("all records after compaction:")
	return kvs.ForEach(func(key, value []byte, deleted bool) bool {
		fmt.Printf("  %s = %s (deleted: %t)\n", key, value, deleted)
		return true
	})
}

// backedByFile keeps the same store in a file, so it outlives the process.
func backedByFile() error {
	dir, err := os.MkdirTemp("", "litekv")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "store.kv")

	// SyncEvery trades a bounded window of writes for not waiting on the disk
	// every time. The default, SyncAlways, waits; SyncNever never does.
	kvs, err := litekv.Open(path, litekv.Options{Sync: litekv.SyncEvery, Interval: time.Second})
	if err != nil {
		return err
	}
	// Close syncs whatever the timer has not, and runs even if what follows
	// panics. It does not run if the process is killed, which costs nothing:
	// every record is with the operating system by the time Write returns, so
	// only losing power can take one back.
	defer kvs.Close()

	if err := kvs.Write([]byte("persisted"), []byte("across restarts")); err != nil {
		return err
	}

	// Sync forces the flush the timer would have done, whatever the policy.
	if err := kvs.Sync(); err != nil {
		return err
	}
	if err := kvs.Close(); err != nil {
		return err
	}

	reopened, err := litekv.Open(path, litekv.Options{})
	if err != nil {
		return err
	}
	defer reopened.Close()

	value, err := reopened.Read([]byte("persisted"))
	if err != nil {
		return err
	}
	fmt.Println("after reopening, persisted =", string(value))

	// The file holds exactly the Data slice, so it can be loaded by hand
	// instead. Recover rebuilds the index and drops a damaged tail, reporting
	// how many bytes it dropped.
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	byHand := &litekv.KeyValueStore{Data: raw}
	discarded, err := byHand.Recover()
	if err != nil {
		return err
	}
	fmt.Printf("loaded %d bytes by hand, discarded %d\n", len(raw), discarded)

	value, err = byHand.Read([]byte("persisted"))
	if err != nil {
		return err
	}
	fmt.Println("read from the loaded slice:", string(value))

	return nil
}

// countingLog is a Log of one's own: three methods, and the store will mirror
// every record to it. A real one might encrypt, or send the records somewhere.
type countingLog struct {
	mu      sync.Mutex
	data    []byte
	records int
	syncs   int
}

func (c *countingLog) WriteAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = append(c.data[:off], p...)
	c.records++
	return len(p), nil
}

func (c *countingLog) Truncate(size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if int64(len(c.data)) > size {
		c.data = c.data[:size]
	}
	return nil
}

func (c *countingLog) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.syncs++
	return nil
}

// ownLog mirrors a store to something this package knows nothing about.
func ownLog() error {
	// A store that already holds something, to show the log being seeded.
	kvs := &litekv.KeyValueStore{}
	if err := kvs.Write([]byte("written"), []byte("before attaching")); err != nil {
		return err
	}

	sink := &countingLog{}
	if err := kvs.Attach(sink, litekv.Options{Sync: litekv.SyncNever}); err != nil {
		return err
	}

	// Attach assumes the log already holds what the store does, which it does
	// not here, so put the store's records into it.
	if err := kvs.Rewrite(); err != nil {
		return err
	}

	if err := kvs.Write([]byte("written"), []byte("after attaching")); err != nil {
		return err
	}
	if err := kvs.Sync(); err != nil {
		return err
	}

	fmt.Printf("the log took %d writes and %d syncs, and holds %d bytes\n",
		sink.records, sink.syncs, len(sink.data))

	// Detach syncs once more and leaves an ordinary in-memory store behind.
	if err := kvs.Detach(); err != nil {
		return err
	}

	if err := kvs.Write([]byte("written"), []byte("after detaching")); err != nil {
		return err
	}

	// The log stopped where it was left, while the store carried on.
	fmt.Printf("after detaching: the log holds %d bytes, the store %d\n", len(sink.data), len(kvs.Data))

	// What the log holds is a store in its own right.
	fromLog := &litekv.KeyValueStore{Data: sink.data}
	if _, err := fromLog.Recover(); err != nil {
		return err
	}

	value, err := fromLog.Read([]byte("written"))
	if err != nil {
		return err
	}
	fmt.Println("the log's own copy says written =", string(value))

	return nil
}

// segments shows the store split across several logs, merged in the background
// instead of compacted all at once.
func segments() error {
	dir, err := os.MkdirTemp("", "litekv-db")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Tiny segments, so that a handful of records is enough to see rotation and
	// merging. A real one would be megabytes.
	db, err := litekv.OpenDB(dir, litekv.DBOptions{
		Sync:         litekv.SyncNever,
		SegmentSize:  256,
		MergeTrigger: 1 << 30, // merge only when asked, so the example can show it
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// The same few keys, written over and over: most of these records are dead
	// the moment the next one lands.
	for round := 0; round < 20; round++ {
		for _, key := range []string{"alpha", "beta", "gamma"} {
			if err := db.Write([]byte(key), []byte(fmt.Sprintf("%s-%02d", key, round))); err != nil {
				return err
			}
		}
	}
	if err := db.Delete([]byte("gamma")); err != nil {
		return err
	}

	fmt.Printf("%d records over %d segments\n", db.Len(), db.Segments())

	// Merging keeps the newest record for each live key and drops the rest.
	// Reads and writes carry on while it runs; here it is called directly so
	// that the effect is visible.
	if err := db.Merge(); err != nil {
		return err
	}
	fmt.Printf("after merging: %d segments\n", db.Segments())

	value, err := db.Read([]byte("alpha"))
	if err != nil {
		return err
	}
	fmt.Println("alpha =", string(value))

	// A deleted key reads as deleted until a merge drops the tombstone, and as
	// missing afterwards. Both mean there is no value.
	if _, err := db.Read([]byte("gamma")); errors.Is(err, litekv.ErrorKeyNotFound) || errors.Is(err, litekv.ErrorKeyDeleted) {
		fmt.Println("gamma is gone")
	} else if err != nil {
		return err
	}

	return db.ForEach(func(key, value []byte) bool {
		fmt.Printf("  %s = %s\n", key, value)
		return true
	})
}
