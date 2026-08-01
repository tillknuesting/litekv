// Command example walks through litekv from one end to the other: a store in
// memory, the same store saved and loaded by hand, one that keeps a file, one
// that mirrors its writes somewhere else, and one split across segments.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/tillknuesting/litekv"
)

// must keeps the example to one straight line. A real program would handle
// these rather than give up.
func must(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	dir, err := os.MkdirTemp("", "litekv")
	must(err)
	defer os.RemoveAll(dir)

	// ---------------------------------------------------------------- memory
	// The zero value is a working store that touches no disk.
	fmt.Println("== in memory ==")

	kvs := &litekv.KeyValueStore{}

	must(kvs.Write([]byte("foo"), []byte("bar")))

	// Read hands back a copy, yours to keep or modify.
	value, err := kvs.Read([]byte("foo"))
	must(err)
	fmt.Println("foo =", string(value))

	// View hands back the stored bytes instead, saving the copy. They last
	// only until the callback returns.
	must(kvs.View([]byte("foo"), func(value []byte) error {
		fmt.Println("foo, without copying =", string(value))
		return nil
	}))

	// An update appends a new record and points the index at it.
	must(kvs.Write([]byte("foo"), []byte("bar2")))

	// A delete appends a tombstone, so a deleted key reads differently from one
	// that was never written.
	must(kvs.Write([]byte("temporary"), []byte("value")))
	must(kvs.Delete([]byte("temporary")))

	_, err = kvs.Read([]byte("temporary"))
	fmt.Println("temporary:", err)
	_, err = kvs.Read([]byte("never written"))
	fmt.Println("never written:", err)

	// Every version of every key is still there until compaction.
	fmt.Println("records before compaction:")
	must(kvs.PrintAllKeyValuePairs())

	must(kvs.Compact())

	fmt.Println("records after compaction:")
	must(kvs.ForEach(func(key, value []byte, deleted bool) bool {
		fmt.Printf("  %s = %s\n", key, value)
		return true
	}))

	// ------------------------------------------------------------ by hand
	// The Data slice is the whole store, so persistence can be entirely your
	// own business.
	fmt.Println("\n== saved and loaded by hand ==")

	path := filepath.Join(dir, "by-hand.kv")
	must(os.WriteFile(path, kvs.Data, 0o644))

	raw, err := os.ReadFile(path)
	must(err)

	loaded := &litekv.KeyValueStore{Data: raw}

	// Recover rebuilds the index and drops a tail that a crash left half
	// written, reporting how many bytes went.
	discarded, err := loaded.Recover()
	must(err)
	fmt.Printf("loaded %d bytes, discarded %d\n", len(raw), discarded)

	value, err = loaded.Read([]byte("foo"))
	must(err)
	fmt.Println("foo =", string(value))

	// -------------------------------------------------------------- a log
	// Or hand the store somewhere to mirror its writes to. Anything with
	// WriteAt, Truncate and Sync will do, which an *os.File already has.
	fmt.Println("\n== mirrored to a log ==")

	file, err := os.Create(filepath.Join(dir, "mirrored.kv"))
	must(err)

	mirrored := &litekv.KeyValueStore{}
	must(mirrored.Attach(file, litekv.Options{Sync: litekv.SyncNever}))

	must(mirrored.Write([]byte("mirrored"), []byte("as it is written")))
	must(mirrored.Sync())

	info, err := file.Stat()
	must(err)
	fmt.Printf("the log holds %d bytes\n", info.Size())

	// Detach leaves an ordinary in-memory store behind.
	must(mirrored.Detach())
	must(file.Close())

	// ------------------------------------------------------------- a file
	// Open does all of that for you, and recovers on the way in.
	fmt.Println("\n== kept in a file ==")

	path = filepath.Join(dir, "store.kv")

	// SyncEvery trades a bounded window of writes for not waiting on the disk
	// every time. The default, SyncAlways, waits; SyncNever never does.
	store, err := litekv.Open(path, litekv.Options{Sync: litekv.SyncEvery, Interval: time.Second})
	must(err)

	must(store.Write([]byte("persisted"), []byte("across restarts")))

	// Close syncs whatever the timer has not. A process that dies without it
	// loses nothing either: a record is with the operating system as soon as
	// Write returns, and only losing power can take one back.
	must(store.Close())

	store, err = litekv.Open(path, litekv.Options{})
	must(err)

	value, err = store.Read([]byte("persisted"))
	must(err)
	fmt.Println("after reopening, persisted =", string(value))
	must(store.Close())

	// ---------------------------------------------------------- segments
	// A DB spreads the store over several logs. One takes the writes; the rest
	// are frozen, keep only their keys in memory, and are merged in the
	// background instead of the store being compacted all at once. Each frozen
	// log gets its index written beside it, so opening does not have to read
	// the records again to find out where the keys are.
	fmt.Println("\n== split across segments ==")

	// Tiny segments, so a handful of records is enough to see them rotate. A
	// real one would be megabytes.
	db, err := litekv.OpenDB(filepath.Join(dir, "data"), litekv.DBOptions{
		Sync:         litekv.SyncNever,
		SegmentSize:  256,
		MergeTrigger: 1 << 30, // merge only when asked, so it can be shown
	})
	must(err)

	// The same few keys over and over: most of these records are dead the
	// moment the next one lands.
	for round := 0; round < 20; round++ {
		for _, key := range []string{"alpha", "beta", "gamma"} {
			must(db.Write([]byte(key), []byte(fmt.Sprintf("%s-%02d", key, round))))
		}
	}
	must(db.Delete([]byte("gamma")))

	fmt.Printf("%d keys over %d segments\n", db.Len(), db.Segments())

	// Merging keeps the newest record for each live key and drops the rest.
	// Reads and writes carry on while it runs; here it is asked for directly so
	// that the effect is visible.
	must(db.Merge())
	fmt.Printf("after merging: %d segments\n", db.Segments())

	value, err = db.Read([]byte("alpha"))
	must(err)
	fmt.Println("alpha =", string(value))

	// A deleted key reads as deleted until a merge drops the tombstone, and as
	// missing afterwards. Both mean there is no value.
	_, err = db.Read([]byte("gamma"))
	if errors.Is(err, litekv.ErrorKeyNotFound) || errors.Is(err, litekv.ErrorKeyDeleted) {
		fmt.Println("gamma is gone")
	}

	must(db.ForEach(func(key, value []byte) bool {
		fmt.Printf("  %s = %s\n", key, value)
		return true
	}))

	must(db.Close())
}
