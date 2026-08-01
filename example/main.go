// Command example walks through litekv from one end to the other: a store in
// memory, what it can tell you about itself, the same store saved and loaded by
// hand, one that mirrors its writes somewhere else, one that keeps a file, and
// one split across segments for more than fits in memory.
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

	// Every record carries when it was written.
	written, err := kvs.Modified([]byte("foo"))
	must(err)
	fmt.Println("foo was written", time.Since(written).Round(time.Millisecond), "ago")

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

	// ------------------------------------------------------------- checking
	// What a store can be asked about itself.
	fmt.Println("\n== checking and rebuilding ==")

	// Verify reads every record and checks it against its checksum.
	must(kvs.Verify())
	fmt.Printf("%d bytes, all records intact\n", kvs.Size())

	// The index can be saved and put back, which is cheaper than working it
	// out from the records again. Restore the data before the index for it.
	saved, err := kvs.SaveIndex()
	must(err)
	must(kvs.LoadIndex(saved))
	fmt.Printf("index saved in %d bytes\n", len(saved))

	// Or work it out from the records, if it was lost. A *litekv.CorruptAtError
	// here means the data has a damaged tail; the index still covers
	// everything before that offset.
	must(kvs.RebuildIndex())

	// --------------------------------------------------------------- by hand
	// The Data slice is the whole store, so persistence can be entirely your
	// own business.
	fmt.Println("\n== saved and loaded by hand ==")

	path := filepath.Join(dir, "by-hand.kv")
	must(os.WriteFile(path, kvs.Data, 0o644))

	raw, err := os.ReadFile(path)
	must(err)

	loaded := &litekv.KeyValueStore{Data: raw}

	// Recover rebuilds the index, checks the records as it goes, and drops a
	// tail that a crash left half written, saying how many bytes went.
	discarded, err := loaded.Recover()
	must(err)
	fmt.Printf("loaded %d bytes, discarded %d\n", len(raw), discarded)

	value, err = loaded.Read([]byte("foo"))
	must(err)
	fmt.Println("foo =", string(value))

	// ----------------------------------------------------------------- a log
	// Or hand the store somewhere to mirror its writes to. Anything with
	// WriteAt, Truncate and Sync will do, which an *os.File already has.
	fmt.Println("\n== mirrored to a log ==")

	file, err := os.Create(filepath.Join(dir, "mirrored.kv"))
	must(err)

	mirrored := &litekv.KeyValueStore{}
	must(mirrored.Write([]byte("written"), []byte("before attaching")))

	must(mirrored.Attach(file, litekv.Options{Sync: litekv.SyncNever}))

	// Attaching assumes the log already holds what the store does. It does not
	// here, so put the records into it.
	must(mirrored.Rewrite())

	must(mirrored.Write([]byte("written"), []byte("after attaching")))
	must(mirrored.Sync())

	info, err := file.Stat()
	must(err)
	fmt.Printf("the log holds %d bytes\n", info.Size())

	// Detach leaves an ordinary in-memory store behind.
	must(mirrored.Detach())
	must(file.Close())

	// ---------------------------------------------------------------- a file
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

	// -------------------------------------------------------------- segments
	// A DB spreads the store over several logs, for more than fits in memory.
	// One takes the writes; the rest are frozen, keep only their keys in
	// memory with their records left on the disk, and have their index written
	// beside them so opening does not have to read them again.
	fmt.Println("\n== split across segments ==")

	// Tiny segments, so a handful of records is enough to see them rotate. A
	// real one would be megabytes. Merging is size tiered: MergeTrigger counts
	// logs of a size rather than logs in all, and 1 turns it off, which is
	// what an append-only workload of write-once keys wants.
	db, err := litekv.OpenDB(filepath.Join(dir, "data"), litekv.DBOptions{
		Sync:         litekv.SyncNever,
		SegmentSize:  256,
		MergeTrigger: 1, // off, so the merge below is the only one
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

	// Merge compacts the whole store. The background one merges logs of a size
	// as they collect, and reads and writes carry on while either runs.
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

	// Sync reaches every log, and reports a rotation that could not be
	// finished, which Write does not: a record is stored either way.
	must(db.Sync())
	must(db.Close())
}
