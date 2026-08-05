// Command example walks through litekv from one end to the other: a store in
// memory, what it can tell you about itself, the same store saved and loaded by
// hand, one that mirrors its writes somewhere else, one that keeps a file, one
// followed by a replica over a connection, one split across segments for more
// than fits in memory, and that one followed by a replica of its own.
//
// It calls every exported function in the package, so a call that stops working
// stops this too.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
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

// catchUp brings a follower up to date a batch at a time. This is the shape a
// transport that answers requests takes — the follower asks, the leader answers
// once, and it asks again — and it is what Follow does without the asking.
func catchUp(leader, follower *litekv.KeyValueStore) error {
	pos := follower.Position()

	for {
		var batch bytes.Buffer

		next, err := leader.Since(pos, &batch, litekv.ReplicaOptions{})
		if err != nil {
			return err
		}
		if next == pos {
			return nil // up to date
		}
		if pos, err = follower.Apply(pos, &batch, litekv.ReplicaOptions{}); err != nil {
			return err
		}
	}
}

// waitFor gives an asynchronous follower a moment to arrive. There is nothing
// to assert about how long that takes, which is what asynchronous means.
func waitFor(arrived func() bool) {
	deadline := time.Now().Add(10 * time.Second)
	for !arrived() {
		if time.Now().After(deadline) {
			log.Fatalln("the follower never caught up")
		}
		time.Sleep(time.Millisecond)
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

	// ------------------------------------------------------------ replicated
	// A log is an ordered, checksummed, append-only stream of records, so a
	// follower holding the first N bytes of one and given the bytes after them
	// holds the same store. Nothing in the library opens a socket: it hands
	// over a position and a run of records, and moving them is this code's job.
	fmt.Println("\n== replicated to a follower ==")

	leader := &litekv.KeyValueStore{}
	for i := 0; i < 5; i++ {
		must(leader.Write([]byte(fmt.Sprintf("key-%d", i)), []byte("value")))
	}

	replica := &litekv.KeyValueStore{}

	// A connection. net.Pipe rather than a socket so that the example needs no
	// port and no cleanup; over TCP the code either side of it is the same.
	client, server := net.Pipe()
	stop := make(chan struct{})

	var streaming sync.WaitGroup
	streaming.Add(2)

	// The leader's end: read the position the follower has reached, then send
	// records from there for as long as it stays connected. A position is
	// twenty bytes, which is the only thing the two ends have to agree on.
	go func() {
		defer streaming.Done()
		defer server.Close()

		var asked [20]byte
		if _, err := io.ReadFull(server, asked[:]); err != nil {
			return
		}

		var from litekv.Position
		if err := from.UnmarshalBinary(asked[:]); err != nil {
			return
		}

		leader.Follow(from, server, stop, litekv.ReplicaOptions{})
	}()

	// The follower's end: say where it has got to, then apply what arrives
	// until the connection ends. A record carries its own lengths, so there is
	// no framing to agree on either.
	go func() {
		defer streaming.Done()
		defer client.Close()

		from := replica.Position()

		here, err := from.MarshalBinary()
		if err != nil {
			return
		}
		if _, err := client.Write(here); err != nil {
			return
		}

		// It returns when the connection does, which is not a failure here.
		replica.Apply(from, client, litekv.ReplicaOptions{})
	}()

	waitFor(func() bool { return replica.Size() == leader.Size() })
	fmt.Printf("the follower caught up: %d bytes\n", replica.Size())

	// Changed is what tells a leader there is something to send. Follow is
	// waiting on exactly this inside the goroutine above, which is why a record
	// written now crosses without the follower asking again.
	changed := leader.Changed()
	must(leader.Write([]byte("late"), []byte("record")))
	<-changed

	waitFor(func() bool { return replica.Size() == leader.Size() })

	value, err = replica.Read([]byte("late"))
	must(err)
	fmt.Println("and kept up:", string(value))

	close(stop)
	streaming.Wait()

	// Compaction moves every record, so no follower's position survives one.
	// The leader says so rather than sending records that would splice one
	// history onto another, and the follower empties itself and starts again.
	must(leader.Write([]byte("key-0"), []byte("updated")))
	must(leader.Compact())

	_, err = leader.Since(replica.Position(), io.Discard, litekv.ReplicaOptions{})
	if errors.Is(err, litekv.ErrorDiverged) {
		fmt.Println("after the leader compacted:", err)

		must(replica.Reset())
		must(catchUp(leader, replica))
	}

	value, err = replica.Read([]byte("key-0"))
	must(err)
	fmt.Printf("the follower took the log again: key-0 = %s, %d bytes\n", value, replica.Size())

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

	// ------------------------------------------------ a DB with a follower
	// A DB cannot ship its log the way a single store does: its logs are merged
	// in the background, and a merge writes its output over the oldest log it
	// replaces, so a file keeps its name while becoming something else. What
	// crosses is records instead, and the follower lays them out however its
	// own rotations and merges decide.
	fmt.Println("\n== a DB followed by another ==")

	replicaDB, err := litekv.OpenDB(filepath.Join(dir, "replica"), litekv.DBOptions{
		Sync: litekv.SyncNever,

		// A different size from the leader's, to make the point that the two
		// agree on what they hold and on nothing about their files.
		SegmentSize: 512,
	})
	must(err)

	// A follower with no position needs a snapshot: the live records, and the
	// position they are current as of.
	var snapshot bytes.Buffer
	at, err := db.Snapshot(&snapshot, litekv.ReplicaOptions{})
	must(err)
	must(replicaDB.ApplySnapshot(at, &snapshot, litekv.ReplicaOptions{}))

	// Len counts tombstones as well as live keys, and a snapshot carries no
	// tombstones, so the follower's count is the live one.
	fmt.Printf("the follower took %d live keys over %d logs; the leader is over %d\n",
		replicaDB.Len(), replicaDB.Segments(), db.Segments())

	// And then the tail. A position is twenty-eight bytes on the wire and has to
	// travel with the records: a DB follower cannot work out where it is from
	// its own logs the way a follower of a single store can.
	//
	// Take the channel before the write, never after. Changed is closed by the
	// next change, so one taken afterwards waits for the change after that —
	// which is what Follow does for you when a leader streams.
	more := db.Changed()
	must(db.Write([]byte("delta"), []byte("written after the snapshot")))
	<-more

	pos := replicaDB.Applied()
	for {
		var batch bytes.Buffer

		next, err := db.Since(pos, &batch, litekv.ReplicaOptions{})
		must(err)
		if next == pos {
			break
		}

		encoded, err := next.MarshalBinary()
		must(err)

		var arrived litekv.DBPosition
		must(arrived.UnmarshalBinary(encoded))

		pos, err = replicaDB.Apply(pos, arrived, &batch, litekv.ReplicaOptions{})
		must(err)
	}

	value, err = replicaDB.Read([]byte("delta"))
	must(err)
	fmt.Println("the follower kept up: delta =", string(value))

	if replicaDB.Position() != db.Position() {
		fmt.Println("and the two stores are laid out entirely differently, as they should be")
	}

	// Follow is that loop left running. It hands each batch to a callback along
	// with the position it leads to — both have to reach the other end, since a
	// DB follower cannot work out where it is from its own logs. Over a
	// connection the callback is where they would be framed and written.
	stopDB := make(chan struct{})
	streamed := make(chan error, 1)

	go func() {
		_, err := db.Follow(replicaDB.Applied(), func(batch []byte, next litekv.DBPosition) error {
			_, err := replicaDB.Apply(replicaDB.Applied(), next, bytes.NewReader(batch), litekv.ReplicaOptions{})
			return err
		}, stopDB, litekv.ReplicaOptions{})

		streamed <- err
	}()

	must(db.Write([]byte("epsilon"), []byte("streamed across")))
	waitFor(func() bool {
		_, err := replicaDB.Read([]byte("epsilon"))
		return err == nil
	})
	fmt.Println("and a record written while it streamed arrived without being asked for")

	close(stopDB)

	// A real follower would answer litekv.ErrorDiverged here by taking another
	// snapshot: a merge can take the log it was resting at while it waits, and
	// nothing holds one open for it.
	if err := <-streamed; err != nil && !errors.Is(err, litekv.ErrorDiverged) {
		must(err)
	}

	// Reset empties a follower, which is what it does when a leader says there
	// is no position the two agree on.
	must(replicaDB.Reset())
	fmt.Printf("after a reset the follower holds %d keys and no position: %t\n",
		replicaDB.Len(), replicaDB.Applied() == litekv.DBPosition{})

	must(replicaDB.Close())

	// Sync reaches every log, and reports a rotation that could not be
	// finished, which Write does not: a record is stored either way.
	must(db.Sync())
	must(db.Close())
}
