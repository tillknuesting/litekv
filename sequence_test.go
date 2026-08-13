package litekv

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// A number on every record is what makes two positions in a DB's stream
// comparable. Offsets are only comparable inside one log, and a DB has many, so
// the end of one and the start of the next have nothing in common to say they
// are the same place. The number says it.
//
// It has to be in the record rather than counted, and that is the part worth
// holding onto: merging drops records, so two stores holding the same stream
// would count different numbers for the same record and answer the same
// question about a position differently. See TestMergeKeepsTheHighestNumber.

// numbersIn returns the number on every record in the store, oldest log first
// and in log order within each, which is the order they were written in.
func numbersIn(t *testing.T, db *DB) []uint64 {
	t.Helper()

	var seqs []uint64

	db.mu.RLock()
	frozen := append([]*diskSegment(nil), db.frozen...)
	active := db.active
	db.mu.RUnlock()

	for i := len(frozen) - 1; i >= 0; i-- { // db.frozen is newest first
		if err := frozen[i].scan(func(pos int64, raw []byte, r Record) bool {
			seqs = append(seqs, r.Seq)
			return true
		}); err != nil {
			t.Fatalf("scanning log %d: %v", frozen[i].id(), err)
		}
	}

	active.kvs.RLock()
	err := active.kvs.scan(func(pos, next int64, r Record) bool {
		seqs = append(seqs, r.Seq)
		return true
	})
	active.kvs.RUnlock()
	if err != nil {
		t.Fatalf("scanning the active log: %v", err)
	}

	return seqs
}

// rising checks that the numbers are 1, 2, 3 and so on: every record numbered,
// in the order the records are in, with nothing repeated and nothing skipped.
func rising(t *testing.T, seqs []uint64, what string) {
	t.Helper()

	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("%s: record %d carries number %d, want %d (%v)", what, i, seq, i+1, seqs)
		}
	}
}

func TestRecordCarriesItsNumber(t *testing.T) {
	for _, test := range []struct {
		name    string
		record  Record
		version uint8
		header  int64
	}{
		{"neither", Record{}, recordV1, headerSizeV1},
		{"an expiry", Record{Expires: 1 << 40}, recordV2, headerSizeV2},
		{"a number", Record{Seq: 42}, recordV3, headerSizeV3},
		{"both", Record{Expires: 1 << 40, Seq: 42}, recordV4, headerSizeV4},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := test.record
			record.Type = RecordTypeNormal
			record.Timestamp = 1234
			record.Key = []byte("key")
			record.Value = []byte("value")
			record.KeyLength = 3
			record.ValueLength = 5
			record.Version = record.version()
			record.Crc = record.calculateChecksum()

			if record.Version != test.version {
				t.Errorf("written as version %d, want %d", record.Version, test.version)
			}

			raw := record.appendTo(nil)
			if int64(len(raw)) != test.header+8 {
				t.Errorf("took %d bytes, want %d", len(raw), test.header+8)
			}

			// The checksum folded from the fields and the one taken over the
			// bytes have to agree, or half the package disagrees with the other
			// half about every record.
			if got := checksumSerialized(raw); got != record.Crc {
				t.Errorf("the serialized record checksums to %d, the fields to %d", got, record.Crc)
			}

			back, next, err := parseRecordAt(raw, 0)
			if err != nil {
				t.Fatalf("parsing back: %v", err)
			}
			if next != int64(len(raw)) {
				t.Errorf("the record ends at %d of %d bytes", next, len(raw))
			}
			if back.Seq != test.record.Seq {
				t.Errorf("came back numbered %d, want %d", back.Seq, test.record.Seq)
			}
			if back.Expires != test.record.Expires {
				t.Errorf("came back expiring at %d, want %d", back.Expires, test.record.Expires)
			}
			if back.size() != int64(len(raw)) {
				t.Errorf("reports a size of %d, took %d", back.size(), len(raw))
			}
		})
	}
}

// TestKeyValueStoreIsUnnumbered holds the bargain the wider layouts make: a
// store that is not asked to number pays nothing for the fact that numbering
// exists.
func TestKeyValueStoreIsUnnumbered(t *testing.T) {
	kvs := &KeyValueStore{}

	if err := kvs.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}

	kvs.RLock()
	err := kvs.scan(func(pos, next int64, r Record) bool {
		if r.Seq != 0 {
			t.Errorf("a record at %d is numbered %d", pos, r.Seq)
		}
		if r.Version != recordV1 {
			t.Errorf("a record at %d is in layout %d, want %d", pos, r.Version, recordV1)
		}
		return true
	})
	kvs.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	// And its positions carry no number, so nothing compares them by one.
	if got := kvs.Position().Seq; got != 0 {
		t.Errorf("an unnumbered store reports position number %d, want 0", got)
	}
}

func TestDBNumbersEveryRecord(t *testing.T) {
	db, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := range 30 {
		if err := db.Write(fmt.Appendf(nil, "key-%02d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	// A delete is a record like any other and takes a number like one.
	if err := db.Delete([]byte("key-00")); err != nil {
		t.Fatal(err)
	}
	// So is a record with an expiry, which is the layout carrying both.
	if err := db.WriteExpiring([]byte("late"), []byte("value"), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	seqs := numbersIn(t, db)
	if len(seqs) != 32 {
		t.Fatalf("the store holds %d records, want 32", len(seqs))
	}
	rising(t, seqs, "a DB's logs")

	if db.Segments() < 2 {
		t.Fatal("the store never rotated; the numbers were not asked to cross a log")
	}
}

// TestNumbersFollowTheRecordOrder is why the number is handed out under the
// write lock rather than by an atomic counter taken before it. Two writers
// taking numbers first and appending afterwards would put them in the log in
// the other order, and a position naming the last record would then name a
// number with a bigger one behind it.
func TestNumbersFollowTheRecordOrder(t *testing.T) {
	db, err := OpenDB(t.TempDir(), DBOptions{Sync: SyncNever, SegmentSize: 1 << 20, MergeTrigger: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var writers sync.WaitGroup
	for w := range 8 {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := range 50 {
				if err := db.Write(fmt.Appendf(nil, "key-%d-%02d", w, i), []byte("value")); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(w)
	}
	writers.Wait()

	rising(t, numbersIn(t, db), "eight writers at once")
}

// TestNumbersSurviveARestart is the case the hint file carries a number for. A
// store that rotated and then stopped comes back with an empty log, and there is
// no record in it to carry on from — so the number has to come from the log
// before it, whether or not that log is being read at all.
func TestNumbersSurviveARestart(t *testing.T) {
	for _, hints := range []bool{true, false} {
		name := "with hints"
		if !hints {
			name = "without hints"
		}

		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			db, err := OpenDB(dir, smallSegments(200))
			if err != nil {
				t.Fatal(err)
			}

			for i := range 20 {
				if err := db.Write(fmt.Appendf(nil, "key-%02d", i), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}

			// Rotate, so the store comes back to an empty log with nothing in it
			// to say what has already been handed out.
			db.mu.Lock()
			err = db.rotateLocked()
			db.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if !hints {
				// Without them the logs are read the long way, which has to
				// arrive at the same number.
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if len(entry.Name()) > 5 && entry.Name()[len(entry.Name())-5:] == hintSuffix {
						if err := os.Remove(dir + "/" + entry.Name()); err != nil {
							t.Fatal(err)
						}
					}
				}
			}

			reopened, err := OpenDB(dir, smallSegments(200))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()

			if got := reopened.Position().Log.Seq; got != 21 {
				t.Errorf("a reopened store would number its next record %d, want 21", got)
			}

			for i := 20; i < 25; i++ {
				if err := reopened.Write(fmt.Appendf(nil, "key-%02d", i), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}

			rising(t, numbersIn(t, reopened), "written either side of a restart")
		})
	}
}

// TestMergeKeepsTheHighestNumber is the reason a merged log records the highest
// number of its inputs rather than of what it kept. A merge drops records, and
// the one it drops may be the newest of the lot: reading the merged file back
// would then say a number below one already handed out, and the next write would
// reuse it — two records in the same place in the stream, and a position naming
// one of them naming both.
func TestMergeKeepsTheHighestNumber(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}

	for i := range 20 {
		if err := db.Write(fmt.Appendf(nil, "key-%02d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	// The newest record in the store is a tombstone, and merging everything is
	// entitled to drop it: the run reaches the oldest log, so there is nothing
	// older for it to hide.
	if err := db.Delete([]byte("key-00")); err != nil {
		t.Fatal(err)
	}
	highest := db.Position().Log.Seq - 1

	db.mu.Lock()
	err = db.rotateLocked()
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Read([]byte("key-00")); err != ErrorKeyNotFound {
		t.Fatalf("the tombstone was not dropped, so this tests nothing: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDB(dir, smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Position().Log.Seq; got <= highest {
		t.Errorf("a reopened store would number its next record %d, which was already given to a record (%d)", got, highest)
	}

	if err := reopened.Write([]byte("after"), []byte("the merge")); err != nil {
		t.Fatal(err)
	}

	seqs := numbersIn(t, reopened)
	for i, seq := range seqs[:len(seqs)-1] {
		if seq >= seqs[len(seqs)-1] {
			t.Errorf("record %d carries number %d, at or above the %d given after the merge", i, seq, seqs[len(seqs)-1])
		}
	}
}

// TestFollowerKeepsTheLeadersNumbers is what makes two stores answer the same
// question about a position the same way: the numbers cross with the records
// rather than being made up at each end.
func TestFollowerKeepsTheLeadersNumbers(t *testing.T) {
	leader, err := OpenDB(t.TempDir(), smallSegments(200))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	follower, err := OpenDB(t.TempDir(), smallSegments(600))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	// Enough records that the snapshot does not arrive in one piece: a follower
	// applies what has arrived as it arrives, and this is about what each of
	// those pieces is allowed to do to the numbering.
	value := make([]byte, 64)
	for i := range 400 {
		if err := leader.Write(fmt.Appendf(nil, "key-%03d", i), value); err != nil {
			t.Fatal(err)
		}
	}

	// A snapshot ships the newest version of every key by asking the newest log
	// first, so the numbers arrive descending and the last piece to arrive holds
	// the lowest of them. A follower that took each piece at face value would
	// end up numbering from there, and reuse most of the stream.
	followDB(t, leader, follower, ReplicaOptions{})

	// The two lay their logs out differently — different sizes, and a snapshot
	// carries only the live records — so what has to match is the numbers on the
	// records they both hold, and the number their next record would take.
	if got, want := follower.Applied().Log.Seq, leader.Position().Log.Seq; got != want {
		t.Errorf("the follower is at number %d, the leader at %d", got, want)
	}

	numbers := map[string]uint64{}
	if err := leader.ForEach(func(key, value []byte) bool {
		numbers[string(key)] = 0
		return true
	}); err != nil {
		t.Fatal(err)
	}
	for _, seq := range numbersIn(t, leader) {
		if seq == 0 {
			t.Fatal("the leader wrote an unnumbered record")
		}
	}

	mirrored := numbersIn(t, follower)
	for _, seq := range mirrored {
		if seq == 0 {
			t.Fatal("the follower kept a record with no number on it")
		}
	}

	// Promoted, it carries on from the highest number it holds rather than
	// starting again, or two records would share a place in the stream.
	if _, err := follower.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := follower.Write([]byte("mine"), []byte("now")); err != nil {
		t.Fatal(err)
	}

	own := numbersIn(t, follower)
	last := own[len(own)-1]
	for i, seq := range own[:len(own)-1] {
		if seq >= last {
			t.Errorf("record %d of the promoted store carries %d, at or above the %d it wrote itself", i, seq, last)
		}
	}
	if last <= mirrored[len(mirrored)-1] {
		t.Errorf("the promoted store numbered its own write %d, at or below the %d it had applied", last, mirrored[len(mirrored)-1])
	}
}

// BenchmarkNumbering is what a record's number costs the store that writes it:
// eight bytes in the record, and a checksum taken under the write lock instead
// of before it, since the number is not known until the lock is held.
//
// The second of those is the one to watch. It is a pass over the record's bytes
// where there was none, and it happens where readers are shut out — the trade
// the "contiguous CRC" entry in AGENTS.md turned down when it was optional. It
// buys back most of what it costs by being contiguous rather than a fold of the
// fields, which is the faster of the two.
func BenchmarkNumbering(b *testing.B) {
	for _, size := range []int{16, 1024} {
		value := make([]byte, size)

		for _, numbered := range []bool{false, true} {
			name := fmt.Sprintf("plain/%d", size)
			if numbered {
				name = fmt.Sprintf("numbered/%d", size)
			}

			b.Run(name, func(b *testing.B) {
				kvs := &KeyValueStore{}
				if numbered {
					kvs.number(0)
				}

				key := make([]byte, 16)

				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					// Bounded, or this measures append and the allocator: see
					// the note in AGENTS.md about write benchmarks.
					if len(kvs.Data) > 1<<26 {
						b.StopTimer()
						kvs.Data = kvs.Data[:0]
						kvs.Index = nil
						kvs.lastRecord = 0
						b.StartTimer()
					}
					binary.LittleEndian.PutUint64(key, uint64(i))
					if err := kvs.Write(key, value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
