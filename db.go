package litekv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DB is a store split across several logs instead of one.
//
// A single log has to be compacted whole: the store stops, every live record is
// copied, and until it finishes there are two copies of the data. A DB writes to
// one active log and closes it once it reaches a size, after which it is never
// written again. Those frozen logs are merged in the background, and because the
// merge builds a new file rather than editing the old ones, reads and writes
// carry on against the old ones the whole time and only the swap at the end
// needs the store to itself.
//
// Each log keeps its own index, so a lookup asks the active log first and then
// the frozen ones from newest to oldest, stopping at the first answer. That is
// what makes a record in a newer log shadow an older one, and a tombstone in a
// newer log shadow a value in an older one. Merging keeps the number of logs
// small, so a lookup does not have many to ask.
//
// KeyValueStore remains the thing to use for one log, in memory or in a file.
type DB struct {
	// mu guards the segment list. Reads and writes take it for reading and
	// work through the segments, which have their own locks; only rotating and
	// swapping in a merge take it for writing, and both are brief.
	mu sync.RWMutex

	dir  string
	opts DBOptions

	active *memSegment    // takes the writes, and is the only log held in memory
	frozen []*diskSegment // newest first, never written again, records on the disk

	nextID  uint64
	merging bool
	closed  bool

	// mergeMu lets only one merge run at a time. Two at once would build the
	// same file under the same temporary name and rename it out from under
	// each other. It is taken before db.mu, never the other way round.
	mergeMu sync.Mutex
	merges  sync.WaitGroup
}

// DBOptions configures a DB. The zero value syncs every write, rotates at 4 MiB
// and merges once four logs have piled up.
type DBOptions struct {
	// Sync is the policy for the active log, as for a single store.
	Sync SyncPolicy

	// Interval is the sync period under SyncEvery. Zero means one second.
	Interval time.Duration

	// SegmentSize is the size at which the active log is frozen and a new one
	// started. Zero means 4 MiB.
	SegmentSize int64

	// MergeTrigger is how many frozen logs may pile up before they are merged
	// in the background. Zero means four, and one disables merging.
	MergeTrigger int
}

const (
	defaultSegmentSize  = 4 << 20
	defaultMergeTrigger = 4

	segmentSuffix = ".seg"
	mergeSuffix   = ".merging"
)

func (o DBOptions) segmentSize() int64 {
	if o.SegmentSize <= 0 {
		return defaultSegmentSize
	}
	return o.SegmentSize
}

func (o DBOptions) mergeTrigger() int {
	if o.MergeTrigger <= 0 {
		return defaultMergeTrigger
	}
	return o.MergeTrigger
}

// OpenDB opens the DB in dir, creating the directory if it does not exist.
// Close it when finished.
//
// Every log in the directory is read and indexed. A crash can leave the active
// one with a record half written, which is recovered exactly as for a single
// store, and can leave a half built merge behind, which is discarded.
func OpenDB(dir string, opts DBOptions) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	ids, err := segmentIDs(dir)
	if err != nil {
		return nil, err
	}

	db := &DB{dir: dir, opts: opts}

	// An interrupted merge leaves its half built file behind. The logs it was
	// merging are all still there, so it can simply go.
	if err := db.removeStaleMerges(); err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		if err := db.start(1); err != nil {
			return nil, err
		}
		return db, nil
	}

	// The newest log is the one to carry on writing to, and is read into
	// memory. The rest are indexed where they lie: their records stay on the
	// disk and are read back a key at a time.
	for i, id := range ids {
		if i == len(ids)-1 {
			if err := db.start(id); err != nil {
				db.closeSegments()
				return nil, err
			}
			continue
		}

		frozen, err := openDiskSegment(id, db.path(id))
		if err != nil {
			db.closeSegments()
			return nil, err
		}
		db.frozen = append([]*diskSegment{frozen}, db.frozen...) // newest first
	}

	db.nextID = ids[len(ids)-1] + 1
	return db, nil
}

// path is where the log with this id lives.
func (db *DB) path(id uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%010d%s", id, segmentSuffix))
}

// start opens a fresh active log with the given id.
func (db *DB) start(id uint64) error {
	kvs, err := Open(db.path(id), Options{Sync: db.opts.Sync, Interval: db.opts.Interval})
	if err != nil {
		return err
	}

	db.active = &memSegment{segID: id, kvs: kvs}
	db.nextID = id + 1
	return nil
}

// segmentIDs returns the ids of the logs in dir, oldest first.
func segmentIDs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ids []uint64
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)
		if err != nil {
			continue // not ours
		}
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (db *DB) removeStaleMerges() error {
	matches, err := filepath.Glob(filepath.Join(db.dir, "*"+mergeSuffix))
	if err != nil {
		return err
	}
	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			return err
		}
	}
	return nil
}

// Write stores the key and value in the active log, freezing it and starting a
// new one once it has grown past the segment size.
func (db *DB) Write(key, value []byte) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrorClosed
	}

	active := db.active
	err := active.kvs.Write(key, value)
	db.mu.RUnlock()

	if err != nil {
		return err
	}
	return db.rotateIfFull(active)
}

// Delete marks the key deleted, which is a record in the active log like any
// other. It shadows whatever the older logs hold until a merge drops both.
func (db *DB) Delete(key []byte) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrorClosed
	}

	active := db.active
	err := active.kvs.Delete(key)
	db.mu.RUnlock()

	if err != nil {
		return err
	}
	return db.rotateIfFull(active)
}

// Read returns a copy of the value stored under key, asking the active log
// first and then the frozen ones from newest to oldest.
//
// A key that was deleted reports ErrorKeyDeleted until a merge drops the
// tombstone, and ErrorKeyNotFound afterwards. Both mean the same thing to a
// caller: there is no value.
//
// A closed DB reports ErrorClosed. Unlike a closed KeyValueStore, which can go
// on answering from memory, a DB keeps the values of its frozen logs on the
// disk, and closing shuts the files they are in.
func (db *DB) Read(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrorClosed
	}

	for _, seg := range db.searchOrder() {
		value, err := seg.read(key)
		// Anything but "this log has never heard of the key" is the answer,
		// including a tombstone: a newer log's delete shadows an older value.
		if !errors.Is(err, ErrorKeyNotFound) {
			return value, err
		}
	}

	return nil, ErrorKeyNotFound
}

// View calls fn with the stored bytes rather than a copy of them, under the
// same terms as KeyValueStore.View: valid until fn returns, and fn must not
// call back into the store.
func (db *DB) View(key []byte, fn func(value []byte) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrorClosed
	}

	for _, seg := range db.searchOrder() {
		err := seg.view(key, fn)
		if !errors.Is(err, ErrorKeyNotFound) {
			return err
		}
	}

	return ErrorKeyNotFound
}

// searchOrder is the logs newest first. Callers must hold db.mu.
func (db *DB) searchOrder() []readable {
	order := make([]readable, 0, len(db.frozen)+1)
	if db.active != nil {
		order = append(order, db.active)
	}
	for _, seg := range db.frozen {
		order = append(order, seg)
	}
	return order
}

// ForEach calls fn with every live key and its value, skipping the records that
// newer logs have superseded and the keys that tombstones have deleted. The
// order is unspecified. The key and value are only valid until fn returns.
func (db *DB) ForEach(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrorClosed
	}

	seen := make(map[string]bool)

	var err error
	for _, seg := range db.searchOrder() {
		stopped := false

		seg.eachKey(func(key string, pos int64) bool {
			// The first log to hold a key is the one whose answer counts.
			if seen[key] {
				return true
			}
			seen[key] = true

			record, raw, readErr := seg.recordAt(pos)
			if readErr != nil {
				err = readErr
				return false
			}
			if record.Crc != checksumSerialized(raw) {
				err = fmt.Errorf("record at offset %d: %w", pos, ErrorChecksumMismatch)
				return false
			}
			if record.Type != RecordTypeNormal {
				return true
			}
			if !fn(record.Key, record.Value) {
				stopped = true
				return false
			}
			return true
		})

		if err != nil || stopped {
			return err
		}
	}

	return nil
}

// Len returns the number of keys the DB holds, tombstones included, which is an
// upper bound on the live keys.
func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	seen := make(map[string]bool)
	for _, seg := range db.searchOrder() {
		seg.eachKey(func(key string, _ int64) bool {
			seen[key] = true
			return true
		})
	}

	return len(seen)
}

// Segments returns how many logs the DB is spread over, which is one plus the
// number waiting to be merged.
func (db *DB) Segments() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return len(db.frozen) + 1
}

// rotateIfFull freezes the active log if it has grown past the segment size and
// is still the one that was just written to.
func (db *DB) rotateIfFull(written *memSegment) error {
	if written.size() < db.opts.segmentSize() {
		return nil
	}

	db.mu.Lock()

	// Another writer may have rotated already.
	if db.closed || db.active != written {
		db.mu.Unlock()
		return nil
	}

	// Freezing hands the records over to the disk: the store and the Data
	// slice it was holding go, and what stays in memory is the index.
	frozen, err := freeze(written, db.opts.Sync)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	db.frozen = append([]*diskSegment{frozen}, db.frozen...)
	if err := db.start(db.nextID); err != nil {
		db.mu.Unlock()
		return err
	}

	db.mergeInBackground()
	db.mu.Unlock()

	return nil
}

// mergeInBackground starts a merge if enough logs have piled up and one is not
// already running. Callers must hold db.mu for writing.
func (db *DB) mergeInBackground() {
	if db.merging || db.closed || len(db.frozen) < db.opts.mergeTrigger() {
		return
	}

	db.merging = true
	db.merges.Add(1)

	go func() {
		defer db.merges.Done()
		db.merge()

		db.mu.Lock()
		db.merging = false
		db.mu.Unlock()
	}()
}

// Merge merges the frozen logs now, rather than waiting for enough of them to
// pile up, and returns when it is done. Reads and writes carry on throughout.
func (db *DB) Merge() error {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()

	if closed {
		return ErrorClosed
	}
	return db.merge()
}

// merge combines every frozen log into one.
//
// The merged log is written beside the others and then renamed over the oldest
// of them, and only then are the rest removed, oldest first. That order is what
// makes an interrupted merge harmless. At every point the logs still present
// are the merged one plus the newest few of the ones it replaced, and since
// those are newer they are asked first: a key they hold answers from them, and
// a key they do not falls through to the merged log, which holds the newest
// version of everything older. A tombstone is only dropped once every log that
// could still hold the value it hides has gone.
func (db *DB) merge() error {
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	db.mu.RLock()
	victims := append([]*diskSegment(nil), db.frozen...) // newest first
	closed := db.closed
	db.mu.RUnlock()

	if closed || len(victims) < 2 {
		return nil
	}

	oldest := victims[len(victims)-1]

	temp := db.path(oldest.id()) + mergeSuffix
	if err := mergeInto(temp, victims); err != nil {
		return err
	}

	if err := os.Rename(temp, db.path(oldest.id())); err != nil {
		os.Remove(temp)
		return err
	}
	syncDir(db.dir)

	// Indexing it back checks every checksum of what was just written, and
	// leaves the records where they are: on the disk.
	merged, err := openDiskSegment(oldest.id(), db.path(oldest.id()))
	if err != nil {
		return err
	}

	db.mu.Lock()
	replaced := make(map[uint64]bool, len(victims))
	for _, seg := range victims {
		replaced[seg.id()] = true
	}

	// Anything frozen while the merge ran is newer than the merged log and
	// stays where it is.
	var kept []*diskSegment
	for _, seg := range db.frozen {
		if !replaced[seg.id()] {
			kept = append(kept, seg)
		}
	}
	db.frozen = append(kept, merged)
	db.mu.Unlock()

	// Oldest first, so that what is left on disk is always answerable. None of
	// these is worth syncing: the oldest has already been renamed over and the
	// rest are about to be removed.
	for i := len(victims) - 1; i >= 0; i-- {
		victims[i].closeNoSync()
		if victims[i].id() != oldest.id() {
			os.Remove(db.path(victims[i].id()))
		}
	}
	syncDir(db.dir)

	return nil
}

// mergeInto writes the records worth keeping from the given logs, which are
// newest first, into a new file at path: the newest version of every key that
// is not deleted, in the order the surviving records were written.
//
// The records are streamed through a buffer rather than gathered up, so merging
// a store costs no more memory than merging a small one.
func mergeInto(path string, victims []*diskSegment) error {
	type locator struct {
		seg *diskSegment
		pos int64
	}

	// Newest first, so the first log to hold a key is the one that decides.
	live := make(map[string]locator)
	for _, seg := range victims {
		seg.eachKey(func(key string, pos int64) bool {
			if _, ok := live[key]; !ok {
				live[key] = locator{seg: seg, pos: pos}
			}
			return true
		})
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	failed := func(err error) error {
		file.Close()
		os.Remove(path)
		return err
	}

	writer := bufio.NewWriterSize(file, 64<<10)

	// Oldest first, so the merged log keeps the order the records were written
	// in and merging the same logs twice produces the same bytes.
	for i := len(victims) - 1; i >= 0; i-- {
		seg := victims[i]

		var writeErr error
		scanErr := seg.scan(func(pos int64, raw []byte, r Record) bool {
			// Deleted keys are dropped outright: every log that could hold the
			// value a tombstone hides is part of this merge.
			if r.Type != RecordTypeNormal {
				return true
			}
			if loc, ok := live[string(r.Key)]; !ok || loc.seg != seg || loc.pos != pos {
				return true
			}
			if _, err := writer.Write(raw); err != nil {
				writeErr = err
				return false
			}
			return true
		})

		if scanErr != nil {
			return failed(scanErr)
		}
		if writeErr != nil {
			return failed(writeErr)
		}
	}

	if err := writer.Flush(); err != nil {
		return failed(err)
	}
	if err := file.Sync(); err != nil {
		return failed(err)
	}
	return file.Close()
}

// syncDir makes a rename in dir durable. Not every filesystem supports it, so a
// failure is not fatal.
func syncDir(dir string) {
	if handle, err := os.Open(dir); err == nil {
		handle.Sync()
		handle.Close()
	}
}

// Sync syncs every log that could still be holding unsynced records: the active
// one, and any frozen one whose timer has not got to it yet.
func (db *DB) Sync() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil
	}

	// Only the active log can be holding anything unsynced; a frozen one is
	// never written.
	if db.active == nil {
		return nil
	}
	return db.active.sync()
}

// Close waits for a running merge and closes every log. A closed DB refuses
// everything: the values of its frozen logs are on the disk, and their files
// are shut. Len and Segments still report what it was holding.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	db.merges.Wait()

	// A merge started by hand is not in that WaitGroup, so wait for it too.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeSegments()
}

// closeSegments closes every log, returning the first error. Callers must hold
// db.mu for writing.
func (db *DB) closeSegments() error {
	var err error

	if db.active != nil {
		err = db.active.close()
	}
	for _, seg := range db.frozen {
		if cerr := seg.close(); err == nil {
			err = cerr
		}
	}

	return err
}
