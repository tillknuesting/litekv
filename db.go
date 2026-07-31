package litekv

import (
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

	active *segment   // takes the writes
	frozen []*segment // newest first, never written again

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

// segment is one log of the DB, with its own records and its own index.
type segment struct {
	id  uint64
	kvs *KeyValueStore
}

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

	// The newest log is the one to carry on writing to; the rest are frozen.
	for i, id := range ids {
		policy := DBOptions{Sync: SyncNever}
		if i == len(ids)-1 {
			policy = opts
		}

		kvs, err := Open(db.path(id), Options{Sync: policy.Sync, Interval: policy.Interval})
		if err != nil {
			db.closeSegments()
			return nil, err
		}

		seg := &segment{id: id, kvs: kvs}
		if i == len(ids)-1 {
			db.active = seg
		} else {
			db.frozen = append([]*segment{seg}, db.frozen...) // newest first
		}
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

	db.active = &segment{id: id, kvs: kvs}
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
// Reads keep working on a closed DB, as they do on a closed store: closing
// releases the files, and the records are already in memory.
func (db *DB) Read(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, seg := range db.searchOrder() {
		value, err := seg.kvs.Read(key)
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

	for _, seg := range db.searchOrder() {
		err := seg.kvs.View(key, fn)
		if !errors.Is(err, ErrorKeyNotFound) {
			return err
		}
	}

	return ErrorKeyNotFound
}

// searchOrder is the logs newest first. Callers must hold db.mu.
func (db *DB) searchOrder() []*segment {
	order := make([]*segment, 0, len(db.frozen)+1)
	if db.active != nil {
		order = append(order, db.active)
	}
	return append(order, db.frozen...)
}

// ForEach calls fn with every live key and its value, skipping the records that
// newer logs have superseded and the keys that tombstones have deleted. The
// order is unspecified. The key and value are only valid until fn returns.
func (db *DB) ForEach(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	seen := make(map[string]bool)

	for _, seg := range db.searchOrder() {
		var err error
		stopped := false

		seg.kvs.RLock()
		for key, pos := range seg.kvs.Index {
			// The first log to hold a key is the one whose answer counts.
			if seen[key] {
				continue
			}
			seen[key] = true

			record, next, parseErr := parseRecordAt(seg.kvs.Data, pos)
			if parseErr != nil {
				err = parseErr
				break
			}
			if record.Crc != checksumSerialized(seg.kvs.Data[pos:next]) {
				err = fmt.Errorf("record at offset %d: %w", pos, ErrorChecksumMismatch)
				break
			}
			if record.Type != RecordTypeNormal {
				continue
			}
			if !fn(record.Key, record.Value) {
				stopped = true
				break
			}
		}
		seg.kvs.RUnlock()

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
		seg.kvs.RLock()
		for key := range seg.kvs.Index {
			seen[key] = true
		}
		seg.kvs.RUnlock()
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
func (db *DB) rotateIfFull(written *segment) error {
	if written.kvs.Size() < db.opts.segmentSize() {
		return nil
	}

	db.mu.Lock()

	// Another writer may have rotated already.
	if db.closed || db.active != written {
		db.mu.Unlock()
		return nil
	}

	db.frozen = append([]*segment{written}, db.frozen...)
	if err := db.start(db.nextID); err != nil {
		db.mu.Unlock()
		return err
	}

	db.mergeInBackground()
	db.mu.Unlock()

	// Freezing a log does not sync it. The sync policy says when a record
	// reaches the disk and rotation is no reason to override it: under
	// SyncAlways it is already there, under SyncEvery the timer will see to it,
	// and under SyncNever the caller asked for no syncs at all.
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
	victims := append([]*segment(nil), db.frozen...) // newest first
	closed := db.closed
	db.mu.RUnlock()

	if closed || len(victims) < 2 {
		return nil
	}

	oldest := victims[len(victims)-1]

	data, err := mergedRecords(victims)
	if err != nil {
		return err
	}

	temp := db.path(oldest.id) + mergeSuffix
	if err := writeFileSynced(temp, data); err != nil {
		return err
	}

	if err := os.Rename(temp, db.path(oldest.id)); err != nil {
		os.Remove(temp)
		return err
	}
	syncDir(db.dir)

	// Reading it back checks every checksum of what was just written.
	merged, err := Open(db.path(oldest.id), Options{Sync: SyncNever})
	if err != nil {
		return err
	}

	db.mu.Lock()
	replaced := make(map[uint64]bool, len(victims))
	for _, seg := range victims {
		replaced[seg.id] = true
	}

	// Anything frozen while the merge ran is newer than the merged log and
	// stays where it is.
	var kept []*segment
	for _, seg := range db.frozen {
		if !replaced[seg.id] {
			kept = append(kept, seg)
		}
	}
	db.frozen = append(kept, &segment{id: oldest.id, kvs: merged})
	db.mu.Unlock()

	// Oldest first, so that what is left on disk is always answerable. None of
	// these is worth syncing: the oldest has already been renamed over and the
	// rest are about to be removed.
	for i := len(victims) - 1; i >= 0; i-- {
		victims[i].kvs.closeNoSync()
		if victims[i].id != oldest.id {
			os.Remove(db.path(victims[i].id))
		}
	}
	syncDir(db.dir)

	return nil
}

// mergedRecords returns the records to keep from the given logs, which are
// newest first: the newest version of every key that is not deleted, in the
// order the surviving records were originally written.
func mergedRecords(victims []*segment) ([]byte, error) {
	type locator struct {
		seg *segment
		pos int64
	}

	// Newest first, so the first log to hold a key is the one that decides.
	live := make(map[string]locator)
	for _, seg := range victims {
		seg.kvs.RLock()
		for key, pos := range seg.kvs.Index {
			if _, ok := live[key]; !ok {
				live[key] = locator{seg: seg, pos: pos}
			}
		}
		seg.kvs.RUnlock()
	}

	var size int64
	for _, seg := range victims {
		size += seg.kvs.Size()
	}
	data := make([]byte, 0, size)

	// Oldest first, so the merged log keeps the order the records were written
	// in and merging the same logs twice produces the same bytes.
	var err error
	for i := len(victims) - 1; i >= 0; i-- {
		seg := victims[i]

		seg.kvs.RLock()
		scanErr := seg.kvs.scan(func(pos, next int64, r Record) bool {
			// Deleted keys are dropped outright: every log that could hold the
			// value the tombstone hides is part of this merge.
			if r.Type != RecordTypeNormal {
				return true
			}
			if loc, ok := live[string(r.Key)]; ok && loc.seg == seg && loc.pos == pos {
				data = append(data, seg.kvs.Data[pos:next]...)
			}
			return true
		})
		seg.kvs.RUnlock()

		if scanErr != nil {
			err = scanErr
			break
		}
	}

	return data, err
}

func writeFileSynced(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		file.Close()
		os.Remove(path)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return err
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

	var err error
	for _, seg := range db.searchOrder() {
		if serr := seg.kvs.Sync(); err == nil {
			err = serr
		}
	}
	return err
}

// Close waits for a running merge and closes every log. A closed DB refuses
// writes, and goes on serving reads from memory.
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
		err = db.active.kvs.Close()
	}
	for _, seg := range db.frozen {
		if cerr := seg.kvs.Close(); err == nil {
			err = cerr
		}
	}

	return err
}
