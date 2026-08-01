package litekv

import (
	"bufio"
	"errors"
	"fmt"
	"iter"
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

	// rotateErr is a rotation that could not be finished. The record that
	// triggered it was stored either way, so it is not the writer's error to
	// hear; Sync and Close report it instead.
	rotateErr error

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

	// MergeTrigger is how many logs of a size may pile up before they are
	// merged into one. Zero means two, and one disables merging.
	//
	// Merging is size tiered: only logs of roughly the same size are merged
	// together, so a large one is rewritten only when enough of its own size
	// has collected beside it. The store settles at about MergeTrigger logs per
	// size, which is a handful in total, and two keeps that as low as it goes
	// at the cost of merging more often.
	MergeTrigger int
}

const (
	defaultSegmentSize  = 4 << 20
	defaultMergeTrigger = 2

	// tierRatio is how much bigger a log has to be to count as a size of its
	// own. Four means a log is only merged with others within four times its
	// size, so merging one of them does not drag the whole store through.
	tierRatio = 4

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

// sizeTier puts a log into a size class: how many times its size divides by
// tierRatio before it is down to a freshly rotated one.
func sizeTier(size, base int64) int {
	if base <= 0 {
		base = defaultSegmentSize
	}

	tier := 0
	for size >= base*tierRatio {
		size /= tierRatio
		tier++
	}
	return tier
}

// pickMerge chooses the logs to merge next: the oldest run of logs of the same
// size that has enough of them, as a half-open range over db.frozen, which runs
// newest first.
//
// The run has to be contiguous. Merging logs with others left between them
// would put records of different ages into one log, and the order they are
// asked in is the only thing that decides which version of a key wins.
//
// Tombstones can only be dropped by a merge that reaches the oldest log, since
// any log older than the run could still hold the value a tombstone hides.
// Callers must hold db.mu.
func (db *DB) pickMerge() (victims []*diskSegment, dropTombstones, ok bool) {
	trigger := db.opts.mergeTrigger()
	base := db.opts.segmentSize()

	from, to, tier := 0, 0, 0
	for i := 0; i < len(db.frozen); {
		runTier := sizeTier(db.frozen[i].bytes, base)

		j := i
		for j < len(db.frozen) && sizeTier(db.frozen[j].bytes, base) == runTier {
			j++
		}

		// The smallest size first: those merges are the cheap ones, and they
		// are what keeps the number of logs down.
		if j-i >= trigger && (to == 0 || runTier < tier) {
			from, to, tier = i, j, runTier
		}

		i = j
	}

	if to == 0 {
		return nil, false, false
	}
	return append([]*diskSegment(nil), db.frozen[from:to]...), to == len(db.frozen), true
}

// OpenDB opens the DB in dir, creating the directory if it does not exist.
// Close it when finished.
//
// Every log in the directory is read and indexed. A crash can leave the active
// one with a record half written, which is recovered exactly as for a single
// store, and can leave a half built merge behind, which is discarded.
func OpenDB(dir string, opts DBOptions) (*DB, error) {
	if err := disk.MkdirAll(dir, 0o755); err != nil {
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
	entries, err := disk.ReadDir(dir)
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

// removeStaleMerges clears what an interrupted merge left behind: its half
// built log, and any hint whose log is no longer there.
func (db *DB) removeStaleMerges() error {
	entries, err := disk.ReadDir(db.dir)
	if err != nil {
		return err
	}

	present := make(map[string]bool, len(entries))
	for _, entry := range entries {
		present[entry.Name()] = true
	}

	for _, entry := range entries {
		name := entry.Name()

		switch {
		case strings.HasSuffix(name, mergeSuffix):
			// Half of a merge, of a log or of a hint. Everything it was built
			// from is still there, so it can simply go.
		case strings.HasSuffix(name, hintSuffix):
			// A hint whose log has gone describes nothing.
			log := strings.TrimSuffix(name, hintSuffix) + segmentSuffix
			if present[log] {
				continue
			}
		default:
			continue
		}

		if err := disk.Remove(filepath.Join(db.dir, name)); err != nil {
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

	// The record is stored. Rotating is housekeeping, and a failure at it is
	// not a reason to tell the caller their write did not happen.
	db.rotateIfFull(active)
	return nil
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

	db.rotateIfFull(active)
	return nil
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

	for seg := range db.searchOrder() {
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

	for seg := range db.searchOrder() {
		err := seg.view(key, fn)
		if !errors.Is(err, ErrorKeyNotFound) {
			return err
		}
	}

	return ErrorKeyNotFound
}

// searchOrder yields the logs newest first, which is the order every lookup
// goes in and the only thing deciding which version of a key wins. Callers must
// hold db.mu for as long as they are ranging over it.
//
// It yields rather than returning a slice because building one allocated on
// every read: 160 bytes and an allocation for a call that is otherwise a map
// lookup and a copy, and for View, which exists precisely so that a read need
// not allocate, it was the only allocation left. It also made a DB read worse
// the more goroutines were reading, since what the allocator hands out on ten
// cores it eventually has to collect on all of them.
//
// Nothing is cached here on purpose. The order has to track db.active and
// db.frozen exactly, and a copy of it kept alongside them would be one more
// thing to update in the four places those change — which is the shape of the
// header-offset bug this package has already had twice.
func (db *DB) searchOrder() iter.Seq[readable] {
	return func(yield func(readable) bool) {
		if db.active != nil {
			if !yield(db.active) {
				return
			}
		}
		for _, seg := range db.frozen {
			if !yield(seg) {
				return
			}
		}
	}
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
	for seg := range db.searchOrder() {
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
	for seg := range db.searchOrder() {
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
// rotateIfFull freezes the active log if it has grown past the segment size and
// is still the one that was just written to.
//
// A failure here is remembered rather than returned. The record that prompted
// the rotation is already stored, and a store that cannot rotate goes on
// working with one log larger than it meant to have, which is worth carrying on
// with and worth reporting. Sync and Close report it.
func (db *DB) rotateIfFull(written *memSegment) {
	if written.size() < db.opts.segmentSize() {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Another writer may have rotated already.
	if db.closed || db.active != written {
		return
	}

	// Freezing hands the records over to the disk: the store and the Data
	// slice it was holding go, and what stays in memory is the index.
	frozen, err := freeze(written, db.opts.Sync)
	if err != nil {
		db.rotateErr = err
		return
	}

	db.frozen = append([]*diskSegment{frozen}, db.frozen...)
	if err := db.start(db.nextID); err != nil {
		db.rotateErr = err
		return
	}

	db.rotateErr = nil
	db.mergeInBackground()
}

// mergeInBackground starts a merge if enough logs have piled up and one is not
// already running. Callers must hold db.mu for writing.
func (db *DB) mergeInBackground() {
	if db.merging || db.closed {
		return
	}
	if _, _, ok := db.pickMerge(); !ok {
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

// Merge merges every frozen log into one now, rather than waiting for enough of
// a size to collect, and returns when it is done. Reads and writes carry on
// throughout. This is the whole store compacted: nothing superseded and no
// deleted key survives it.
func (db *DB) Merge() error {
	// The logs to merge have to be chosen with the merge lock already held, or
	// a merge running in the background could remove them in between.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	db.mu.RLock()
	closed := db.closed
	victims := append([]*diskSegment(nil), db.frozen...)
	db.mu.RUnlock()

	if closed {
		return ErrorClosed
	}
	if len(victims) < 2 {
		return nil
	}
	return db.mergeLocked(victims, true)
}

// merge is the background merge: it takes the run of same sized logs that
// pickMerge chooses, and keeps going while another run is worth merging, since
// combining one run can complete the next size up.
func (db *DB) merge() error {
	for {
		db.mergeMu.Lock()

		db.mu.RLock()
		closed := db.closed
		victims, dropTombstones, ok := db.pickMerge()
		db.mu.RUnlock()

		if closed || !ok {
			db.mergeMu.Unlock()
			return nil
		}

		err := db.mergeLocked(victims, dropTombstones)
		db.mergeMu.Unlock()

		if err != nil {
			return err
		}
	}
}

// mergeLocked combines a contiguous run of logs, newest first, into one.
// Callers must hold db.mergeMu, and must have chosen the run while holding it.
//
// The merged log is written beside the others and then renamed over the oldest
// of them, and only then are the rest removed, oldest first. That order is what
// makes an interrupted merge harmless. At every point the logs still present
// are the merged one plus the newest few of the ones it replaced, and since
// those are newer they are asked first: a key they hold answers from them, and
// a key they do not falls through to the merged log, which holds the newest
// version of everything older in the run.
func (db *DB) mergeLocked(victims []*diskSegment, dropTombstones bool) error {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()

	if closed || len(victims) < 2 {
		return nil
	}

	oldest := victims[len(victims)-1]

	temp := db.path(oldest.id()) + mergeSuffix
	index, size, err := mergeInto(temp, victims, dropTombstones)
	if err != nil {
		return err
	}

	// The hint beside the log about to be replaced describes what is there now.
	// It has to go before the rename, or a crash in between would leave it
	// beside a log it does not describe.
	if err := removeHint(db.path(oldest.id())); err != nil {
		disk.Remove(temp)
		return err
	}

	if err := disk.Rename(temp, db.path(oldest.id())); err != nil {
		disk.Remove(temp)
		return err
	}
	syncDir(db.dir)

	// The merge knows where it put every record, so there is nothing to read
	// back: the index it built is the index of the new log.
	merged, err := adoptMerged(oldest.id(), db.path(oldest.id()), index, size)
	if err != nil {
		return err
	}

	db.mu.Lock()
	replaced := make(map[uint64]bool, len(victims))
	for _, seg := range victims {
		replaced[seg.id()] = true
	}

	// Rebuilt by id rather than by position: logs frozen while the merge ran
	// were added to the front, and the merged log belongs where its oldest
	// input was. Ids only ever increase, so ordering by id is ordering by age.
	kept := []*diskSegment{merged}
	for _, seg := range db.frozen {
		if !replaced[seg.id()] {
			kept = append(kept, seg)
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].id() > kept[j].id() })
	db.frozen = kept
	db.mu.Unlock()

	// Oldest first, so that what is left on disk is always answerable. None of
	// these is worth syncing: the oldest has already been renamed over and the
	// rest are about to be removed.
	for i := len(victims) - 1; i >= 0; i-- {
		victims[i].closeNoSync()
		if victims[i].id() != oldest.id() {
			removeHint(db.path(victims[i].id()))
			disk.Remove(db.path(victims[i].id()))
		}
	}
	syncDir(db.dir)

	return nil
}

// mergeInto writes the records worth keeping from the given logs, which are
// newest first, into a new file at path: the newest version of every key, in
// the order the surviving records were written.
//
// A tombstone is only dropped when dropTombstones says every log that could
// hold the value it hides is part of this merge. Otherwise it is carried into
// the merged log, where it goes on shadowing whatever is older.
//
// The records are streamed through a buffer rather than gathered up, so merging
// a store costs no more memory than merging a small one.
func mergeInto(path string, victims []*diskSegment, dropTombstones bool) (map[string]int64, int64, error) {
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

	file, err := disk.Open(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, 0, err
	}

	failed := func(err error) (map[string]int64, int64, error) {
		file.Close()
		disk.Remove(path)
		return nil, 0, err
	}

	// Where every record lands in the new log, which is its index.
	merged := make(map[string]int64, len(live))
	var written int64

	writer := bufio.NewWriterSize(file, 64<<10)

	// Oldest first, so the merged log keeps the order the records were written
	// in and merging the same logs twice produces the same bytes.
	for i := len(victims) - 1; i >= 0; i-- {
		seg := victims[i]

		var writeErr error
		scanErr := seg.scan(func(pos int64, raw []byte, r Record) bool {
			if r.Type != RecordTypeNormal && dropTombstones {
				return true
			}
			if loc, ok := live[string(r.Key)]; !ok || loc.seg != seg || loc.pos != pos {
				return true
			}
			if _, err := writer.Write(raw); err != nil {
				writeErr = err
				return false
			}
			merged[string(r.Key)] = written
			written += int64(len(raw))
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
	if err := file.Close(); err != nil {
		disk.Remove(path)
		return nil, 0, err
	}

	return merged, written, nil
}

// adoptMerged opens a log the merge has just written, taking the index the
// merge already built rather than reading the log again, and writes the hint
// that saves the next open from reading it either.
func adoptMerged(id uint64, path string, index map[string]int64, size int64) (*diskSegment, error) {
	file, err := disk.Open(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	writeHint(path, size, index)

	return &diskSegment{segID: id, path: path, file: file, index: index, bytes: size}, nil
}

// syncDir makes a rename in dir durable. Not every filesystem supports it, so a
// failure is not fatal.
func syncDir(dir string) {
	if handle, err := disk.Open(dir, os.O_RDONLY, 0); err == nil {
		handle.Sync()
		handle.Close()
	}
}

// Sync flushes every log to the disk: the active one, and the frozen ones,
// which under SyncNever may be holding records the operating system has not
// written out yet.
//
// It also reports a rotation that could not be finished, which Write does not,
// since the record that prompted one is stored whether or not the rotation
// that followed it worked.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	err := db.rotateErr
	db.rotateErr = nil

	if db.active != nil {
		if serr := db.active.sync(); err == nil {
			err = serr
		}
	}
	// A frozen log is never written again, but under SyncNever it may never
	// have been synced either, and asking for a sync means all of it.
	for _, seg := range db.frozen {
		if serr := seg.sync(); err == nil {
			err = serr
		}
	}
	return err
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
	err := db.rotateErr
	db.rotateErr = nil

	// Closing the active store syncs it; the frozen ones have to be asked,
	// since closing a file does not flush it.
	if db.active != nil {
		if cerr := db.active.close(); err == nil {
			err = cerr
		}
	}
	for _, seg := range db.frozen {
		if serr := seg.sync(); err == nil {
			err = serr
		}
		if cerr := seg.close(); err == nil {
			err = cerr
		}
	}

	return err
}
