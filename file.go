package litekv

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// A store keeps its records in the Data slice and nothing else is required.
// The zero value is an in-memory store that touches no disk, Data is yours to
// save and restore however you like, and everything below is opt in.
//
// There are three ways to use it:
//
//   - In memory, as before. Nothing here applies.
//   - With your own byte slice: set Data, call Recover, and save it whenever
//     and wherever you want.
//   - Mirrored to a log, so that every write goes somewhere durable as it
//     happens. Open gives you a file; Attach takes any Log you implement.

// Log is where a store mirrors its records as they are written. An *os.File
// satisfies it, and so does anything else that can hold bytes at offsets:
// implement it to keep the log somewhere this package knows nothing about.
//
// The store only ever appends. WriteAt is called with the record and the offset
// of the end of the log, Truncate only ever shortens it, and both are called
// under the store's write lock, so implementations need not be safe for
// concurrent use. Sync may be called concurrently with WriteAt.
type Log interface {
	// WriteAt writes one record at off, which is the current end of the log.
	WriteAt(p []byte, off int64) (n int, err error)

	// Truncate cuts the log to size, discarding anything beyond it.
	Truncate(size int64) error

	// Sync makes everything written so far durable.
	Sync() error
}

// SyncPolicy decides when a write is on the disk rather than merely handed to
// whatever holds the log. This is the whole durability question, and there is
// no answer that is both free and safe.
type SyncPolicy int

const (
	// SyncAlways syncs before a write returns, so a write that returned nil
	// survives losing power. It is the default because losing acknowledged
	// writes should be something a caller opts into rather than something that
	// happens quietly.
	//
	// The cost is a sync per write, which on a Raspberry Pi with an SD card is
	// milliseconds, and every reader waits for it: the sync happens under the
	// write lock, because there is no way to acknowledge a durable write
	// without waiting for the disk.
	SyncAlways SyncPolicy = iota

	// SyncEvery syncs on a timer, set by Options.Interval. A crash of the
	// process loses nothing, since the bytes are already with the operating
	// system; losing power loses at most the last interval of writes.
	SyncEvery

	// SyncNever never syncs, leaving it to whatever holds the log. Writes cost
	// no more than an in-memory store. A crash of the process still loses
	// nothing when the log is an ordinary file, but losing power loses whatever
	// the operating system was holding, which it promises nothing about.
	SyncNever
)

// defaultInterval is the sync period for SyncEvery when Options.Interval is not
// set.
const defaultInterval = time.Second

// Options configures how a store treats its log. The zero value syncs on every
// write.
type Options struct {
	Sync SyncPolicy

	// Interval is the sync period under SyncEvery. Zero means one second.
	Interval time.Duration
}

// ErrorClosed is returned by a store whose log has been closed.
const ErrorClosed = Error("store is closed")

// ErrorAttached is returned when attaching a log to a store that has one.
const ErrorAttached = Error("store already has a log")

// logState is the durable half of a store, nil for one that lives only in
// memory.
type logState struct {
	log      Log
	policy   SyncPolicy
	interval time.Duration

	// path and owned are set when the store opened the file itself, which is
	// what lets it close the file and rewrite it by rename.
	path  string
	owned bool

	unsynced bool          // writes that have not been synced
	stop     chan struct{} // closed to stop the syncing goroutine
	done     sync.WaitGroup
	closed   bool
}

// Open opens the store held in the file at path, creating it if it does not
// exist. Close it when finished.
//
// The file holds exactly the bytes of the Data slice, so a store written this
// way can be read back by loading the file into Data by hand, and a Data slice
// built in memory can be written out and opened here.
//
// A crash can leave a record half written at the end of the log. Opening
// recovers, dropping everything from the first record that fails to decode or
// fails its checksum and truncating the file to match. Such a record cannot
// have been acknowledged under SyncAlways, which waits for the sync before
// returning; under the other policies it may have been, and that is what they
// trade away.
func Open(path string, opts Options) (*KeyValueStore, error) {
	file, err := openDisk(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	// Read it by offset rather than by cursor: everything else in this package
	// addresses a log by position, and a file with a read cursor is one more
	// thing a stand-in for one would have to get right.
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	data := make([]byte, info.Size())
	if len(data) > 0 {
		if _, err := io.ReadFull(io.NewSectionReader(file, 0, info.Size()), data); err != nil {
			file.Close()
			return nil, err
		}
	}

	kvs := &KeyValueStore{Data: data}
	if err := kvs.attach(file, opts, path, true); err != nil {
		file.Close()
		return nil, err
	}

	if _, err := kvs.Recover(); err != nil {
		kvs.Close()
		return nil, err
	}

	return kvs, nil
}

// Attach mirrors every later write to log, and applies opts to it. The log is
// assumed to already hold exactly what Data holds, which is the case when Data
// was read from it; call Rewrite if it does not.
//
// The caller keeps ownership: Close syncs the log but does not close it, since
// Log cannot be closed. A store opened by Open owns its file and does close it.
func (kvs *KeyValueStore) Attach(log Log, opts Options) error {
	return kvs.attach(log, opts, "", false)
}

func (kvs *KeyValueStore) attach(log Log, opts Options, path string, owned bool) error {
	kvs.Lock()
	defer kvs.Unlock()

	if kvs.state != nil && !kvs.state.closed {
		return ErrorAttached
	}

	interval := opts.Interval
	if interval <= 0 {
		interval = defaultInterval
	}

	kvs.state = &logState{
		log:      log,
		policy:   opts.Sync,
		interval: interval,
		path:     path,
		owned:    owned,
		stop:     make(chan struct{}),
	}

	if opts.Sync == SyncEvery {
		kvs.state.done.Add(1)
		go kvs.syncEvery(kvs.state)
	}

	return nil
}

// Detach stops mirroring writes, after one last sync, and leaves the store as
// an in-memory one holding the same data. It does not close the log.
func (kvs *KeyValueStore) Detach() error {
	kvs.Lock()
	state := kvs.state
	kvs.state = nil
	kvs.Unlock()

	if state == nil || state.closed {
		return nil
	}

	state.closed = true
	close(state.stop)
	state.done.Wait()

	return state.log.Sync()
}

// Recover rebuilds the index from the Data slice, keeping the records up to the
// first one that fails to decode or fails its checksum, and discarding the
// rest. It reports how many bytes it discarded, which is zero for an intact
// store, and truncates the log to match when there is one.
//
// This is what Open does after reading a file, and what to call after loading a
// Data slice from anywhere else. RebuildIndex is the cheaper version that
// checks framing but not checksums, and never discards anything.
//
// A record that fails its checksum part way through an otherwise intact log
// ends the log there: without a marker to resynchronise on, there is no way to
// know where the next record begins. Verify reports such damage without
// discarding anything.
func (kvs *KeyValueStore) Recover() (int64, error) {
	kvs.Lock()
	defer kvs.Unlock()

	index := make(map[string]int64)

	// A record that fails to decode ends the scan with an error, and one that
	// fails its checksum ends it by returning false. Either way the log is only
	// trustworthy up to that point.
	var good int64
	kvs.scan(func(pos, next int64, r Record) bool {
		if r.Crc != checksumSerialized(kvs.Data[pos:next]) {
			return false
		}
		index[string(r.Key)] = pos
		good = next
		return true
	})

	kvs.Index = index

	discarded := int64(len(kvs.Data)) - good
	if discarded == 0 {
		return 0, nil
	}

	kvs.Data = kvs.Data[:good]

	if kvs.state != nil && !kvs.state.closed {
		if err := kvs.state.log.Truncate(good); err != nil {
			return discarded, err
		}
		if err := kvs.state.log.Sync(); err != nil {
			return discarded, err
		}
	}

	return discarded, nil
}

// Rewrite replaces the contents of the log with what the store holds now. It is
// how a compacted store gets shorter on disk, and how to seed a log that does
// not yet hold what Data does.
//
// For a store opened by Open this is crash safe: the new log is written beside
// the old one and renamed over it, so an interrupted rewrite leaves either the
// whole old log or the whole new one. For a log supplied by Attach it is a
// truncate followed by a write, and an interrupted rewrite leaves neither, so
// an implementation that cares should do better.
func (kvs *KeyValueStore) Rewrite() error {
	kvs.Lock()
	defer kvs.Unlock()

	return kvs.rewrite()
}

// rewrite is Rewrite with the write lock already held.
func (kvs *KeyValueStore) rewrite() error {
	state := kvs.state
	if state == nil {
		return nil
	}
	if state.closed {
		return ErrorClosed
	}

	if !state.owned {
		if err := state.log.Truncate(0); err != nil {
			return err
		}
		if _, err := state.log.WriteAt(kvs.Data, 0); err != nil {
			return err
		}
		state.unsynced = false
		return state.log.Sync()
	}

	return kvs.rewriteFile(state)
}

// rewriteFile replaces an owned file by writing a new one alongside it and
// renaming it into place.
func (kvs *KeyValueStore) rewriteFile(state *logState) error {
	temp := state.path + ".rewrite"

	file, err := openDisk(temp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if _, err := file.Write(kvs.Data); err != nil {
		file.Close()
		os.Remove(temp)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(temp)
		return err
	}
	if err := os.Rename(temp, state.path); err != nil {
		file.Close()
		os.Remove(temp)
		return err
	}

	// The rename is only durable once the directory holding it is synced. Not
	// every filesystem supports that, so a failure here is not fatal.
	if dir, err := openDisk(filepath.Dir(state.path), os.O_RDONLY, 0); err == nil {
		dir.Sync()
		dir.Close()
	}

	if old, ok := state.log.(io.Closer); ok {
		old.Close()
	}
	state.log = file
	state.unsynced = false
	return nil
}

// Sync syncs the log, if there is one. It is what SyncEvery does on its timer,
// and is worth calling before a planned shutdown under any policy other than
// SyncAlways.
func (kvs *KeyValueStore) Sync() error {
	kvs.Lock()
	state := kvs.state
	if state == nil || state.closed {
		kvs.Unlock()
		return nil
	}
	state.unsynced = false
	log := state.log
	kvs.Unlock()

	return log.Sync()
}

// Close syncs the store and, if it opened the log itself, closes it. A closed
// store rejects writes. Closing a store that lives only in memory does nothing.
//
// Close is worth deferring, and a deferred Close does run while a panic
// unwinds. It does not run on os.Exit, which log.Fatal calls, or on a signal
// that is not handled, or on SIGKILL. That costs less than it sounds: a record
// is handed to the operating system as Write returns, so a process that dies
// without closing loses nothing, and Open recovers everything it wrote. Only
// losing power loses records, and only those the sync policy had not yet
// covered, which no amount of deferring can help with.
//
// Under SyncEvery the timer goroutine holds a reference to the store, so a
// store that is abandoned rather than closed keeps that goroutine and its file
// descriptor for the life of the process.
func (kvs *KeyValueStore) Close() error {
	kvs.Lock()
	state := kvs.state
	if state == nil || state.closed {
		kvs.Unlock()
		return nil
	}
	state.closed = true
	kvs.Unlock()

	// Outside the lock, because the syncing goroutine takes it.
	close(state.stop)
	state.done.Wait()

	err := state.log.Sync()

	if state.owned {
		if closer, ok := state.log.(io.Closer); ok {
			if cerr := closer.Close(); err == nil {
				err = cerr
			}
		}
	}

	return err
}

// closeNoSync closes the log without syncing it, for a log whose contents are
// about to be thrown away. Syncing one of those only costs a barrier the disk
// then has nothing to do with.
func (kvs *KeyValueStore) closeNoSync() error {
	kvs.Lock()
	state := kvs.state
	if state == nil || state.closed {
		kvs.Unlock()
		return nil
	}
	state.closed = true
	kvs.Unlock()

	close(state.stop)
	state.done.Wait()

	if state.owned {
		if closer, ok := state.log.(io.Closer); ok {
			return closer.Close()
		}
	}
	return nil
}

// syncEvery syncs on a timer for SyncEvery.
func (kvs *KeyValueStore) syncEvery(state *logState) {
	defer state.done.Done()

	ticker := time.NewTicker(state.interval)
	defer ticker.Stop()

	for {
		select {
		case <-state.stop:
			return

		case <-ticker.C:
			// Hold the lock only to read the flag. Syncing under it would stop
			// every reader for as long as the disk takes, which is the cost
			// this policy exists to avoid.
			kvs.Lock()
			unsynced := state.unsynced
			state.unsynced = false
			kvs.Unlock()

			if unsynced {
				state.log.Sync()
			}
		}
	}
}

// appendRecord adds the record to the Data slice and to the log, and points the
// index at it. Callers must hold the write lock.
//
// The order matters: the index is pointed at the record only once both have
// taken it, so a write that fails leaves the store exactly as it was rather
// than half applied.
func (kvs *KeyValueStore) appendRecord(record *Record, key []byte) error {
	state := kvs.state
	if state != nil && state.closed {
		return ErrorClosed
	}

	pos := int64(len(kvs.Data))
	kvs.Data = record.appendTo(kvs.Data)

	if state != nil {
		if err := kvs.writeToLog(state, kvs.Data[pos:], pos); err != nil {
			kvs.Data = kvs.Data[:pos]
			return err
		}
	}

	if kvs.Index == nil {
		kvs.Index = make(map[string]int64)
	}
	kvs.Index[string(key)] = pos
	return nil
}

// writeToLog puts one record at pos and applies the sync policy. A write that
// fails part way is truncated away, so recovery has nothing half written to
// trip over.
func (kvs *KeyValueStore) writeToLog(state *logState, record []byte, pos int64) error {
	if _, err := state.log.WriteAt(record, pos); err != nil {
		state.log.Truncate(pos) // best effort; recovery drops a torn tail anyway
		return err
	}

	switch state.policy {
	case SyncAlways:
		return state.log.Sync()
	case SyncEvery:
		state.unsynced = true
	}

	return nil
}
