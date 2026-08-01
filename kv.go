package litekv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"math"
	"runtime"
	"sync"
	"time"
)

// RecordType is a custom uint8 type that represents the type of a record.
// It is used to differentiate between normal and deleted records in the KeyValueStore.
type RecordType uint8

// Define constants for the different record types.
const (
	// RecordTypeNormal represents a normal record, which contains a key-value pair.
	RecordTypeNormal RecordType = iota

	// RecordTypeDeleted represents a deleted record, which is marked as deleted but not removed from the Data slice.
	RecordTypeDeleted
)

// Record versions, and the size of the fixed-width part of each.
//
// The byte after the checksum says which layout a record is in. In the first
// format that byte was the record type, which is only ever 0 or 1, so a version
// of 2 or more cannot be mistaken for one of those and the two can sit in the
// same log. That is what lets a store written before versions existed go on
// being read.
const (
	// recordV0 is the original layout: Crc (4), Type (1), KeyLength (4),
	// ValueLength (4). It is read but no longer written.
	recordV0     = 0
	headerSizeV0 = 13

	// recordV1 is the current layout: Crc (4), Version (1), Type (1),
	// Timestamp (8), KeyLength (4), ValueLength (4).
	recordV1     = 2
	headerSizeV1 = 22

	// recordVersion is what new records are written as.
	recordVersion = recordV1

	// headerSize is the largest header, which is what a reader has to have in
	// hand before it knows which layout it is looking at.
	headerSize = headerSizeV1
)

// headerSizeFor returns the fixed-width size of a record of this version, and
// whether the version is one this package knows.
func headerSizeFor(version uint8) (int64, bool) {
	switch {
	case version <= 1: // the type byte of the original layout
		return headerSizeV0, true
	case version == recordV1:
		return headerSizeV1, true
	default:
		return 0, false
	}
}

// maxFieldLen is the largest key or value that fits in the uint32 length fields.
const maxFieldLen = math.MaxUint32

// Record represents a single key-value pair along with its metadata in the KeyValueStore.
// It contains fields for the CRC checksum, record type (normal or deleted), key, value, key length, and value length.
type Record struct {
	Crc         uint32     // 4 bytes: The CRC-32 checksum used to ensure the integrity of the stored record.
	Type        RecordType // 1 byte: The record type, which can be either RecordTypeNormal or RecordTypeDeleted.
	Key         []byte     // Variable length: The key of the key-value pair.
	Value       []byte     // Variable length: The value of the key-value pair.
	KeyLength   uint32     // 4 bytes: The length of the key.
	ValueLength uint32     // 4 bytes: The length of the value.

	// Version is the layout this record was written in. Records read from a
	// store written before versions existed report 0, and carry no timestamp.
	Version uint8

	// Timestamp is when the record was written, in nanoseconds since the Unix
	// epoch, or zero for a record from before there were timestamps. It is the
	// writer's clock, so it says when the store was told, not the order two
	// stores did anything in.
	Timestamp int64
}

// Written returns when the record was written, or the zero time for one from a
// store written before records carried timestamps.
func (r Record) Written() time.Time {
	if r.Timestamp == 0 {
		return time.Time{}
	}
	return time.Unix(0, r.Timestamp)
}

// Error is a custom error type that wraps a string. It is used for providing
// specific error messages related to the KeyValueStore operations.
type Error string

// Error method implements the error interface for the custom Error type.
// It returns the string representation of the error message.
func (e Error) Error() string { return string(e) }

// Define constants for common error scenarios in KeyValueStore operations.
const (
	// ErrorKeyDeleted is returned when trying to read a key that has been deleted.
	ErrorKeyDeleted = Error("key is deleted")

	// ErrorKeyNotFound is returned when trying to read a key that does not exist.
	ErrorKeyNotFound = Error("key not found")

	// ErrorChecksumMismatch is returned when a record's calculated checksum
	// does not match its stored checksum, indicating Data corruption.
	ErrorChecksumMismatch = Error("checksum mismatch")

	// ErrorCorruptData is returned when a record cannot be decoded from the Data
	// slice, for example because its length fields run past the end of the slice.
	// Errors reported by a scan of the whole store also match this via errors.Is.
	ErrorCorruptData = Error("corrupt data")

	// ErrorKeyMismatch is returned when the Index points at a record that holds a
	// different key than the one being looked up, which means the Index is stale
	// or belongs to a different store.
	ErrorKeyMismatch = Error("index points at a different key")

	// ErrorRecordTooLarge is returned when a key or value does not fit in the
	// uint32 length fields of the binary format.
	ErrorRecordTooLarge = Error("key or value exceeds 4 GiB")
)

// CorruptAtError reports the offset in the Data slice at which decoding stopped.
// It matches ErrorCorruptData under errors.Is, so callers that do not care about
// the offset can test for ErrorCorruptData.
type CorruptAtError struct {
	Offset int64
}

func (e *CorruptAtError) Error() string {
	return fmt.Sprintf("corrupt record at offset %d", e.Offset)
}

func (e *CorruptAtError) Is(target error) bool { return target == ErrorCorruptData }

// KeyValueStore is a simple key-value store implementation.
// It utilizes a byte slice (Data) to store serialized records and a map (Index) to map keys to their position in the Data byte slice.
// The KeyValueStore struct also embeds a reader-writer lock to ensure thread safety during concurrent read and write operations.
//
// Data and Index are exported so that the store can be backed by a file or by
// POSIX shared memory. Callers that touch them directly must hold the embedded
// lock, and must call RebuildIndex or Recover after replacing Data.
//
// The zero value is an in-memory store that touches no disk. Open backs a store
// with a file, and Attach mirrors it to any Log; see file.go. Neither is
// required, and Data remains yours to save and restore by hand.
//
// The zero value is ready to use, and a store must not be copied once used.
type KeyValueStore struct {
	shardedRWMutex                  // Embed the lock to ensure thread safety during concurrent read and write operations.
	Data           []byte           // A byte slice that holds the serialized records.
	Index          map[string]int64 // A map that maps keys (as strings) to their position in the Data byte slice.

	// state is the log the store mirrors writes to, nil for a store that lives
	// only in memory. See file.go.
	state *logState
}

// maxShards bounds the read side of the store's lock.
//
// Both sides of this are linear in the shard count: concurrent reads get faster
// and every write pays for one more lock acquisition. Measured on a ten core
// machine, a 1 KiB View from ten goroutines and a 16 byte Write:
//
//	shards      concurrent read      write
//	     1             95.8 ns      48.3 ns
//	     2             60.9 ns      52.2 ns
//	     4             43.7 ns      59.5 ns
//	     8             32.8 ns      75.9 ns
//
// Four keeps most of the read win for a third of the write cost. A store that
// is read far more often than it is written can raise this; one with a single
// reader can set it to 1 and get the old behaviour back.
const maxShards = 4

// cacheLine is an upper bound on the cache line size of the platforms Go runs
// on: 128 bytes on arm64, 64 on amd64.
const cacheLine = 128

// numShards is the number of read shards actually in use, a power of two no
// larger than the number of cores. A single core store keeps one shard and so
// behaves exactly like a plain sync.RWMutex.
var numShards = shardCount(runtime.GOMAXPROCS(0))

// hashSeed spreads keys over the shards. It is per process, so a store's shard
// layout is not predictable from the outside.
var hashSeed = maphash.MakeSeed()

func shardCount(procs int) int {
	n := 1
	for n*2 <= procs && n*2 <= maxShards {
		n *= 2
	}
	return n
}

// shardedRWMutex is a reader-writer lock whose read side is split over several
// independent mutexes, one per key hash.
//
// A sync.RWMutex serializes its own readers: every RLock writes to the same
// counter, so the cache line holding it has to be handed from core to core, and
// concurrent readers end up slower than a single one. Splitting the read side
// means readers of different keys touch different cache lines and scale with
// the cores available. A writer still excludes everyone, by taking every shard.
type shardedRWMutex struct {
	shards [maxShards]paddedRWMutex
}

// paddedRWMutex is a mutex padded out past a cache line, so that two shards can
// never share one. Without the padding the shards would sit next to each other
// in the same line and the cores would go back to trading it.
type paddedRWMutex struct {
	sync.RWMutex
	_ [cacheLine]byte
}

// rlockKey read-locks the shard guarding key and returns it, ready to unlock.
// Holding any one shard is enough to exclude a writer, because a writer holds
// them all.
func (m *shardedRWMutex) rlockKey(key []byte) *paddedRWMutex {
	shard := &m.shards[0]
	if numShards > 1 {
		shard = &m.shards[maphash.Bytes(hashSeed, key)&uint64(numShards-1)]
	}
	shard.RLock()
	return shard
}

// RLock acquires the lock for reading. It is the entry point for callers
// reading Data or Index directly; the store's own reads use rlockKey, which
// spreads them over the shards instead of crowding onto this one.
func (m *shardedRWMutex) RLock() { m.shards[0].RLock() }

// RUnlock releases a lock acquired by RLock.
func (m *shardedRWMutex) RUnlock() { m.shards[0].RUnlock() }

// Lock acquires the lock for writing, which means acquiring every shard. The
// shards are always taken in the same order, and a reader only ever holds one,
// so this cannot deadlock.
func (m *shardedRWMutex) Lock() {
	for i := 0; i < numShards; i++ {
		m.shards[i].Lock()
	}
}

// Unlock releases a lock acquired by Lock.
func (m *shardedRWMutex) Unlock() {
	for i := numShards - 1; i >= 0; i-- {
		m.shards[i].Unlock()
	}
}

// Write takes a key and a value, both in byte slices, and stores them in the KeyValueStore instance.
// This method creates a new Record for the given key-value pair, appends it to the Data byte slice
// and updates the Index map to map the key to the position of the new record.
//
// It returns ErrorRecordTooLarge if the key or the value does not fit in the
// uint32 length fields of the binary format. The key and value are copied into
// Data, so the caller may reuse both slices afterwards.
func (kvs *KeyValueStore) Write(key, value []byte) error {
	if uint64(len(key)) > maxFieldLen || uint64(len(value)) > maxFieldLen {
		return ErrorRecordTooLarge
	}

	record := &Record{
		Version:     recordVersion,
		Type:        RecordTypeNormal,
		Timestamp:   time.Now().UnixNano(),
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}
	record.Crc = record.calculateChecksum()

	kvs.Lock()
	defer kvs.Unlock()

	return kvs.appendRecord(record, key)
}

// Size returns the number of bytes the store's records occupy, superseded
// records and tombstones included.
func (kvs *KeyValueStore) Size() int64 {
	kvs.RLock()
	defer kvs.RUnlock()

	return int64(len(kvs.Data))
}

// Read takes a key in the form of a byte slice and retrieves the associated value from the KeyValueStore instance.
// It returns a copy of the value, or an error if the key is not found, is deleted, or the record cannot be
// verified: ErrorChecksumMismatch for a damaged record, ErrorKeyMismatch or ErrorCorruptData for an Index
// entry that does not describe the record it points at.
func (kvs *KeyValueStore) Read(key []byte) ([]byte, error) {
	defer kvs.rlockKey(key).RUnlock()

	stored, err := kvs.lookup(key)
	if err != nil {
		return nil, err
	}

	// stored aliases kvs.Data, which later writes and Compact may reuse, so hand
	// the caller its own copy.
	value := make([]byte, len(stored))
	copy(value, stored)
	return value, nil
}

// View calls fn with the value stored under key, passing the bytes held in the
// Data slice rather than a copy of them. It saves an allocation and a copy per
// read, which is worth having for large values, at the cost of a sharper
// contract: the value is only valid until fn returns, fn must not modify it,
// and fn must not call back into the store, which is locked for reading while
// it runs.
//
// The error from fn is returned as is. Lookup failures are reported exactly as
// by Read, in which case fn is not called.
func (kvs *KeyValueStore) View(key []byte, fn func(value []byte) error) error {
	defer kvs.rlockKey(key).RUnlock()

	stored, err := kvs.lookup(key)
	if err != nil {
		return err
	}
	return fn(stored)
}

// lookup resolves key to the value held in the Data slice, verifying the record
// it lands on. The returned slice aliases Data. Callers must hold at least a
// read lock.
func (kvs *KeyValueStore) lookup(key []byte) ([]byte, error) {
	pos, exists := kvs.Index[string(key)]
	if !exists {
		return nil, ErrorKeyNotFound
	}

	record, next, err := parseRecordAt(kvs.Data, pos)
	if err != nil {
		return nil, err
	}

	if record.Crc != checksumSerialized(kvs.Data[pos:next]) {
		return nil, ErrorChecksumMismatch
	}

	// The record decoded cleanly, so a key that differs from the one asked for
	// means the Index is stale rather than the Data being damaged.
	if !bytes.Equal(record.Key, key) {
		return nil, ErrorKeyMismatch
	}

	if record.Type != RecordTypeNormal {
		return nil, ErrorKeyDeleted
	}

	return record.Value, nil
}

// Modified returns when the newest record for key was written. It reports
// ErrorKeyDeleted for a key whose newest record is a tombstone, since that
// record has a time of its own: when the key was deleted.
//
// A record written before the format carried timestamps reports the zero time.
func (kvs *KeyValueStore) Modified(key []byte) (time.Time, error) {
	defer kvs.rlockKey(key).RUnlock()

	pos, exists := kvs.Index[string(key)]
	if !exists {
		return time.Time{}, ErrorKeyNotFound
	}

	record, next, err := parseRecordAt(kvs.Data, pos)
	if err != nil {
		return time.Time{}, err
	}
	if record.Crc != checksumSerialized(kvs.Data[pos:next]) {
		return time.Time{}, ErrorChecksumMismatch
	}
	if !bytes.Equal(record.Key, key) {
		return time.Time{}, ErrorKeyMismatch
	}
	if record.Type != RecordTypeNormal {
		return record.Written(), ErrorKeyDeleted
	}

	return record.Written(), nil
}

// Delete takes a key in the form of a byte slice and marks the associated record as deleted in the KeyValueStore instance.
// It achieves this by creating a new Record with the RecordType set to RecordTypeDeleted and appending it to the Data byte slice.
// It also updates the Index map to map the key to the position of the new deleted record.
//
// Deleting a key that was never written is not an error; it appends a tombstone
// that Compact later drops. It returns ErrorRecordTooLarge for an oversized key.
func (kvs *KeyValueStore) Delete(key []byte) error {
	if uint64(len(key)) > maxFieldLen {
		return ErrorRecordTooLarge
	}

	record := &Record{
		Version:   recordVersion,
		Type:      RecordTypeDeleted,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		KeyLength: uint32(len(key)),
	}
	record.Crc = record.calculateChecksum()

	kvs.Lock()
	defer kvs.Unlock()

	return kvs.appendRecord(record, key)
}

// calculateChecksum calculates the CRC-32 (IEEE) checksum over the record's Type,
// KeyLength, ValueLength, Key and Value fields, in that order. The Crc field
// itself is excluded. The checksum is computed incrementally, so no intermediate
// copy of the key or value is made.
//
// The nine header bytes are folded in a byte at a time rather than being built
// in a local array: hash/crc32's arm64 path does not mark its argument
// noescape, so handing it a stack array moves that array to the heap and costs
// an allocation on every read and every write. Records that are already
// serialized are cheaper to check with checksumSerialized.
func (r *Record) calculateChecksum() uint32 {
	crc := ^uint32(0)
	crc = crcFoldByte(crc, recordVersion)
	crc = crcFoldByte(crc, byte(r.Type))
	crc = crcFoldUint64(crc, uint64(r.Timestamp))
	crc = crcFoldUint32(crc, r.KeyLength)
	crc = crcFoldUint32(crc, r.ValueLength)

	crc = crc32.Update(^crc, crc32.IEEETable, r.Key)
	return crc32.Update(crc, crc32.IEEETable, r.Value)
}

// crcFoldUint64 folds a little-endian uint64 into a pre-complemented CRC.
func crcFoldUint64(crc uint32, v uint64) uint32 {
	crc = crcFoldUint32(crc, uint32(v))
	return crcFoldUint32(crc, uint32(v>>32))
}

// checksumSerialized calculates the checksum of a record that is already laid
// out in its binary form, which is a single pass over contiguous memory and so
// noticeably faster than calculateChecksum. record must span exactly one
// record, from its Crc field through the end of its value.
func checksumSerialized(record []byte) uint32 {
	return crc32.ChecksumIEEE(record[4:])
}

// crcFoldByte folds one byte into a CRC held in hash/crc32's internal,
// pre-complemented form.
func crcFoldByte(crc uint32, b byte) uint32 {
	return crc32.IEEETable[byte(crc)^b] ^ (crc >> 8)
}

// crcFoldUint32 folds a little-endian uint32 into a pre-complemented CRC.
func crcFoldUint32(crc uint32, v uint32) uint32 {
	crc = crcFoldByte(crc, byte(v))
	crc = crcFoldByte(crc, byte(v>>8))
	crc = crcFoldByte(crc, byte(v>>16))
	return crcFoldByte(crc, byte(v>>24))
}

// appendTo serializes the Record and appends it to dst, returning the extended
// slice, always in the current layout. Fields are written in little-endian
// order: Crc, Version, Type, Timestamp, KeyLength, ValueLength, Key, Value.
func (r *Record) appendTo(dst []byte) []byte {
	var hdr [headerSizeV1]byte
	binary.LittleEndian.PutUint32(hdr[0:4], r.Crc)
	hdr[4] = recordVersion
	hdr[5] = byte(r.Type)
	binary.LittleEndian.PutUint64(hdr[6:14], uint64(r.Timestamp))
	binary.LittleEndian.PutUint32(hdr[14:18], r.KeyLength)
	binary.LittleEndian.PutUint32(hdr[18:22], r.ValueLength)

	dst = append(dst, hdr[:]...)
	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)
	return dst
}

// size returns the number of bytes the record occupies in the Data slice, in
// the layout it was read in.
func (r *Record) size() int64 {
	header, ok := headerSizeFor(r.Version)
	if !ok {
		header = headerSizeV1
	}
	return header + int64(len(r.Key)) + int64(len(r.Value))
}

// parseRecordAt decodes the record starting at pos in data and returns it along
// with the offset of the following record. The declared key and value lengths
// are checked against the bytes actually available, so damaged or attacker
// supplied input yields an error instead of a panic or a multi-gigabyte
// allocation. The returned Key and Value alias data and must be copied before
// being handed to a caller or retained across a mutation of the store.
func parseRecordAt(data []byte, pos int64) (Record, int64, error) {
	if pos < 0 || pos > int64(len(data)) {
		return Record{}, 0, &CorruptAtError{Offset: pos}
	}

	buf := data[pos:]

	header, ok := decodeHeader(buf)
	if !ok {
		return Record{}, 0, &CorruptAtError{Offset: pos}
	}

	// Widening to uint64 keeps the sum from overflowing on 32-bit platforms.
	end := uint64(header.size) + uint64(header.keyLength) + uint64(header.valueLength)
	if end > uint64(len(buf)) {
		return Record{}, 0, &CorruptAtError{Offset: pos}
	}

	keyEnd := uint64(header.size) + uint64(header.keyLength)

	r := header.record()
	r.Key = buf[header.size:keyEnd:keyEnd]
	r.Value = buf[keyEnd:end:end]

	return r, pos + int64(end), nil
}

// recordHeader is the fixed part of a record, whichever layout it is in.
type recordHeader struct {
	crc         uint32
	version     uint8
	recordType  RecordType
	timestamp   int64
	keyLength   uint32
	valueLength uint32
	size        int64 // what the header itself takes
}

// record returns the header as a Record, with the key and value still to fill
// in.
func (h recordHeader) record() Record {
	return Record{
		Crc:         h.crc,
		Version:     h.version,
		Type:        h.recordType,
		Timestamp:   h.timestamp,
		KeyLength:   h.keyLength,
		ValueLength: h.valueLength,
	}
}

// decodeHeader reads the fixed part of a record from the front of buf. It
// reports false if the version is not one this package knows, or if buf is
// shorter than the layout it turns out to be.
//
// This is the only place the byte offsets of a record are written down. They
// were in three places once, and adding a field left two of them reading the
// wrong bytes while compiling perfectly well.
func decodeHeader(buf []byte) (recordHeader, bool) {
	if len(buf) < headerSizeV0 {
		return recordHeader{}, false
	}

	size, known := headerSizeFor(buf[4])
	if !known || int64(len(buf)) < size {
		return recordHeader{}, false
	}

	h := recordHeader{crc: binary.LittleEndian.Uint32(buf[0:4]), size: size}

	if size == headerSizeV0 {
		h.version = recordV0
		h.recordType = RecordType(buf[4])
		h.keyLength = binary.LittleEndian.Uint32(buf[5:9])
		h.valueLength = binary.LittleEndian.Uint32(buf[9:13])
		return h, true
	}

	h.version = buf[4]
	h.recordType = RecordType(buf[5])
	h.timestamp = int64(binary.LittleEndian.Uint64(buf[6:14]))
	h.keyLength = binary.LittleEndian.Uint32(buf[14:18])
	h.valueLength = binary.LittleEndian.Uint32(buf[18:22])
	return h, true
}

// scan walks the records in the Data slice in order and calls fn for each one
// with the offsets it spans, stopping early if fn returns false. It returns a
// *CorruptAtError as soon as a record fails to decode. Callers must hold at
// least a read lock.
func (kvs *KeyValueStore) scan(fn func(pos, next int64, r Record) bool) error {
	var pos int64
	for pos < int64(len(kvs.Data)) {
		record, next, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return err
		}
		// By value: handing a *Record to a func value would defeat escape
		// analysis and put a record on the heap for every one scanned.
		if !fn(pos, next, record) {
			return nil
		}
		pos = next
	}
	return nil
}

// offsets returns the start offset of every record in the Data slice, in order.
// Walking those offsets backwards lets the index builders insert each key
// exactly once, which matters because inserting into a map[string]... converts
// the key to a string and so allocates, while looking one up does not. A store
// holding many versions of the same key would otherwise allocate per record
// rather than per key. Callers must hold at least a read lock.
func (kvs *KeyValueStore) offsets() ([]int64, error) {
	// A record is at least a header, which bounds the count, but that bound is
	// wildly high for a store of large values, so cap the head start.
	offs := make([]int64, 0, min(len(kvs.Data)/headerSize+1, 4096))

	var pos int64
	for pos < int64(len(kvs.Data)) {
		_, next, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return offs, err
		}
		offs = append(offs, pos)
		pos = next
	}

	return offs, nil
}

// SaveIndex serializes the KeyValueStore's Index (a map of keys to their position in the Data byte slice)
// using the gob package, and returns the serialized Index as a byte slice.
// This method can be used to persist the Index to disk or another storage medium, for later restoration.
func (kvs *KeyValueStore) SaveIndex() ([]byte, error) {
	kvs.RLock()
	defer kvs.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kvs.Index); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// LoadIndex deserializes a byte slice containing a serialized Index (a map of keys to their position in the Data byte slice)
// using the gob package, and restores the deserialized Index to the KeyValueStore.
// This method can be used to load a previously saved Index from disk or another storage medium.
//
// The Index replaces the current one rather than being merged into it, and every
// entry is checked against the Data slice before it is installed: an entry that
// does not point at a record holding that exact key makes LoadIndex fail with
// ErrorKeyMismatch or ErrorCorruptData and leaves the store untouched. Populate
// Data before calling LoadIndex.
func (kvs *KeyValueStore) LoadIndex(data []byte) error {
	index := make(map[string]int64)
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&index); err != nil {
		return err
	}

	kvs.Lock()
	defer kvs.Unlock()

	for key, pos := range index {
		record, _, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return fmt.Errorf("index entry %q at offset %d: %w", key, pos, err)
		}
		if string(record.Key) != key {
			return fmt.Errorf("index entry %q points at record %q: %w", key, record.Key, ErrorKeyMismatch)
		}
	}

	kvs.Index = index
	return nil
}

// latestOffsets returns the offset of the newest live record for each key.
// Records are appended, so a later record supersedes an earlier one with the
// same key, and a tombstone removes the key until a later write re-adds it.
// Callers must hold at least a read lock.
func (kvs *KeyValueStore) latestOffsets() (map[string]int64, error) {
	offs, err := kvs.offsets()
	if err != nil {
		return nil, err
	}

	// Walking backwards, the first record seen for a key is its newest, so every
	// key is inserted exactly once and the superseded records cost nothing but a
	// lookup. Tombstoned keys are marked rather than skipped, so that an earlier
	// record cannot resurrect them, and dropped afterwards.
	const deleted = int64(-1)

	latest := make(map[string]int64)
	for i := len(offs) - 1; i >= 0; i-- {
		pos := offs[i]

		record, _, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return nil, err
		}
		if _, seen := latest[string(record.Key)]; seen {
			continue
		}

		if record.Type == RecordTypeNormal {
			latest[string(record.Key)] = pos
		} else {
			latest[string(record.Key)] = deleted
		}
	}

	for key, pos := range latest {
		if pos == deleted {
			delete(latest, key)
		}
	}

	return latest, nil
}

// Compact iterates through the KeyValueStore's Data byte slice, identifies the latest record for each key,
// and rebuilds the Data slice and Index, dropping superseded records and deleted keys.
// This method is useful for reducing the storage size and improving the performance of the KeyValueStore.
//
// Surviving records keep their relative order, so compacting the same store
// twice produces byte-identical Data. If the Data slice cannot be decoded,
// Compact returns an error and leaves the store unchanged.
func (kvs *KeyValueStore) Compact() error {
	kvs.Lock()
	defer kvs.Unlock()

	latest, err := kvs.latestOffsets()
	if err != nil {
		return err
	}

	var total int64
	for _, pos := range latest {
		record, _, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return err
		}
		total += record.size()
	}

	data := make([]byte, 0, total)
	index := make(map[string]int64, len(latest))

	// Walk the Data slice a second time instead of ranging over the map, so that
	// the compacted layout does not depend on Go's map iteration order. Surviving
	// records are copied verbatim rather than re-serialized.
	err = kvs.scan(func(pos, next int64, r Record) bool {
		if r.Type != RecordTypeNormal {
			return true
		}
		if survivor, ok := latest[string(r.Key)]; !ok || survivor != pos {
			return true
		}
		index[string(r.Key)] = int64(len(data))
		data = append(data, kvs.Data[pos:next]...)
		return true
	})
	if err != nil {
		return err
	}

	kvs.Data = data
	kvs.Index = index

	// A store with a log has just shortened its data; the log has to follow, or
	// the next recovery would bring the compacted records back.
	return kvs.rewrite()
}

// RebuildIndex iterates through the KeyValueStore's Data byte slice, deserializes each record,
// and rebuilds the Index by calculating the position of each key in the Data slice.
// This method is useful when the Index has been lost or corrupted and needs to be reconstructed.
//
// If a record fails to decode, which is what a torn append at the tail of the
// Data slice looks like, RebuildIndex still installs the Index built from the
// records before it and returns a *CorruptAtError. Truncating Data to that
// offset discards the damaged tail.
func (kvs *KeyValueStore) RebuildIndex() error {
	kvs.Lock()
	defer kvs.Unlock()

	offs, err := kvs.offsets()

	// Backwards, so that each key is inserted once: see offsets.
	index := make(map[string]int64)
	for i := len(offs) - 1; i >= 0; i-- {
		record, _, perr := parseRecordAt(kvs.Data, offs[i])
		if perr != nil {
			break
		}
		if _, seen := index[string(record.Key)]; !seen {
			index[string(record.Key)] = offs[i]
		}
	}

	kvs.Index = index
	return err
}

// Verify walks every record in the Data slice and checks it against its stored
// checksum. It returns ErrorChecksumMismatch for a damaged record and a
// *CorruptAtError for one that cannot be decoded at all, both wrapped with the
// offset at which the problem was found.
func (kvs *KeyValueStore) Verify() error {
	kvs.RLock()
	defer kvs.RUnlock()

	var bad error
	err := kvs.scan(func(pos, next int64, r Record) bool {
		if r.Crc != checksumSerialized(kvs.Data[pos:next]) {
			bad = fmt.Errorf("record at offset %d: %w", pos, ErrorChecksumMismatch)
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	return bad
}

// ForEach calls fn for every record in the Data slice, in the order the records
// were written, stopping early if fn returns false. Superseded records and
// tombstones are included; deleted reports whether a record is a tombstone. The
// key and value passed to fn alias the Data slice and are only valid until fn
// returns.
func (kvs *KeyValueStore) ForEach(fn func(key, value []byte, deleted bool) bool) error {
	kvs.RLock()
	defer kvs.RUnlock()

	return kvs.scan(func(_, _ int64, r Record) bool {
		return fn(r.Key, r.Value, r.Type == RecordTypeDeleted)
	})
}

// PrintAllKeyValuePairs iterates through the KeyValueStore's Data byte slice, deserializes each record,
// and prints the key, value, and record type for each record.
// This method is useful for debugging and getting an overview of the KeyValueStore's contents.
func (kvs *KeyValueStore) PrintAllKeyValuePairs() error {
	return kvs.ForEach(func(key, value []byte, deleted bool) bool {
		fmt.Printf("Key: %s, Value: %s, Deleted: %t\n", key, value, deleted)
		return true
	})
}
