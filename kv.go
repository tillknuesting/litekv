package litekv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
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

// headerSize is the size of the fixed-width part of a serialized record:
// Crc (4) + Type (1) + KeyLength (4) + ValueLength (4).
const headerSize = 13

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
// The KeyValueStore struct also embeds the sync.RWMutex to ensure thread safety during concurrent read and write operations.
//
// Data and Index are exported so that the store can be backed by a file or by
// POSIX shared memory. Callers that touch them directly must hold the embedded
// mutex, and must call RebuildIndex after replacing Data.
type KeyValueStore struct {
	sync.RWMutex                  // Embed the RWMutex to ensure thread safety during concurrent read and write operations.
	Data         []byte           // A byte slice that holds the serialized records.
	Index        map[string]int64 // A map that maps keys (as strings) to their position in the Data byte slice.
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
		Type:        RecordTypeNormal,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}
	record.Crc = record.calculateChecksum()

	kvs.Lock()
	defer kvs.Unlock()

	if kvs.Index == nil {
		kvs.Index = make(map[string]int64)
	}

	kvs.Index[string(key)] = int64(len(kvs.Data))
	kvs.Data = record.appendTo(kvs.Data)
	return nil
}

// Read takes a key in the form of a byte slice and retrieves the associated value from the KeyValueStore instance.
// It returns a copy of the value, or an error if the key is not found, is deleted, or the record cannot be
// verified: ErrorChecksumMismatch for a damaged record, ErrorKeyMismatch or ErrorCorruptData for an Index
// entry that does not describe the record it points at.
func (kvs *KeyValueStore) Read(key []byte) ([]byte, error) {
	kvs.RLock()
	defer kvs.RUnlock()

	pos, exists := kvs.Index[string(key)]
	if !exists {
		return nil, ErrorKeyNotFound
	}

	record, _, err := parseRecordAt(kvs.Data, pos)
	if err != nil {
		return nil, err
	}

	if record.Crc != record.calculateChecksum() {
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

	// record.Value aliases kvs.Data, which later writes and Compact may reuse,
	// so hand the caller its own copy.
	value := make([]byte, len(record.Value))
	copy(value, record.Value)
	return value, nil
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
		Type:      RecordTypeDeleted,
		Key:       key,
		KeyLength: uint32(len(key)),
	}
	record.Crc = record.calculateChecksum()

	kvs.Lock()
	defer kvs.Unlock()

	if kvs.Index == nil {
		kvs.Index = make(map[string]int64)
	}

	kvs.Index[string(key)] = int64(len(kvs.Data))
	kvs.Data = record.appendTo(kvs.Data)
	return nil
}

// calculateChecksum calculates the CRC-32 (IEEE) checksum over the record's Type,
// KeyLength, ValueLength, Key and Value fields, in that order. The Crc field
// itself is excluded. The checksum is computed incrementally, so no intermediate
// copy of the key or value is made.
func (r *Record) calculateChecksum() uint32 {
	var hdr [headerSize - 4]byte
	hdr[0] = byte(r.Type)
	binary.LittleEndian.PutUint32(hdr[1:5], r.KeyLength)
	binary.LittleEndian.PutUint32(hdr[5:9], r.ValueLength)

	crc := crc32.ChecksumIEEE(hdr[:])
	crc = crc32.Update(crc, crc32.IEEETable, r.Key)
	crc = crc32.Update(crc, crc32.IEEETable, r.Value)
	return crc
}

// appendTo serializes the Record and appends it to dst, returning the extended
// slice. Fields are written in little-endian order: Crc, Type, KeyLength,
// ValueLength, Key, Value.
func (r *Record) appendTo(dst []byte) []byte {
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], r.Crc)
	hdr[4] = byte(r.Type)
	binary.LittleEndian.PutUint32(hdr[5:9], r.KeyLength)
	binary.LittleEndian.PutUint32(hdr[9:13], r.ValueLength)

	dst = append(dst, hdr[:]...)
	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)
	return dst
}

// size returns the number of bytes the record occupies in the Data slice.
func (r *Record) size() int64 {
	return headerSize + int64(len(r.Key)) + int64(len(r.Value))
}

// parseRecordAt decodes the record starting at pos in data and returns it along
// with the offset of the following record. The declared key and value lengths
// are checked against the bytes actually available, so damaged or attacker
// supplied input yields an error instead of a panic or a multi-gigabyte
// allocation. The returned Key and Value alias data and must be copied before
// being handed to a caller or retained across a mutation of the store.
func parseRecordAt(data []byte, pos int64) (Record, int64, error) {
	if pos < 0 || pos > int64(len(data)) || int64(len(data))-pos < headerSize {
		return Record{}, 0, &CorruptAtError{Offset: pos}
	}

	buf := data[pos:]

	var r Record
	r.Crc = binary.LittleEndian.Uint32(buf[0:4])
	r.Type = RecordType(buf[4])
	r.KeyLength = binary.LittleEndian.Uint32(buf[5:9])
	r.ValueLength = binary.LittleEndian.Uint32(buf[9:13])

	// Widening to uint64 keeps the sum from overflowing on 32-bit platforms.
	end := uint64(headerSize) + uint64(r.KeyLength) + uint64(r.ValueLength)
	if end > uint64(len(buf)) {
		return Record{}, 0, &CorruptAtError{Offset: pos}
	}

	keyEnd := headerSize + uint64(r.KeyLength)
	r.Key = buf[headerSize:keyEnd:keyEnd]
	r.Value = buf[keyEnd:end:end]

	return r, pos + int64(end), nil
}

// scan walks the records in the Data slice in order and calls fn for each one,
// stopping early if fn returns false. It returns a *CorruptAtError as soon as a
// record fails to decode. Callers must hold at least a read lock.
func (kvs *KeyValueStore) scan(fn func(pos int64, r *Record) bool) error {
	var pos int64
	for pos < int64(len(kvs.Data)) {
		record, next, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return err
		}
		if !fn(pos, &record) {
			return nil
		}
		pos = next
	}
	return nil
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
	latest := make(map[string]int64)

	err := kvs.scan(func(pos int64, r *Record) bool {
		key := string(r.Key)
		if r.Type == RecordTypeNormal {
			latest[key] = pos
		} else {
			delete(latest, key)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return latest, nil
}

// findLatestRecords returns the newest live record for each key in the store.
// The Key and Value of each returned record alias the Data slice.
// Callers must hold at least a read lock.
func (kvs *KeyValueStore) findLatestRecords() (map[string]*Record, error) {
	latest, err := kvs.latestOffsets()
	if err != nil {
		return nil, err
	}

	records := make(map[string]*Record, len(latest))
	for key, pos := range latest {
		record, _, err := parseRecordAt(kvs.Data, pos)
		if err != nil {
			return nil, err
		}
		records[key] = &record
	}

	return records, nil
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
	// the compacted layout does not depend on Go's map iteration order.
	err = kvs.scan(func(pos int64, r *Record) bool {
		if r.Type != RecordTypeNormal {
			return true
		}
		if survivor, ok := latest[string(r.Key)]; !ok || survivor != pos {
			return true
		}
		index[string(r.Key)] = int64(len(data))
		data = r.appendTo(data)
		return true
	})
	if err != nil {
		return err
	}

	kvs.Data = data
	kvs.Index = index
	return nil
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

	index := make(map[string]int64)
	err := kvs.scan(func(pos int64, r *Record) bool {
		index[string(r.Key)] = pos
		return true
	})

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
	err := kvs.scan(func(pos int64, r *Record) bool {
		if r.Crc != r.calculateChecksum() {
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

	return kvs.scan(func(_ int64, r *Record) bool {
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
