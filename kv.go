package litekv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"sync"
)

// RecordType is a custom uint8 type that represents the type of a record.
// It is used to differentiate between normal and deleted records in the KeyValueStore.
type RecordType uint8

// Define constants for the different record types.
const (
	// RecordTypeNormal represents a normal record, which contains a key-value pair.
	RecordTypeNormal RecordType = iota

	// RecordTypeDeleted represents a deleted record, which is marked as deleted but not removed from the data slice.
	RecordTypeDeleted
)

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
	// does not match its stored checksum, indicating data corruption.
	ErrorChecksumMismatch = Error("checksum mismatch")
)

// KeyValueStore is a simple key-value store implementation.
// It utilizes a byte slice (data) to store serialized records and a map (index) to map keys to their position in the data byte slice.
// The KeyValueStore struct also embeds the sync.RWMutex to ensure thread safety during concurrent read and write operations.
type KeyValueStore struct {
	sync.RWMutex                  // Embed the RWMutex to ensure thread safety during concurrent read and write operations.
	data         []byte           // A byte slice that holds the serialized records.
	index        map[string]int64 // A map that maps keys (as strings) to their position in the data byte slice.
}

// Write takes a key and a value, both in byte slices, and stores them in the KeyValueStore instance.
// This method is responsible for creating a new Record with the given key-value pair and appending it
// to the data byte slice. It also updates the index map to map the key to the position of the new record.
func (kvs *KeyValueStore) Write(key, value []byte) {
	// First, create a new record with the given key and value.
	// Set the record type to "RecordTypeNormal" to indicate that it's a normal record, not a deleted one.
	record := &Record{
		Type:        RecordTypeNormal,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}

	// Calculate the CRC checksum of the record and store it in the record's Crc field.
	record.Crc = record.calculateChecksum()

	// Acquire a write lock on the KeyValueStore instance to ensure thread safety.
	kvs.Lock()
	// Defer the unlocking operation so that the lock is released after the method finishes.
	defer kvs.Unlock()

	// Check if the index map is initialized, and if not, initialize it.
	if kvs.index == nil {
		kvs.index = make(map[string]int64)
	}

	// Update the index map to associate the key with the current position in the data byte slice.
	kvs.index[string(key)] = int64(len(kvs.data))

	// Convert the record to a byte slice and append it to the data byte slice.
	kvs.data = append(kvs.data, record.toBytes()...)
}

// Read takes a key in the form of a byte slice and retrieves the associated value from the KeyValueStore instance.
// It returns the value as a byte slice, or an error if the key is not found, deleted, or there's a checksum mismatch.
func (kvs *KeyValueStore) Read(key []byte) ([]byte, error) {
	// Acquire a read lock on the KeyValueStore instance to ensure thread safety.
	kvs.RLock()
	// Defer the unlocking operation so that the lock is released after the method finishes.
	defer kvs.RUnlock()

	// Use the index map to find the position of the record associated with the given key.
	// If the key doesn't exist in the index map, return an ErrorKeyNotFound error.
	pos, exists := kvs.index[string(key)]
	if !exists {
		return nil, ErrorKeyNotFound
	}

	// Create a new bytes.Buffer with the data from the position found in the index map.
	buf := bytes.NewBuffer(kvs.data[pos:])

	// Initialize a new Record instance and deserialize the record data from the bytes.Buffer.
	record := new(Record)
	record.fromBytes(buf)

	// Verify the CRC checksum of the record. If the checksum doesn't match, return an ErrorChecksumMismatch error.
	if record.Crc != record.calculateChecksum() {
		return nil, ErrorChecksumMismatch
	}

	// Check the record type. If it's a normal record, return the value as a byte slice.
	// If the record type is deleted, return an ErrorKeyDeleted error.
	if record.Type == RecordTypeNormal {
		return record.Value, nil
	}
	return nil, ErrorKeyDeleted
}

// Delete takes a key in the form of a byte slice and marks the associated record as deleted in the KeyValueStore instance.
// It achieves this by creating a new Record with the RecordType set to RecordTypeDeleted and appending it to the data byte slice.
// It also updates the index map to map the key to the position of the new deleted record.
func (kvs *KeyValueStore) Delete(key []byte) {
	// Create a new Record with the given key and set the record type to "RecordTypeDeleted" to indicate that it's a deleted record.
	record := &Record{
		Type:      RecordTypeDeleted,
		Key:       key,
		KeyLength: uint32(len(key)),
	}

	// Calculate the CRC checksum of the record and store it in the record's Crc field.
	record.Crc = record.calculateChecksum()

	// Acquire a write lock on the KeyValueStore instance to ensure thread safety.
	kvs.Lock()
	// Defer the unlocking operation so that the lock is released after the method finishes.
	defer kvs.Unlock()

	// Update the index map to associate the key with the current position in the data byte slice.
	kvs.index[string(key)] = int64(len(kvs.data))

	// Convert the record to a byte slice and append it to the data byte slice.
	kvs.data = append(kvs.data, record.toBytes()...)
}

// calculateChecksum calculates the CRC checksum of a Record instance.
// It does this by serializing the record's fields, excluding the Crc field, into a bytes.Buffer,
// and then calculating the CRC-32 checksum of the buffer's contents using the IEEE polynomial.
// The resulting uint32 checksum value is returned by the method.
func (r *Record) calculateChecksum() uint32 {
	// Create a new bytes.Buffer to store the serialized record fields.
	buf := new(bytes.Buffer)

	// Write the record fields to the buffer in little-endian byte order, excluding the Crc field.
	// The fields written are: Type, KeyLength, ValueLength, Key, and Value.
	binary.Write(buf, binary.LittleEndian, r.Type)
	binary.Write(buf, binary.LittleEndian, r.KeyLength)
	binary.Write(buf, binary.LittleEndian, r.ValueLength)
	buf.Write(r.Key)
	buf.Write(r.Value)

	// Calculate the CRC-32 checksum of the buffer's contents using the IEEE polynomial
	// and return the resulting uint32 checksum value.
	return crc32.ChecksumIEEE(buf.Bytes())
}

// toBytes serializes a Record instance into a byte slice.
// It writes the record fields, including the Crc field, into a bytes.Buffer in little-endian byte order,
// and returns the buffer's contents as a byte slice.
func (r *Record) toBytes() []byte {
	// Create a new bytes.Buffer to store the serialized record fields.
	buf := new(bytes.Buffer)

	// Write the record fields to the buffer in little-endian byte order.
	// The fields written are: Crc, Type, KeyLength, ValueLength, Key, and Value.
	binary.Write(buf, binary.LittleEndian, r.Crc)
	binary.Write(buf, binary.LittleEndian, r.Type)
	binary.Write(buf, binary.LittleEndian, r.KeyLength)
	binary.Write(buf, binary.LittleEndian, r.ValueLength)
	buf.Write(r.Key)
	buf.Write(r.Value)

	// Return the buffer's contents as a byte slice.
	return buf.Bytes()
}

// fromBytes deserializes a byte slice into a Record instance.
// It takes a bytes.Buffer containing the serialized record data as an input,
// reads the record fields in little-endian byte order, and stores them in the corresponding fields of the Record instance.
func (r *Record) fromBytes(buf *bytes.Buffer) {
	// Read the record fields from the buffer in little-endian byte order.
	// The fields read are: Crc, Type, KeyLength, ValueLength.
	binary.Read(buf, binary.LittleEndian, &r.Crc)
	binary.Read(buf, binary.LittleEndian, &r.Type)
	binary.Read(buf, binary.LittleEndian, &r.KeyLength)
	binary.Read(buf, binary.LittleEndian, &r.ValueLength)

	// Allocate memory for the Key and Value fields based on their lengths,
	// then read the Key and Value data from the buffer into the allocated memory.
	r.Key = make([]byte, r.KeyLength)
	r.Value = make([]byte, r.ValueLength)
	buf.Read(r.Key)
	buf.Read(r.Value)
}

// SaveIndex serializes the KeyValueStore's index (a map of keys to their position in the data byte slice)
// using the gob package, and returns the serialized index as a byte slice.
// This method can be used to persist the index to disk or another storage medium, for later restoration.
func (kvs *KeyValueStore) SaveIndex() ([]byte, error) {
	kvs.Lock()
	defer kvs.Unlock()

	// Create a new bytes.Buffer to store the serialized index.
	var buf bytes.Buffer

	// Create a new gob.Encoder that writes to the buffer.
	encoder := gob.NewEncoder(&buf)

	// Encode the KeyValueStore's index using the gob.Encoder.
	err := encoder.Encode(kvs.index)

	// If there's an error during encoding, return nil and the error.
	if err != nil {
		return nil, err
	}

	// Return the buffer's contents as a byte slice, and no error.
	return buf.Bytes(), nil
}

// LoadIndex deserializes a byte slice containing a serialized index (a map of keys to their position in the data byte slice)
// using the gob package, and restores the deserialized index to the KeyValueStore.
// This method can be used to load a previously saved index from disk or another storage medium.
func (kvs *KeyValueStore) LoadIndex(data []byte) error {
	kvs.Lock()
	defer kvs.Unlock()

	// Create a new bytes.Buffer initialized with the input data.
	buf := bytes.NewBuffer(data)

	// Create a new gob.Decoder that reads from the buffer.
	decoder := gob.NewDecoder(buf)

	// Decode the serialized index using the gob.Decoder into the KeyValueStore's index.
	err := decoder.Decode(&kvs.index)

	// If there's an error during decoding, return the error.
	if err != nil {
		return err
	}

	// Return no error, indicating that the index was successfully loaded.
	return nil
}

// findLatestRecords iterates through the KeyValueStore's data byte slice and constructs a map of the latest records for each key.
// This method is used internally by the Compact() method to identify the latest records that need to be kept during compaction.
func (kvs *KeyValueStore) findLatestRecords() map[string]*Record {
	// Initialize an empty map to store the latest records for each key.
	latestRecords := make(map[string]*Record)

	// Create a new bytes.Buffer initialized with the KeyValueStore's data.
	buf := bytes.NewBuffer(kvs.data)

	// Iterate through the data byte slice as long as there are remaining bytes.
	for buf.Len() > 0 {
		// Create a new Record.
		record := new(Record)

		// Deserialize the record from the buffer.
		record.fromBytes(buf)

		// Check if the current record is of type RecordTypeNormal.
		if record.Type == RecordTypeNormal {
			// Convert the byte slice key to a string.
			key := string(record.Key)

			// Check if the key already exists in the latestRecords map.
			_, exists := latestRecords[key]

			// Update the latestRecords map if:
			// 1. The key doesn't exist yet.
			// 2. The current record is of type RecordTypeNormal.
			if !exists {
				latestRecords[key] = record
			}
		}
	}

	// Return the map of latest records.
	return latestRecords
}

// Compact iterates through the KeyValueStore's data byte slice, identifies the latest records for each key,
// and rebuilds the data slice and index, effectively removing any deleted or outdated records.
// This method is useful for reducing the storage size and improving the performance of the KeyValueStore.
func (kvs *KeyValueStore) Compact() {
	// Acquire a lock on the KeyValueStore to ensure thread safety.
	kvs.Lock()
	defer kvs.Unlock()

	// Find the latest records for each key using the findLatestRecords method.
	latestRecords := kvs.findLatestRecords()

	// Rebuild the data slice and index by initializing them as empty.
	kvs.data = make([]byte, 0)
	kvs.index = make(map[string]int64)

	// Iterate through the latest records.
	for key, record := range latestRecords {
		// Check if the record is of type RecordTypeNormal.
		if record.Type == RecordTypeNormal {
			// Calculate the new position for the key in the data slice.
			pos := int64(len(kvs.data))

			// Update the index with the new position.
			kvs.index[key] = pos

			// Append the record's serialized form to the data slice.
			kvs.data = append(kvs.data, record.toBytes()...)
		}
	}
}

// PrintAllKeyValuePairs iterates through the KeyValueStore's data byte slice, deserializes each record,
// and prints the key, value, and record type for each record.
// This method is useful for debugging and getting an overview of the KeyValueStore's contents.
func (kvs *KeyValueStore) PrintAllKeyValuePairs() {
	// Acquire a read lock on the KeyValueStore to ensure thread safety.
	kvs.RLock()
	defer kvs.RUnlock()

	// Create a new bytes.Buffer initialized with the KeyValueStore's data.
	buf := bytes.NewBuffer(kvs.data)

	// Iterate through the data byte slice as long as there are remaining bytes.
	for buf.Len() > 0 {
		// Create a new Record.
		record := new(Record)

		// Deserialize the record from the buffer.
		record.fromBytes(buf)

		// Print the key, value, and record type for the current record.
		fmt.Printf("Key: %s, Value: %s, Type: %b\n", record.Key, record.Value, record.Type)
	}
}

// RebuildIndex iterates through the KeyValueStore's data byte slice, deserializes each record,
// and rebuilds the index by calculating the position of each key in the data slice.
// This method is useful when the index has been lost or corrupted and needs to be reconstructed.
func (kvs *KeyValueStore) RebuildIndex() {
	// Acquire a lock on the KeyValueStore to ensure thread safety.
	kvs.Lock()
	defer kvs.Unlock()

	// Create a new bytes.Buffer initialized with the KeyValueStore's data.
	buf := bytes.NewBuffer(kvs.data)

	// Initialize a new empty index.
	kvs.index = make(map[string]int64)

	// Initialize a variable to track the position of the current record in the data byte slice.
	var pos int64

	// Iterate through the data byte slice as long as there are remaining bytes.
	for buf.Len() > 0 {
		// Create a new Record.
		record := new(Record)

		// Deserialize the record from the buffer.
		record.fromBytes(buf)

		// Convert the byte slice key to a string.
		key := string(record.Key)

		// Update the index with the key's position in the data byte slice.
		kvs.index[key] = pos

		// Calculate the size of the current record (Crc, Type, KeyLength, ValueLength, Key, and Value fields).
		recordSize := int64(4 + 1 + 4 + 4 + len(record.Key) + len(record.Value))

		// Update the position for the next record.
		pos += recordSize
	}
}
