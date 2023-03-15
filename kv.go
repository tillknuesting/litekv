package litekv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"sync"
)

const (
	RecordTypeNormal RecordType = iota
	RecordTypeDeleted
)

type RecordType uint8

// Record 1 Mio  = 25 MB on disk uncompressed nad 8 MB gzip compressed
type Record struct {
	Crc         uint32     // 4 bytes
	Type        RecordType // 1 byte
	Key         []byte     // variable length
	Value       []byte     // variable length
	KeyLength   uint32     // 4 bytes
	ValueLength uint32     // 4 bytes
}

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrorKeyDeleted       = Error("key is deleted")
	ErrorKeyNotFound      = Error("key not found")
	ErrorChecksumMismatch = Error("checksum mismatch")
)

type KeyValueStore struct {
	sync.RWMutex
	data  []byte
	index map[string]int64 // map keys to their position in the data byte slice
}

func (kvs *KeyValueStore) Write(key, value []byte) {
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

	// Update the index
	if kvs.index == nil {
		kvs.index = make(map[string]int64)
	}
	kvs.index[string(key)] = int64(len(kvs.data))

	kvs.data = append(kvs.data, record.toBytes()...)
}

func (kvs *KeyValueStore) Read(key []byte) ([]byte, error) {
	kvs.RLock()
	defer kvs.RUnlock()

	// Use the index for faster lookups
	pos, exists := kvs.index[string(key)]
	if !exists {
		return nil, ErrorKeyNotFound
	}

	buf := bytes.NewBuffer(kvs.data[pos:])

	record := new(Record)
	record.fromBytes(buf)

	// Verify checksum
	if record.Crc != record.calculateChecksum() {
		return nil, ErrorChecksumMismatch
	}

	if record.Type == RecordTypeNormal {
		return record.Value, nil
	}
	return nil, ErrorKeyDeleted
}

func (kvs *KeyValueStore) Delete(key []byte) {
	record := &Record{
		Type:      RecordTypeDeleted,
		Key:       key,
		KeyLength: uint32(len(key)),
	}

	record.Crc = record.calculateChecksum()

	kvs.Lock()
	defer kvs.Unlock()

	// Update the index
	kvs.index[string(key)] = int64(len(kvs.data))

	kvs.data = append(kvs.data, record.toBytes()...)
}

func (r *Record) calculateChecksum() uint32 {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, r.Type)
	binary.Write(buf, binary.LittleEndian, r.KeyLength)
	binary.Write(buf, binary.LittleEndian, r.ValueLength)
	buf.Write(r.Key)
	buf.Write(r.Value)
	return crc32.ChecksumIEEE(buf.Bytes())
}

func (r *Record) toBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, r.Crc)
	binary.Write(buf, binary.LittleEndian, r.Type)
	binary.Write(buf, binary.LittleEndian, r.KeyLength)
	binary.Write(buf, binary.LittleEndian, r.ValueLength)
	buf.Write(r.Key)
	buf.Write(r.Value)
	return buf.Bytes()
}

func (r *Record) fromBytes(buf *bytes.Buffer) {
	binary.Read(buf, binary.LittleEndian, &r.Crc)
	binary.Read(buf, binary.LittleEndian, &r.Type)
	binary.Read(buf, binary.LittleEndian, &r.KeyLength)
	binary.Read(buf, binary.LittleEndian, &r.ValueLength)
	r.Key = make([]byte, r.KeyLength)
	r.Value = make([]byte, r.ValueLength)
	buf.Read(r.Key)
	buf.Read(r.Value)
}

func (kvs *KeyValueStore) SaveIndex() ([]byte, error) {
	kvs.Lock()
	defer kvs.Unlock()

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(kvs.index)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (kvs *KeyValueStore) LoadIndex(data []byte) error {
	kvs.Lock()
	defer kvs.Unlock()

	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(&kvs.index)
	if err != nil {
		return err
	}

	return nil
}

func (kvs *KeyValueStore) findLatestRecords() map[string]*Record {
	latestRecords := make(map[string]*Record)
	buf := bytes.NewBuffer(kvs.data)

	for buf.Len() > 0 {
		record := new(Record)
		record.fromBytes(buf)

		if record.Type == RecordTypeNormal {
			key := string(record.Key)
			_, exists := latestRecords[key]

			// Update the latestRecords map if:
			// 1. The key doesn't exist yet.
			// 2. The current record is of type RecordTypeNormal.
			if !exists {
				latestRecords[key] = record
			}
		}
	}

	return latestRecords
}

func (kvs *KeyValueStore) Compact() {
	kvs.Lock()
	defer kvs.Unlock()

	// Find the latest records for each key
	latestRecords := kvs.findLatestRecords()

	// Rebuild the data slice and index
	kvs.data = make([]byte, 0)
	kvs.index = make(map[string]int64)

	for key, record := range latestRecords {
		if record.Type == RecordTypeNormal {
			pos := int64(len(kvs.data))
			kvs.index[key] = pos
			kvs.data = append(kvs.data, record.toBytes()...)
		}
	}
}

func (kvs *KeyValueStore) PrintAllKeyValuePairs() {
	kvs.RLock()
	defer kvs.RUnlock()

	buf := bytes.NewBuffer(kvs.data)

	for buf.Len() > 0 {
		record := new(Record)
		record.fromBytes(buf)

		fmt.Printf("Key: %s, Value: %s, Type: %b\n", record.Key, record.Value, record.Type)

	}
}

func (kvs *KeyValueStore) RebuildIndex() {
	kvs.Lock()
	defer kvs.Unlock()

	buf := bytes.NewBuffer(kvs.data)
	kvs.index = make(map[string]int64)

	var pos int64

	for buf.Len() > 0 {
		record := new(Record)
		record.fromBytes(buf)

		key := string(record.Key)
		kvs.index[key] = pos

		recordSize := int64(4 + 1 + 4 + 4 + len(record.Key) + len(record.Value)) // size of Crc, Type, KeyLength, ValueLength, Key, Value
		pos += recordSize
	}
}
