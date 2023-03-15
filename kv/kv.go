package kv

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sync"
)

const (
	RecordTypeNormal RecordType = iota
	RecordTypeDeleted
)

type RecordType uint8

// Record 1 Mio  = 25 MB on disk, 8 MB gzip compressed
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
	data []byte
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

	kvs.data = append(kvs.data, record.toBytes()...)
}

func (kvs *KeyValueStore) Read(key []byte) ([]byte, error) {
	kvs.RLock()
	defer kvs.RUnlock()

	buf := bytes.NewBuffer(kvs.data)

	var pos int64
	records := make([]*Record, 0)

	for buf.Len() > 0 {
		record := new(Record)
		record.fromBytes(buf)

		// Verify checksum
		if record.Crc != record.calculateChecksum() {
			return nil, ErrorChecksumMismatch
		}

		records = append(records, record)
		pos += int64(4 + 1 + 4 + 4 + len(record.Key) + len(record.Value)) // size of Crc, Type, KeyLength, ValueLength, Key, Value
	}

	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if bytes.Equal(record.Key, key) {
			if record.Type == RecordTypeNormal {
				return record.Value, nil
			}
			return nil, ErrorKeyDeleted
		}
	}

	return nil, ErrorKeyNotFound
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
