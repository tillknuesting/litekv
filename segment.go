package litekv

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
)

// A DB is made of segments of two kinds.
//
// The active one takes the writes and is a KeyValueStore: its records are in
// memory as well as in its file, because they are still being appended to.
//
// A frozen one holds nothing but its index. The records stay on the disk and
// are read back when a key asks for them, which is what keeps a store larger
// than memory workable: what has to fit is the keys, not the values. This is
// the arrangement Bitcask is built on, and the reason its own description says
// only that all the keys must fit in memory.
// readable is what a lookup needs of a segment, which is all the two kinds have
// in common: the active one answers from memory, a frozen one from its file.
type readable interface {
	// read returns the value stored under key.
	read(key []byte) ([]byte, error)

	// view calls fn with the value, which is only valid until fn returns.
	view(key []byte, fn func(value []byte) error) error

	// eachKey calls fn with every key this segment indexes and where its
	// newest record sits.
	eachKey(fn func(key string, pos int64) bool)

	// recordAt returns the record at pos along with the bytes it was decoded
	// from, so that a caller can check it.
	recordAt(pos int64) (Record, []byte, error)
}

// memSegment is the active segment: a store holding its records in memory and
// mirroring them to its file.
type memSegment struct {
	segID uint64
	kvs   *KeyValueStore
}

func (m *memSegment) size() int64  { return m.kvs.Size() }
func (m *memSegment) sync() error  { return m.kvs.Sync() }
func (m *memSegment) close() error { return m.kvs.Close() }

func (m *memSegment) read(key []byte) ([]byte, error) { return m.kvs.Read(key) }

func (m *memSegment) view(key []byte, fn func(value []byte) error) error {
	return m.kvs.View(key, fn)
}

func (m *memSegment) eachKey(fn func(key string, pos int64) bool) {
	m.kvs.RLock()
	defer m.kvs.RUnlock()

	for key, pos := range m.kvs.Index {
		if !fn(key, pos) {
			return
		}
	}
}

func (m *memSegment) recordAt(pos int64) (Record, []byte, error) {
	m.kvs.RLock()
	defer m.kvs.RUnlock()

	record, next, err := parseRecordAt(m.kvs.Data, pos)
	if err != nil {
		return Record{}, nil, err
	}
	return record, m.kvs.Data[pos:next], nil
}

// diskSegment is a frozen segment: an index in memory, records on the disk.
type diskSegment struct {
	segID uint64
	path  string
	file  *os.File
	index map[string]int64
	bytes int64
}

// openDiskSegment indexes the segment at path without holding its records in
// memory, and truncates a tail that a crash left half written.
//
// The index comes from the hint file beside it when there is a usable one,
// which is the difference between reading twenty bytes per key and reading the
// whole log. Otherwise the log is read and a hint written for next time.
func openDiskSegment(id uint64, path string) (*diskSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if index, ok := loadHint(path, info.Size()); ok {
		return &diskSegment{segID: id, path: path, file: file, index: index, bytes: info.Size()}, nil
	}

	index, good, err := indexSegment(file, info.Size())
	if err != nil {
		file.Close()
		return nil, err
	}

	if good < info.Size() {
		if err := file.Truncate(good); err != nil {
			file.Close()
			return nil, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, err
		}
	}

	// Reading the log the long way is worth writing down, so that opening it
	// again does not. A hint that cannot be written is not worth failing over.
	writeHint(path, good, index)

	return &diskSegment{segID: id, path: path, file: file, index: index, bytes: good}, nil
}

// freeze turns the active segment into a frozen one, letting go of the records
// it was holding in memory and keeping only the index it had already built.
func freeze(m *memSegment, policy SyncPolicy) (*diskSegment, error) {
	// Closing the store stops its own syncing, so anything the timer would
	// have got to has to be seen to here. SyncAlways has already done it, and
	// SyncNever asked for none.
	if policy == SyncEvery {
		if err := m.kvs.Sync(); err != nil {
			return nil, err
		}
	}

	m.kvs.RLock()
	index := m.kvs.Index
	size := int64(len(m.kvs.Data))
	path := ""
	if m.kvs.state != nil {
		path = m.kvs.state.path
	}
	m.kvs.RUnlock()

	if path == "" {
		return nil, errors.New("litekv: cannot freeze a segment with no file")
	}

	// Open the log to read from before letting go of the store that was
	// writing it. The other way round, a failure here would leave the store
	// closed and the segment it was the active half of unwritable.
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	// The records are on the disk, so the store and its Data slice can go.
	if err := m.kvs.closeNoSync(); err != nil {
		file.Close()
		return nil, err
	}

	// The index is already built, so writing it down here saves opening the
	// store from ever having to read this log.
	writeHint(path, size, index)

	return &diskSegment{segID: m.segID, path: path, file: file, index: index, bytes: size}, nil
}

func (d *diskSegment) id() uint64 { return d.segID }

// sync flushes a frozen log. It is never written, but it may never have been
// synced either: SyncNever means what it says, and the records of a log frozen
// under it are only as durable as the operating system has got round to making
// them.
func (d *diskSegment) sync() error { return d.file.Sync() }

func (d *diskSegment) close() error       { return d.file.Close() }
func (d *diskSegment) closeNoSync() error { return d.file.Close() }

func (d *diskSegment) eachKey(fn func(key string, pos int64) bool) {
	for key, pos := range d.index {
		if !fn(key, pos) {
			return
		}
	}
}

func (d *diskSegment) recordAt(pos int64) (Record, []byte, error) {
	return readRecordAt(d.file, d.bytes, pos)
}

func (d *diskSegment) read(key []byte) ([]byte, error) {
	pos, ok := d.index[string(key)]
	if !ok {
		return nil, ErrorKeyNotFound
	}

	record, raw, err := readRecordAt(d.file, d.bytes, pos)
	if err != nil {
		return nil, err
	}
	if record.Crc != checksumSerialized(raw) {
		return nil, ErrorChecksumMismatch
	}
	if !bytes.Equal(record.Key, key) {
		return nil, ErrorKeyMismatch
	}
	if record.Type != RecordTypeNormal {
		return nil, ErrorKeyDeleted
	}

	// The bytes were read into a buffer of this record's own, so there is
	// nothing to copy: no one else holds it.
	return record.Value, nil
}

func (d *diskSegment) view(key []byte, fn func(value []byte) error) error {
	value, err := d.read(key)
	if err != nil {
		return err
	}
	return fn(value)
}

func (d *diskSegment) scan(fn func(pos int64, raw []byte, r Record) bool) error {
	return scanSegment(d.file, d.bytes, func(pos int64, raw []byte, r Record) bool {
		return fn(pos, raw, r)
	})
}

// readRecordAt reads one record from a file. The lengths it declares are
// checked against what is actually there, so a damaged file gives an error
// rather than an enormous allocation.
func readRecordAt(file io.ReaderAt, size, pos int64) (Record, []byte, error) {
	if pos < 0 || pos > size || size-pos < headerSizeV0 {
		return Record{}, nil, &CorruptAtError{Offset: pos}
	}

	// Read as much header as there could be, which for a short record at the
	// end of the log is less than the largest layout takes.
	probe := int64(headerSize)
	if remaining := size - pos; remaining < probe {
		probe = remaining
	}

	header := make([]byte, probe)
	if _, err := file.ReadAt(header, pos); err != nil {
		return Record{}, nil, err
	}

	fixed, ok := decodeHeader(header)
	if !ok {
		return Record{}, nil, &CorruptAtError{Offset: pos}
	}
	headerLen, keyLen, valueLen := fixed.size, fixed.keyLength, fixed.valueLength

	total := uint64(headerLen) + uint64(keyLen) + uint64(valueLen)
	if total > uint64(size-pos) {
		return Record{}, nil, &CorruptAtError{Offset: pos}
	}

	raw := make([]byte, total)
	copy(raw, header[:headerLen])
	if _, err := file.ReadAt(raw[headerLen:], pos+headerLen); err != nil {
		return Record{}, nil, err
	}

	record, _, err := parseRecordAt(raw, 0)
	if err != nil {
		return Record{}, nil, err
	}
	return record, raw, nil
}

// scanSegment walks the records of a file in order, reading it through a buffer
// rather than holding it all at once.
func scanSegment(file io.ReaderAt, size int64, fn func(pos int64, raw []byte, r Record) bool) error {
	reader := bufio.NewReaderSize(io.NewSectionReader(file, 0, size), 64<<10)

	buf := make([]byte, 0, 1<<12)
	var pos int64

	for pos < size {
		if size-pos < headerSizeV0 {
			return &CorruptAtError{Offset: pos}
		}

		// The first layout's header is the shorter one, so read that much and
		// then find out which layout this is.
		buf = buf[:headerSizeV0]
		if _, err := io.ReadFull(reader, buf); err != nil {
			return &CorruptAtError{Offset: pos}
		}

		headerLen, known := headerSizeFor(buf[4])
		if !known || size-pos < headerLen {
			return &CorruptAtError{Offset: pos}
		}
		if headerLen > headerSizeV0 {
			buf = buf[:headerLen]
			if _, err := io.ReadFull(reader, buf[headerSizeV0:]); err != nil {
				return &CorruptAtError{Offset: pos}
			}
		}

		fixed, ok := decodeHeader(buf)
		if !ok {
			return &CorruptAtError{Offset: pos}
		}
		keyLen, valueLen := fixed.keyLength, fixed.valueLength

		total := uint64(headerLen) + uint64(keyLen) + uint64(valueLen)
		if total > uint64(size-pos) {
			return &CorruptAtError{Offset: pos}
		}

		if uint64(cap(buf)) < total {
			// The header has already been taken from the reader, so put it
			// into the new buffer straight from the file.
			buf = make([]byte, total)
			if _, err := file.ReadAt(buf[:headerLen], pos); err != nil {
				return err
			}
		}
		buf = buf[:total]

		if _, err := io.ReadFull(reader, buf[headerLen:]); err != nil {
			return &CorruptAtError{Offset: pos}
		}

		record, _, err := parseRecordAt(buf, 0)
		if err != nil {
			return err
		}

		if !fn(pos, buf, record) {
			return nil
		}

		pos += int64(total)
	}

	return nil
}

// indexSegment builds the index of a segment file, checking every record
// against its checksum, and reports where the good part of it ends.
func indexSegment(file io.ReaderAt, size int64) (map[string]int64, int64, error) {
	index := make(map[string]int64)

	var good int64
	err := scanSegment(file, size, func(pos int64, raw []byte, r Record) bool {
		if r.Crc != checksumSerialized(raw) {
			return false
		}
		index[string(r.Key)] = pos
		good = pos + int64(len(raw))
		return true
	})

	// A record that will not decode ends the good part of the log, exactly as
	// one that fails its checksum does.
	var corrupt *CorruptAtError
	if err != nil && !errors.As(err, &corrupt) {
		return nil, 0, err
	}

	return index, good, nil
}
