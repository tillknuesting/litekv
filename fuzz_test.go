package litekv

import (
	"bytes"
	"testing"
)

// FuzzHint feeds arbitrary bytes to the hint parser, which reads a file this
// package wrote but has no reason to trust: a damaged disk, an older version,
// or something else entirely can be sitting where a hint should be.
//
// Refusing is always allowed. What is not allowed is accepting a hint and
// handing back an offset that is not in the log, since a hint is taken at its
// word and every read afterwards trusts it.
func FuzzHint(f *testing.F) {
	var index map[string]int64
	index = map[string]int64{"alpha": 0, "beta": 40}

	dir := f.TempDir()
	segment := dir + "/0000000001" + segmentSuffix
	if err := writeHint(segment, 1024, index); err != nil {
		f.Fatal(err)
	}
	written, err := disk.ReadFile(hintPath(segment))
	if err != nil {
		f.Fatal(err)
	}

	f.Add(written, int64(1024))
	f.Add(written, int64(0))
	f.Add([]byte{}, int64(0))
	f.Add([]byte("LKVH"), int64(10))
	f.Add(make([]byte, hintHeaderSize+4), int64(0))

	f.Fuzz(func(t *testing.T, data []byte, segmentSize int64) {
		got, ok := parseHint(data, segmentSize)
		if !ok {
			return
		}

		for key, pos := range got {
			if pos < 0 || pos+headerSizeV0 > segmentSize {
				t.Fatalf("accepted a hint putting %q at %d, outside a log of %d bytes", key, pos, segmentSize)
			}
		}
	})
}

// FuzzSegmentBytes feeds arbitrary bytes to the half of the package that reads
// a log without holding it in memory: the streaming indexer and the reader that
// fetches one record by offset.
//
// Nothing here may panic or hang, whatever the bytes claim their lengths are.
// What the indexer accepts, the reader has to agree with: an offset it indexed
// must decode, and the record there must hold the key it was indexed under.
func FuzzSegmentBytes(f *testing.F) {
	current := &KeyValueStore{}
	current.Write([]byte("alpha"), []byte("one"))
	current.Delete([]byte("alpha"))
	current.Write([]byte("beta"), []byte(""))
	f.Add(current.Data)

	var old []byte
	old = appendV0(old, RecordTypeNormal, []byte("old"), []byte("record"))
	f.Add(old)
	f.Add(append(append([]byte(nil), old...), current.Data...))

	f.Add([]byte{})
	f.Add(make([]byte, headerSizeV0))
	f.Add(make([]byte, headerSizeV1))

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bytes.NewReader(data)
		size := int64(len(data))

		index, good, err := indexSegment(reader, size)
		if err != nil {
			t.Fatalf("indexing bytes should refuse them, not fail: %v", err)
		}
		if good < 0 || good > size {
			t.Fatalf("indexed up to %d of %d bytes", good, size)
		}

		for key, pos := range index {
			record, raw, err := readRecordAt(reader, good, pos)
			if err != nil {
				t.Fatalf("indexed %q at %d and then could not read it: %v", key, pos, err)
			}
			if record.Crc != checksumSerialized(raw) {
				t.Fatalf("indexed %q at %d, where the record fails its checksum", key, pos)
			}
			if string(record.Key) != key {
				t.Fatalf("indexed %q at %d, where the record holds %q", key, pos, record.Key)
			}
		}

		// Walking the part that indexed cleanly must agree with it.
		seen := 0
		if err := scanSegment(reader, good, func(pos int64, raw []byte, r Record) bool {
			seen++
			return true
		}); err != nil {
			t.Fatalf("scanning the good part of the log failed: %v", err)
		}
		if seen < len(index) {
			t.Fatalf("the walk saw %d records for %d indexed keys", seen, len(index))
		}
	})
}
