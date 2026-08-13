package litekv

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyValueStore_Write(t *testing.T) {
	type writeTest struct {
		key         []byte
		value       []byte
		expectedErr error
		readKey     []byte
		readValue   []byte
	}

	tests := []writeTest{
		{
			key:         []byte("foo"),
			value:       []byte("bar"),
			expectedErr: nil,
			readKey:     []byte("foo"),
			readValue:   []byte("bar"),
		},
		{
			key:         []byte("hello"),
			value:       []byte("world"),
			expectedErr: ErrorKeyNotFound,
			readKey:     []byte("incorrect"),
			readValue:   nil,
		},
	}

	for i, test := range tests {
		kvs := &KeyValueStore{}

		kvs.Write(test.key, test.value)
		value, err := kvs.Read(test.readKey)

		if !errors.Is(err, test.expectedErr) {
			t.Errorf("test %d: expected error '%v', got '%v'", i, test.expectedErr, err)
		}

		if test.readValue != nil && value != nil && string(value) != string(test.readValue) {
			t.Errorf("test %d: expected value '%s', got '%s'", i, string(test.readValue), string(value))
		}
	}
}

func TestKeyValueStore_Read(t *testing.T) {
	type readTest struct {
		writeKey    []byte
		writeValue  []byte
		modifyData  bool
		readKey     []byte
		expectedErr error
		readValue   []byte
	}

	tests := []readTest{
		{
			writeKey:    []byte("foo"),
			writeValue:  []byte("bar"),
			modifyData:  false,
			readKey:     []byte("foo"),
			expectedErr: nil,
			readValue:   []byte("bar"),
		},
		{
			writeKey:    []byte("hello"),
			writeValue:  []byte("world"),
			modifyData:  false,
			readKey:     []byte("nonexistent"),
			expectedErr: ErrorKeyNotFound,
			readValue:   nil,
		},
		{
			writeKey:    []byte("test"),
			writeValue:  []byte("checksum"),
			modifyData:  true,
			readKey:     []byte("test"),
			expectedErr: ErrorChecksumMismatch,
			readValue:   nil,
		},
	}

	for i, test := range tests {
		kvs := &KeyValueStore{}

		kvs.Write(test.writeKey, test.writeValue)

		// Modify Data to create a checksum mismatch
		if test.modifyData {
			kvs.Data[0]++
		}

		value, err := kvs.Read(test.readKey)

		if !errors.Is(err, test.expectedErr) {
			t.Errorf("test %d: expected error '%v', got '%v'", i, test.expectedErr, err)
		}

		if test.readValue != nil && value != nil && string(value) != string(test.readValue) {
			t.Errorf("test %d: expected value '%s', got '%s'", i, string(test.readValue), string(value))
		}
	}
}

func TestKeyValueStore_Delete(t *testing.T) {
	type deleteTest struct {
		writeKey    []byte
		writeValue  []byte
		deleteKey   []byte
		readKey     []byte
		expectedErr error
		readValue   []byte
	}

	tests := []deleteTest{
		{
			writeKey:    []byte("foo"),
			writeValue:  []byte("bar"),
			deleteKey:   []byte("foo"),
			readKey:     []byte("foo"),
			expectedErr: ErrorKeyDeleted,
			readValue:   nil,
		},
		{
			writeKey:    []byte("hello"),
			writeValue:  []byte("world"),
			deleteKey:   []byte("nonexistent"),
			readKey:     []byte("hello"),
			expectedErr: nil,
			readValue:   []byte("world"),
		},
	}

	for i, test := range tests {
		kvs := &KeyValueStore{}

		kvs.Write(test.writeKey, test.writeValue)
		kvs.Delete(test.deleteKey)
		value, err := kvs.Read(test.readKey)

		if !errors.Is(err, test.expectedErr) {
			t.Errorf("test %d: expected error '%v', got '%v'", i, test.expectedErr, err)
		}

		if test.readValue != nil && value != nil && string(value) != string(test.readValue) {
			t.Errorf("test %d: expected value '%s', got '%s'", i, string(test.readValue), string(value))
		}
	}
}

func TestKeyValueStore_Compact(t *testing.T) {
	tests := []struct {
		name  string
		setup func(kvs *KeyValueStore)
		want  map[string]string // key -> value, "" means the key must be gone
	}{
		{
			name: "drops deleted keys",
			setup: func(kvs *KeyValueStore) {
				kvs.Write([]byte("foo"), []byte("bar"))
				kvs.Write([]byte("foo2"), []byte("bar2"))
				kvs.Delete([]byte("foo"))
			},
			want: map[string]string{"foo": "", "foo2": "bar2"},
		},
		{
			name: "keeps the newest value of an updated key",
			setup: func(kvs *KeyValueStore) {
				kvs.Write([]byte("k"), []byte("v1"))
				kvs.Write([]byte("k"), []byte("v2"))
				kvs.Write([]byte("k"), []byte("v3"))
			},
			want: map[string]string{"k": "v3"},
		},
		{
			name: "keeps a key written again after a delete",
			setup: func(kvs *KeyValueStore) {
				kvs.Write([]byte("k"), []byte("v1"))
				kvs.Delete([]byte("k"))
				kvs.Write([]byte("k"), []byte("v2"))
			},
			want: map[string]string{"k": "v2"},
		},
		{
			name: "drops a key deleted after being rewritten",
			setup: func(kvs *KeyValueStore) {
				kvs.Write([]byte("k"), []byte("v1"))
				kvs.Write([]byte("k"), []byte("v2"))
				kvs.Delete([]byte("k"))
			},
			want: map[string]string{"k": ""},
		},
		{
			name:  "empty store",
			setup: func(kvs *KeyValueStore) {},
			want:  map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kvs := &KeyValueStore{}
			test.setup(kvs)

			before := make(map[string]string, len(test.want))
			for key := range test.want {
				value, err := kvs.Read([]byte(key))
				if err == nil {
					before[key] = string(value)
				}
			}

			if err := kvs.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}

			live := 0
			for key, want := range test.want {
				value, err := kvs.Read([]byte(key))
				switch {
				case want == "" && !errors.Is(err, ErrorKeyNotFound):
					t.Errorf("key %q: expected it to be gone, got '%s' (err %v)", key, value, err)
				case want != "" && err != nil:
					t.Errorf("key %q: unexpected error %v", key, err)
				case want != "" && string(value) != want:
					t.Errorf("key %q: expected '%s', got '%s'", key, want, value)
				}
				if want != "" {
					live++
				}
			}

			// Compaction must not change what the store answers.
			for key, want := range before {
				value, err := kvs.Read([]byte(key))
				if err != nil || string(value) != want {
					t.Errorf("key %q: compaction changed the answer from '%s' to '%s' (err %v)", key, want, value, err)
				}
			}

			if len(kvs.Index) != live {
				t.Errorf("expected %d indexed keys after compaction, got %d", live, len(kvs.Index))
			}

			// Compaction is deterministic and idempotent.
			compacted := string(kvs.Data)
			if err := kvs.Compact(); err != nil {
				t.Fatalf("second Compact: %v", err)
			}
			if string(kvs.Data) != compacted {
				t.Errorf("compacting twice changed the Data slice")
			}
		})
	}
}

func TestKeyValueStore_CompactPreservesOrder(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("1"))
	kvs.Write([]byte("b"), []byte("2"))
	kvs.Write([]byte("c"), []byte("3"))
	kvs.Delete([]byte("b"))

	if err := kvs.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	var keys []string
	err := kvs.ForEach(func(key, value []byte, deleted bool) bool {
		keys = append(keys, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ForEach: %v", err)
	}

	if strings.Join(keys, ",") != "a,c" {
		t.Errorf("expected records in write order [a c], got %v", keys)
	}
}

func TestKeyValueStore_RebuildIndex(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("foo2"), []byte("bar2"))
	kvs.Write([]byte("foo3"), []byte("bar3"))
	kvs.Write([]byte("foo"), []byte("updated"))
	kvs.Delete([]byte("foo3"))

	before := make(map[string]int64, len(kvs.Index))
	maps.Copy(before, kvs.Index)

	kvs.Index = nil
	if err := kvs.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if len(kvs.Index) != len(before) {
		t.Errorf("rebuilt %d keys, want %d", len(kvs.Index), len(before))
	}
	for key, want := range before {
		got, ok := kvs.Index[key]
		if !ok {
			t.Errorf("key %q is missing after the rebuild", key)
		} else if got != want {
			t.Errorf("key %q: offset %d after the rebuild, was %d", key, got, want)
		}
	}

	// Every key has to point at its newest record, tombstones included.
	value, err := kvs.Read([]byte("foo"))
	if err != nil || string(value) != "updated" {
		t.Errorf("foo: got '%s' (%v), want 'updated'", value, err)
	}
	if _, err := kvs.Read([]byte("foo3")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("foo3: expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
}

// TestBinaryFormat pins the layout of a record. The checksum and the timestamp
// are blanked first, since they differ from one run to the next; what is left
// is the shape: version, type, lengths, key, value.
func TestBinaryFormat(t *testing.T) {
	const golden = "00000000020000000000000000000300000003000000666f6f626172" +
		"000000000200000000000000000001000000000000006b" +
		"00000000020100000000000000000300000000000000666f6f"

	kvs := &KeyValueStore{}
	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("k"), []byte(""))
	kvs.Delete([]byte("foo"))

	data := append([]byte(nil), kvs.Data...)
	kvs.scan(func(pos, next int64, r Record) bool {
		for i := pos; i < pos+4; i++ {
			data[i] = 0 // checksum
		}
		for i := pos + 6; i < pos+14; i++ {
			data[i] = 0 // timestamp
		}
		return true
	})

	if got := hex.EncodeToString(data); got != golden {
		t.Errorf("record layout changed:\n got %s\nwant %s", got, golden)
	}
}

// TestReadsTheOldFormat is the promise that adding a version and a timestamp
// did not orphan what was already written. These bytes are a store from before
// either existed, where the byte after the checksum was the record type.
func TestReadsTheOldFormat(t *testing.T) {
	const old = "0923b16f000300000003000000666f6f626172" +
		"e5c4912e0001000000000000006b" +
		"3250a00a010300000000000000666f6f"

	data, err := hex.DecodeString(old)
	if err != nil {
		t.Fatal(err)
	}

	kvs := &KeyValueStore{Data: data}

	discarded, err := kvs.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if discarded != 0 {
		t.Errorf("Recover discarded %d bytes of an intact old store", discarded)
	}
	if err := kvs.Verify(); err != nil {
		t.Errorf("Verify: %v", err)
	}

	if value, err := kvs.Read([]byte("k")); err != nil || len(value) != 0 {
		t.Errorf("k: got '%s' (%v), want an empty value", value, err)
	}
	if _, err := kvs.Read([]byte("foo")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("foo: expected '%v', got '%v'", ErrorKeyDeleted, err)
	}

	// Old records carry no timestamp, and say so rather than claiming one.
	kvs.scan(func(pos, next int64, r Record) bool {
		if r.Version != recordV0 {
			t.Errorf("record at %d reports version %d, want %d", pos, r.Version, recordV0)
		}
		if !r.Written().IsZero() {
			t.Errorf("record at %d claims to have been written at %v", pos, r.Written())
		}
		return true
	})

	// The store goes on working: new records are written in the new layout
	// beside the old ones, and compaction keeps both readable.
	if err := kvs.Write([]byte("new"), []byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := kvs.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := kvs.Verify(); err != nil {
		t.Errorf("Verify after compaction: %v", err)
	}
	for key, want := range map[string]string{"k": "", "new": "record"} {
		if value, err := kvs.Read([]byte(key)); err != nil || string(value) != want {
			t.Errorf("%s: got '%s' (%v), want '%s'", key, value, err, want)
		}
	}
}

// TestTimestamps checks that a record remembers when it was written.
func TestTimestamps(t *testing.T) {
	kvs := &KeyValueStore{}

	before := time.Now()
	if err := kvs.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	after := time.Now()

	written, err := kvs.Modified([]byte("k"))
	if err != nil {
		t.Fatalf("Modified: %v", err)
	}
	if written.Before(before) || written.After(after) {
		t.Errorf("written at %v, expected between %v and %v", written, before, after)
	}

	time.Sleep(2 * time.Millisecond)
	if err := kvs.Write([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	again, err := kvs.Modified([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !again.After(written) {
		t.Errorf("rewriting left the time at %v, was %v", again, written)
	}

	// A deleted key has a time of its own: when it was deleted.
	if err := kvs.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, err := kvs.Modified([]byte("k")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
	if _, err := kvs.Modified([]byte("missing")); !errors.Is(err, ErrorKeyNotFound) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyNotFound, err)
	}
}

func TestKeyValueStore_DeleteOnEmptyStore(t *testing.T) {
	kvs := &KeyValueStore{}

	if err := kvs.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete on an empty store: %v", err)
	}

	if _, err := kvs.Read([]byte("missing")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
}

func TestKeyValueStore_ReadWithBadIndex(t *testing.T) {
	tests := []struct {
		name        string
		corrupt     func(kvs *KeyValueStore)
		expectedErr error
	}{
		{
			name:        "offset past the end of Data",
			corrupt:     func(kvs *KeyValueStore) { kvs.Index["a"] = int64(len(kvs.Data)) + 100 },
			expectedErr: ErrorCorruptData,
		},
		{
			name:        "negative offset",
			corrupt:     func(kvs *KeyValueStore) { kvs.Index["a"] = -1 },
			expectedErr: ErrorCorruptData,
		},
		{
			name:        "offset in the middle of a record",
			corrupt:     func(kvs *KeyValueStore) { kvs.Index["a"] = 7 },
			expectedErr: ErrorCorruptData,
		},
		{
			name:        "offset of another key's record",
			corrupt:     func(kvs *KeyValueStore) { kvs.Index["a"] = kvs.Index["b"] },
			expectedErr: ErrorKeyMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kvs := &KeyValueStore{}
			kvs.Write([]byte("a"), []byte("AAA"))
			kvs.Write([]byte("b"), []byte("BBB"))
			test.corrupt(kvs)

			value, err := kvs.Read([]byte("a"))
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected '%v', got '%v' (value '%s')", test.expectedErr, err, value)
			}
			if value != nil {
				t.Errorf("expected no value, got '%s'", value)
			}
		})
	}
}

// TestKeyValueStore_CorruptLengthHeader guards against a corrupt length field
// being trusted as an allocation size.
func TestKeyValueStore_CorruptLengthHeader(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("k"), []byte("v"))

	// Bytes 18..22 hold ValueLength. Claim the maximum.
	binary.LittleEndian.PutUint32(kvs.Data[18:22], math.MaxUint32)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	if _, err := kvs.Read([]byte("k")); !errors.Is(err, ErrorCorruptData) {
		t.Errorf("expected '%v', got '%v'", ErrorCorruptData, err)
	}

	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Errorf("reading a 14 byte store allocated %d bytes", allocated)
	}

	if err := kvs.RebuildIndex(); !errors.Is(err, ErrorCorruptData) {
		t.Errorf("RebuildIndex: expected '%v', got '%v'", ErrorCorruptData, err)
	}
	if err := kvs.Compact(); !errors.Is(err, ErrorCorruptData) {
		t.Errorf("Compact: expected '%v', got '%v'", ErrorCorruptData, err)
	}
}

func TestKeyValueStore_LoadIndex(t *testing.T) {
	src := &KeyValueStore{}
	src.Write([]byte("a"), []byte("0123456789"))
	src.Write([]byte("b"), []byte("0123456789"))

	exported, err := src.SaveIndex()
	if err != nil {
		t.Fatalf("SaveIndex: %v", err)
	}

	t.Run("round trip", func(t *testing.T) {
		dst := &KeyValueStore{Data: src.Data}
		if err := dst.LoadIndex(exported); err != nil {
			t.Fatalf("LoadIndex: %v", err)
		}
		value, err := dst.Read([]byte("b"))
		if err != nil || string(value) != "0123456789" {
			t.Errorf("expected '0123456789', got '%s' (err %v)", value, err)
		}
	})

	t.Run("rejects an index that does not match Data", func(t *testing.T) {
		dst := &KeyValueStore{}
		dst.Write([]byte("a"), []byte("x")) // shorter Data, different layout

		if err := dst.LoadIndex(exported); err == nil {
			t.Fatal("expected an error for a foreign index")
		}
		if _, ok := dst.Index["b"]; ok {
			t.Error("a rejected index was installed anyway")
		}
	})

	t.Run("replaces rather than merges", func(t *testing.T) {
		dst := &KeyValueStore{Data: src.Data}
		dst.Index = map[string]int64{"stale": 0}

		if err := dst.LoadIndex(exported); err != nil {
			t.Fatalf("LoadIndex: %v", err)
		}
		if _, ok := dst.Index["stale"]; ok {
			t.Error("stale key survived LoadIndex")
		}
	})
}

func TestKeyValueStore_ReadReturnsACopy(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("k"), []byte("value"))

	value, err := kvs.Read([]byte("k"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	value[0] = 'X'

	again, err := kvs.Read([]byte("k"))
	if err != nil || string(again) != "value" {
		t.Errorf("mutating a returned value changed the store: got '%s' (err %v)", again, err)
	}
}

// TestKeyValueStore_RebuildIndexTornTail simulates a crash part way through an
// append: everything before the damaged record must still be recoverable.
func TestKeyValueStore_RebuildIndexTornTail(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("AAA"))
	kvs.Write([]byte("b"), []byte("BBB"))

	torn := int64(len(kvs.Data))
	kvs.Write([]byte("c"), []byte("CCC"))
	kvs.Data = kvs.Data[:len(kvs.Data)-2]

	err := kvs.RebuildIndex()

	var corrupt *CorruptAtError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected a *CorruptAtError, got '%v'", err)
	}
	if corrupt.Offset != torn {
		t.Errorf("expected the damage reported at offset %d, got %d", torn, corrupt.Offset)
	}

	value, err := kvs.Read([]byte("a"))
	if err != nil || string(value) != "AAA" {
		t.Errorf("expected 'AAA', got '%s' (err %v)", value, err)
	}

	// Dropping the damaged tail leaves a clean store.
	kvs.Data = kvs.Data[:corrupt.Offset]
	if err := kvs.RebuildIndex(); err != nil {
		t.Errorf("RebuildIndex after truncation: %v", err)
	}
	if err := kvs.Verify(); err != nil {
		t.Errorf("Verify after truncation: %v", err)
	}
}

func TestKeyValueStore_Verify(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("AAA"))
	kvs.Write([]byte("b"), []byte("BBB"))

	if err := kvs.Verify(); err != nil {
		t.Fatalf("Verify on an intact store: %v", err)
	}

	kvs.Data[len(kvs.Data)-1]++
	if err := kvs.Verify(); !errors.Is(err, ErrorChecksumMismatch) {
		t.Errorf("expected '%v', got '%v'", ErrorChecksumMismatch, err)
	}
}

func TestKeyValueStore_Concurrent(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("seed"), []byte("seed"))

	// Enough distinct keys that every lock shard is exercised, and a mix of the
	// paths that take a shard, take them all, or walk the whole store.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "key%d", i)
			for j := range 100 {
				kvs.Write(key, fmt.Appendf(nil, "value%d", j))
				kvs.Read(key)
				kvs.Read([]byte("seed"))
				kvs.View([]byte("seed"), func(value []byte) error {
					if string(value) != "seed" {
						t.Errorf("View saw '%s'", value)
					}
					return nil
				})
				if j%25 == 0 {
					kvs.Delete(key)
					kvs.ForEach(func(key, value []byte, deleted bool) bool { return true })
				}
			}
		}(i)
	}

	// A reader using the exported lock directly, as the docs describe.
	wg.Go(func() {
		for range 200 {
			kvs.RLock()
			_ = len(kvs.Index)
			_ = len(kvs.Data)
			kvs.RUnlock()
		}
	})

	wg.Wait()

	if err := kvs.Verify(); err != nil {
		t.Errorf("Verify: %v", err)
	}
	if err := kvs.Compact(); err != nil {
		t.Errorf("Compact: %v", err)
	}
}

func FuzzKeyValueStore_WriteReadDelete(f *testing.F) {
	kvs := &KeyValueStore{}
	f.Fuzz(func(t *testing.T, a []byte, b []byte) {
		kvs.Write(a, b)
		kvs.Read(a)
		kvs.Delete(a)
	})
}

// FuzzKeyValueStore_Data feeds arbitrary bytes in through the Data slice, which
// is how the store is meant to be backed by a file or shared memory. No input
// may panic or hang, whatever it claims its record lengths to be.
func FuzzKeyValueStore_Data(f *testing.F) {
	seed := &KeyValueStore{}
	seed.Write([]byte("a"), []byte("AAA"))
	seed.Delete([]byte("a"))
	seed.Write([]byte("b"), []byte(""))
	f.Add(seed.Data)
	f.Add([]byte{})
	f.Add(make([]byte, headerSizeV1))
	f.Add(make([]byte, headerSizeV2))

	f.Fuzz(func(t *testing.T, data []byte) {
		kvs := &KeyValueStore{Data: data}

		kvs.RebuildIndex()
		kvs.Verify()
		kvs.ForEach(func(key, value []byte, deleted bool) bool { return true })

		// Whatever the store answers before compaction it must still answer after.
		live := make(map[string]string)
		for key := range kvs.Index {
			if value, err := kvs.Read([]byte(key)); err == nil {
				live[key] = string(value)
			}
		}

		if err := kvs.Compact(); err != nil {
			return
		}

		for key, want := range live {
			got, err := kvs.Read([]byte(key))
			if err != nil || string(got) != want {
				t.Fatalf("compaction lost %q: was '%s', now '%s' (err %v)", key, want, got, err)
			}
		}
		if err := kvs.RebuildIndex(); err != nil {
			t.Fatalf("compacted store fails RebuildIndex: %v", err)
		}
	})
}

func TestKeyValueStore_View(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("k"), []byte("value"))
	kvs.Write([]byte("gone"), []byte("x"))
	kvs.Delete([]byte("gone"))

	// Where the value sits, asked of the record rather than worked out from a
	// header size. This test used to add headerSize to the offset, which was
	// right until a second layout existed and headerSize became the largest of
	// them rather than the one a plain Write uses. That is the hard-coded
	// offset trap in AGENTS.md, sprung a third time.
	stored, _, err := parseRecordAt(kvs.Data, kvs.Index["k"])
	if err != nil {
		t.Fatal(err)
	}

	var seen string
	if err := kvs.View([]byte("k"), func(value []byte) error {
		seen = string(value)
		// The value must be the stored bytes, not a copy of them.
		if &value[0] != &stored.Value[0] {
			t.Error("View copied the value")
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	if seen != "value" {
		t.Errorf("expected 'value', got '%s'", seen)
	}

	called := false
	err = kvs.View([]byte("gone"), func(value []byte) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
	if called {
		t.Error("View called fn for a deleted key")
	}

	sentinel := errors.New("sentinel")
	if err := kvs.View([]byte("k"), func(value []byte) error { return sentinel }); err != sentinel {
		t.Errorf("expected the error from fn, got '%v'", err)
	}
}

// Benchmarks keep the store bounded: appending gigabyte values in a b.N loop
// measures the allocator, and on a small machine it simply runs out of memory.

func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key:%016d", i)
	}
	return keys
}

// BenchmarkKeyValueStore_WriteUpdate rewrites the same key over and over: the update path.
func BenchmarkKeyValueStore_WriteUpdate(b *testing.B) {
	for _, size := range []int{16, 1024, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			key := []byte("key:0000000000000000")
			value := make([]byte, size)
			kvs := &KeyValueStore{}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				kvs.Write(key, value)
				if len(kvs.Data) > 1<<26 {
					kvs.Data = kvs.Data[:0]
				}
			}
		})
	}
}

// BenchmarkKeyValueStore_WriteInsert writes a fresh key every time: the insert path.
func BenchmarkKeyValueStore_WriteInsert(b *testing.B) {
	for _, size := range []int{16, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			keys := makeKeys(4096)
			value := make([]byte, size)
			kvs := &KeyValueStore{}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				kvs.Write(keys[i%len(keys)], value)
				if len(kvs.Data) > 1<<26 {
					kvs.Data = kvs.Data[:0]
					kvs.Index = nil
				}
			}
		})
	}
}

// writeBoundBytes is the size at which a concurrent write benchmark starts its
// log over, and writeBoundChecks how many writes a goroutine makes between
// looking. Left alone, a run of these appends for its whole duration — several
// gigabytes a second across eight goroutines — and what gets measured is append
// growing a slice. Checking rarely keeps the extra lock acquisition down to
// about a hundredth of a percent of them.
const (
	writeBoundBytes  = 1 << 26 // 64 MiB, as the serial write benchmarks use
	writeBoundChecks = 8192
)

// BenchmarkKeyValueStore_WriteParallel is the other half of the lock.
//
// A read takes one shard, so reads of different keys run beside each other and
// scale; ReadScaleParallel is that. A write takes every shard, so writes
// exclude each other and a second writer has nothing to do but wait. This puts
// a number on that, which until now was read off the lock rather than measured.
//
// What to look for is not whether it gets faster, which it cannot: b.N is split
// between the writers, so ns/op is the cost of one write however many are
// running, and perfect serialisation would hold it flat while each waits its
// turn. Anything above flat is the handover — four mutexes moving between cores
// — and that is paid on top of the serialisation, not instead of it.
//
// The single-writer case is the baseline and should land on WriteUpdate/16.
func BenchmarkKeyValueStore_WriteParallel(b *testing.B) {
	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			// A key each, so what is contended is the lock and not one bucket
			// of the map.
			keys := makeKeys(writers)
			value := make([]byte, 16)
			kvs := &KeyValueStore{}

			var wg sync.WaitGroup
			b.ReportAllocs()
			b.ResetTimer()

			for w := range writers {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()

					written := 0
					for i := w; i < b.N; i += writers {
						if err := kvs.Write(keys[w], value); err != nil {
							// Not Fatal: this is not the goroutine that ran the
							// benchmark, and only that one may call it.
							b.Error(err)
							return
						}

						// Under the same lock a write takes, so a reset can
						// never land between a record and the index entry
						// pointing at it.
						if written++; written%writeBoundChecks == 0 {
							kvs.Lock()
							if len(kvs.Data) > writeBoundBytes {
								kvs.Data = kvs.Data[:0]
								clear(kvs.Index)
							}
							kvs.Unlock()
						}
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()
			reportThroughput(b, len(value))
		})
	}
}

// scaleKeyLen is the width of a key from makeKeys, which is what a record costs
// before its value.
const scaleKeyLen = len("key:0000000000000000")

// scaleMaxBytes bounds a store built by the scaling benchmarks. The whole cross
// product is not affordable in memory — a million 16 KiB values is 17 GiB — so
// a size past this is skipped, which is what leaves the staircase ragged at the
// large end. Raise it if the machine has the room and the corner matters.
const scaleMaxBytes = 5 << 28 // 1.25 GiB

// buildScaleStore fills a store with count keys of size-byte values and returns
// it with the keys and a random order to probe them in.
//
// The Data slice is allocated once, at the size it will end up: growing it by
// append doubles it as it goes, which for a gigabyte store means transiently
// holding two of them and measuring the allocator on the way there.
//
// The probe order is a precomputed permutation. Reading keys in the order they
// were written lets the prefetcher hide the very misses these benchmarks exist
// to expose, and building the order inside the timed loop would measure the
// generator instead.
func buildScaleStore(b *testing.B, count, size int) (*KeyValueStore, [][]byte, []int) {
	b.Helper()

	keys := makeKeys(count)
	value := make([]byte, size)
	kvs := &KeyValueStore{
		Data: make([]byte, 0, int64(count)*int64(headerSize+scaleKeyLen+size)),
	}
	for _, key := range keys {
		if err := kvs.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}
	return kvs, keys, rand.Perm(count)
}

// reportThroughput adds the payload rate to a benchmark's results, in bytes a
// second as the rest of the suite reports it and in Mbit/s beside it. The
// second is the unit for comparing against a link rather than against a disk:
// gigabit ethernet is 1000 Mbit/s, and a Raspberry Pi's SD card a few hundred.
//
// It counts value bytes handed to the caller, not bytes touched to find them,
// and at small values it is mostly a restatement of the call overhead — a
// 50 ns lookup of a 16-byte value "does" 2.6 Gbit/s, which says nothing about
// bandwidth. It only begins to describe bandwidth once the value is large
// enough to dominate the lookup. Call it with the timer stopped, so that
// b.Elapsed covers exactly the loop.
func reportThroughput(b *testing.B, bytesPerOp int) {
	b.Helper()

	b.SetBytes(int64(bytesPerOp))
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(bytesPerOp)*8*float64(b.N)/seconds/1e6, "Mbit/s")
	}
}

// BenchmarkKeyValueStore_ReadScale is the read that does not fit in cache.
//
// Every other read benchmark here holds 1024 keys. That index is a few tens of
// kilobytes, it never leaves L2, and what it reports is the best case. A store
// with a million keys has an index of tens of megabytes, and a lookup there is
// a hash and a walk out to main memory. The distance between the two ends of
// this benchmark is what to expect as a store grows, and it is the number to
// quote at anyone sizing one.
//
// The two axes are separate costs and worth reading separately. Growing the key
// count grows the index, and misses looking the record up. Growing the value
// grows Data, and misses reading the record out once found — plus the copy, so
// this is also where the throughput figures mean anything. Only the corner where
// both are large says what a real store of that size does, and it is the corner
// nothing else here covers: Read/65536 has large values behind a tiny index.
//
// View is used so that nothing is allocated in the loop.
//
// The rows in the middle are the least trustworthy numbers in this suite. Where
// the store is on its way out of L2 — around 16k to 131k keys, depending on the
// value — a row is tight within a session and moves 10 to 20% between them:
// 178 ns, 156 ns and 186 ns for 131072keys/16B on three consecutive runs of the
// same code. The ends are steady to a few percent. Compare a middle row only
// against another from the same session, and do not go looking for what changed
// between two of them, because on past evidence it was nothing.
func BenchmarkKeyValueStore_ReadScale(b *testing.B) {
	for _, count := range []int{1 << 10, 1 << 14, 1 << 17, 1 << 20} {
		for _, size := range []int{16, 1024, 16384} {
			if int64(count)*int64(headerSize+scaleKeyLen+size) > scaleMaxBytes {
				continue
			}

			// Built out here rather than inside b.Run, which calls its function
			// again for every attempt at b.N: a million keys would be written
			// for each of them and the benchmark would be mostly setup.
			kvs, keys, probe := buildScaleStore(b, count, size)

			b.Run(fmt.Sprintf("%dkeys/%dB", count, size), func(b *testing.B) {
				var sink int
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kvs.View(keys[probe[i%count]], func(v []byte) error {
						sink += len(v)
						return nil
					}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				reportThroughput(b, size)
			})

			// A gigabyte store is dropped here rather than whenever the
			// collector next looks, so that two of them are never live at once.
			// Nothing above is read again, so all of it is already unreachable.
			runtime.GC()
		}
	}
}

// BenchmarkKeyValueStore_ReadScaleParallel is the same walk on every core.
//
// It answers a question the serial one leaves open: whether a store that has
// stopped fitting in cache still scales. The sharded lock spreads readers, and
// a lookup that misses to memory leaves the core idle while it waits, which is
// room for another core to be doing the same. Read against the serial numbers
// above, not against ReadParallel, which holds 1024 keys and measures the lock
// rather than the memory.
func BenchmarkKeyValueStore_ReadScaleParallel(b *testing.B) {
	for _, count := range []int{1 << 10, 1 << 20} {
		for _, size := range []int{16, 1024} {
			if int64(count)*int64(headerSize+scaleKeyLen+size) > scaleMaxBytes {
				continue
			}

			kvs, keys, probe := buildScaleStore(b, count, size)

			b.Run(fmt.Sprintf("%dkeys/%dB", count, size), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					// Each goroutine starts at its own point in the
					// permutation, so they are not all queued behind the same
					// cache line.
					i := rand.IntN(count)
					sink := 0
					for pb.Next() {
						kvs.View(keys[probe[i%count]], func(v []byte) error {
							sink += len(v)
							return nil
						})
						i++
					}
				})
				b.StopTimer()
				reportThroughput(b, size)
			})

			runtime.GC()
		}
	}
}

func BenchmarkKeyValueStore_Read(b *testing.B) {
	for _, size := range []int{16, 1024, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			keys := makeKeys(1024)
			value := make([]byte, size)
			kvs := &KeyValueStore{}
			for _, key := range keys {
				kvs.Write(key, value)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = kvs.Read(keys[i%len(keys)])
			}
		})
	}
}

func BenchmarkKeyValueStore_View(b *testing.B) {
	for _, size := range []int{16, 1024, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			keys := makeKeys(1024)
			value := make([]byte, size)
			kvs := &KeyValueStore{}
			for _, key := range keys {
				kvs.Write(key, value)
			}
			var sink int
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				kvs.View(keys[i%len(keys)], func(v []byte) error {
					sink += len(v)
					return nil
				})
			}
		})
	}
}

func BenchmarkKeyValueStore_ViewParallel(b *testing.B) {
	keys := makeKeys(1024)
	value := make([]byte, 1024)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		kvs.Write(key, value)
	}
	b.SetBytes(1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		sink := 0
		for pb.Next() {
			kvs.View(keys[i%len(keys)], func(v []byte) error {
				sink += len(v)
				return nil
			})
			i++
		}
	})
}

func BenchmarkKeyValueStore_ReadParallel(b *testing.B) {
	keys := makeKeys(1024)
	value := make([]byte, 1024)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		kvs.Write(key, value)
	}
	b.SetBytes(1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = kvs.Read(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkKeyValueStore_Compact(b *testing.B) {
	keys := makeKeys(4096)
	value := make([]byte, 256)
	golden := &KeyValueStore{}
	for range 4 { // every key written 4 times
		for _, key := range keys {
			golden.Write(key, value)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		kvs := &KeyValueStore{Data: append([]byte(nil), golden.Data...)}
		kvs.RebuildIndex()
		b.StartTimer()
		kvs.Compact()
	}
}

func BenchmarkKeyValueStore_RebuildIndex(b *testing.B) {
	keys := makeKeys(4096)
	value := make([]byte, 256)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		kvs.Write(key, value)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvs.RebuildIndex()
	}
}

// BenchmarkKeyValueStore_ReadUnderWrite is the workload nearly every real store
// has and none of the benchmarks above had: reads on every core with writes
// going on beside them.
//
// A read takes one shard and a write takes all four, so a write does not slow
// readers down, it stops them. What that costs cannot be seen in either half
// measured alone — ReadScaleParallel has no writer, WriteParallel has no
// readers — and it is the number to have before deciding whether a background
// writer is affordable. The zero-writer case is there to be subtracted.
//
// Writers rewrite a key of their own with a small value, the cheapest write
// there is, so what is measured is the lock and not the copying.
func BenchmarkKeyValueStore_ReadUnderWrite(b *testing.B) {
	const readKeys = 4096

	for _, writers := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			// Readers share the first readKeys; each writer owns one past them,
			// so a writer never changes the size of a value a reader is timing.
			keys := makeKeys(readKeys + writers)
			value := make([]byte, 1024)
			kvs := &KeyValueStore{}
			for _, key := range keys[:readKeys] {
				if err := kvs.Write(key, value); err != nil {
					b.Fatal(err)
				}
			}

			// The store as readers must keep finding it. A writer that has
			// grown the log past the bound puts this back instead of truncating
			// to nothing, so no reader is ever left hunting for a key that a
			// reset removed. It happens under the write lock, which no reader
			// can hold at the same time, so none of them sees it half done.
			golden := append([]byte(nil), kvs.Data...)
			goldenIndex := maps.Clone(kvs.Index)

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()

					small := make([]byte, 16)
					written := 0
					for {
						select {
						case <-stop:
							return
						default:
						}

						if err := kvs.Write(keys[readKeys+w], small); err != nil {
							b.Error(err)
							return
						}
						if written++; written%writeBoundChecks == 0 {
							kvs.Lock()
							if len(kvs.Data) > writeBoundBytes {
								kvs.Data = append(kvs.Data[:0], golden...)
								kvs.Index = maps.Clone(goldenIndex)
							}
							kvs.Unlock()
						}
					}
				}(w)
			}

			var missed atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.IntN(readKeys)
				sink := 0
				for pb.Next() {
					err := kvs.View(keys[i%readKeys], func(v []byte) error {
						sink += len(v)
						return nil
					})
					if err != nil {
						missed.Add(1)
					}
					i++
				}
			})
			b.StopTimer()

			close(stop)
			wg.Wait()

			// A read that found nothing was timing the wrong thing, and there
			// should be none of them: the reset above holds the write lock for
			// as long as the store is inconsistent. This is the assertion that
			// the trick works.
			if n := missed.Load(); n > 0 {
				b.Errorf("%d reads found no record", n)
			}
			reportThroughput(b, len(value))
		})
	}
}

// BenchmarkKeyValueStore_WriteScale is the write side of ReadScale.
//
// WriteInsert cycles 4096 keys, so its index never grows and its map operations
// always land in a table that is small and hot. A large store has neither, and
// the question is whether a write pays the same fixed tax for missing out to
// memory that a read was shown to. Writes here are updates, so the index stays
// the size it was filled to and what changes between rows is only how far the
// map has to reach.
func BenchmarkKeyValueStore_WriteScale(b *testing.B) {
	const recordBytes = headerSize + scaleKeyLen + 16

	for _, count := range []int{1 << 10, 1 << 14, 1 << 17, 1 << 20} {
		keys := makeKeys(count)
		value := make([]byte, 16)
		kvs := &KeyValueStore{
			Data: make([]byte, 0, int64(count)*int64(recordBytes)),
		}
		for _, key := range keys {
			if err := kvs.Write(key, value); err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprint(count), func(b *testing.B) {
			// Room for every record this run will append, taken before the
			// clock starts. The alternative is append doubling a slice that is
			// already hundreds of megabytes, which measures the allocator and
			// briefly holds two copies of it. Truncating instead is not open
			// here: the index points into what would be thrown away.
			if need := len(kvs.Data) + b.N*recordBytes; cap(kvs.Data) < need {
				grown := make([]byte, len(kvs.Data), need)
				copy(grown, kvs.Data)
				kvs.Data = grown
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kvs.Write(keys[i%count], value); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportThroughput(b, len(value))
		})

		runtime.GC()
	}
}

// BenchmarkKeyValueStore_Delete is the tombstone path: the same append as a
// write, with no value on it.
func BenchmarkKeyValueStore_Delete(b *testing.B) {
	keys := makeKeys(4096)
	value := make([]byte, 16)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		if err := kvs.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := kvs.Delete(keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
		if len(kvs.Data) > writeBoundBytes {
			kvs.Data = kvs.Data[:0]
			kvs.Index = nil
		}
	}
}

// BenchmarkKeyValueStore_ReadMissing is what a lookup costs when there is
// nothing to return.
//
// Both answers come out of the index without touching the log — one finds no
// entry, the other finds a tombstone — so both should be cheaper than the hit
// that Read/16 measures. It is worth knowing by how much for anything that
// probes for keys it expects to be absent.
func BenchmarkKeyValueStore_ReadMissing(b *testing.B) {
	keys := makeKeys(1024)
	absent := makeKeys(2048)[1024:] // never written
	value := make([]byte, 16)

	kvs := &KeyValueStore{}
	for _, key := range keys {
		if err := kvs.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}
	// The second half of the live keys becomes tombstones.
	for _, key := range keys[512:] {
		if err := kvs.Delete(key); err != nil {
			b.Fatal(err)
		}
	}

	for _, probe := range []struct {
		name string
		keys [][]byte
		want error
	}{
		{"absent", absent, ErrorKeyNotFound},
		{"deleted", keys[512:], ErrorKeyDeleted},
	} {
		b.Run(probe.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := kvs.Read(probe.keys[i%len(probe.keys)]); !errors.Is(err, probe.want) {
					b.Fatalf("got %v, want %v", err, probe.want)
				}
			}
		})
	}
}

// BenchmarkKeyValueStore_ForEach is the only way to walk the store, since the
// index is a hash map and has no order to iterate. It reads every record,
// superseded ones and tombstones included, so its cost is the size of the log
// rather than the number of keys — which is the argument for compacting one
// that is walked often.
func BenchmarkKeyValueStore_ForEach(b *testing.B) {
	keys := makeKeys(4096)
	value := make([]byte, 256)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		if err := kvs.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	var sink int
	b.SetBytes(kvs.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := kvs.ForEach(func(_, v []byte, _ bool) bool {
			sink += len(v)
			return true
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKeyValueStore_Verify checks every record against its checksum, which
// is the whole log read and folded. It is what a store pays to find rot that a
// read of an untouched key would not.
func BenchmarkKeyValueStore_Verify(b *testing.B) {
	keys := makeKeys(4096)
	value := make([]byte, 256)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		if err := kvs.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(kvs.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := kvs.Verify(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKeyValueStore_Index is the saved index against the rebuilt one, which
// is the question SaveIndex exists to answer, and the answer is not the obvious
// one.
//
// The scan that loading is meant to save turns out not to be the expensive
// thing it sounds like. RebuildIndex walks the log by asking each header how
// long its record is and stepping over the value without reading it, so it
// costs the number of records and not the number of bytes. Measured against
// value sizes from 16 B to 64 KiB — a log from a quarter of a megabyte to 268
// of them — it does not move: 392, 406, 393 and 398 µs.
//
// So both sides are proportional to the keys, and the sweep is over key count
// for that reason. Loading loses at every one of them — 12% at 4096 keys, 14%
// at 16384, 16% at 65536 — because a gob decode and a random-access check per
// key costs more than stepping through headers a prefetcher can see coming.
//
// That is not the whole case for SaveIndex, and this benchmark cannot make it.
// Data is in memory here, so rebuilding touches the header of every record for
// free. Read back off a disk that has not cached it, rebuilding faults in a
// page for every record it steps to, which for large values is a page per key
// across the whole log, while loading reads one compact file end to end. That
// is the same argument the hint files make for a DB, and on a Raspberry Pi's
// SD card it is the argument that matters. Measuring it needs a cold page
// cache, which nothing here has.
func BenchmarkKeyValueStore_Index(b *testing.B) {
	for _, count := range []int{1 << 12, 1 << 14, 1 << 16} {
		keys := makeKeys(count)
		value := make([]byte, 256)
		kvs := &KeyValueStore{}
		for _, key := range keys {
			if err := kvs.Write(key, value); err != nil {
				b.Fatal(err)
			}
		}

		saved, err := kvs.SaveIndex()
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("%dkeys/rebuild", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kvs.RebuildIndex(); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("%dkeys/load", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kvs.LoadIndex(saved); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("%dkeys/save", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := kvs.SaveIndex(); err != nil {
					b.Fatal(err)
				}
			}
		})

		runtime.GC()
	}
}

// BenchmarkKeyValueStore_Recover is what opening a store off a log costs when
// the index has to be worked out again and every record checked on the way.
// RebuildIndex is the same walk without the checksums, so the two together say
// what the checking costs.
func BenchmarkKeyValueStore_Recover(b *testing.B) {
	keys := makeKeys(4096)
	value := make([]byte, 256)
	kvs := &KeyValueStore{}
	for _, key := range keys {
		if err := kvs.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(kvs.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kvs.Recover(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestShardCount(t *testing.T) {
	for procs := 1; procs <= 64; procs++ {
		n := shardCount(procs)

		switch {
		case n < 1 || n > maxShards:
			t.Errorf("shardCount(%d) = %d, outside [1, %d]", procs, n, maxShards)
		case n&(n-1) != 0:
			t.Errorf("shardCount(%d) = %d, not a power of two", procs, n)
		case n > procs:
			t.Errorf("shardCount(%d) = %d, more shards than cores", procs, n)
		case n*2 <= procs && n*2 <= maxShards:
			t.Errorf("shardCount(%d) = %d, could have been %d", procs, n, n*2)
		}
	}
}

// TestShardedRWMutex checks that the lock is a lock: a writer excludes readers
// on every shard, not just the one its key hashes to.
func TestShardedRWMutex(t *testing.T) {
	var m shardedRWMutex

	// Every shard must be reachable, or the read side is not really sharded.
	seen := make(map[*paddedRWMutex]bool)
	for i := range 1000 {
		shard := m.rlockKey(fmt.Appendf(nil, "key%d", i))
		shard.RUnlock()
		seen[shard] = true
	}
	if len(seen) != numShards {
		t.Errorf("keys reached %d of %d shards", len(seen), numShards)
	}

	// With the write lock held, no shard may be read-lockable.
	m.Lock()
	for i := range m.shards[:numShards] {
		if m.shards[i].TryRLock() {
			m.shards[i].RUnlock()
			m.Unlock()
			t.Fatalf("shard %d was read-lockable while the write lock was held", i)
		}
	}
	m.Unlock()

	// And afterwards every shard is free again.
	for i := range m.shards[:numShards] {
		if !m.shards[i].TryRLock() {
			t.Errorf("shard %d stayed locked after Unlock", i)
		} else {
			m.shards[i].RUnlock()
		}
	}
}

// TestShardedRWMutexExcludes has a writer and readers spread over the shards
// share a counter that only the lock protects. Run with -race.
func TestShardedRWMutexExcludes(t *testing.T) {
	var m shardedRWMutex
	guarded := 0

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "key%d", i)
			for j := range 500 {
				if j%10 == 0 {
					m.Lock()
					guarded++
					m.Unlock()
					continue
				}
				shard := m.rlockKey(key)
				_ = guarded
				shard.RUnlock()
			}
		}(i)
	}
	wg.Wait()

	if guarded != 8*50 {
		t.Errorf("expected 400 guarded increments, got %d", guarded)
	}
}

func TestKeyValueStore_PrintAllKeyValuePairs(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("1"))
	kvs.Write([]byte("b"), []byte("2"))
	kvs.Delete([]byte("a"))

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = write

	printErr := kvs.PrintAllKeyValuePairs()

	os.Stdout = stdout
	write.Close()

	printed, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	if printErr != nil {
		t.Fatalf("PrintAllKeyValuePairs: %v", printErr)
	}

	want := "Key: a, Value: 1, Deleted: false\n" +
		"Key: b, Value: 2, Deleted: false\n" +
		"Key: a, Value: , Deleted: true\n"
	if string(printed) != want {
		t.Errorf("printed:\n%s\nwant:\n%s", printed, want)
	}
}

func TestKeyValueStore_LoadIndexRejectsGarbage(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("a"), []byte("1"))

	if err := kvs.LoadIndex([]byte("this is not gob")); err == nil {
		t.Error("LoadIndex accepted garbage")
	}
	// The store is untouched.
	if value, err := kvs.Read([]byte("a")); err != nil || string(value) != "1" {
		t.Errorf("a: got '%s' (err %v) after a rejected index", value, err)
	}
}

func TestKeyValueStore_VerifyReportsBadFraming(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("k"), []byte("v"))

	// A value length that runs past the end of the data.
	binary.LittleEndian.PutUint32(kvs.Data[18:22], 1<<20)

	err := kvs.Verify()
	var corrupt *CorruptAtError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected a *CorruptAtError, got '%v'", err)
	}
	if corrupt.Offset != 0 {
		t.Errorf("reported offset %d, want 0", corrupt.Offset)
	}
	if !errors.Is(err, ErrorCorruptData) {
		t.Error("a *CorruptAtError should match ErrorCorruptData")
	}
}
