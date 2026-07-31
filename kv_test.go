package litekv

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
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

func BenchmarkKeyValueStore_Write(b *testing.B) {
	kvs := &KeyValueStore{}

	// Run the benchmark with different key inputs and value sizes
	for _, tc := range []struct {
		key      []byte
		valueLen int
	}{
		{[]byte("foo"), 1},
		{[]byte("baz"), 1024},
		{[]byte("quux"), 1048576},
		{[]byte("zuux"), 104857600},
		{[]byte("xuux"), 1073741824},
	} {
		b.Run(fmt.Sprintf("key=%s,valueLen=%d", tc.key, tc.valueLen), func(b *testing.B) {
			b.ReportAllocs()
			// Generate a value of the specified length
			value := make([]byte, tc.valueLen)
			rand.Read(value)

			// Set the bytes processed per operation
			b.SetBytes(int64(tc.valueLen))

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				kvs.Write(tc.key, value)
			}
		})
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

func BenchmarkKeyValueStore_Read(b *testing.B) {
	kvs := &KeyValueStore{}

	// Write some sample Data to the store
	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("baz"), []byte(strings.Repeat("a", 1024)))
	kvs.Write([]byte("quux"), []byte(strings.Repeat("b", 1048576)))
	kvs.Write([]byte("zuux"), []byte(strings.Repeat("c", 104857600)))
	kvs.Write([]byte("xuux"), []byte(strings.Repeat("d", 1073741824)))

	// Run the benchmark with different key inputs and value sizes
	for _, tc := range []struct {
		key      []byte
		valueLen int
	}{
		{[]byte("foo"), 1},
		{[]byte("baz"), 1024},
		{[]byte("quux"), 1048576},
		{[]byte("zuux"), 104857600},
		{[]byte("xuux"), 1073741824},
	} {
		b.Run(fmt.Sprintf("key=%s,valueLen=%d", tc.key, tc.valueLen), func(b *testing.B) {
			b.ReportAllocs()
			// Generate a value of the specified length
			value := make([]byte, tc.valueLen)
			rand.Read(value)

			// Write the value to the store
			kvs.Write(tc.key, value)

			// Set the bytes processed per operation
			b.SetBytes(int64(tc.valueLen))

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				_, _ = kvs.Read(tc.key)
			}
		})
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

func BenchmarkKeyValueStore_Delete(b *testing.B) {
	kvs := &KeyValueStore{}

	// Write some sample Data to the store
	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("baz"), []byte(strings.Repeat("a", 1024)))
	kvs.Write([]byte("quux"), []byte(strings.Repeat("b", 1048576)))

	// Run the benchmark with different key inputs and value sizes
	for _, tc := range []struct {
		key      []byte
		valueLen int
	}{
		{[]byte("foo"), 1},
		{[]byte("baz"), 1024},
		{[]byte("quux"), 1048576},
	} {
		b.Run(fmt.Sprintf("key=%s,valueLen=%d", tc.key, tc.valueLen), func(b *testing.B) {
			b.ReportAllocs()
			// Generate a value of the specified length
			value := make([]byte, tc.valueLen)
			rand.Read(value)

			// Write the value to the store
			kvs.Write(tc.key, value)

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				kvs.Delete(tc.key)
			}
		})
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
	// TODO: Refactor test to consider more cases
	kvs := &KeyValueStore{}

	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("foo2"), []byte("bar2"))
	kvs.Write([]byte("foo3"), []byte("bar3"))

	fmt.Println("kvs.Index", kvs.Index)

	kvsTemp := kvs.Index
	kvs.Index = nil

	kvs.RebuildIndex()

	if kvs.Index == nil {
		t.Errorf("Index is nil")
	} else if len(kvsTemp) != len(kvsTemp) {
		t.Errorf("Index is not equal to kvsTemp")
	} else if kvs.Index["foo"] != kvsTemp["foo"] {
		t.Errorf("Index is not equal to kvsTemp")
	} else if kvs.Index["foo2"] != kvsTemp["foo2"] {
		t.Errorf("Index is not equal to kvsTemp")
	}
}

// TestKeyValueStore_BinaryFormatUnchanged pins the on-disk layout, so that a
// store written by an older version of the library still reads back.
func TestKeyValueStore_BinaryFormatUnchanged(t *testing.T) {
	const golden = "0923b16f000300000003000000666f6f626172" + // write foo=bar
		"e5c4912e0001000000000000006b" + // write k=
		"3250a00a010300000000000000666f6f" // delete foo

	kvs := &KeyValueStore{}
	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("k"), []byte(""))
	kvs.Delete([]byte("foo"))

	if got := hex.EncodeToString(kvs.Data); got != golden {
		t.Errorf("binary format changed:\n got %s\nwant %s", got, golden)
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

	// Bytes 9..13 hold ValueLength. Claim the maximum.
	binary.LittleEndian.PutUint32(kvs.Data[9:13], math.MaxUint32)

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

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			for j := 0; j < 100; j++ {
				kvs.Write(key, []byte(fmt.Sprintf("value%d", j)))
				kvs.Read(key)
				kvs.Read([]byte("seed"))
				if j%25 == 0 {
					kvs.Delete(key)
				}
			}
		}(i)
	}
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
	f.Add(make([]byte, headerSize))

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
