package litekv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
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

			if kvs.Index.Len() != live {
				t.Errorf("expected %d indexed keys after compaction, got %d", live, kvs.Index.Len())
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

	before := make(map[string]int64)
	for _, key := range []string{"foo", "foo2", "foo3"} {
		pos, ok := kvs.Index.Lookup(kvs.Data, []byte(key))
		if !ok {
			t.Fatalf("key %q missing from the index before the rebuild", key)
		}
		before[key] = pos
	}

	kvs.Index = Tree{}
	if _, ok := kvs.Index.Lookup(kvs.Data, []byte("foo")); ok {
		t.Fatal("the index was not actually cleared")
	}

	if err := kvs.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if kvs.Index.Len() != len(before) {
		t.Errorf("expected %d keys after the rebuild, got %d", len(before), kvs.Index.Len())
	}
	for key, want := range before {
		got, ok := kvs.Index.Lookup(kvs.Data, []byte(key))
		if !ok {
			t.Errorf("key %q missing after the rebuild", key)
		} else if got != want {
			t.Errorf("key %q: offset %d after the rebuild, was %d", key, got, want)
		}
	}

	// The rebuilt index must point at the newest record for each key.
	value, err := kvs.Read([]byte("foo"))
	if err != nil || string(value) != "updated" {
		t.Errorf("expected 'updated', got '%s' (err %v)", value, err)
	}
	if _, err := kvs.Read([]byte("foo3")); !errors.Is(err, ErrorKeyDeleted) {
		t.Errorf("expected '%v', got '%v'", ErrorKeyDeleted, err)
	}
}

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

// reindex points key at an arbitrary offset, the way a stale or damaged index
// would. The key must already be stored, since the tree indexes the key bytes
// where they lie in Data.
func reindex(kvs *KeyValueStore, key string, pos int64) {
	stored, ok := kvs.Index.Lookup(kvs.Data, []byte(key))
	if !ok {
		panic("reindex: " + key + " is not in the store")
	}
	kvs.Index.Insert(kvs.Data, stored+headerSize, len(key), pos)
}

func TestKeyValueStore_ReadWithBadIndex(t *testing.T) {
	tests := []struct {
		name        string
		corrupt     func(kvs *KeyValueStore)
		expectedErr error
	}{
		{
			name:        "offset past the end of Data",
			corrupt:     func(kvs *KeyValueStore) { reindex(kvs, "a", int64(len(kvs.Data))+100) },
			expectedErr: ErrorCorruptData,
		},
		{
			name:        "negative offset",
			corrupt:     func(kvs *KeyValueStore) { reindex(kvs, "a", -1) },
			expectedErr: ErrorCorruptData,
		},
		{
			name:        "offset in the middle of a record",
			corrupt:     func(kvs *KeyValueStore) { reindex(kvs, "a", 7) },
			expectedErr: ErrorCorruptData,
		},
		{
			name: "offset of another key's record",
			corrupt: func(kvs *KeyValueStore) {
				pos, _ := kvs.Index.Lookup(kvs.Data, []byte("b"))
				reindex(kvs, "a", pos)
			},
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
		if _, ok := dst.Index.Lookup(dst.Data, []byte("b")); ok {
			t.Error("a rejected index was installed anyway")
		}
	})

	t.Run("replaces rather than merges", func(t *testing.T) {
		dst := &KeyValueStore{Data: src.Data}
		dst.Write([]byte("stale"), []byte("s"))

		if err := dst.LoadIndex(exported); err != nil {
			t.Fatalf("LoadIndex: %v", err)
		}
		if _, ok := dst.Index.Lookup(dst.Data, []byte("stale")); ok {
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
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			for j := 0; j < 100; j++ {
				kvs.Write(key, []byte(fmt.Sprintf("value%d", j)))
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			kvs.RLock()
			_ = kvs.Index.Len()
			_ = len(kvs.Data)
			kvs.RUnlock()
		}
	}()

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
		kvs.Index.WalkPrefix(kvs.Data, nil, func(pos int64) bool {
			record, _, err := parseRecordAt(kvs.Data, pos)
			if err != nil {
				return false
			}
			key := string(record.Key)
			if value, err := kvs.Read([]byte(key)); err == nil {
				live[key] = string(value)
			}
			return true
		})

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

func TestKeyValueStore_PrefixScan(t *testing.T) {
	kvs := &KeyValueStore{}
	for _, key := range []string{"user:2", "user:10", "user:1", "userx", "use", "zzz", ""} {
		kvs.Write([]byte(key), []byte("v-"+key))
	}
	kvs.Write([]byte("user:1"), []byte("v-updated")) // superseded record
	kvs.Write([]byte("user:9"), []byte("v-user:9"))
	kvs.Delete([]byte("user:9")) // tombstone
	kvs.Delete([]byte("gone"))   // tombstone for a key never written

	collect := func(prefix string) []string {
		var got []string
		if err := kvs.PrefixScan([]byte(prefix), func(key, value []byte) bool {
			got = append(got, fmt.Sprintf("%s=%s", key, value))
			return true
		}); err != nil {
			t.Fatalf("PrefixScan(%q): %v", prefix, err)
		}
		return got
	}

	tests := []struct {
		prefix string
		want   string
	}{
		// Ascending byte order, deleted and superseded records left out.
		{"user:", "user:1=v-updated,user:10=v-user:10,user:2=v-user:2"},
		{"user", "user:1=v-updated,user:10=v-user:10,user:2=v-user:2,userx=v-userx"},
		{"use", "use=v-use,user:1=v-updated,user:10=v-user:10,user:2=v-user:2,userx=v-userx"},
		{"user:1", "user:1=v-updated,user:10=v-user:10"},
		{"zzz", "zzz=v-zzz"},
		{"nothing", ""},
		{"user:99", ""},
	}

	for _, test := range tests {
		if got := strings.Join(collect(test.prefix), ","); got != test.want {
			t.Errorf("prefix %q:\n got %s\nwant %s", test.prefix, got, test.want)
		}
	}

	// The empty prefix visits every live key, including the empty one.
	all := collect("")
	if len(all) != 7 || all[0] != "=v-" {
		t.Errorf("empty prefix: got %v", all)
	}

	// fn can stop the scan.
	var seen int
	if err := kvs.PrefixScan([]byte("user"), func(key, value []byte) bool {
		seen++
		return false
	}); err != nil {
		t.Fatalf("PrefixScan: %v", err)
	}
	if seen != 1 {
		t.Errorf("expected the scan to stop after 1 key, saw %d", seen)
	}

	// A damaged record is reported rather than handed over.
	kvs.Data[len(kvs.Data)-1]++
	if err := kvs.PrefixScan(nil, func(key, value []byte) bool { return true }); !errors.Is(err, ErrorChecksumMismatch) {
		t.Errorf("expected '%v', got '%v'", ErrorChecksumMismatch, err)
	}
}

func TestKeyValueStore_View(t *testing.T) {
	kvs := &KeyValueStore{}
	kvs.Write([]byte("k"), []byte("value"))
	kvs.Write([]byte("gone"), []byte("x"))
	kvs.Delete([]byte("gone"))

	var seen string
	if err := kvs.View([]byte("k"), func(value []byte) error {
		seen = string(value)
		// The value must be the stored bytes, not a copy of them.
		pos, _ := kvs.Index.Lookup(kvs.Data, []byte("k"))
		if &value[0] != &kvs.Data[pos+headerSize+1] {
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
	err := kvs.View([]byte("gone"), func(value []byte) error {
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
		keys[i] = []byte(fmt.Sprintf("key:%016d", i))
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
					kvs.Index = Tree{}
				}
			}
		})
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
	for round := 0; round < 4; round++ { // every key written 4 times
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
	for i := 0; i < 1000; i++ {
		shard := m.rlockKey([]byte(fmt.Sprintf("key%d", i)))
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
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			for j := 0; j < 500; j++ {
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

// BenchmarkKeyValueStore_PrefixScan measures a prefix query against the only
// way to answer one before the tree: walk every record and filter.
func BenchmarkKeyValueStore_PrefixScan(b *testing.B) {
	kvs := &KeyValueStore{}
	value := make([]byte, 64)
	for i := 0; i < 100_000; i++ {
		kvs.Write([]byte(fmt.Sprintf("user:%08d:profile", i)), value)
	}

	// Matches ten of the hundred thousand keys.
	prefix := []byte("user:0000123")

	b.Run("tree", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			matched := 0
			kvs.PrefixScan(prefix, func(key, value []byte) bool {
				matched++
				return true
			})
			if matched != 10 {
				b.Fatalf("matched %d keys, want 10", matched)
			}
		}
	})

	b.Run("scan-all", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			matched := 0
			kvs.ForEach(func(key, value []byte, deleted bool) bool {
				if !deleted && bytes.HasPrefix(key, prefix) {
					matched++
				}
				return true
			})
			if matched != 10 {
				b.Fatalf("matched %d keys, want 10", matched)
			}
		}
	})
}
