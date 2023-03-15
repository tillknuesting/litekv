package kv

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
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

		// Modify data to create a checksum mismatch
		if test.modifyData {
			kvs.data[0]++
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

	// Write some sample data to the store
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

	// Write some sample data to the store
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

func FuzzKeyValueStore_WriteReadDelete(f *testing.F) {
	kvs := &KeyValueStore{}
	f.Fuzz(func(t *testing.T, a []byte, b []byte) {
		kvs.Write(a, b)
		kvs.Read(a)
		kvs.Delete(a)
	})
}
