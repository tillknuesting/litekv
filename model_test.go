package litekv

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// model is what the store is supposed to behave like: a plain map, with a note
// of which keys were deleted rather than never written, since the store tells
// those apart.
type model struct {
	live    map[string]string
	deleted map[string]bool
}

func newModel() *model {
	return &model{live: map[string]string{}, deleted: map[string]bool{}}
}

func (m *model) write(key, value string) {
	m.live[key] = value
	delete(m.deleted, key)
}

func (m *model) delete(key string) {
	delete(m.live, key)
	m.deleted[key] = true
}

// compact drops the tombstones, which is what compaction does to a store: a
// deleted key stops being deleted and goes back to being absent.
func (m *model) compact() {
	m.deleted = map[string]bool{}
}

// check holds the store to the model for every key either of them knows about.
func (m *model) check(t *testing.T, kvs *KeyValueStore, step string) {
	t.Helper()

	for key, want := range m.live {
		value, err := kvs.Read([]byte(key))
		if err != nil {
			t.Fatalf("%s: key %q: %v, want '%s'", step, key, err, want)
		}
		if string(value) != want {
			t.Fatalf("%s: key %q: got '%s', want '%s'", step, key, value, want)
		}
	}

	for key := range m.deleted {
		_, err := kvs.Read([]byte(key))
		if !errors.Is(err, ErrorKeyDeleted) {
			t.Fatalf("%s: deleted key %q: got %v, want %v", step, key, err, ErrorKeyDeleted)
		}
	}

	if got := len(kvs.Index); got != len(m.live)+len(m.deleted) {
		t.Fatalf("%s: %d keys indexed, want %d live plus %d deleted",
			step, got, len(m.live), len(m.deleted))
	}

	// Whatever the store answers, its records must still be intact.
	if err := kvs.Verify(); err != nil {
		t.Fatalf("%s: Verify: %v", step, err)
	}
}

// TestModel runs a long random mix of operations against a map that says what
// the answers should be, checking after every one. The maintenance operations
// are in the mix rather than at the end, so compaction and index rebuilds have
// to survive being interleaved with writes rather than only being tried on a
// quiet store.
func TestModel(t *testing.T) {
	backings := []struct {
		name string
		open func(t *testing.T) (*KeyValueStore, func())
	}{
		{
			name: "in memory",
			open: func(t *testing.T) (*KeyValueStore, func()) {
				return &KeyValueStore{}, func() {}
			},
		},
		{
			name: "attached log",
			open: func(t *testing.T) (*KeyValueStore, func()) {
				kvs := &KeyValueStore{}
				if err := kvs.Attach(&memLog{}, Options{Sync: SyncNever}); err != nil {
					t.Fatal(err)
				}
				return kvs, func() { kvs.Close() }
			},
		},
		{
			name: "file",
			open: func(t *testing.T) (*KeyValueStore, func()) {
				kvs, err := Open(filepath.Join(t.TempDir(), "kv"), Options{Sync: SyncNever})
				if err != nil {
					t.Fatal(err)
				}
				return kvs, func() { kvs.Close() }
			},
		},
	}

	for _, backing := range backings {
		t.Run(backing.name, func(t *testing.T) {
			kvs, done := backing.open(t)
			defer done()

			m := newModel()
			random := rand.New(rand.NewSource(1))

			// A small key space, so that writes collide, keys get rewritten,
			// deleted keys come back, and compaction has something to do.
			keys := make([]string, 40)
			for i := range keys {
				keys[i] = fmt.Sprintf("key%02d", i)
			}
			// Two keys that are easy to get wrong.
			keys = append(keys, "", "\x00\xff\x00")

			for step := range 3000 {
				key := keys[random.Intn(len(keys))]

				switch n := random.Intn(100); {
				case n < 45: // write
					value := fmt.Sprintf("value-%d-%d", step, random.Intn(1000))
					if err := kvs.Write([]byte(key), []byte(value)); err != nil {
						t.Fatalf("step %d: Write: %v", step, err)
					}
					m.write(key, value)

				case n < 65: // delete
					if err := kvs.Delete([]byte(key)); err != nil {
						t.Fatalf("step %d: Delete: %v", step, err)
					}
					m.delete(key)

				case n < 80: // read a key that may or may not be there
					value, err := kvs.Read([]byte(key))
					want, live := m.live[key]
					switch {
					case live && (err != nil || string(value) != want):
						t.Fatalf("step %d: key %q: got '%s' (%v), want '%s'", step, key, value, err, want)
					case !live && m.deleted[key] && !errors.Is(err, ErrorKeyDeleted):
						t.Fatalf("step %d: deleted key %q: %v", step, key, err)
					case !live && !m.deleted[key] && !errors.Is(err, ErrorKeyNotFound):
						t.Fatalf("step %d: absent key %q: %v", step, key, err)
					}

				case n < 88: // walk a snapshot of the live keys
					seen := map[string]string{}
					err := kvs.ForEach(func(key, value []byte, deleted bool) bool {
						if deleted {
							delete(seen, string(key))
						} else {
							seen[string(key)] = string(value)
						}
						return true
					})
					if err != nil {
						t.Fatalf("step %d: ForEach: %v", step, err)
					}
					if len(seen) != len(m.live) {
						t.Fatalf("step %d: ForEach saw %d live keys, want %d", step, len(seen), len(m.live))
					}
					for key, want := range m.live {
						if seen[key] != want {
							t.Fatalf("step %d: ForEach: key %q is '%s', want '%s'", step, key, seen[key], want)
						}
					}

				case n < 93: // rebuild the index from the records
					if err := kvs.RebuildIndex(); err != nil {
						t.Fatalf("step %d: RebuildIndex: %v", step, err)
					}

				case n < 96: // save and reload the index
					saved, err := kvs.SaveIndex()
					if err != nil {
						t.Fatalf("step %d: SaveIndex: %v", step, err)
					}
					if err := kvs.LoadIndex(saved); err != nil {
						t.Fatalf("step %d: LoadIndex: %v", step, err)
					}

				case n < 99: // compact
					if err := kvs.Compact(); err != nil {
						t.Fatalf("step %d: Compact: %v", step, err)
					}
					m.compact()

				default: // recover, which must change nothing on an intact store
					discarded, err := kvs.Recover()
					if err != nil {
						t.Fatalf("step %d: Recover: %v", step, err)
					}
					if discarded != 0 {
						t.Fatalf("step %d: Recover discarded %d bytes of an intact store", step, discarded)
					}
				}

				if step%50 == 0 {
					m.check(t, kvs, fmt.Sprintf("step %d", step))
				}
			}

			m.check(t, kvs, "final")
		})
	}
}

// TestModelSurvivesReopening runs the same kind of mix, but reopens the file
// part way through, so every record has to mean the same thing after a restart
// as it did before one.
func TestModelSurvivesReopening(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv")

	kvs, err := Open(path, Options{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	m := newModel()
	random := rand.New(rand.NewSource(2))

	keys := make([]string, 25)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	for round := range 12 {
		for step := range 200 {
			key := keys[random.Intn(len(keys))]

			switch n := random.Intn(10); {
			case n < 6:
				value := fmt.Sprintf("round%d-step%d", round, step)
				if err := kvs.Write([]byte(key), []byte(value)); err != nil {
					t.Fatalf("round %d: Write: %v", round, err)
				}
				m.write(key, value)
			case n < 9:
				if err := kvs.Delete([]byte(key)); err != nil {
					t.Fatalf("round %d: Delete: %v", round, err)
				}
				m.delete(key)
			default:
				if err := kvs.Compact(); err != nil {
					t.Fatalf("round %d: Compact: %v", round, err)
				}
				m.compact()
			}
		}

		m.check(t, kvs, fmt.Sprintf("round %d before closing", round))

		if err := kvs.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}

		// What the file holds is what the store held.
		onDisk, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(onDisk) != string(kvs.Data) {
			t.Fatalf("round %d: the file holds %d bytes, the store held %d", round, len(onDisk), len(kvs.Data))
		}

		kvs, err = Open(path, Options{Sync: SyncNever})
		if err != nil {
			t.Fatalf("round %d: reopen: %v", round, err)
		}

		m.check(t, kvs, fmt.Sprintf("round %d after reopening", round))
	}

	if err := kvs.Close(); err != nil {
		t.Fatal(err)
	}
}
