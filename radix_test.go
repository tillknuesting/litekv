package litekv

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"testing"
	"unsafe"
)

// indexKeys builds a data slice holding the keys and a tree indexing them,
// which is the arrangement the store uses: the keys live in data and the tree
// only points at them. Each key maps to its position in the input.
func indexKeys(keys []string) (Tree, []byte) {
	var tree Tree
	var data []byte

	for i, key := range keys {
		off := int64(len(data))
		data = append(data, key...)
		tree.Insert(data, off, len(key), int64(i))
	}

	return tree, data
}

func TestTreeLookup(t *testing.T) {
	keys := []string{
		"",         // the empty key lives on the root
		"a",        // a key that is a prefix of others
		"ab",       //
		"abc",      //
		"abd",      // forces a split under "ab"
		"b",        //
		"banana",   //
		"band",     // shares "ban" with banana
		"bandage",  // extends a key that is itself stored
		"xyzzy",    //
		"\x00\xff", // bytes that are not text
	}

	tree, data := indexKeys(keys)

	if tree.Len() != len(keys) {
		t.Errorf("Len is %d, want %d", tree.Len(), len(keys))
	}

	for want, key := range keys {
		got, ok := tree.Lookup(data, []byte(key))
		if !ok {
			t.Errorf("key %q missing", key)
		} else if got != int64(want) {
			t.Errorf("key %q: got %d, want %d", key, got, want)
		}
	}

	for _, missing := range []string{"c", "ac", "abcd", "ban", "bandages", "xyz", "\xff"} {
		if _, ok := tree.Lookup(data, []byte(missing)); ok {
			t.Errorf("key %q should not be in the tree", missing)
		}
	}
}

func TestTreeInsertReplaces(t *testing.T) {
	tree, data := indexKeys([]string{"key", "keyed"})

	// Re-indexing a key already held must replace its value, not add a key.
	tree.Insert(data, 0, len("key"), 99)

	if tree.Len() != 2 {
		t.Errorf("Len is %d after replacing, want 2", tree.Len())
	}
	if got, _ := tree.Lookup(data, []byte("key")); got != 99 {
		t.Errorf("key: got %d, want 99", got)
	}
	if got, _ := tree.Lookup(data, []byte("keyed")); got != 1 {
		t.Errorf("keyed: got %d, want 1", got)
	}
}

func TestTreeEmpty(t *testing.T) {
	var tree Tree
	var data []byte

	if tree.Len() != 0 {
		t.Errorf("Len is %d, want 0", tree.Len())
	}
	if _, ok := tree.Lookup(data, []byte("anything")); ok {
		t.Error("an empty tree returned a key")
	}
	if !tree.WalkPrefix(data, nil, func(int64) bool { t.Error("empty tree walked a key"); return true }) {
		t.Error("walking an empty tree reported an early stop")
	}
}

func TestTreeWalkPrefix(t *testing.T) {
	keys := []string{"a", "ab", "abc", "abd", "b", "banana", "band", "bandage", "", "xyzzy"}
	tree, data := indexKeys(keys)

	tests := []struct {
		prefix string
		want   []string
	}{
		{"", []string{"", "a", "ab", "abc", "abd", "b", "banana", "band", "bandage", "xyzzy"}},
		{"a", []string{"a", "ab", "abc", "abd"}},
		{"ab", []string{"ab", "abc", "abd"}},
		{"abc", []string{"abc"}},
		{"ban", []string{"banana", "band", "bandage"}}, // ends inside a node
		{"band", []string{"band", "bandage"}},          // ends at a node
		{"banda", []string{"bandage"}},                 // ends inside a leaf
		{"bandage!", nil},                              // longer than any key
		{"c", nil},                                     // no such branch
		{"abz", nil},                                   // diverges inside a node
		{"x", []string{"xyzzy"}},                       //
	}

	for _, test := range tests {
		var got []string
		tree.WalkPrefix(data, []byte(test.prefix), func(value int64) bool {
			got = append(got, keys[value])
			return true
		})

		if strings.Join(got, ",") != strings.Join(test.want, ",") {
			t.Errorf("prefix %q: got %v, want %v", test.prefix, got, test.want)
		}
	}
}

func TestTreeWalkPrefixStops(t *testing.T) {
	keys := []string{"a", "ab", "abc", "abd"}
	tree, data := indexKeys(keys)

	var seen []string
	completed := tree.WalkPrefix(data, []byte("a"), func(value int64) bool {
		seen = append(seen, keys[value])
		return len(seen) < 2
	})

	if completed {
		t.Error("WalkPrefix reported it finished after fn stopped it")
	}
	if strings.Join(seen, ",") != "a,ab" {
		t.Errorf("got %v, want [a ab]", seen)
	}
}

// TestTreeAgainstMap holds the tree to the behaviour of the map it replaced,
// over key sets built to collide on prefixes.
func TestTreeAgainstMap(t *testing.T) {
	shapes := []struct {
		name string
		key  func(i int) string
	}{
		{"shared prefix", func(i int) string { return fmt.Sprintf("user:%08d:profile", i) }},
		{"short", func(i int) string { return fmt.Sprintf("%d", i) }},
		{"random bytes", func(i int) string {
			b := make([]byte, 1+rand.Intn(24))
			rand.Read(b)
			return string(b)
		}},
		{"nested", func(i int) string { return strings.Repeat("x", i%40) }},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			rand.Seed(1)

			want := make(map[string]int64)
			var tree Tree
			var data []byte

			for i := 0; i < 2000; i++ {
				key := shape.key(i)
				off := int64(len(data))
				data = append(data, key...)
				tree.Insert(data, off, len(key), int64(i))
				want[key] = int64(i)
			}

			if tree.Len() != len(want) {
				t.Errorf("Len is %d, want %d", tree.Len(), len(want))
			}

			for key, value := range want {
				got, ok := tree.Lookup(data, []byte(key))
				if !ok {
					t.Fatalf("key %q missing", key)
				}
				if got != value {
					t.Fatalf("key %q: got %d, want %d", key, got, value)
				}
			}

			// A full walk must produce every key exactly once, in byte order.
			var walked []int64
			tree.WalkPrefix(data, nil, func(value int64) bool {
				walked = append(walked, value)
				return true
			})
			if len(walked) != len(want) {
				t.Fatalf("walk produced %d keys, want %d", len(walked), len(want))
			}

			keys := make([]string, 0, len(want))
			for key := range want {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			// walked holds the value stored for each key; map it back through want.
			for i, value := range walked {
				if want[keys[i]] != value {
					t.Fatalf("walk position %d: got value %d, want %d (key %q)", i, value, want[keys[i]], keys[i])
				}
			}
		})
	}
}

// FuzzTree checks the tree against a map for arbitrary keys.
func FuzzTree(f *testing.F) {
	f.Add([]byte("a\nab\nabc"), []byte("ab"))
	f.Add([]byte("\x00\n\x00\x01"), []byte(""))
	f.Add([]byte(""), []byte("x"))

	f.Fuzz(func(t *testing.T, joined, prefix []byte) {
		// Newline splits the input into keys, so no key here contains one; the
		// tree itself puts no restriction on what a key may hold.
		keys := strings.Split(string(joined), "\n")

		want := make(map[string]int64)
		var tree Tree
		var data []byte
		for i, key := range keys {
			off := int64(len(data))
			data = append(data, key...)
			tree.Insert(data, off, len(key), int64(i))
			want[key] = int64(i)
		}

		if tree.Len() != len(want) {
			t.Fatalf("Len is %d, want %d", tree.Len(), len(want))
		}

		for key, value := range want {
			got, ok := tree.Lookup(data, []byte(key))
			if !ok || got != value {
				t.Fatalf("key %q: got %d, %v, want %d", key, got, ok, value)
			}
		}

		// WalkPrefix must find exactly the keys a filter over the map would.
		matched := 0
		for key := range want {
			if strings.HasPrefix(key, string(prefix)) {
				matched++
			}
		}

		walked := 0
		tree.WalkPrefix(data, prefix, func(int64) bool {
			walked++
			return true
		})
		if walked != matched {
			t.Fatalf("prefix %q: walked %d keys, %d match", prefix, walked, matched)
		}
	})
}

// keyShapes are the key sets the index benchmarks and the memory report run
// over. How much of a key is shared with its neighbours is what decides both
// the depth of the tree and how much it saves over a map.
var keyShapes = []struct {
	name string
	key  func(i int) string
}{
	{"shared-prefix", func(i int) string { return fmt.Sprintf("user:%08d:profile", i) }},
	{"path-like", func(i int) string { return fmt.Sprintf("/var/log/service-%d/%d.log", i%8, i) }},
	{"long-shared", func(i int) string {
		return fmt.Sprintf("/organisations/acme/projects/backend/environments/production/services/api/instances/%06d", i)
	}},
	{"random", func(i int) string {
		b := make([]byte, 20)
		rand.New(rand.NewSource(int64(i))).Read(b)
		return string(b)
	}},
}

const indexBenchKeys = 100_000

func shapeKeys(key func(int) string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = key(i)
	}
	return keys
}

func BenchmarkIndexLookup(b *testing.B) {
	for _, shape := range keyShapes {
		keys := shapeKeys(shape.key, indexBenchKeys)
		byteKeys := make([][]byte, len(keys))
		for i, key := range keys {
			byteKeys[i] = []byte(key)
		}

		b.Run(shape.name+"/tree", func(b *testing.B) {
			tree, data := indexKeys(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := tree.Lookup(data, byteKeys[i%len(byteKeys)]); !ok {
					b.Fatal("missing key")
				}
			}
		})

		b.Run(shape.name+"/map", func(b *testing.B) {
			index := make(map[string]int64, len(keys))
			for i, key := range keys {
				index[key] = int64(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := index[string(byteKeys[i%len(byteKeys)])]; !ok {
					b.Fatal("missing key")
				}
			}
		})
	}
}

func BenchmarkIndexInsert(b *testing.B) {
	for _, shape := range keyShapes {
		keys := shapeKeys(shape.key, indexBenchKeys)

		// The data slice is built once; both indexes just record positions in it.
		var data []byte
		offs := make([]int64, len(keys))
		for i, key := range keys {
			offs[i] = int64(len(data))
			data = append(data, key...)
		}

		b.Run(shape.name+"/tree", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var tree Tree
				for j, key := range keys {
					tree.Insert(data, offs[j], len(key), offs[j])
				}
			}
		})

		b.Run(shape.name+"/map", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				index := make(map[string]int64)
				for j, key := range keys {
					// As the store did it: the key comes out of Data, so the map
					// has to copy it. That copy is the allocation the tree avoids.
					index[string(data[offs[j]:offs[j]+int64(len(key))])] = offs[j]
				}
			}
		})
	}
}

// treeBytes adds up what the tree's nodes and edge slices occupy. Counting the
// structure is exact, where sampling the heap is not: HeapAlloc deltas around a
// build under-report by enough to be useless here.
func treeBytes(tree *Tree) uintptr {
	var total uintptr

	var visit func(n *node)
	visit = func(n *node) {
		total += unsafe.Sizeof(node{}) + uintptr(cap(n.edges))*unsafe.Sizeof(edge{})
		for i := range n.edges {
			visit(n.edges[i].node)
		}
	}

	if tree.root != nil {
		visit(tree.root)
	}

	return total
}

// TestIndexMemory reports what each index costs per key. Run it with -v; it
// asserts nothing, because the answer depends entirely on the key shape.
//
// The map is preallocated so that it allocates nothing it then throws away,
// which makes TotalAlloc over the build equal to what it retains.
func TestIndexMemory(t *testing.T) {
	for _, shape := range keyShapes {
		keys := shapeKeys(shape.key, indexBenchKeys)

		var data []byte
		offs := make([]int64, len(keys))
		for i, key := range keys {
			offs[i] = int64(len(data))
			data = append(data, key...)
		}

		var tree Tree
		for j, key := range keys {
			tree.Insert(data, offs[j], len(key), offs[j])
		}

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		index := make(map[string]int64, len(keys))
		for j, key := range keys {
			// As the store built it: the key comes out of Data, so the map has to
			// copy it. The tree points at those same bytes and copies nothing.
			index[string(data[offs[j]:offs[j]+int64(len(key))])] = offs[j]
		}
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(index)

		nodes := 0
		tree.WalkPrefix(data, nil, func(int64) bool { nodes++; return true })

		t.Logf("%-14s %d keys of %d bytes: tree %5.1f B/key, map %5.1f B/key",
			shape.name, len(keys), len(keys[0]),
			float64(treeBytes(&tree))/float64(len(keys)),
			float64(after.TotalAlloc-before.TotalAlloc)/float64(len(keys)))
	}
}
