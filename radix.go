package litekv

import "bytes"

// Tree is a radix tree that maps keys to the offset of the record holding them.
// It replaces the map that used to index the store, and buys the store ordered
// traversal: the keys under a prefix can be found without looking at every key,
// which a map cannot do.
//
// The tree holds no key bytes of its own. Every key it indexes is already
// stored in the Data slice of the store, so a node records where its slice of
// the key lives rather than copying it, and every method takes the Data slice
// to resolve those references against. Indexing a key therefore allocates
// nodes and never key bytes, and the tree stays valid when Data is reallocated
// by an append, because offsets survive what pointers would not.
//
// The consequence is that a Tree only means anything alongside the exact Data
// slice its offsets came from. Replacing Data means rebuilding the tree.
//
// The zero value is an empty tree ready for use.
type Tree struct {
	root  *node
	count int
}

// node is one point in the tree. Its prefix is the run of key bytes shared by
// everything beneath it, which is what keeps the tree shallow: a key of n bytes
// costs one node per branching point rather than one node per byte.
//
// Whether a node carries a value is a flag rather than a reserved offset, so
// that every int64 stays a usable value. It costs nothing: the field sits in
// padding the struct had anyway.
type node struct {
	prefixOff int64 // where this node's slice of the key starts in Data
	value     int64 // record offset for the key ending here, if hasValue
	edges     []edge
	prefixLen int32
	hasValue  bool
}

// edge links a node to a child. The label repeats the first byte of the child's
// prefix so that a lookup can choose a branch without touching the child, and
// edges are kept sorted by it so that a walk comes out in byte order.
type edge struct {
	label byte
	node  *node
}

// Len returns the number of keys in the tree.
func (t *Tree) Len() int { return t.count }

// prefix returns the node's slice of the key, resolved against data.
func (n *node) prefix(data []byte) []byte {
	return data[n.prefixOff : n.prefixOff+int64(n.prefixLen)]
}

// edge finds the child to follow for label.
func (n *node) edge(label byte) *node {
	// Most nodes branch a handful of ways, where a scan beats a search; the
	// wide ones, near the root of a set of keys that vary early, do not.
	if len(n.edges) <= 8 {
		for i := range n.edges {
			if n.edges[i].label == label {
				return n.edges[i].node
			}
		}
		return nil
	}

	i := n.searchEdges(label)
	if i < len(n.edges) && n.edges[i].label == label {
		return n.edges[i].node
	}
	return nil
}

// searchEdges returns the position where label is or would be inserted.
func (n *node) searchEdges(label byte) int {
	lo, hi := 0, len(n.edges)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if n.edges[mid].label < label {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// addEdge inserts a child, keeping the edges sorted by label.
func (n *node) addEdge(label byte, child *node) {
	i := n.searchEdges(label)
	n.edges = append(n.edges, edge{})
	copy(n.edges[i+1:], n.edges[i:])
	n.edges[i] = edge{label: label, node: child}
}

// Insert indexes the key held at data[keyOff:keyOff+keyLen], recording value as
// its record offset and replacing any offset already stored for that key.
//
// The key must be the one stored in data at that position: the tree reads those
// bytes both now and on every later lookup.
func (t *Tree) Insert(data []byte, keyOff int64, keyLen int, value int64) {
	if t.root == nil {
		// The root spans no bytes, so every key starts by matching it.
		t.root = &node{}
	}

	n := t.root
	rest := data[keyOff : keyOff+int64(keyLen)]

	for {
		prefix := n.prefix(data)
		shared := commonPrefixLen(prefix, rest)

		// The key diverges part way through this node, so the node has to break
		// in two: the shared head stays where the parent points, and the tail
		// becomes a child holding everything that used to hang off the node.
		if shared < len(prefix) {
			tail := &node{
				prefixOff: n.prefixOff + int64(shared),
				prefixLen: n.prefixLen - int32(shared),
				value:     n.value,
				hasValue:  n.hasValue,
				edges:     n.edges,
			}
			n.prefixLen = int32(shared)
			n.value = 0
			n.hasValue = false
			n.edges = nil
			n.addEdge(prefix[shared], tail)
		}

		rest = rest[shared:]

		// The key ends here, so this node carries its offset.
		if len(rest) == 0 {
			if !n.hasValue {
				t.count++
			}
			n.value = value
			n.hasValue = true
			return
		}

		if child := n.edge(rest[0]); child != nil {
			n = child
			continue
		}

		// Nothing shares the rest of the key: hang it off this node whole.
		n.addEdge(rest[0], &node{
			prefixOff: keyOff + int64(keyLen-len(rest)),
			prefixLen: int32(len(rest)),
			value:     value,
			hasValue:  true,
		})
		t.count++
		return
	}
}

// Lookup returns the record offset stored for key.
func (t *Tree) Lookup(data, key []byte) (int64, bool) {
	n := t.root
	rest := key

	for n != nil {
		prefix := n.prefix(data)
		if !bytes.HasPrefix(rest, prefix) {
			return 0, false
		}
		rest = rest[len(prefix):]

		if len(rest) == 0 {
			if !n.hasValue {
				return 0, false
			}
			return n.value, true
		}

		n = n.edge(rest[0])
	}

	return 0, false
}

// WalkPrefix calls fn with the record offset of every key that starts with
// prefix, in ascending byte order, and stops early if fn returns false. An
// empty prefix walks every key. It reports whether the walk ran to the end.
func (t *Tree) WalkPrefix(data, prefix []byte, fn func(value int64) bool) bool {
	n := t.root
	rest := prefix

	for n != nil {
		if len(rest) == 0 {
			return n.walk(fn)
		}

		nodePrefix := n.prefix(data)

		// The prefix runs out inside this node. Everything below it matches, as
		// long as the node's own bytes agree so far.
		if len(rest) < len(nodePrefix) {
			if !bytes.HasPrefix(nodePrefix, rest) {
				return true
			}
			return n.walk(fn)
		}

		if !bytes.HasPrefix(rest, nodePrefix) {
			return true
		}
		rest = rest[len(nodePrefix):]

		if len(rest) == 0 {
			return n.walk(fn)
		}

		n = n.edge(rest[0])
	}

	return true
}

// walk visits this node and everything below it in byte order. A node's own key
// is shorter than any key beneath it, so it comes first.
func (n *node) walk(fn func(value int64) bool) bool {
	if n.hasValue && !fn(n.value) {
		return false
	}

	for i := range n.edges {
		if !n.edges[i].node.walk(fn) {
			return false
		}
	}

	return true
}

// commonPrefixLen returns the length of the longest run of bytes that a and b
// begin with.
func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}

	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}

	return n
}
