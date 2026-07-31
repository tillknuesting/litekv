package litekv

import "bytes"

// Tree is an adaptive radix tree that maps keys to the offset of the record
// holding them. It replaces the map that used to index the store, and buys the
// store ordered traversal: the keys under a prefix can be found without looking
// at every key, which a map cannot do.
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

// A node holds its children in one of three shapes, sized to how many it has.
// This is what "adaptive" means: a node that branches two ways should not pay
// what a node that branches two hundred ways needs, and a node that branches
// two hundred ways should not be searched.
//
// Uniform nodes are what make a plain radix tree slow. Searching a sorted list
// of a hundred children means jumping around a kilobyte of memory, and each
// jump is a cache miss, on every level of every lookup.
const (
	// kindInline keeps the labels in the node itself, so finding a child reads
	// no memory beyond the node already loaded.
	kindInline = iota

	// kindIndexed keeps a 256 byte table of slots, one per possible label. One
	// load finds the slot, a second the child.
	kindIndexed

	// kindDirect indexes the children by the label itself. No search at all.
	kindDirect
)

const (
	inlineMax  = 8  // labels that fit in the node
	indexedMax = 48 // children worth indexing rather than addressing directly
)

// node is one point in the tree. Its prefix is the run of key bytes shared by
// everything beneath it, which is what keeps the tree shallow: a key of n bytes
// costs one node per branching point rather than one node per byte.
//
// Unlike the fixed prefix of the published Adaptive Radix Tree, which stores a
// handful of bytes and re-checks the rest at the leaf, this prefix is an offset
// into Data and so has no length limit. The keys are already there to point at.
type node struct {
	prefixOff int64       // where this node's slice of the key starts in Data
	value     int64       // record offset for the key ending here, if hasValue
	children  []*node     // kindDirect indexes this by label; otherwise packed
	index     *[256]uint8 // kindIndexed: label to slot, biased by one
	prefixLen int32
	nchild    uint16 // a direct node can hold 256 children, which a uint8 cannot count
	kind      uint8
	hasValue  bool
	labels    [inlineMax]byte // kindInline: the labels, sorted
}

// Len returns the number of keys in the tree.
func (t *Tree) Len() int { return t.count }

// prefix returns the node's slice of the key, resolved against data.
func (n *node) prefix(data []byte) []byte {
	return data[n.prefixOff : n.prefixOff+int64(n.prefixLen)]
}

// child returns the child to follow for label, or nil.
func (n *node) child(label byte) *node {
	switch n.kind {
	case kindInline:
		// The labels sit in the node, which the caller has already loaded, so
		// this walks bytes that are certain to be in cache.
		for i := 0; i < int(n.nchild); i++ {
			if n.labels[i] == label {
				return n.children[i]
			}
		}
		return nil

	case kindIndexed:
		if slot := n.index[label]; slot != 0 {
			return n.children[slot-1]
		}
		return nil

	default:
		return n.children[label]
	}
}

// addChild links child under label, growing the node to a wider shape when the
// current one is full.
func (n *node) addChild(label byte, child *node) {
	switch n.kind {
	case kindInline:
		if int(n.nchild) == inlineMax {
			n.growToIndexed()
			n.addChild(label, child)
			return
		}

		// Kept sorted, so that an inline node walks in byte order for free.
		at := 0
		for at < int(n.nchild) && n.labels[at] < label {
			at++
		}
		copy(n.labels[at+1:], n.labels[at:n.nchild])
		n.labels[at] = label
		n.children = append(n.children, nil)
		copy(n.children[at+1:], n.children[at:])
		n.children[at] = child

	case kindIndexed:
		if int(n.nchild) == indexedMax {
			n.growToDirect()
			n.addChild(label, child)
			return
		}
		n.children = append(n.children, child)
		n.index[label] = uint8(len(n.children)) // biased by one: 0 means empty

	default:
		n.children[label] = child
	}

	n.nchild++
}

// growToIndexed moves an inline node to a 256 slot index.
func (n *node) growToIndexed() {
	n.index = new([256]uint8)
	for i := 0; i < int(n.nchild); i++ {
		n.index[n.labels[i]] = uint8(i + 1)
	}
	n.kind = kindIndexed
}

// growToDirect moves an indexed node to one addressed by the label itself.
func (n *node) growToDirect() {
	children := make([]*node, 256)
	for label := 0; label < 256; label++ {
		if slot := n.index[label]; slot != 0 {
			children[label] = n.children[slot-1]
		}
	}
	n.children = children
	n.index = nil
	n.kind = kindDirect
}

// takeChildrenOf moves the whole child structure of src onto n, leaving src
// with none. It is how a split hands the old node's children to its tail.
func (n *node) takeChildrenOf(src *node) {
	n.children, src.children = src.children, nil
	n.index, src.index = src.index, nil
	n.labels, src.labels = src.labels, [inlineMax]byte{}
	n.nchild, src.nchild = src.nchild, 0
	n.kind, src.kind = src.kind, kindInline
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
			}
			tail.takeChildrenOf(n)

			n.prefixLen = int32(shared)
			n.value = 0
			n.hasValue = false
			n.addChild(prefix[shared], tail)
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

		if child := n.child(rest[0]); child != nil {
			n = child
			continue
		}

		// Nothing shares the rest of the key: hang it off this node whole.
		n.addChild(rest[0], &node{
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

		n = n.child(rest[0])
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

		n = n.child(rest[0])
	}

	return true
}

// walk visits this node and everything below it in byte order. A node's own key
// is shorter than any key beneath it, so it comes first.
func (n *node) walk(fn func(value int64) bool) bool {
	if n.hasValue && !fn(n.value) {
		return false
	}

	// Inline nodes hold their children sorted; the wider shapes are ordered by
	// label, so walking the labels in order puts the children in order too.
	switch n.kind {
	case kindInline:
		for i := 0; i < int(n.nchild); i++ {
			if !n.children[i].walk(fn) {
				return false
			}
		}

	case kindIndexed:
		// Both wide shapes are scanned by label, to come out in order, and both
		// stop as soon as every child has been seen: a node with ten children
		// should not cost a walk of all two hundred and fifty six slots.
		for label, seen := 0, uint16(0); label < 256 && seen < n.nchild; label++ {
			if slot := n.index[label]; slot != 0 {
				seen++
				if !n.children[slot-1].walk(fn) {
					return false
				}
			}
		}

	default:
		for label, seen := 0, uint16(0); label < 256 && seen < n.nchild; label++ {
			if child := n.children[label]; child != nil {
				seen++
				if !child.walk(fn) {
					return false
				}
			}
		}
	}

	return true
}

// commonPrefixLen returns the length of the longest run of bytes that a and b
// begin with.
func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))

	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}

	return n
}
