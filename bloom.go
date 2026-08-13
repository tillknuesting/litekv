package litekv

import (
	"hash/maphash"
	"math/bits"
)

// bloom is a Bloom filter over the keys of one frozen log: it answers "not
// here" for certain and "probably here" otherwise, so a lookup that it turns
// down never has to ask the index.
//
// The point of one here is not the usual point. The textbook filter exists to
// keep a lookup off the disk, and that is not what this would buy, since a
// frozen log already keeps every key in memory and answers a miss without any
// I/O at all. What it might buy is cache: the index of a million keys is about
// 59 MB and a miss in it is a walk out to memory, while a filter over the same
// keys is about 1.2 MB and fits in L2. Whether that is worth having is the
// question this exists to answer, and the answer is measured rather than
// assumed.
//
// It is blocked: every bit for a key lands in one 64-byte line, so a probe
// touches one cache line rather than scattering probes over the whole filter.
// An unblocked filter of this size would take a miss per probe and would be
// competing with the map on the map's own terms.
type bloom struct {
	blocks []bloomBlock
	mask   uint64 // len(blocks)-1, so a power of two
}

// bloomBlock is one cache line, 512 bits.
type bloomBlock [8]uint64

const (
	// bloomBits is bits per key, which sets the false-positive rate. Ten is the
	// usual choice and lands near 1%.
	bloomBits = 10

	// bloomProbes is bits set per key. Seven is optimal for ten bits in an
	// unblocked filter; six costs a little accuracy and one less dependent load
	// on every lookup, which is the trade that matters when the whole point is
	// speed.
	bloomProbes = 6
)

// bloomSeed is per process. Filters are built when a log is opened and never
// written down, so nothing depends on them hashing the same way twice.
var bloomSeed = maphash.MakeSeed()

// maybeBloom builds a filter over an index, or returns nil when the index is
// small enough that one would cost more than it saves.
//
// A filter only earns its keep once a lookup has to go to memory for its map
// bucket, and a small index never does: it stays in cache and a map lookup is
// then a few nanoseconds that six probes and a hash cannot beat. Below the
// threshold the filter is pure overhead, measured at about 8%.
func maybeBloom(index map[string]int64, min int) *bloom {
	if min < 0 || len(index) < min {
		return nil
	}
	return newBloom(index)
}

// newBloom builds a filter over the keys of an index.
func newBloom(index map[string]int64) *bloom {
	blocks := max(len(index)*bloomBits/512+1, 1)
	// A power of two, so choosing a block is a mask rather than a division.
	blocks = 1 << bits.Len(uint(blocks-1))

	b := &bloom{blocks: make([]bloomBlock, blocks), mask: uint64(blocks - 1)}
	for key := range index {
		b.add(key)
	}
	return b
}

// locate picks the block a hash belongs in and the two values its bits are
// derived from. Splitting one hash into two and stepping h1 by h2 gives the
// probes independence enough for the rate to hold, at one hash rather than six.
//
// It takes the hash rather than the key so that the two entry points below can
// hash a string and a byte slice without either having to convert: a read holds
// []byte and converting it to pass here would put an allocation on the very
// path this exists to make cheaper. maphash gives both the same value for the
// same bytes, which is what lets a filter built from the index answer a
// lookup.
func (b *bloom) locate(h uint64) (*bloomBlock, uint32, uint32) {
	block := &b.blocks[(h>>32)&b.mask]
	// h2 is forced odd so that stepping by it cannot revisit the same bit.
	return block, uint32(h), uint32(h>>16) | 1
}

func (b *bloom) add(key string) {
	block, h1, h2 := b.locate(maphash.String(bloomSeed, key))
	for i := range uint32(bloomProbes) {
		bit := (h1 + i*h2) & 511
		block[bit>>6] |= 1 << (bit & 63)
	}
}

// mayContain reports whether the key might be in the log. False is certain;
// true may be wrong, at about the rate bloomBits was chosen for.
//
// It must never answer false for a key that is present. That would not be a
// slow lookup but a lost record, since the caller takes it as ErrorKeyNotFound
// and asks no further. TestBloomHasNoFalseNegatives is the guard.
func (b *bloom) mayContain(key []byte) bool {
	block, h1, h2 := b.locate(maphash.Bytes(bloomSeed, key))
	for i := range uint32(bloomProbes) {
		bit := (h1 + i*h2) & 511
		if block[bit>>6]&(1<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

// bytes is how much memory the filter holds, for comparing against the index it
// sits in front of.
func (b *bloom) bytes() int64 { return int64(len(b.blocks)) * 64 }
