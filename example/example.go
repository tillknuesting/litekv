package main

import (
	"fmt"
	"log"

	"github.com/tillknuesting/litekv"
)

func main() {
	kvs := &litekv.KeyValueStore{}

	// Write a key-value pair to the store
	if err := kvs.Write([]byte("foo"), []byte("bar")); err != nil {
		log.Fatalln(err)
	}

	// Export the index, which can be saved to disk for persistence
	// The index maps keys to their positions within the store
	indexExported, err := kvs.SaveIndex()
	if err != nil {
		log.Fatalln(err)
	}
	// Import the index back into the store, allowing for efficient lookups.
	// LoadIndex validates every entry against the data, so restore the data first.
	if err := kvs.LoadIndex(indexExported); err != nil {
		log.Fatalln(err)
	}

	// Read the value associated with the key "foo"
	v, err := kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	// Rebuild the index from the current data, in case the index is lost or corrupted.
	// A *litekv.CorruptAtError here means the data has a damaged tail, for example
	// from an append cut short by a crash; the index still covers everything
	// before that offset.
	if err := kvs.RebuildIndex(); err != nil {
		log.Fatalln(err)
	}

	// Check every stored record against its checksum
	if err := kvs.Verify(); err != nil {
		log.Fatalln(err)
	}

	// Update the value associated with the key "foo"
	if err := kvs.Write([]byte("foo"), []byte("newValue")); err != nil {
		log.Fatalln(err)
	}

	// Read the updated value associated with the key "foo"
	v, err = kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	// Delete the key-value pair with the key "foo"
	if err := kvs.Delete([]byte("foo")); err != nil {
		log.Fatalln(err)
	}

	// Attempt to read the deleted key-value pair
	v, err = kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	if err := kvs.Write([]byte("foo2"), []byte("bar2")); err != nil {
		log.Fatalln(err)
	}

	// Print all key-value pairs before compaction
	// Compaction removes superseded records and deleted key-value pairs
	fmt.Println("All key = Val before compaction:")
	if err := kvs.PrintAllKeyValuePairs(); err != nil {
		log.Fatalln(err)
	}

	// Perform compaction on the KeyValueStore
	if err := kvs.Compact(); err != nil {
		log.Fatalln(err)
	}

	// Print all key-value pairs after compaction
	fmt.Println("All key = Val after compaction:")
	if err := kvs.PrintAllKeyValuePairs(); err != nil {
		log.Fatalln(err)
	}

	// Walk the store without printing it
	err = kvs.ForEach(func(key, value []byte, deleted bool) bool {
		fmt.Printf("%s = %s (deleted: %t)\n", key, value, deleted)
		return true
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Find every key under a prefix. The index is a radix tree, so this costs
	// what the matches cost rather than a walk of the whole store.
	for _, key := range []string{"user:1", "user:2", "session:1"} {
		if err := kvs.Write([]byte(key), []byte("v-"+key)); err != nil {
			log.Fatalln(err)
		}
	}

	fmt.Println("Keys under user::")
	err = kvs.PrefixScan([]byte("user:"), func(key, value []byte) bool {
		fmt.Printf("  %s = %s\n", key, value)
		return true
	})
	if err != nil {
		log.Fatalln(err)
	}
}
