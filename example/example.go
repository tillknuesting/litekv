package main

import (
	"fmt"
	"log"

	"github.com/tillknuesting/litekv"
)

func main() {
	kvs := &litekv.KeyValueStore{}

	// Write a key-value pair to the store
	kvs.Write([]byte("foo"), []byte("bar"))

	// Export the index, which can be saved to disk for persistence
	// The index maps keys to their positions within the store
	indexExported, err := kvs.SaveIndex()
	if err != nil {
		log.Fatalln(err)
	}
	// Import the index back into the store, allowing for efficient lookups
	err = kvs.LoadIndex(indexExported)
	if err != nil {
		log.Fatalln(err)
	}

	// Read the value associated with the key "foo"
	v, err := kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	// Rebuild the index from the current data, in case the index is lost or corrupted
	kvs.RebuildIndex()

	// Update the value associated with the key "foo"
	kvs.Write([]byte("foo"), []byte("newValue"))

	// Read the updated value associated with the key "foo"
	v, err = kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	// Delete the key-value pair with the key "foo"
	kvs.Delete([]byte("foo"))

	// Attempt to read the deleted key-value pair
	v, err = kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	// Print all key-value pairs before compaction
	// Compaction removes duplicates and deleted key-value pairs
	fmt.Println("All key = Val before compaction:")
	kvs.PrintAllKeyValuePairs()

	// Perform compaction on the KeyValueStore
	kvs.Compact()

	// Print all key-value pairs after compaction
	fmt.Println("All key = Val after compaction:")
	kvs.PrintAllKeyValuePairs()
}
