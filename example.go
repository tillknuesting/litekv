package main

import (
	"fmt"
	"github.com/tillknuesting/litekv/kv"
)

func main() {
	kvs := &kv.KeyValueStore{}

	kvs.Write([]byte("foo"), []byte("bar"))

	v, err := kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	kvs.Write([]byte("foo"), []byte("newValue"))

	v, err = kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	kvs.Delete([]byte("foo"))

	v, err = kvs.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}
}
