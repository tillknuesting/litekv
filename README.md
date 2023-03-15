# LiteKV
LiteKV is a simple, lightweight, and efficient in-memory key-value store written in Go.
It supports basic operations like reading, writing, deleting, and updating key-value pairs,
as well as advanced features like exporting and importing index, rebuilding index,
and compacting the store. LiteKV uses an append-only data structure,
which provides better write performance and data durability.

## Technical Aspects
### Append-only Storage
LiteKV stores data in an append-only manner, which means that new key-value pairs are always added
to the end of the store, even when updating existing keys. This approach provides better write performance
and ensures that data remains intact even in the case of a crash or failure.
### Compaction
As the store grows, it may accumulate duplicate or deleted entries, which can affect performance and
memory usage. LiteKV provides a compaction feature that removes duplicates and deleted key-value pairs,
making the store more efficient.

To compact the store, simply call the Compact method:
```go
kvs.Compact()
```
### Binary Storage Format

LiteKV uses a custom binary format to store key-value pairs. 
Each entry consists of a checksum, record type, key length, value length, key, and value. 
The checksum is calculated using the CRC32 algorithm and helps in detecting corruption or 
inconsistencies in the data. The record type indicates whether the entry is a normal key-value pair
or a deleted one.

LiteKV uses byte slices as the underlying data format for storing key-value pairs. Byte slices offer 
versatility and flexibility, making it easier to perform various operations such as saving data to disk or using POSIX shared memory.

By using byte slices, LiteKV allows you to seamlessly integrate the stored data with various storage
solutions or inter-process communication methods, enhancing the overall usability and adaptability of the
library in different use cases.


## Getting Started

To use LiteKV, first import the library:
```go
import (
"github.com/tillknuesting/litekv"
)
```
Then, create a new instance of KeyValueStore:
```go
kvs := &litekv.KeyValueStore{}
```
You can now perform basic operations on the store:
```go
kvs.Write([]byte("foo"), []byte("bar"))
value, err := kvs.Read([]byte("foo"))
kvs.Delete([]byte("foo"))
```
## Running Tests

To run the tests for LiteKV, navigate to the project directory and execute:
```go
go test ./...
```
## Running Benchmarks
To run the benchmarks for LiteKV, navigate to the project directory and execute:
```go
go test -bench=. ./...
```

## Fuzz Testing
Fuzz testing is a powerful technique to uncover potential issues in your code by providing
a wide range of random inputs.

To run fuzz tests for LiteKV, navigate to the project directory and execute:
```go
go test -fuzz .
```


