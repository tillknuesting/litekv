package litekv

import (
	"io"
	"os"
)

// diskFile is the part of *os.File this package uses. Nothing here needs a real
// file beyond these, so a test can stand in for one.
type diskFile interface {
	io.ReaderAt
	io.WriterAt
	io.Writer
	io.Closer

	Truncate(size int64) error
	Sync() error
	Stat() (os.FileInfo, error)
}

// openDisk opens a file, and is the one place this package does.
//
// It is a variable so that a test can watch what the package asks of the disk
// and in what order, or make one of those asks fail. Some of what this package
// promises is about ordering — that a log is synced before it is closed, that a
// hint is removed before the log it describes is replaced — and ordering is not
// something the result of an operation shows. Only watching does.
//
// Replacing it is for tests in this package, which do not run in parallel.
var openDisk = func(name string, flag int, perm os.FileMode) (diskFile, error) {
	return os.OpenFile(name, flag, perm)
}
