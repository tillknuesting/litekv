package litekv

import (
	"io"
	"os"
)

// disk is how this package touches the filesystem, and the only way it does.
//
// It is a variable so that a test can watch what the package asks of the disk
// and in what order, or make one of those asks fail. Some of what this package
// promises is about ordering — that a log is synced before it is closed, that a
// hint is removed before the log it describes is replaced, that a merged log is
// renamed into place only after it is safely written — and ordering is not
// something the result of an operation shows. Only watching does. The same seam
// makes it possible to see what happens when a rename or a removal fails, which
// on a real disk means filling one or taking its permissions away.
//
// Replacing it is for tests in this package, which do not run in parallel.
var disk fileSystem = osDisk{}

// fileSystem is the handful of operations this package needs of a disk.
type fileSystem interface {
	// Open opens a file, creating it when the flags say so.
	Open(name string, flag int, perm os.FileMode) (diskFile, error)

	// Remove deletes a file. Removing one that is not there is not an error.
	Remove(name string) error

	// Rename moves a file over whatever is at the destination, atomically.
	Rename(from, to string) error

	// ReadDir lists a directory.
	ReadDir(name string) ([]os.DirEntry, error)

	// ReadFile reads a whole file.
	ReadFile(name string) ([]byte, error)

	// MkdirAll creates a directory and its parents.
	MkdirAll(name string, perm os.FileMode) error

	// Lock takes an exclusive lock on the file at name, creating it if it is
	// not there. It does not wait for a lock somebody else holds: it reports
	// ErrorLocked and leaves.
	//
	// The lock is released by Unlock, and by the process ending however it
	// ends — including a kill, a panic and losing the machine. That is the
	// whole reason it is a lock rather than a file that is created and
	// deleted: a store that will not open after a crash until somebody logs in
	// and removes something is worse than the corruption it was meant to
	// prevent.
	Lock(name string) (diskLock, error)
}

// diskLock is a lock somebody is holding on a file.
type diskLock interface {
	// Unlock releases it. Releasing one twice is the caller's mistake and not
	// something this reports.
	Unlock() error
}

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

// osDisk is the real thing.
type osDisk struct{}

func (osDisk) Open(name string, flag int, perm os.FileMode) (diskFile, error) {
	return os.OpenFile(name, flag, perm)
}

func (osDisk) Remove(name string) error {
	err := os.Remove(name)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (osDisk) Rename(from, to string) error { return os.Rename(from, to) }

func (osDisk) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(name) }

func (osDisk) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

func (osDisk) MkdirAll(name string, perm os.FileMode) error { return os.MkdirAll(name, perm) }

// Lock is the one operation here that needs a real file rather than a diskFile:
// an advisory lock is taken on a descriptor and diskFile has none, since nothing
// else in this package wants one. So the platform half lives in lock_unix.go and
// its neighbours, and this seam is where a test gets in.
func (osDisk) Lock(name string) (diskLock, error) { return lockFile(name) }
