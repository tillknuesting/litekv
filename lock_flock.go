//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package litekv

import (
	"errors"
	"os"
	"syscall"
)

// lockingEnforced says whether this platform can actually keep a second
// process out. It is what the tests for that skip on, so that a platform
// without a lock reports the tests it did not run rather than passing them.
const lockingEnforced = true

// lockFile takes an exclusive advisory lock on the file at name.
//
// # Why a lock and not a file that is created and removed
//
// The obvious way to write this is O_CREAT|O_EXCL: create a file, fail if it is
// there, delete it on the way out. It is portable, it needs no syscall this
// package does not already make, and it is wrong for a database. A store that
// lost power comes back with the file still there and refuses to open until
// somebody notices and removes it — so the crash this whole package is built to
// survive becomes the crash that needs a human at three in the morning. The
// usual patch for that is to write the pid in and check whether it is alive,
// which is a race against pid reuse and means nothing at all in a container,
// where everything is pid 1.
//
// A lock held on a descriptor has none of that. The kernel drops it when the
// process ends, for any reason it might end, including one that ran no deferred
// function and one that never got to run again at all.
//
// # What it does not cover
//
// Two things, both in the README's Limitations.
//
// It is advisory: it excludes anything that asks for the same lock, which is
// another litekv, and nothing else. A shell redirect into a log is unaffected.
//
// It is local. On NFS, flock is emulated by POSIX locks on Linux and is local
// to the machine on several other systems, so two machines with the same export
// mounted can both believe they have it. A store on a network filesystem is
// already outside what the sync policies can promise, and this does not change
// that.
func lockFile(name string) (diskLock, error) {
	// os.OpenFile and not disk.Open: a lock is taken on a descriptor and
	// diskFile deliberately has none. See osDisk.Lock for where the seam is
	// instead.
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	// LOCK_NB, so that a second store reports which of the two it is rather
	// than waiting for a lock the first has no intention of giving up. A
	// blocking open would leave the operator with a process that has said
	// nothing and done nothing, which is the failure that is hard to read.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if lockConflict(err) {
			return nil, ErrorLocked
		}
		return nil, err
	}

	return &flockedFile{file: file}, nil
}

// lockConflict says whether this is somebody else holding the lock, as opposed
// to anything else having gone wrong.
//
// Its own function so that there is something to test. ErrorLocked means one
// thing to whoever reads it — go and find the other process — and a directory
// that could not be locked for any other reason must not send them looking for
// a process that does not exist. There is no way to make flock fail with
// anything but EWOULDBLOCK on a descriptor this code has just opened, so the
// wiring is tested by contention and the translation is tested here.
func lockConflict(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK)
}

type flockedFile struct{ file *os.File }

// Unlock releases the lock and closes the file.
//
// The close alone would release it. Asking first is so that the release is
// something this code did, in an order a test can watch, rather than a side
// effect of a descriptor going away — and so that a close that fails still
// leaves the lock gone.
func (l *flockedFile) Unlock() error {
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	if cerr := l.file.Close(); err == nil {
		err = cerr
	}
	return err
}
