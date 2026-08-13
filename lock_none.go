//go:build !(linux || darwin || freebsd || openbsd || netbsd || dragonfly)

package litekv

// lockingEnforced is false here, which is what the tests for a second process
// being kept out skip on. See its other definition in lock_flock.go.
const lockingEnforced = false

// lockFile does nothing on a platform this package cannot lock on, and says so
// by leaving no file behind either.
//
// The list in the build constraint is not "unix": syscall.Flock is missing on
// solaris and aix, which the unix tag covers, and LockFileEx is missing from
// the standard library on Windows — it lives in golang.org/x/sys, and this
// module has no dependencies and is not taking its first one for a lock file.
// So Windows, solaris, aix, plan9 and wasm all land here.
//
// Two choices were available and this is the less bad one. Refusing to open at
// all would take away platforms that work today to gain a guarantee those
// platforms cannot give, which is a regression dressed as safety. Opening
// without the lock leaves them exactly where every platform was before this
// existed, which the README says plainly rather than implying the lock is
// everywhere.
//
// No file is created, deliberately. A LOCK sitting in the directory that locks
// nothing is worse than no LOCK at all: it is a thing an operator would
// reasonably read as protection.
func lockFile(string) (diskLock, error) { return unlockedStore{}, nil }

type unlockedStore struct{}

func (unlockedStore) Unlock() error { return nil }
