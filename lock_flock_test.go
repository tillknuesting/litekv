//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package litekv

import (
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
)

// TestOnlyAContendedLockIsErrorLocked. ErrorLocked is a message to a person:
// something else has this directory, go and find it. Every other way of failing
// to take a lock has to keep its own error, or that person is sent looking for
// a process that is not there.
//
// The one that matters in practice — the file could not be opened at all — is
// covered by TestALockThatCannotBeTakenIsNotReportedAsLocked, which is a real
// directory with its permissions taken away. This covers the arm below it,
// where flock itself refused for a reason that is not contention. That cannot
// be arranged on a descriptor the process has just opened, so the translation
// is asked directly rather than through a fault nobody can produce.
func TestOnlyAContendedLockIsErrorLocked(t *testing.T) {
	for _, c := range []struct {
		name     string
		err      error
		conflict bool
	}{
		{"contention", syscall.EWOULDBLOCK, true},
		{"contention, wrapped", os.NewSyscallError("flock", syscall.EWOULDBLOCK), true},
		{"no permission", syscall.EACCES, false},
		{"not a descriptor", syscall.EBADF, false},
		{"interrupted", syscall.EINTR, false},
		{"out of memory", syscall.ENOMEM, false},
		{"not an errno at all", io.ErrUnexpectedEOF, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := lockConflict(c.err); got != c.conflict {
				t.Errorf("lockConflict(%v) = %v, want %v", c.err, got, c.conflict)
			}
		})
	}

	// EAGAIN and EWOULDBLOCK are the same number on every platform in the
	// build constraint above, and flock is documented against EWOULDBLOCK.
	// Asserted rather than assumed, because if they ever differ somewhere this
	// says so instead of the lock quietly reporting the wrong error there.
	if !errors.Is(syscall.EAGAIN, syscall.EWOULDBLOCK) {
		t.Error("EAGAIN and EWOULDBLOCK differ here; lockConflict has to take both")
	}
}
