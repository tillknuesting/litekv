package litekv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The lock exists because of one failure and it is worth naming before the
// tests: two processes with the same directory open write over each other's
// active log, and the first anybody hears of it is a checksum that does not
// match — by which point both stores are wrong and neither can say which
// records it lost. Nothing checked before this. A typo in a unit file was
// enough.

// TestASecondOpenIsRefused. The whole point, in one process.
func TestASecondOpenIsRefused(t *testing.T) {
	if !lockingEnforced {
		t.Skip("this platform opens without a lock; see lock_none.go")
	}

	dir := t.TempDir()

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	second, err := openWithin(t, dir)
	if !errors.Is(err, ErrorLocked) {
		t.Fatalf("a second open of the same directory: %v", err)
	}
	if second != nil {
		second.Close()
		t.Fatal("a refused open handed back a store anyway")
	}

	// And the store that has it is untouched by the attempt. A lock that
	// refused the second open but left the first unable to write would have
	// traded one failure for another.
	if err := db.Write([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("writing after a refused open: %v", err)
	}
	value, err := db.Read([]byte("k"))
	if err != nil || string(value) != "v" {
		t.Fatalf("reading after a refused open: %q %v", value, err)
	}
}

// openWithin opens a directory and refuses to wait longer than a request to a
// locked directory could honestly take.
//
// Every test here that expects ErrorLocked goes through it, because there are
// two ways to get the refusal wrong and only one of them is an error a caller
// ever sees. A lock asked for without LOCK_NB is not refused, it is queued —
// and the second process sits there having said nothing, holding a directory
// the first has no intention of giving up. A test that called OpenDB directly
// would hang rather than fail, which turns a mutation caught in seconds into a
// suite that has to be killed by its deadline ten minutes later.
func openWithin(t *testing.T, dir string) (*DB, error) {
	t.Helper()

	type opened struct {
		db  *DB
		err error
	}
	answered := make(chan opened, 1)
	go func() {
		db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
		answered <- opened{db, err}
	}()

	select {
	case got := <-answered:
		return got.db, got.err

	case <-time.After(15 * time.Second):
		t.Fatal("the open waited for the lock instead of being refused")
		return nil, nil
	}
}

// TestClosingLetsTheNextOpenIn. The lock is held for the life of the store and
// not a moment past it, which is what makes restarting a normal thing to do.
func TestClosingLetsTheNextOpenIn(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatalf("reopening a closed store: %v", err)
	}
	defer again.Close()

	value, err := again.Read([]byte("k"))
	if err != nil || string(value) != "v" {
		t.Fatalf("after reopening: %q %v", value, err)
	}
}

// TestClosingTwiceReleasesTheLockOnce. Close is documented as safe to call
// twice, and the second call must not release a lock it no longer holds: the
// descriptor it was taken on is closed by then, and the number that was its
// descriptor belongs to whatever the process opened next.
func TestClosingTwiceReleasesTheLockOnce(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}

	if n := watcher.count("unlock", lockName); n != 1 {
		t.Errorf("the lock was released %d times, want once", n)
	}

	// And the directory is genuinely free afterwards rather than merely
	// reported to be.
	again, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening after a double close: %v", err)
	}
	again.Close()
}

// TestAFailedOpenLetsGoOfTheLock. There is nobody to release it later: OpenDB
// handed back no store to close, so a lock kept here is a directory this
// process has shut against itself for as long as it runs. It is also the
// failure that hides, since the symptom appears at the *next* open and points
// at the wrong thing entirely.
func TestAFailedOpenLetsGoOfTheLock(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	// Every operation an open makes, one at a time, from the one after the
	// lock itself. A sweep rather than a chosen fault, because "the open
	// failed" has as many shapes as it has operations and the interesting one
	// is whichever gets added next year.
	for nth := 2; ; nth++ {
		watcher.inject(dir, nth, 0, 0)

		db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
		if err == nil {
			// Past the end of the operations an open makes: this one
			// succeeded, so there is nothing left to fail.
			db.Close()
			if nth == 2 {
				t.Fatal("no operation could be made to fail; the watcher is not in the way")
			}
			break
		}

		// Whatever went wrong, the lock is not still held. Asked by opening
		// again with the disk working, which is what the next restart does.
		watcher.inject("", 0, 0, 0)

		after, err := OpenDB(dir, DBOptions{Sync: SyncNever})
		if err != nil {
			t.Fatalf("failing operation %d of an open left the directory locked: %v", nth, err)
		}
		if err := after.Close(); err != nil {
			t.Fatal(err)
		}

		if nth > 200 {
			t.Fatal("an open makes more operations than this sweep expected")
		}
	}
}

// TestTheLockIsTakenBeforeTheDirectoryIsRead. Ordering, and it is the ordering
// that makes the lock worth anything. Two opens that each read the directory
// and only then find out about each other have already both decided which log
// to carry on writing to, and both decided the same one.
func TestTheLockIsTakenBeforeTheDirectoryIsRead(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	order := watcher.order()

	locked := slices.Index(order, "lock:"+lockName)
	if locked < 0 {
		t.Fatalf("the directory was never locked: %v", order)
	}

	// Only the mkdirall may come first — the directory has to exist before
	// there is anywhere to put the lock.
	for i, op := range order[:locked] {
		if !strings.HasPrefix(op, "mkdirall:") {
			t.Errorf("operation %d (%s) happened before the lock was taken: %v", i, op, order)
		}
	}
}

// TestTheLockIsReleasedAfterTheLastLogIsClosed. The other end of the same rule.
// A store that let go while it was still syncing would hand the directory to
// the next process mid-sentence.
func TestTheLockIsReleasedAfterTheLastLogIsClosed(t *testing.T) {
	watcher := &watchedDisk{}
	watcher.install(t)

	dir := t.TempDir()

	// Merging off, for the reason TestTheLockFileIsLeftBehindAndIgnored gives:
	// this counts logs, and a merge running underneath makes that a number
	// nobody chose.
	db, err := OpenDB(dir, DBOptions{Sync: SyncNever, SegmentSize: 512, MergeTrigger: 1})
	if err != nil {
		t.Fatal(err)
	}

	// Enough to rotate, so there is a frozen log to close as well as an active
	// one and the release has more than one thing to come after.
	for i := range 200 {
		if err := db.Write(fmt.Appendf(nil, "key%03d", i), make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
	}
	if db.Segments() < 2 {
		t.Fatalf("the store did not rotate: %d logs", db.Segments())
	}

	watcher.reset()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	order := watcher.order()
	released := slices.Index(order, "unlock:"+lockName)
	if released < 0 {
		t.Fatalf("the lock was never released: %v", order)
	}
	if released != len(order)-1 {
		t.Errorf("the lock was released before closing finished: %v", order[released:])
	}

	closes := 0
	for _, op := range order[:released] {
		if strings.HasPrefix(op, "close:") {
			closes++
		}
	}
	if closes < 2 {
		t.Errorf("only %d logs were closed before the lock went: %v", closes, order)
	}
}

// TestTheLockFileIsLeftBehindAndIgnored. It is never removed, because removing
// it is how one lock becomes two — see lockName. So every path that walks the
// directory has to step over it, and this is the check that they do rather than
// the comment saying they should.
func TestTheLockFileIsLeftBehindAndIgnored(t *testing.T) {
	dir := t.TempDir()

	// Merging off. A count of logs is only a fixed number while nothing is
	// combining them, and this test compares one across a close and reopen —
	// with merging on it passes alone and fails under the suite, which is not
	// a test of anything. It cost a mutation verdict to find: an unlock that
	// only closed the file was reported as caught here, and the failure was
	// eight logs where there had been fourteen.
	opts := DBOptions{Sync: SyncNever, SegmentSize: 512, MergeTrigger: 1}

	db, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		if err := db.Write(fmt.Appendf(nil, "key%03d", i), make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
	}
	segments, keys := db.Segments(), db.Len()
	if segments < 2 {
		t.Fatalf("the store did not rotate: %d logs", segments)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if lockingEnforced {
		if _, err := os.Stat(filepath.Join(dir, lockName)); err != nil {
			t.Fatalf("the lock file did not survive Close: %v", err)
		}
	}

	// Opened again with the file sitting there: it is not counted as a log, it
	// is not merged, and it is not swept.
	again, err := OpenDB(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()

	if got := again.Segments(); got != segments {
		t.Errorf("logs after reopening: %d, want %d", got, segments)
	}
	if got := again.Len(); got != keys {
		t.Errorf("keys after reopening: %d, want %d", got, keys)
	}

	ids, err := segmentIDs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != segments {
		t.Errorf("segmentIDs found %d logs, want %d: it is counting the lock", len(ids), segments)
	}
}

// TestALockThatCannotBeTakenIsNotReportedAsLocked. ErrorLocked means one thing
// — somebody else has it — and an operator acts on it by finding the other
// process. A directory that cannot be written to reporting the same error
// sends them looking for a process that does not exist.
func TestALockThatCannotBeTakenIsNotReportedAsLocked(t *testing.T) {
	if !lockingEnforced {
		t.Skip("this platform opens without a lock; see lock_none.go")
	}
	if os.Geteuid() == 0 {
		t.Skip("root writes to a directory whatever its mode says")
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })

	_, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err == nil {
		t.Fatal("opening a directory that cannot be written to succeeded")
	}
	if errors.Is(err, ErrorLocked) {
		t.Errorf("a directory that cannot hold a lock file reported ErrorLocked: %v", err)
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Errorf("want a permission error, got %v", err)
	}
}

// TestResettingForASnapshotKeepsTheLock. ApplySnapshot empties the directory
// and closes every log to do it, which is every ingredient of a release except
// the intent. A follower that let go of its directory in the middle of taking a
// snapshot would be a follower another process could open and write to while
// the snapshot was still arriving.
func TestResettingForASnapshotKeepsTheLock(t *testing.T) {
	if !lockingEnforced {
		t.Skip("this platform opens without a lock; see lock_none.go")
	}

	leader, follower := t.TempDir(), t.TempDir()

	source, err := OpenDB(leader, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for i := range 50 {
		if err := source.Write(fmt.Appendf(nil, "key%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	target, err := OpenDB(follower, DBOptions{Sync: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	var buf strings.Builder
	at, release, err := source.Snapshot(&buf, ReplicaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	release()

	if err := target.ApplySnapshot(at, strings.NewReader(buf.String()), ReplicaOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := openWithin(t, follower); !errors.Is(err, ErrorLocked) {
		t.Fatalf("a store that took a snapshot let go of its directory: %v", err)
	}
	if got := target.Len(); got != 50 {
		t.Errorf("keys after the snapshot: %d, want 50", got)
	}
}

// TestAnotherProcessIsKeptOutAndAKillLetsItIn is the one that tests the claim
// as it is written down: not "a second OpenDB in this process" but a second
// process, which is the arrangement that corrupts a store.
//
// It also tests the reason this is a lock on a descriptor and not a file that
// gets created and deleted. The holder is killed outright — no deferred Close,
// no signal handler, nothing that could tidy up — and the directory opens
// straight afterwards. A lock file with a pid in it fails this: it comes back
// still there, and the store refuses to start until somebody logs in.
func TestAnotherProcessIsKeptOutAndAKillLetsItIn(t *testing.T) {
	if !lockingEnforced {
		t.Skip("this platform opens without a lock; see lock_none.go")
	}

	dir := t.TempDir()

	held, stop := holdInAnotherProcess(t, dir)
	if held != dir {
		t.Fatalf("the helper reported holding %q, want %q", held, dir)
	}

	if _, err := openWithin(t, dir); !errors.Is(err, ErrorLocked) {
		t.Fatalf("another process had the directory open: %v", err)
	}

	stop()

	// Straight after, with nothing having tidied up. Retried briefly because
	// the kernel releases the lock as the process is reaped, and Wait
	// returning is not quite the same instant on every system.
	deadline := time.Now().Add(10 * time.Second)
	for {
		db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
		if err == nil {
			defer db.Close()

			// The helper's write is here, which is how this test knows a
			// process really did open the directory rather than a mistake in
			// the plumbing having produced ErrorLocked for some other reason.
			// It is also the check that a killed writer left a readable store:
			// the helper wrote under SyncNever and was killed, so what survives
			// is whatever the recovery on this open made of it.
			value, err := db.Read([]byte("held"))
			if err != nil || string(value) != "by the helper" {
				t.Errorf("the helper's record did not survive its killing: %q %v", value, err)
			}
			return
		}
		if !errors.Is(err, ErrorLocked) {
			t.Fatalf("opening after the holder was killed: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("a killed process left the directory locked behind it")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// holdInAnotherProcess starts this test binary again, pointed at helperTest,
// and waits until it reports that it has the directory. The returned function
// kills it.
func holdInAnotherProcess(t *testing.T, dir string) (string, func()) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^"+helperTest+"$", "-test.timeout=5m")
	cmd.Env = append(os.Environ(), helperEnv+"="+dir)
	cmd.Stderr = os.Stderr

	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	killed := false
	kill := func() {
		if killed {
			return
		}
		killed = true
		cmd.Process.Kill()
		cmd.Wait()
	}
	t.Cleanup(kill)

	// The helper says one line and then blocks. Reading it is what makes this
	// a test of the lock rather than of who won a race to the directory.
	lines := bufio.NewScanner(out)
	for lines.Scan() {
		if line := strings.TrimSpace(lines.Text()); strings.HasPrefix(line, helperMarker) {
			return strings.TrimPrefix(line, helperMarker), kill
		}
	}

	kill()
	t.Fatalf("the helper process never reported holding %s", dir)
	return "", func() {}
}

const (
	helperTest   = "TestHelperHoldsADirectory"
	helperEnv    = "LITEKV_TEST_HOLD_DIR"
	helperMarker = "HOLDING "
)

// TestHelperHoldsADirectory is not a test. It is the other process: run with
// helperEnv set, it opens that directory, says so, and then waits to be killed.
//
// os.Exit rather than a failure, on both paths, because a helper that reported
// through the testing package would have its output read as the parent suite's
// own and a helper that returned would release the lock through Close — which
// is the one thing this must not do.
func TestHelperHoldsADirectory(t *testing.T) {
	dir := os.Getenv(helperEnv)
	if dir == "" {
		t.Skip("the other process for TestAnotherProcessIsKeptOutAndAKillLetsItIn")
	}

	db, err := OpenDB(dir, DBOptions{Sync: SyncNever})
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper could not open", dir, err)
		os.Exit(3)
	}
	if err := db.Write([]byte("held"), []byte("by the helper")); err != nil {
		fmt.Fprintln(os.Stderr, "helper could not write", err)
		os.Exit(3)
	}

	fmt.Println(helperMarker + dir)
	os.Stdout.Sync()

	// Not select{}, which the runtime reports as a deadlock and turns into an
	// exit this test cannot tell from a crash. A sleep is a goroutine with
	// something to wait for.
	time.Sleep(4 * time.Minute)
	os.Exit(4) // the parent was meant to kill this long ago
}
