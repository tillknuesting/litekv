package main

import "github.com/tillknuesting/litekv/mutate"

// What to break, and what to call it.
//
// Each entry names a file relative to the repository root, says what the break
// means, and gives the exact text to replace and what to replace it with. The
// text is matched exactly and must appear exactly once — see the mutation traps
// in AGENTS.md for every way that goes wrong quietly.

var mutations = []mutate.Mutation{
	// --- one process owns a directory ---------------------------------------
	{
		File: "lock_flock.go",
		Name: "the lock is shared rather than exclusive",
		Old:  "syscall.LOCK_EX|syscall.LOCK_NB",
		New:  "syscall.LOCK_SH|syscall.LOCK_NB",
	},
	{
		File: "db.go",
		Name: "the lock is taken after the directory has been read",
		Old:  "\tlock, err := disk.Lock(filepath.Join(dir, lockName))\n\tif err != nil {\n\t\treturn nil, err\n\t}",
		New:  "\tif _, err := segmentIDs(dir); err != nil {\n\t\treturn nil, err\n\t}\n\tlock, err := disk.Lock(filepath.Join(dir, lockName))\n\tif err != nil {\n\t\treturn nil, err\n\t}",
	},
	{
		File: "db.go",
		Name: "an open that fails keeps the lock",
		Old:  "\tdb, err := openLocked(dir, opts, lock)\n\tif err != nil {\n\t\tlock.Unlock()\n\t\treturn nil, err\n\t}",
		New:  "\tdb, err := openLocked(dir, opts, lock)\n\tif err != nil {\n\t\treturn nil, err\n\t}",
	},
	{
		File: "db.go",
		Name: "Close lets go of the lock before it closes the logs",
		Old:  "\terr := db.closeSegments()\n",
		New:  "\tif uerr := db.lock.Unlock(); uerr != nil {\n\t\treturn uerr\n\t}\n\terr := db.closeSegments()\n",
	},
	{
		File: "db.go",
		Name: "Close never lets go of the lock",
		Old:  "\tif uerr := db.lock.Unlock(); err == nil {\n\t\terr = uerr\n\t}",
		New:  "\t_ = db.lock",
	},
	// Both filters, not one. LOCK is turned away twice over — it has no .seg
	// suffix and its name is not a number — and removing either alone changes
	// nothing, which is two checks hiding each other in the way AGENTS.md
	// describes for latestOffsets. The mutation has to take both.
	{
		File: "db.go",
		Name: "the lock file is counted as a log",
		Old:  "\t\tif entry.IsDir() || !strings.HasSuffix(name, segmentSuffix) {\n\t\t\tcontinue\n\t\t}\n\t\tid, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)\n\t\tif err != nil {\n\t\t\tcontinue // not ours\n\t\t}",
		New:  "\t\tif entry.IsDir() {\n\t\t\tcontinue\n\t\t}\n\t\tid, _ := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)",
	},
	// The errors.Is call is left standing and only its result widened: taking
	// the call out leaves "errors" imported and not used, which is a mutation
	// that does not build and therefore never ran. See the mutation traps in
	// AGENTS.md.
	{
		File: "lock_flock.go",
		Name: "any failure to lock is reported as somebody else holding it",
		Old:  "\treturn errors.Is(err, syscall.EWOULDBLOCK)\n",
		New:  "\treturn errors.Is(err, syscall.EWOULDBLOCK) || true\n",
	},
	{
		File: "lock_flock.go",
		Name: "the lock is taken but waited for rather than refused",
		Old:  "syscall.LOCK_EX|syscall.LOCK_NB",
		New:  "syscall.LOCK_EX",
	},
	// There is deliberately no mutation for dropping the explicit LOCK_UN from
	// Unlock. Closing the descriptor releases the lock by itself, so a version
	// without it behaves identically — an equivalent mutant, not a mutation,
	// and one that would sit in the survivor list forever implying a missing
	// test. The explicit call is there for the ordering, which
	// TestTheLockIsReleasedAfterTheLastLogIsClosed checks through the seam.
}
