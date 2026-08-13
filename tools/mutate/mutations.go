package main

// What to break, and what to call it.
//
// Each entry names a file, says what the break means, and gives the exact text
// to replace and what to replace it with. The text is matched exactly and must
// appear exactly once — see the mutation traps in AGENTS.md for every way that
// goes wrong quietly.
//
// A bare file name is a file under server/. A name with a slash is relative to
// the repository, which is how an engine file is written: "./db.go". See locate
// in main.go for why the two are not interchangeable.

type mutation struct {
	file string
	name string
	old  string
	new  string
}

var mutations = []mutation{
	// --- one process owns a directory ---------------------------------------
	// Engine files, named with a leading ./ — see locate() in mutate.py. These
	// run the engine's suite, which is fifteen times the server's, so a sweep
	// that includes them is minutes rather than seconds.
	{
		file: "lock_flock.go",
		name: "the lock is shared rather than exclusive",
		old:  "syscall.LOCK_EX|syscall.LOCK_NB",
		new:  "syscall.LOCK_SH|syscall.LOCK_NB",
	},
	{
		file: "db.go",
		name: "the lock is taken after the directory has been read",
		old:  "\tlock, err := disk.Lock(filepath.Join(dir, lockName))\n\tif err != nil {\n\t\treturn nil, err\n\t}",
		new:  "\tif _, err := segmentIDs(dir); err != nil {\n\t\treturn nil, err\n\t}\n\tlock, err := disk.Lock(filepath.Join(dir, lockName))\n\tif err != nil {\n\t\treturn nil, err\n\t}",
	},
	{
		file: "db.go",
		name: "an open that fails keeps the lock",
		old:  "\tdb, err := openLocked(dir, opts, lock)\n\tif err != nil {\n\t\tlock.Unlock()\n\t\treturn nil, err\n\t}",
		new:  "\tdb, err := openLocked(dir, opts, lock)\n\tif err != nil {\n\t\treturn nil, err\n\t}",
	},
	{
		file: "db.go",
		name: "Close lets go of the lock before it closes the logs",
		old:  "\terr := db.closeSegments()\n",
		new:  "\tif uerr := db.lock.Unlock(); uerr != nil {\n\t\treturn uerr\n\t}\n\terr := db.closeSegments()\n",
	},
	{
		file: "db.go",
		name: "Close never lets go of the lock",
		old:  "\tif uerr := db.lock.Unlock(); err == nil {\n\t\terr = uerr\n\t}",
		new:  "\t_ = db.lock",
	},
	// Both filters, not one. LOCK is turned away twice over — it has no .seg
	// suffix and its name is not a number — and removing either alone changes
	// nothing, which is two checks hiding each other in the way AGENTS.md
	// describes for latestOffsets. The mutation has to take both.
	{
		file: "db.go",
		name: "the lock file is counted as a log",
		old:  "\t\tif entry.IsDir() || !strings.HasSuffix(name, segmentSuffix) {\n\t\t\tcontinue\n\t\t}\n\t\tid, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)\n\t\tif err != nil {\n\t\t\tcontinue // not ours\n\t\t}",
		new:  "\t\tif entry.IsDir() {\n\t\t\tcontinue\n\t\t}\n\t\tid, _ := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)",
	},
	// The errors.Is call is left standing and only its result widened: taking
	// the call out leaves "errors" imported and not used, which is a mutation
	// that does not build and therefore never ran. See the mutation traps in
	// AGENTS.md.
	{
		file: "lock_flock.go",
		name: "any failure to lock is reported as somebody else holding it",
		old:  "\treturn errors.Is(err, syscall.EWOULDBLOCK)\n",
		new:  "\treturn errors.Is(err, syscall.EWOULDBLOCK) || true\n",
	},
	{
		file: "lock_flock.go",
		name: "the lock is taken but waited for rather than refused",
		old:  "syscall.LOCK_EX|syscall.LOCK_NB",
		new:  "syscall.LOCK_EX",
	},
	// There is deliberately no mutation for dropping the explicit LOCK_UN from
	// Unlock. Closing the descriptor releases the lock by itself, so a version
	// without it behaves identically — an equivalent mutant, not a mutation,
	// and one that would sit in the survivor list forever implying a missing
	// test. The explicit call is there for the ordering, which
	// TestTheLockIsReleasedAfterTheLastLogIsClosed checks through the seam.
}
