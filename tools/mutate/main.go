// Command mutate breaks the code on purpose, one change at a time, and sees
// whether a test notices.
//
//	go run ./tools/mutate                 # all of them
//	go run ./tools/mutate replica batch   # only those whose name matches
//
// Every mutation must be caught. A mutation that survives is a promise the code
// makes that nothing is holding it to. Every one of these must be caught: the
// survivors that are allowed on purpose all belonged to the server, and left
// with it.
//
// # Why this is parallel, and why each worker gets its own copy of the tree
//
// The suite takes forty-five seconds under -race. A sweep of sixty-six
// mutations of the server once took twenty-five minutes, and the difference is
// the thing worth knowing: a caught mutation is usually caught by a test
// waiting out a deadline.
// TestOneSmallRecordArrivesAtOnce gives a record fifteen seconds to arrive;
// waitForPositions gives a follower thirty. Those numbers are right — a shorter
// deadline is a test that fails on a busy machine and says nothing about the
// store, which AGENTS.md has a trap about — so the sweep is slow for the same
// reason the tests are trustworthy.
//
// What that means is that a sweep is nearly all waiting, and waiting
// parallelises. Each worker gets its own copy of the repository because a
// mutation is an edit to a file, and eight of those in one directory is eight
// mutations at once in a way nobody could read the results of.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mutate:", err)
		os.Exit(1)
	}
}

// workers is how many trees are swept at once. More than the cores buys
// nothing: the waiting is on timers, but the test binary that runs between the
// waits is not free, and neither is the compile after each edit.
func workers() int {
	return max(1, min(8, runtime.NumCPU()-2))
}

func run(only []string) error {
	root, err := repository()
	if err != nil {
		return err
	}

	chosen := choose(only)
	if len(chosen) == 0 {
		return fmt.Errorf("nothing matched %q", only)
	}

	// Beside the repository rather than in the system temporary directory. Two
	// runs lost half their mutations to "go.mod file not found", which is a
	// worker whose tree was reaped under it — some environments clean $TMPDIR
	// on a timer, and a ten-minute sweep is long enough to be caught by one.
	pool := filepath.Join(root, ".mutate")
	os.RemoveAll(pool)
	if err := os.MkdirAll(pool, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(pool)

	started := time.Now()
	results := sweep(root, pool, chosen)

	missed := map[string]string{}
	for name, verdict := range results {
		if strings.HasPrefix(verdict, "SURVIVED") ||
			strings.HasPrefix(verdict, "SKIPPED") ||
			strings.HasPrefix(verdict, "TIMED OUT") {
			missed[name] = verdict
		}
	}

	fmt.Printf("\n%d/%d caught in %.0fs on %d workers\n",
		len(results)-len(missed), len(chosen), time.Since(started).Seconds(), workers())

	// Counted against what was asked for, not against what finished. A runner
	// that died half way through once reported nothing at all.
	if len(results) != len(chosen) {
		return fmt.Errorf("only %d of %d mutations reported", len(results), len(chosen))
	}

	if len(missed) > 0 {
		fmt.Println("\nnot caught:")
		names := make([]string, 0, len(missed))
		for name := range missed {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Printf("  %-40s %s\n", missed[name], name)
		}
		if len(missed) == 1 {
			return fmt.Errorf("1 mutation was not caught")
		}
		return fmt.Errorf("%d mutations were not caught", len(missed))
	}
	return nil
}

// choose is the mutations whose name matches any of the filters, or all of them
// when there are none.
func choose(only []string) []mutation {
	if len(only) == 0 {
		return mutations
	}

	var chosen []mutation
	for _, m := range mutations {
		for _, want := range only {
			if strings.Contains(m.name, want) {
				chosen = append(chosen, m)
				break
			}
		}
	}
	return chosen
}

// sweep runs every mutation across the workers and returns each one's verdict
// by name, printing them as they land.
func sweep(root, pool string, chosen []mutation) map[string]string {
	queue := make(chan mutation)
	go func() {
		defer close(queue)
		for _, m := range chosen {
			queue <- m
		}
	}()

	var (
		mu      sync.Mutex
		results = make(map[string]string, len(chosen))
		done    int
	)

	var running sync.WaitGroup
	for w := range workers() {
		running.Add(1)
		go func() {
			defer running.Done()

			// One tree per worker, named for the worker and not for the
			// mutation, so that the copy is paid for once per worker rather
			// than once per mutation.
			where := filepath.Join(pool, fmt.Sprintf("w%d", w))

			for m := range queue {
				verdict := attempt(root, where, m)

				// Printed as they land rather than at the end, so a sweep that
				// is going badly says so while it is still going.
				mu.Lock()
				done++
				results[m.name] = verdict
				fmt.Printf("[%3d/%d] %-58s %s\n", done, len(chosen), verdict, m.name)
				mu.Unlock()
			}
		}()
	}
	running.Wait()

	return results
}

// attempt breaks one thing, runs the tests, and puts the file back.
func attempt(root, where string, m mutation) string {
	if err := tree(root, where); err != nil {
		return "SKIPPED (no tree: " + err.Error() + ")"
	}

	full, pkg := locate(where, m.file)

	source, err := os.ReadFile(full)
	if err != nil {
		return "SKIPPED (" + err.Error() + ")"
	}

	// Not a pass. An exact-text pattern that matches nothing or twice is a
	// mutation that never ran, and it is the commonest way a sweep quietly
	// stops testing what it says it tests.
	if found := strings.Count(string(source), m.old); found != 1 {
		return fmt.Sprintf("SKIPPED (matched %d times)", found)
	}

	broken := strings.Replace(string(source), m.old, m.new, 1)
	if err := os.WriteFile(full, []byte(broken), 0o644); err != nil {
		return "SKIPPED (" + err.Error() + ")"
	}
	defer os.WriteFile(full, source, 0o644)

	return verdict(where, pkg)
}

// verdict builds and tests a tree that has already been broken.
func verdict(where, pkg string) string {
	// go vet is the gate rather than go build because vet catches the
	// unreachable code a clumsy mutation leaves behind — and because a mutation
	// that does not typecheck did not run either. It is the package the mutation
	// is in and not a fixed one, which it used to be: a mutation outside that
	// package was only ever type-checked as somebody else's dependency, so vet's
	// own analyses never looked at it.
	if out, err := goCommand(where, "vet", pkg); err != nil {
		// The message, not just the fact. A bare "does not build" is a mutation
		// that did not run and no way to find out why, which is half a day the
		// next person does not have.
		return "SKIPPED (does not build: " + lastLine(out, 60) + ")"
	}

	// -count=1 because the mutated tree is otherwise a cache hit away from
	// reporting the unmutated result.
	//
	// -timeout, because go test defaults to ten minutes and a mutation that
	// deadlocks the suite would spend all of it.
	//
	// -race, because some of what this package promises is only visible to the
	// detector: a heartbeat goroutine writing to a ResponseWriter after its
	// handler returned is a race and nothing else, and a sweep without the flag
	// reports it as caught by nobody. It costs about a second a mutation and
	// buys the whole class.
	out, err := goCommand(where, "test", "-race", "-count=1", "-timeout", suiteTimeout, pkg)
	if err == nil {
		return "SURVIVED"
	}

	// A deadline that fired is reported as itself and never as a catch. A test
	// binary killed by -timeout exits non-zero exactly like a failing one, so a
	// timeout set below what a suite honestly needs turns every mutation in
	// that package into a pass — a sweep that reports 100% caught and tested
	// nothing. See suiteTimeout, which is the number that must never be tuned
	// down to make a sweep finish sooner.
	if strings.Contains(out, "test timed out") {
		if hung := stillRunning(out); len(hung) > 0 {
			return "caught (hung " + strings.Join(hung, ", ") + ")"
		}
		return "TIMED OUT after " + suiteTimeout + " (raise it; this is not a catch)"
	}

	if caught := failed(out); len(caught) > 0 {
		return "caught by " + strings.Join(caught, ", ")
	}

	// Failed without naming a test: a panic, or a build failure of the test
	// binary that vet did not see. Caught, but say so.
	return "caught (the package failed without naming a test)"
}

// failed is the tests that reported FAIL, at most four of them.
//
// The line is trimmed first, because a subtest's FAIL line is indented under
// its parent and an untrimmed prefix check reports "failed without naming a
// test" for every mutation that only a t.Run caught.
func failed(out string) []string {
	var caught []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "---" && fields[1] == "FAIL:" {
			caught = append(caught, fields[2])
			if len(caught) == 4 {
				break
			}
		}
	}
	return caught
}

// stillRunning is the tests Go named in its timeout dump, at most three.
//
// A test that was running when the deadline fired is a test the mutation hung,
// which is a catch and a legitimate one — the suite hangs rather than passes. A
// deadline that fired with nothing running is the deadline being too short, and
// that is not a catch at all.
//
// Go indents these by two tabs under a "running tests:" line, not one. Matching
// a single tab found nothing and reported a hang as an unraised timeout, which
// is the same kind of quiet miss this tool is otherwise careful about.
func stillRunning(out string) []string {
	var hung []string
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(line, "\t") || !strings.HasPrefix(trimmed, "Test") {
			continue
		}
		name, _, found := strings.Cut(trimmed, " (")
		if !found {
			continue
		}
		hung = append(hung, name)
		if len(hung) == 3 {
			break
		}
	}
	return hung
}

// lastLine is the final non-empty line of some output, cut to at most n
// characters, which is enough of a compiler error to act on.
func lastLine(out string, n int) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	last := strings.TrimSpace(lines[len(lines)-1])
	if last == "" {
		return "?"
	}
	if len(last) > n {
		return last[:n]
	}
	return last
}
