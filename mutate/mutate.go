// Package mutate breaks code on purpose, one change at a time, and reports
// whether a test noticed.
//
// It is the machinery only. What to break is a caller's list, and a caller is
// a tiny main in the repository being swept:
//
//	func main() {
//		err := mutate.Run(mutations, mutate.Options{Timeout: "600s"}, os.Args[1:])
//		if err != nil {
//			fmt.Fprintln(os.Stderr, "mutate:", err)
//			os.Exit(1)
//		}
//	}
//
// It lives in this module because two repositories need it and neither should
// hold a copy — litekvd depends on this module already, so importing it costs
// nothing that was not already being paid. It is a development tool and not
// part of the storage engine; nothing in the engine imports it.
//
// # Why a sweep is parallel, and why each worker gets its own copy of the tree
//
// A caught mutation is usually caught by a test waiting out a deadline, so a
// sweep is nearly all waiting — and waiting parallelises. A suite of a few
// seconds can take twenty-five minutes to sweep sixty-six mutations in one
// process, and the deadlines are not the problem: a shorter one is a test that
// fails on a busy machine and says nothing about the code.
//
// Each worker gets its own copy of the repository because a mutation is an edit
// to a file, and eight of those in one directory is eight mutations at once in
// a way nobody could read the results of.
package mutate

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

// Mutation is one break to make, and what to call it when it is reported.
//
// Old is matched exactly and must appear exactly once in File. That is checked,
// and a pattern matching nothing or twice is reported as SKIPPED rather than
// passed over — an exact-text pattern that has rotted is the commonest way a
// sweep quietly stops testing what it says it tests.
type Mutation struct {
	// File is a path relative to the repository root, such as
	// "server/keys.go". The package to vet and test is worked out from it.
	File string

	// Name is what the mutation means, in a few words. It is what a filter
	// matches on and what a verdict is printed against, so two mutations with
	// the same name would report over each other.
	Name string

	// Old is the exact text to replace, and New is what replaces it.
	Old string
	New string
}

// Options configures a sweep.
type Options struct {
	// Timeout is how long a mutated tree gets before its tests are killed, as
	// a Go duration string for go test -timeout.
	//
	// It belongs to the caller because suites are not the same size, and it is
	// the one setting here that must never be tuned down to make a sweep finish
	// sooner: a test binary killed by the deadline exits non-zero exactly like
	// a failing one, so a timeout below what a suite honestly needs turns every
	// mutation into a reported catch and the sweep tests nothing. Give it
	// several times what the suite takes when it is the only thing running,
	// because during a sweep it will not be. Empty means ten minutes.
	Timeout string

	// Workers is how many trees are swept at once. Zero picks a default from
	// the machine. More than the cores buys nothing: the waiting is on timers,
	// but the test binary that runs between the waits is not free, and neither
	// is the compile after each edit.
	Workers int

	// Root is the repository to sweep. Empty walks up from the working
	// directory looking for a go.mod, which is what a tool run out of a
	// checkout wants.
	Root string
}

func (o Options) timeout() string {
	if o.Timeout == "" {
		return "600s"
	}
	return o.Timeout
}

func (o Options) workers() int {
	if o.Workers > 0 {
		return o.Workers
	}
	return max(1, min(8, runtime.NumCPU()-2))
}

// Run sweeps the mutations whose name contains one of only, or all of them when
// only is empty, and prints each verdict as it lands.
//
// It returns an error when anything was not caught, so that a caller can exit
// non-zero: a mutation that survives is a promise the code makes that nothing
// is holding it to.
func Run(mutations []Mutation, opts Options, only []string) error {
	root := opts.Root
	if root == "" {
		found, err := repository()
		if err != nil {
			return err
		}
		root = found
	}

	chosen := choose(mutations, only)
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
	results := sweep(root, pool, chosen, opts)

	missed := map[string]string{}
	for name, verdict := range results {
		if strings.HasPrefix(verdict, "SURVIVED") ||
			strings.HasPrefix(verdict, "SKIPPED") ||
			strings.HasPrefix(verdict, "TIMED OUT") {
			missed[name] = verdict
		}
	}

	fmt.Printf("\n%d/%d caught in %.0fs on %d workers\n",
		len(results)-len(missed), len(chosen), time.Since(started).Seconds(), opts.workers())

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
func choose(mutations []Mutation, only []string) []Mutation {
	if len(only) == 0 {
		return mutations
	}

	var chosen []Mutation
	for _, m := range mutations {
		for _, want := range only {
			if strings.Contains(m.Name, want) {
				chosen = append(chosen, m)
				break
			}
		}
	}
	return chosen
}

// sweep runs every mutation across the workers and returns each one's verdict
// by name, printing them as they land.
func sweep(root, pool string, chosen []Mutation, opts Options) map[string]string {
	queue := make(chan Mutation)
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
	for w := range opts.workers() {
		running.Add(1)
		go func() {
			defer running.Done()

			// One tree per worker, named for the worker and not for the
			// mutation, so that the copy is paid for once per worker rather
			// than once per mutation.
			where := filepath.Join(pool, fmt.Sprintf("w%d", w))

			for m := range queue {
				verdict := attempt(root, where, m, opts)

				// Printed as they land rather than at the end, so a sweep that
				// is going badly says so while it is still going.
				mu.Lock()
				done++
				results[m.Name] = verdict
				fmt.Printf("[%3d/%d] %-58s %s\n", done, len(chosen), verdict, m.Name)
				mu.Unlock()
			}
		}()
	}
	running.Wait()

	return results
}

// attempt breaks one thing, runs the tests, and puts the file back.
func attempt(root, where string, m Mutation, opts Options) string {
	if err := tree(root, where); err != nil {
		return "SKIPPED (no tree: " + err.Error() + ")"
	}

	full, pkg := locate(where, m.File)

	source, err := os.ReadFile(full)
	if err != nil {
		return "SKIPPED (" + err.Error() + ")"
	}

	if found := strings.Count(string(source), m.Old); found != 1 {
		return fmt.Sprintf("SKIPPED (matched %d times)", found)
	}

	broken := strings.Replace(string(source), m.Old, m.New, 1)
	if err := os.WriteFile(full, []byte(broken), 0o644); err != nil {
		return "SKIPPED (" + err.Error() + ")"
	}
	defer os.WriteFile(full, source, 0o644)

	return verdict(where, pkg, opts.timeout())
}

// verdict builds and tests a tree that has already been broken.
func verdict(where, pkg, timeout string) string {
	// go vet is the gate rather than go build because vet catches the
	// unreachable code a clumsy mutation leaves behind — and because a mutation
	// that does not typecheck did not run either. It is the package the
	// mutation is in and not a fixed one, which it used to be: a mutation
	// outside that package was only ever type-checked as somebody else's
	// dependency, so vet's own analyses never looked at it.
	if out, err := goCommand(where, "vet", pkg); err != nil {
		// The message, not just the fact. A bare "does not build" is a mutation
		// that did not run and no way to find out why, which is half a day the
		// next person does not have.
		return "SKIPPED (does not build: " + lastLine(out, 60) + ")"
	}

	// -count=1 because the mutated tree is otherwise a cache hit away from
	// reporting the unmutated result.
	//
	// -race, because some of what a package promises is only visible to the
	// detector: a goroutine writing to something after the call that owned it
	// returned is a race and nothing else, and a sweep without the flag reports
	// it as caught by nobody. It costs about a second a mutation and buys the
	// whole class.
	out, err := goCommand(where, "test", "-race", "-count=1", "-timeout", timeout, pkg)
	if err == nil {
		return "SURVIVED"
	}

	// A deadline that fired is reported as itself and never as a catch. See
	// Options.Timeout for why that distinction is the difference between a
	// sweep and a sweep-shaped waste of ten minutes.
	if strings.Contains(out, "test timed out") {
		if hung := stillRunning(out); len(hung) > 0 {
			return "caught (hung " + strings.Join(hung, ", ") + ")"
		}
		return "TIMED OUT after " + timeout + " (raise it; this is not a catch)"
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
