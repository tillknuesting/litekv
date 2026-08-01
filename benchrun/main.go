// Command benchrun runs the benchmark suite the way this repository trusts
// numbers: the whole suite, start to finish, several times over, rather than
// each benchmark repeated back to back.
//
// The difference is not pedantry. "go test -count=10" finishes one benchmark
// before starting the next, so a machine that warms up over the session hands
// its early benchmarks a cold clock and its later ones a hot one, and the drift
// lands as a bias inside each result. Alternating spreads every benchmark's
// samples across the whole session, so the same drift lands as noise in all of
// them, where benchstat can see it and say so.
//
// Usage:
//
//	go run ./benchrun                        # ten passes of everything
//	go run ./benchrun -passes 5              # five passes
//	go run ./benchrun -bench ReadScale       # only what matches a regexp
//	go run ./benchrun -benchtime 3s          # longer samples, for a noisy one
//
// The raw samples are written to bench/<timestamp>.txt and summarised with
// benchstat if it is installed. Keep the file: two of them can be compared.
//
//	benchstat bench/old.txt bench/new.txt
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchrun:", err)
		os.Exit(1)
	}
}

func run() error {
	passes := flag.Int("passes", 10, "how many times to run the whole suite")
	pattern := flag.String("bench", ".", "regexp of benchmarks to run, as go test -bench takes")
	benchtime := flag.String("benchtime", "1s", "how long to run each benchmark for, as go test -benchtime takes")
	out := flag.String("out", "", "file for the raw samples (default bench/<timestamp>.txt)")
	flag.Parse()

	if *passes < 1 {
		return fmt.Errorf("-passes must be at least 1, got %d", *passes)
	}

	// Everything happens at the module root, so that this works from wherever
	// it is invoked and writes its samples to one place.
	root, err := moduleRoot()
	if err != nil {
		return err
	}

	path := *out
	if path == "" {
		path = filepath.Join(root, "bench", time.Now().Format("20060102-150405")+".txt")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	samples, err := os.Create(path)
	if err != nil {
		return err
	}
	defer samples.Close()

	fmt.Printf("==> %d passes of %q at %s each, interleaved\n", *passes, *pattern, *benchtime)
	fmt.Printf("==> raw samples: %s\n\n", path)

	// A benchmark is only as quiet as the machine under it. Say what the machine
	// was, so that a number which looks wrong later can be blamed on the right
	// thing.
	fmt.Fprintf(os.Stderr, "%s/%s, %d cores, GOMAXPROCS=%d\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	if load, ok := loadAverage(); ok {
		fmt.Fprintf(os.Stderr, "load average at start: %s\n", load)
	}
	fmt.Fprintln(os.Stderr)

	for pass := 1; pass <= *passes; pass++ {
		fmt.Fprintf(os.Stderr, "\rpass %d/%d", pass, *passes)

		cmd := exec.Command("go", "test",
			"-run", "xxx", // no tests, only benchmarks
			"-bench", *pattern,
			"-benchtime", *benchtime,
			"-count", "1",
		)
		cmd.Dir = root
		cmd.Stdout = samples
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr)
			return fmt.Errorf("pass %d: %w", pass, err)
		}
	}
	fmt.Fprintf(os.Stderr, "\rdone: %d passes            \n\n", *passes)

	if load, ok := loadAverage(); ok {
		fmt.Fprintf(os.Stderr, "load average at end: %s\n\n", load)
	}

	if err := samples.Close(); err != nil {
		return err
	}
	return summarise(root, path)
}

// summarise hands the samples to benchstat, which is what turns a pile of runs
// into a median and a variance. Without it the samples are still there and
// still readable, so this is a missing convenience rather than a failure.
func summarise(root, path string) error {
	if _, err := exec.LookPath("benchstat"); err != nil {
		fmt.Fprintln(os.Stderr, "benchstat is not installed, so the raw samples are all there is:")
		fmt.Fprintln(os.Stderr, "  go install golang.org/x/perf/cmd/benchstat@latest")
		fmt.Fprintf(os.Stderr, "  benchstat %s\n", path)
		return nil
	}

	// Relative to the root it runs in, so that benchstat's column header is
	// "bench/x.txt" rather than the whole path from the filesystem root.
	arg := path
	if rel, err := filepath.Rel(root, path); err == nil {
		arg = rel
	}

	cmd := exec.Command("benchstat", arg)
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// moduleRoot walks up from the working directory for the go.mod, so that the
// samples of a run started in a subdirectory still land beside those of one
// started at the top.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}

// loadAverage reports what else the machine is doing, best effort. There is no
// way to ask for this in the standard library, and the alternative is a
// dependency, which this repository does not have and does not want for a line
// of context. Linux keeps it in a file; everywhere else this reports nothing
// rather than pretending.
func loadAverage() (string, bool) {
	// Linux keeps it in a file, and the first three fields are the averages.
	if raw, err := os.ReadFile("/proc/loadavg"); err == nil {
		var one, five, fifteen float64
		if _, err := fmt.Sscanf(string(raw), "%f %f %f", &one, &five, &fifteen); err == nil {
			return fmt.Sprintf("%.2f %.2f %.2f", one, five, fifteen), true
		}
	}

	// uptime is not in POSIX but is on every Unix that matters, macOS included.
	// It ends with "load average: a, b, c" or "load averages: a b c".
	out, err := exec.Command("uptime").Output()
	if err != nil {
		return "", false
	}
	_, rest, found := strings.Cut(string(out), "load average")
	if !found {
		return "", false
	}
	return strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(rest), "s:")), true
}
