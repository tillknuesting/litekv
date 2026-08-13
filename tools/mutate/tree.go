package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
)

// skipped are the directories a worker's copy does without.
//
// .mutate is where the copies live and it is inside the repository, so copying
// it would be copying the other workers' trees into this one — a sweep that
// never finishes rather than one that fails.
var skipped = map[string]bool{
	".git":     true,
	".mutate":  true,
	"bench":    true,
	"testdata": true,
}

// repository is the root of the checkout this is being run from, found by
// walking up from the working directory until a go.mod turns up.
//
// Not the source file's own path, which is what runtime.Caller would give: that
// is where the tool was compiled from and not where it is being run, and the
// two stop being the same the moment anybody installs it.
func repository() (string, error) {
	where, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(where, "go.mod")); err == nil {
			return where, nil
		}
		parent := filepath.Dir(where)
		if parent == where {
			return "", errors.New("no go.mod above the working directory; run this inside the repository")
		}
		where = parent
	}
}

// tree makes sure this worker's copy of the repository is there.
//
// Checked for every time rather than made once and remembered. A sweep lost
// thirty-six of sixty-six mutations to "go.mod file not found", which is what a
// worker whose directory went away under it looks like; the tool has no
// business assuming a temporary directory it made ten minutes ago is still
// there, and a mutation that silently did not run is the failure this whole
// tool exists to avoid.
func tree(root, where string) error {
	if _, err := os.Stat(filepath.Join(where, "go.mod")); err == nil {
		return nil
	}
	if err := os.RemoveAll(where); err != nil {
		return err
	}
	return copyTree(root, where)
}

// copyTree copies a directory, leaving out skipped.
func copyTree(from, to string) error {
	return filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return os.MkdirAll(to, 0o755)
		}
		if skipped[entry.Name()] {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}

		// Regular files and nothing else. A symlink or a socket copied wrongly
		// is a tree that builds differently from the one being tested, which is
		// a sweep reporting on something other than this repository — so it is
		// an error rather than a thing to skip quietly.
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file (%s)", relative, info.Mode())
		}
		return copyFile(path, target, info.Mode().Perm())
	})
}

func copyFile(from, to string, perm os.FileMode) error {
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()

	target, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	if _, err := io.Copy(target, source); err != nil {
		target.Close()
		return err
	}
	return target.Close()
}

// suiteTimeout is how long a mutated tree gets before its tests are killed.
//
// This suite takes about forty-five seconds under -race, so ten minutes is room
// to spare on a machine running eight of them at once. Raising it is cheap;
// lowering it below what the suite honestly needs is the worst thing that can be
// done to this tool, because a test binary killed by the deadline exits non-zero
// exactly like a failing one — every mutation would report caught and the sweep
// would be testing nothing.
const suiteTimeout = "600s"

// locate is the file to break and the package to run it in.
//
// A path relative to the repository root, always: "db.go". It used to take a
// bare name as meaning server/ and a slash as meaning the root, which saved
// some typing and cost a rule nobody could keep in their head — and a sweep
// that breaks a file nobody meant to break is the failure this whole tool
// exists to avoid.
func locate(where, path string) (full, pkg string) {
	clean := filepath.Clean(path)

	inside := filepath.Dir(clean)
	if inside == "." {
		return filepath.Join(where, clean), "./"
	}
	return filepath.Join(where, clean), "./" + inside
}

// goCommand runs the go tool in a worker's tree and hands back everything it
// said, output and errors together, so that a caller has the message and not
// only the fact.
func goCommand(where string, args ...string) (string, error) {
	command := exec.Command("go", args...)
	command.Dir = where

	out, err := command.CombinedOutput()
	return string(out), err
}
