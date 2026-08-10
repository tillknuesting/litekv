#!/usr/bin/env python3
"""Break the server on purpose, one change at a time, and see whether a test notices.

    python3 tools/mutate.py                 # all of them
    python3 tools/mutate.py replica batch   # only those whose name matches

Every mutation must be caught. A mutation that survives is a promise the code
makes that nothing is holding it to; the four that are allowed to survive are
listed in AGENTS.md with the reason for each, and a fifth is news.

# Why this is parallel, and why each worker gets its own copy of the tree

The suite takes three seconds. A sweep of sixty-six mutations took twenty-five
minutes, and the difference is the thing worth knowing: a *caught* mutation is
usually caught by a test waiting out a deadline. TestOneSmallRecordArrivesAtOnce
gives a record fifteen seconds to arrive; waitForPositions gives a follower
thirty. Those numbers are right — a shorter deadline is a test that fails on a
busy machine and says nothing about the store, which AGENTS.md has a trap about
— so the sweep is slow for the same reason the tests are trustworthy.

What that means is that a sweep is nearly all waiting, and waiting parallelises.
Each worker gets its own copy of the repository because a mutation is an edit to
a file, and eight of those in one directory is eight mutations at once in a way
nobody could read the results of.
"""

import concurrent.futures
import os
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mutations import MUTATIONS  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# More than the cores buys nothing: the waiting is on timers, but the test binary
# that runs between the waits is not free, and neither is the compile after each
# edit.
WORKERS = min(8, max(1, (os.cpu_count() or 4) - 2))

out = threading.Lock()
workers = threading.local()


def tree(pool):
    """The copy of the repository this worker owns.

    Checked for every time rather than made once and remembered. A sweep lost
    thirty-six of sixty-six mutations to `go.mod file not found`, which is what
    a worker whose directory went away under it looks like; the tool has no
    business assuming a temporary directory it made ten minutes ago is still
    there, and a mutation that silently did not run is the failure this whole
    file exists to avoid.
    """
    if not hasattr(workers, "dir") or not os.path.isdir(workers.dir):
        workers.dir = os.path.join(pool, f"w{threading.get_ident()}")
        # .mutate is where these copies live, and it is inside ROOT. Copying
        # it would be copying the other workers' trees into this one, which is
        # a sweep that never finishes rather than one that fails.
        shutil.copytree(ROOT, workers.dir, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns(".git", ".mutate", "bench", "testdata"))
    return workers.dir


def run(pool, path, name, old, new):
    where = tree(pool)
    full = os.path.join(where, "server", path)

    source = open(full).read()
    if source.count(old) != 1:
        # Not a pass. An exact-text pattern that matches nothing or twice is a
        # mutation that never ran, and it is the commonest way a sweep quietly
        # stops testing what it says it tests.
        return f"SKIPPED (matched {source.count(old)} times)"

    open(full, "w").write(source.replace(old, new))
    try:
        # go vet is the gate rather than go build because vet catches the
        # unreachable code a clumsy mutation leaves behind — and because a
        # mutation that does not typecheck did not run either.
        build = subprocess.run(["go", "vet", "./server/"], cwd=where,
                               capture_output=True, text=True, errors="replace")
        if build.returncode != 0:
            # The message, not just the fact. A bare "does not build" is a
            # mutation that did not run and no way to find out why, which is
            # half a day the next person does not have.
            why = (build.stderr or build.stdout).strip().splitlines()
            return "SKIPPED (does not build: " + (why[-1][:60] if why else "?") + ")"

        # -count=1 because the mutated tree is otherwise a cache hit away from
        # reporting the unmutated result.
        # -timeout, because go test defaults to ten minutes and a mutation that
        # deadlocks the suite would spend all of it. A mutated tree that hangs
        # is a caught mutation; the real suite takes three seconds, so ninety
        # is room to spare and a two-order-of-magnitude saving on the one
        # mutation that wedges something.
        test = subprocess.run(["go", "test", "-count=1", "-timeout", "90s", "./server/"],
                              cwd=where, capture_output=True, text=True, errors="replace")
        if test.returncode == 0:
            return "SURVIVED"

        # Stripped, because a subtest's FAIL line is indented under its parent
        # and an unstripped prefix check reports "failed without naming a test"
        # for every mutation that only a t.Run caught.
        caught = []
        for line in test.stdout.splitlines():
            fields = line.strip().split()
            if len(fields) >= 3 and fields[0] == "---" and fields[1] == "FAIL:":
                caught.append(fields[2])
        if not caught:
            # Failed without naming a test: a panic, or a build failure of the
            # test binary that vet did not see. Caught, but say so.
            return "caught (the package failed without naming a test)"
        return "caught by " + ", ".join(caught[:4])
    finally:
        open(full, "w").write(source)


def main():
    only = sys.argv[1:]
    chosen = [m for m in MUTATIONS if not only or any(o in m[1] for o in only)]
    if not chosen:
        print(f"nothing matched {only}")
        return 1

    started = time.time()
    results = {}

    # Beside the repository rather than in the system temp directory. Two runs
    # lost half their mutations to `go.mod file not found`, which is a worker
    # whose tree was reaped under it — some environments clean $TMPDIR on a
    # timer, and a ten-minute sweep is long enough to be caught by one.
    pool = os.path.join(ROOT, ".mutate")
    shutil.rmtree(pool, ignore_errors=True)
    os.makedirs(pool, exist_ok=True)

    try:
        with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool_of_workers:
            futures = {pool_of_workers.submit(run, pool, *m): m for m in chosen}

            for done, future in enumerate(
                    concurrent.futures.as_completed(futures), start=1):
                name = futures[future][1]
                results[name] = future.result()

                # Printed as they land rather than at the end, so a sweep that
                # is going badly says so while it is still going.
                with out:
                    print(f"[{done:2d}/{len(chosen)}] {results[name]:<58} {name}",
                          flush=True)
    finally:
        shutil.rmtree(pool, ignore_errors=True)

    missed = {n: v for n, v in results.items()
              if v.startswith("SURVIVED") or v.startswith("SKIPPED")}

    print(f"\n{len(results) - len(missed)}/{len(chosen)} caught "
          f"in {time.time() - started:.0f}s on {WORKERS} workers")

    # Counted against what was asked for, not against what finished. A runner
    # that died half way through once reported nothing at all.
    if len(results) != len(chosen):
        print(f"only {len(results)} of {len(chosen)} mutations reported")
        return 1

    if missed:
        print("\nnot caught:")
        for name, verdict in sorted(missed.items()):
            print(f"  {verdict:<40} {name}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
