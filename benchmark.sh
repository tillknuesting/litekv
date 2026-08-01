#!/usr/bin/env bash
#
# Runs the benchmarks the way this repository trusts numbers: the whole suite,
# start to finish, several times over, rather than each benchmark repeated back
# to back.
#
# The difference matters. `go test -count=10` runs one benchmark ten times in a
# row before moving to the next, so a machine that warms up over the session
# hands its early benchmarks a cold clock and its later ones a hot one, and the
# drift lands as a bias in each result. Alternating spreads every benchmark's
# samples across the whole session, so the drift lands as noise in all of them
# instead, where benchstat can see it and say so.
#
# Usage:
#   ./benchmark.sh                    # 10 passes of everything
#   ./benchmark.sh 5                  # 5 passes
#   ./benchmark.sh 5 KeyValueStore    # 5 passes of benchmarks matching a regexp
#
# Writes the raw samples to bench/<timestamp>.txt and prints the summary. Keep
# the raw file: benchstat compares two of them.
#
#   benchstat bench/old.txt bench/new.txt

set -euo pipefail

passes=${1:-10}
pattern=${2:-.}
benchtime=${BENCHTIME:-1s}

mkdir -p bench
out="bench/$(date +%Y%m%d-%H%M%S).txt"

echo "==> $passes passes of '$pattern' at $benchtime each, interleaved"
echo "==> raw samples: $out"
echo

# A benchmark is only as quiet as the machine under it. Say what the machine was
# doing so a number that looks wrong later can be blamed on the right thing.
echo "load average at start: $(uptime | sed 's/.*load averages*: //')" >&2
if ! pmset -g therm 2>/dev/null | grep -q "No thermal warning level"; then
	echo "WARNING: this machine has recorded thermal pressure; numbers will be low" >&2
fi
echo >&2

for pass in $(seq 1 "$passes"); do
	printf '\rpass %d/%d' "$pass" "$passes" >&2
	go test -run xxx -bench "$pattern" -benchtime "$benchtime" -count 1 >>"$out"
done
printf '\rdone: %d passes            \n\n' "$passes" >&2

echo "load average at end: $(uptime | sed 's/.*load averages*: //')" >&2
echo >&2

if command -v benchstat >/dev/null; then
	benchstat "$out"
else
	echo "benchstat is not installed, so here are the raw samples." >&2
	echo "  go install golang.org/x/perf/cmd/benchstat@latest" >&2
	cat "$out"
fi
