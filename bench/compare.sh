#!/usr/bin/env bash
# Prints the differences between two results of the benchmark suite.
#
#   bench/compare.sh bench/results/before.json bench/results/after.json
#
# For every scenario in both, each derived number with its change in
# percent. See bench/README.md for what each number means.

set -euo pipefail

if [ $# -ne 2 ]; then
  sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo run --quiet --release --manifest-path "$ROOT/Cargo.toml" -p orcher-bench -- compare "$1" "$2"
