#!/bin/sh
# Writes README.registry.md: README.md as the package registries show it.
#
# README.md's banner is two images that GitHub shows by its own theme
# (#gh-dark-mode-only, #gh-light-mode-only). The registries ignore those
# markers and would show both, so their copy carries the same banner as one
# <picture> that follows the reader's colour scheme. Everything else is
# README.md unchanged.
#
#   scripts/registry-readme.sh          write README.registry.md
#   scripts/registry-readme.sh --check  fail if it is out of date (CI)
set -eu
cd "$(dirname "$0")/.."

render() {
  echo '<!-- Generated from README.md by scripts/registry-readme.sh: edit README.md, then run it. -->'
  awk '
    /^<!-- banner -->$/ { inside = 1; next }
    inside && /^<!-- \/banner -->$/ {
      if (dark == "" || light == "") { print "registry-readme: banner needs a dark and a light image" > "/dev/stderr"; exit 1 }
      print "<p>"
      print "  <picture>"
      print "    <source media=\"(prefers-color-scheme: dark)\" srcset=\"" dark "\">"
      print "    <source media=\"(prefers-color-scheme: light)\" srcset=\"" light "\">"
      print "    <img alt=\"" alt "\" src=\"" dark "\" width=\"100%\">"
      print "  </picture>"
      print "</p>"
      inside = 0; done = 1; next
    }
    inside {
      if (match($0, /src="[^"]*#gh-dark-mode-only"/)) { dark = substr($0, RSTART + 5, RLENGTH - 6); sub(/#gh-dark-mode-only$/, "", dark) }
      if (match($0, /src="[^"]*#gh-light-mode-only"/)) { light = substr($0, RSTART + 5, RLENGTH - 6); sub(/#gh-light-mode-only$/, "", light) }
      if (alt == "" && match($0, /alt="[^"]*"/)) alt = substr($0, RSTART + 5, RLENGTH - 6)
      next
    }
    { print }
    END { if (!done) { print "registry-readme: no <!-- banner --> block in README.md" > "/dev/stderr"; exit 1 } }
  ' README.md
}

if [ "${1:-}" = "--check" ]; then
  if ! render | cmp -s - README.registry.md; then
    echo "README.registry.md is out of date: run scripts/registry-readme.sh" >&2
    exit 1
  fi
else
  render > README.registry.md
fi
