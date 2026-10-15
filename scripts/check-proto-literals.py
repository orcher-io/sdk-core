#!/usr/bin/env python3
"""Fails if a proto field added to orcher-proto would break this workspace's build.

A field added to a proto message is a compatible change on the wire, but it
breaks Rust code that names every field of the generated struct (E0063,
missing field) or destructures it without `..`. Such code turns every
additive protocol change into a breaking release of this crate.

The check asks the compiler, so there is nothing to keep in sync: it copies
the orcher-proto source this workspace resolves, adds one field to every
message, and builds the workspace (all targets, tests included) against that
copy. A message built without `..Default::default()` fails the build, and
the compiler names the place.

Usage: python3 scripts/check-proto-literals.py
Needs only cargo and python3; orcher-proto compiles its protos with protox,
so no protoc is needed.
"""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORK = ROOT / "target" / "proto-literal-check"
# The highest field number protobuf allows, so it never collides with a real
# field (19000-19999 are reserved by protobuf itself, this is far above).
PROBE = "  bool literal_check_probe = 536870911;\n"


def with_probe_fields(source: str) -> str:
    """Adds PROBE to every message, nested ones included.

    Walks the text once, skipping comments and strings, and keeps a stack of
    the open blocks; the closing brace of a message gets the probe field
    inserted before it.
    """
    out = []
    stack = []
    pending = None  # the keyword of the block about to open
    i, n = 0, len(source)
    word = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
    while i < n:
        c = source[i]
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = n if end == -1 else end
            out.append(source[i:end])
            i = end
            continue
        if source.startswith("/*", i):
            end = source.find("*/", i) + 2
            out.append(source[i:end])
            i = end
            continue
        if c in "\"'":
            end = i + 1
            while source[end] != c:
                end += 2 if source[end] == "\\" else 1
            out.append(source[i : end + 1])
            i = end + 1
            continue
        m = word.match(source, i)
        if m and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_")):
            token = m.group(0)
            if token in ("message", "enum", "oneof", "service", "rpc", "extend"):
                pending = token
            out.append(token)
            i = m.end()
            continue
        if c == "{":
            stack.append(pending)
            pending = None
        elif c == "}":
            if stack.pop() == "message":
                out.append(PROBE)
        elif c == ";":
            pending = None
        out.append(c)
        i += 1
    if stack:
        sys.exit("unbalanced braces in a proto file")
    return "".join(out)


def main() -> int:
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1"], cwd=ROOT
        )
    )
    protos = [p for p in metadata["packages"] if p["name"] == "orcher-proto"]
    if len(protos) != 1:
        sys.exit(f"expected one orcher-proto in the build, found {len(protos)}")
    source = Path(protos[0]["manifest_path"]).parent

    copy = WORK / "orcher-proto"
    shutil.rmtree(copy, ignore_errors=True)
    shutil.copytree(source, copy, ignore=shutil.ignore_patterns("target"))
    messages = 0
    for proto in sorted((copy / "proto").glob("*.proto")):
        text = with_probe_fields(proto.read_text())
        messages += text.count(PROBE)
        proto.write_text(text)
    print(f"orcher-proto {protos[0]['version']}: probe field added to {messages} messages")

    # The patch changes the lockfile; put it back afterwards.
    lock = ROOT / "Cargo.lock"
    saved = lock.read_bytes() if lock.exists() else None
    try:
        result = subprocess.run(
            [
                "cargo",
                "check",
                "--workspace",
                "--all-targets",
                "--all-features",
                "--quiet",
                "--config",
                f'patch.crates-io.orcher-proto.path="{copy}"',
                "--target-dir",
                str(WORK / "target"),
            ],
            cwd=ROOT,
        )
    finally:
        if saved is not None:
            lock.write_bytes(saved)
    if result.returncode != 0:
        print(
            "\nA proto message above is built or destructured naming every field.\n"
            "Add `..Default::default()` (or `..` in a pattern) so a field added to\n"
            "orcher-proto does not break this crate.",
            file=sys.stderr,
        )
        return 1
    print("every orcher-proto message is built with room for new fields")
    return 0


if __name__ == "__main__":
    sys.exit(main())
