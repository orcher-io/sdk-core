#!/usr/bin/env python3
"""Fails if generated protocol types are named outside the places allowed to.

The generated types (orcher-proto) are the wire format, not this crate's
vocabulary. `src/wire/` converts between them and the crate's own types; the
files listed in ALLOWED may name them too, each for the reason given, until
the step that moves them behind the wire. A new file that names a generated
type fails here, and so does a listed file that no longer does: the list only
shrinks.

Tests (`*_tests.rs`, `tests/`) build wire messages to talk to fake engines and
are not checked.
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# A path into the generated code, or the re-export of its runtime crates.
GENERATED = re.compile(
    r"\borcher_proto\b|\bcrate::proto\b|\bproto::(orcher|kernel|host|prost|prost_types|tonic)\b"
    r"|\bprost_types\b"
)

ALLOWED = {
    # The crate-private alias of the generated protocols.
    "src/lib.rs": "the crate-private alias of the kernel and host protocols",
    # The kernel and host build and read the kernel and host protocols'
    # messages here and hand every language SDK their encoded bytes, which
    # each SDK decodes with generated code of its own. None of them is in the
    # public API (scripts/check-public-surface.py keeps it so): #31 Phase B
    # chose encoded messages over a hand-written mirror.
    "src/kernel/activation.rs": "kernel protocol messages",
    "src/kernel/completion.rs": "kernel protocol messages",
    "src/kernel/step_ids.rs": "kernel protocol messages",
    "src/worker/host/mod.rs": "host protocol messages",
    "src/worker/host/tasks.rs": "host protocol messages",
    "src/worker/host/actors.rs": "host protocol messages, actor service calls",
    "src/worker/host/workflows.rs": "kernel protocol messages",
    "src/client/protocol.rs": "client protocol messages",
    # Calls to the engine, built where they are made.
    "src/client/actor.rs": "actor service calls",
    "src/client/namespace.rs": "namespace service calls",
    "src/client/workflow.rs": "workflow service calls",
    "src/worker/poll/polling.rs": "execution service polls",
    "src/worker/poll/completion.rs": "execution service completions",
    "src/worker/drivers/workflow.rs": "execution service calls",
    "src/worker/drivers/task.rs": "execution service calls",
    "src/worker/drivers/actor.rs": "actor service calls",
    "src/worker/drivers/registration.rs": "worker service calls",
    "src/worker/heartbeat.rs": "execution service heartbeats",
    "src/worker/lifecycle.rs": "execution service shutdown notices",
    "src/worker/record.rs": "recorded engine messages",
    # The engine's journal, checked before an activation runs.
    "src/state/journal.rs": "journal entries",
    "src/state/replayer.rs": "journal entries",
}


def names_generated(path: pathlib.Path) -> bool:
    for line in path.read_text().splitlines():
        if line.lstrip().startswith("//"):
            continue
        if GENERATED.search(line):
            return True
    return False


def main() -> int:
    naming = set()
    for path in sorted((ROOT / "src").rglob("*.rs")):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("src/wire/") or rel.endswith("_tests.rs"):
            continue
        if names_generated(path):
            naming.add(rel)

    problems = []
    for rel in sorted(naming - ALLOWED.keys()):
        problems.append(
            f"{rel} names a generated protocol type: convert in src/wire/ instead"
        )
    for rel in sorted(ALLOWED.keys() - naming):
        problems.append(
            f"{rel} no longer names a generated type: remove it from ALLOWED"
        )
    if problems:
        print("\n".join(problems))
        return 1
    print(f"generated types stay behind the wire ({len(naming)} files still allowed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
