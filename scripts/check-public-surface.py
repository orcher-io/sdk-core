#!/usr/bin/env python3
"""Fails when the public API names a generated protocol type, or the protobuf
and gRPC runtimes under it.

A language SDK exchanges the kernel, host and client protocols with this crate
as encoded messages and decodes them with generated code of its own, so a new
orcher-proto here must change nothing an SDK compiles against. A public
signature that names one of these types breaks that, and this is where it
shows: rustdoc links every type a public item names, so a link into one of
these crates is a leak.

Blanket implementations are left out: a dependency that implements a trait
for every type (tonic does, for `IntoRequest`) shows on every page, and no
item here chooses it.

Reads the docs `cargo doc --no-deps` wrote; set CARGO_TARGET_DIR to where they
are if it is not ./target.
"""

import os
import pathlib
import re
import sys

CRATES = ("orcher_proto", "prost", "prost_types", "tonic")
LINK = re.compile(
    r'title="[a-z ]+ (?:%s)::[A-Za-z0-9_:]+"|href="[^"]*/(?:%s)/[^"]*"'
    % ("|".join(CRATES), "|".join(CRATES))
)
BLANKET = re.compile(
    r'<h2 id="blanket-implementations".*?(?=<h2 id=|</section>\s*</div>\s*</main>|$)',
    re.S,
)


def main() -> int:
    docs = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "doc" / "orcher_sdk_core"
    if not docs.is_dir():
        print(f"no docs at {docs}: run cargo doc --no-deps first", file=sys.stderr)
        return 2
    leaks = {}
    for page in docs.rglob("*.html"):
        html = BLANKET.sub("", page.read_text(encoding="utf-8"))
        for link in LINK.findall(html):
            leaks.setdefault(link, set()).add(str(page.relative_to(docs)))
    if leaks:
        print("the public API names generated or runtime types:", file=sys.stderr)
        for link, pages in sorted(leaks.items()):
            print(f"  {link}  ({', '.join(sorted(pages)[:3])})", file=sys.stderr)
        return 1
    print("the public API names no generated type")
    return 0


if __name__ == "__main__":
    sys.exit(main())
