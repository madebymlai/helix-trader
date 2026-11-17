#!/usr/bin/env python3
"""Checks that each entry of the release in progress is short.

An entry of `RELEASES.md`'s top section, the release in progress up to its
`---`, is one line of 160 characters at most: what changed, its migration if
it breaks something, the how and why left to its pull request
(`docs/releases.md`). Released sections stay as they were published.

Usage: check_release_entries.py [FILE ...] (`RELEASES.md` when none is
given).
"""

from __future__ import annotations

import sys
from pathlib import Path

LIMIT = 160


def long_entries(text: str) -> list[tuple[int, int]]:
    """The line number and length of each entry of the top section over `LIMIT`."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if line.strip() == "---":
            break

        if line.startswith("- ") and len(line) > LIMIT:
            found.append((number, len(line)))

    return found


def main(paths: list[str]) -> int:
    failed = False
    for path in paths or ["RELEASES.md"]:
        for number, length in long_entries(Path(path).read_text(encoding="utf-8")):
            print(f"{path}:{number}: an entry of {length} characters, over {LIMIT}: say what changed, the how and why in the pull request")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
