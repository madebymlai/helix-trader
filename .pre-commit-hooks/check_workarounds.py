#!/usr/bin/env python3
"""Checks that each workaround cites the issue that retires it.

A Rust comment block or Markdown paragraph that marks code as a workaround
(`Temporary`, "is temporary", "workaround", "hack", "until/once ... is
fixed", "stands in for Nautilus", "goes once Nautilus ...", "this module
goes") cites an issue: `nautilus_trader#N` for a gap in Nautilus,
`helix-trader#N` (or "helix-trader issue #N", or either issue's URL) for one
not tracked upstream yet. Helix's own code (`src/`) building
nautilus_network's HTTP or WebSocket client reaches a venue past its
adapter, a workaround too: a citation within the five lines above the call.

With `--open`, GitHub is asked (through `gh api`) for each cited issue's
state: a workaround whose cited issues are all closed is due to go.

Usage: check_workarounds.py [--open] [FILE ...] (every tracked .rs and .md
file when none is given).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# RELEASES.md records changes, workarounds among them, rather than marking code.
EXCLUDED = ("crates/lse/docs-snapshot/", ".nautilus-engineering/", "RELEASES.md")

# "Temporary" opening a sentence; the other markers in any case.
MARKER_CASED = re.compile(r"\bTemporary\b")
MARKER = re.compile(
    r"\b(?:is|are|stays|remains) temporary\b"
    r"|\bwork[- ]?arounds?\b"
    r"|\bhacks?\b"
    r"|\b(?:until|once)\b[^.;:]{0,80}?\b(?:is|are) (?:fixed|merged|released)\b"
    r"|\bstands? in for Nautilus\b"
    r"|\bgoes once Nautilus\b"
    r"|\bthis (?:module|file|function|type|call) goes\b",
    re.IGNORECASE,
)
CITATION = re.compile(
    r"\b(?P<repo>nautilus_trader|helix-trader)(?: issue)? ?#(?P<number>\d+)\b"
    r"|github\.com/[\w.-]+/(?P<url_repo>nautilus_trader|helix-trader)/(?:issues|pull)/(?P<url_number>\d+)"
)
DIRECT_CLIENT = re.compile(
    r"\b(?:HttpClient::(?:builder|new)|WebSocketClient::connect|SocketClient::connect)\s*\("
)
DIRECT_CLIENT_WINDOW = 5


@dataclass(frozen=True)
class Workaround:
    """A workaround at `path:line`, citing `issues` (repository, number)."""

    path: str
    line: int
    what: str
    issues: tuple[tuple[str, int], ...]


def citations(text: str) -> tuple[tuple[str, int], ...]:
    """The issues `text` cites, in order, each once."""
    found: list[tuple[str, int]] = []
    for match in CITATION.finditer(text):
        repo = match["repo"] or match["url_repo"]
        number = int(match["number"] or match["url_number"])
        if (repo, number) not in found:
            found.append((repo, number))
    return tuple(found)


def is_marked(text: str) -> bool:
    return bool(MARKER_CASED.search(text) or MARKER.search(text))


def blocks(path: str, lines: list[str]) -> list[list[tuple[int, str]]]:
    """The comment blocks of a Rust file or the paragraphs of a Markdown
    file, each a list of its lines' numbers and texts."""
    is_rust = path.endswith(".rs")
    found: list[list[tuple[int, str]]] = []
    block: list[tuple[int, str]] = []
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("//") if is_rust else stripped:
            block.append((number, stripped.lstrip("/!").strip() if is_rust else stripped))
        elif block:
            found.append(block)
            block = []
    if block:
        found.append(block)
    return found


def joined(block: list[tuple[int, str]]) -> str:
    return " ".join(text for _, text in block)


def marked_line(block: list[tuple[int, str]]) -> int | None:
    """The line of `block` where its first workaround marker starts (a
    marker may run over three lines), or None if it holds none."""
    if not is_marked(joined(block)):
        return None
    for width in (1, 2, 3):
        for index, (number, _) in enumerate(block):
            if is_marked(joined(block[index : index + width])):
                return number
    return block[0][0]


def workarounds(path: str, text: str) -> list[Workaround]:
    """The workarounds of the file at `path`, whose content is `text`."""
    lines = text.splitlines()
    found = []
    for block in blocks(path, lines):
        line = marked_line(block)
        if line is not None:
            found.append(Workaround(path, line, "workaround", citations(joined(block))))
    if path.endswith(".rs") and path.startswith("src/"):
        for number, line in enumerate(lines, start=1):
            if DIRECT_CLIENT.search(line) and not line.strip().startswith("//"):
                window = lines[max(0, number - 1 - DIRECT_CLIENT_WINDOW) : number]
                found.append(
                    Workaround(
                        path,
                        number,
                        "venue client past its adapter",
                        citations("\n".join(window)),
                    )
                )
    return found


def tracked_files() -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "*.rs", "*.md"], capture_output=True, text=True, check=True
    )
    return listed.stdout.split()


def own_repository() -> str:
    """This repository's owner/name on GitHub, which helix-trader# cites."""
    viewed = subprocess.run(
        ["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
        capture_output=True,
        text=True,
        check=True,
    )
    return viewed.stdout.strip()


def issue_states(issues: set[tuple[str, int]]) -> dict[tuple[str, int], str]:
    """Each issue's state on GitHub, `open` or `closed`."""
    repositories = {"nautilus_trader": "nautechsystems/nautilus_trader", "helix-trader": own_repository()}
    states = {}
    for repo, number in sorted(issues):
        answered = subprocess.run(
            ["gh", "api", f"repos/{repositories[repo]}/issues/{number}"],
            capture_output=True,
            text=True,
            check=True,
        )
        states[(repo, number)] = json.loads(answered.stdout)["state"]
    return states


def main(argv: list[str]) -> int:
    check_open = "--open" in argv
    paths = [arg for arg in argv if arg != "--open"] or tracked_files()
    found: list[Workaround] = []
    for path in paths:
        if not path.endswith((".rs", ".md")) or path.startswith(EXCLUDED):
            continue
        found.extend(workarounds(path, Path(path).read_text(encoding="utf-8")))

    failures = [
        f"{w.path}:{w.line}: {w.what} cites no issue: cite the nautilus_trader#N "
        "or helix-trader#N issue that retires it"
        for w in found
        if not w.issues
    ]
    if check_open:
        cited = [w for w in found if w.issues]
        states = issue_states({issue for w in cited for issue in w.issues})
        for w in cited:
            if all(states[issue] == "closed" for issue in w.issues):
                named = ", ".join(f"{repo}#{number}" for repo, number in w.issues)
                failures.append(
                    f"{w.path}:{w.line}: {w.what} whose issues are all closed ({named}): "
                    "drop it, or cite the issue that still holds it"
                )
    for failure in failures:
        print(failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
