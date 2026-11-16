#!/usr/bin/env python3
"""Tests of check_workarounds.py's detection, without GitHub."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from check_workarounds import workarounds  # noqa: E402


def found(path: str, text: str) -> list[tuple[int, tuple[tuple[str, int], ...]]]:
    return [(w.line, w.issues) for w in workarounds(path, text)]


class TestWorkarounds(unittest.TestCase):
    def test_a_temporary_comment_citing_nautilus_is_cited(self) -> None:
        text = "fn a() {}\n// Temporary until nautilus_trader#5472 is fixed: IB then\n// fills through the node.\n"
        self.assertEqual(found("src/a.rs", text), [(2, (("nautilus_trader", 5472),))])

    def test_a_temporary_comment_citing_nothing_is_uncited(self) -> None:
        self.assertEqual(found("src/a.rs", "/// Temporary: the fill goes later.\nfn a() {}\n"), [(1, ())])

    def test_a_citation_anywhere_in_the_comment_block_counts(self) -> None:
        text = "//! It stands in for Nautilus: the query is built here.\n//!\n//! helix-trader issue #15\n//! (<https://github.com/owner/helix-trader/issues/15>).\n"
        self.assertEqual(found("src/a.rs", text), [(1, (("helix-trader", 15),))])

    def test_a_citation_in_another_comment_block_does_not_count(self) -> None:
        text = "// A hack around the client.\nlet x = 1;\n// nautilus_trader#1\n"
        self.assertEqual(found("src/a.rs", text), [(1, ())])

    def test_a_marker_over_two_lines_is_found_at_its_start(self) -> None:
        text = "/// The query is built here.\n/// It goes once the builder is\n/// fixed upstream.\n"
        self.assertEqual(found("src/a.rs", text), [(2, ())])

    def test_a_temporary_file_is_no_workaround(self) -> None:
        text = "//! the file is replaced atomically (a temporary file renamed over it)\n"
        self.assertEqual(found("src/a.rs", text), [])

    def test_a_markdown_paragraph_is_a_block(self) -> None:
        text = "The fill is temporary: IB's client answers no envelope.\n\nnautilus_trader#5472 tracks it.\n"
        self.assertEqual(found("src/README.md", text), [(1, ())])

    def test_a_venue_client_in_src_needs_a_citation_above(self) -> None:
        cited = "// The adapter reads no leverage: helix-trader#21.\nlet http = HttpClient::builder()\n"
        bare = "let http = HttpClient::builder()\n"
        self.assertEqual(found("src/venues/okx/account.rs", cited), [(2, (("helix-trader", 21),))])
        self.assertEqual(found("src/venues/okx/account.rs", bare), [(1, ())])

    def test_an_adapter_crate_builds_its_venue_client_freely(self) -> None:
        self.assertEqual(found("crates/lse/src/http/client.rs", "let client = HttpClient::builder()\n"), [])


if __name__ == "__main__":
    unittest.main()
