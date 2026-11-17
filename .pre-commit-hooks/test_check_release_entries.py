#!/usr/bin/env python3
"""Tests of check_release_entries.py."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from check_release_entries import long_entries  # noqa: E402


def entry(length: int) -> str:
    return "- " + "a" * (length - 2)


class TestReleaseEntries(unittest.TestCase):
    def test_an_entry_of_160_characters_passes(self) -> None:
        self.assertEqual(long_entries(f"# Helix Trader 0.2.0\n\n### Fixes\n\n{entry(160)}\n"), [])

    def test_an_entry_of_161_characters_is_reported_with_its_line_and_length(self) -> None:
        self.assertEqual(long_entries(f"# Helix Trader 0.2.0\n\n### Fixes\n\n{entry(80)}\n{entry(161)}\n"), [(6, 161)])

    def test_an_entry_of_a_released_section_passes_however_long(self) -> None:
        self.assertEqual(long_entries(f"# Helix Trader 0.2.0\n\n{entry(80)}\n\n---\n\n# Helix Trader 0.1.0\n\n{entry(400)}\n"), [])

    def test_a_line_other_than_an_entry_passes_however_long(self) -> None:
        self.assertEqual(long_entries(f"# Helix Trader 0.2.0\n\n{'a' * 400}\n"), [])


if __name__ == "__main__":
    unittest.main()
