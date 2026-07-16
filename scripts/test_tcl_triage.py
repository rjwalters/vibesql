#!/usr/bin/env python3
"""Unit tests for the root-cause clustering logic in ``tcl_triage.py``.

These exercise the family-grouping / normalization logic in isolation — no
results DB and no vibesql binary required — so the clustering is verified
independent of any real (or certified) failure data.

Run with:
    python3 scripts/test_tcl_triage.py
or under pytest:
    pytest scripts/test_tcl_triage.py
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tcl_triage import (  # noqa: E402
    NO_MESSAGE_FAMILY,
    CERTIFIED_TAG_PATTERN,
    cluster_errors,
    normalize_error_message,
    results_db_reachable,
    _extract_json_array,
)


class TestNormalize(unittest.TestCase):
    def test_none_and_empty_collapse_to_no_message(self):
        self.assertEqual(normalize_error_message(None), NO_MESSAGE_FAMILY)
        self.assertEqual(normalize_error_message(""), NO_MESSAGE_FAMILY)
        self.assertEqual(normalize_error_message("   "), NO_MESSAGE_FAMILY)

    def test_single_quoted_literals_collapse(self):
        a = normalize_error_message("Table 't1' already exists")
        b = normalize_error_message("Table 'customers' already exists")
        self.assertEqual(a, b)
        self.assertEqual(a, "Table '?' already exists")

    def test_numbers_collapse(self):
        a = normalize_error_message("Error executing statement 1: boom")
        b = normalize_error_message("Error executing statement 42: boom")
        self.assertEqual(a, b)
        self.assertIn("statement N", a)

    def test_hex_addresses_collapse(self):
        a = normalize_error_message("bad pointer 0xdeadbeef here")
        b = normalize_error_message("bad pointer 0x00ff12ab here")
        self.assertEqual(a, b)
        self.assertIn("0xN", a)

    def test_double_quoted_and_backtick_literals_collapse(self):
        a = normalize_error_message('no such column: "foo"')
        b = normalize_error_message('no such column: "bar"')
        self.assertEqual(a, b)
        c = normalize_error_message("near `x`: syntax error")
        d = normalize_error_message("near `yy`: syntax error")
        self.assertEqual(c, d)

    def test_realistic_full_message_collapses(self):
        a = normalize_error_message(
            "Query failed: Error executing statement 1: Table 't1' already "
            "exists Error: 1 statements failed"
        )
        b = normalize_error_message(
            "Query failed: Error executing statement 1: Table 'zebra' already "
            "exists Error: 1 statements failed"
        )
        self.assertEqual(a, b)

    def test_whitespace_normalized(self):
        a = normalize_error_message("Output   mismatch\n")
        b = normalize_error_message("Output mismatch")
        self.assertEqual(a, b)

    def test_distinct_root_causes_stay_separate(self):
        a = normalize_error_message("Output mismatch")
        b = normalize_error_message("no such function: SOUNDEX")
        self.assertNotEqual(a, b)

    def test_identifier_suffix_numbers_are_preserved(self):
        # A number that is part of an identifier (t1) is inside a quote here and
        # collapses; but a bare identifier token like "utf16" must not be
        # mangled into "utfN".
        msg = normalize_error_message("encoding utf16 unsupported")
        self.assertIn("utf16", msg)


class TestCluster(unittest.TestCase):
    def test_families_aggregate_counts(self):
        rows = [
            {"error_message": "Output mismatch", "n": 30},
            {"error_message": "Table 't1' already exists", "n": 5},
            {"error_message": "Table 'foo' already exists", "n": 3},
            {"error_message": "Output mismatch\n", "n": 2},
            {"error_message": None, "n": 4},
        ]
        families = cluster_errors(rows)
        by_key = {f.key: f for f in families}

        # "Output mismatch" (30) + whitespace variant (2) = 32.
        self.assertEqual(by_key["Output mismatch"].count, 32)
        self.assertEqual(by_key["Output mismatch"].distinct_messages, 2)

        # Two table-name variants collapse to one family with 8.
        self.assertEqual(by_key["Table '?' already exists"].count, 8)
        self.assertEqual(by_key["Table '?' already exists"].distinct_messages, 2)

        # NULL becomes its own family.
        self.assertEqual(by_key[NO_MESSAGE_FAMILY].count, 4)

    def test_families_sorted_by_leverage(self):
        rows = [
            {"error_message": "rare", "n": 1},
            {"error_message": "common", "n": 100},
            {"error_message": "medium", "n": 10},
        ]
        families = cluster_errors(rows)
        self.assertEqual([f.count for f in families], [100, 10, 1])

    def test_sample_messages_captured_and_capped(self):
        rows = [
            {"error_message": f"Table 't{i}' already exists", "n": 1}
            for i in range(10)
        ]
        families = cluster_errors(rows)
        self.assertEqual(len(families), 1)
        # Samples are capped at 3 raw examples.
        self.assertLessEqual(len(families[0].sample_messages), 3)
        self.assertEqual(families[0].count, 10)


class TestCertificationPattern(unittest.TestCase):
    def test_certified_tag_matches(self):
        self.assertTrue(CERTIFIED_TAG_PATTERN.search("aws-c7i.8xlarge-32c-clean2"))
        self.assertTrue(CERTIFIED_TAG_PATTERN.search("aws-c7i.8xlarge-32c-clean3"))

    def test_non_certified_tags_do_not_match(self):
        self.assertFalse(
            bool(CERTIFIED_TAG_PATTERN.search("aws-c7i.8xlarge-32c-final"))
        )
        self.assertFalse(bool(CERTIFIED_TAG_PATTERN.search("local-macbook")))
        self.assertIsNone(CERTIFIED_TAG_PATTERN.search(""))


class TestJsonExtraction(unittest.TestCase):
    def test_strips_trailing_row_count(self):
        raw = '[\n  {\n    "m": "22"\n  }\n]\n1 rows'
        self.assertEqual(_extract_json_array(raw), '[\n  {\n    "m": "22"\n  }\n]')

    def test_returns_none_when_no_array(self):
        self.assertIsNone(_extract_json_array("Error: something broke"))

    def test_handles_bracket_inside_string_value(self):
        raw = '[{"error_message": "list index [0] out of range"}]\n1 rows'
        extracted = _extract_json_array(raw)
        self.assertIsNotNone(extracted)
        self.assertTrue(extracted.endswith("]"))


class TestResultsDbReachable(unittest.TestCase):
    """A WAL-mode results DB (checkpoints + wal, no .vbsql snapshot file) must
    be recognized as reachable; a genuinely-absent DB must not."""

    def setUp(self):
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.db = os.path.join(self.dir, "tcl_test_results.vbsql")

    def tearDown(self):
        self._tmp.cleanup()

    def test_absent_db_is_unreachable(self):
        # No file, no checkpoints, no wal.
        self.assertFalse(results_db_reachable(self.db))

    def test_snapshot_file_is_reachable(self):
        with open(self.db, "w", encoding="utf-8") as fh:
            fh.write("-- VibeSQL Database dump\n")
        self.assertTrue(results_db_reachable(self.db))

    def test_wal_mode_checkpoints_is_reachable(self):
        # Extension-stripped sibling dir with a .vchk, no .vbsql file.
        ckpt = os.path.join(self.dir, "tcl_test_results-checkpoints")
        os.mkdir(ckpt)
        with open(os.path.join(ckpt, "checkpoint_53.vchk"), "wb") as fh:
            fh.write(b"VCHK\x02\x00\x00\x00")
        self.assertFalse(os.path.exists(self.db))  # precondition: no snapshot
        self.assertTrue(results_db_reachable(self.db))

    def test_empty_checkpoints_dir_without_vchk_is_unreachable(self):
        os.mkdir(os.path.join(self.dir, "tcl_test_results-checkpoints"))
        self.assertFalse(results_db_reachable(self.db))

    def test_wal_sibling_is_reachable(self):
        with open(os.path.join(self.dir, "tcl_test_results.wal"), "wb") as fh:
            fh.write(b"\x00" * 32)
        self.assertTrue(results_db_reachable(self.db))

    def test_non_vbsql_path_uses_path_as_stem(self):
        # A bare basename (no .vbsql extension) recovers from <path>-checkpoints/.
        bare = os.path.join(self.dir, "tcl_test_results")
        ckpt = os.path.join(self.dir, "tcl_test_results-checkpoints")
        os.mkdir(ckpt)
        with open(os.path.join(ckpt, "checkpoint_1.vchk"), "wb") as fh:
            fh.write(b"VCHK")
        self.assertTrue(results_db_reachable(bare))


if __name__ == "__main__":
    unittest.main(verbosity=2)
