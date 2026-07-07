#!/usr/bin/env python3
"""Unit tests for scripts/tcl_parser.py file-skip logic.

Focus: TclTestParser.should_skip_file must mirror the native
`vibesql_skip_files` exact-match semantics in scripts/tester_vibesql.tcl,
rather than over-matching sibling files via unanchored substring regexes
(regression guard for issue #5911).

Run with:  python3 -m pytest scripts/test_tcl_parser.py
       or:  python3 scripts/test_tcl_parser.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tcl_parser import TclTestParser  # noqa: E402


# --- Acceptance criteria from issue #5911 -----------------------------------

# (filename, should_be_skipped) — exact per-file entries that MUST be skipped,
# and their sibling files that MUST NOT be skipped (the #5911 over-match bug).
EXACT_SKIP_CASES = [
    # lock family: only `lock` and `lock5` are native skip entries.
    ("lock.test", True),
    ("lock5.test", True),
    ("lock2.test", False),
    ("lock3.test", False),
    ("lock4.test", False),
    ("lock6.test", False),
    ("lock7.test", False),
    ("nolock.test", False),
    ("sharedlock.test", False),
    ("shmlock.test", False),
    ("superlock.test", False),
    # jrnlmode family: `jrnlmode` and `jrnlmode3` skip; `jrnlmode2` does not.
    ("jrnlmode.test", True),
    ("jrnlmode3.test", True),
    ("jrnlmode2.test", False),
    # quota family: only `quota-glob` skips; `quota`/`quota2` do not.
    ("quota-glob.test", True),
    ("quota.test", False),
    ("quota2.test", False),
    # snapshot family: only `snapshot` skips; numbered/fault siblings do not.
    ("snapshot.test", True),
    ("snapshot2.test", False),
    ("snapshot3.test", False),
    ("snapshot4.test", False),
    ("snapshot_fault.test", False),
    ("snapshot_up.test", False),
    # bind family: only `bind` skips; `bind2`/`bindxfer` do not.
    ("bind.test", True),
    ("bind2.test", False),
    ("bindxfer.test", False),
    # symlink family: only `symlink` skips; `symlink2` does not.
    ("symlink.test", True),
    ("symlink2.test", False),
    # badutf: both `badutf` and `badutf2` are native skip entries.
    ("badutf.test", True),
    ("badutf2.test", True),
    # shared: only `shared6` skips; other shared* files do not.
    ("shared6.test", True),
    ("shared.test", False),
    ("shared2.test", False),
    ("shared7.test", False),
    # strict: `strict2` skips; `strict1` does not.
    ("strict2.test", True),
    ("strict1.test", False),
    # single-file C-API entries with no siblings.
    ("manydb.test", True),
    ("varint.test", True),
    ("pagesize.test", True),
    ("filefmt.test", True),
    ("colmeta.test", True),
    ("tableapi.test", True),
    ("ptrchng.test", True),
    ("ieee754.test", True),
    ("trustschema1.test", True),
]

# Intentionally-broad family patterns (pre-existing, deliberately unanchored).
# These MUST continue to skip the whole family.
BROAD_SKIP_CASES = [
    ("wal.test", True),
    ("wal2.test", True),
    ("walcrash.test", True),
    ("journal1.test", True),
    ("memjournal.test", True),
    ("vacuum.test", True),
    ("vacuum2.test", True),
    ("autovacuum.test", True),
    ("attach.test", True),
    ("attach2.test", True),
    ("malloc.test", True),
    ("corrupt.test", True),
    ("corruptL.test", True),  # exact entry AND caught by broad `corrupt`
    ("crash.test", True),
    ("fts3.test", True),
    ("rtree.test", True),
    # Core SQL files that must NOT be skipped.
    ("select1.test", False),
    ("where.test", False),
    ("join.test", False),
    ("insert.test", False),
    ("index.test", False),
]


def _skipped(name: str) -> bool:
    return TclTestParser().should_skip_file(name) is not None


def test_exact_skip_semantics():
    failures = []
    for name, expected in EXACT_SKIP_CASES:
        actual = _skipped(name)
        if actual != expected:
            failures.append(
                f"{name}: expected skip={expected}, got skip={actual}"
            )
    assert not failures, "Exact-skip mismatches:\n  " + "\n  ".join(failures)


def test_broad_family_skip_semantics():
    failures = []
    for name, expected in BROAD_SKIP_CASES:
        actual = _skipped(name)
        if actual != expected:
            failures.append(
                f"{name}: expected skip={expected}, got skip={actual}"
            )
    assert not failures, "Broad-skip mismatches:\n  " + "\n  ".join(failures)


def test_all_native_exact_entries_have_a_stem_entry():
    """Every SKIP_FILE_EXACT stem must actually be skipped as `<stem>.test`."""
    parser = TclTestParser()
    for stem in TclTestParser.SKIP_FILE_EXACT:
        assert parser.should_skip_file(f"{stem}.test") is not None, (
            f"exact entry {stem!r} unexpectedly not skipped"
        )


def test_no_broad_pattern_over_matches_removed_5844_entries():
    """The #5844 per-file stems must no longer live in the broad regex."""
    over_match_probes = [
        "lock2", "lock7", "jrnlmode2", "quota", "quota2",
        "snapshot2", "bind2", "symlink2",
    ]
    parser = TclTestParser()
    for stem in over_match_probes:
        assert parser.should_skip_file(f"{stem}.test") is None, (
            f"{stem}.test should NOT be skipped after #5911 fix"
        )


if __name__ == "__main__":
    tests = [
        test_exact_skip_semantics,
        test_broad_family_skip_semantics,
        test_all_native_exact_entries_have_a_stem_entry,
        test_no_broad_pattern_over_matches_removed_5844_entries,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except AssertionError as exc:
            failed += 1
            print(f"FAIL  {t.__name__}\n{exc}")
    if failed:
        print(f"\n{failed} test(s) failed")
        sys.exit(1)
    print(f"\nAll {len(tests)} tests passed")
