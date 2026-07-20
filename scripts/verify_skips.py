#!/usr/bin/env python3
"""
Skip List Verification Tool

Tests whether skipped TCL tests can now pass.
For each skip entry, temporarily removes it and runs just that test.

Usage:
    ./scripts/verify_skips.py                    # Check all skips
    ./scripts/verify_skips.py --category GLOB    # Check GLOB-related skips
    ./scripts/verify_skips.py --test index-23.0  # Check specific test
"""

import argparse
import fnmatch
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

# Repository root
REPO_ROOT = Path(__file__).parent.parent
SHIM_FILE = REPO_ROOT / "scripts" / "tester_vibesql.tcl"
TCL_TEST_DIR = REPO_ROOT / "docs" / "reference" / "sqlite" / "test"

# ---------------------------------------------------------------------------
# Skip-honesty audit (issue #6154 / epic #5779)
#
# See docs/reference/tcl-skip-policy.md for the two-bucket taxonomy. The audit
# below is the structural enforcement of the standing rule "no new skip may be
# added without declaring its bucket": it scans every whole-file / pattern skip
# rationale for Bucket-B smell phrases (in-scope-SQL "differs"/"not implemented"
# language) and fails if it finds one that is neither on the acknowledged
# Bucket-B worklist nor declared as a Bucket-A category override.
# ---------------------------------------------------------------------------

# Rationale substrings that mark a skip as hiding an in-scope SQL gap.
BUCKET_B_SMELL_PHRASES = (
    "behavior differs",
    "handling differs",
    "features differ",
    "feature differs",
    "not fully supported",
    "not implemented",
    "optimization differs",
    "options differ",
)

# Pattern/file keys already adjudicated Bucket B in the #6154 static pass. They
# are expected to carry smell phrases; the policy doc records the Phase-2
# fix-or-unskip worklist for each. Keyed by the exact array key in the shim.
ACK_BUCKET_B = {
    # curator-flagged
    "collate1-", "collate2-", "collate3-", "collate4-", "collate5-",
    "collate7-", "collate8-", "collate9-", "collateA-",
    "types-", "types2-", "subquery-",
    "without_rowid1-", "without_rowid2-", "without_rowid5-", "without_rowid6-",
    "autoindex3-", "autoindex4-", "autoindex5-",
    "unique2-", "rowid-", "resolver01-", "misc3-", "misc4-", "aggnested-",
    # additional entries flagged by this audit
    "printf2-", "e_totalchanges-",
    "altertab2-", "altertab3-", "alterlegacy-",
    "window1-66.", "window2-66.", "window1-69.",
    "orderby8-1.", "orderbyA-",
    "boundary1-", "boundary2-", "boundary3-", "boundary4-",
    "like2-", "tableopts-", "randexpr-", "sidedelete-",
}

# Pattern/file keys whose rationale contains a smell phrase but which are
# defensibly Bucket A (the phrase describes an out-of-scope subsystem, not an
# in-scope SQL gap). Keep this list short and principled — each maps to a
# category in docs/reference/tcl-skip-policy.md.
ACK_BUCKET_A_OVERRIDE = {
    "e_wal-",        # A2: VibeSQL ships its own WAL; SQLite WAL-pragma pager semantics N/A
    "indexexpr3-",   # A7: asserts EXPLAIN COVERING INDEX output, not results
    "utf16align-",   # A9: UTF-16 encoding internals; VibeSQL is UTF-8 by construction
}

# Human-readable names for the Bucket-A categories defined in
# docs/reference/tcl-skip-policy.md. Used by the completeness audit (to validate
# category codes) and by the by-category skip report (--categorize-skips).
BUCKET_A_CATEGORIES = {
    "A1": "C-API / statement-handle surface",
    "A2": "VFS / pager internals",
    "A3": "Unshipped extensions / virtual tables",
    "A4": "Incremental blob I/O",
    "A5": "Harness-only functions & TCL helpers",
    "A6": "Permutation / suite dispatchers",
    "A7": "Internal optimizer / VDBE counters",
    "A8": "Built-in-test fuzzers",
    "A9": "Documented intentional engine divergence",
    "A10": "Error-message-format divergence",
    "A11": "CLI shell / command-line tooling surface",
    "A12": "Concurrency / threading / multi-process model",
}

# Canonical Bucket-A classification of every whole-file (vibesql_skip_files) and
# every Bucket-A pattern (vibesql_skip_patterns) declaration, keyed by the exact
# array key in the shim, mapping to a category code in BUCKET_A_CATEGORIES. This
# is the single machine-readable source of truth behind the prose taxonomy in
# docs/reference/tcl-skip-policy.md.
#
# The audit (--audit-buckets) requires EVERY whole-file/pattern skip to be either
# in this map (declared Bucket A) or in ACK_BUCKET_B (declared Bucket B). An entry
# in neither is an *undeclared* skip and fails the check — this is the structural
# enforcement of "no skip may be added without declaring its bucket" (issue #6154
# deliverable 4). When a new skip is added to the shim, it MUST be added here (with
# a category) or to ACK_BUCKET_B (with a policy-doc worklist entry).
BUCKET_A_CLASSIFICATION = {
    # --- whole-file skips (vibesql_skip_files), all 60 ---
    # A1: C-API / statement-handle surface
    "capi2": "A1", "capi3": "A1", "capi3b": "A1", "capi3c": "A1",
    "capi3d": "A1", "capi3e": "A1", "tkt2409": "A1", "colmeta": "A1",
    "tableapi": "A1", "bind": "A1", "ptrchng": "A1", "delete_db": "A1",
    "intarray": "A1", "snapshot": "A1", "shared6": "A1", "symlink": "A1",
    "quota-glob": "A1", "varint": "A1",
    # A2: VFS / pager internals
    "jrnlmode": "A2", "jrnlmode3": "A2", "pagesize": "A2", "filefmt": "A2",
    "corruptL": "A2", "lock": "A2", "lock5": "A2", "sort2": "A2",
    "strict2": "A2",
    # A3: Unshipped extensions / virtual tables
    "fts3": "A3", "rtree": "A3", "index6": "A3", "index7": "A3",
    "orderby7": "A3", "ieee754": "A3", "trustschema1": "A3",
    # A4: Incremental blob I/O
    "incrblob": "A4", "incrblob2": "A4", "incrblob3": "A4", "incrblob4": "A4",
    "incrblob_err": "A4", "incrblobfault": "A4",
    # A5: Harness-only functions & TCL helpers
    "intreal": "A5", "func4": "A5", "trigger6": "A5", "update2": "A5",
    # A6: Permutation / suite dispatchers
    "all": "A6", "full": "A6", "quick": "A6", "veryquick": "A6",
    "extraquick": "A6", "rbu": "A6", "session": "A6",
    # A7: Internal optimizer / VDBE counters
    "insert4": "A7", "insert5": "A7", "whereJ": "A7", "where8": "A7",
    "malloc4": "A7", "manydb": "A7",
    # A8: Built-in-test fuzzers
    "bitvec": "A8",
    # A9: Documented intentional engine divergence
    "badutf": "A9", "badutf2": "A9",

    # --- Bucket-A pattern skips (vibesql_skip_patterns) ---
    # A2
    "e_wal-": "A2",
    "e_reindex-1.": "A2",  # writable_schema/B-tree-corruption REINDEX harness (#6195; precedent fkey1-8.3)
    # A5 (harness-only collation / temp-session / libc-time helpers)
    "select9-2.*.3": "A5", "select9-2.*.6": "A5",
    "temptable-": "A5", "temptable2-": "A5",
    "date-6.": "A5", "date4-": "A5",
    "e_reindex-2.": "A5", "reindex-2.": "A5", "reindex-3.": "A5",  # custom db-collate REINDEX (#6195, #5720)
    # A7 (EXPLAIN / VDBE-flag / fault-injection assertions)
    "indexexpr3-": "A7", "fordelete-": "A7",
    "fkey_malloc-": "A7", "windowfault-": "A7",
    # A9
    "utf16align-": "A9",
    # A10 (error-message-format divergence)
    "subselect-1.2": "A10",

    # --- issue #6180: certified out-of-scope failures reclassified as
    # Bucket-A whole-file skips (the operator-gated half of #6154). Rationale is
    # per-category in docs/reference/tcl-skip-policy.md; the matching skip-file
    # entries with prose rationales live in scripts/tester_vibesql.tcl.
    # A1 (33)
    "notify1": "A1", "notify2": "A1", "notify3": "A1", "hook": "A1", "hook2": "A1",
    "trace": "A1", "trace2": "A1", "trace3": "A1", "backup": "A1", "backup2": "A1",
    "backup4": "A1", "backup5": "A1", "backup_ioerr": "A1", "scanstatus": "A1",
    "scanstatus2": "A1", "dbstatus": "A1", "dbstatus2": "A1", "stmt": "A1", "bindxfer": "A1",
    "bind2": "A1", "snapshot2": "A1", "snapshot3": "A1", "snapshot4": "A1",
    "snapshot_up": "A1", "snapshot_fault": "A1", "cacheflush": "A1", "dataversion1": "A1",
    "busy": "A1", "busy2": "A1", "interrupt": "A1", "interrupt2": "A1", "openv2": "A1",
    "shrink": "A1",
    # A2 (76)
    "memjournal": "A2", "memjournal2": "A2", "mjournal": "A2", "subjournal": "A2",
    "journal1": "A2", "journal2": "A2", "journal3": "A2", "trans2": "A2", "avtrans": "A2",
    "pager1": "A2", "pager2": "A2", "pager3": "A2", "pager4": "A2", "walmode": "A2",
    "jrnlmode2": "A2", "cache": "A2", "cachespill": "A2", "pcache": "A2", "pcache2": "A2",
    "lookaside": "A2", "quota": "A2", "quota2": "A2", "shared": "A2", "shared2": "A2",
    "shared3": "A2", "shared4": "A2", "shared7": "A2", "shared8": "A2", "shared9": "A2",
    "sharedA": "A2", "shared_err": "A2", "sharedlock": "A2", "multiplex2": "A2",
    "multiplex3": "A2", "multiplex4": "A2", "securedel": "A2", "securedel2": "A2",
    "cksumvfs": "A2", "reservebytes": "A2", "chunksize": "A2", "fallocate": "A2",
    "superlock": "A2", "nolock": "A2", "tempdb": "A2", "tempdb2": "A2", "corrupt": "A2",
    "corrupt2": "A2", "corrupt4": "A2", "corrupt6": "A2", "corruptB": "A2", "corruptC": "A2",
    "corruptF": "A2", "ioerr": "A2", "ioerr2": "A2", "io": "A2", "wal9": "A2", "walseh1": "A2",
    "e_walckpt": "A2", "e_walhook": "A2", "e_walauto": "A2", "exclusive": "A2",
    "exclusive2": "A2", "lock2": "A2", "lock3": "A2", "lock4": "A2", "lock6": "A2",
    "lock7": "A2", "rowallock": "A2", "rdonly": "A2", "readonly": "A2", "uri": "A2",
    "uri2": "A2", "e_uri": "A2", "8_3_names": "A2", "shortread1": "A2", "diskfull": "A2",
    # A3 (4)
    "icu": "A3", "normalize": "A3", "extension01": "A3", "stmtvtab1": "A3",
    # A4 (4)
    "e_blobopen": "A4", "e_blobwrite": "A4", "e_blobclose": "A4", "e_blobbytes": "A4",
    # A7 (23)
    "malloc": "A7", "malloc3": "A7", "malloc5": "A7", "memsubsys1": "A7", "memsubsys2": "A7",
    "mem5": "A7", "mmap1": "A7", "pagerfault": "A7", "pagerfault2": "A7", "indexfault": "A7",
    "btreefault": "A7", "rollbackfault": "A7", "sortfault": "A7", "tempfault": "A7",
    "savepointfault": "A7", "existsfault": "A7", "altermalloc2": "A7", "altermalloc3": "A7",
    "mallocAll": "A7", "softheap1": "A7", "memleak": "A7", "fuzz_malloc": "A7",
    "imposter1": "A7",
    # A9 (3)
    "enc": "A9", "enc2": "A9", "enc3": "A9",
    # A11 (9)
    "shell1": "A11", "shell2": "A11", "shell3": "A11", "shell4": "A11", "shell5": "A11",
    "shell7": "A11", "shell9": "A11", "shellA": "A11", "sqldiff1": "A11",
    # A12 (12)
    "mutex1": "A12", "mutex2": "A12", "thread001": "A12", "thread002": "A12",
    "thread003": "A12", "thread004": "A12", "thread005": "A12", "thread1": "A12",
    "thread2": "A12", "thread3": "A12", "walthread": "A12", "pendingrace": "A12",
}


def parse_skip_list(shim_content: str) -> dict:
    """Extract skip entries from the shim file."""
    skips = {}

    # Find the vibesql_skip_tests array
    match = re.search(r'array set vibesql_skip_tests \{(.*?)\}', shim_content, re.DOTALL)
    if not match:
        print("ERROR: Could not find vibesql_skip_tests in shim file")
        return skips

    skip_section = match.group(1)

    # Parse each skip entry: test_name "reason"
    pattern = r'^\s*([a-zA-Z0-9_.-]+)\s+"([^"]+)"'
    for line in skip_section.split('\n'):
        m = re.match(pattern, line.strip())
        if m:
            test_name = m.group(1)
            reason = m.group(2)
            skips[test_name] = reason

    return skips


def parse_partial_skip_files(shim_content: str) -> dict:
    """Extract partial-file documented-skip entries from the shim file.

    These are files (keyed by basename) where a documented subset of tests is
    auto-skipped by a regex detector while the rest of the file keeps running.
    They are recorded in the vibesql_partial_skip_files array purely for
    discoverability (audit-by-file-name); the actual skipping is done elsewhere,
    so we only report them here rather than attempting to unskip-and-rerun.
    """
    partial = {}

    match = re.search(
        r'array set vibesql_partial_skip_files \{(.*?)\n\}',
        shim_content,
        re.DOTALL,
    )
    if not match:
        # Not an error: the array is optional.
        return partial

    section = match.group(1)

    # Each entry: file_basename "reason" — reason may contain escaped chars but
    # no unescaped double-quote, matching the TCL array literal convention.
    pattern = r'^\s*([a-zA-Z0-9_.-]+)\s+"([^"]+)"'
    for line in section.split('\n'):
        m = re.match(pattern, line.strip())
        if m:
            partial[m.group(1)] = m.group(2)

    return partial


def parse_whole_file_skips(shim_content: str) -> dict:
    """Extract whole-file skip entries from the shim file.

    These are the vibesql_skip_files array entries: test FILES (keyed by
    basename, e.g. ``intreal``, ``capi3``) that are skipped in their entirety
    because every test in them exercises a SQLite-internal / C-API surface with
    no SQL-CLI-reachable coverage. This is the largest skip category and, until
    now, was invisible to the audit (only vibesql_skip_tests and, since #6066,
    vibesql_partial_skip_files were parsed).

    Like parse_partial_skip_files, this is a DISCOVERABILITY report: the actual
    skipping is enforced elsewhere in the shim, so we only surface the entries
    rather than unskip-and-rerun them.

    Resilient parsing (matching the #6066 partial-skip style):
      * The array literal closes with a bare ``}`` at the start of a line, so we
        capture up to ``\\n}`` rather than the first ``}`` (reasons contain
        braces, e.g. ``{ run_test_suite fts3 }``).
      * A reason may embed an escaped double-quote (``\\"sqlite3_shutdown\\"`` in
        the sort2 entry), so the value pattern accepts escaped characters
        instead of stopping at the first quote.
    """
    files = {}

    match = re.search(
        r'array set vibesql_skip_files \{(.*?)\n\}',
        shim_content,
        re.DOTALL,
    )
    if not match:
        # Not an error: the array is optional (mirrors parse_partial_skip_files).
        return files

    section = match.group(1)

    # Each entry: file_basename "reason". The reason is a TCL double-quoted
    # string whose only escaped metacharacter of concern is an embedded \" —
    # (?:[^"\\]|\\.)* consumes any escaped char (\", \\, ...) without terminating
    # the value early. Anchored per-line so a stray brace inside a reason (e.g.
    # "{ run_test_suite fts3 }") does not split an entry.
    pattern = r'^\s*([a-zA-Z0-9_.-]+)\s+"((?:[^"\\]|\\.)*)"'
    for line in section.split('\n'):
        m = re.match(pattern, line.strip())
        if m:
            # Unescape \" so the reported reason reads naturally.
            files[m.group(1)] = m.group(2).replace('\\"', '"')

    return files


def parse_skip_patterns(shim_content: str) -> dict:
    """Extract glob-pattern skip entries from the shim file.

    These are the ``vibesql_skip_patterns`` list entries, each a TCL brace-quoted
    ``{pattern "reason"}``. The pattern is a glob prefix (e.g. ``collate1-``) and
    the reason is a double-quoted rationale string. Returns ``{pattern: reason}``.
    """
    patterns = {}

    match = re.search(
        r'variable vibesql_skip_patterns \{(.*?)\n\}',
        shim_content,
        re.DOTALL,
    )
    if not match:
        return patterns

    # Each entry is {<pattern> "<reason>"} on its own line. The pattern token has
    # no whitespace; the reason is a double-quoted string.
    for m in re.finditer(r'\{(\S+)\s+"((?:[^"\\]|\\.)*)"\}', match.group(1)):
        patterns[m.group(1)] = m.group(2).replace('\\"', '"')

    return patterns


def audit_buckets(shim_content: str) -> int:
    """Enforce the skip-honesty standing rule (issue #6154).

    Runs three gates over every whole-file (vibesql_skip_files) and pattern
    (vibesql_skip_patterns) skip declaration:

      1. COMPLETENESS (deliverable 4): every skip must be declared in exactly one
         bucket — present in BUCKET_A_CLASSIFICATION (a category code) or in
         ACK_BUCKET_B (the Phase-2 worklist). A skip in NEITHER is *undeclared*
         and fails. This turns "no skip may be added without declaring its bucket"
         into an enforced check rather than a review convention: a newly-added
         shim skip that no one classified will fail CI here.

      2. CONSISTENCY: no skip may be in both maps, and every Bucket-A category
         code must be a known A1–A10 category.

      3. ANTI-HIDING (the original smell gate): a Bucket-B smell phrase
         ("behavior differs", "not implemented", …) attached to a skip that is
         classified Bucket A is only allowed when the key is on the explicit
         ACK_BUCKET_A_OVERRIDE list (a certified false-positive, e.g. e_wal-).
         This stops an in-scope-SQL gap from being buried in Bucket A to silence
         it.

    Returns 0 when clean, 1 on any violation. See docs/reference/tcl-skip-policy.md
    for the taxonomy this enforces.
    """
    patterns = parse_skip_patterns(shim_content)
    files = parse_whole_file_skips(shim_content)

    def smells(reason: str):
        rl = reason.lower()
        return [p for p in BUCKET_B_SMELL_PHRASES if p in rl]

    acknowledged_b = []       # smell-bearing, on the Bucket-B worklist (expected)
    undeclared = []           # in neither bucket map (deliverable-4 failure)
    both_buckets = []         # in both maps (contradiction)
    bad_category = []         # Bucket-A category code not in A1–A10
    hidden_b = []             # smell-bearing but classified A without an override

    all_entries = []
    for kind, entries in (("pattern", patterns), ("whole-file", files)):
        for key, reason in sorted(entries.items()):
            all_entries.append((kind, key, reason))

    for kind, key, reason in all_entries:
        in_a = key in BUCKET_A_CLASSIFICATION
        in_b = key in ACK_BUCKET_B
        hits = smells(reason)

        if in_a and in_b:
            both_buckets.append((kind, key))
        if not in_a and not in_b:
            undeclared.append((kind, key, hits, reason))
            continue
        if in_a:
            cat = BUCKET_A_CLASSIFICATION[key]
            if cat not in BUCKET_A_CATEGORIES:
                bad_category.append((kind, key, cat))
            if hits and key not in ACK_BUCKET_A_OVERRIDE:
                hidden_b.append((kind, key, hits, reason))
        if in_b and hits:
            acknowledged_b.append((kind, key, hits))

    print("=" * 60)
    print("SKIP-HONESTY BUCKET AUDIT (issue #6154)")
    print("=" * 60)
    print(f"Patterns scanned:      {len(patterns)}")
    print(f"Whole-file scanned:    {len(files)}")
    print(f"Bucket-A classified:   "
          f"{sum(1 for _, k, _ in all_entries if k in BUCKET_A_CLASSIFICATION)}")
    print(f"Bucket-B worklist:     "
          f"{sum(1 for _, k, _ in all_entries if k in ACK_BUCKET_B)}")
    print(f"Bucket-A overrides:    {len(ACK_BUCKET_A_OVERRIDE)}")

    if acknowledged_b:
        print("\nAcknowledged Bucket-B skips (Phase-2 fix-or-unskip worklist):")
        for kind, key, hits in acknowledged_b:
            print(f"  [{kind}] {key}  ({', '.join(hits)})")

    failed = False

    if undeclared:
        failed = True
        print("\nERROR: undeclared skip(s) — every skip must declare a bucket")
        print("(add the key to BUCKET_A_CLASSIFICATION with an A1–A10 category, or")
        print(" to ACK_BUCKET_B with a policy-doc worklist entry):")
        for kind, key, hits, reason in undeclared:
            extra = f" (smell: {', '.join(hits)})" if hits else ""
            print(f"  [{kind}] {key}{extra}")
            print(f"      reason: {reason[:100]}")

    if both_buckets:
        failed = True
        print("\nERROR: skip(s) declared in BOTH Bucket A and Bucket B:")
        for kind, key in both_buckets:
            print(f"  [{kind}] {key}")

    if bad_category:
        failed = True
        print("\nERROR: Bucket-A skip(s) with an unknown category code:")
        for kind, key, cat in bad_category:
            print(f"  [{kind}] {key} -> {cat} (not in A1–A10)")

    if hidden_b:
        failed = True
        print("\nERROR: Bucket-B smell on a Bucket-A skip without an override")
        print("(a 'differs'/'not implemented' in-scope-SQL gap may not be buried")
        print(" in Bucket A; add to ACK_BUCKET_B or justify via ACK_BUCKET_A_OVERRIDE):")
        for kind, key, hits, reason in hidden_b:
            print(f"  [{kind}] {key}  ({', '.join(hits)})")
            print(f"      reason: {reason[:100]}")

    if failed:
        n = len(undeclared) + len(both_buckets) + len(bad_category) + len(hidden_b)
        print(f"\nFAIL: {n} bucket-declaration violation(s).")
        return 1

    print("\nOK: every whole-file/pattern skip declares exactly one bucket "
          "(Bucket A category or Bucket-B worklist), and no in-scope-SQL gap is "
          "hidden in Bucket A.")
    return 0


def find_stale_whole_file_skips(file_skips: dict) -> list:
    """Return skip-file basenames whose <basename>.test no longer exists.

    A whole-file skip names a test FILE by basename; if the referenced
    ``<basename>.test`` is gone from the test tree, the entry is stale and can be
    removed. Cheap check: one filesystem stat per entry, no test execution.
    """
    stale = []
    for basename in sorted(file_skips):
        if not (TCL_TEST_DIR / f"{basename}.test").exists():
            stale.append(basename)
    return stale


def get_test_file(test_name: str) -> str:
    """Determine which test file contains a test based on naming convention."""
    # Test names follow pattern: filename-N.N or filename-N.N.N
    # e.g., "index-23.0" -> "index.test"
    # e.g., "insert2-3.2" -> "insert2.test"
    match = re.match(r'^([a-zA-Z0-9]+)-', test_name)
    if match:
        base = match.group(1)
        # Handle numbered test files (e.g., "select1", "insert2")
        test_file = f"{base}.test"
        if (TCL_TEST_DIR / test_file).exists():
            return test_file
    return None


def run_single_test(test_name: str, test_file: str, temp_shim: Path) -> tuple:
    """
    Run a single test using a modified shim that doesn't skip it.
    Returns (passed: bool, output: str)
    """
    # Run the test file and grep for the specific test result
    cmd = [
        "tclsh",
        str(temp_shim),
        str(TCL_TEST_DIR / test_file)
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(REPO_ROOT)
        )
        output = result.stdout + result.stderr

        # Check if the test passed
        # Look for patterns like "index-23.0... Ok" or "FAIL: index-23.0"
        if re.search(rf'{re.escape(test_name)}.*Ok\b', output):
            return (True, output)
        elif re.search(rf'FAIL.*{re.escape(test_name)}|{re.escape(test_name)}.*FAIL', output):
            return (False, output)
        elif re.search(rf'{re.escape(test_name)}.*SKIP', output, re.IGNORECASE):
            return (None, output)  # Still skipped
        else:
            # Unknown result - look for any output about the test
            return (False, output)
    except subprocess.TimeoutExpired:
        return (False, "TIMEOUT")
    except Exception as e:
        return (False, str(e))


def create_modified_shim(original_content: str, tests_to_unskip: list) -> str:
    """Create a modified shim with certain tests removed from the skip list."""
    modified = original_content

    for test_name in tests_to_unskip:
        # Remove the skip entry for this test
        # Match: test_name "reason"
        pattern = rf'^\s*{re.escape(test_name)}\s+"[^"]+"\n?'
        modified = re.sub(pattern, '', modified, flags=re.MULTILINE)

    return modified


def categorize_skips(skips: dict) -> dict:
    """Categorize skips by reason type."""
    categories = {
        "GLOB": [],
        "TEMP_TABLE": [],
        "DB_CHANGES": [],
        "EXPLAIN": [],
        "CASCADE": [],
        "SQLITE_API": [],
        "EXPRESSION_INDEX": [],
        "OTHER": []
    }

    for test_name, reason in skips.items():
        reason_lower = reason.lower()

        if "glob" in reason_lower:
            categories["GLOB"].append(test_name)
        elif "temp" in reason_lower and "table" in reason_lower:
            categories["TEMP_TABLE"].append(test_name)
        elif "changes" in reason_lower or "total_changes" in reason_lower:
            categories["DB_CHANGES"].append(test_name)
        elif "explain" in reason_lower or "eqp" in reason_lower:
            categories["EXPLAIN"].append(test_name)
        elif "cascade" in reason_lower:
            categories["CASCADE"].append(test_name)
        elif "sqlite" in reason_lower and ("api" in reason_lower or "internal" in reason_lower or "stat" in reason_lower):
            categories["SQLITE_API"].append(test_name)
        elif "expression" in reason_lower and "index" in reason_lower:
            categories["EXPRESSION_INDEX"].append(test_name)
        else:
            categories["OTHER"].append(test_name)

    return categories


def verify_tests(tests_to_check: list, shim_content: str, verbose: bool = False) -> dict:
    """Verify a list of tests and return results."""
    results = {
        "passed": [],
        "failed": [],
        "still_skipped": [],
        "error": []
    }

    # Group tests by file for efficiency
    tests_by_file = {}
    for test_name in tests_to_check:
        test_file = get_test_file(test_name)
        if test_file:
            if test_file not in tests_by_file:
                tests_by_file[test_file] = []
            tests_by_file[test_file].append(test_name)
        else:
            results["error"].append((test_name, "Could not determine test file"))

    # Process each test file
    for test_file, tests in tests_by_file.items():
        print(f"\nTesting {len(tests)} skip(s) from {test_file}...")

        # Create modified shim with these tests unskipped
        modified_shim = create_modified_shim(shim_content, tests)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.tcl', delete=False) as f:
            f.write(modified_shim)
            temp_shim_path = Path(f.name)

        try:
            for test_name in tests:
                passed, output = run_single_test(test_name, test_file, temp_shim_path)

                if passed is True:
                    print(f"  OUTDATED SKIP: {test_name}")
                    results["passed"].append(test_name)
                elif passed is False:
                    if verbose:
                        print(f"  STILL FAILS: {test_name}")
                    results["failed"].append(test_name)
                else:
                    if verbose:
                        print(f"  STILL SKIPPED: {test_name}")
                    results["still_skipped"].append(test_name)
        finally:
            temp_shim_path.unlink()

    return results


# ---------------------------------------------------------------------------
# By-category excluded-skip report (issue #6154 deliverable 5, LOCAL run)
#
# Attributes the `skipped` detail rows of the LOCAL results DB to the Bucket-A
# categories (and the Bucket-B worklist) using the same classification that the
# audit enforces — so `make test-tcl-status` can state "the excluded-test count
# by category" for the run it just produced. This is the automatable, in-tree
# half of the reconciled-denominator deliverable; the CERTIFIED bench-runner run
# remains an operator step (see docs/reference/tcl-skip-policy.md "Deferred
# work"). Input is the CLI `--format raw` framing (fields joined by ASCII 31,
# rows terminated by ASCII 30), which is unambiguous even for file paths that
# contain pipes or newlines.
# ---------------------------------------------------------------------------

_RAW_FIELD_SEP = "\x1f"  # ASCII 31, between values within a row
_RAW_ROW_SEP = "\x1e"    # ASCII 30, terminates each row


def _basename_of(file_path: str) -> str:
    """'.../sqlite/test/collate1.test' -> 'collate1'."""
    base = file_path.rsplit("/", 1)[-1]
    if base.endswith(".test"):
        base = base[:-len(".test")]
    return base


def build_skip_lookups(shim_content: str):
    """Return (file_cat, pattern_cat) mapping each shim skip key to (bucket, cat).

    bucket is 'A' or 'B'; cat is an A-code (for Bucket A) or None (for Bucket B).
    Whole-file keys are matched by test-file basename; pattern keys by glob over
    the test name. Only keys that are actually declared (in BUCKET_A_CLASSIFICATION
    or ACK_BUCKET_B) are included — the audit guarantees that is every key.
    """
    def bucket_of(key):
        if key in BUCKET_A_CLASSIFICATION:
            return ("A", BUCKET_A_CLASSIFICATION[key])
        if key in ACK_BUCKET_B:
            return ("B", None)
        return None

    file_cat = {}
    for key in parse_whole_file_skips(shim_content):
        b = bucket_of(key)
        if b:
            file_cat[key] = b

    pattern_cat = {}
    for key in parse_skip_patterns(shim_content):
        b = bucket_of(key)
        if b:
            pattern_cat[key] = b

    return file_cat, pattern_cat


def classify_skip_row(file_path: str, test_name: str, file_cat: dict, pattern_cat: dict):
    """Classify one `skipped` detail row.

    Returns (bucket, category, key):
      * bucket   — 'A', 'B', or 'named' (a vibesql_skip_tests entry or a runtime
                   ifcapable self-skip that is not individually bucketed in the
                   static pass — Phase-2 / operator-deferred).
      * category — an A-code for Bucket A, else None.
      * key      — the matching shim skip key, or None for 'named'.
    """
    base = _basename_of(file_path)

    # 1. Whole-file skip (every row in the file shares the file's bucket).
    if base in file_cat:
        bucket, cat = file_cat[base]
        return (bucket, cat, base)

    # 2. atof1 partial skip: only the real2hex()/hex2real() loop tests are
    #    skipped (harness-only functions, A5); the non-loop atof1-2.x/atof-3.x
    #    tests keep RUNNING and are never 'skipped', so any atof1 skipped row is a
    #    loop test. (Landmine #6065 — see the shim detector.)
    if base == "atof1":
        return ("A", "A5", "atof1 (partial: real2hex/hex2real)")

    # 3. Pattern skip: match the test name against '<pattern>*'. Prefer the most
    #    specific (longest) pattern so overlapping prefixes are deterministic.
    for key in sorted(pattern_cat, key=len, reverse=True):
        if fnmatch.fnmatch(test_name, key + "*") or fnmatch.fnmatch(test_name, key):
            bucket, cat = pattern_cat[key]
            return (bucket, cat, key)

    # 4. Named skip / runtime self-skip — not individually bucketed here.
    return ("named", None, None)


def categorize_skip_stream(shim_content: str, stream_text: str) -> int:
    """Print a by-category breakdown of `skipped` rows read from stdin.

    stream_text is the CLI `--format raw` payload of a
    `SELECT file_path, test_name ... WHERE status='skipped'` query. Reconciles:
    the printed per-category counts sum to the total skipped rows.
    """
    file_cat, pattern_cat = build_skip_lookups(shim_content)

    rows = [r for r in stream_text.split(_RAW_ROW_SEP) if r != ""]
    if not rows:
        print("No 'skipped' rows in the latest local run "
              "(nothing to attribute by category).")
        return 0

    a_counts = Counter()   # A-code -> n
    b_count = 0
    named_count = 0
    for r in rows:
        fields = r.split(_RAW_FIELD_SEP)
        file_path = fields[0] if fields else ""
        test_name = fields[1] if len(fields) > 1 else ""
        bucket, cat, _key = classify_skip_row(file_path, test_name, file_cat, pattern_cat)
        if bucket == "A":
            a_counts[cat] += 1
        elif bucket == "B":
            b_count += 1
        else:
            named_count += 1

    total = sum(a_counts.values()) + b_count + named_count
    a_total = sum(a_counts.values())

    print(f"Excluded (skipped) rows in latest local run: {total}")
    print("(Attributed via the same classification the bucket audit enforces.)")
    print()
    print("Bucket A — out-of-scope by design (honest to exclude):")
    for code in sorted(BUCKET_A_CATEGORIES, key=lambda c: int(c[1:])):
        n = a_counts.get(code, 0)
        if n:
            print(f"  {code:<3} {BUCKET_A_CATEGORIES[code]:<42} {n:>8}")
    print(f"  {'':<3} {'Bucket A subtotal':<42} {a_total:>8}")
    print()
    print("Bucket B — skipped-because-failing (must reach zero; honest to count):")
    print(f"  {'':<3} {'Bucket B subtotal':<42} {b_count:>8}")
    print()
    print("Named / runtime self-skips (per-entry adjudication deferred — Phase 2):")
    print(f"  {'':<3} {'vibesql_skip_tests + ifcapable self-skips':<42} {named_count:>8}")
    print(f"  {'':<3} {'':<42} {'-' * 8:>8}")
    print(f"  {'':<3} {'TOTAL excluded (reconciles to run skipped)':<42} {total:>8}")
    print()
    print("Note: this is the LOCAL run's excluded count. The CERTIFIED "
          "bench-runner denominator")
    print("remains an operator step — see docs/reference/tcl-skip-policy.md "
          "\"Deferred work\".")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Verify TCL test skip entries")
    parser.add_argument("--category", help="Check only skips in this category")
    parser.add_argument("--test", help="Check only this specific test")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--list-categories", action="store_true", help="List skip categories and counts")
    parser.add_argument("--audit-buckets", action="store_true",
                        help="Enforce the skip-honesty standing rule (issue #6154): "
                             "fail if any whole-file/pattern skip lacks a declared bucket")
    parser.add_argument("--categorize-skips", action="store_true",
                        help="Read a `--format raw` (file_path, test_name) skip stream "
                             "on stdin and print the excluded-test count by Bucket-A "
                             "category (issue #6154 deliverable 5, local run)")
    args = parser.parse_args()

    # Read the shim file
    with open(SHIM_FILE, 'r') as f:
        shim_content = f.read()

    if args.audit_buckets:
        return audit_buckets(shim_content)

    if args.categorize_skips:
        return categorize_skip_stream(shim_content, sys.stdin.read())

    # Parse skip list
    skips = parse_skip_list(shim_content)
    print(f"Found {len(skips)} skip entries")

    # Parse partial-file documented skips (discoverability records; not rerun-verified)
    partial_files = parse_partial_skip_files(shim_content)
    if partial_files:
        print(f"Found {len(partial_files)} partial-file documented skip(s)")

    # Parse whole-file skips (vibesql_skip_files) — the largest skip category.
    # Discoverability record; enforced elsewhere in the shim, so not rerun-verified.
    whole_files = parse_whole_file_skips(shim_content)
    if whole_files:
        print(f"Found {len(whole_files)} whole-file documented skip(s)")
    stale_whole_files = find_stale_whole_file_skips(whole_files)
    if stale_whole_files:
        print(
            f"WARNING: {len(stale_whole_files)} whole-file skip(s) reference a "
            f".test file that no longer exists: {', '.join(stale_whole_files)}"
        )

    # Categorize skips
    categories = categorize_skips(skips)

    if args.list_categories:
        print("\nSkip categories:")
        for cat, tests in sorted(categories.items(), key=lambda x: -len(x[1])):
            print(f"  {cat}: {len(tests)} tests")
        if partial_files:
            print(f"  PARTIAL_FILE: {len(partial_files)} files (documented per-test/regex subset skips)")
            for fname, reason in sorted(partial_files.items()):
                # Show a truncated reason so the record is auditable at a glance.
                summary = reason if len(reason) <= 100 else reason[:97] + "..."
                print(f"    - {fname}: {summary}")
        if whole_files:
            print(f"  WHOLE_FILE: {len(whole_files)} files (documented whole-file skips)")
            for fname, reason in sorted(whole_files.items()):
                # Show a truncated reason so the record is auditable at a glance.
                summary = reason if len(reason) <= 100 else reason[:97] + "..."
                flag = " [STALE: .test file missing]" if fname in stale_whole_files else ""
                print(f"    - {fname}: {summary}{flag}")
        return 0

    # Determine which tests to check
    tests_to_check = []

    if args.test:
        if args.test in skips:
            tests_to_check = [args.test]
        else:
            print(f"ERROR: Test '{args.test}' not found in skip list")
            return 1
    elif args.category:
        cat_upper = args.category.upper()
        if cat_upper in categories:
            tests_to_check = categories[cat_upper]
        else:
            print(f"ERROR: Category '{args.category}' not found")
            print(f"Available categories: {', '.join(categories.keys())}")
            return 1
    else:
        # Check all skips - but start with likely candidates
        # Priority: GLOB, DB_CHANGES, TEMP_TABLE
        tests_to_check = (
            categories["GLOB"] +
            categories["DB_CHANGES"] +
            categories["TEMP_TABLE"]
        )

    if not tests_to_check:
        print("No tests to check")
        return 0

    print(f"\nChecking {len(tests_to_check)} skip entries...")

    # Verify tests
    results = verify_tests(tests_to_check, shim_content, verbose=args.verbose)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Outdated skips (tests now pass): {len(results['passed'])}")
    print(f"Still failing:                   {len(results['failed'])}")
    print(f"Still skipped:                   {len(results['still_skipped'])}")
    print(f"Errors:                          {len(results['error'])}")

    if results['passed']:
        print("\nOUTDATED SKIPS TO REMOVE:")
        for test in results['passed']:
            print(f"  - {test}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
