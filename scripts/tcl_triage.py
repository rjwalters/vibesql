#!/usr/bin/env python3
"""Triage TCL conformance test failures into root-cause families.

This tool reads a VibeSQL TCL results database (``tcl_test_results.vbsql``) and
produces a Markdown report with:

1. Per-file failure counts (the biggest failing files).
2. Per-error-message counts (the raw, ungrouped messages).
3. **Root-cause families** — error messages normalized (quoted literals,
   numbers and hex addresses stripped) so that structurally-identical failures
   across many files/tests collapse into a single, rankable family.
4. A reconciliation section comparing the detail-table failure sum against the
   summary-table total, plus a certification stamp.

CRITICAL HONESTY CONTRACT
-------------------------
The report is PROMINENTLY stamped ``CERTIFIED`` or ``NON-CERTIFIED``.

- ``CERTIFIED`` is only claimed when the run's ``machine_tag`` matches a known
  certified bench-runner tag (see ``CERTIFIED_TAG_PATTERN``). Even then the
  operator must confirm the run corresponds to tag ``aws-c7i.8xlarge-32c-clean2``
  *or later* per issue #6155.
- Everything else (local runs, a stale backup, a DB with no ``machine_tag``
  column at all) is stamped ``NON-CERTIFIED`` and MUST NOT be used to file the
  per-family child fix-issues. Those depend on the certified count (~7,106 as of
  the certified ``clean2`` run) which cannot be reconciled on a local checkout.

The value of this tool is the *reusable, honest* triage pipeline — never a
fabricated certified number.

Usage
-----
    python3 scripts/tcl_triage.py [--db PATH] [--output PATH]
                                  [--vibesql PATH] [--run-id N]
                                  [--top-files N] [--top-errors N]
                                  [--top-families N]

Defaults to the canonical results DB
(``~/.vibesql/test_results/tcl_test_results.vbsql``) and fails loudly with a
clear message if it is absent — it never silently emits an empty or fabricated
report.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

CANONICAL_DB = os.path.expanduser("~/.vibesql/test_results/tcl_test_results.vbsql")

# A run is only treated as CERTIFIED when its machine_tag matches this pattern.
# The certified baseline for issue #6155 is tag `aws-c7i.8xlarge-32c-clean2`
# (632,911 passed / 7,106 failed / 17 markers). "clean2 or later" is expected;
# any aws bench-runner "clean" tag is treated as *candidate* certified, and the
# operator is still told to confirm it is clean2 or newer.
CERTIFIED_TAG_PATTERN = re.compile(r"aws-.*clean", re.IGNORECASE)

# Statuses that count as a failure for triage purposes (mirrors the canonical
# pass-rate math in CLAUDE.md: markers count as failures).
MARKER_STATUSES = ("timeout", "incomplete", "error")


# --------------------------------------------------------------------------- #
# Root-cause normalization (the unit-testable core)
# --------------------------------------------------------------------------- #

_HEX_RE = re.compile(r"0x[0-9a-fA-F]+")
_SQUOTE_RE = re.compile(r"'[^']*'")
_DQUOTE_RE = re.compile(r'"[^"]*"')
_BACKTICK_RE = re.compile(r"`[^`]*`")
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])\d+(?:\.\d+)?(?![A-Za-z0-9_])")
_WS_RE = re.compile(r"\s+")

NO_MESSAGE_FAMILY = "(no error message)"


def normalize_error_message(message: str | None) -> str:
    """Collapse a raw error message to a structural root-cause family key.

    Variable parts (quoted literals, numbers, hex addresses) are replaced with
    placeholders so that, e.g.::

        Query failed: ... Table 't1' already exists ... 1 statements failed
        Query failed: ... Table 'foo' already exists ... 1 statements failed

    both normalize to the same family::

        Query failed: ... Table '?' already exists ... N statements failed

    NULL / empty messages collapse to ``(no error message)`` so failures with no
    diagnostic are still counted as their own family rather than silently
    dropped.
    """
    if message is None:
        return NO_MESSAGE_FAMILY
    text = message.strip()
    if not text:
        return NO_MESSAGE_FAMILY

    # Order matters: hex before generic numbers; quoted literals before numbers
    # so digits inside quotes don't leak through as `N`.
    text = _HEX_RE.sub("0xN", text)
    text = _SQUOTE_RE.sub("'?'", text)
    text = _DQUOTE_RE.sub('"?"', text)
    text = _BACKTICK_RE.sub("`?`", text)
    text = _NUMBER_RE.sub("N", text)
    text = _WS_RE.sub(" ", text).strip()
    return text


@dataclass
class Family:
    """A root-cause family: a normalized key plus the raw rows it absorbed."""

    key: str
    count: int = 0
    distinct_messages: int = 0
    sample_messages: list[str] = field(default_factory=list)


def cluster_errors(error_rows: list[dict]) -> list[Family]:
    """Aggregate per-error-message rows into normalized root-cause families.

    ``error_rows`` is a list of ``{"error_message": str|None, "n": int}`` dicts
    (the output of the per-error-message GROUP BY query). Returns families
    sorted by descending total count (leverage), ties broken by key for
    determinism.
    """
    families: dict[str, Family] = {}
    for row in error_rows:
        raw = row.get("error_message")
        count = int(row.get("n", 0) or 0)
        key = normalize_error_message(raw)
        fam = families.get(key)
        if fam is None:
            fam = Family(key=key)
            families[key] = fam
        fam.count += count
        fam.distinct_messages += 1
        if raw and len(fam.sample_messages) < 3 and raw not in fam.sample_messages:
            fam.sample_messages.append(raw)
    return sorted(families.values(), key=lambda f: (-f.count, f.key))


# --------------------------------------------------------------------------- #
# VibeSQL query plumbing
# --------------------------------------------------------------------------- #


class TriageError(RuntimeError):
    """Raised for unrecoverable triage failures (missing DB, unreadable, etc.)."""


def default_vibesql_bin() -> str:
    """Locate the release vibesql binary relative to the repo root."""
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(repo_root, "target", "release", "vibesql")


def results_db_reachable(db_path: str) -> bool:
    """Return True if ``db_path`` is an openable VibeSQL results database.

    A plain ``os.path.exists`` check is wrong for the common case: in WAL mode
    (the CLI default) there is *no* ``<name>.vbsql`` snapshot file at all — the
    database is recovered from the extension-stripped siblings
    ``<name>-checkpoints/`` (``*.vchk``) plus ``<name>.wal``. Both the certified
    bench-runner DB and every local results DB are stored this way, so the file
    check alone rejects databases the ``vibesql`` binary opens fine.

    Consider the DB reachable if any of these hold:
      - the ``db_path`` file itself exists (snapshot / legacy SQL-dump mode), or
      - a ``<stem>-checkpoints/`` directory exists with at least one ``*.vchk``
        (WAL mode, where ``<stem>`` is ``db_path`` minus a trailing ``.vbsql``), or
      - a ``<stem>.wal`` write-ahead log exists.

    The genuinely-absent case (no file, no checkpoints, no WAL) returns False so
    the caller still fails loudly and never fabricates data.
    """
    if os.path.exists(db_path):
        return True
    stem = db_path[: -len(".vbsql")] if db_path.endswith(".vbsql") else db_path
    checkpoints_dir = stem + "-checkpoints"
    if os.path.isdir(checkpoints_dir) and any(
        name.endswith(".vchk") for name in os.listdir(checkpoints_dir)
    ):
        return True
    return os.path.exists(stem + ".wal")


def _extract_json_array(raw: str) -> str | None:
    """Extract the JSON array from vibesql output.

    vibesql's ``--format json`` prints a JSON array, sometimes followed by an
    "N rows" trailer on stdout/stderr. The array's closing bracket is the last
    ``]`` in the output (any ``]`` inside string values precedes it), so we take
    from the first ``[`` to the last ``]`` inclusive.
    """
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end < start:
        return None
    return raw[start : end + 1]


class Db:
    """Thin wrapper around ``vibesql <db> --format json -c <sql>``."""

    def __init__(self, db_path: str, vibesql_bin: str):
        self.db_path = db_path
        self.vibesql_bin = vibesql_bin

    def try_query(self, sql: str) -> list[dict] | None:
        """Run a query; return rows, or None if the query errored.

        Used both for real queries and for capability probes (e.g. does the
        ``machine_tag`` column exist). None means "could not run / no JSON" —
        callers decide whether that is fatal.
        """
        proc = subprocess.run(
            [self.vibesql_bin, self.db_path, "--format", "json", "-c", sql],
            capture_output=True,
            text=True,
        )
        combined = proc.stdout + "\n" + proc.stderr
        payload = _extract_json_array(combined)
        if payload is None:
            return None
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, list):
            return None
        return data

    def query(self, sql: str) -> list[dict]:
        rows = self.try_query(sql)
        if rows is None:
            raise TriageError(
                f"Query failed against {self.db_path!r}:\n  {sql.strip()}"
            )
        return rows

    def column_exists(self, table: str, column: str) -> bool:
        return self.try_query(f"SELECT {column} FROM {table} LIMIT 1") is not None


# --------------------------------------------------------------------------- #
# Report assembly
# --------------------------------------------------------------------------- #


@dataclass
class RunContext:
    run_id: int
    machine_tag: str | None
    started_at: str | None
    certified: bool
    summary_failed: int | None  # from tcl_test_runs, if available
    summary_total: int | None
    summary_passed: int | None
    summary_skipped: int | None


def _as_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def gather(db: Db, run_id: int | None, top_files: int, top_errors: int) -> dict:
    """Run all triage queries and return the raw material for the report."""
    # Determine the run_id: default to the latest run that has DETAIL rows,
    # matching the canonical queries in CLAUDE.md (the summary table can outpace
    # the detail table — exactly the divergence we want to surface).
    if run_id is None:
        rows = db.query("SELECT MAX(run_id) AS m FROM tcl_test_results")
        if not rows or rows[0].get("m") in (None, ""):
            raise TriageError(
                "No rows in tcl_test_results — the DB has no per-test detail "
                "data to triage. Run `make test-tcl` (local) or point --db at a "
                "populated certified results DB."
            )
        run_id = _as_int(rows[0]["m"])

    # Also surface the summary-table max run_id so a divergence is visible.
    summary_max_rows = db.try_query("SELECT MAX(run_id) AS m FROM tcl_test_runs")
    summary_max_run = (
        _as_int(summary_max_rows[0].get("m"))
        if summary_max_rows and summary_max_rows[0].get("m") not in (None, "")
        else None
    )

    # machine_tag may not exist on older-schema DBs (e.g. the June backup).
    has_machine_tag = db.column_exists("tcl_test_runs", "machine_tag")
    machine_tag = None
    if has_machine_tag:
        tag_rows = db.try_query(
            f"SELECT machine_tag FROM tcl_test_runs WHERE run_id = {run_id} LIMIT 1"
        )
        if tag_rows and tag_rows[0].get("machine_tag") not in (None, ""):
            machine_tag = str(tag_rows[0]["machine_tag"])

    # Summary-table row for this run (may be absent if detail-only run_id).
    summary_rows = db.try_query(
        "SELECT total_tests, passed, failed, skipped "
        f"FROM tcl_test_runs WHERE run_id = {run_id} LIMIT 1"
    )
    summary = summary_rows[0] if summary_rows else {}

    certified = bool(
        machine_tag and CERTIFIED_TAG_PATTERN.search(machine_tag)
    )

    ctx = RunContext(
        run_id=run_id,
        machine_tag=machine_tag,
        started_at=(summary.get("started_at") if summary else None),
        certified=certified,
        summary_failed=_as_int(summary.get("failed")) if summary else None,
        summary_total=_as_int(summary.get("total_tests")) if summary else None,
        summary_passed=_as_int(summary.get("passed")) if summary else None,
        summary_skipped=_as_int(summary.get("skipped")) if summary else None,
    )

    # started_at lives on the summary row; fetch separately (not selected above).
    started_rows = db.try_query(
        f"SELECT started_at FROM tcl_test_runs WHERE run_id = {run_id} LIMIT 1"
    )
    if started_rows and started_rows[0].get("started_at"):
        ctx.started_at = str(started_rows[0]["started_at"])

    # Detail-table status tally (failed + markers), so we can reconcile against
    # the summary total and count markers explicitly.
    status_rows = db.query(
        "SELECT status, COUNT(*) AS n FROM tcl_test_results "
        f"WHERE run_id = {run_id} GROUP BY status"
    )
    status_counts = {r["status"]: _as_int(r.get("n")) for r in status_rows}
    detail_failed = status_counts.get("failed", 0)
    detail_markers = sum(status_counts.get(s, 0) for s in MARKER_STATUSES)

    # Per-file failure counts (failed + markers count as failures).
    per_file = db.query(
        "SELECT file_path, "
        "SUM(CASE WHEN status IN ('failed','timeout','incomplete','error') "
        "THEN 1 ELSE 0 END) AS failed "
        "FROM tcl_test_results "
        f"WHERE run_id = {run_id} "
        "GROUP BY file_path HAVING failed > 0 "
        f"ORDER BY failed DESC LIMIT {top_files}"
    )

    # Per-error-message counts (status='failed', matching the issue query).
    per_error = db.query(
        "SELECT error_message, COUNT(*) AS n FROM tcl_test_results "
        f"WHERE run_id = {run_id} AND status = 'failed' "
        "GROUP BY error_message ORDER BY n DESC "
        f"LIMIT {top_errors}"
    )

    # For clustering we want ALL failed error messages grouped, not just the
    # top-N, so families are complete. Re-query without the display LIMIT.
    all_errors = db.query(
        "SELECT error_message, COUNT(*) AS n FROM tcl_test_results "
        f"WHERE run_id = {run_id} AND status = 'failed' "
        "GROUP BY error_message"
    )

    return {
        "ctx": ctx,
        "summary_max_run": summary_max_run,
        "status_counts": status_counts,
        "detail_failed": detail_failed,
        "detail_markers": detail_markers,
        "per_file": per_file,
        "per_error": per_error,
        "all_errors": all_errors,
    }


PROCESS_DOC = """\
## Process: generating the CERTIFIED breakdown and filing child fix-issues

This report is only actionable for filing per-family child fix-issues when it is
generated from the **certified** results DB. The certified run
(tag `aws-c7i.8xlarge-32c-clean2` or later) lives on the AWS bench-runner and is
archived off-box; it is NOT present in a local checkout. To produce the
certified breakdown:

1. **Obtain the certified DB.** Either run this tool directly on the bench-runner
   against `~/.vibesql/test_results/tcl_test_results.vbsql`, or copy that DB
   (and its `-checkpoints/` sibling) down to a workstation first.
2. **Confirm the run is certified.** Run
   `./target/release/vibesql <db> -c "SELECT run_id, machine_tag, started_at
   FROM tcl_test_runs ORDER BY run_id DESC LIMIT 5"` and verify the latest run's
   `machine_tag` is `aws-c7i.8xlarge-32c-clean2` or a later certified tag. This
   tool stamps CERTIFIED only when `machine_tag` matches that pattern.
3. **Regenerate this report:**
   `python3 scripts/tcl_triage.py --output docs/reference/tcl-triage-report.md`
   (the default `--db` is the canonical path). Confirm the banner reads
   CERTIFIED and that the per-file failure sum reconciles to the summary
   `failed` total (~7,106 for the certified `clean2` run).
4. **Cluster into families** using the "Root-cause families" table below. Each
   family with a substantial count (rule of thumb: **>= 10 failures**, or any
   smaller cluster that is obviously one fixable engine root cause) becomes a
   candidate child issue.
5. **For each family, file a focused child issue** (`Part of #5779`) containing:
   - The normalized family key and its total failure count.
   - Representative failing tests (`file_path` + `test_name`) — get them with
     `SELECT file_path, test_name, expected_output, actual_output
      FROM tcl_test_results WHERE run_id = <RUN> AND status='failed'
      AND error_message LIKE '<pattern>' LIMIT 20`.
   - Expected-vs-actual for one or two representatives.
   - The suspected engine location (grep the relevant `crates/vibesql-*` module).
   - An acceptance criterion tied to those specific tests going green.
6. **Do NOT reclassify a real wrong-answer as out-of-scope** to shrink the
   number. A failure that genuinely tests SQLite-internal-only behavior (no
   SQL-reachable equivalent) belongs in the skip-policy audit (#6154, Bucket A)
   with a justifying entry — never silently dropped here.

Until a CERTIFIED run of this report exists, **do not file the per-family child
fix-issues** — their counts would be fabricated or local-only. This deferral is
intentional and honest, not incomplete work.
"""


def render_report(
    data: dict, db_path: str, vibesql_bin: str, source_label: str | None = None
) -> str:
    ctx: RunContext = data["ctx"]
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if ctx.certified:
        banner = (
            "> # ✅ CERTIFIED DATA\n"
            f"> This report was generated from a run tagged "
            f"`{ctx.machine_tag}`, which matches the certified bench-runner\n"
            "> pattern. **Still confirm** the tag is `aws-c7i.8xlarge-32c-clean2`"
            " or later per issue #6155 before filing child issues.\n"
        )
    else:
        banner = (
            "> # ⚠️ NON-CERTIFIED / LOCAL-RUN DATA — DO NOT FILE CHILD ISSUES FROM THIS\n"
            ">\n"
            "> The numbers below come from a **non-certified** results DB "
            "(local run, stale backup, or a DB with no\n"
            "> `machine_tag`). They are **NOT** the certified ~7,106-failure "
            "breakdown from tag `aws-c7i.8xlarge-32c-clean2`.\n"
            "> **This report MUST be regenerated against the certified AWS "
            "bench-runner DB before any per-family child\n"
            "> fix-issues are filed off it.** See the process section at the "
            "bottom. Presenting these counts as certified would\n"
            "> be exactly the dishonesty issue #6155 guards against.\n"
        )

    lines: list[str] = []
    lines.append("# TCL Conformance Failure Triage Report")
    lines.append("")
    lines.append(banner)
    lines.append("")

    # Provenance
    lines.append("## Provenance")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Generated at (UTC) | `{now}` |")
    lines.append(f"| Results DB | `{db_path}` |")
    if source_label:
        lines.append(f"| Source label | {source_label} |")
    lines.append(f"| vibesql binary | `{vibesql_bin}` |")
    lines.append(f"| Run ID (detail table) | `{ctx.run_id}` |")
    lines.append(
        f"| Machine tag | `{ctx.machine_tag}` |"
        if ctx.machine_tag
        else "| Machine tag | _absent (older schema or local run)_ |"
    )
    lines.append(
        f"| Run started at | `{ctx.started_at}` |"
        if ctx.started_at
        else "| Run started at | _unknown_ |"
    )
    lines.append(
        f"| Certification | **{'CERTIFIED (verify clean2+)' if ctx.certified else 'NON-CERTIFIED'}** |"
    )
    lines.append("")

    # Reconciliation
    lines.append("## Reconciliation")
    lines.append("")
    detail_failed = data["detail_failed"]
    detail_markers = data["detail_markers"]
    summary_failed = ctx.summary_failed
    lines.append("| Metric | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Detail-table `failed` rows (this run) | {detail_failed} |")
    lines.append(
        f"| Detail-table marker rows (timeout/incomplete/error) | {detail_markers} |"
    )
    lines.append(
        f"| Detail-table failures incl. markers | {detail_failed + detail_markers} |"
    )
    if summary_failed is not None:
        lines.append(f"| Summary-table (`tcl_test_runs`) `failed` | {summary_failed} |")
        reconciles = summary_failed == detail_failed
        lines.append(
            f"| Detail vs summary `failed` reconcile? | "
            f"{'✅ yes' if reconciles else '⚠️ NO — investigate before triaging'} |"
        )
    else:
        lines.append(
            "| Summary-table (`tcl_test_runs`) `failed` | _no summary row for "
            "this run_id_ |"
        )
    summary_max = data["summary_max_run"]
    if summary_max is not None and summary_max != ctx.run_id:
        lines.append(
            f"| ⚠️ Summary max run_id ({summary_max}) != detail max run_id "
            f"({ctx.run_id}) | summary table outran detail rows |"
        )
    lines.append("")
    if summary_failed is None or summary_failed != detail_failed:
        lines.append(
            "> **Reconciliation note:** the per-file failure counts below sum to "
            "the detail-table failure total, which does **not** exactly match the "
            "summary-table `failed` here. Reconcile the run before trusting the "
            "breakdown for child-issue filing (see issue #6155)."
        )
        lines.append("")

    # Status breakdown
    lines.append("## Status breakdown (this run)")
    lines.append("")
    lines.append("| Status | Count |")
    lines.append("| --- | --- |")
    for status, count in sorted(
        data["status_counts"].items(), key=lambda kv: (-kv[1], kv[0])
    ):
        lines.append(f"| `{status}` | {count} |")
    lines.append("")

    # Per-file
    per_file = data["per_file"]
    lines.append(f"## Top {len(per_file)} files by failure count")
    lines.append("")
    lines.append("| File | Failures (incl. markers) |")
    lines.append("| --- | --- |")
    for row in per_file:
        lines.append(f"| `{row['file_path']}` | {_as_int(row.get('failed'))} |")
    lines.append("")

    # Per-error (raw)
    per_error = data["per_error"]
    lines.append(f"## Top {len(per_error)} raw error messages (`status='failed'`)")
    lines.append("")
    lines.append("| Count | Error message |")
    lines.append("| --- | --- |")
    for row in per_error:
        msg = row.get("error_message")
        display = "_(NULL / empty)_" if msg in (None, "") else f"`{_md_cell(msg)}`"
        lines.append(f"| {_as_int(row.get('n'))} | {display} |")
    lines.append("")

    # Families (clustered)
    families = cluster_errors(data["all_errors"])
    lines.append("## Root-cause families (normalized, ranked by leverage)")
    lines.append("")
    lines.append(
        "Error messages normalized (quoted literals -> `'?'`, numbers -> `N`, "
        "hex -> `0xN`) so structurally-identical failures collapse into one "
        "family. Rank = total failures; high-count families are the "
        "highest-leverage fixes."
    )
    lines.append("")
    lines.append("| Rank | Failures | Distinct raw msgs | Family (normalized) |")
    lines.append("| --- | --- | --- | --- |")
    for i, fam in enumerate(families, start=1):
        key_display = (
            "_(no error message)_"
            if fam.key == NO_MESSAGE_FAMILY
            else f"`{_md_cell(fam.key)}`"
        )
        lines.append(
            f"| {i} | {fam.count} | {fam.distinct_messages} | {key_display} |"
        )
    lines.append("")
    lines.append(f"_{len(families)} distinct root-cause families total._")
    lines.append("")

    # Process doc
    lines.append(PROCESS_DOC)
    lines.append("")

    return "\n".join(lines)


def _md_cell(text: str) -> str:
    """Escape a value for a Markdown table cell (pipes, backticks, newlines)."""
    return (
        text.replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("`", "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Triage TCL conformance failures into root-cause families."
    )
    parser.add_argument(
        "--db",
        default=CANONICAL_DB,
        help=f"Path to the results DB (default: {CANONICAL_DB}).",
    )
    parser.add_argument(
        "--vibesql",
        default=default_vibesql_bin(),
        help="Path to the vibesql binary (default: target/release/vibesql).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write the Markdown report here (default: stdout).",
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Triage a specific run_id (default: latest run with detail rows).",
    )
    parser.add_argument("--top-files", type=int, default=40)
    parser.add_argument("--top-errors", type=int, default=30)
    parser.add_argument(
        "--source-label",
        default=None,
        help="Human description of the data source, shown in the provenance "
        "table (e.g. 'stale June 2026 local backup' or 'certified tag "
        "aws-c7i.8xlarge-32c-clean2'). Does NOT affect the CERTIFIED/"
        "NON-CERTIFIED stamp, which is derived from machine_tag only.",
    )
    args = parser.parse_args(argv)

    if not results_db_reachable(args.db):
        sys.stderr.write(
            "ERROR: results DB not found:\n"
            f"  {args.db}\n\n"
            "This tool does NOT fabricate data. To triage TCL failures you need a\n"
            "populated results DB (a `.vbsql` snapshot, or its WAL-mode siblings:\n"
            "a `-checkpoints/` directory with a `*.vchk`, or a `.wal` file).\n"
            "Options:\n"
            "  - Local, NON-CERTIFIED: run `make test-tcl` (or `make test-tcl-all`)\n"
            "    then re-run this tool.\n"
            "  - CERTIFIED: obtain the AWS bench-runner DB (tag\n"
            "    aws-c7i.8xlarge-32c-clean2 or later) and pass it via --db.\n"
        )
        return 2

    if not os.path.exists(args.vibesql):
        sys.stderr.write(
            f"ERROR: vibesql binary not found: {args.vibesql}\n"
            "Build it with `cargo build --release`, or pass --vibesql PATH.\n"
        )
        return 2

    db = Db(args.db, args.vibesql)
    try:
        data = gather(db, args.run_id, args.top_files, args.top_errors)
    except TriageError as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 1

    report = render_report(data, args.db, args.vibesql, args.source_label)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(report)
        cert = "CERTIFIED" if data["ctx"].certified else "NON-CERTIFIED"
        sys.stderr.write(
            f"Wrote {cert} triage report -> {args.output} "
            f"(run_id={data['ctx'].run_id})\n"
        )
    else:
        sys.stdout.write(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
