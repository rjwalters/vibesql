# TCL Conformance Failure Triage Report

> # ⚠️ NON-CERTIFIED / LOCAL-RUN DATA — DO NOT FILE CHILD ISSUES FROM THIS
>
> The numbers below come from a **non-certified** results DB (local run, stale backup, or a DB with no
> `machine_tag`). They are **NOT** the certified ~7,106-failure breakdown from tag `aws-c7i.8xlarge-32c-clean2`.
> **This report MUST be regenerated against the certified AWS bench-runner DB before any per-family child
> fix-issues are filed off it.** See the process section at the bottom. Presenting these counts as certified would
> be exactly the dishonesty issue #6155 guards against.


## Provenance

| Field | Value |
| --- | --- |
| Generated at (UTC) | `2026-07-16T15:50:05Z` |
| Results DB | `/Users/rwalters/.vibesql/test_results/tcl_test_results.vbsql.bak-20260626-174440` |
| Source label | STALE local backup tcl_test_results.vbsql.bak-20260626-174440 (2026-06-26; NOT the certified AWS bench-runner DB — used only to demonstrate the tool) |
| vibesql binary | `/Users/rwalters/GitHub/vibesql/target/release/vibesql` |
| Run ID (detail table) | `22` |
| Machine tag | _absent (older schema or local run)_ |
| Run started at | `2026-06-18T20:01:41.467228` |
| Certification | **NON-CERTIFIED** |

## Reconciliation

| Metric | Value |
| --- | --- |
| Detail-table `failed` rows (this run) | 1968 |
| Detail-table marker rows (timeout/incomplete/error) | 6 |
| Detail-table failures incl. markers | 1974 |
| Summary-table (`tcl_test_runs`) `failed` | 39 |
| Detail vs summary `failed` reconcile? | ⚠️ NO — investigate before triaging |
| ⚠️ Summary max run_id (26) != detail max run_id (22) | summary table outran detail rows |

> **Reconciliation note:** the per-file failure counts below sum to the detail-table failure total, which does **not** exactly match the summary-table `failed` here. Reconcile the run before trusting the breakdown for child-issue filing (see issue #6155).

## Status breakdown (this run)

| Status | Count |
| --- | --- |
| `skipped` | 2149 |
| `failed` | 1968 |
| `passed` | 1205 |
| `error` | 6 |

## Top 40 files by failure count

| File | Failures (incl. markers) |
| --- | --- |
| `docs/reference/sqlite/test/joinD.test` | 716 |
| `docs/reference/sqlite/test/joinB.test` | 176 |
| `docs/reference/sqlite/test/func.test` | 79 |
| `docs/reference/sqlite/test/indexexpr1.test` | 74 |
| `docs/reference/sqlite/test/index6.test` | 70 |
| `docs/reference/sqlite/test/selectA.test` | 68 |
| `docs/reference/sqlite/test/joinC.test` | 61 |
| `docs/reference/sqlite/test/update.test` | 42 |
| `docs/reference/sqlite/test/in.test` | 39 |
| `docs/reference/sqlite/test/index.test` | 38 |
| `docs/reference/sqlite/test/join.test` | 38 |
| `docs/reference/sqlite/test/joinE.test` | 35 |
| `docs/reference/sqlite/test/select3.test` | 32 |
| `docs/reference/sqlite/test/intpkey.test` | 29 |
| `docs/reference/sqlite/test/join5.test` | 27 |
| `docs/reference/sqlite/test/where.test` | 23 |
| `docs/reference/sqlite/test/indexexpr2.test` | 22 |
| `docs/reference/sqlite/test/orderby5.test` | 22 |
| `docs/reference/sqlite/test/join8.test` | 21 |
| `docs/reference/sqlite/test/delete.test` | 19 |
| `docs/reference/sqlite/test/whereM.test` | 19 |
| `docs/reference/sqlite/test/insert.test` | 16 |
| `docs/reference/sqlite/test/whereH.test` | 16 |
| `docs/reference/sqlite/test/insert4.test` | 14 |
| `docs/reference/sqlite/test/insert2.test` | 13 |
| `docs/reference/sqlite/test/joinF.test` | 13 |
| `docs/reference/sqlite/test/wherelimit2.test` | 11 |
| `docs/reference/sqlite/test/func4.test` | 10 |
| `docs/reference/sqlite/test/select1.test` | 10 |
| `docs/reference/sqlite/test/whereG.test` | 10 |
| `docs/reference/sqlite/test/select2.test` | 9 |
| `docs/reference/sqlite/test/whereK.test` | 9 |
| `docs/reference/sqlite/test/index3.test` | 8 |
| `docs/reference/sqlite/test/indexedby.test` | 8 |
| `docs/reference/sqlite/test/join2.test` | 8 |
| `docs/reference/sqlite/test/where2.test` | 8 |
| `docs/reference/sqlite/test/whereJ.test` | 8 |
| `docs/reference/sqlite/test/where6.test` | 7 |
| `docs/reference/sqlite/test/whereF.test` | 7 |
| `docs/reference/sqlite/test/whereL.test` | 7 |

## Top 30 raw error messages (`status='failed'`)

| Count | Error message |
| --- | --- |
| 1596 | `Output mismatch` |
| 40 | `Query failed: Error executing statement 1: Table 't1' already exists Error: 1 statements failed` |
| 37 | `Query failed: Timeout after 5.0s` |
| 17 | `Query failed: Error executing statement 1: Table 't1' not found Error: 1 statements failed` |
| 15 | `Error message mismatch` |
| 10 | `Expected error but query succeeded` |
| 8 | `Query failed: Error executing statement 1: Incompatible types for LIKE: Float64 vs String pattern Error: 1 statements failed` |
| 8 | `Query failed: Error executing statement 1: Table 'x1' not found Error: 1 statements failed` |
| 8 | `Query failed: Error executing statement 1: no such function: test_auxdata Error: 1 statements failed` |
| 6 | `Query failed: Error executing statement 1: Storage error: Failed to commit transaction: Transaction error: No active transaction to commit Error: 1 statements failed` |
| 6 | `Query failed: Error executing statement 1: Table 't3' not found Error: 1 statements failed` |
| 6 | `Query failed: Error executing statement 1: Table 't4' not found Error: 1 statements failed` |
| 5 | `Query failed: Error executing statement 1: Parse error: near "(": syntax error Error: 1 statements failed` |
| 5 | `Query failed: Error executing statement 1: Table 'v0' not found Error: 1 statements failed` |
| 5 | `Query failed: Error executing statement 1: table t2 has 2 columns but 1 values were supplied Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: Column 'b' not found (searched tables: t1). Available columns: x Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: Column 'c0' not found (searched tables: t1). Available columns: a Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: Incompatible types for LIKE: Int64 vs String pattern Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: ON clause references tables to its right Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: Parse error: Expected data type Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: Table 't0' already exists Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 1: no such function: testfunc Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 4: Parse error: Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, ASSERTION, PROCEDURE, or FUNCTION after CREATE Error: 1 statements failed` |
| 4 | `Query failed: Error executing statement 7: UNIQUE constraint failed: t1.a Error: 1 statements failed` |
| 3 | `Query failed: Error executing statement 1: Column 'x' not found (searched tables: t2). Available columns: a, b Error: 1 statements failed` |
| 3 | `Query failed: Error executing statement 1: Parse error: near "MATCH": syntax error Error: 1 statements failed` |
| 3 | `Query failed: Error executing statement 1: Table 't0' not found Error: 1 statements failed` |
| 3 | `Query failed: Error executing statement 1: Table 't2' not found Error: 1 statements failed` |
| 3 | `Query failed: Error executing statement 1: Table 'x2' not found Error: 1 statements failed` |
| 3 | `Query failed: Error executing statement 1: no such function: test_destructor_count Error: 1 statements failed` |

## Root-cause families (normalized, ranked by leverage)

Error messages normalized (quoted literals -> `'?'`, numbers -> `N`, hex -> `0xN`) so structurally-identical failures collapse into one family. Rank = total failures; high-count families are the highest-leverage fixes.

| Rank | Failures | Distinct raw msgs | Family (normalized) |
| --- | --- | --- | --- |
| 1 | 1596 | 1 | `Output mismatch` |
| 2 | 90 | 39 | `Query failed: Error executing statement N: Table '?' not found Error: N statements failed` |
| 3 | 53 | 9 | `Query failed: Error executing statement N: Table '?' already exists Error: N statements failed` |
| 4 | 37 | 1 | `Query failed: Timeout after N.0s` |
| 5 | 22 | 11 | `Query failed: Error executing statement N: Parse error: near "?": syntax error Error: N statements failed` |
| 6 | 15 | 1 | `Error message mismatch` |
| 7 | 10 | 1 | `Expected error but query succeeded` |
| 8 | 8 | 1 | `Query failed: Error executing statement N: Incompatible types for LIKE: Float64 vs String pattern Error: N statements failed` |
| 9 | 8 | 1 | `Query failed: Error executing statement N: no such function: test_auxdata Error: N statements failed` |
| 10 | 6 | 2 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: x Error: N statements failed` |
| 11 | 6 | 3 | `Query failed: Error executing statement N: Parse error: Expected data type Error: N statements failed` |
| 12 | 6 | 1 | `Query failed: Error executing statement N: Storage error: Failed to commit transaction: Transaction error: No active transaction to commit Error: N statements failed` |
| 13 | 5 | 2 | `Query failed: Error executing statement N: Parse error: Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, ASSERTION, PROCEDURE, or FUNCTION after CREATE Error: N statements failed` |
| 14 | 5 | 1 | `Query failed: Error executing statement N: table t2 has N columns but N values were supplied Error: N statements failed` |
| 15 | 4 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: a Error: N statements failed` |
| 16 | 4 | 1 | `Query failed: Error executing statement N: Incompatible types for LIKE: Int64 vs String pattern Error: N statements failed` |
| 17 | 4 | 2 | `Query failed: Error executing statement N: Index '?' not found Error: N statements failed` |
| 18 | 4 | 1 | `Query failed: Error executing statement N: ON clause references tables to its right Error: N statements failed` |
| 19 | 4 | 4 | `Query failed: Error executing statement N: Table '?' not found Error executing statement N: Table '?' not found Error: N statements failed` |
| 20 | 4 | 1 | `Query failed: Error executing statement N: UNIQUE constraint failed: t1.a Error: N statements failed` |
| 21 | 4 | 1 | `Query failed: Error executing statement N: no such function: testfunc Error: N statements failed` |
| 22 | 4 | 2 | `Query failed: error: unexpected argument '?' found tip: to pass '?' as a value, use '?' Usage: vibesql <DATABASE> For more information, try '?'.` |
| 23 | 3 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t2). Available columns: a, b Error: N statements failed` |
| 24 | 3 | 1 | `Query failed: Error executing statement N: no such column: vkey Error executing statement N: Column '?' not found (searched tables: t1) Error: N statements failed` |
| 25 | 3 | 2 | `Query failed: Error executing statement N: no such function: test_destructor Error: N statements failed` |
| 26 | 3 | 1 | `Query failed: Error executing statement N: no such function: test_destructor_count Error: N statements failed` |
| 27 | 3 | 1 | `Query failed: Error executing statement N: no such function: test_frombind Error: N statements failed` |
| 28 | 2 | 2 | `Expected success but got error: Error executing statement N: Table '?' not found Error: N statements failed` |
| 29 | 2 | 1 | `Expected success but got error: Error executing statement N: UNIQUE constraint failed: t8.a Error: N statements failed` |
| 30 | 2 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: main.t0). Available columns: c0 Error: N statements failed` |
| 31 | 2 | 2 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1, t2). Available columns: a, b, c, d Error: N statements failed` |
| 32 | 2 | 1 | `Query failed: Error executing statement N: Incompatible types for string equality: String vs Double(N) Error: N statements failed` |
| 33 | 2 | 1 | `Query failed: Error executing statement N: Type error: JSON operator requires string, got Integer(N) Error: N statements failed` |
| 34 | 2 | 1 | `Query failed: Error executing statement N: Type mismatch: Numeric(N) LIKE Varchar("?") Error: N statements failed` |
| 35 | 2 | 1 | `Query failed: Error executing statement N: no such function: testdirectonly Error: N statements failed` |
| 36 | 2 | 1 | `Query failed: Error executing statement N: no such function: uppercaseconversionfunctionwithaverylongname Error: N statements failed` |
| 37 | 2 | 1 | `Query failed: Error executing statement N: table t0 has N columns but N values were supplied Error: N statements failed` |
| 38 | 2 | 2 | `Query failed: Error executing statement N: table t1 has N columns but N values were supplied Error: N statements failed` |
| 39 | 2 | 2 | `Query failed: Error: Failed to load database: Failed to execute statement N in "?": Constraint violation: UNIQUE constraint failed: duplicate key in expression index '?' Statement: CREATE UNIQUE INDEX t1x1 ON t1 (a GLOB b)` |
| 40 | 2 | 1 | `Query failed: error: unexpected argument '?' found tip: to pass '-- The use of the "?" alias in the WHERE clause is technically -- illegal, but SQLite allows it for historical reasons. In this -- test and the next, verify that "?" can be use` |
| 41 | 1 | 1 | `Expected success but got error: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: a Error: N statements failed` |
| 42 | 1 | 1 | `Expected success but got error: Error executing statement N: Column '?' not found (searched tables: t1, __selfjoin_right_t1_2). Available columns: x, y, x, y Error: N statements failed` |
| 43 | 1 | 1 | `Expected success but got error: Error executing statement N: Parse error: unknown join type: NATURAL LEFT FULL Error: N statements failed` |
| 44 | 1 | 1 | `Expected success but got error: Error executing statement N: Table '?' already exists Error: N statements failed` |
| 45 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: main.t1). Available columns: w, x, y Error: N statements failed` |
| 46 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: main.t4). Available columns: w, z Error: N statements failed` |
| 47 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t0, v0). Available columns: c0, c0 Error: N statements failed` |
| 48 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: a, b Error: N statements failed` |
| 49 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: a, b, c Error: N statements failed` |
| 50 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: c0, col_0, x Error: N statements failed` |
| 51 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1). Available columns: x, y Error: N statements failed` |
| 52 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t1, t2). Available columns: c, b, c, d Error: N statements failed` |
| 53 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t2). Available columns: a, b, c, d, e, f Error: N statements failed` |
| 54 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t2). Available columns: d, e, f, a, b, c Error: N statements failed` |
| 55 | 1 | 1 | `Query failed: Error executing statement N: Column '?' not found (searched tables: t2, t1, t3). Available columns: u, x, x, y, w, v Error: N statements failed` |
| 56 | 1 | 1 | `Query failed: Error executing statement N: Constraint violation: UNIQUE constraint failed: a (multiple rows would have same key) Error: N statements failed` |
| 57 | 1 | 1 | `Query failed: Error executing statement N: Parse error: near "?": syntax error Error executing statement N: Table '?' not found Error: N statements failed` |
| 58 | 1 | 1 | `Query failed: Error executing statement N: Parse error: unknown join type: LEFT RIGHT Error: N statements failed` |
| 59 | 1 | 1 | `Query failed: Error executing statement N: Table '?' already exists Error executing statement N: no such function: randstr Error: N statements failed` |
| 60 | 1 | 1 | `Query failed: Error executing statement N: UNIQUE constraint failed: t3.c, t3.b Error: N statements failed` |
| 61 | 1 | 1 | `Query failed: Error executing statement N: Unsupported expression: Column reference '?' not supported in INSERT VALUES. Did you mean to use a procedural variable? Error: N statements failed` |
| 62 | 1 | 1 | `Query failed: Error executing statement N: misuse of aggregate: sum() Error: N statements failed` |
| 63 | 1 | 1 | `Query failed: Error executing statement N: no such function: legacy_count Error: N statements failed` |
| 64 | 1 | 1 | `Query failed: Error executing statement N: no such function: randstr Error executing statement N: Column '?' not found (searched tables: t1). Available columns: a, b Error: N statements failed` |
| 65 | 1 | 1 | `Query failed: Error executing statement N: no such function: test_destructor16 Error: N statements failed` |
| 66 | 1 | 1 | `Query failed: Error executing statement N: table t3 has no column named d Error: N statements failed` |
| 67 | 1 | 1 | `Query failed: Error executing statement N: wrong number of arguments to function sqlite_version() Error: N statements failed` |
| 68 | 1 | 1 | `Query failed: Error: Failed to load database: Failed to execute statement N in "?": Constraint violation: UNIQUE constraint failed: duplicate key in expression index '?' Statement: CREATE UNIQUE INDEX i0 ON t0 (c1, N\|c0)` |

_68 distinct root-cause families total._

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

