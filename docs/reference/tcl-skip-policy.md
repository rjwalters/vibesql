# TCL Skip-Honesty Policy

**Status:** canonical. Part of epic #5779. Delivers the static (source-only)
half of issue #6154: the full Bucket-A/Bucket-B classification of every in-tree
skip declaration, enforced-completeness auditing (`--audit-buckets`), and the
local by-category excluded-skip report in `make test-tcl-status`. The certified
excluded-row denominator remains operator-gated (see "Deferred work").

An honest claim of "N% SQLite compatibility" is meaningless without a
**defensible skip policy**: a documented taxonomy in which every whole-file and
partial skip falls into exactly one bucket, with the "we skip because we fail"
bucket driven to **zero**. Until every skip carries a category-level rationale a
skeptical outside observer would accept, we cannot honestly say what "100%" is a
percentage *of*.

This document is the canonical taxonomy. It enumerates the Bucket-A categories
with rationale, records the Bucket-B worklist, and states the standing rule that
**no new skip may be added without declaring its bucket**.

---

## The two-bucket rule

Every skip must be exactly one of:

**Bucket A — Out-of-scope by design (HONEST to exclude).** The test exercises
functionality VibeSQL deliberately does not implement, and never will as an
in-scope SQL engine, OR it depends on a test-harness surface (C-API,
TCL-registered helper, internal counter) that is unreachable from the SQL CLI. A
skeptic reading the rationale agrees it is not a SQL-conformance gap.

**Bucket B — Skipped because we fail / can't yet (DISHONEST to exclude).** Any
skip that hides a real, in-scope SQL feature we do not yet support correctly.
These must be **un-skipped** and either fixed or left visibly failing (counted in
the denominator). This bucket must reach **zero** for an honest 100%.

The tell for a Bucket-B smell is rationale language like *"behavior differs"*,
*"handling differs"*, *"features differ"*, *"not implemented"*, or *"not fully
supported"* attached to an **in-scope SQL surface** (collation, type affinity,
subqueries, ROWID, UNIQUE, ALTER TABLE, name resolution, window functions).
Contrast with a defensible Bucket-A rationale, which names a specific
out-of-scope subsystem (a C-API entry point, a VFS/pager internal, an unshipped
extension, or a harness-only function).

---

## Scope of this document (read this first)

The issue title speaks of "all 174,982 skipped test **rows**". That row count is
a property of the **certified full run** (tag `aws-c7i.8xlarge-32c-final`,
run_id under #6144) whose results DB lives only on the AWS bench runner — it is
**not** available in this working tree. The certified run tells you the *volume*
each skip declaration suppresses (how many numbered test-row instances a given
glob matched); it does **not** change the *category* of any declaration.

This document therefore classifies the **static skip declarations** that are
fully in-tree in `scripts/tester_vibesql.tcl`, which is everything the category
decision actually depends on:

| Array | Count | What it declares |
|-------|-------|------------------|
| `vibesql_skip_files` | 60 | whole-file skips (basename → reason) |
| `vibesql_partial_skip_files` | 1 | documented partial-skip record (`atof1`) |
| `vibesql_skip_tests` | 1,528 | individually-named test skips |
| `vibesql_skip_patterns` | 56 | glob-pattern skip rules |

**Every whole-file and pattern declaration now has an enforced bucket.** All 60
whole-file skips and all 56 pattern skips are classified below (73 Bucket A + 43
Bucket B = 116), and `scripts/verify_skips.py --audit-buckets` fails if any
whole-file/pattern entry is left without a bucket. The machine-readable source of
truth for the Bucket-A category of each entry is `BUCKET_A_CLASSIFICATION` in
`scripts/verify_skips.py`; this prose taxonomy and that map must stay in sync
(the audit is what keeps them honest). The 1,528 named `vibesql_skip_tests`
entries are not individually category-mapped in this static pass (see the
named-test note below and "Deferred work").

**Deferred to operator data (deliverable 5 of #6154):** the reconciled
excluded-row denominator "by category" and any `ifcapable`-guarded self-skip that
produced `skipped` rows only at runtime require the certified bench-runner DB.
Those are explicitly out of scope for the static classification and are tracked
as a follow-up. See "Deferred work" at the end. The fuzz-corpus smoke-skips named
in the issue body (`fuzz-oss1` / `fuzzer1` / `dbfuzz001`) are **not** present as
static declarations in `scripts/tester_vibesql.tcl`; if they suppressed rows in
the certified run they did so via runtime `ifcapable` guards, which fall under
the deferred half.

---

## Bucket A categories (defensible out-of-scope)

Every whole-file skip (all 60) and 13 of the 56 pattern skips are Bucket A. They
group into the following categories. The rationale is stated **per category** so
the audit is a short list of principles, not 116 ad-hoc notes.

### A1. C-API / statement-handle surface

Tests assert `sqlite3_*` C-API behavior (prepare / step / column metadata /
bind / value-pointer / autocommit / error codes) that is unreachable from the SQL
CLI. `execsql` blocks in these files are setup only; there is no
`do_execsql_test` SQL-reachable coverage.

- Whole-file: `capi2`, `capi3`, `capi3b`, `capi3c`, `capi3d`, `capi3e`,
  `tkt2409`, `colmeta`, `tableapi`, `bind`, `ptrchng`, `delete_db`,
  `intarray`, `snapshot`, `shared6`, `symlink`, `quota-glob`, `varint`.
- Rationale: no SQL surface. VibeSQL is driven through SQL text; the C library
  API is not a conformance target.

### A2. VFS / pager internals

Tests exercise the rollback-journal pager, page layout, byte-exact file format,
file locking, or deliberately corrupted page images — none of which exist in
VibeSQL, which uses its own WAL and has no B-tree page layer.

- Whole-file: `jrnlmode`, `jrnlmode3`, `pagesize`, `filefmt`, `corruptL`,
  `lock`, `lock5`, `sort2` (multi-threaded-sorter PMA config via
  `sqlite3_config`), `strict2` (writable_schema rootpage aliasing).
- Pattern: `e_wal-` ("WAL mode not implemented" — VibeSQL ships its own WAL;
  SQLite's WAL-pragma pager semantics are not applicable).

### A3. Unshipped extensions / virtual tables

Tests require an extension or virtual-table module VibeSQL does not ship (FTS3/4,
R-tree, RBU, session, `wholenumber` vtab, `ieee754`, trusted-schema).

- Whole-file: `fts3`, `rtree`, `rbu`, `session`, `index6`, `index7` (wholenumber
  vtab data population), `orderby7` (FTS3 join), `ieee754`, `trustschema1`.
- Note: the permutation dispatchers below (A6) also recurse into these.

### A4. Incremental blob I/O

Tests use `incrblob` — SQLite's incremental blob streaming C API, no SQL surface.

- Whole-file: `incrblob`, `incrblob2`, `incrblob3`, `incrblob4`,
  `incrblob_err`, `incrblobfault`.

### A5. Harness-only functions & TCL helpers

Tests depend on a function or variable that exists only as a TCL test helper
(`db func` / `db function` / `db collate` registration) or a SQLite test
extension, with no SQL-CLI-reachable equivalent.

- Whole-file: `intreal`, `func4` (`tointeger`/`toreal` from the `totype` test
  extension), `trigger6` (`counter()` TCL helper), `update2` (`repeat()`
  registered via `db func repeat [list string repeat]` — harness-provided, not a
  SQLite built-in).
- Pattern: `select9-2.*.3`, `select9-2.*.6` (custom `reverse` collation via
  `db collate`, not bridgeable to the CLI subprocess), `temptable-`,
  `temptable2-` (require cross-test session state the process-per-batch shim
  cannot hold), `date-6.` (localtime DST needs the `SQLITE_TESTCTRL_LOCALTIME_FAULT`
  harness override), `date4-` (compares against libc `strftime`; shim stubs it via
  `clock format`).

### A6. Permutation / suite dispatchers

`all` / `full` / `quick` / `veryquick` / `extraquick` / `rbu` / `session` are
`permutations.test` dispatchers that re-source the nonexistent `$testdir/tester.tcl`
(the shim ships `tester_vibesql.tcl`) and would double-run content files the
per-file runner already executes individually. A file-level skip yields one clean
`skipped` row instead of an `incomplete` marker.

- Whole-file: `all`, `full`, `quick`, `veryquick`, `extraquick`, `rbu`,
  `session`.

### A7. Internal optimizer / VDBE counters

Tests assert internal opcode counters, transfer-optimization counts, or STAT4
histogram-driven plan choices — query *results* are correct, but the counters
have no VibeSQL equivalent.

- Whole-file: `insert4`, `insert5` (xfer-opt counters), `whereJ` (STAT4),
  `where8` (`sqlite_search_count` step counts), `malloc4` (OOM fault injection),
  `manydb` (~116 concurrent connections; shim is process-per-batch).
- Pattern: `indexexpr3-` (asserts `EXPLAIN` COVERING INDEX output), `fordelete-`
  (btree `FORDELETE` VDBE flag), `fkey_malloc-`, `windowfault-` (fault
  injection).

### A8. Built-in-test fuzzers

Gated by `ifcapable builtin_test`; drive a C-level fuzzer for an internal
structure with no SQL surface.

- Whole-file: `bitvec` (`sqlite3BitvecBuiltinTest`; ~750k assertions,
  internals-only — #6140/#6143).

### A9. Documented intentional engine divergence

VibeSQL's Rust string pipeline enforces valid UTF-8 by construction, so
byte-preserving invalid-UTF-8 behavior is a *deliberate* design divergence, not a
conformance gap it intends to close. Reasonable skeptic accepts "we are UTF-8
strict by construction."

- Whole-file: `badutf`, `badutf2`.
- Pattern: `utf16align-` (UTF-16 alignment/encoding internals; VibeSQL is UTF-8).
- Borderline flag: this is the one Bucket-A category whose members are *failing*
  rather than *unreachable*. It stays Bucket A only because the divergence is
  documented and intentional. Any new member must justify why the divergence is a
  design choice and not an unfixed bug.

### A10. Error-message-format divergence

Result set / behavior is correct; only the exact error **text** differs from
SQLite's. Not a conformance gap for a differently-worded but semantically
equivalent error.

- Pattern: `subselect-1.2` ("row value misused" vs "sub-select returns N
  columns").
- (Many named `vibesql_skip_tests` entries also fall here — see below.)

---

## Bucket B worklist (dishonest skips — must reach zero)

These skip declarations hide **in-scope SQL** behind blanket "differs" / "not
implemented" rationales. Per the two-bucket rule they are Bucket B: each must be
**un-skipped** and either fixed or left visibly `failed`. This PR **classifies**
them; the actual un-skip + fix-or-fail-visibly is **Phase 2** (see "Deferred
work") because verifying that an un-skip fails *cleanly* (without regressing
passing tests in the same file, and without a monster-file row explosion
dominating the raw pass rate) requires a full-suite run on the quiet bench runner.

### B — curator-flagged pattern entries (all confirmed Bucket B)

Each was scrutinized for a defensible Bucket-A rationale; none survives, because
each names an **in-scope SQL surface**:

| Pattern(s) | Rationale as written | Why it is Bucket B |
|-----------|----------------------|--------------------|
| `collate1-` … `collateA-` (8) | "Collation behavior differs" | Built-in `BINARY`/`NOCASE`/`RTRIM` collation is core SQL. Custom-collation *sub*-cases are already separately Bucket A (the `db collate` detector), so the blanket file glob over-skips real coverage. |
| `types-`, `types2-` | "Type handling differs" | Type affinity is core SQL. |
| `subquery-` | "Subquery handling differs" | Subqueries are core SQL. |
| `without_rowid1/2/5/6-` | "WITHOUT ROWID tables not fully supported" | WITHOUT ROWID is a real SQLite feature; "not fully" + the fact that `without_rowid3/4` are *not* skipped is direct evidence of a partial engine gap. |
| `autoindex3/4/5-` | "Automatic indexing not implemented" | Automatic (transient) indexing. Result-only cases must still pass; plan-assertion cases are Bucket A **only if** verified to assert `EXPLAIN`/`sqlite_search_count`. Blanket skip is not defensible; `autoindex1/2` are not skipped. |
| `unique2-` | "UNIQUE constraint handling differs" | UNIQUE constraints are core SQL. |
| `rowid-` | "ROWID behavior differs" | ROWID semantics are core SQLite conformance. |
| `resolver01-` | "Name resolution handling differs" | Name/scope resolution is core SQL. |
| `misc3-`, `misc4-` | "Miscellaneous tests - various features differ" | Vague-by-construction; indefensible as out-of-scope. |
| `aggnested-` | "Nested aggregate functions not fully supported" | Nested/correlated aggregates are core SQL. |

### B — additional pattern entries flagged by this audit (not in the curator list)

Honesty requires reporting the Bucket-B smells the curator did not pre-list:

| Pattern(s) | Rationale as written | Disposition |
|-----------|----------------------|-------------|
| `printf2-` | "format() function behavior differs" | `format()`/`printf()` is a SQL function → Bucket B. |
| `e_totalchanges-` | "total_changes() not implemented" | `total_changes()` is SQL-reachable → Bucket B (engine gap). |
| `altertab2-`, `altertab3-`, `alterlegacy-` | "ALTER TABLE features differ / legacy" | ALTER TABLE is core SQL → Bucket B. |
| `window1-66.`, `window2-66.`, `window1-69.` | "json_group_array/object / total() as window function not implemented" | Window-function coverage of real aggregates → Bucket B (engine gap). |
| `orderby8-1.` | "ORDER BY with many columns - stress test" | ORDER BY is core SQL; "stress test" is not an out-of-scope rationale → Bucket B (verify). |
| `orderbyA-` | "ORDER BY optimization differs" | Bucket B **unless** verified to assert only the query plan (then A7). |
| `boundary1-` … `boundary4-` | "Boundary condition tests - stress test" | Integer-boundary affinity/comparison is core SQL → Bucket B (verify). |
| `like2-` | "LIKE operator test - setup cascade failures" | Cascade from an upstream failure → fix the upstream cause, do not blanket-skip. |
| `tableopts-` | "Table options differ" | Bucket B (verify which options). |
| `randexpr-` | "Random expression stress test" | Machine-generated expression fuzzer. Needs a **documented decision** (same shape as the fuzz-corpus question): either run for a certified number, or justify as a no-incremental-signal corpus. Until decided, treat as Bucket B. |
| `sidedelete-` | "'sequence' conflicts with VibeSQL parser keyword" | VibeSQL reserves a keyword SQLite does not. Parser divergence → Bucket B (de-reserve, or document as an intentional divergence with a real rationale — "differs" alone is not one). |

### B — named-test array (`vibesql_skip_tests`, 1,528 entries)

The named-test array is dominated by Bucket-A categories (harness/TCL-helper,
extension/vtab, internal/EXPLAIN/VDBE, error-message-format, and
test-infra-cascade). A residual subset carries the same "differs" / "not
supported" / "not implemented" Bucket-B smell attached to in-scope SQL. Per-entry
adjudication of 1,528 rows is Phase-2 per-file work (the same fix-or-unskip
verification loop as the patterns) and is **not** attempted in this static pass;
the standing-rule check below flags the smell so the residual cannot grow
silently.

---

## Landmines (do not regress)

- **`atof1` partial skip (#6065).** The ~39,998 dynamically-named
  `atof1-1.$i.1/.2` loop tests call `real2hex()`/`hex2real()` — harness-only
  functions (Bucket A, category A5). But the ~7 non-loop `atof1-2.x`/`atof-3.x`
  tests are legitimate `do_execsql_test` coverage and **must keep running**.
  `atof1-2.10/2.20/2.30` (UTF-16be `substr`) and `atof-3.1` (large-literal REAL
  precision) are REAL open engine bugs tracked in #6065 — they must keep
  reporting `failed`, **never** be reclassified as skipped. This is why `atof1`
  is a `vibesql_partial_skip_files` record and **not** a whole-file skip.
- **`fuzz.test` real failures (#6070–#6073).** The residual `fuzz.test` failures
  are tracked by OPEN ISSUES, not by skip entries — they keep running and
  reporting `failed`. No `vibesql_skip_tests` entry may be added for them.
- **`bitvec` (#6140/#6143)** and the **permutation dispatchers** (`all`/`quick`/
  `full`/…) are confirmed Bucket A (categories A8 and A6).

---

## Standing rule: no undeclared skip

**No new whole-file or pattern skip may be added to
`scripts/tester_vibesql.tcl` without declaring its bucket.**

- A Bucket-A skip must name one of the categories above (A1–A10) in its rationale
  string, or add a new category to this document with a principled rationale.
- A Bucket-B skip is not permitted as a *permanent* entry. If a temporary skip is
  unavoidable, it must be recorded here in the Bucket-B worklist with a tracking
  issue and the intent to un-skip.

This is enforced (not merely documented) by
`scripts/verify_skips.py --audit-buckets`, which runs three gates over every
whole-file and pattern skip:

1. **Completeness** — every skip must be declared in exactly one bucket: present
   in `BUCKET_A_CLASSIFICATION` (with an A1–A10 category) or in `ACK_BUCKET_B`
   (the worklist above). A skip in **neither** map is an *undeclared* skip and
   fails the check. This is the structural enforcement of the standing rule: a
   newly-added shim skip that no one classified — even one with a perfectly bland,
   smell-free rationale — cannot pass silently.
2. **Consistency** — no skip may appear in both maps, and every Bucket-A category
   code must be a real A1–A10 category.
3. **Anti-hiding** — a Bucket-B smell phrase ("behavior differs", "not
   implemented", …) attached to a skip classified Bucket A is rejected unless the
   key is on the explicit `ACK_BUCKET_A_OVERRIDE` list (a certified
   false-positive, e.g. `e_wal-`). This stops an in-scope-SQL gap from being
   buried in Bucket A to silence it.

A future contributor adding a skip must therefore either add it to
`BUCKET_A_CLASSIFICATION` with a category (and update this doc) or add it to
`ACK_BUCKET_B` with a worklist entry — the check will not let it pass silently.
Run it locally and in review:

```bash
scripts/verify_skips.py --audit-buckets
```

---

## Local by-category excluded-skip report (deliverable 5, LOCAL half — done)

`make test-tcl-status` now itemises the excluded (`skipped`) rows of the **latest
local run** by skip-honesty bucket. The runner's `skipped` detail rows are read
with the CLI `--format raw` framing (ASCII 30/31 record/field separators, so file
paths with pipes or newlines parse unambiguously) and attributed to a bucket by
`scripts/verify_skips.py --categorize-skips`, which reuses the **same**
`BUCKET_A_CLASSIFICATION` map the audit enforces (single source of truth). The
report prints a per-A-category count, a Bucket-B subtotal (honest: currently
non-zero — that is the Phase-2 worklist), a named/runtime-self-skip subtotal
(per-entry adjudication deferred), and a TOTAL that reconciles to the run's
`skipped` count. `atof1` skipped rows attribute to A5 — by construction only the
`real2hex()`/`hex2real()` loop tests are skipped, so no non-loop landmine row can
leak in.

This is the **automatable, in-tree** half of the reconciled-denominator
deliverable. It runs against whatever local results DB exists; it does **not**
require the certified bench-runner DB. The certified denominator remains an
operator step below.

## Deferred work (operator data required)

These parts of #6154 depend on the certified bench-runner DB and are **not**
delivered by the static classification:

1. **Reconciled excluded-row denominator by category, CERTIFIED.** Run
   `make test-tcl-status` against the certified
   `~/.vibesql/test_results/tcl_test_results.vbsql` on the quiet AWS bench runner
   and publish the per-category breakdown of the certified run's 174,982 skipped
   rows. The tooling is already wired (see the local report above); this is a
   pure operator/data step — point it at the certified DB and record the numbers.
2. **`ifcapable`-guarded runtime self-skips.** Enumerate skips that only appear at
   runtime (including the `fuzz-oss1`/`fuzzer1`/`dbfuzz001` smoke-skips) and
   classify them. The categorizer already lands these in the "named / runtime
   self-skips" line; certifying each one's bucket needs the certified run.
3. **Drive Bucket B to zero.** Un-skip each Bucket-B pattern (and the named-test
   residual), fix the gap or leave it visibly `failed`, and confirm on a quiet
   full-suite run that no previously-passing test in the same file regresses.

Tracked under #6154.
