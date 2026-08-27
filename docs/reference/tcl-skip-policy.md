# TCL Skip-Honesty Policy

**Status:** canonical. Part of epic #5779. Delivers the static (source-only)
half of issue #6154: the full Bucket-A/Bucket-B classification of every in-tree
skip declaration, enforced-completeness auditing (`--audit-buckets`), and the
local by-category excluded-skip report in `make test-tcl-status`. The certified
by-category failure denominator (the operator-gated half) is delivered by #6180;
see the "#6180 additions" subsection and "Deferred work" item 1.

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
| `vibesql_skip_files` | 224 | whole-file skips (basename → reason) |
| `vibesql_partial_skip_files` | 1 | documented partial-skip record (`atof1`) |
| `vibesql_skip_tests` | 1,528 | individually-named test skips |
| `vibesql_skip_patterns` | 56 | glob-pattern skip rules |

**Every whole-file and pattern declaration now has an enforced bucket.** All 224
whole-file skips and all 56 pattern skips are classified below (237 Bucket A + 43
Bucket B = 280), and `scripts/verify_skips.py --audit-buckets` fails if any
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

Every whole-file skip (all 224) and 13 of the 56 pattern skips are Bucket A. They
group into the following categories. The 164 whole-file skips added by issue
#6180 (reclassifying certified out-of-scope failures) are listed in the
dedicated #6180 subsection at the end of this section; categories A11 and A12
were introduced by that work. The rationale is stated **per category** so
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
  SQLite's WAL-pragma pager semantics are not applicable), `e_reindex-1.`
  (the `e_reindex-1.*` block sets `PRAGMA writable_schema=1` + `sqlite3_db_config
  DEFENSIVE 0` to delete/reinsert `sqlite_master` index rows, corrupt the on-disk
  B-tree, then observe `REINDEX` + `PRAGMA integrity_check` repair it — the
  SQLite-internal writable_schema/B-tree-corruption harness, same precedent as
  the named `fkey1-8.3` skip; `e_reindex-1.4`'s bare `REINDEX` depends on the
  corrupted state from `1.1`–`1.3`, so the whole section is out of scope. #6195).

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
  `clock format`), `e_reindex-2.`, `reindex-2.`, `reindex-3.` (custom Tcl
  collations registered via `db collate collA/collB` / `db collate c1/c2`, used to
  verify `REINDEX` rebuilds indexes when a collation function changes; the shim
  cannot bridge these C-API collations to the CLI subprocess — same class as
  `select9-2.*.3`. These are the collation-gated half of the `e_reindex`/`reindex`
  **straddlers**: the bare-`REINDEX` + built-in-collation cases `e_reindex-0.*`,
  `reindex-1.*`, and `reindex-4.*` stay visible; `reindex-4.*` uses no custom
  collation and remains in-scope. #6195, #5720).

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

### A11. CLI shell / command-line tooling surface

Tests drive the `sqlite3` CLI shell's dot-commands (`.stats`, `.import`, `.dump`,
…) or a standalone command-line tool (`sqldiff`). They exercise the reference
tooling, not the SQL engine, and are unreachable from VibeSQL's own CLI.

- Whole-file (#6180): `shell1`, `shell2`, `shell3`, `shell4`, `shell5`, `shell7`, `shell9`, `shellA`, `sqldiff1`.

### A12. Concurrency / threading / multi-process model

Tests spawn multiple OS threads or concurrent named connections that share
in-process state (shared-cache, WAL, pcache) to provoke races, deadlocks, or
mutex-ordering assertions. The process-per-batch shim runs each file in its own
worker and cannot host the threaded/multi-connection model these files require.

- Whole-file (#6180): `mutex1`, `mutex2`, `thread001`, `thread002`, `thread003`, `thread004`, `thread005`, `thread1`, `thread2`, `thread3`, `walthread`, `pendingrace`.

### #6180 additions — certified out-of-scope failures reclassified as Bucket A

Issue #6180 (the operator-gated half of #6154) verified 164 whole files against
the certified run (`aws-c7i.8xlarge-32c-clean2`, run_id=1) and reclassified their
certified out-of-scope failures from visible `failed` into documented Bucket-A
whole-file skips. Each is declared in `BUCKET_A_CLASSIFICATION`
(`scripts/verify_skips.py`) with the category shown, and carries a prose
rationale in `scripts/tester_vibesql.tcl`:

- **A1** (C-API): `notify1`, `notify2`, `notify3`, `hook`, `hook2`, `trace`, `trace2`, `trace3`, `backup`, `backup2`, `backup4`, `backup5`, `backup_ioerr`, `scanstatus`, `scanstatus2`, `dbstatus`, `dbstatus2`, `stmt`, `bindxfer`, `bind2`, `snapshot2`, `snapshot3`, `snapshot4`, `snapshot_up`, `snapshot_fault`, `cacheflush`, `dataversion1`, `busy`, `busy2`, `interrupt`, `interrupt2`, `openv2`, `shrink`.
- **A2** (VFS/pager): `memjournal`, `memjournal2`, `mjournal`, `subjournal`, `journal1`, `journal2`, `journal3`, `trans2`, `avtrans`, `pager1`, `pager2`, `pager3`, `pager4`, `walmode`, `jrnlmode2`, `cache`, `cachespill`, `pcache`, `pcache2`, `lookaside`, `quota`, `quota2`, `shared`, `shared2`, `shared3`, `shared4`, `shared7`, `shared8`, `shared9`, `sharedA`, `shared_err`, `sharedlock`, `multiplex2`, `multiplex3`, `multiplex4`, `securedel`, `securedel2`, `cksumvfs`, `reservebytes`, `chunksize`, `fallocate`, `superlock`, `nolock`, `tempdb`, `tempdb2`, `corrupt`, `corrupt2`, `corrupt4`, `corrupt6`, `corruptB`, `corruptC`, `corruptF`, `ioerr`, `ioerr2`, `io`, `wal9`, `walseh1`, `e_walckpt`, `e_walhook`, `e_walauto`, `exclusive`, `exclusive2`, `lock2`, `lock3`, `lock4`, `lock6`, `lock7`, `rowallock`, `rdonly`, `readonly`, `uri`, `uri2`, `e_uri`, `8_3_names`, `shortread1`, `diskfull`.
- **A3** (extensions/vtab): `icu`, `normalize`, `extension01`, `stmtvtab1`.
- **A4** (incremental blob I/O): `e_blobopen`, `e_blobwrite`, `e_blobclose`, `e_blobbytes`.
- **A7** (internal/fault-injection): `malloc`, `malloc3`, `malloc5`, `memsubsys1`, `memsubsys2`, `mem5`, `mmap1`, `pagerfault`, `pagerfault2`, `indexfault`, `btreefault`, `rollbackfault`, `sortfault`, `tempfault`, `savepointfault`, `existsfault`, `altermalloc2`, `altermalloc3`, `mallocAll`, `softheap1`, `memleak`, `fuzz_malloc`, `imposter1`.
- **A9** (UTF-8-by-construction divergence): `enc`, `enc2`, `enc3`.
- **A11** (CLI tooling): `shell1`, `shell2`, `shell3`, `shell4`, `shell5`, `shell7`, `shell9`, `shellA`, `sqldiff1`.
- **A12** (concurrency/threading): `mutex1`, `mutex2`, `thread001`, `thread002`, `thread003`, `thread004`, `thread005`, `thread1`, `thread2`, `thread3`, `walthread`, `pendingrace`.

**Deliberately NOT reclassified (straddlers — stay visibly `failed`).** Per the
never-hide-an-in-scope-gap rule and the `atof1`/#6065 precedent, files that mix
out-of-scope subsystems with in-scope SQL were left failing rather than
bulk-skipped: SAVEPOINT (`savepoint*`), VACUUM/ATTACH (`vacuum*`, `attach*`,
`e_vacuum`), behavioral PRAGMAs (`pragma*`, `queryonly`), the SQL-reachable
`changes()`/`total_changes()`/`last_insert_rowid()`/`zeroblob()` function files
(`changes*`, `laststmtchanges`, `lastinsert`, `zeroblob`, `e_changes`), in-memory
databases (`memdb*`), transaction/ROLLBACK semantics (`trans`, `trans3`,
`rollback`, `rollback2`), the parser tokenizer (`tokenize`), and the TCL-interface
file (`tclsqlite`). These remain in-scope failures tracked by #6170–#6177.

**PRAGMA family detail (#6175).** The `pragma*` straddler above covers several
distinct Bucket-A-eligible sub-categories that stay visibly `failed` (not
skip-listed) rather than being pulled into A1/A2 as named entries, because
none is a static skip declaration — per the "Scope of this document" note
above, this document classifies `vibesql_skip_*` array entries, and these
PRAGMAs are deliberately left failing, not skipped.

**Every test ID below was re-derived from a live re-run** of `pragma.test`
(146 passed / 50 failed / 46 skipped), `pragma2.test` (6/5/13), `pragma3.test`
(11/13/0), `pragma4.test` (8/9/69) and `pragma6.test` (2/1/0) on 2026-08-21
against `origin/main` @ `64fac47e8`, native `tclsh` 8.6.18. The five bullets
below enumerate that run's **complete** failing set for those files (50+5+13+9+1
= 78 IDs), not a curated sample — so a reader can check the list against a fresh
run rather than trust it. Do not copy example IDs forward from issue comments
without re-running; several IDs previously listed here had already been fixed
(see "now passing" note below).

  - **A2-equivalent (pager/journal-internal):** `page_count`/`max_page_count`
    (pragma-14.1/14.2/14.2uc/14.3/14.3uc/14.5), `freelist_count`
    (pragma2-1.1/1.2/1.3/1.4 and pragma2-3.2 — tracked separately as #6414,
    which is scoped to `freelist_count` and nothing else), proxy locking
    (pragma-16.1/16.2.1/16.3/16.4/16.5/16.7/16.8/16.8.1).
  - **A1-equivalent (C-API- or harness-helper-only, no SQL surface):**
    `btree_from_db` (pragma-9.1.1/9.2.1/9.3.1/9.10/9.18), `sqlite3_db_config
    DEFENSIVE` (pragma-8.1.3), and VDBE-opcode-level `EXPLAIN` output
    (pragma4-2.100, which asserts the `P4_INTARRAY` rendering of `OP_IntegrityCk`
    in `EXPLAIN PRAGMA integrity_check`; VibeSQL has no VDBE opcode stream to
    render, so its parser rejects `EXPLAIN PRAGMA` outright).
  - **testvfs (A1-equivalent, stubbed no-op `proc testvfs` in
    `scripts/tester_vibesql.tcl`, not wired to real VFS-shim behavior):**
    pragma-19.1/19.2/19.3/19.4/19.5.
  - **Corruption harness (A2-equivalent, same precedent as `e_reindex-1.*`):**
    `integrity_check` over a deliberately corrupted page image written with
    `hexio_write` — pragma-3.2/3.3/3.5/3.6b/3.7/3.8.1/3.8.2/3.10/3.11/3.12/3.13/
    3.14/3.16/3.17/3.18, plus `21.1`/`22.2`/`22.4.2` (the runner reports the
    trailing pragma.test blocks without a `pragma-` prefix, matching the
    `do_test` names in the file) and pragma6-1.0 (`decode_hexdb`). The same
    missing `hexio_write` helper aborts pragma.test at file scope, producing
    pragma-filescope-err.1 (`invalid command name "hexio_write"`) and its two
    cascaded successors .2/.3. pragma-3.21/3.22/3.23 are the adjacent
    `PRAGMA writable_schema` variant of the same harness (`sqlite_master` is
    rewritten to fabricate UNIQUE/NOT NULL violations for `integrity_check` to
    find) — same bucket, different corruption vector.
  - **TCL-shim architecture limitation:** pragma3.test's 13 `data_version`
    failures (pragma3-140/150/160/170/180/190/195/200/201/310/320/330/340) and
    pragma4.test's 8 ATTACH-dependent failures (4.1.3/4.1.4/4.2.4/4.3.4/4.4.3
    plus pragma4-filescope-err.1/.2/.3, all failing with
    `no such table: aux.sqlite_master`) — root-caused as two distinct mechanisms
    (a hardcoded `data_version` return in
    `crates/vibesql-cli/src/executor/mod.rs`, and the shim's own
    ATTACH-setup-rescue statement-stripping in `scripts/tester_vibesql.tcl`'s
    `do_test`) in #6467, filed from #6175's re-verification. pragma-8.2.11 is a
    byproduct of the same ATTACH skip-cascade.

**Previously listed here, now passing — do not re-cite these as failures.**
Earlier triage (and #6175's 2026-08-21 Curator Enhancement, which flagged its own
numbers as not independently re-verified) listed
`cache_size`/`default_cache_size` (pragma-1.*), `synchronous` (pragma-5.*),
`lock_status` (pragma-7.3), `get_pwd` (pragma-9.5) and `cache_spill`
(pragma2-5.1/5.2/5.3) as still-failing. All of them **pass** in the re-run above.
`cache_spill` in particular was fixed by #6456 and its tracking issue #6415 is
closed; it was never in scope for #6414, which covers `freelist_count` only.

Full per-test itemization and rationale live in issue #6175's comment history
(most recently the 2026-08-21 re-verification), not duplicated here.

The `REINDEX` files `e_reindex` and `reindex` are handled the same straddler way
(#6195, #5720): the collation-gated and writable_schema-corruption sub-sections
are narrow **pattern** skips (`e_reindex-2.`/`reindex-2.`/`reindex-3.` → A5;
`e_reindex-1.` → A2), while the in-scope bare-`REINDEX` + built-in-collation cases
(`e_reindex-0.*`, `reindex-1.*`, `reindex-4.*`) stay visible — including the
genuinely-failing `reindex-1.9` (`REINDEX bogus` should raise "unable to identify
the object to be reindexed" but the shim strips `REINDEX` to a no-op), which is
kept visible as an in-scope failure and tracked in its own linked engine issue
(#6232) rather than hidden.

**ATTACH/DETACH — narrow, per-test un-skip (#6363, Phase 3 of #6310).** The
blanket ATTACH/DETACH/`aux.*`-schema skip in `uses_sqlite_internals` remains the
default everywhere: VibeSQL's `ATTACH` state is per-connection, and the TCL
shim's per-batch spawn-a-fresh-CLI-process architecture (see
`scripts/tester_vibesql.tcl` ~line 278) means an alias attached in one batch is
gone before the next, so the skip stays a legitimate harness-architecture
limitation for the vast majority of the ~131 ATTACH-touching files in the suite
(tracked broadly by #6404). #6363 added a narrow, opt-in exception: a per-batch
ATTACH/DETACH state-replay mechanism (mirroring the existing temp-view/trigger
replay), gated to an explicit allow-list (`vibesql_attach_replay_files`) of
files verified not to hit either of two distinct, still-open engine gaps
discovered during that work — `<alias>.sqlite_master` introspection is not yet
implemented (blocks `e_droptrigger.test`/`e_dropview.test`, which walk
`PRAGMA database_list` and query every attached database's `sqlite_master`),
and the shim's TEMP-table-demotion limitation (#6429) independently blocks
`e_droptrigger.test`. Only `trigger1.test` is on the allow-list today, and only
its `trigger1-10.0`/`trigger1-10.1` tests are individually verified safe to
un-skip (a second, narrower `vibesql_attach_ok` allow-list); the remaining
`trigger1.test` ATTACH-adjacent tests (10.2–10.11, 20.1) stay skipped or
visibly failing pending their own follow-up. This is a Bucket-A-adjacent
per-test carve-out, not a bucket reclassification — the skip declaration and
its rationale are unchanged for every file not on the allow-list.

**Certified by-category denominator (run_id=1, `tcl_test_results`).** After
reclassification the certified 7,123 detail-table failures (incl. markers)
partition exactly as: Bucket A **3,206** (A1 769, A2 1,790, A3 39, A4 149, A5 2,
A7 158, A9 63, A11 157, A12 79) — of which **3,197** are caught by the new #6180
whole-file skips — plus Bucket B **14** (worklist patterns) plus **3,903**
remaining in-scope failures that stay visible. 3,206 + 14 + 3,903 = 7,123, the
honest in-scope-failure denominator.

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
| `types2-` | "Type handling differs" | Type affinity is core SQL. (`types-` was resolved: Part of #6172 removed the blanket pattern and re-verified the file at 51/55 (92.7%) passing for real, with 4 named per-test Bucket-A skips for the record_sizes/btree_open SQLite-internal-B-tree-API dependency — see the `types` entry in `vibesql_partial_skip_files`. `types2-` remains an open Bucket-B item: a fresh measurement (Part of #6172) found it also un-skips to mostly-passing, 367/398 (92.2%), but its 31 failures span several apparently-distinct root causes not yet triaged.) |
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

1. **Reconciled excluded-row denominator by category, CERTIFIED.** *(Delivered by
   #6180.)* The certified run (`aws-c7i.8xlarge-32c-clean2`, run_id=1) is now
   locally reachable at `~/.vibesql/test_results/tcl_test_results.vbsql`; its
   7,123 detail-table failures (incl. markers) partition by Bucket-A category as
   recorded in the "#6180 additions" subsection above (Bucket A 3,206 + Bucket B
   14 + remaining in-scope 3,903 = 7,123). The 1-row detail-vs-summary gap
   (summary failed 7,124 vs detail 7,123) is fixed at the source in
   `scripts/tcl_runner.py` (`_reconcile_details_to_counts`) so future runs
   reconcile exactly.
2. **`ifcapable`-guarded runtime self-skips.** Enumerate skips that only appear at
   runtime (including the `fuzz-oss1`/`fuzzer1`/`dbfuzz001` smoke-skips) and
   classify them. The categorizer already lands these in the "named / runtime
   self-skips" line; certifying each one's bucket needs the certified run.
3. **Drive Bucket B to zero.** Un-skip each Bucket-B pattern (and the named-test
   residual), fix the gap or leave it visibly `failed`, and confirm on a quiet
   full-suite run that no previously-passing test in the same file regresses.

Tracked under #6154.
