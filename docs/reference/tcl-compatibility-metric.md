# The Canonical "SQLite Compatibility %" Metric

**Status:** canonical. Part of epic #5779. Defines issue #6156.

This document defines, precisely and once, what VibeSQL means when it claims a
**"SQLite compatibility percentage."** It fixes the exact numerator and
denominator, names the honest claim sentence (with its scope/exclusions clause),
and freezes the inclusion rules so the number is comparable across runs.

> **What this document is and is not.** This defines *what* the compatibility %
> means and wires the headline into `make test-tcl-status`. It does **not**
> assert that the number *is* 100%. Reaching an actual 100% depends on (a)
> certified full-suite results from the quiet bench runner and (b) driving the
> Bucket-B skip worklist to zero and triaging the remaining failures — both
> tracked elsewhere (#6154 deliverable 5, #6155, the epic). The metric is the
> honest ruler; the reading against that ruler is separate work.

---

## 1. The denominator: "in-scope, SQL-reachable SQLite tests"

The compatibility % is a percentage **of a defined universe**. That universe is:

> Every SQLite TCL test **except** those excluded by a **Bucket-A** skip
> declaration in [`tcl-skip-policy.md`](./tcl-skip-policy.md).

Concretely:

```
in_scope_tests  =  all_tests  −  bucket_A_excluded_tests
```

- **`all_tests`** — every numbered test row the SQLite TCL suite would emit.
- **`bucket_A_excluded_tests`** — tests suppressed by a **Bucket A** skip:
  out-of-scope *by design*. These exercise functionality VibeSQL deliberately
  does not implement (the C API, VFS/pager internals, unshipped extensions,
  incremental-blob I/O, harness-only TCL helpers, internal VDBE/optimizer
  counters, built-in-test fuzzers, and the documented UTF-8-strict divergence).
  The full taxonomy — categories A1–A10, every whole-file and pattern skip that
  qualifies — lives in [`tcl-skip-policy.md`](./tcl-skip-policy.md) and is **not**
  re-enumerated here. That document is the single source of truth for what is
  out of scope; this document only *points at it* to define the denominator.

**Critically, Bucket-A exclusions are the *only* legitimate exclusions.**
A **Bucket-B** skip (a real, in-scope SQL feature hidden behind a "behavior
differs" / "not implemented" rationale) is **dishonest to exclude** and stays in
the denominator: it counts against the compatibility % until it is fixed or left
visibly failing. Driving Bucket B to zero is what makes an honest 100% possible;
excluding Bucket-B tests would inflate the number by hiding real gaps. See the
two-bucket rule in [`tcl-skip-policy.md`](./tcl-skip-policy.md).

### How the denominator maps onto the results database

In `~/.vibesql/test_results/tcl_test_results.vbsql`, each Bucket-A exclusion
surfaces at runtime as a `status='skipped'` row (the shim declares it via
`vibesql_skip_files` / `vibesql_skip_patterns` / `vibesql_skip_tests`). Every
in-scope test that actually ran surfaces as `passed`, `failed`, or a
non-completion marker (`timeout`/`incomplete`/`error`). Therefore:

```
in_scope_scored  =  passed + failed + timeout + incomplete + error   (skipped excluded)
excluded         =  skipped
```

This is exactly the denominator of the **canonical pass-rate query** already
frozen in `CLAUDE.md` (§ "Results Tables and the Canonical Pass-Rate Query") —
`skipped` rows are excluded, marker rows count as failures. The compatibility %
does **not** invent a new query; it *is* that query, re-labeled as the headline
compatibility claim.

> **Local vs. certified denominator (honesty note).** In a local worktree the
> `skipped`-row count reflects whatever ran on this machine, and some Bucket-A
> exclusions only appear at runtime via `ifcapable` guards rather than as static
> declarations. The fully reconciled **excluded-row count by category** against
> the certified 174,982-skip run (tag `aws-c7i.8xlarge-32c-final`) requires the
> bench-runner DB and is **deferred to #6154 deliverable 5**. What is buildable
> and honest *today* is the definition above plus the per-run numbers; the
> by-category certified reconciliation is explicitly out of scope for #6156.

---

## 2. The headline metric: raw-row pass rate (and why)

Two defensible weightings exist. We fix **one** as THE headline and keep the
other as a supplementary lens, consistent with `CLAUDE.md`'s existing
"Raw headline vs. file-weighted metrics" guidance.

### THE headline: raw per-test-row pass rate

```
compatibility_%  =  100 × passed / (passed + failed + timeout + incomplete + error)
```

summed across every in-scope test row in the run (`skipped` excluded). **This is
the canonical "SQLite compatibility %".**

Why raw-row is the headline:

1. **It is the epic-comparable number.** Epic #5779's baseline — "Raw pass rate:
   72.3% — 116,719 passed / 44,624 failed" — is itself a raw per-row ratio with
   these same inclusion rules. Only a raw-to-raw comparison is valid (see § 4).
   Making anything else the headline would silently break comparability.
2. **It answers "if you run an in-scope SQLite test at random, how likely is
   VibeSQL to pass it?"** — the question an outside engineer actually means by
   "compatibility."
3. **It cannot be gamed by file granularity.** Splitting or merging test files
   does not move a raw-row number; it *does* move a file-averaged number.

### The supplementary lens: file-weighted mean

The raw ratio has one honest hazard: it is dominated by whichever files emit the
most rows. A few "monster" files (`fuzz.test` ~25k rows, `func.test` ~14.7k) that
VibeSQL fails wholesale can drag the raw headline into single digits even while
normal SQL files pass 90–99%. To keep that visible **without** changing the
headline, `make test-tcl-status` also reports the file-weighted **mean per-file
pass rate** and **clean vs. dirty file counts** (already computed in
`cmd_status()`; issue #6137). These are reported *alongside*, never in place of,
the raw headline, and are **not** comparable to the epic's 72.3% (different
denominator). Per `CLAUDE.md`: use the **raw** number for cross-run / vs-epic
comparison; use the **file-weighted** number for "how broad is coverage, ignoring
monster-file domination." **Never quote the file-weighted number against 72.3%.**

---

## 3. The honest claim sentence

A bare percentage is not an honest claim. The claim **must** name what it
excludes. The canonical sentence (numbers filled from the live run, never
hardcoded):

> **VibeSQL passes `{passed}`/`{scored}` (`{rate}%`) of the `{N}` in-scope
> SQLite TCL tests (`{K}` files); it deliberately does not implement SQLite's C
> API, VFS/pager internals, or unshipped extensions, so `{P}` out-of-scope tests
> are excluded (enumerated by category in
> [`tcl-skip-policy.md`](./tcl-skip-policy.md)).**

where, for the reported run:

| Placeholder | Meaning | Source |
|-------------|---------|--------|
| `{passed}`  | in-scope tests that passed | `SUM(status='passed')` |
| `{scored}` = `{N}` | in-scope tests scored (denominator) | `SUM(status IN passed,failed,timeout,incomplete,error)` |
| `{rate}`    | `100 × passed / scored`, 1 dp | derived |
| `{K}`       | files that produced detail rows | `COUNT(DISTINCT file_path)` |
| `{P}`       | out-of-scope tests excluded | `SUM(status='skipped')` |

The acceptance bar for this sentence: **would a skeptical outside engineer accept
it as honest?** It passes that bar only because it (a) states the denominator,
(b) names the excluded *categories* rather than hiding them behind a bare
percentage, and (c) links to the per-category enumeration. A percentage without
the exclusions clause is **not** an approved claim.

`make test-tcl-status` prints this sentence, filled from the live run, as its
compatibility headline (see § 5).

---

## 4. Relationship to epic #5779's 72.3% baseline

Epic #5779 recorded a baseline "Raw pass rate: **72.3%** — 116,719 passed /
44,624 failed" across **728 files**. The compatibility % defined here uses the
**same weighting and the same inclusion rules** as that baseline (raw per-row,
`skipped` excluded, marker rows as failures), so the two are directly
comparable — *that is the whole point of fixing raw-row as the headline.*

The current headline number will nonetheless usually **differ** from 72.3%, and
that difference is **not** a "72 → 100" delta to be claimed. It differs because:

1. **Different file/skip universe.** The 728-file baseline predates the
   skip-honesty audit ([`tcl-skip-policy.md`](./tcl-skip-policy.md)). The
   in-scope denominator moves as Bucket-A/Bucket-B classification changes which
   tests are excluded vs. counted. A run over a different universe is a different
   denominator.
2. **Monster-file domination.** The raw number swings sharply with whether the
   high-row files (`fuzz.test`, `func.test`, …) ran and how they fared, because
   they contribute tens of thousands of rows each.
3. **Marker rows.** A run with any `timeout`/`incomplete`/`error` markers reads
   *worse* (markers count as failures) and is **not** comparable to a clean-run
   baseline — it must be re-run on a quiet machine before comparison.

**Honest-claim rule:** do not present a headline computed over one universe as a
delta against 72.3% computed over another. Quote the current number *with its run
universe* (files-with-results, marker count), and only compare raw-to-raw over
the same inclusion rules. There is no legitimate "we went from 72.3% to 100%"
narrative unless both numbers are raw-row ratios over the same in-scope universe
on clean runs.

---

## 5. Wiring: what `make test-tcl-status` prints

`make test-tcl-status` → `./scripts/tcltest status` → `cmd_status()`. The
compatibility headline is printed as a dedicated **"SQLite compatibility metric"**
block that:

1. Prints the raw-row compatibility % (numerator/denominator from the canonical
   query in § 2) as the headline.
2. Prints the **honest claim sentence** from § 3, with `{passed}/{scored}`,
   `{rate}`, `{K}`, and `{P}` filled from the live run.
3. Prints the **excluded out-of-scope categories** — the static Bucket-A skip
   category counts from `scripts/verify_skips.py --list-categories` — so the
   exclusions clause is backed by a visible breakdown, not just a link.
4. Links to [`tcl-skip-policy.md`](./tcl-skip-policy.md) (the exclusion taxonomy)
   and to this document (the metric definition).

The pre-existing **file-weighted** supplementary metrics (§ 2) and the run-universe
reconciliation are unchanged and still printed alongside. If no results database
exists yet, the block degrades gracefully (it prints the definition and the
static category breakdown; the live numbers are simply absent).

> The per-category **excluded-row count for the certified run** is **not** printed
> by the local tool — that reconciliation needs the bench-runner DB and is
> deferred to **#6154 deliverable 5**. Locally, the tool shows the static
> Bucket-A category declarations (the *shape* of the exclusions) plus this run's
> own `skipped` count (the `{P}` in the claim sentence).

---

## 6. Frozen inclusion rules (do not change silently)

For the compatibility % to stay comparable across runs and against the epic, its
inclusion rules are **frozen**. Changing any of them silently breaks every
historical comparison:

1. **Weighting:** raw per-test-row (not file-weighted).
2. **Numerator:** `status = 'passed'`.
3. **Denominator:** `status IN ('passed','failed','timeout','incomplete','error')`.
4. **`skipped` rows are excluded** from the denominator (they are the Bucket-A
   out-of-scope exclusions).
5. **Marker rows** (`timeout`/`incomplete`/`error`) **count as failures**, so a
   compromised run reads *worse*, never silently smaller.
6. **Scope of exclusions:** only **Bucket-A** skips may reduce the denominator.
   Bucket-B skips stay in (counted against the rate) until fixed or reclassified
   in [`tcl-skip-policy.md`](./tcl-skip-policy.md).

These rules are identical to the canonical pass-rate query frozen in `CLAUDE.md`.
If that query changes, this document and `cmd_status()` must change with it — the
three must never drift. `CLAUDE.md`'s "Which number do I quote?" guidance links
here so the definitions stay in sync.

---

## Related documents

- [`tcl-skip-policy.md`](./tcl-skip-policy.md) — the exclusion taxonomy
  (Bucket A vs. Bucket B; defines what leaves the denominator).
- `CLAUDE.md` § "SQLite TCL Test Suite" — the canonical pass-rate SQL, the
  raw-vs-file-weighted guidance, and results-table schema.
- `scripts/tcltest` `cmd_status()` — the wiring that prints this metric.
- `scripts/verify_skips.py --list-categories` — the live Bucket-A category counts.
</content>
</invoke>
