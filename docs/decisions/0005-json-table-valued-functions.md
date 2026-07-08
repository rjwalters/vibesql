# ADR-0005: Table-Valued Functions for `json_each` / `json_tree` (Feasibility)

**Status**: Proposed (feasibility spike — recommendation: **GO, phased**)

**Date**: 2026-07-07

**Deciders**: Claude Code (Loom Builder) + rwalters

**Related**:
- Issue [#5981](https://github.com/rjwalters/vibesql/issues/5981) — spike(json): Phase 3, feasibility of table-valued functions for `json_each`/`json_tree` (this ADR)
- Issue [#5786](https://github.com/rjwalters/vibesql/issues/5786) — decision: JSON1 extension scope (parent)
- Issue [#5779](https://github.com/rjwalters/vibesql/issues/5779) — JSON1 epic
- Issue [#5980](https://github.com/rjwalters/vibesql/issues/5980) — JSON Phase 2: scalar mutation functions (in flight, disjoint from this spike)
- [ADR-0002](0002-parser-strategy.md) — Hand-written recursive-descent parser (this ADR builds on it)

## Context and Problem Statement

SQLite's JSON1 extension exposes two **table-valued functions** (TVFs) that appear
in the FROM clause rather than as scalar expressions:

- `json_each(json[, path])` — one row per immediate child of the JSON value at
  `path` (top level if `path` omitted).
- `json_tree(json[, path])` — one row per node in the JSON value, recursively
  (depth-first).

Both expose the same eight-column result contract:

```
key, value, type, atom, id, parent, fullkey, path
```

These functions account for roughly **80–100 failing assertions** across
`json101.test` and `json102.test`. Unlike the scalar JSON functions (Phase 1
`json_extract`/`json_valid`/etc. already landed; Phase 2 mutation functions in
flight on #5980), `json_each`/`json_tree` **cannot** be implemented as scalar
functions: they produce a *relation*, and VibeSQL today has **no table-valued
function dispatch path anywhere in the stack** — parser, executor, or the
optimizer's correlation machinery.

This ADR is a **feasibility spike**. Per #5981 it adds **no implementation
code** to `crates/`; the only artifact is this document. It answers four
questions:

1. What exists today for FROM-clause parse / plan / execute?
2. What is the proposed TVF architecture per layer, and are lateral/correlated
   arguments in the first cut?
3. What is the rough cost per layer, and which tests does a non-correlated first
   cut recover vs. which need lateral support?
4. Go / no-go, with either a decomposed list of child issues or a justified skip.

> **Branch-point note.** This survey describes `main` as of commit `9356ed4`
> (the base of `feature/issue-5981`). `crates/vibesql-executor`'s JSON scalar
> code (`.../evaluator/functions/sqlite_compat/json_funcs.rs`) is concurrently
> being modified on #5980 (Phase 2 scalar mutation). That work is disjoint from
> the FROM-clause / TVF paths surveyed here; the reusable JSON-path parser noted
> below (`parse_sqlite_json_path`) is stable on both branches.

## Section 1 — Survey of Existing FROM Parse / Plan / Execute Paths

VibeSQL is a **hand-written recursive-descent parser** (ADR-0002) with **no
intermediate logical/physical plan IR** — it compiles the AST directly to
recursive execution. There is a single FROM-clause AST enum threaded through all
three layers.

### 1a. AST — `FromClause` has exactly four variants (no function variant)

`crates/vibesql-ast/src/select.rs` (lines 304–358); arena mirror at
`crates/vibesql-ast/src/arena/select.rs` (lines 113–143):

```rust
pub enum FromClause {
    Table    { name: String, alias: Option<String>, column_aliases: Option<Vec<String>>, quoted: bool, index_hint: Option<IndexHint> },
    Join     { left: Box<FromClause>, right: Box<FromClause>, join_type: JoinType, condition: Option<Expression>, using_columns: Option<Vec<String>>, natural: bool, alias: Option<String> },
    Subquery { query: Box<SelectStmt>, alias: String, column_aliases: Option<Vec<String>> },
    Values   { rows: Vec<Vec<Expression>>, alias: String, column_aliases: Option<Vec<String>> },
}
```

There is **no `Function` / `TableFunction` variant**, and no `generate_series`,
`UNNEST`, or any other TVF-shaped construct anywhere in the tree. `Values` is
the closest analog: a FROM item whose rows are *computed from expressions*
rather than read from stored storage.

### 1b. Parser — FROM grammar

`crates/vibesql-parser/src/parser/select/from_clause.rs`:
`parse_from_clause()` (lines 11–88) → `parse_table_reference()` (lines 113–447).
The decision tree in `parse_table_reference()`:

- `Token::LParen` → peek the next keyword: `SELECT`/`WITH` ⇒ `Subquery`;
  `VALUES` ⇒ `Values`; else a parenthesized join expression (recurses).
- `Token::Identifier | DelimitedIdentifier | String` (lines 332–389) ⇒ `Table`.
  **An identifier immediately followed by `(` is not handled** — `FROM foo(...)`
  currently falls through and errors.
- Keyword-as-table-name (lines 401–438) for SQLite compat.

Alias and column-alias-list parsing already exist and are reusable:
`parse_alias_name()` and `parse_column_alias_list()`
(`crates/vibesql-parser/src/parser/helpers.rs`) already parse `AS x` and
`AS x(a, b, c)`, and `Values`/`Subquery` already store `column_aliases`.

The parser has **no external dependency** (no `sqlparser-rs`) — nothing is
inherited; every construct is bespoke (confirmed in
`crates/vibesql-parser/Cargo.toml`).

### 1c. Execution — direct AST-to-rows, no plan IR

Central dispatch: `execute_from_clause()` in
`crates/vibesql-executor/src/select/scan/mod.rs` (lines 55–192) matches the four
variants directly:

| Variant | Handler | File |
|---|---|---|
| `Table` | `execute_table_scan_with_identifier` | `select/scan/table.rs` (244–496) |
| `Join` | `execute_join` (recursive left-deep) | `select/scan/join_scan/mod.rs` (62–280) |
| `Subquery` | `execute_derived_table` | `select/scan/derived.rs` |
| `Values` | `execute_values` | `select/scan/values.rs` (26–119) |

Each handler returns a `FromResult` (schema + materialized rows).

**`execute_values` is the precedent to copy.** It builds an empty-schema
`CombinedExpressionEvaluator`, evaluates each row's expressions, collects
`Row`s, derives a schema from the inferred column types, and returns
`FromResult::from_rows(schema, rows)`. A TVF handler follows the *same shape*:
evaluate the argument expression(s) to a JSON value, expand it into rows, attach
the fixed 8-column schema, return `FromResult`.

Row-source abstraction: the `RowIterator` trait
(`select/iterator/mod.rs` lines 69–93) with a concrete `TableScanIterator`
(`select/iterator/scan.rs`). Materialized `FromResult` is the dominant path
today; the iterator layer is a partially-integrated PoC. A first-cut TVF can
**materialize** (like `Values`) and skip the iterator layer entirely.

### 1d. Correlation / lateral machinery (already present, but not LATERAL)

`crates/vibesql-executor/src/correlation.rs::is_correlated()` (lines 47–63)
detects a FROM/subquery referencing outer-scope columns. The scan handlers
already thread `outer_row: Option<&Row>` and
`outer_schema: Option<&CombinedSchema>` through `execute_from_clause`,
`execute_join`, and `execute_table_scan` — this is how correlated **subqueries**
in FROM are evaluated per outer row (predicate push-down is disabled when
`outer_row.is_some()`).

However: there is **no `LATERAL` keyword**, **no dependent-join / `Apply`
operator**, and **no re-evaluation of a FROM sibling per row of a preceding
sibling**. Comma-joined siblings (`FROM a, b`) are executed independently and
cross-joined; today `b` **cannot** reference `a`'s columns. That gap is the crux
of the lateral question below.

### 1e. Scalar-function dispatch (contrast) + reusable JSON-path parser

Scalar functions dispatch through one big `match name.to_uppercase()` in
`crates/vibesql-executor/src/evaluator/functions/mod.rs::eval_scalar_function`
(lines 45–318), called from `eval_function` in
`.../evaluator/expressions/special.rs`. There is **no analogous
`eval_table_function` dispatcher** — that is precisely the missing infrastructure.

Reusable asset: the JSON path grammar
(`json_funcs.rs::parse_sqlite_json_path` → `Vec<PathSegment>`, with
`PathSegment::{Key, Index, IndexFromEnd}`) already parses `$`, `.key`,
`."quoted"`, `[n]`, `[#-n]`. The optional `path` argument of
`json_each`/`json_tree` should reuse it verbatim.

### 1f. Cost multiplier: exhaustive `FromClause` matches

Adding a fifth `FromClause` variant is **not** a localized change. Grep finds
~85 sites (across ~138 files) that pattern-match `FromClause` variants —
optimizer correlation checks
(`optimizer/subquery_rewrite/correlation.rs`, `.../detection.rs`), aggregation
detection, fast-path checks, view expansion, the arena AST mirror + its
`convert.rs`, `visitor.rs`, and `pretty_print.rs`. Many are exhaustive `match`
arms that will fail to compile until a new arm is added. This is a *known,
mechanical* cost — the compiler enumerates every site — but it is real and
dominates the parser/executor line count.

## Section 2 — Proposed TVF Architecture (per layer)

### 2a. AST — add a `TableFunction` variant

```rust
// crates/vibesql-ast/src/select.rs  (+ arena mirror in arena/select.rs)
FromClause::TableFunction {
    name: String,                        // e.g. "json_each" (normalized lowercase)
    args: Vec<Expression>,               // 1..=2 args: json value, optional path
    alias: Option<String>,               // FROM json_each(x) AS je
    column_aliases: Option<Vec<String>>, // AS je(k, v)
}
```

`args` are ordinary `Expression`s — this is what makes a *correlated* argument
(`json_each(t.j)`) representable without any further AST change: `t.j` is just a
column-reference `Expression`. Whether it is *evaluated* correlated is an
execution decision (Section 2c), not a parse decision.

Every one of the ~85 exhaustive `FromClause` matches (Section 1f) gets a new arm.
For read-only/analysis sites (correlation detection, aggregation detection,
visitor walk, pretty-print, arena convert) the arm is mechanical.

### 2b. Parser — recognize `ident(` in FROM position

In `parse_table_reference()`, the `Identifier` branch (lines 332–389): after
reading the identifier, if the next token is `Token::LParen`, parse a
comma-separated `Expression` argument list, then reuse the existing
`parse_alias_name()` / `parse_column_alias_list()` for `AS je(...)`. Produce
`FromClause::TableFunction`. Only names on an allow-list (`json_each`,
`json_tree`) are accepted as TVFs in the first cut; any other `ident(` in FROM
is a parse error, preserving current behavior for everything else. Mirror the
same change in the arena parser (`arena_parser/select.rs`).

### 2c. Executor — a table-function dispatcher + JSON expander

1. New `execute_from_clause` arm → `execute_table_function(name, args, alias,
   column_aliases, database, cte_results, outer_row, outer_schema)` in a new
   `select/scan/table_function.rs`, modeled line-for-line on
   `execute_values`.
2. Evaluate `args[0]` (and optional `args[1]` path) with
   `CombinedExpressionEvaluator`. **Correlation falls out for free**: pass the
   already-threaded `outer_row`/`outer_schema` into the evaluator so
   `json_each(t.j)` resolves `t.j` against the current outer row — *provided the
   TVF is nested where an outer row exists* (a correlated subquery, or the
   lateral wiring in Phase 2 below).
3. Parse the optional path with the existing `parse_sqlite_json_path`.
4. Expand:
   - `json_each`: iterate immediate children of the (possibly path-navigated)
     value — array elements or object members; a scalar yields exactly one row.
   - `json_tree`: depth-first recursive walk emitting one row per node
     (including the root).
   For each node emit the 8 columns `key, value, type, atom, id, parent,
   fullkey, path` per SQLite semantics (`atom` = value for leaves, NULL for
   containers; `id`/`parent` = stable pre-order node ids; `fullkey`/`path` =
   JSONPath strings).
5. Return `FromResult::from_rows(fixed_8col_schema, rows)`, applying
   `alias`/`column_aliases` exactly as `Values` does.

A new scalar registry entry is **not** needed; a small dedicated dispatcher
(only two names) is enough. A JSON DOM already exists behind the scalar JSON
functions; the expander walks that DOM.

### 2d. Lateral / correlated arguments — **NOT in the first cut**

The common real-world (and heavily-tested) form is
`FROM t, json_each(t.j)` — the TVF argument references a **preceding sibling**
FROM item. That is exactly the **LATERAL** semantics VibeSQL does not have: sibling
FROM items are cross-joined independently and the right sibling cannot see the
left sibling's columns (Section 1d). Supporting it requires a **dependent join**:
for each row of `t`, re-evaluate `json_each(t.j)` and cross-product. That is a
genuine execution-model addition (an `Apply`/dependent-join operator, or
special-casing a TVF sibling to iterate the preceding result), independent of
JSON.

**First cut = non-correlated only:** `json_each`/`json_tree` where every
argument is a literal, bind parameter, or otherwise outer-independent
expression, plus the case where the TVF is the correlated child of a subquery
that *already* threads an outer row (e.g.
`EXISTS(SELECT 1 FROM json_each(t.j, '$.items') ...)` — the subquery path already
supplies `outer_row`). The bare comma-lateral form `FROM t, json_each(t.j)` is
Phase 2.

## Section 3 — Cost Estimate and Test Recovery

### 3a. Rough sizing per layer

| Layer | Work | Rough size |
|---|---|---|
| AST | New `TableFunction` variant (both mirrors) + ~85 mechanical match arms across ~138 files + `visitor`/`pretty_print`/arena `convert` | **M** (mostly mechanical; compiler-enumerated) |
| Parser | `ident(` detection in FROM (both parsers) + arg list, reuse alias helpers | **S** |
| Executor (Phase 1) | `table_function.rs` (mirror `execute_values`) + JSON `json_each`/`json_tree` expander producing the 8 columns; reuse `parse_sqlite_json_path` and existing JSON DOM | **M** |
| Executor (Phase 2, lateral) | Dependent-join / lateral wiring so a TVF sibling re-evaluates per row of a preceding FROM item | **L** (new execution-model capability, JSON-independent) |

Overall: **Phase 1 is Medium** (dominated by the mechanical AST fan-out plus the
JSON tree/each expander); **Phase 2 (lateral) is Large** and is really a
general-purpose LATERAL feature that JSON merely motivates.

### 3b. Test recovery: non-correlated first cut vs. lateral

From a grep-grounded classification of `json101.test` + `json102.test`
(≈32 distinct TVF test blocks):

| Bucket | Meaning | Count | Phase |
|---|---|---:|---|
| A — Non-correlated | `FROM json_each('literal'/computed)`; also TVF inside a correlated subquery that already gets an outer row | ~15 (~65% of json101; 0 of json102) | **Phase 1 recovers** |
| B — Lateral / correlated sibling | `FROM t, json_each(t.j)`, `FROM t, json_tree(t.j[,'$.path'])` | ~17 (~35%) — **all 8 of json102's TVF blocks** | **Needs Phase 2** |

Representative Bucket A (Phase-1-recoverable), verbatim:

```sql
SELECT fullkey, atom, '|' FROM json_tree(json_set('{}','$.x',123,'$.x',456));
SELECT json_insert('{}','$.a',value) FROM json_tree('[1,2,3]') WHERE atom IS NULL;
SELECT fullkey FROM json_each('123');
SELECT fullkey FROM json_each('null');
```

Representative Bucket B (needs lateral), verbatim:

```sql
SELECT j2.rowid, jx.rowid, fullkey, path, key
  FROM j2, json_tree(j2.json) AS jx WHERE ...;
SELECT DISTINCT user.name
  FROM user, json_each(user.phone) WHERE json_each.value LIKE '704-%';
SELECT big.rowid, fullkey, value
  FROM big, json_tree(big.json) WHERE json_tree.type NOT IN ('object','array');
```

Column priority observed in the tests: `fullkey` (14+), `key`/`value` (8+),
`atom` (5+), `path`/`type`/`id` (3–5); `parent` is exercised 0 times but must
still be emitted for contract completeness. The optional `path` argument, where
present, is always a **literal** — so Phase 1 can treat it as a constant.

**Bottom line:** a non-correlated Phase 1 recovers roughly **~65% of json101's**
TVF assertions and **~0% of json102's** (json102 is entirely the
`FROM table, json_each(col)` lateral idiom). Full recovery of both files
requires Phase 2 lateral support.

## Section 4 — Go / No-Go Recommendation

**Recommendation: GO, phased — build Phase 1 (parse + non-correlated
execute) now; gate Phase 2 (lateral) on a separate go/no-go once Phase 1 lands
and the lateral cost is re-confirmed against the ~85-site AST fan-out actually
encountered.**

Rationale:

- **The architecture fits cleanly.** `execute_values` is a working precedent for
  "a FROM item that computes its rows"; the correlation plumbing
  (`outer_row`/`outer_schema`) already exists; the JSON-path parser and DOM
  already exist. Phase 1 is additive and mechanical, not a redesign.
- **Phase 1 delivers real, isolated value** (~65% of json101's TVF tests, plus
  unblocking the `json_each`/`json_tree` *syntax* end-to-end) at Medium cost,
  and de-risks Phase 2 by proving the AST/parser/executor wiring first.
- **Phase 2 (lateral) is the expensive, general part** and should not be smuggled
  in under a JSON label: a dependent-join/`LATERAL` capability is a
  cross-cutting execution-model feature that other work (UNNEST, correlated
  TVFs generally) would also want. Decoupling it keeps each PR reviewable and
  lets the operator decide whether the remaining ~35% (all of json102) justifies
  a general LATERAL implementation now or later.
- **Not a skip:** the failures are widely-used app functionality, the path is
  clear, and nothing here is speculative. A documented-skip would forfeit an
  achievable ~65% for no architectural benefit.

### Proposed decomposition (child issues — listed, NOT filed; filing is the operator's call)

1. **`feat(ast): add FromClause::TableFunction variant`** — variant on both the
   standard and arena `FromClause`; add arms to all exhaustive matches
   (correlation/detection/aggregation/fast-path/view/visitor/pretty-print/arena
   convert). Size **M** (mechanical, compiler-enumerated). No behavior change
   yet. *Blocks 2 and 3.*
2. **`feat(parser): parse json_each/json_tree in FROM position`** — recognize an
   allow-listed `ident(` in `parse_table_reference()` (both parsers), parse the
   arg list, reuse `parse_alias_name`/`parse_column_alias_list`. Size **S**.
   *Depends on 1.*
3. **`feat(executor): non-correlated json_each/json_tree table functions`** — new
   `select/scan/table_function.rs` mirroring `execute_values`; JSON `json_each`
   (one level) + `json_tree` (recursive) expanders emitting the 8-column
   contract; reuse `parse_sqlite_json_path` and the existing JSON DOM. Recovers
   Bucket A (~65% of json101). Size **M**. *Depends on 1, 2.*
4. **`spike/feat(executor): LATERAL / dependent-join for TVF siblings`** —
   separate go/no-go: evaluate a TVF (or subquery) sibling per row of a preceding
   FROM item so `FROM t, json_each(t.j)` works. Recovers Bucket B (~35%,
   including all of json102). Size **L**; general LATERAL capability, JSON is the
   motivator. *Depends on 3; gated on re-confirmed cost.*
5. **`chore(tcl): re-baseline json101/json102 after Phase 1`** — record recovered
   assertions; document the remaining Bucket-B failures as pending Phase 2
   (LATERAL) so epic #5779's baseline reflects reality rather than a silent gap.

## Consequences

### Positive
- Unblocks `json_each`/`json_tree` syntax end-to-end and recovers ~65% of
  json101's TVF assertions at Medium cost, with no redesign.
- Introduces the *first* TVF slot in the AST/parser/executor — a reusable hook
  for future TVFs (UNNEST, `generate_series`).
- Cleanly separates the cheap JSON-specific work (Phases 1–3) from the expensive
  general LATERAL work (Phase 4).

### Negative / Risks
- Adding a `FromClause` variant touches ~85 match sites across ~138 files;
  although mechanical, it is a wide (and merge-conflict-prone) diff — sequence it
  ahead of, or coordinate with, other FROM-touching work.
- Phase 1 leaves the *most common real-world idiom*
  (`FROM t, json_each(t.j)`, all of json102) still failing until Phase 4; the
  re-baseline issue (#5) must document this so the epic isn't misread as "done."
- Phase 4 (LATERAL) is genuinely Large and may not be justified by JSON alone —
  hence the explicit gate.

### Neutral
- Phase 1 materializes rows (like `Values`) and ignores the partial
  `RowIterator` PoC; if/when the iterator path is promoted, a streaming TVF
  iterator can be added without changing the AST/parser contract.

## Validation

Success criteria for this decision:
1. ✅ This ADR merged with the four required sections (survey, architecture,
   cost + test recovery, go/no-go with decomposition).
2. ✅ Recommendation cross-posted to #5981 and #5786.
3. ⏳ If GO accepted: issues 1–5 above filed by the operator and sequenced.
4. ⏳ Phase 1 (issues 1–3) recovers Bucket-A assertions in `json101.test`.
5. ⏳ Phase 4 gated on a re-confirmed LATERAL cost estimate.

## References
- SQLite JSON1 `json_each`/`json_tree`: https://www.sqlite.org/json1.html#jeach
- ADR-0002 (hand-written parser): [0002-parser-strategy.md](0002-parser-strategy.md)
- `execute_values` precedent: `crates/vibesql-executor/src/select/scan/values.rs`
- Correlation plumbing: `crates/vibesql-executor/src/correlation.rs`
- JSON path parser (reuse): `crates/vibesql-executor/src/evaluator/functions/sqlite_compat/json_funcs.rs`

---

**Status**: PROPOSED — recommendation **GO (phased)**

**Next Steps**: operator decides whether to file child issues 1–5; Phase 1
(issues 1–3) is the immediately actionable, self-contained unit.
