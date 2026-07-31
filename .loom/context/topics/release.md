# VibeSQL Release Conventions

Project-specific context injected by `methodology-inject.sh` whenever `/repo:release` (or release-cut phrasing) comes up. This **augments** `.claude/commands/repo/release.md` (Repo Skills `/repo:release`, Phases 0-7); read both.

This file carries only **advisory** reminders. The **procedural** half — steps that must run at a named phase boundary (CHANGELOG house style, irreversibility gate, publish-workflow polling, GitHub-Release gating, website follow-ups) — lives in `.repo/release-policy.md`, which `/repo:release` loads and validates at Phase 0 via its `## seam: <name>` sections.

## Advisory reminders (any phase)

### Pre-flight: four version-bearing files

VibeSQL has four version-bearing file groups: `Cargo.toml` (workspace.package + member-crate internal `vibesql-*` pins), `pyproject.toml` (root), `crates/vibesql-python-bindings/pyproject.toml`, and `Cargo.lock`. `scripts/version.sh check` (invoked by `/repo:release`'s Phase 2 drift gate) covers all of them. Drift surfaces as a non-zero exit and a `DRIFT: …` line per offending file — stop and resolve before bumping.

CI gating policy (which workflows are release-blocking) is procedural and lives in `.repo/release-policy.md` under `## seam: pre-flight`.

### Phase 3: commit-volume expectation

VibeSQL cycles are commit-heavy — the `[0.1.4]` entry triaged **878 commits**. Warn the operator upfront if `git log <last-tag>..HEAD --oneline | wc -l` exceeds 100. Group by scope for the CHANGELOG spine:

```bash
git log "$LAST..HEAD" --pretty=format:'%s' \
  | grep -oE '^(feat|fix|chore|refactor|test|docs|perf)\([^)]+\)' \
  | sort | uniq -c | sort -rn | head -30
```

VibeSQL uses scoped conventional commits almost exclusively: `feat(mvcc):`, `fix(parser):`, `chore(deps):`.

### Phase 3: VibeSQL-specific semver categories

VibeSQL is **pre-1.0**, so MAJOR/MINOR semantics are looser. Treat `0.X.0` as "meaningful capability or compatibility change" and `0.X.Y` as "everything else". Don't reach for `1.0.0` without a separate conversation about API stability.

**MAJOR bump (rare pre-1.0)**: Changed `.vbsql` on-disk format that old files can't read; removed SQL feature or changed semantics so valid queries now error or return different results; removed/renamed public Rust API on a published crate (`vibesql-parser`, `vibesql-types`, `vibesql-storage`, `vibesql-executor`, `vibesql-server`, `vibesql-cli`, `vibesql-wasm-bindings`, `vibesql-l10n`); removed/renamed Python binding; changed server wire protocol incompatibly; changed CLI flags incompatibly.

**MINOR bump**: New SQL syntax (e.g. `VACUUM` / `VACUUM INTO`); new optimizer pass or join algorithm; new public Rust API on a published crate; new Python or WASM binding; new benchmark suite or observability surface; new storage subsystem capability.

**PATCH bump**: Bug fixes; performance improvements with no semantic change; internal refactors; docs/CLAUDE.md/README updates; dependency bumps; test additions; web demo updates.

### Phase 5: scripts/version.sh drives the bump

`/repo:release` Phase 2 detects `scripts/version.sh` as the version tool (executable, first match wins), and Phase 5 dispatches `./scripts/version.sh bump <level> --tag`. The script exposes `show` / `list` / `check` / `bump <level> [--tag]` / `set <X.Y.Z> [--tag]`. `bump`/`set` touch all four version-bearing files atomically (workspace.package + member-crate `vibesql-*` pins + both pyprojects + `cargo update -w` to refresh `Cargo.lock`), so Phase 5's auto-dispatch drives the entire bump without operator intervention.

**DO NOT bump** `crates/vibesql-sqllogictest/Cargo.toml` (pinned to upstream sqllogictest's `0.28.x`) or `web-demo/package.json` (independent version line). The bump script intentionally leaves them alone. If the operator wants either changed, that's a separate decision.

## Important Notes (VibeSQL-specific)

- **Four version-bearing files, propagation pattern**: only `[workspace.package]` in `Cargo.toml` carries the canonical Rust version; every member crate inherits via `version.workspace = true`. The exception is `crates/vibesql-sqllogictest/Cargo.toml`, which pins to upstream sqllogictest's `0.28.x` — do not touch it.
- **`Cargo.lock` IS a version-bearing file, and it IS tracked**: every `vibesql-*` workspace crate has an entry in `Cargo.lock`, so `cargo update -w` is required after bumping `Cargo.toml` — it is not a no-op. `Cargo.lock` is committed in this repo (this workspace ships binaries — see `.gitignore`), and `scripts/version.sh` stages and commits it atomically with the other version sources (`Cargo.toml`, member `vibesql-*` pins, both pyprojects) on the release commit.
- **Web demo and dashboard data are separate**: `make website` + `wrangler deploy` are NOT part of the release tag flow — they run after to refresh the public dashboard (see `## seam: post-summary` in `.repo/release-policy.md`).
- **Do not auto-publish to extra registries.** The release flow only triggers what the existing workflows do. If the operator later adds a Homebrew formula, npm package, etc., that's a separate manual step.
