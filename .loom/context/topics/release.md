# VibeSQL Release Conventions

Project-specific context injected by `methodology-inject.sh` whenever `/loom:release` is invoked. This **augments** `.claude/commands/loom/release.md` (the loom-default v0.10.3 skill); read both.

## Pre-flight (Phase 1 augmentation)

VibeSQL has four version-bearing files that the default skill's tool-detection probe (`./scripts/version.sh` → `cargo-release` → …) does **not** fully cover today:

- `Cargo.toml` (`[workspace.package]` block — propagates to every `version.workspace = true` member crate)
- `pyproject.toml` (root, drives the `vibesql` PyPI wheel)
- `crates/vibesql-python-bindings/pyproject.toml` (the actual Python bindings manifest)
- `Cargo.lock` (every `vibesql-*` workspace crate has an entry that needs `cargo update -w` to refresh)

Run this consistency check **in addition** to the default Phase 1 checks. Stop if the four sources disagree — resolve drift before bumping anything:

```bash
grep -m1 '^version' Cargo.toml
grep -m1 '^version' pyproject.toml
grep -m1 '^version' crates/vibesql-python-bindings/pyproject.toml
grep -A1 '^name = "vibesql"' Cargo.lock | grep '^version' | sort -u
```

### CI gating policy

VibeSQL CI is non-uniform — not every workflow is release-blocking:

- **Blocking**: `ci-main.yml`, `ci-extended.yml` failing on main → fix first.
- **NOT blocking** (note them, but ask the operator before letting them stop a release): `fuzz.yml`, `miri.yml`. These run long and red runs are often pre-existing.

## Gather Changes (Phase 2 augmentation)

VibeSQL release cycles are commit-heavy — the `[0.1.4]` entry triaged **878 commits**. Warn the operator upfront if `git log <last-tag>..HEAD --oneline | wc -l` exceeds 100; budget time for Phase 4.

Group commits by conventional-commit scope (the spine of the CHANGELOG):

```bash
git log "$LAST..HEAD" --pretty=format:'%s' \
  | grep -oE '^(feat|fix|chore|refactor|test|docs|perf)\([^)]+\)' \
  | sort | uniq -c | sort -rn | head -30
```

VibeSQL uses scoped conventional commits almost exclusively: `feat(mvcc):`, `fix(parser):`, `chore(deps):`, etc.

## Semver Decision (Phase 3 augmentation — VibeSQL-specific categories)

VibeSQL is **pre-1.0**, so MAJOR/MINOR semantics are looser. Treat `0.X.0` as "meaningful capability or compatibility change" and `0.X.Y` as "everything else". Don't reach for `1.0.0` without a separate conversation about API stability.

### MAJOR bump (`0.X.0` → `(X+1).0.0`, rare pre-1.0)
- Changed `.vbsql` on-disk format such that old files can't be read by the new build (catalog format, page layout, MVCC visibility metadata)
- Removed a SQL feature or changed semantics so valid queries now error or return different results
- Removed/renamed a public Rust API on a published crate (`vibesql-parser`, `vibesql-types`, `vibesql-storage`, `vibesql-executor`, `vibesql-server`, `vibesql-cli`, `vibesql-wasm-bindings`, `vibesql-l10n`)
- Removed/renamed a Python binding (`vibesql` PyPI package public surface)
- Changed the server wire protocol incompatibly
- Changed CLI flag names or default behavior incompatibly

### MINOR bump (`0.X.Y` → `0.(X+1).0`)
- New SQL syntax (e.g., the recent `VACUUM` / `VACUUM INTO`)
- New optimizer pass or join algorithm
- New public Rust API on a published crate
- New Python or WASM binding
- New benchmark suite or significant observability surface
- New storage subsystem capability (MVCC visibility filtering, on-demand vacuum, etc.)

### PATCH bump (`0.X.Y` → `0.X.(Y+1)`)
- Bug fixes that don't change the public contract
- Performance improvements with no semantic change
- Internal refactors with no public-API change
- Documentation, CLAUDE.md, README updates
- Dependency bumps (`chore(deps):`)
- Test additions, fuzzer/miri fixes
- Web demo updates

## CHANGELOG Style (Phase 4 OVERRIDE — vibesql diverges from Keep-a-Changelog)

The default skill enforces Keep-a-Changelog format (`### Added`, `### Changed`, `### Fixed`). **VibeSQL does NOT use this format.** Override Phase 4's section structure:

- `## [X.Y.Z] - YYYY-MM-DD` header with today's date
- **Theme paragraph** immediately after the header — 1-3 sentences naming the release theme and headline numbers ("N commits since X.Y.(Z-1)", pass-rate milestones, etc.)
- **Top-level sections by THEME, not by Added/Changed/Fixed**: `### Performance`, `### SQL Compatibility`, `### MVCC`, `### Storage`, `### Parser`, `### Optimizer`, `### Bug Fixes`, `### Infrastructure`, `### Documentation`
- **Sub-sections** for major initiatives: `#### Phase N: <Name>`
- Reference issues/PRs with `(#NNNN)` format
- Concise but informative bullet points — feature name in bold, then short description

For a release with 100+ commits, expect a 100+-line entry. That's normal. Read `head -200 CHANGELOG.md` to anchor on style.

## Apply Changes (Phase 5 — scripts/version.sh interface gap)

**KNOWN GAP**: `./scripts/version.sh` currently exposes `set X.Y.Z [--tag]` but **not** the `list` / `bump <level>` subcommands the v0.10.3 default skill expects. Until version.sh is extended, the Phase 5 dispatch will print `unknown command: list` / `unknown command: bump`. Workaround:

1. The operator manually runs `./scripts/version.sh set <X.Y.Z>` (which handles the workspace.package version + internal `vibesql-*` dependency pins across all crates).
2. **Then** the operator manually bumps the two pyproject.toml files (version.sh does not touch them):
   ```bash
   sed -i '' 's/^version = ".*"/version = "<X.Y.Z>"/' pyproject.toml
   sed -i '' 's/^version = ".*"/version = "<X.Y.Z>"/' crates/vibesql-python-bindings/pyproject.toml
   ```
3. Refresh Cargo.lock: `cargo update -w`
4. Verify all four sources agree (see Phase 1 augmentation above).
5. Commit + tag using the snippet at the bottom of the default skill's Phase 5.

**DO NOT bump** `crates/vibesql-sqllogictest/Cargo.toml` (pinned to upstream sqllogictest's `0.28.x`) or `web-demo/package.json` (tracks its own version independently). If the operator wants to bump those, that's a separate decision.

## Push & Registry Publishes (Phase 6 OVERRIDE — dual-workflow gate)

The default skill's Phase 6 assumes ONE optional release workflow (`release.yml`). **VibeSQL has TWO** release workflows that both fire on `push: tags: ['v*']`:

- `.github/workflows/release-crates.yml` — publishes workspace crates to **crates.io**
- `.github/workflows/release-pypi.yml` — builds wheels on Linux/macOS/Windows and publishes `vibesql` to **PyPI**

Both publishes are **public and irreversible** — crates.io and PyPI do not allow re-publishing the same version. If a workflow fails mid-flight, the operator may need to yank the version and cut `X.Y.(Z+1)`.

### Explicit operator confirmation (REQUIRED)

Before pushing the tag, ask the operator literally:

> Pushing tag v<X.Y.Z> will trigger automatic publishes to **crates.io** (workspace crates) and **PyPI** (`vibesql` Python package). Both publishes are irreversible. Push? (y/N)

Do NOT proceed without an explicit "yes".

### Branch-protection fallback

If branch protection on `main` blocks direct push of the release commit:

1. Push the bump commit to a feature branch: `git push origin HEAD:chore/release-v<X.Y.Z>`
2. Open a PR, get it merged.
3. After merge, locally check out the new `main` HEAD and **re-tag** if the commit SHA changed (`git tag -d v<X.Y.Z>` then `git tag -a v<X.Y.Z>` against the new HEAD).
4. Push the tag from main: `git push origin v<X.Y.Z>`.

### Monitor BOTH workflows before continuing

```bash
gh run list --workflow release-crates.yml --limit 1
gh run list --workflow release-pypi.yml --limit 1
gh run watch <run-id>   # optional, for each
```

**Wait for BOTH to complete successfully** before creating the GitHub Release (override of the default's "create release immediately" behavior). If either fails, stop and triage with the operator — do NOT create the GitHub Release on top of a partial publish.

## GitHub Release (Phase 7 OVERRIDE — separate phase after registry success)

After both registry workflows succeed, create the GitHub Release with the CHANGELOG block as release notes:

```bash
NEW_VERSION=<X.Y.Z>
notes=$(awk '/^## \['"$NEW_VERSION"'\]/{flag=1; next} /^## \[/{flag=0} flag' CHANGELOG.md)

gh release create "v$NEW_VERSION" \
  --title "v$NEW_VERSION" \
  --notes "$notes"
```

The `release-crates.yml` workflow does NOT create the GitHub Release on its own (it only verifies the tag matches `Cargo.toml`'s version and publishes). The GitHub Release is the canonical human-facing announcement and is created here.

No build artifacts are attached — both registry mirrors are the binary distribution. The release is source + the published crates + the published wheels.

## Post-Release Follow-ups

Add these to the operator hand-off:
- `make website` + commit the updated web-demo data so the live demo reflects the new release.
- `wrangler deploy` from main to push the website (Cloudflare).
- Open any tracking issue for follow-up cleanup found during CHANGELOG drafting.

## Important Notes (VibeSQL-specific)

- **Four version-bearing files, propagation pattern**: only `[workspace.package]` in `Cargo.toml` carries the canonical Rust version; every member crate inherits via `version.workspace = true`. The exception is `crates/vibesql-sqllogictest/Cargo.toml`, which pins to upstream sqllogictest's `0.28.x` — do not touch it.
- **`Cargo.lock` IS a version-bearing file**: every `vibesql-*` workspace crate has an entry in `Cargo.lock`. `cargo update -w` is required after bumping `Cargo.toml` — it is not a no-op.
- **Web demo and dashboard data are separate**: `make website` + `wrangler deploy` are NOT part of the release tag flow — they're done after to refresh the public dashboard.
- **Do not auto-publish to extra registries.** This skill only triggers what the existing workflows do. If the operator later adds a Homebrew formula, npm package, etc., that's a separate manual step.
