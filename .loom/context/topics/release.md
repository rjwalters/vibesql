# VibeSQL Release Conventions

Project-specific context injected by `methodology-inject.sh` whenever `/loom:release` is invoked. This **augments** `.claude/commands/loom/release.md` (the loom-default v0.10.4 skill); read both.

Procedural overrides target the named extension points the default skill exposes (see its "Operator extension points" section for the authoritative list of seams).

## Advisory reminders (any phase)

### Pre-flight: four version-bearing files must agree

VibeSQL has four version-bearing files. The default's Phase 2a tool detection finds `./scripts/version.sh` first; `scripts/version.sh check` verifies the workspace Cargo files but does NOT check the two pyproject files. Run this Phase 1 sanity check in addition to whatever the default does:

```bash
grep -m1 '^version' Cargo.toml
grep -m1 '^version' pyproject.toml
grep -m1 '^version' crates/vibesql-python-bindings/pyproject.toml
grep -A1 '^name = "vibesql"' Cargo.lock | grep '^version' | sort -u
```

All four must report the same version. Stop and resolve drift before bumping.

### Pre-flight: CI gating policy

Not every workflow is release-blocking:

- **Blocking** when failing on main: `ci-main.yml`, `ci-extended.yml` → fix first.
- **NOT blocking** (note them, but ask the operator before letting them stop a release): `fuzz.yml`, `miri.yml`. Long runs and intermittent reds are normal.

### Phase 2: commit-volume expectation

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

### Phase 5: scripts/version.sh interface gap

VibeSQL's `scripts/version.sh` exposes `set X.Y.Z [--tag]` and `check`, but **not** the `bump <level>` / `list` subcommands the v0.10.4 default's Phase 5 dispatches to. Workaround until version.sh is extended:

1. Operator manually runs `./scripts/version.sh set <X.Y.Z>` (handles workspace.package version + internal `vibesql-*` dependency pins).
2. Operator manually bumps the two pyproject files (version.sh doesn't touch them):
   ```bash
   sed -i '' 's/^version = ".*"/version = "<X.Y.Z>"/' pyproject.toml
   sed -i '' 's/^version = ".*"/version = "<X.Y.Z>"/' crates/vibesql-python-bindings/pyproject.toml
   ```
3. Refresh Cargo.lock: `cargo update -w`
4. Verify all four sources agree (see "Pre-flight" above).
5. Commit + tag (the snippet at the end of the default's Phase 5).

**DO NOT bump** `crates/vibesql-sqllogictest/Cargo.toml` (pinned to upstream sqllogictest's `0.28.x`) or `web-demo/package.json` (independent version line). Confirm explicitly if the operator wants those changed.

## Procedural overrides at named seams

### At extension point `pre-changelog-style`

VibeSQL does NOT use Keep-a-Changelog's "Added/Changed/Fixed/Removed" grouping. Override Phase 4's default style with VibeSQL's themed-section convention:

- `## [X.Y.Z] - YYYY-MM-DD` header with today's date.
- **Theme paragraph** immediately after the header — 1-3 sentences naming the release theme and headline numbers ("N commits since X.Y.(Z-1)", pass-rate milestones, etc.).
- **Top-level sections by THEME**, not by Added/Changed/Fixed: `### Performance`, `### SQL Compatibility`, `### MVCC`, `### Storage`, `### Parser`, `### Optimizer`, `### Bug Fixes`, `### Infrastructure`, `### Documentation`.
- **Sub-sections** for major initiatives: `#### Phase N: <Name>`.
- Reference issues/PRs with `(#NNNN)` format. Feature name in bold, then short description.
- For a release with 100+ commits, expect a 100+-line entry. That's normal — read `head -200 CHANGELOG.md` to anchor on style (see `[0.1.4]`).

### At extension point `pre-push`

Before pushing the tag, prompt the operator literally with the irreversibility warning:

> Pushing tag `v<X.Y.Z>` will trigger automatic publishes to **crates.io** (workspace crates) and **PyPI** (`vibesql` Python package). Both publishes are **irreversible** — crates.io and PyPI do not allow re-publishing the same version. Push? (y/N)

Do NOT proceed without an explicit "yes".

If branch protection on `main` blocks the direct push:
1. Push the bump commit to a feature branch: `git push origin HEAD:chore/release-v<X.Y.Z>`
2. Open a PR, get it merged.
3. After merge, check out the new `main` HEAD and re-tag if the commit SHA changed (`git tag -d v<X.Y.Z>` then `git tag -a v<X.Y.Z>`).
4. Push the tag from main: `git push origin v<X.Y.Z>`.

### At extension point `post-push`

After pushing the tag, TWO release workflows fire from `push: tags: ['v*']`:
- `.github/workflows/release-crates.yml` → publishes workspace crates to crates.io
- `.github/workflows/release-pypi.yml` → builds wheels (Linux/macOS/Windows) and publishes `vibesql` to PyPI

Poll BOTH workflows for completion before continuing to `pre-github-release`:

```bash
gh run list --workflow release-crates.yml --limit 1 --json status,conclusion
gh run list --workflow release-pypi.yml --limit 1 --json status,conclusion
```

Optional: `gh run watch <run-id>` for live progress. Time out after 30 minutes and ask the operator. If either workflow fails, stop and triage — do NOT proceed to GitHub Release creation on top of a partial publish.

### At extension point `pre-github-release`

Do NOT run `gh release create` until both workflows from `post-push` report `success`. Compose the release notes from the just-promoted CHANGELOG block:

```bash
NEW_VERSION=<X.Y.Z>
notes=$(awk '/^## \['"$NEW_VERSION"'\]/{flag=1; next} /^## \[/{flag=0} flag' CHANGELOG.md)

gh release create "v$NEW_VERSION" \
  --title "v$NEW_VERSION" \
  --notes "$notes"
```

The `release-crates.yml` workflow does NOT create the GitHub Release on its own (it only verifies the tag matches `Cargo.toml`'s version and publishes). The GitHub Release is the canonical human-facing announcement and is created here. No build artifacts are attached — both registry mirrors are the binary distribution.

### At extension point `post-summary`

Append these VibeSQL-specific follow-ups to the operator hand-off:

- `make website` + commit the updated web-demo data so the live demo reflects the new release.
- `wrangler deploy` from main to push the website (Cloudflare).
- Open any tracking issue for follow-up cleanup found during CHANGELOG drafting.

## Important Notes (VibeSQL-specific)

- **Four version-bearing files, propagation pattern**: only `[workspace.package]` in `Cargo.toml` carries the canonical Rust version; every member crate inherits via `version.workspace = true`. The exception is `crates/vibesql-sqllogictest/Cargo.toml`, which pins to upstream sqllogictest's `0.28.x` — do not touch it.
- **`Cargo.lock` IS a version-bearing file**: every `vibesql-*` workspace crate has an entry in `Cargo.lock`. `cargo update -w` is required after bumping `Cargo.toml` — it is not a no-op.
- **Web demo and dashboard data are separate**: `make website` + `wrangler deploy` are NOT part of the release tag flow — they run after to refresh the public dashboard.
- **Do not auto-publish to extra registries.** This skill only triggers what the existing workflows do. If the operator later adds a Homebrew formula, npm package, etc., that's a separate manual step.
