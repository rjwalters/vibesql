# Release Manager

You are preparing a release of **VibeSQL** from the {{workspace}} repository.

## Overview

This skill guides a careful, interactive release process. Every release must:
1. Verify CI is green on main
2. Analyze what changed since the last release (typically 100+ commits)
3. Help the user decide the correct semver bump
4. Draft and refine the CHANGELOG entry
5. Update version across all four version-bearing files (workspace `Cargo.toml`, `Cargo.lock`, root `pyproject.toml`, `crates/vibesql-python-bindings/pyproject.toml`)
6. Commit, tag, and (with confirmation) push — **pushing the tag triggers automatic publishes to crates.io AND PyPI**
7. Create a GitHub Release with the CHANGELOG entry as the release notes
8. Verify the two registry workflows succeeded

**Do not rush. Each phase requires user confirmation before proceeding.**

**Critical**: this is NOT the generic `/loom:bump` skill. `/loom:bump` works for any project shape and generates a `scripts/version.sh` helper. This skill is VibeSQL-specific and knows about the four version files, the `version.workspace = true` propagation pattern, the two release workflows, and VibeSQL's CHANGELOG conventions.

## Phase 1: Pre-flight Checks

Before starting, verify the release is safe to cut:

```bash
# CI status on main
gh run list --branch main --limit 10 --json name,conclusion,status --jq '.[] | "\(.status)/\(.conclusion // "-"): \(.name)"'

# Open PRs that might need to land first
gh pr list --state open --limit 20 --json number,title,labels --jq '.[] | "#\(.number) \(.title)"'

# Uncommitted changes
git status

# All four version-bearing files agree
grep -m1 '^version' Cargo.toml
grep -m1 '^version' pyproject.toml
grep -m1 '^version' crates/vibesql-python-bindings/pyproject.toml
grep -A1 '^name = "vibesql"' Cargo.lock | grep '^version' | sort -u
```

Present findings to the user. Stop if:
- `ci-main.yml` or `ci-extended.yml` is failing on main → fix first.
- The four version sources disagree → resolve drift first.
- High-priority open PRs (`loom:urgent`, `loom:champion-approved` near merge, anything user flags) should land first → ask.

`fuzz.yml` and `miri.yml` running long or red are NOT release blockers on their own — note them but ask the operator.

## Phase 2: Gather Changes

```bash
# Last release tag
LAST=$(git tag --sort=-v:refname | head -1)
echo "Last release: $LAST"

# Current workspace version
grep -A1 '^\[workspace.package\]' Cargo.toml | grep '^version'

# All commits since last tag
git log "$LAST..HEAD" --oneline | wc -l
git log "$LAST..HEAD" --oneline

# High-level file change stats
git diff "$LAST..HEAD" --stat | tail -1

# Group commits by conventional-commit scope (vibesql convention)
git log "$LAST..HEAD" --pretty=format:'%s' \
  | grep -oE '^(feat|fix|chore|refactor|test|docs|perf)\([^)]+\)' \
  | sort | uniq -c | sort -rn | head -30
```

VibeSQL release cycles are commit-heavy — the `[0.1.4]` entry triaged 878 commits. Expect a substantial Phase 4 and warn the user upfront if `git log` returns >100 commits.

Present:
- **Last release**: tag name, date, and version (`git for-each-ref --format='%(refname:short) %(taggerdate:short)' refs/tags/$LAST`)
- **Commits since release**: total count
- **By conventional-commit scope**: e.g. `feat(mvcc): 12`, `feat(parser): 8`, `fix(executor): 6` — this is the spine of the CHANGELOG
- **Subsystems touched**: from the diff stats, group by crate (`crates/vibesql-*`), `scripts/`, `web-demo/`, `docs/`

If there are zero commits since the last tag, stop and tell the user there's nothing to release.

## Phase 3: Semver Decision

Present a semver analysis. Reference https://semver.org. VibeSQL is pre-1.0, so MAJOR/MINOR semantics are looser than usual — treat `0.X.0` as "meaningful capability or compatibility change" and `0.X.Y` as "everything else".

### Breaking Changes (MAJOR bump — `0.X.0` → `(X+1).0.0`, rare pre-1.0)
- Changed `.vbsql` on-disk format in a way old files can't be read by the new build (catalog format, page layout, MVCC visibility metadata, etc.)
- Removed a SQL feature or changed semantics in a way valid queries now error or return different results
- Removed or renamed a public Rust API on a published crate (`vibesql-parser`, `vibesql-types`, `vibesql-storage`, `vibesql-executor`, `vibesql-server`, `vibesql-cli`, `vibesql-wasm-bindings`, `vibesql-l10n`)
- Removed or renamed a Python binding (`vibesql` PyPI package public surface)
- Changed the server wire protocol incompatibly
- Changed CLI flag names or default behavior incompatibly

### New Capabilities (MINOR bump — `0.X.Y` → `0.(X+1).0`)
- New SQL syntax accepted by the parser (e.g., the recent `VACUUM` / `VACUUM INTO`)
- New optimizer pass or join algorithm (e.g., morsel-driven parallel execution)
- New public Rust API exported from a published crate
- New Python or WASM binding exposed
- New benchmark suite or significant new observability surface
- New storage subsystem capability (e.g., MVCC visibility filtering, on-demand vacuum)

### Bug Fixes / Internal (PATCH bump — `0.X.Y` → `0.X.(Y+1)`)
- Bug fixes that don't change the public contract
- Performance improvements with no semantic change
- Internal refactors with no public-API change
- Documentation, CLAUDE.md, README updates
- Dependency bumps (`chore(deps):`)
- Test additions, fuzzer/miri fixes
- Web demo updates

Present your recommendation and **ask the user to confirm or override**. Do not proceed until confirmed. You will need the explicit `X.Y.Z` string for Phase 5.

## Phase 4: Draft CHANGELOG

Draft a CHANGELOG entry following the rich format established by the `[0.1.4]` entry. Read it directly with `head -200 CHANGELOG.md` and match the style — VibeSQL's entries are denser than the standard Keep-a-Changelog format.

VibeSQL CHANGELOG conventions (from `[0.1.3]` and `[0.1.4]`):
- `## [X.Y.Z] - YYYY-MM-DD` header with today's date
- **Theme paragraph** immediately after the header — 1–3 sentences naming the release theme and headline numbers ("N commits since X.Y.(Z-1)", pass-rate milestones, etc.)
- **Top-level sections by theme**, not just Added/Changed/Fixed: `### Performance`, `### SQL Compatibility`, `### MVCC`, `### Storage`, `### Parser`, `### Optimizer`, `### Bug Fixes`, `### Infrastructure`, `### Documentation`
- **Sub-sections** for major initiatives: `#### Phase N: <Name>`
- Reference issues/PRs with `(#NNNN)` format
- Concise but informative bullet points — feature name in bold, then short description

For a release with 100+ commits, expect the entry to be 100+ lines. That's normal.

Workflow:
1. Read the latest entry to anchor on style.
2. Walk the commit list grouped by scope (from Phase 2's analysis).
3. Cluster commits into themes — these become the `###` sections.
4. Identify the headline 2–4 themes for the opening paragraph.
5. Draft, present to user, iterate. Iterate until approved.

## Phase 5: Apply Changes

Once the user approves CHANGELOG and version, apply both as a single combined release commit, then tag.

Let `<X.Y.Z>` be the explicit version confirmed in Phase 3.

1. **Update `CHANGELOG.md`**: insert the new entry directly below the file header (above the previous `## [X.Y.(Z-1)]` entry).

2. **Bump version in workspace `Cargo.toml`** (this propagates to all `version.workspace = true` member crates automatically):
   ```bash
   awk -v ver="<X.Y.Z>" '
     /^\[workspace.package\]/ { in_wp=1 }
     /^\[/ && !/^\[workspace.package\]/ { in_wp=0 }
     in_wp && /^version = "/ { print "version = \"" ver "\""; next }
     { print }
   ' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml
   ```

3. **Bump version in root `pyproject.toml`**:
   ```bash
   sed -i '' 's/^version = ".*"/version = "<X.Y.Z>"/' pyproject.toml
   ```
   (On Linux: `sed -i 's/...'`.)

4. **Bump version in `crates/vibesql-python-bindings/pyproject.toml`**:
   ```bash
   sed -i '' 's/^version = ".*"/version = "<X.Y.Z>"/' crates/vibesql-python-bindings/pyproject.toml
   ```

5. **Refresh `Cargo.lock`** so every `vibesql-*` entry picks up the new version:
   ```bash
   cargo update -w
   ```

6. **Verify all four sources agree**:
   ```bash
   grep -m1 '^version' Cargo.toml                                        # workspace.package
   grep -m1 '^version' pyproject.toml
   grep -m1 '^version' crates/vibesql-python-bindings/pyproject.toml
   grep -A1 '^name = "vibesql-storage"' Cargo.lock | grep '^version'     # spot-check
   ```
   All four MUST report `<X.Y.Z>`. If `Cargo.lock` still shows the old version for vibesql crates, the `cargo update -w` step is required — it is not a no-op.

7. **DO NOT bump** `crates/vibesql-sqllogictest/Cargo.toml` (pinned to upstream sqllogictest's `0.28.x`) or `web-demo/package.json` (tracks its own version independently). If the user wants to bump those, that's a separate decision — confirm explicitly before touching either.

8. **Commit CHANGELOG + version bumps together**:
   ```bash
   git add CHANGELOG.md Cargo.toml Cargo.lock pyproject.toml crates/vibesql-python-bindings/pyproject.toml
   git commit -m "chore: release v<X.Y.Z>"
   ```

9. **Tag** (annotated, NOT pushed yet):
   ```bash
   git tag -a v<X.Y.Z> -m "v<X.Y.Z>"
   ```

Show the user `git show HEAD --stat`, the tag (`git tag --list v<X.Y.Z>`), and the CHANGELOG diff. Ask for final confirmation before Phase 6.

## Phase 6: Push and Trigger Registry Publishes

**STOP. Read this whole phase before pushing.**

Pushing the tag fires TWO GitHub Actions workflows automatically:
- `.github/workflows/release-crates.yml` — publishes the workspace crates to **crates.io**
- `.github/workflows/release-pypi.yml` — builds wheels on Linux/macOS/Windows and publishes `vibesql` to **PyPI**

Both publishes are **public and irreversible** — crates.io and PyPI do not allow re-publishing the same version. If the workflows fail mid-flight, the operator may need to yank versions and cut `X.Y.(Z+1)`. This is much more consequential than the anvil flow.

Explicitly confirm with the operator:

> Pushing tag v<X.Y.Z> will trigger automatic publishes to **crates.io** (workspace crates) and **PyPI** (`vibesql` Python package). Both publishes are irreversible. Push? (y/N)

Do NOT proceed without an explicit "yes".

If branch protection on `main` requires PRs, the direct push of the release commit will fail. Fallback flow:
1. Push the bump commit to a feature branch: `git push origin HEAD:chore/release-v<X.Y.Z>`
2. Open a PR, get it merged.
3. After merge, locally check out the new `main` HEAD and re-tag if the commit SHA changed.
4. Push the tag from main: `git push origin v<X.Y.Z>`.

Normal flow (direct push allowed):
```bash
git push origin main
git push origin v<X.Y.Z>
```

Immediately after pushing, monitor the workflows:
```bash
gh run list --workflow release-crates.yml --limit 1
gh run list --workflow release-pypi.yml --limit 1
gh run watch <run-id>   # optional, for each
```

Wait for BOTH to complete before Phase 7. If either fails, stop and triage with the operator — do NOT create the GitHub Release on top of a partial publish.

## Phase 7: Create the GitHub Release

After both registry workflows succeed:

```bash
# Extract the just-promoted block from CHANGELOG.md as release notes
NEW_VERSION=<X.Y.Z>
notes=$(awk '/^## \['"$NEW_VERSION"'\]/{flag=1; next} /^## \[/{flag=0} flag' CHANGELOG.md)

gh release create "v$NEW_VERSION" \
  --title "v$NEW_VERSION" \
  --notes "$notes"
```

The `release-crates.yml` workflow does NOT create the GitHub Release on its own (it only verifies the tag matches `Cargo.toml`'s version and publishes). The GitHub Release is the canonical human-facing announcement and is created here.

No build artifacts are attached — both registry mirrors are the binary distribution. The release is source + the published crates + the published wheels.

## Phase 8: Post-Release Summary

Present a summary:

```
## Release v<X.Y.Z> complete

- Commit:           <sha>
- Tag:              v<X.Y.Z>
- crates.io:        published (N crates) — <link to release-crates run>
- PyPI:             published (M wheels)   — <link to release-pypi run>
- GitHub Release:   created — <release URL>
- CHANGELOG:        updated with N items
- Version files:    4 files updated (Cargo.toml, Cargo.lock, pyproject.toml, crates/vibesql-python-bindings/pyproject.toml)
- Commits triaged:  <N commits since v<X.Y.(Z-1)>>
```

Then suggest the typical follow-ups:
- `make website` + commit the updated web-demo data so the live demo reflects the new release.
- `wrangler deploy` from main to push the website (Cloudflare).
- Open any tracking issue for follow-up cleanup found during CHANGELOG drafting.

## Important Notes

- **Four version-bearing files, propagation pattern**: only the `[workspace.package]` block in `Cargo.toml` carries the canonical Rust version; every member crate inherits via `version.workspace = true`. The exception is `crates/vibesql-sqllogictest/Cargo.toml`, which pins itself to upstream sqllogictest's version (currently `0.28.x`) — do not touch it.
- **`Cargo.lock` IS a version-bearing file**: every `vibesql-*` workspace crate also has an entry in `Cargo.lock`. `cargo update -w` is required after bumping `Cargo.toml` — it is not a no-op.
- **Two registry workflows fire on tag push**: `release-crates.yml` and `release-pypi.yml`. Both are triggered by `push: tags: ['v*']`. Verify both succeeded before creating the GitHub Release.
- **Pre-1.0 semver is looser**: VibeSQL is at `0.1.x` — `0.X.0` bumps mean "meaningful capability or compatibility change", `0.X.Y` means "everything else". Don't reach for `1.0.0` on this skill without a separate conversation about API stability.
- **VibeSQL releases are large**: cycles routinely accumulate 100+ commits before a cut. Phase 4 is the bottleneck — budget time for it.
- **Conventional commits**: VibeSQL uses conventional commit prefixes (`feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`, `perf:`), almost always scoped (`feat(mvcc):`, `fix(parser):`, `chore(deps):`).
- **Branch protection**: if direct pushes to `main` are blocked, use the feature-branch fallback in Phase 6. Tags can be created and pushed AFTER the PR merges.
- **Web demo and dashboard data are separate**: `make website` + the `wrangler deploy` step are NOT part of the release tag flow — they're done after the release to refresh the public dashboard.
- **Do not auto-publish to extra registries.** This skill only triggers what the existing workflows do. If the operator later adds a Homebrew formula, npm package, etc., that's a separate manual step.
