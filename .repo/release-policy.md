# Release policy — VibeSQL

Procedural release steps bound to `/repo:release` seams. This file is loaded and
validated by `/repo:release` **Phase 0** (see `.claude/commands/repo/release.md`,
"Extension points — per-project release policy"). Every section below **augments**
the corresponding phase's default action — nothing here replaces a built-in step.

Advisory release context (version-bearing files, semver categories,
`scripts/version.sh` notes) lives in `.loom/context/topics/release.md`, which is
injected as conversation context; this file is only for steps that must *run* at
a named phase boundary.

## seam: pre-flight

CI gating policy — not every workflow is release-blocking:

- **Blocking** when failing on main: `ci-main.yml`, `ci-extended.yml` → fix first.
- **NOT blocking** (note them, but ask the operator before letting them stop a
  release): `fuzz.yml`, `miri.yml`. Long runs and intermittent reds are normal.

## seam: pre-changelog-style

VibeSQL does NOT use Keep-a-Changelog's "Added/Changed/Fixed/Removed" grouping.
Draft the CHANGELOG entry in VibeSQL's themed-section convention instead:

- `## [X.Y.Z] - YYYY-MM-DD` header with today's date.
- **Theme paragraph** immediately after the header — 1-3 sentences naming the
  release theme and headline numbers ("N commits since X.Y.(Z-1)", pass-rate
  milestones, etc.).
- **Top-level sections by THEME**, not by Added/Changed/Fixed: `### Performance`,
  `### SQL Compatibility`, `### MVCC`, `### Storage`, `### Parser`,
  `### Optimizer`, `### Bug Fixes`, `### Infrastructure`, `### Documentation`.
- **Sub-sections** for major initiatives: `#### Phase N: <Name>`.
- Reference issues/PRs with `(#NNNN)` format. Feature name in bold, then short
  description.
- For a release with 100+ commits, expect a 100+-line entry. That's normal —
  read `head -200 CHANGELOG.md` to anchor on style (see `[0.1.4]`).

## seam: pre-push

Before pushing the tag, prompt the operator literally with the irreversibility
warning:

> Pushing tag `v<X.Y.Z>` will trigger automatic publishes to **crates.io**
> (workspace crates) and **PyPI** (`vibesql` Python package). Both publishes are
> **irreversible** — crates.io and PyPI do not allow re-publishing the same
> version. Push? (y/N)

Do NOT proceed without an explicit "yes".

If branch protection on `main` blocks the direct push:

1. Push the bump commit to a feature branch: `git push origin HEAD:chore/release-v<X.Y.Z>`
2. Open a PR, get it merged.
3. After merge, check out the new `main` HEAD and re-tag if the commit SHA
   changed (`git tag -d v<X.Y.Z>` then `git tag -a v<X.Y.Z>`).
4. Push the tag from main: `git push origin v<X.Y.Z>`.

## seam: post-push

After pushing the tag, TWO release workflows fire from `push: tags: ['v*']`:

- `.github/workflows/release-crates.yml` → publishes workspace crates to crates.io
- `.github/workflows/release-pypi.yml` → builds wheels (Linux/macOS/Windows) and
  publishes `vibesql` to PyPI

Poll BOTH workflows for completion before continuing to `pre-github-release`:

```bash
gh run list --workflow release-crates.yml --limit 1 --json status,conclusion
gh run list --workflow release-pypi.yml --limit 1 --json status,conclusion
```

Optional: `gh run watch <run-id>` for live progress. Time out after 30 minutes
and ask the operator. If either workflow fails, stop and triage — do NOT proceed
to GitHub Release creation on top of a partial publish.

## seam: pre-github-release

Do NOT run `gh release create` until both workflows from `post-push` report
`success`. Compose the release notes from the just-promoted CHANGELOG block:

```bash
NEW_VERSION=<X.Y.Z>
notes=$(awk '/^## \['"$NEW_VERSION"'\]/{flag=1; next} /^## \[/{flag=0} flag' CHANGELOG.md)

gh release create "v$NEW_VERSION" \
  --title "v$NEW_VERSION" \
  --notes "$notes"
```

The `release-crates.yml` workflow does NOT create the GitHub Release on its own
(it only verifies the tag matches `Cargo.toml`'s version and publishes). The
GitHub Release is the canonical human-facing announcement and is created here.
No build artifacts are attached — both registry mirrors are the binary
distribution.

## seam: post-summary

Append these VibeSQL-specific follow-ups to the operator hand-off:

- `make website` + commit the updated web-demo data so the live demo reflects
  the new release.
- `wrangler deploy` from main to push the website (Cloudflare).
- Open any tracking issue for follow-up cleanup found during CHANGELOG drafting.
