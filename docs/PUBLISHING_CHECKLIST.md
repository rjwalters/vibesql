# VibeSQL Release Guide

This document describes the release process for VibeSQL across all distribution channels.

## Distribution Channels

| Channel | Packages | Current Version |
|---------|----------|-----------------|
| **crates.io** | 11 Rust crates | 0.1.2 |
| **npm** | `@vibesql/client`, `@vibesql/drizzle` | 0.1.2 |
| **PyPI** | `vibesql` (Python bindings) | 0.1.2 |

## Pre-Release Checklist

### Code Quality

- [ ] All tests pass: `cargo test --release`
- [ ] No clippy warnings: `cargo clippy --all-targets`
- [ ] Documentation builds: `cargo doc --no-deps`
- [ ] Examples compile: `cargo build --examples`

### Version Updates

- [ ] Update version in root `Cargo.toml` (workspace.package.version)
- [ ] Update `packages/vibesql-client-ts/package.json`
- [ ] Update `packages/vibesql-drizzle/package.json`
- [ ] Update `crates/vibesql-python-bindings/pyproject.toml`
- [ ] Update CHANGELOG.md with release notes

### Documentation

- [ ] CHANGELOG.md has entry for new version
- [ ] README.md is up to date
- [ ] Any new features are documented

## Release Process

### Step 1: Prepare the Release

```bash
# Ensure clean working tree
git status

# Run full test suite
cargo test --release

# Dry-run crates.io publish to catch issues
./scripts/publish-crates.sh
```

### Step 2: Update Versions

Edit `Cargo.toml` (root):
```toml
[workspace.package]
version = "0.1.3"  # Update this
```

Edit `packages/vibesql-client-ts/package.json`:
```json
{
  "version": "0.1.3"
}
```

Edit `packages/vibesql-drizzle/package.json`:
```json
{
  "version": "0.1.3"
}
```

Edit `crates/vibesql-python-bindings/pyproject.toml`:
```toml
[project]
version = "0.1.3"
```

### Step 3: Update CHANGELOG

Add entry to CHANGELOG.md:
```markdown
## [0.1.3] - YYYY-MM-DD

### Added
- ...

### Changed
- ...

### Fixed
- ...
```

### Step 4: Commit and Tag

```bash
git add -A
git commit -m "Release v0.1.3"
git tag -a v0.1.3 -m "Release v0.1.3"
git push origin main
git push origin v0.1.3
```

### Step 5: Publish to crates.io

**First time setup:**
```bash
# Get API token from https://crates.io/settings/tokens
cargo login <token>
```

**Publish:**
```bash
./scripts/publish-crates.sh --publish
```

This publishes 11 crates in dependency order:
1. vibesql-types
2. vibesql-ast
3. vibesql-catalog
4. vibesql-parser
5. vibesql-storage
6. vibesql-executor
7. vibesql (main crate)
8. vibesql-cli
9. vibesql-server
10. vibesql-wasm-bindings
11. vibesql-python-bindings

Note: `vibesql-sqllogictest` is not published (test harness only).

The script handles:
- Temporarily disabling `[patch.crates-io]` section
- Using `--no-verify` to bypass workspace verification issues
- Handling "already published" errors gracefully
- 10-second delays between crates for indexing

### Step 6: Publish to npm

**First time setup:**
```bash
# Login to npm
npm login
# Or for scoped packages with 2FA:
npm login --scope=@vibesql
```

**Publish TypeScript client:**
```bash
cd packages/vibesql-client-ts
pnpm build
npm publish --access public
```

**Publish Drizzle adapter:**
```bash
cd packages/vibesql-drizzle
pnpm build
npm publish --access public
```

### Step 7: Publish to PyPI (Automatic)

PyPI publishing is **automated** via GitHub Actions with trusted publishing.

**Trigger:** The `release-pypi.yml` workflow runs automatically when you push a version tag (`v*`).

**What it does:**
- Builds wheels for 5 platforms:
  - Linux x86_64 and aarch64
  - macOS Intel (x86_64) and Apple Silicon (aarch64)
  - Windows x64
- Builds source distribution
- Publishes to PyPI using OIDC trusted publishing (no tokens needed)

**Manual trigger (if needed):**
```bash
gh workflow run release-pypi.yml --ref main
```

**First time setup (already done):**
1. Configure pending publisher at https://pypi.org/manage/account/publishing/
2. Create GitHub environment named `pypi`

### Step 8: Create GitHub Release

```bash
gh release create v0.1.3 --title "v0.1.3" --notes-from-tag
```

Or manually:
1. Go to: https://github.com/rjwalters/vibesql/releases/new
2. Select tag: `v0.1.3`
3. Title: `v0.1.3`
4. Copy release notes from CHANGELOG.md
5. Publish release

## Post-Release

- [ ] Verify crates appear on crates.io
- [ ] Verify packages appear on npmjs.com
- [ ] Verify `vibesql` appears on pypi.org
- [ ] Verify docs.rs builds documentation
- [ ] Update any version badges in README
- [ ] Announce release (if applicable)

## Troubleshooting

### crates.io publish fails

**"crate already exists"**: You cannot re-publish the same version. Bump the version number.

**Dependency not found**: Crates must be published in dependency order. Wait 10-30 seconds between publishes (the script handles this).

**Authentication error**: Run `cargo login <token>` with a fresh token.

**"sqllogictest cannot be published"**: The publish script handles this by temporarily disabling the `[patch.crates-io]` section.

### npm publish fails

**"You must be logged in"**: Run `npm login`

**"403 Forbidden"**: For scoped packages (@vibesql/*), use `npm publish --access public`

**"Version already exists"**: Bump the version in package.json

### PyPI publish fails

**Workflow not triggered**: Ensure you pushed a tag matching `v*` pattern.

**OIDC authentication failed**: Check that:
- Trusted publisher is configured at https://pypi.org/manage/account/publishing/
- Workflow name matches exactly: `release-pypi.yml`
- Repository owner/name match: `rjwalters/vibesql`
- Environment name matches: `pypi`

**Build fails**: Check the GitHub Actions logs. Common issues:
- Missing Rust toolchain (should be set up by workflow)
- Cross-compilation issues for aarch64

**Manual publish (fallback):**
```bash
cd crates/vibesql-python-bindings
pip install maturin
maturin publish --username __token__ --password <pypi-token>
```

### Yanking a bad release

**crates.io:**
```bash
cargo yank --version 0.1.2 vibesql
```

**npm:**
```bash
npm deprecate @vibesql/client@0.1.2 "Critical bug, please upgrade"
# Or unpublish within 72 hours:
npm unpublish @vibesql/client@0.1.2
```

**PyPI:**
```bash
# Delete via web UI at https://pypi.org/manage/project/vibesql/releases/
# Or use twine (within 72 hours of upload):
pip install twine
twine delete vibesql 0.1.2
```

## Important Notes

- **Versions are permanent** on crates.io - you cannot modify a published version
- **npm has 72-hour window** to unpublish, after that only deprecation
- **PyPI allows deletion** via web interface, but best practice is to yank/deprecate
- **docs.rs** automatically builds documentation for crates.io releases
- **Test before publishing** - dry-run everything first

## PyPI v0.1.4 Incident (2026-01-19)

PyPI's release history for `vibesql` skips from `0.1.3` to `0.2.0` — no `0.1.4`
distribution will ever be published. This is intentional and permanent; this
section documents what is known about why.

### What happened

- The `release-pypi.yml` workflow ran for tag `v0.1.4` as
  [run `21127010213`](https://github.com/rjwalters/vibesql/actions/runs/21127010213).
- All five `Build wheels` jobs (Linux x86_64/aarch64, macOS x86_64/arm64,
  Windows x64) and the `Build source distribution` job reported success and
  uploaded their artifacts.
- The final `Publish to PyPI` job reported `failure`. PyPI never received
  the v0.1.4 sdist or any of the v0.1.4 wheels.
- The subsequent v0.2.0 run
  ([`27573307431`](https://github.com/rjwalters/vibesql/actions/runs/27573307431))
  used the same workflow file with no relevant configuration changes between
  the two releases, and it succeeded on the first try. `vibesql 0.2.0` is now
  live on PyPI with the sdist plus all five wheels (Linux x86_64/aarch64,
  macOS x86_64/arm64, Windows x64).

### What is not knowable now

GitHub Actions log retention is 90 days; the v0.1.4 run logs return HTTP 410
and the specific error from `pypa/gh-action-pypi-publish` is no longer
recoverable. The exact failure mode cannot be confirmed in retrospect.

### Most likely cause (inference, not confirmation)

Because the v0.2.0 publish succeeded on the same workflow with no operator
intervention on the PyPI side, the v0.1.4 failure is most consistent with a
**transient upload-side fault** in one of:

1. A 5xx response from PyPI's upload endpoint (PyPI does occasionally serve
   transient 502/503s, especially during deploys).
2. An OIDC token-exchange edge case — the GitHub-issued OIDC token must
   match PyPI's trusted-publisher record exactly on owner, repository,
   workflow filename, and environment; a timing or claim-mismatch glitch
   would surface as a 4xx without leaving residue.
3. A partial upload where some artifacts succeeded and a retry then hit
   `400 File already exists` for the already-uploaded files, failing the
   whole step.

We do **not** have evidence to single out any one of these, and we should
not claim a root cause we cannot verify.

### Recovery

No recovery was attempted: `v0.1.4` was abandoned on PyPI and the next
release (`v0.2.0`) shipped through the same pipeline without changes. The
gap on PyPI's history is permanent — only `0.1.2`, `0.1.3`, and `0.2.0`
will ever appear.

### Mitigation shipped alongside this note

`release-pypi.yml` now passes `skip-existing: true` to
`pypa/gh-action-pypi-publish`. This makes partial re-runs idempotent: if a
future publish uploads some distributions and fails on others, retrying the
job will skip the already-uploaded files instead of 4xx-ing the whole step.
This is the PyPI analogue of the `cargo publish --skip-existing` pattern
used for crates.io publishing.

`skip-existing: true` does **not** silently overwrite published files —
PyPI forbids that. It only suppresses the "file already exists" error for
distributions that are byte-identical to what is already on PyPI.

### Operator follow-up (not enforced)

Periodically re-verify the trusted-publisher record at
<https://pypi.org/manage/project/vibesql/settings/publishing/>. It should
match exactly:

- Owner: `rjwalters`
- Repository: `vibesql`
- Workflow filename: `release-pypi.yml`
- Environment: `pypi`

A drift in any of these (for example, a repo rename) would surface as an
OIDC publish failure on the very next release.

### Future observability

If a future publish failure is suspected to be on the PyPI side, options
to consider before logs expire:

- Add `verbose: true` to the publish action (gives more detail on which
  file failed and why).
- Pipe the `pypa/gh-action-pypi-publish` output to `$GITHUB_STEP_SUMMARY`
  so the failure mode is preserved in the workflow run summary, which has
  a longer effective retention than raw step logs.
- Snapshot `dist/` contents to a workflow artifact before the publish step
  so the exact files attempted can be re-inspected later.

These are not implemented here — they are listed so the next operator
hitting a similar failure does not have to re-derive the option set.

## Quick Reference

```bash
# Full release (after version updates and changelog)
git add -A
git commit -m "Release v0.1.3"
git tag -a v0.1.3 -m "Release v0.1.3"
git push origin main
git push origin v0.1.3

# Publish to crates.io
./scripts/publish-crates.sh --publish

# Publish to npm
cd packages/vibesql-client-ts && pnpm build && npm publish --access public
cd packages/vibesql-drizzle && pnpm build && npm publish --access public

# PyPI publishes automatically from the tag push!

# Create GitHub release
gh release create v0.1.3 --title "v0.1.3" --generate-notes
```
