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
