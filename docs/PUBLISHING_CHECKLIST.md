# VibeSQL Release Guide

This document describes the release process for VibeSQL across all distribution channels.

## Distribution Channels

| Channel | Packages | Current Version |
|---------|----------|-----------------|
| **crates.io** | 11 Rust crates | 0.1.1 |
| **npm** | `@vibesql/client`, `@vibesql/drizzle` | 0.1.1, 0.1.0 |

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
version = "0.1.2"  # Update this
```

Edit `packages/vibesql-client-ts/package.json`:
```json
{
  "version": "0.1.2"
}
```

Edit `packages/vibesql-drizzle/package.json`:
```json
{
  "version": "0.1.2"
}
```

### Step 3: Update CHANGELOG

Add entry to CHANGELOG.md:
```markdown
## [0.1.2] - YYYY-MM-DD

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
git commit -m "Release v0.1.2"
git tag -a v0.1.2 -m "Release v0.1.2"
git push origin main
git push origin v0.1.2
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

### Step 7: Create GitHub Release

1. Go to: https://github.com/rjwalters/vibesql/releases/new
2. Select tag: `v0.1.2`
3. Title: `v0.1.2`
4. Copy release notes from CHANGELOG.md
5. Publish release

## Post-Release

- [ ] Verify crates appear on crates.io
- [ ] Verify packages appear on npmjs.com
- [ ] Verify docs.rs builds documentation
- [ ] Update any version badges in README
- [ ] Announce release (if applicable)

## Troubleshooting

### crates.io publish fails

**"crate already exists"**: You cannot re-publish the same version. Bump the version number.

**Dependency not found**: Crates must be published in dependency order. Wait 10-30 seconds between publishes (the script handles this).

**Authentication error**: Run `cargo login <token>` with a fresh token.

### npm publish fails

**"You must be logged in"**: Run `npm login`

**"403 Forbidden"**: For scoped packages (@vibesql/*), use `npm publish --access public`

**"Version already exists"**: Bump the version in package.json

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

## Important Notes

- **Versions are permanent** on crates.io - you cannot modify a published version
- **npm has 72-hour window** to unpublish, after that only deprecation
- **docs.rs** automatically builds documentation for crates.io releases
- **Test before publishing** - dry-run everything first
