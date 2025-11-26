# Publishing Checklist for v0.1.0

## Pre-Publication Checklist

### Completed

- [x] Rename all crates with `vibesql-*` namespace
- [x] Add version requirements to all internal dependencies
- [x] Add publishing metadata to all Cargo.toml files
- [x] Update main crate documentation (src/lib.rs)
- [x] Verify all crate names available on crates.io
- [x] Create publish script (`scripts/publish-crates.sh`)
- [x] LICENSE-MIT and LICENSE-APACHE files exist
- [x] CHANGELOG.md exists
- [x] README.md is comprehensive

### Before Publishing

- [ ] Run full test suite: `cargo test --all`
- [ ] Run clippy: `cargo clippy --all-targets`
- [ ] Build documentation: `cargo doc --no-deps`
- [ ] Verify examples compile: `cargo build --examples`
- [ ] Dry-run publish: `./scripts/publish-crates.sh`

### Optional

- [ ] SECURITY.md - vulnerability reporting process
- [ ] CONTRIBUTING.md - contributor guidelines

## Publishing Process

1. **Commit all changes and ensure clean working tree**

2. **Create and push git tag:**
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

3. **Get crates.io API token:**
   - Visit https://crates.io/settings/tokens
   - Create new token
   - Run: `cargo login <token>`

4. **Publish to crates.io:**
   ```bash
   ./scripts/publish-crates.sh --publish
   ```

5. **Create GitHub Release:**
   - Go to: https://github.com/rjwalters/vibesql/releases/new
   - Select tag: v0.1.0
   - Add release notes from CHANGELOG.md

## Important Notes

- **Versions are permanent** - Once published, you cannot modify a version
- **You can yank** - If there's a critical issue, yank the version
- **Documentation is auto-generated** - docs.rs will build and host docs

## Post-Publication

- [ ] Update README badges with crates.io version
- [ ] Announce release
