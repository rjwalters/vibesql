# GitHub Workflows

This directory contains CI/CD workflows for the vibesql project.

## Workflows

### `ci.yml` - CI (Pull Requests)

**Trigger**: On pull requests to `main` (plus manual dispatch)

**Purpose**: Fast CI optimized for PR feedback speed
- Verifies TPC-H/TPC-DS benchmark queries haven't been modified
- Guards against web-demo lockfile drift (`pnpm install --frozen-lockfile`)
- Runs unit and integration tests in release mode (SQLLogicTest suite skipped)
- Checks the wasm32 build of `vibesql-wasm-bindings` (catches wasm-only breakage)
- Checks `vibesql-storage` with `--features storage-all` (catches opendal-gated breakage)

Extended validation (TPC-DS, SQLLogicTest, PostgreSQL, TCL) lives in `ci-extended.yml`.

### `ci-main.yml` - CI (main)

**Trigger**: On every push to `main` (plus manual dispatch)

**Purpose**:
- Runs unit tests in release mode (SQLLogicTest suite skipped)
- Runs the sqltest conformance suite (informational, `continue-on-error`)

Note: the web demo is **not** deployed by CI — deployment is manual via
`cd web-demo && wrangler deploy` (see CLAUDE.md).

### `ci-extended.yml` - Extended Validation

**Trigger**: Manual only (workflow_dispatch), with boolean inputs to select suites

**Purpose**: On-demand, informational validation before merging significant PRs
(extracted from PR CI to save ~90 min per PR):
- TPC-DS validation (correctness check against DuckDB)
- SQLLogicTest sample
- PostgreSQL regression tests
- SQLite TCL tests
- `scripts/` Python test suite (builds Python bindings)
- Summary job aggregating results

Run with: `gh workflow run ci-extended.yml`

### `fuzz.yml` - Fuzz Testing

**Trigger**: Weekly schedule (Sunday 3 AM UTC) and manual dispatch (with a
`duration` input, default 300s)

**Purpose**: Runs `cargo-fuzz` (nightly toolchain) across a matrix of targets:
`sql_parser`, `expr_eval`, `type_convert`, `differential_sqlite`. When crashes
are found, it files (or comments on) a GitHub issue labeled `bug`/`fuzzing`
with the crash inputs and reproduction steps.

### `miri.yml` - MIRI Undefined Behavior Detection

**Trigger**: Weekly schedule (Sunday 4 AM UTC, after fuzzing) and manual dispatch

**Purpose**: Runs MIRI (nightly toolchain) on `vibesql-storage` Row tests and a
sample of `vibesql-executor` tests (pure-algorithm tests without file I/O) to
detect undefined behavior. Run manually before releases or when modifying
unsafe code: `gh workflow run miri.yml`

### `i18n-check.yml` - i18n Validation

**Trigger**: On pull requests touching translation-related paths
(`crates/vibesql-l10n/`, `web-demo/src/i18n/resources/`, the translation-check
scripts, or the workflow itself)

**Purpose**: Validates Fluent translation files:
- CLI Fluent syntax check (`check-ftl`)
- CLI translation completeness
- Web translation completeness
- Strict check

### `release-crates.yml` - Release to crates.io

**Trigger**: On push of a `v*` tag; manual dispatch supports a `dry_run` input

**Purpose**: Verifies the workspace `Cargo.toml` version matches the tag, then
builds and publishes the `vibesql-*` crates to crates.io.

### `release-pypi.yml` - Release to PyPI

**Trigger**: On push of a `v*` tag; manual dispatch supports a `dry_run` input

**Purpose**: Builds Python wheels across a platform matrix (Linux x86_64 and
aarch64, macOS x86_64 and Apple Silicon, Windows x86_64) plus an sdist, then
publishes to PyPI via trusted publishing (OIDC).

### `label-external-contributors.yml` - Issue Labeling

**Trigger**: On issues opened (plus manual dispatch)

**Purpose**: Automatically labels issues from external (non-collaborator)
authors with `external` and posts a welcome comment.
