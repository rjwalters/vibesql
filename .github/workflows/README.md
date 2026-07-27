# GitHub Workflows

This directory contains CI/CD workflows for the vibesql project.

## Workflows

### `ci-and-deploy.yml` - Main CI/CD Pipeline

**Trigger**: On every push to `main`

**Purpose**:
- Runs all unit and integration tests
- Runs sqltest conformance suite (SQL:1999 Core)
- Generates SQLLogicTest punchlist from manual test results
- Runs benchmarks
- Generates badges and documentation
- Deploys to GitHub Pages

**SQLLogicTest Strategy**:
- Uses systematic punchlist approach (not random sampling)
- Punchlist generated from `target/sqllogictest_cumulative.json` (manual test results)
- Badge shows: `{passed}✓ {failed}✗ {untested}? ({pass_rate}%)`
- Progress reflects actual systematic testing, not random coverage

### `deploy-pages.yml` - GitHub Pages Deployment

**Trigger**: Called by `ci-and-deploy.yml` workflow

**Purpose**:
- Builds web demo (optional, controlled by input)
- Downloads historical data from GitHub Pages
- Merges new test results (sqltest, punchlist, benchmarks)
- Generates badge JSON files
- Deploys everything to GitHub Pages

**Inputs**:
- `artifact-name`: Name of artifact with test results to deploy
- `benchmark-artifact-name`: Name of artifact with benchmark results
- `rebuild-web-demo`: Whether to rebuild web demo from scratch (default: true)

### `label-external-contributors.yml` - PR Labeling

**Trigger**: On pull requests

**Purpose**: Automatically labels PRs from external contributors

### `redeploy-web-demo.yml` - Manual Web Demo Redeploy

**Trigger**: Manual (workflow_dispatch)

**Purpose**: Redeploys the web demo without running tests (useful for fixing deployment issues)

## SQLLogicTest Punchlist Strategy

The project uses a **systematic punchlist approach** to achieve 100% SQLLogicTest conformance:

### How It Works

1. **Test Corpus**: 623 test files, ~5.9 million individual SQL tests
2. **Punchlist Generation**: `scripts/generate_punchlist.py` scans all test files and categorizes them
3. **Manual Testing**: Use `scripts/test_one_file.sh` to test individual files and identify root causes
4. **Progress Tracking**: Results tracked in `target/sqllogictest_cumulative.json`
5. **Badge**: Shows `{passed}✓ {failed}✗ {untested}? ({pass_rate}%)`

### Current Status (as of last punchlist generation)

| Category | Total | Passing | Pass Rate |
|----------|-------|---------|-----------|
| index    | 214   | 75      | 35.0%     |
| evidence | 12    | 6       | 50.0%     |
| random   | 391   | 2       | 0.5%      |
| ddl      | 1     | 0       | 0.0%      |
| other    | 5     | 0       | 0.0%      |
| **TOTAL**| **623**| **83** | **13.3%** |

### Development Workflow

1. **Test a file**: `./scripts/test_one_file.sh index/delete/10/slt_good_0.test`
2. **Read error**: Identify what's missing or broken
3. **Fix code**: Implement the missing feature or fix the bug
4. **Test again**: Verify the fix works
5. **Update results**: Run the file multiple times to ensure stability
6. **Regenerate punchlist**: `python3 scripts/generate_punchlist.py`
7. **Commit changes**: Results automatically reflected in badge on next CI run

### Files

- **Punchlist JSON**: `target/sqllogictest_punchlist.json` (generated locally, uploaded to gh-pages)
- **Punchlist CSV**: `target/sqllogictest_punchlist.csv` (human-readable, sortable)
- **Strategy Guide**: `PUNCHLIST_100_CONFORMANCE.md` (comprehensive strategic guide)
- **Quick Start**: `QUICK_START.md` (2-minute overview)

### Published Resources

- **Punchlist Results**: `https://vibesql.org/badges/sqllogictest_punchlist.json`
- **Badge Endpoint**: `https://img.shields.io/endpoint?url=https://vibesql.org/badges/sqllogictest.json`
- **Conformance Report**: `https://vibesql.org/conformance.html`

## Maintenance

### Updating Test Results

After fixing issues and testing files:

```bash
# Regenerate punchlist with updated results
python3 scripts/generate_punchlist.py

# Commit the updated cumulative results
git add target/sqllogictest_cumulative.json
git commit -m "Update SQLLogicTest results"

# Push to trigger CI (updates badge automatically)
git push
```

### Troubleshooting

**Badge not updating**:
- Check that `target/sqllogictest_punchlist.json` is being generated in CI
- Verify badge JSON is being created by `generate_badges.sh`
- Check that deployment to gh-pages succeeded
- Clear browser cache and check badge endpoint directly

**Punchlist generation fails**:
- Ensure `third_party/sqllogictest/test/` directory exists
- Check that `target/sqllogictest_cumulative.json` has valid JSON
- Run `python3 scripts/generate_punchlist.py` locally to see errors
