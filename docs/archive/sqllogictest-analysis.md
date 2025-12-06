# SQLLogicTest Analysis Tools

This document describes the analysis tools for SQLLogicTest suite results.

## Overview

The SQLLogicTest suite runs 623 test files from the official [dolthub/sqllogictest](https://github.com/dolthub/sqllogictest) repository, containing millions of individual SQL test cases. Due to the large number of tests and various error types, we've created analysis tools to:

- Categorize errors by type (parse errors, missing features, unsupported syntax)
- Count occurrences of each error type
- Generate actionable summary reports
- Track progress over time

## Tools

### 1. `analyze_sqllogictest.py` - Test Output Parser

**Location**: `scripts/analyze_sqllogictest.py`

Parses SQLLogicTest output and generates reports.

**Usage**:
```bash
# From stdin
cargo test --test sqllogictest_suite -- --nocapture | python3 scripts/analyze_sqllogictest.py

# From file
python3 scripts/analyze_sqllogictest.py target/sqllogictest_raw.log
```

**Outputs**:
- Console: Human-readable analysis report
- `target/sqllogictest_analysis.json`: JSON summary for programmatic use
- `target/sqllogictest_analysis.md`: Markdown report for documentation

**Error Categories**:
- **missing_data_types**: SQL data types not yet implemented (e.g., TEXT)
- **missing_ddl_statements**: DDL statements not yet implemented (CREATE INDEX, CREATE VIEW, etc.)
- **lexer_errors**: Lexer issues (backtick identifiers, special characters)
- **sqllogictest_syntax**: Unsupported SQLLogicTest directives (onlyif, skipif)
- **other_parse_errors**: Other parsing errors

### 2. `run_sqllogictest_analysis.sh` - CI-Friendly Wrapper

**Location**: `scripts/run_sqllogictest_analysis.sh`

Runs the test suite and generates all reports with proper error handling.

**Usage**:
```bash
# Default (2 minute timeout)
./scripts/run_sqllogictest_analysis.sh

# Custom timeout
./scripts/run_sqllogictest_analysis.sh --timeout 300

# Help
./scripts/run_sqllogictest_analysis.sh --help
```

**Outputs**:
- `target/sqllogictest_raw.log`: Raw test output
- `target/sqllogictest_analysis.json`: JSON summary
- `target/sqllogictest_analysis.md`: Markdown report
- `target/sqllogictest_results.json`: Test results (from test runner)

## CI Integration

The analysis runs automatically in CI on every push. Results are:

1. **Uploaded as artifacts**: Available for download from GitHub Actions
2. **Deployed to GitHub Pages**: Available at `https://rjwalters.github.io/vibesql/badges/`
   - `sqllogictest_analysis.json` - JSON summary
   - `sqllogictest_analysis.md` - Markdown report

### Viewing Results

**In GitHub Actions**:
1. Go to Actions tab
2. Select a workflow run
3. Scroll to "Artifacts" section
4. Download "test-results" artifact

**On GitHub Pages**:
- JSON: https://rjwalters.github.io/vibesql/badges/sqllogictest_analysis.json
- Markdown: https://rjwalters.github.io/vibesql/badges/sqllogictest_analysis.md

## Output Format

### JSON Summary Structure

```json
{
  "summary": {
    "total_files": 623,
    "passed": 5,
    "failed": 618,
    "pass_rate": 0.8
  },
  "error_categories": {
    "missing_data_types": 174,
    "missing_ddl_statements": 7,
    "sqllogictest_syntax": 432,
    "lexer_errors": 1
  },
  "missing_features": {
    "Data type: text": 174,
    "CREATE INDEX/VIEW/TRIGGER": 7,
    "Backtick identifiers": 1
  },
  "sqllogictest_syntax_errors": {
    "onlyif mysql # aggregate syntax: ": 131,
    "skipif postgresql # ...": 64
  },
  "top_parse_errors": {}
}
```

### Markdown Report

The markdown report includes:
- Summary statistics (total, passed, failed, pass rate)
- Error categories table
- Missing features table (prioritized by frequency)
- Unsupported SQLLogicTest syntax table

Example:

```markdown
# SQLLogicTest Analysis Report

## Summary

- **Total test files**: 623
- **Passed**: 5 (0.8%)
- **Failed**: 618 (99.2%)

## Error Categories

| Category | Count |
|----------|------:|
| sqllogictest_syntax | 432 |
| missing_data_types | 174 |
| missing_ddl_statements | 7 |
| lexer_errors | 1 |

## Missing Features

| Feature | Occurrences |
|---------|------------:|
| Data type: text | 174 |
| CREATE INDEX/VIEW/TRIGGER | 7 |
| Backtick identifiers | 1 |
```

## Using Analysis Results

### Identifying Priorities

The analysis helps prioritize implementation work:

1. **High-impact features**: Features blocking many tests (e.g., TEXT data type = 174 tests)
2. **Low-hanging fruit**: Simple features blocking fewer tests
3. **Library limitations**: SQLLogicTest syntax errors require library updates

### Tracking Progress

Compare analysis results over time:

```bash
# Run analysis and save with timestamp
./scripts/run_sqllogictest_analysis.sh
cp target/sqllogictest_analysis.json analysis_$(date +%Y%m%d).json

# Compare with previous run
diff analysis_20250101.json analysis_20250201.json
```

### Automating Checks

Use JSON output in scripts:

```bash
# Check if pass rate improved
OLD_RATE=$(jq -r '.summary.pass_rate' old_analysis.json)
NEW_RATE=$(jq -r '.summary.pass_rate' target/sqllogictest_analysis.json)

if (( $(echo "$NEW_RATE > $OLD_RATE" | bc -l) )); then
  echo "✓ Pass rate improved: $OLD_RATE% → $NEW_RATE%"
else
  echo "✗ Pass rate decreased: $OLD_RATE% → $NEW_RATE%"
fi
```

## Troubleshooting

### Script Not Found

Ensure you're running from the repository root:
```bash
cd /path/to/vibesql
./scripts/run_sqllogictest_analysis.sh
```

### Python Not Found

The script needs Python 3. Install if missing:
```bash
# macOS
brew install python3

# Ubuntu/Debian
apt-get install python3

# Or use virtual environment
python3 -m venv .venv
source .venv/bin/activate
```

### Test Timeout

Increase timeout for slower machines:
```bash
./scripts/run_sqllogictest_analysis.sh --timeout 300  # 5 minutes
```

## Future Enhancements

Potential improvements:

1. **Trend tracking**: Store historical results and show trends
2. **Category breakdown**: More detailed error categorization
3. **Test file details**: Per-file pass/fail status
4. **Interactive dashboard**: Web UI for exploring results
5. **Regression detection**: Alert on new failing tests

## Related Files

- Test runner: `tests/sqllogictest_suite.rs`
- Test corpus: `third_party/sqllogictest/test/`
- CI workflow: `.github/workflows/ci-and-deploy.yml`
- Analysis script: `scripts/analyze_sqllogictest.py`
- Wrapper script: `scripts/run_sqllogictest_analysis.sh`
