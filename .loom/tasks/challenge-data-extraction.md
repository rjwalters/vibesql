# Task: Build Data Extraction Pipeline for SQL Database Challenge

## Context

We are adding a new landing page to vibesql.org that positions VibeSQL as a **benchmark for multi-agent software development**. The landing page will showcase our development metrics as a baseline that others can try to beat with their own AI orchestration frameworks.

We need a script that extracts historical development metrics from git history and test results, outputting JSON data that the website can visualize.

## Your Task

Create a Python script `scripts/extract_challenge_data.py` that:

1. Extracts development metrics from the git repository
2. Extracts test pass rates from the SQLLogicTest results database
3. Correlates the two datasets by git commit SHA
4. Outputs `web-demo/public/data/challenge_baseline.json`

## Data Sources

### 1. Git Repository (current directory)

- First commit: 2025-10-25
- ~3,465 commits total
- Rust codebase in `crates/` directory

Available git data:
- Commit timestamps
- Commit SHAs
- Lines of code (count `*.rs` files in `crates/`)

### 2. SQLLogicTest Results Database

Location: `~/.vibesql/test_results/sqllogictest_results.vbsql`

This is a VibeSQL database (SQLite-compatible). Query it using:
```bash
./target/release/vibesql ~/.vibesql/test_results/sqllogictest_results.vbsql -c "SQL HERE"
```

Schema:
```sql
-- test_runs table
CREATE TABLE test_runs (
    run_id INTEGER PRIMARY KEY,
    started_at TEXT,
    completed_at TEXT,
    total_files INTEGER,
    passed INTEGER,
    failed INTEGER,
    untested INTEGER,
    git_commit TEXT,      -- 40-char SHA, can join with git history
    branch_name TEXT,
    ci_run_id TEXT
);
```

Example query:
```bash
./target/release/vibesql ~/.vibesql/test_results/sqllogictest_results.vbsql -c "
SELECT started_at, passed, failed, git_commit
FROM test_runs
ORDER BY run_id DESC
LIMIT 5
"
```

## Expected Output

Create `web-demo/public/data/challenge_baseline.json`:

```json
{
  "generated_at": "2026-01-02T12:00:00Z",
  "project": {
    "name": "VibeSQL",
    "repo": "https://github.com/rjwalters/vibesql",
    "first_commit": "2025-10-25",
    "latest_commit": "2026-01-02"
  },
  "summary": {
    "duration_days": 69,
    "duration_weeks": 10,
    "total_commits": 3465,
    "total_loc": 460787,
    "total_rust_files": 245,
    "final_pass_rate": 99.0,
    "final_passed": 616,
    "final_total": 622
  },
  "weekly_timeline": [
    {
      "week": "2025-W43",
      "week_start": "2025-10-20",
      "commits": 86,
      "cumulative_commits": 86,
      "loc": 15000,
      "pass_rate": null,
      "passed": null,
      "total_tests": null
    },
    {
      "week": "2025-W44",
      "week_start": "2025-10-27",
      "commits": 530,
      "cumulative_commits": 616,
      "loc": 85000,
      "pass_rate": 45.2,
      "passed": 280,
      "total_tests": 622
    }
    // ... more weeks
  ],
  "daily_timeline": [
    {
      "date": "2025-10-25",
      "commits": 12,
      "cumulative_commits": 12
    }
    // ... more days (for detailed chart)
  ],
  "test_milestones": [
    {
      "milestone": "50%",
      "date": "2025-11-05",
      "commit": "abc123...",
      "days_from_start": 11
    },
    {
      "milestone": "75%",
      "date": "2025-11-15",
      "commit": "def456...",
      "days_from_start": 21
    },
    {
      "milestone": "90%",
      "date": "2025-11-28",
      "commit": "ghi789...",
      "days_from_start": 34
    },
    {
      "milestone": "95%",
      "date": "2025-12-10",
      "commit": "jkl012...",
      "days_from_start": 46
    }
  ]
}
```

## Implementation Notes

### Getting LOC at Historical Commits

To get LOC at a specific commit without checking out:
```bash
git ls-tree -r --name-only <commit-sha> -- crates/ | grep '\.rs$' | \
  xargs -I {} git show <commit-sha>:{} 2>/dev/null | wc -l
```

Or sample weekly (faster): checkout the last commit of each week and count.

### Correlating Test Results with Git

The `test_runs.git_commit` column contains 40-char SHAs. Match these against git history to place test results on the timeline.

Note: Test results only exist for recent history (test infrastructure was added later). Earlier weeks will have `pass_rate: null`.

### Handling Missing Data

- Early commits won't have test data - use `null` for pass_rate
- Some weeks might have no commits - still include them with `commits: 0`
- If LOC sampling is too slow, sample every N commits or weekly snapshots

### Running the Script

The script should be runnable as:
```bash
python3 scripts/extract_challenge_data.py
```

And should output:
```
Extracting git history...
  Found 3465 commits from 2025-10-25 to 2026-01-02
Extracting test results...
  Found 47 test runs with git commits
Computing LOC timeline (sampling weekly)...
  Week 2025-W43: 15,234 LOC
  Week 2025-W44: 85,102 LOC
  ...
Computing milestones...
  50% reached at 2025-11-05
  75% reached at 2025-11-15
  ...
Writing web-demo/public/data/challenge_baseline.json
Done!
```

## Verification

After running, verify the output:
```bash
cat web-demo/public/data/challenge_baseline.json | python3 -m json.tool | head -50
```

## Dependencies

Use only Python standard library (subprocess, json, datetime, etc.). No external packages required.

## Files to Create

1. `scripts/extract_challenge_data.py` - main script
2. `web-demo/public/data/challenge_baseline.json` - output (generated)

## Not In Scope

- Website UI changes (separate task)
- Modifying test infrastructure
- Historical backfill of test results
