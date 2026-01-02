#!/usr/bin/env python3
"""
Extract development metrics from git history and test results for the VibeSQL challenge baseline.

Usage:
    python3 scripts/extract_challenge_data.py

Output:
    web-demo/public/data/challenge_baseline.json
"""

import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


def run_command(cmd: list[str], cwd: str = None) -> str:
    """Run a command and return stdout."""
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        print(f"Warning: Command failed: {' '.join(cmd)}", file=sys.stderr)
        print(f"  stderr: {result.stderr}", file=sys.stderr)
        return ""
    return result.stdout.strip()


def get_iso_week(date: datetime) -> str:
    """Get ISO week string like '2025-W43'."""
    return f"{date.isocalendar()[0]}-W{date.isocalendar()[1]:02d}"


def get_week_start(date: datetime) -> datetime:
    """Get the Monday of the week containing the given date."""
    return date - timedelta(days=date.weekday())


def extract_git_history(repo_root: str) -> list[dict]:
    """Extract commit history from git."""
    print("Extracting git history...")

    # Get all commits with date and SHA
    output = run_command([
        "git", "log", "--format=%H %ad", "--date=iso-strict", "--reverse"
    ], cwd=repo_root)

    commits = []
    for line in output.split("\n"):
        if not line.strip():
            continue
        parts = line.split(" ", 1)
        if len(parts) != 2:
            continue
        sha, date_str = parts
        # Parse ISO date
        try:
            date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        commits.append({
            "sha": sha,
            "date": date.date().isoformat(),
            "datetime": date
        })

    print(f"  Found {len(commits)} commits from {commits[0]['date']} to {commits[-1]['date']}")
    return commits


def extract_test_results(vibesql_path: str, db_path: str) -> list[dict]:
    """Extract test results from the SQLLogicTest database."""
    print("Extracting test results...")

    # Query all test runs with git commits
    query = """
    SELECT started_at, passed, failed, total_files, git_commit
    FROM test_runs
    WHERE git_commit IS NOT NULL AND git_commit != ''
    ORDER BY started_at ASC
    """

    output = run_command([
        vibesql_path, db_path, "-c", query
    ])

    results = []
    lines = output.split("\n")

    # Skip header lines (find the data rows)
    in_data = False
    for line in lines:
        line = line.strip()
        if not line or line.startswith("+") or line.startswith("|"):
            if line.startswith("|") and "STARTED_AT" not in line.upper():
                # This is a data row
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 5:
                    try:
                        started_at = parts[0]
                        passed = int(parts[1]) if parts[1] else 0
                        failed = int(parts[2]) if parts[2] else 0
                        # total_files in column 4 is the number of test files, not test count
                        # Use passed + failed as the actual test count
                        total = passed + failed
                        git_commit = parts[4]

                        results.append({
                            "started_at": started_at,
                            "passed": passed,
                            "failed": failed,
                            "total": total,
                            "git_commit": git_commit,
                            "pass_rate": round(passed / total * 100, 1) if total > 0 else 0
                        })
                    except (ValueError, IndexError):
                        continue

    # Filter out incomplete runs (less than 400 tests suggests partial run)
    MIN_TESTS = 400
    results = [r for r in results if r["total"] >= MIN_TESTS]

    print(f"  Found {len(results)} test runs with git commits (filtered to runs with >= {MIN_TESTS} tests)")
    return results


def get_loc_at_commit(sha: str, repo_root: str) -> tuple[int, int]:
    """Get lines of code and file count at a specific commit."""
    # List all .rs files in crates/ at this commit
    files_output = run_command([
        "git", "ls-tree", "-r", "--name-only", sha, "--", "crates/"
    ], cwd=repo_root)

    if not files_output:
        return 0, 0

    rs_files = [f for f in files_output.split("\n") if f.endswith(".rs")]
    file_count = len(rs_files)

    if file_count == 0:
        return 0, 0

    # Count lines in all .rs files
    total_lines = 0
    for f in rs_files:
        content = run_command(["git", "show", f"{sha}:{f}"], cwd=repo_root)
        if content:
            total_lines += len(content.split("\n"))

    return total_lines, file_count


def sample_loc_weekly(commits: list[dict], repo_root: str) -> dict[str, dict]:
    """Sample LOC at the end of each week (much faster than per-commit)."""
    print("Computing LOC timeline (sampling weekly)...")

    # Group commits by week
    weeks = defaultdict(list)
    for commit in commits:
        date = datetime.fromisoformat(commit["date"])
        week = get_iso_week(date)
        weeks[week].append(commit)

    # Get last commit of each week
    loc_samples = {}
    sorted_weeks = sorted(weeks.keys())

    for i, week in enumerate(sorted_weeks):
        week_commits = weeks[week]
        last_commit = week_commits[-1]  # Last commit of the week

        # Only sample every few weeks for speed, or all weeks if few enough
        sample_interval = max(1, len(sorted_weeks) // 15)  # ~15 samples
        if i % sample_interval == 0 or i == len(sorted_weeks) - 1:
            loc, file_count = get_loc_at_commit(last_commit["sha"], repo_root)
            loc_samples[week] = {
                "loc": loc,
                "file_count": file_count,
                "commit": last_commit["sha"][:12]
            }
            print(f"  Week {week}: {loc:,} LOC ({file_count} files)")
        else:
            # Interpolate from nearest samples later
            loc_samples[week] = None

    # Interpolate missing weeks
    last_known = {"loc": 0, "file_count": 0}
    for week in sorted_weeks:
        if loc_samples[week] is not None:
            last_known = loc_samples[week]
        else:
            loc_samples[week] = last_known.copy()

    return loc_samples


def compute_weekly_timeline(commits: list[dict], test_results: list[dict], loc_samples: dict) -> list[dict]:
    """Compute weekly aggregated timeline."""
    print("Computing weekly timeline...")

    # Group commits by week
    weeks = defaultdict(list)
    for commit in commits:
        date = datetime.fromisoformat(commit["date"])
        week = get_iso_week(date)
        weeks[week].append(commit)

    # Create test result index by commit SHA
    test_by_commit = {r["git_commit"]: r for r in test_results}

    # Build timeline
    timeline = []
    cumulative_commits = 0
    sorted_weeks = sorted(weeks.keys())

    for week in sorted_weeks:
        week_commits = weeks[week]
        cumulative_commits += len(week_commits)

        # Get week start date
        first_commit_date = datetime.fromisoformat(week_commits[0]["date"])
        week_start = get_week_start(first_commit_date).isoformat()

        # Find best test result for this week (last one with highest pass rate)
        week_test = None
        for commit in reversed(week_commits):
            if commit["sha"] in test_by_commit:
                result = test_by_commit[commit["sha"]]
                if week_test is None or result["pass_rate"] > week_test["pass_rate"]:
                    week_test = result

        # Get LOC for this week
        loc_info = loc_samples.get(week, {})

        entry = {
            "week": week,
            "week_start": week_start,
            "commits": len(week_commits),
            "cumulative_commits": cumulative_commits,
            "loc": loc_info.get("loc"),
            "rust_files": loc_info.get("file_count"),
        }

        if week_test:
            entry["pass_rate"] = week_test["pass_rate"]
            entry["passed"] = week_test["passed"]
            entry["total_tests"] = week_test["total"]
        else:
            entry["pass_rate"] = None
            entry["passed"] = None
            entry["total_tests"] = None

        timeline.append(entry)

    return timeline


def compute_daily_timeline(commits: list[dict]) -> list[dict]:
    """Compute daily commit counts."""
    print("Computing daily timeline...")

    # Group commits by date
    days = defaultdict(int)
    for commit in commits:
        days[commit["date"]] += 1

    # Build timeline
    timeline = []
    cumulative = 0
    for date in sorted(days.keys()):
        cumulative += days[date]
        timeline.append({
            "date": date,
            "commits": days[date],
            "cumulative_commits": cumulative
        })

    return timeline


def compute_milestones(test_results: list[dict], commits: list[dict], first_date: str) -> list[dict]:
    """Find when each pass rate milestone was first reached."""
    print("Computing milestones...")

    milestones_targets = [50, 75, 90, 95, 99]
    milestones = []

    # Create commit date index
    commit_dates = {c["sha"]: c["date"] for c in commits}
    first_date_dt = datetime.fromisoformat(first_date)

    # Sort test results by date
    sorted_results = sorted(test_results, key=lambda r: r["started_at"])

    reached = set()
    for result in sorted_results:
        for target in milestones_targets:
            if target not in reached and result["pass_rate"] >= target:
                reached.add(target)

                commit_date = commit_dates.get(result["git_commit"], result["started_at"][:10])
                try:
                    date_dt = datetime.fromisoformat(commit_date)
                    days_from_start = (date_dt - first_date_dt).days
                except:
                    days_from_start = None

                milestone = {
                    "milestone": f"{target}%",
                    "date": commit_date,
                    "commit": result["git_commit"][:12],
                    "days_from_start": days_from_start,
                    "passed": result["passed"],
                    "total": result["total"]
                }
                milestones.append(milestone)
                print(f"  {target}% reached at {commit_date} (day {days_from_start})")

    return milestones


def main():
    # Paths
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    vibesql_path = repo_root / "target" / "release" / "vibesql"
    db_path = Path.home() / ".vibesql" / "test_results" / "sqllogictest_results.vbsql"
    output_path = repo_root / "web-demo" / "public" / "data" / "challenge_baseline.json"

    # Check paths
    if not vibesql_path.exists():
        print(f"Error: vibesql binary not found at {vibesql_path}", file=sys.stderr)
        print("Run: cargo build --release", file=sys.stderr)
        sys.exit(1)

    if not db_path.exists():
        print(f"Warning: Test results database not found at {db_path}", file=sys.stderr)
        test_results = []
    else:
        test_results = extract_test_results(str(vibesql_path), str(db_path))

    # Extract data
    commits = extract_git_history(str(repo_root))

    if not commits:
        print("Error: No commits found", file=sys.stderr)
        sys.exit(1)

    # Sample LOC weekly
    loc_samples = sample_loc_weekly(commits, str(repo_root))

    # Compute timelines
    weekly_timeline = compute_weekly_timeline(commits, test_results, loc_samples)
    daily_timeline = compute_daily_timeline(commits)

    # Compute milestones
    first_date = commits[0]["date"]
    milestones = compute_milestones(test_results, commits, first_date) if test_results else []

    # Get final stats
    latest_commit = commits[-1]
    first_commit = commits[0]
    duration_days = (datetime.fromisoformat(latest_commit["date"]) -
                     datetime.fromisoformat(first_commit["date"])).days

    # Get final LOC
    final_loc, final_files = get_loc_at_commit(latest_commit["sha"], str(repo_root))

    # Get final test stats
    final_test = test_results[-1] if test_results else None

    # Build output
    output = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "project": {
            "name": "VibeSQL",
            "repo": "https://github.com/rjwalters/vibesql",
            "first_commit": first_commit["date"],
            "latest_commit": latest_commit["date"]
        },
        "summary": {
            "duration_days": duration_days,
            "duration_weeks": (duration_days + 6) // 7,
            "total_commits": len(commits),
            "total_loc": final_loc,
            "total_rust_files": final_files,
            "final_pass_rate": final_test["pass_rate"] if final_test else None,
            "final_passed": final_test["passed"] if final_test else None,
            "final_total": final_test["total"] if final_test else None
        },
        "weekly_timeline": weekly_timeline,
        "daily_timeline": daily_timeline,
        "test_milestones": milestones
    }

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write output
    print(f"\nWriting {output_path}")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print("Done!")
    print(f"\nSummary:")
    print(f"  Duration: {duration_days} days ({output['summary']['duration_weeks']} weeks)")
    print(f"  Commits: {len(commits):,}")
    print(f"  LOC: {final_loc:,}")
    print(f"  Files: {final_files}")
    if final_test:
        print(f"  Pass rate: {final_test['pass_rate']}% ({final_test['passed']}/{final_test['total']})")


if __name__ == "__main__":
    main()
