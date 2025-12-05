#!/usr/bin/env bash
#
# Backup benchmark data to the repository for web deployment.
#
# This script:
# 1. Exports benchmark data from the dogfooding database (sysbench, tpcc, tpcds, tpch)
# 2. Optionally runs TPC-H comparison benchmarks (vibesql vs sqlite vs duckdb)
# 3. Commits changes to the repository
#
# Usage:
#   ./scripts/backup_benchmarks.sh              # Export from DB only
#   ./scripts/backup_benchmarks.sh --compare    # Also run TPC-H comparison
#   ./scripts/backup_benchmarks.sh --push       # Commit and push
#   ./scripts/backup_benchmarks.sh --compare --push  # Full update and push
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCHMARK_DIR="$REPO_ROOT/web-demo/public/benchmarks"

# Parse arguments
RUN_COMPARE=false
DO_PUSH=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --compare|-c)
            RUN_COMPARE=true
            shift
            ;;
        --push|-p)
            DO_PUSH=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--compare] [--push]"
            echo ""
            echo "Options:"
            echo "  --compare, -c   Run TPC-H comparison benchmarks (vibesql vs sqlite)"
            echo "  --push, -p      Commit and push changes to remote"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

cd "$REPO_ROOT"

echo "=== Benchmark Data Backup ==="
echo "Repository: $REPO_ROOT"
echo "Output dir: $BENCHMARK_DIR"
echo ""

# Step 1: Export from dogfooding database
echo "Step 1: Exporting from dogfooding database..."
python3 scripts/export_benchmark_json.py --all --verbose
echo ""

# Step 2: Optionally run TPC-H comparison benchmarks
if [ "$RUN_COMPARE" = true ]; then
    echo "Step 2: Running TPC-H comparison benchmarks..."
    echo "  This may take 5-10 minutes..."
    python3 scripts/run_tpch_benchmarks.py --output "$BENCHMARK_DIR/benchmark_results.json"
    echo ""
else
    echo "Step 2: Skipping TPC-H comparison (use --compare to run)"
    echo ""
fi

# Step 3: Show what changed
echo "Step 3: Checking changes..."
if git diff --quiet "$BENCHMARK_DIR"; then
    echo "  No changes to benchmark data."
    exit 0
fi

echo "  Changed files:"
git diff --stat "$BENCHMARK_DIR"
echo ""

# Step 4: Commit if there are changes
echo "Step 4: Committing changes..."
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
git add "$BENCHMARK_DIR"
git commit -m "chore(bench): Backup benchmark data ($TIMESTAMP)

Exported from dogfooding database and local benchmark runs.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"

echo ""

# Step 5: Optionally push
if [ "$DO_PUSH" = true ]; then
    echo "Step 5: Pushing to remote..."
    git pull --rebase origin main
    git push origin main
    echo ""
    echo "=== Done! Changes pushed to remote ==="
else
    echo "Step 5: Skipping push (use --push to push)"
    echo ""
    echo "=== Done! Changes committed locally ==="
    echo "Run 'git push origin main' to deploy."
fi
