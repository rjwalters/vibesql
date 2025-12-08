#!/usr/bin/env bash
# Backup test results databases to the repository
# Backs up both SQLLogicTest results and benchmark results
# Keeps only the 5 most recent backups of each type

set -euo pipefail

# Get script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Paths
SOURCE_DIR="$HOME/.vibesql/test_results"
BACKUP_DIR="$REPO_ROOT/test_results"
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")

# Database files to backup
SQLLOGICTEST_DB="$SOURCE_DIR/sqllogictest_results.vbsql"
SQLLOGICTEST_DB_LEGACY="$SOURCE_DIR/sqllogictest_results.sql"
BENCHMARK_DB="$SOURCE_DIR/benchmark_results.db"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

echo "=============================================="
echo "  VibeSQL Test Results Backup"
echo "=============================================="
echo ""

# Function to backup a database file
backup_database() {
    local source_file="$1"
    local backup_prefix="$2"
    local description="$3"
    local extension="${source_file##*.}"
    local backup_file="$BACKUP_DIR/${backup_prefix}-${TIMESTAMP}.${extension}"

    if [ ! -f "$source_file" ]; then
        echo "  Skipping $description: source not found at $source_file"
        return 1
    fi

    echo "Backing up $description..."
    cp "$source_file" "$backup_file"

    # Get file size for display
    local size
    size=$(du -h "$backup_file" | cut -f1)
    echo "  Created: $(basename "$backup_file") ($size)"

    # Keep only the 5 most recent backups of this type
    cd "$BACKUP_DIR"
    local pattern="${backup_prefix}-*.${extension}"
    ls -t $pattern 2>/dev/null | tail -n +6 | while read -r old_backup; do
        echo "  Removing old backup: $old_backup"
        rm -f "$old_backup"
    done

    return 0
}

# Backup SQLLogicTest results (try new format first, then legacy)
echo "SQLLogicTest Results:"
if [ -f "$SQLLOGICTEST_DB" ]; then
    backup_database "$SQLLOGICTEST_DB" "sqllogictest_results" "SQLLogicTest results (vbsql format)"
elif [ -f "$SQLLOGICTEST_DB_LEGACY" ]; then
    backup_database "$SQLLOGICTEST_DB_LEGACY" "sqllogictest_results" "SQLLogicTest results (legacy SQL format)"
else
    echo "  No SQLLogicTest database found"
fi
echo ""

# Backup benchmark results
echo "Benchmark Results:"
if backup_database "$BENCHMARK_DB" "benchmark_results" "benchmark results"; then
    # Show summary of benchmark data
    if command -v sqlite3 &> /dev/null; then
        echo ""
        echo "  Benchmark summary:"
        sqlite3 "$BENCHMARK_DB" "SELECT benchmark_suite, COUNT(*) as runs, MAX(timestamp) as latest FROM benchmark_runs GROUP BY benchmark_suite ORDER BY latest DESC;" 2>/dev/null | while IFS='|' read -r suite runs latest; do
            echo "    $suite: $runs runs (latest: ${latest:0:10})"
        done
    fi
else
    echo "  No benchmark database found"
fi
echo ""

# Show all current backups
echo "=============================================="
echo "  Current Backups"
echo "=============================================="
cd "$BACKUP_DIR"
echo ""
echo "SQLLogicTest backups:"
ls -lh sqllogictest_results-*.* 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}' || echo "  (none)"
echo ""
echo "Benchmark backups:"
ls -lh benchmark_results-*.db 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}' || echo "  (none)"

echo ""
echo "=============================================="
echo "  Backup Complete"
echo "=============================================="
echo ""
echo "To commit these backups to git:"
echo "  git add test_results/"
echo "  git commit -m \"Update test results database backups\""
