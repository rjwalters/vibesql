#!/bin/bash
# Test script to verify binary database dogfooding implementation

set -e

echo "===== Testing Binary Database Dogfooding ====="
echo

# Move old SQL-based database out of the way
if [ -f ~/.vibesql/test_results/sqllogictest_results.sql ]; then
    echo "Backing up old SQL database..."
    mv ~/.vibesql/test_results/sqllogictest_results.sql ~/.vibesql/test_results/sqllogictest_results.sql.backup-dogfooding
fi

# Remove old binary database if it exists
if [ -f ~/.vibesql/test_results/sqllogictest_results.vbsql ]; then
    echo "Removing old binary database..."
    rm ~/.vibesql/test_results/sqllogictest_results.vbsql
fi

echo "Step 1: Processing test results into binary database..."
python3 scripts/process_test_results.py --input ~/.vibesql/test_results/sqllogictest_results.json

echo
echo "Step 2: Verifying binary database was created..."
if [ -f ~/.vibesql/test_results/sqllogictest_results.vbsql ]; then
    SIZE=$(du -h ~/.vibesql/test_results/sqllogictest_results.vbsql | cut -f1)
    echo "✓ Binary database created: $SIZE"
else
    echo "✗ Binary database NOT created!"
    exit 1
fi

echo
echo "Step 3: Testing query functionality..."
python3 scripts/query_test_results.py --preset recent-runs

echo
echo "===== Success! ====="
echo "Binary database dogfooding is working!"
echo
echo "Database location: ~/.vibesql/test_results/sqllogictest_results.vbsql"
echo "Old SQL backup: ~/.vibesql/test_results/sqllogictest_results.sql.backup-dogfooding"
