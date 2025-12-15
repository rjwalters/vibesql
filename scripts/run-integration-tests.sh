#!/usr/bin/env bash
#
# Run integration tests excluding compliance tests (sqllogictest, pgsql_regress)
# This allows compliance tests to run last for faster failure detection.
#
# Usage:
#   ./scripts/run-integration-tests.sh
#

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Compliance tests to exclude (these run last in Phase 4)
EXCLUDE_PATTERNS="sqllogictest|sqltest|pgsql_regress"

# Get all available test targets from cargo, then filter out compliance tests
# We use --no-run to just compile and list targets without running
# Format: Executable tests/path/name.rs (target/release/deps/name-hash)
TESTS=$(cargo test --release --workspace --no-run 2>&1 | \
    grep -E '^\s+Executable tests/' | \
    sed 's/.*deps\/\([^-]*\)-.*/\1/' | \
    grep -vE "$EXCLUDE_PATTERNS" | \
    sort -u)

if [ -z "$TESTS" ]; then
    echo "No integration tests found (excluding compliance tests)"
    exit 0
fi

# Build the --test flags
TEST_FLAGS=""
for test in $TESTS; do
    TEST_FLAGS="$TEST_FLAGS --test $test"
done

TEST_COUNT=$(echo "$TESTS" | wc -l | tr -d ' ')
echo "Running $TEST_COUNT integration tests (excluding compliance)..."

# Run cargo test with all the test flags
# shellcheck disable=SC2086
cargo test --release --workspace $TEST_FLAGS
