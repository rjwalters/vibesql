#!/usr/bin/env bash
# Test a single SQLLogicTest file and report results
# Usage: test_one_file.sh [test_file_or_path]

set -e

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="${REPO_ROOT}/third_party/sqllogictest/test"
PUNCHLIST_CSV="${REPO_ROOT}/target/sqllogictest_punchlist.csv"

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse test file argument
if [ -z "$1" ]; then
    # Show usage with failing tests
    echo "Usage: $0 <test_file>"
    echo ""
    echo "Test file can be:"
    echo "  - Relative path: index/delete/10/slt_good_0.test"
    echo "  - Full path: /path/to/test.test"
    echo "  - Test name pattern: index::delete::10::slt_good_0"
    echo ""
    echo "First 10 failing tests to fix:"
    grep "FAIL" "$PUNCHLIST_CSV" | head -11 | tail -10 | awk -F',' '{print $1}' | sed 's/\//::/' | sed 's/\.test//'
    exit 1
fi

TEST_FILE="$1"

# Convert various formats to relative path
if [[ "$TEST_FILE" == *"::"* ]]; then
    # Convert test::name::format to path/format
    TEST_FILE=$(echo "$TEST_FILE" | sed 's/::/\//g').test
fi

# Remove leading/trailing whitespace
TEST_FILE=$(echo "$TEST_FILE" | xargs)

# Convert full path to relative
if [[ "$TEST_FILE" == /* ]]; then
    TEST_FILE=$(echo "$TEST_FILE" | sed "s|^$TEST_DIR/||")
fi

# Check if test file exists
FULL_PATH="${TEST_DIR}/${TEST_FILE}"
if [ ! -f "$FULL_PATH" ]; then
    echo -e "${RED}✗ Test file not found: $FULL_PATH${NC}"
    exit 1
fi

echo "Testing: $TEST_FILE"
echo "Full path: $FULL_PATH"
echo ""

# Convert to test name format (replace / with ::, remove .test)
TEST_NAME=$(echo "$TEST_FILE" | sed 's/\//::/g' | sed 's/\.test//')

# Run the test
cd "$REPO_ROOT"

echo -e "${YELLOW}Running: cargo test --test sqllogictest_suite -- --exact $TEST_NAME${NC}"
echo ""

if cargo test --test sqllogictest_suite --release -- --exact "$TEST_NAME" 2>&1; then
    STATUS="PASS"
    COLOR=$GREEN
else
    STATUS="FAIL"
    COLOR=$RED
fi

echo ""
echo -e "${COLOR}Result: $STATUS${NC}"
echo ""
echo "To run again:"
echo "  cargo test --test sqllogictest_suite --release -- --exact $TEST_NAME"
echo ""

# Show next test to try
if [ "$STATUS" = "PASS" ]; then
    echo "✓ Success! Next failing test to try:"
    NEXT=$(grep "FAIL" "$PUNCHLIST_CSV" | head -1 | awk -F',' '{print $1}')
    echo "  scripts/test_one_file.sh '$NEXT'"
else
    echo "✗ Still failing. Check error output above."
fi
