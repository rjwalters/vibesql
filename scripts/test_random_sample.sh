#!/usr/bin/env bash
# Test a sample of random/* tests to see current failure patterns

set -e

echo "Testing sample of random/* tests..."
echo "===================================="
echo ""

# Test one file from each subcategory
tests=(
    "third_party/sqllogictest/test/random/aggregates/slt_good_0.test"
    "third_party/sqllogictest/test/random/expr/slt_good_0.test"
    "third_party/sqllogictest/test/random/select/slt_good_0.test"
    "third_party/sqllogictest/test/random/groupby/slt_good_0.test"
)

for test in "${tests[@]}"; do
    echo "Testing: $test"
    SQLLOGICTEST_FILE="$test" timeout 20 cargo test --release -p vibesql --test sqllogictest_runner run_single_test_file 2>&1 | \
        grep -E "(PASSED|FAILED|Error:|test result)" | tail -10 || true
    echo "---"
    echo ""
done

echo "Sample testing complete!"
