#!/usr/bin/env bash

# Quick conformance summary - shows both SQL:1999 and SQLLogicTest scores

SQL1999_RESULTS="target/sqltest_results.json"
SQLLOGIC_RESULTS="target/sqllogictest_results.json"

echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║     VibeSQL Conformance Summary                ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# SQL:1999 Conformance
if [ -f "$SQL1999_RESULTS" ]; then
    if command -v jq &> /dev/null; then
        if jq -e '.passed and .total and .pass_rate' "$SQL1999_RESULTS" > /dev/null 2>&1; then
            SQL1999_PASSED=$(jq -r '.passed' "$SQL1999_RESULTS")
            SQL1999_TOTAL=$(jq -r '.total' "$SQL1999_RESULTS")
            SQL1999_RATE=$(jq -r '.pass_rate' "$SQL1999_RESULTS")
            printf "📊 SQL:1999 Conformance:  %.1f%% (%d/%d tests)\n" "$SQL1999_RATE" "$SQL1999_PASSED" "$SQL1999_TOTAL"
        else
            echo "📊 SQL:1999 Conformance:  ⚠️  Unreadable results file: $SQL1999_RESULTS"
        fi
    else
        echo "📊 SQL:1999 Conformance:  (install jq for details)"
    fi
else
    echo "📊 SQL:1999 Conformance:  ❌ Not run (run: cargo test run_sql1999_conformance_suite)"
fi

# SQLLogicTest
if [ -f "$SQLLOGIC_RESULTS" ]; then
    if command -v jq &> /dev/null; then
        if jq -e '.total and .passed and .pass_rate' "$SQLLOGIC_RESULTS" > /dev/null 2>&1; then
            SLT_TOTAL=$(jq -r '.total' "$SQLLOGIC_RESULTS")
            SLT_SKIPPED=$(jq -r '.skipped // 0' "$SQLLOGIC_RESULTS")
            SLT_PASSED=$(jq -r '.passed' "$SQLLOGIC_RESULTS")
            SLT_RATE=$(jq -r '.pass_rate' "$SQLLOGIC_RESULTS")
            SLT_RELEVANT=$((SLT_TOTAL - SLT_SKIPPED))
            printf "📊 SQLLogicTest:          %.1f%% (%d/%d files, %d skipped)\n" "$SLT_RATE" "$SLT_PASSED" "$SLT_RELEVANT" "$SLT_SKIPPED"
        else
            echo "📊 SQLLogicTest:          ⚠️  Unreadable results file: $SQLLOGIC_RESULTS"
        fi
    else
        echo "📊 SQLLogicTest:          (install jq for details)"
    fi
else
    echo "📊 SQLLogicTest:          ❌ Not run (run: make test-sqllogictest)"
fi

echo ""
echo "Quick Commands:"
echo "  SQL:1999:      cargo test run_sql1999_conformance_suite"
echo "  SQLLogicTest:  ./scripts/sqllogictest run   (or: make test-sqllogictest)"
echo ""
