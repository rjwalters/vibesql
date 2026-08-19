#!/usr/bin/env bash
# test-classify-error.sh - Unit tests for lib/classify-error.sh (#5631)
#
# Table-driven coverage of `classify_error`'s TOKEN_EXHAUSTED / SESSION_LIMIT /
# MODEL_CREDITS_EXHAUSTED regexes against known Claude CLI limit strings, per
# the "Suggested test" spec in issue #5631: session, weekly, monthly usage,
# monthly spend, per-model, plus the SESSION_LIMIT precedence edge case that
# motivated the ordering in the first place (#3947) and the credit-exhaustion
# class added in #5687.
#
# Usage:
#   ./.loom/scripts/tests/test-classify-error.sh

set -uo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$TEST_DIR/.." && pwd)"
SRC="$SCRIPTS_DIR/lib/classify-error.sh"

# shellcheck source=../lib/classify-error.sh
source "$SRC"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

assert_eq() {
    local expected="$1" actual="$2" msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ "$expected" == "$actual" ]]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg"
        echo "    Expected: '$expected'"
        echo "    Actual:   '$actual'"
    fi
}

echo "--- classify_error: TOKEN_EXHAUSTED phrase family (#5631) ---"

# Table: description | CLI output | expected category
while IFS='|' read -r desc output expected; do
    [[ -z "$desc" ]] && continue
    actual="$(classify_error "$output" 1)"
    assert_eq "$expected" "$actual" "$desc"
done <<'EOF'
session limit|You've hit your session limit|TOKEN_EXHAUSTED
weekly limit|You've hit your weekly limit|TOKEN_EXHAUSTED
monthly usage limit|You've hit your monthly usage limit|TOKEN_EXHAUSTED
monthly spend limit (issue #5631 regression)|You've hit your monthly spend limit - raise it at claude.ai/settings/usage|TOKEN_EXHAUSTED
bare limit, no filler words|You've hit your limit|TOKEN_EXHAUSTED
per-model ceiling (#4501)|You've reached your Fable 5 limit. Run /usage-credits to continue or switch models with /model.|TOKEN_EXHAUSTED
out of extra usage|You are out of extra usage for this billing period|TOKEN_EXHAUSTED
used 100% of weekly limit|You have used 100% of your weekly limit|RECOVERABLE
EOF

echo
echo "--- classify_error: SESSION_LIMIT precedence is unaffected by the widened TOKEN_EXHAUSTED regex (#3947) ---"

assert_eq "SESSION_LIMIT" "$(classify_error "maximum number of concurrent sessions reached" 1)" \
    "concurrent-session capacity fault stays SESSION_LIMIT, not TOKEN_EXHAUSTED"
assert_eq "SESSION_LIMIT" "$(classify_error "Another session is already active" 1)" \
    "'another session is already active' stays SESSION_LIMIT"

echo
echo "--- classify_error: MODEL_CREDITS_EXHAUSTED, the per-model-tier credit class (#5687) ---"

# The incident wording (2026-08-08, wave-width-6 /loom:sweep all) plus the
# nearby phrasings, and — critically — the NEGATIVE cases that must NOT be
# swallowed by the new branch.
while IFS='|' read -r desc output expected; do
    [[ -z "$desc" ]] && continue
    actual="$(classify_error "$output" 1)"
    assert_eq "$expected" "$actual" "$desc"
done <<'EOF'
the observed incident wording|You're out of usage credits. Run /usage-credits or switch models with /model.|MODEL_CREDITS_EXHAUSTED
bare "out of credits"|You are out of credits|MODEL_CREDITS_EXHAUSTED
"ran out of usage credits"|The session ran out of usage credits|MODEL_CREDITS_EXHAUSTED
"no usage credits remaining"|Your account has no usage credits remaining|MODEL_CREDITS_EXHAUSTED
"insufficient credits"|Request failed: insufficient credits for this model|MODEL_CREDITS_EXHAUSTED
case-insensitive|YOU'RE OUT OF USAGE CREDITS|MODEL_CREDITS_EXHAUSTED
negative: unrelated "credit" mention|Payment failed: your credit card was declined|RECOVERABLE
negative: crediting an author|Please credit the original author in the commit message|RECOVERABLE
negative: "credits" with no exhaustion verb|Remaining usage credits: 4200|RECOVERABLE
EOF

echo
echo "--- classify_error: #5687's branch must NOT steal any pre-existing vector (order: after TOKEN_EXHAUSTED) ---"

# #4501's live per-model ceiling mentions /usage-credits too. It predates
# #5687 and must keep TOKEN_EXHAUSTED, which is only true because the new
# branch is checked AFTER the exhaustion regex.
assert_eq "TOKEN_EXHAUSTED" \
    "$(classify_error "You've reached your Fable 5 limit. Run /usage-credits to continue or switch models with /model." 1)" \
    "#4501 per-model ceiling still TOKEN_EXHAUSTED despite mentioning credits"
assert_eq "TOKEN_EXHAUSTED" \
    "$(classify_error "You've hit your weekly limit. You are out of credits." 1)" \
    "a mixed message keeps the older TOKEN_EXHAUSTED classification (order is load-bearing)"
# Exit-code-first (#3233) applies to the new branch like every other.
assert_eq "SUCCESS" "$(classify_error "You're out of usage credits" 0)" \
    "a CLEAN exit whose output mentions credit exhaustion stays SUCCESS (#3233)"
# The capacity fault still wins over both exhaustion classes (#3947).
assert_eq "SESSION_LIMIT" \
    "$(classify_error "maximum number of concurrent sessions reached; you are out of usage credits" 1)" \
    "SESSION_LIMIT still precedes the credit class"

echo
echo "--- classification_is_transient: TOKEN_EXHAUSTED stays retryable (rotation path consumes it first) ---"

for _category in TOKEN_EXHAUSTED MODEL_CREDITS_EXHAUSTED; do
    if classification_is_transient "$_category"; then
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $_category is transient (retry/rotate, not fatal)"
    else
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $_category is transient (retry/rotate, not fatal)"
    fi
done

echo
echo "Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"
[[ $TESTS_FAILED -eq 0 ]] || exit 1
