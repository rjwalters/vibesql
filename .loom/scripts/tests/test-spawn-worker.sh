#!/usr/bin/env bash
# test-spawn-worker.sh — Tests for spawn-worker.sh (runtime dispatcher).
#
# Style matches test-spawn-claude.sh — plain bash, hand-rolled assertions.
# Bats is NOT used in this repository.
#
# The dispatcher resolves its runner (`spawn-<runtime>.sh`) relative to its own
# directory and exec's it. To exercise dispatch without triggering real token
# selection or spawning a real `claude`, each test stages a temp scripts dir
# holding a COPY of spawn-worker.sh, a copy of lib/config-resolver.sh, and
# lightweight STUB runners (spawn-claude.sh / spawn-codex.sh) that just echo
# their args. So no test ever reaches the real spawn-claude.sh token path.
#
# Usage:
#   ./.loom/scripts/tests/test-spawn-worker.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="$3"
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

assert_contains() {
    local needle="$1"
    local haystack="$2"
    local msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ "$haystack" == *"$needle"* ]]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg"
        echo "    Expected substring: '$needle'"
        echo "    In: '$haystack'"
    fi
}

assert_not_contains() {
    local needle="$1"
    local haystack="$2"
    local msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ "$haystack" != *"$needle"* ]]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg"
        echo "    Unexpected substring: '$needle'"
        echo "    In: '$haystack'"
    fi
}

# ============================================================
# Stage a temp scripts dir with a copy of the dispatcher + stub runners.
# ============================================================

STAGE="$(mktemp -d)"
WS="$(mktemp -d)"           # fake workspace holding .loom/config.json
trap 'rm -rf "$STAGE" "$WS"' EXIT

mkdir -p "$STAGE/lib" "$WS/.loom"
cp "$SCRIPTS_DIR/spawn-worker.sh" "$STAGE/spawn-worker.sh"
cp "$SCRIPTS_DIR/lib/config-resolver.sh" "$STAGE/lib/config-resolver.sh"
chmod +x "$STAGE/spawn-worker.sh"

# Stub runners: print each arg on its own bracketed line so args containing
# spaces are observable verbatim (a plain `$*` would collapse them).
_make_stub() {
    local path="$1" label="$2" rc="${3:-0}"
    cat > "$path" <<STUB
#!/usr/bin/env bash
echo "stub-$label reached"
printf 'stub-$label arg=[%s]\n' "\$@"
exit $rc
STUB
    chmod +x "$path"
}
_make_stub "$STAGE/spawn-claude.sh" "claude"
_make_stub "$STAGE/spawn-codex.sh" "codex"

WORKER="$STAGE/spawn-worker.sh"

# LOOM_CONFIG_DEFAULTS_FILE="" disables the private-defaults config tier so a
# stray ~/.local/share/loom/config/defaults.json on the host can't perturb
# resolution. Every invocation below runs with a controlled workspace.
run_worker() {
    # usage: run_worker <ws> [env assignments as VAR=val ...] -- <args...>
    local ws="$1"; shift
    local -a envs=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do envs+=("$1"); shift; done
    shift || true   # drop the --
    env -u LOOM_RUNTIME \
        LOOM_WORKSPACE="$ws" \
        LOOM_CONFIG_DEFAULTS_FILE="" \
        ${envs[@]+"${envs[@]}"} \
        bash "$WORKER" "$@" 2>&1 || true
}

_have_jq=true
command -v jq >/dev/null 2>&1 || _have_jq=false

# ============================================================
# Section 1: default resolution -> spawn-claude.sh
# ============================================================

echo "Testing spawn-worker.sh default resolution..."

# No env, no config file at all -> built-in default "claude".
output="$(run_worker "$WS" -- -p ping)"
assert_contains "stub-claude reached" "$output" \
    "no env + no config dispatches to spawn-claude.sh (zero behavior change)"
assert_contains "runtime=claude (from default)" "$output" \
    "default resolution logs source=default"
assert_not_contains "stub-codex reached" "$output" \
    "default resolution does NOT reach any other runner"

# Args forwarded verbatim.
assert_contains "stub-claude arg=[-p]" "$output" "args passthrough: -p"
assert_contains "stub-claude arg=[ping]" "$output" "args passthrough: ping"

# ============================================================
# Section 2: LOOM_RUNTIME env override
# ============================================================

echo ""
echo "Testing spawn-worker.sh LOOM_RUNTIME env override..."

# Env selects codex even with no config.
output="$(run_worker "$WS" LOOM_RUNTIME=codex -- -p ping)"
assert_contains "stub-codex reached" "$output" \
    "LOOM_RUNTIME=codex dispatches to spawn-codex.sh"
assert_contains "runtime=codex (from env (LOOM_RUNTIME))" "$output" \
    "env resolution logs source=env"
assert_not_contains "stub-claude reached" "$output" \
    "env override does NOT reach spawn-claude.sh"

# Empty LOOM_RUNTIME is treated as unset (falls through to default).
output="$(run_worker "$WS" LOOM_RUNTIME= -- -p ping)"
assert_contains "stub-claude reached" "$output" \
    "empty LOOM_RUNTIME falls through to the default (claude)"
assert_contains "runtime=claude (from default)" "$output" \
    "empty LOOM_RUNTIME resolves source=default"

# ============================================================
# Section 3: config runtimes.default resolution + precedence
# ============================================================

echo ""
echo "Testing spawn-worker.sh config runtimes.default..."

if [[ "$_have_jq" != "true" ]]; then
    echo "  (skipping config-based tests — jq not installed)"
else
    # config runtimes.default=codex, no env -> codex.
    printf '{ "runtimes": { "default": "codex" } }\n' > "$WS/.loom/config.json"
    output="$(run_worker "$WS" -- -p ping)"
    assert_contains "stub-codex reached" "$output" \
        "config runtimes.default=codex dispatches to spawn-codex.sh"
    assert_contains "runtime=codex (from config (runtimes.default))" "$output" \
        "config resolution logs source=config"

    # env beats config: config says codex, env says claude -> claude.
    output="$(run_worker "$WS" LOOM_RUNTIME=claude -- -p ping)"
    assert_contains "stub-claude reached" "$output" \
        "LOOM_RUNTIME env overrides config runtimes.default"
    assert_contains "runtime=claude (from env (LOOM_RUNTIME))" "$output" \
        "env-over-config resolution logs source=env"

    # A config with no runtimes block falls through to the built-in default.
    printf '{ "commit": { "signoff": true } }\n' > "$WS/.loom/config.json"
    output="$(run_worker "$WS" -- -p ping)"
    assert_contains "runtime=claude (from default)" "$output" \
        "config present but no runtimes block resolves to default claude"

    rm -f "$WS/.loom/config.json"
fi

# ============================================================
# Section 4: unknown runtime -> exit 78 (EX_CONFIG)
# ============================================================

echo ""
echo "Testing spawn-worker.sh unknown-runtime failure..."

# Unknown from env: capture exit code and message.
set +e
out_unknown="$(env -u LOOM_RUNTIME \
    LOOM_WORKSPACE="$WS" LOOM_CONFIG_DEFAULTS_FILE="" LOOM_RUNTIME=bogus \
    bash "$WORKER" -p ping 2>&1)"
rc_unknown=$?
set -e
assert_eq "78" "$rc_unknown" "unknown runtime exits 78 (EX_CONFIG)"
assert_contains "Unknown runtime 'bogus'" "$out_unknown" \
    "unknown-runtime message names the resolved runtime"
assert_contains "env (LOOM_RUNTIME)" "$out_unknown" \
    "unknown-runtime message names the source (env)"
assert_contains "Available runtimes on disk:" "$out_unknown" \
    "unknown-runtime message lists available runners"
assert_contains "claude" "$out_unknown" \
    "available-runtimes list includes the staged claude runner"

# Unknown from config: message names the config source.
if [[ "$_have_jq" == "true" ]]; then
    printf '{ "runtimes": { "default": "nope" } }\n' > "$WS/.loom/config.json"
    set +e
    out_cfg_unknown="$(env -u LOOM_RUNTIME \
        LOOM_WORKSPACE="$WS" LOOM_CONFIG_DEFAULTS_FILE="" \
        bash "$WORKER" -p ping 2>&1)"
    rc_cfg_unknown=$?
    set -e
    assert_eq "78" "$rc_cfg_unknown" "unknown runtime from config exits 78"
    assert_contains "config (runtimes.default)" "$out_cfg_unknown" \
        "unknown-from-config message names the config source"
    rm -f "$WS/.loom/config.json"
fi

# ============================================================
# Section 5: args pass through verbatim (incl. spaces) + exit passthrough
# ============================================================

echo ""
echo "Testing spawn-worker.sh arg + exit-code passthrough..."

# Args with embedded spaces survive as single arguments.
output="$(run_worker "$WS" -- -p "hello world" --flag --model=x)"
assert_contains "# LOOM_RUNTIME_RESOLVED runtime=claude" "$output" \
    "dispatcher emits the runtime-neutral resolution marker"
assert_contains "stub-claude arg=[hello world]" "$output" \
    "an arg containing a space is forwarded as ONE argument"
assert_contains "stub-claude arg=[--flag]" "$output" "bare flag forwarded verbatim"
assert_contains "stub-claude arg=[--model=x]" "$output" "--key=value forwarded verbatim"

# The runner's exit code is passed through via exec.
_make_stub "$STAGE/spawn-claude.sh" "claude" 42
set +e
env -u LOOM_RUNTIME LOOM_WORKSPACE="$WS" LOOM_CONFIG_DEFAULTS_FILE="" \
    bash "$WORKER" -p ping >/dev/null 2>&1
rc_pass=$?
set -e
assert_eq "42" "$rc_pass" "runner exit code is passed through via exec"
_make_stub "$STAGE/spawn-claude.sh" "claude" 0   # restore

# ============================================================
# Summary
# ============================================================

echo ""
echo "==================================="
echo "Tests run:    $TESTS_RUN"
echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
if [[ $TESTS_FAILED -gt 0 ]]; then
    echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"
    exit 1
fi
echo "All tests passed."
