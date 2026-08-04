#!/bin/bash
# check-usage.sh - Query Claude API usage via Anthropic OAuth API
#
# Usage:
#   ./.loom/scripts/check-usage.sh           # Returns JSON with usage data
#   ./.loom/scripts/check-usage.sh --status  # Human-readable status
#
# Exit codes:
#   0 - Data returned successfully
#   1 - Token not found or API call failed
#
# Thin stub over the native `loom-daemon usage` subcommand (issue #4275, epic
# #4081 Phase 3 family 5 — the native port of the retired
# `loom_tools.common.usage`). Reads the Claude Code OAuth token from the macOS
# Keychain and calls the Anthropic usage API, caching to
# `.loom/usage-cache.json`. Flags and exit codes are unchanged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/lib/script-helper.sh"

loom_exec_script_helper usage "$@"
