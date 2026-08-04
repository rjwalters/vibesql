#!/bin/bash

# validate-phase.sh - Thin stub over `loom-daemon validate-phase`.
#
# Usage:
#   validate-phase.sh <phase> <issue-number> [options]
#
# The real implementation is native (loom-daemon/src/script_helpers/validate_phase.rs),
# ported from `loom_tools.validate_phase` in issue #4275 (epic #4081 Phase 3
# family 5). Arguments and exit codes are unchanged.
#
# Exit codes:
#   0 - Contract satisfied (initially or after recovery)
#   1 - Contract failed, recovery failed or not possible
#   2 - Invalid arguments

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$SCRIPT_DIR/lib/script-helper.sh"

loom_exec_script_helper validate-phase "$@"
