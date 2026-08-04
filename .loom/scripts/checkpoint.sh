#!/bin/bash

# checkpoint.sh - Manage builder checkpoints for progress tracking
#
# This script allows builders to write checkpoints as they progress through
# stages of work. The shepherd uses these checkpoints to make smarter recovery
# decisions when builders fail.
#
# Usage:
#   checkpoint.sh write --stage <stage> [options]
#   checkpoint.sh read [--json]
#   checkpoint.sh clear
#   checkpoint.sh stages
#
# Stages (in order of progression):
#   planning      - Reading issue, planning approach
#   implementing  - Writing code, making changes
#   tested        - Tests ran (pass or fail)
#   committed     - Changes committed locally
#   pushed        - Branch pushed to remote
#   pr_created    - PR exists with proper labels
#
# Examples:
#   # Write checkpoint when starting implementation
#   checkpoint.sh write --stage implementing --issue 42
#
#   # Write checkpoint after tests pass
#   checkpoint.sh write --stage tested --test-result pass --test-command "pnpm check:ci"
#
#   # Write checkpoint after commit
#   checkpoint.sh write --stage committed --commit-sha abc123
#
#   # Read current checkpoint
#   checkpoint.sh read
#
#   # Read checkpoint as JSON
#   checkpoint.sh read --json
#
# See `loom-daemon checkpoint --help` for full usage. This is a thin stub over
# that native subcommand (issue #4275, epic #4081 Phase 3 family 5 — the native
# port of the retired `loom_tools.checkpoints`); commands and flags are
# unchanged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$SCRIPT_DIR/lib/script-helper.sh"

loom_exec_script_helper checkpoint "$@"
