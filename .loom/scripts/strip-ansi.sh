#!/usr/bin/env bash
# Strip ANSI escape sequences and clean terminal output from stdin.
#
# Thin stub over the native `loom-daemon strip-ansi` subcommand (issue #4275,
# epic #4081 Phase 3 family 5 — the native port of the retired
# `loom_tools.log_filter`). Handles cursor sequences, spinner animations,
# Claude Code TUI banners and other redraw artifacts.
#
# Usage:
#   cat .loom/logs/loom-builder-issue-42.log | ./.loom/scripts/strip-ansi.sh
#   ./.loom/scripts/strip-ansi.sh < .loom/logs/loom-builder-issue-42.log
#   ./.loom/scripts/strip-ansi.sh --file .loom/logs/loom-builder-issue-42.log
#
# With no arguments this is a real-time stdin -> stdout filter (safe for tmux
# `pipe-pane`); `--file` deep-cleans a captured log, which additionally strips
# short redraw debris — a heuristic that needs whole-file context and would
# eat legitimate short lines from a live stream (issue #2798).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/lib/script-helper.sh"

loom_exec_script_helper strip-ansi "$@"
