#!/bin/bash

# sweep-experiment.sh - Thin stub delegating to `loom-daemon sweep-experiment` (#3725)
#
# The /loom:sweep skill shells out to this for the deterministic parts of the
# model-cost experiment: tri-state mode resolution, per-issue arm assignment, the
# startup banner, the durable JSONL append, and the harvest reader. Keeping the
# arithmetic in compiled code (not in the LLM-executed markdown) makes arm
# assignment byte-for-byte deterministic and resume-safe.
#
# Ported from Python to native Rust in issue #4275 (epic #4081 Phase 3 family
# 5); subcommands, flags and output shapes are unchanged. Arm A resolves its
# model through the same code path as `resolve-model.sh`, so the experiment and
# the dispatch path can never disagree (the #4060 contract).
#
# Usage:
#   sweep-experiment.sh resolve-mode
#   sweep-experiment.sh assign-arm --issue N [--complexity complex|routine] [--format json]
#   sweep-experiment.sh banner --issue N [--complexity ...]
#   sweep-experiment.sh record --issue N --phase P --role R [--model M --arm A ...]
#   sweep-experiment.sh harvest [--archive-dir DIR] [--format text|json]
#
# The harvest subcommand is ALSO reachable via `agent-metrics.sh --model-experiment`
# (issue #3725 AC), which forwards here so operators find it next to the existing
# `--by-model` (#3482) cost dimension.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$SCRIPT_DIR/lib/script-helper.sh"

loom_exec_script_helper sweep-experiment "$@"
