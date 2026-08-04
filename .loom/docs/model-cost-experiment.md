# Model-Cost Experiment

Reference detail for the `/loom:sweep` model-cost A/B experiment (#3725). Treated
as the authoritative behavior contract by
[`docs/model-selection-retune.md`](https://github.com/rjwalters/loom/blob/main/docs/model-selection-retune.md).

### Model-Cost Experiment (canary A/B, #3725)

`/loom:sweep` can instrument a run to produce the balanced A/B evidence the
measurement-gated Builder `opus → sonnet` retune (#3718) needs — you cannot
generate it by observation alone, since passive collection only ever measures
whatever the current Builder default is. **Off by default; zero behavior change
unless you turn it on.** Tri-state `sweep.modelExperiment` /
`LOOM_MODEL_EXPERIMENT` (env-over-config, string-valued like `guards.rmScope`):

| Mode | Behavior |
|------|----------|
| `off` (default) | No instrumentation. No `.loom/stats/` file. Byte-for-byte unchanged. |
| `observe` | Passive: one JSONL record per phase to `.loom/stats/sweep-model-stats.jsonl`. No model forcing. Safe anywhere. |
| `experiment` | Active A/B: Builder forced to the per-issue arm's model. **Canary-only.** |

```bash
# Observe on any sweep (no behavior change, just records the outcome-chain):
LOOM_MODEL_EXPERIMENT=observe claude -p "/loom:sweep 123" --dangerously-skip-permissions

# Experiment on a canary (must confirm the canary — else it downgrades to observe):
LOOM_MODEL_EXPERIMENT=experiment LOOM_MODEL_EXPERIMENT_CANARY=1 \
  claude -p "/loom:sweep 123" --dangerously-skip-permissions
```

**Two arms** map onto #3718's inequality: **Arm A = opus-first** (Builder→opus),
**Arm B = sonnet-first + escalate-on-Judge-rejection** (Builder→sonnet, escalating
via the `sweep.escalation` ladder). Arm assignment is a **deterministic,
resume-safe** function of the issue number, **stratified by the Curator complexity
marker** (#3702) so both arms see a comparable difficulty mix — a killed-and-resumed
sweep re-lands the same arm. In `experiment` mode the tier-2.5 complexity bump is
**suppressed** (the marker is used only as the stratification key), so a
`complex`-marked issue on Arm B stays sonnet and the A/B is not confounded.

**Guardrails:** off by default; `observe` safe anywhere; `experiment` refuses to
run on a non-canary target and loudly downgrades to `observe` unless an
**uncommitted** signal confirms a canary — the `LOOM_MODEL_EXPERIMENT_CANARY=1`
env var or the gitignored `.loom/CANARY` sentinel file. The confirmation must be
uncommitted **by design** (#3731): the committed `sweep.modelExperimentCanary`
config flag is **no longer** an accepted confirmation (it would propagate with a
copied config via `defaults/`, firing experiment on production). A git-tracked
`.loom/CANARY` is likewise refused. The `sweep.modelExperiment` *mode* may still
live in committed config — it is inert without the uncommitted confirmation. A
loud startup banner names the active mode, the canary confirmation source, and,
in `experiment`, the arm assigned to each issue. `.loom/stats/` and `.loom/CANARY`
are gitignored.

**Harvesting the evidence:**

```bash
./.loom/scripts/agent-metrics.sh --model-experiment --archive-dir "$LOOM_TRANSCRIPT_ARCHIVE"
```

The load-bearing signal is the **deterministic outcome-chain** (arm / model /
attempt / Judge verdict / Doctor-cycle count / complexity) — that alone answers
#3718's inequality (first-attempt Judge-pass rate + mean Doctor cycles × model
price), and it is stamped into the durable store per phase.

**On token fidelity — read this before trusting a cost number.** There is **no
per-phase real-token split at the Task-result boundary** (the harness does not
surface per-subagent `usage` when a Task returns). Instead, **exact per-role cost
is recovered at harvest time** by parsing each role subagent's durable
`agent-<id>.jsonl` transcript — each Builder / Judge / Doctor invocation is its own
transcript with full per-message `usage` (input/output + cache read/creation
split + model), so this is true per-role granularity, not a whole-process
aggregate. Cost uses the same cache-aware per-model pricing as `loom-daemon`'s
`resource_usage.rs`. Harvest locates transcripts through the #3726 transcript
archive's `agent-<id>`-keyed index (`--archive-dir` = `LOOM_TRANSCRIPT_ARCHIVE`),
joined on the agent-id stamped in each stats record. Over a **multi-day canary**,
run harvest periodically (cron) so usage is extracted before `~/.claude/projects`
is pruned — or rely on the #3726 archive as the durable backstop. Each record
carries a `token_fidelity` tag (`transcript` | `sweep-aggregate-log` | `none`) so
you know exactly what a cost figure came from.
