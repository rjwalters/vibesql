# Release cadence vs. `VERSION`

`VERSION` (and the other five `scripts/version.sh`-managed files, #5517) bumps
on nearly every merge to `main` in this repo — it tracks the tree, not a
release. GitHub Releases are a **separate, deliberately less frequent**
event. This doc states the intended cadence and what it means for the
signed-artifact `--fetch` path (Epic #4990 Phase 3, #5009/#5018/#5020),
closing the gap described in #6010.

## The decision: explicit fleet-rollable releases, not every patch

Tagging every `VERSION` bump is not the goal — this repo bumps `VERSION`
roughly as often as it merges PRs (single digits to dozens of times a day),
so per-patch releases would mean cutting, codesigning, and cosigning cross-
platform artifacts continuously for no operational benefit; most bumps are
mechanical (docs, small fixes) with no fleet-roll urgency behind them.

Instead, a release is cut when there is a **concrete reason to roll the
fleet** from it — e.g. a fix or feature that hosts are waiting on, or simply
"it's been a while and the gap is getting expensive" (the trigger that filed
#6010: cutting a release would have let `--fetch` replace four
`cargo build --release` invocations, two of them on hosts already close to
their breaker trip). There is no fixed interval (daily/weekly) requirement —
the release-vs-`VERSION` gap is expected to fluctuate, not stay pinned at
zero.

`/repo:release` (see `CLAUDE.md` § "Forge Authentication & Releasing") is the
only supported way to cut a release; it is a human/operator-invoked flow, not
something Builder/Judge/Champion trigger automatically.

## What this means for `--fetch`

`defaults/scripts/cli/loom-daemon-update.sh`'s `--fetch` (force) mode resolves
the newest GitHub Release and hard-fails rather than silently falling back to
a source build when no usable artifact resolves — by design (Epic #4990 Phase
3b). Given the cadence above, **`--fetch` is for use at or shortly after a
release boundary**, not as the default fleet-roll path on every `VERSION`
bump. The supported default remains a source build
(`loom-daemon-update.sh` with no `--fetch`, or `--no-fetch` to force it
explicitly) — `--fetch` is an accelerator once a release exists for the
version you want, not a replacement for source-build in the general case.

## Making the gap visible (#6010)

Before this doc, the only signal that `--fetch` could not reach the current
source tree was a hard failure on `--fetch` itself, or an easy-to-miss
"not newer than the installed version" message that only compared against
whatever was already installed — not against the source tree an operator was
about to build from. `loom-daemon-update.sh` now also compares the newest
resolved release against the **source tree's own `VERSION` file** (not just
the installed binary's version) and reports the gap on both paths:

- **Resolution time** (any `--check`/plain run, not just `--fetch`): when the
  newest release is behind the source tree's `VERSION`, a warning is printed:
  `Artifact path cannot reach current source: newest release ... is behind
  this source tree's VERSION (...)`.
- **`--check`**: the same gap is summarized up front —
  `Release gap: installed ..., newest release ..., source ... — the
  artifact-fetch path cannot reach current source until a release >= ... is
  cut.`
- **Forced `--fetch` hard-fail**: the refusal now names the cause when it is
  this gap, rather than only the generic "no usable release artifact was
  resolved" message.

This is advisory only — it never changes exit codes on the plain/`--check`
paths, and the pre-existing hard-fail behavior of a forced `--fetch` with no
usable artifact is unchanged. It exists so an operator planning a fleet roll
can tell, before running anything destructive, whether `--fetch` is currently
usable or whether a release needs to be cut first.

## See also

- `CLAUDE.md` § "Forge Authentication & Releasing" — how `/repo:release` works
  and what it publishes.
- [`.loom/docs/daemon-reference.md`](daemon-reference.md) — daemon self-update
  wrapper scripts (`loom-daemon-start.sh` / `loom-daemon-update.sh`) and how
  they fit the update lifecycle.
- Issue [#6010](https://github.com/rjwalters/loom/issues/6010) — the incident
  and acceptance criteria this doc satisfies.
