# Sweep-Owned Lease Renewal (Epic #6165, Phase 1: #6180)

Epic #6165 gives the `loom:building` claim a liveness dimension — a "lease".
Issue #6179 (a sibling Phase 1 issue) defines the write-only lease record
format and writes it once, at the moment a dispatch acquires
`loom:building`. **This document covers the other half: keeping that record
fresh for the lifetime of the sweep that holds the claim.**

## The lease record this renews

At the time this script was written, #6179 had not yet merged. The format
below is reproduced from #6179's own issue body (the epic's suggested
shape) so this renewal mechanism has a single, precise, testable contract
regardless of merge order — coordinate any format change with #6179's own
doc (`defaults/docs/lease-record.md`, once it lands).

A lease record is an issue comment whose body's literal first line is:

```
<!-- loom:lease host=<host> sweep=<sweep-id> -->
```

Everything after the marker's closing `-->` is free-form prose. Machine
readers — this renewal script included — must never depend on that prose,
only on locating the comment via
`startswith("<!-- loom:lease host=")`. **The liveness signal a reader must
consult is the comment's own forge-assigned `updated_at` timestamp, never
any value embedded in the marker text.**

## Why the sweep renews, not the daemon

This is the load-bearing, non-obvious constraint from the epic body: role
agents run as transient scopes parented to `systemd --user` and routinely
outlive the daemon process that spawned them (loom#6129). Supervisor
liveness (is `loom-daemon` up?) is therefore not the same thing as work
liveness (is the sweep still actively working this issue?). If the daemon
owned renewal, a daemon restart would let a live sweep's lease expire, and a
peer host would then have positive-looking "evidence" to reclaim work that
was never actually abandoned — reproducing the exact bug this epic exists to
fix, from a different direction.

Renewal must therefore be driven by the process actually doing the work:
sweep alive → lease renewed and fresh; sweep dead → renewal stops → lease
expires on its own → a reclaim by another host (Phase 2) is then justified
by positive evidence, not inference from a missing broadcast.

## Mechanism

`defaults/scripts/sweep-lease-renew.sh` (mirrored, via a symlink, into
`.loom/scripts/`) provides:

- **`start <issue> [--interval SECS] [--watch-pid PID] [--host H] [--sweep-id S]`**
  — resolve a liveness PID (the same ancestor-walk `sweep-run-registry.sh`
  uses to find the long-lived `claude -p /loom:sweep ...` orchestrator
  process, never the one-shot Bash-subshell PID of the tool call that
  invokes `start`), then spawn ONE detached background loop that, every
  `--interval` seconds (default 300 = 5 minutes, overridable via
  `SWEEP_LEASE_RENEW_INTERVAL_SECS` too — the epic's suggested cadence,
  pending #6181's real-world measurement), best-effort renews the lease for
  `<issue>` as long as the watched PID stays alive. Prints the loop's PID.
- **`renew-once <issue> [--host H] [--sweep-id S]`** — one synchronous
  renewal cycle: locate the newest comment on `<issue>` whose body starts
  with the lease marker (or, if `--host`/`--sweep-id` are both given, the
  comment whose marker line matches them exactly), and idempotently PATCH
  it. Exit 0 on success, 2 when no matching lease comment exists (a normal,
  silent no-op — not every sweep is daemon-dispatched), 1 on a `gh` failure.
- **`stop <PID>`** — best-effort kill of a loop PID. Not required for
  correctness; the loop already self-terminates.

### Renewal = idempotent PATCH, never a new comment

GitHub does not reliably advance a comment's `updated_at` on a byte-for-byte
identical PATCH, so `renew-once` rewrites a single trailing HTML-comment
line — its own sub-marker, `<!-- loom:lease-renewed at=... by=... -->` — with
a fresh timestamp on every call. This guarantees the body actually changes
(so `updated_at` genuinely advances) while leaving the first-line lease
marker byte-identical, so a `startswith()` reader never sees it move. Like
the primary marker, `loom:lease-renewed`'s `at=` value is for human
debugging only — no reader may treat it as authoritative; the forge's own
`updated_at` always is. A second (or Nth) renewal *replaces* this trailing
line rather than appending another copy, so a long-running sweep's lease
comment never grows unbounded and no duplicate comments ever accumulate.

### Why this needs no explicit `--sweep-id` in normal use

`sweep.md`'s "Step 1a — daemon self-claim check" is the only place `start`
is invoked, and it fires only for the ONE issue `SweepRegistry::dispatch`
told this session it owns (`--claim-owned N` / `LOOM_SWEEP_CLAIM_OWNED=N`).
By construction, the newest lease comment on that issue at that point in
pre-flight IS this session's own — the daemon wrote it immediately before
spawning this exact child process, so "most recent lease comment" and "my
own lease comment" coincide. `--host`/`--sweep-id` remain available as an
exact-match filter for precision (tests, or a future scenario where that
assumption no longer holds), but ordinary sweep usage does not need them.

## Where it is wired in

`defaults/.claude/commands/loom/sweep.md`, "1. Per-issue pre-flight" → "Step
1a — daemon self-claim check": immediately after confirming this session
owns the daemon's claim on issue `N`, before falling through to step 2,
```bash
./.loom/scripts/sweep-lease-renew.sh start "$N" > /dev/null 2>&1 || true
```
Fire-and-forget, best-effort, non-blocking — mirrors #6179's own
write-on-dispatch contract: a failure here changes nothing about whether the
sweep runs. For any sweep with no daemon-dispatched claim on this run
(manual invocation, GH Actions cron, `--no-daemon`, Mode C), Step 1a's
self-claim signal is never true, so this line never executes and there is no
lease to renew anyway.

## What this does not do (Phase 1 scope)

Nothing in the reclamation/dispatch decision path reads the lease or its
renewals yet — that is Phase 2. This document, like #6179's own, is
write-only: it exists so Phase 2 (reclamation) and Phase 3 (fencing) can
consume the renewal cadence/format without re-deriving it. It also does not
fix the acquisition race #4028 documented; Phase 3 bounds that cost, this
phase does not touch it.

See also: [`lease-record.md`](lease-record.md) — #6179's own doc, the
authoritative definition of the marker format and the dispatch-time write
this renewal loop keeps fresh. Also
[`lease-renewal-measurement.md`](lease-renewal-measurement.md) — the
write-volume measurement methodology and a projected (not yet measured)
estimate against this loop's `~5 min` default cadence and the forge's rate
limits (#6181).
