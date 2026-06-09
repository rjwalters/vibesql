# Raft + MVCC: Decomposition of the WAN-Capable Distributed SQL Initiative

**Parent issue:** [#4460 — Architect: WAN-Capable, Strongly Consistent SQL Database Using Raft + MVCC](https://github.com/rjwalters/vibesql/issues/4460)

This document captures the decomposition of the Raft + MVCC architectural proposal into focused, builder-claimable sub-issues. It exists so that future contributors (and future curator passes) have a single place to find the shape of the initiative without re-reading the umbrella issue and its scattered comments.

The proposal in #4460 describes a CockroachDB-style architecture: per-range Raft groups for replication, MVCC for snapshot reads, intent-based distributed transactions, leader leases and follower reads for WAN-latency mitigation. Built end-to-end, this is months of work. The decomposition below breaks it into pieces that can each ship in 1-3 PRs.

## Tracks and current status

Two largely orthogonal tracks of work:

| Track | Status as of decomposition |
|---|---|
| **MVCC** (row versioning + visibility predicates) | In flight. Phase 1a/1b/1c merged. Phase 1d (#5151) active. Decomposition by an earlier curator pass on #5136. |
| **Raft** (consensus, replication, sharding, distributed txn) | Not started. This decomposition. |

MVCC Phase 1 (single-node row versioning) is consensus-protocol-agnostic, so it can complete independently. The Raft track depends on MVCC Phase 1 only at the late integration points (Phases A4 and B1 below).

## Sub-issue map (Raft track)

The Raft track is sequenced in three phases — A (foundations), B (read-path and ordering), C (distributed transactions) — plus an A0 prerequisite for sharding.

```
A0 (Range metadata)  ─────────────────────────┐
A1 (Library choice + adapter trait)           │
   ↓                                          │
A2 (WAL-backed consensus log)                 │
   ↓                                          │
A3 (AppendEntries + RequestVote RPC)          │
   ↓                                          │
A4 (Snapshot transfer + MVCC-GC interlock) ←──┤
   ↓                                          │
B1 (State-machine integration — MVCC apply)   │
   ↓                                          │
B2 (Leader leases + follower reads)           │
   ↓                                          │
C1 (Distributed txn: intents + commit record) ┘
```

| # | Title | Depends on | Notes |
|---|---|---|---|
| [#5195](https://github.com/rjwalters/vibesql/issues/5195) | Raft A1 — choose consensus library + draft adapter trait | nothing | Decision + scaffolding only. ADR + trait + single-node loopback. |
| [#5196](https://github.com/rjwalters/vibesql/issues/5196) | Raft A2 — persist consensus log via WAL | #5195 | Reuses existing WAL machinery; bumps WAL_VERSION. |
| [#5197](https://github.com/rjwalters/vibesql/issues/5197) | Raft A3 — AppendEntries + RequestVote over network | #5196 | First multi-node MVP. 3-node cluster, fault tests. |
| [#5198](https://github.com/rjwalters/vibesql/issues/5198) | Raft A4 — snapshot transfer + MVCC GC interlock | #5197 + MVCC Phase 1 | The genuine MVCC × Raft interaction point. |
| [#5199](https://github.com/rjwalters/vibesql/issues/5199) | Raft B1 — wire MVCC executor as state machine | #5197 + #5198 + #5151 | Where SQL writes actually become replicated. Longest pole. |
| [#5200](https://github.com/rjwalters/vibesql/issues/5200) | Raft B2 — leader leases + bounded-staleness follower reads | #5199 | The WAN-latency win. |
| [#5201](https://github.com/rjwalters/vibesql/issues/5201) | Raft C1 — distributed multi-shard transactions | #5199 + #5202 | Intent + transaction-record protocol. |
| [#5202](https://github.com/rjwalters/vibesql/issues/5202) | Raft A0 — range/shard metadata store | #5195 | Prerequisite for sharding. Parallel-safe with A2/A3. |

### Next-actionable sub-issues (not blocked)

- **#5195 (Raft A1)** — fully unblocked. The ADR + trait scaffolding can start immediately.
- **#5202 (Raft A0)** — blocked only on #5195. Can be picked up in parallel by a second builder once the consensus library is chosen.

Everything else has a clear dependency chain back to one of these two.

## What is deliberately *not* in this decomposition

Several pieces from the architect proposal were considered and explicitly deferred or dropped from the initial slice:

- **Multi-region routing** (proposal §6 partly). The plumbing for follower reads (B2) and leader placement hints lands in this slice; full region-aware client routing is a follow-on once a real multi-region deployment exists to validate against.
- **Serializable isolation across shards** (proposal §5). C1 delivers snapshot isolation across shards via the intent protocol. SSI / write-skew prevention across shards is intentionally a follow-on issue (call it "Raft Phase C2") to keep C1 reviewable.
- **Schema migrations as distributed transactions** (proposal §7). The architect proposal calls for schema changes to flow through MVCC + Raft. Worth its own issue once C1 is in. Not blocked by anything else; can be picked up later.
- **Auto-rebalancing / range splits / merges**. The metadata store in A0 is shaped to accommodate splits but the splitter itself is a separate concern, more naturally tackled after the static-config cluster is provably correct.
- **Always-available writes during partitions / CRDT modes / leaderless serializable commits**. The proposal flags these as non-goals; this decomposition concurs.

## Consensus protocol decision is deferred to #5195

The proposal specifies Raft, but the external comment on #4460 suggesting Viewstamped Replication (VR/VSR, as used by TigerBeetle) is real and worth engaging with. The ADR called for in #5195 is the right place to make that decision once, with file-level rationale, rather than informally in the umbrella thread. MVCC Phase 1 is protocol-agnostic, so this decision does not need to happen before MVCC Phase 1 completes.

## Why the MVCC track is not re-decomposed here

MVCC Phase 1 was already decomposed by a curator pass on #5136 into four sub-phases (1a-1d). Three are merged and 1d is active. Re-doing that work would be duplicative. After Phase 1 lands:

- A future curator pass should file MVCC Phase 2 issues (proposal §2-§3: HLC timestamps, garbage collection, snapshot-read optimization for follower reads). The natural moment to do that is when #5151 closes.
- HLC timestamps in particular should be filed alongside Raft Phase B1 (#5199), where commit ordering becomes a cross-cutting concern.

## Update protocol for this document

This file is the canonical decomposition snapshot. If sub-issues are added, removed, renumbered, or re-scoped, update the table above in the same PR. If a sub-issue is closed without being completed (e.g., replaced by a different approach), note the closure here with a one-line rationale rather than silently dropping it.
