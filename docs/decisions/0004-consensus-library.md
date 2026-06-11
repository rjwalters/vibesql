# ADR-0004: Consensus Library and Replication Topology

**Status**: Accepted

**Date**: 2026-06-11

**Deciders**: Claude Code + rwalters

**Related**:
- Issue [#4460](https://github.com/rjwalters/vibesql/issues/4460) - WAN replication architectural decomposition (parent)
- Issue [#5195](https://github.com/rjwalters/vibesql/issues/5195) - Raft Phase A1: this decision + adapter scaffolding
- Issue [#5200](https://github.com/rjwalters/vibesql/issues/5200) - Phase B2: leader leases + bounded-staleness follower reads
- Issue [#5201](https://github.com/rjwalters/vibesql/issues/5201) - Phase C1: distributed multi-shard transactions (deferred)
- Issue [#5202](https://github.com/rjwalters/vibesql/issues/5202) - Phase A0: range sharding / per-range Raft groups (deferred)
- [ADR-0001](0001-language-choice.md) - Rust implementation language

## Context and Problem Statement

VibeSQL's WAN replication track (#4460) needs a consensus layer so that a
cluster of nodes can agree on a single ordered log of writes. Two decisions
must be made before any replication code can land:

1. **Which consensus implementation do we build on?** A mature Rust library,
   or a hand-rolled protocol?
2. **What replication topology do we target?** One Raft group replicating the
   whole database (rqlite/dqlite/Turso model), or many Raft groups each owning
   a key range (CockroachDB/TiKV model, as originally sketched in #4460)?

This ADR records both decisions and lands a thin, engine-agnostic adapter
trait (`ConsensusBackend`) in a new `vibesql-consensus` crate so the rest of
the Raft track can proceed against a stable interface even if the underlying
engine (or topology) changes later.

**Constraints**:
- VibeSQL is async on **tokio** (server crate); the consensus layer must fit
  that runtime without thread-bridging hacks.
- Sole-developer project: anything that requires hand-maintaining a consensus
  protocol implementation is a long-term liability.
- This phase is **decision + scaffolding only** — no multi-node replication,
  no log-to-WAL wiring, no membership changes, no RPC code.

## Decision Drivers

* **Correctness risk** - Consensus bugs are subtle, rare-event bugs; we want
  an implementation hardened by production use, not a first draft.
* **Async-runtime fit** - Must compose naturally with tokio.
* **Maintenance status** - Actively maintained, responsive upstream.
* **Hooks exposed** - Snapshots, log truncation, and membership changes must
  be pluggable, because VibeSQL will eventually wire these to its own WAL and
  storage engine.
* **Fit with existing MVCC work** - The MVCC Phase 1 machinery (xmin/xmax
  stamping) should compose with replication without redesign.
* **Licensing** - Must be compatible with MIT OR Apache-2.0.
* **Reversibility** - The choice should be swappable behind an adapter trait.

## Considered Options

### Option 1: `openraft` (chosen)

**Description**: Async-native Raft implementation, MIT/Apache-2.0 dual
licensed, used in production by Databend.

**Pros**:
* ✅ **tokio-native async/await API** - no callback-to-future shims needed
* ✅ **Actively maintained** - frequent releases, responsive maintainers
* ✅ **Production-proven** - Databend runs it as its meta-service consensus
* ✅ **Exposes the hooks we need** - pluggable `RaftLogStorage` /
  `RaftStateMachine` traits cover log persistence, snapshot build/install,
  log truncation/purge, and membership changes
* ✅ **Licensing fits** - MIT OR Apache-2.0, same as VibeSQL

**Cons**:
* ❌ Generic-heavy API; type-config boilerplate to learn
* ❌ Younger than `raft-rs`; API has historically evolved between minor versions
  - **Mitigation**: the `ConsensusBackend` adapter trait isolates consumers
    from openraft's types entirely

### Option 2: `raft-rs` (TiKV)

**Description**: TiKV's Raft core, extremely battle-tested at scale.

**Pros**:
* ✅ The most production-hardened Raft in the Rust ecosystem (TiKV)
* ✅ Stable, well-understood API

**Cons**:
* ❌ **Older callback/tick-driven API style** - it is a state-machine library
  you must drive yourself (ready/advance loop); not async-native
* ❌ Significant integration glue required to host it on tokio: we would
  hand-write the driver loop, message plumbing, and storage glue that
  openraft already provides as async traits
* ❌ Log persistence, snapshot transport, and network layer are all
  bring-your-own

**Verdict**: Rejected. The battle-testing is attractive, but the integration
surface we would have to write and maintain is exactly the code most likely
to harbor bugs — and openraft gives us that layer pre-built and proven.

### Option 3: Viewstamped Replication (VR/VSR), hand-rolled

**Description**: VSR (raised by @ansarizafar on #4460; used by TigerBeetle)
is arguably a cleaner protocol than Raft, but **no mature Rust crate exists**
— choosing it means hand-rolling consensus.

**Pros**:
* ✅ Cleaner protocol design; no separate snapshot/log-compaction sub-protocol
* ✅ TigerBeetle demonstrates it works extremely well in production

**Cons**:
* ❌ **No mature Rust implementation to build on** - we would write the
  protocol from scratch
* ❌ **Hand-rolling consensus without deterministic-simulation test
  infrastructure is an unacceptable risk for a sole-developer project.**
  TigerBeetle's VSR is trustworthy because they built deterministic
  simulation testing (the VOPR) *first* and run their entire cluster inside
  it; VibeSQL has no equivalent harness, and building one is a larger project
  than the replication feature itself.

**Verdict**: Rejected (deferred indefinitely). Documented here per the
discussion on #4460.

### Option 4: Hand-rolled Raft

**Description**: Implement Raft ourselves.

**Verdict**: Rejected — strictly worse than the alternatives. It carries the
same "consensus without deterministic-simulation testing" objection as
Option 3, *and* unlike VSR there are mature Rust libraries available, so
hand-rolling buys nothing.

## Topology Decision: Single Raft Group, Whole Database

Independent of library choice, #4460 originally sketched a
CockroachDB-style design: ranges, per-range Raft groups, HLC timestamps, and
distributed transactions. **We are explicitly not building that shape.**

**Chosen topology**: a **single Raft group replicating the whole database**
— the rqlite / dqlite / Turso model. Every committed log entry is a
transaction (or batch) applied to the full database state machine.

**Rationale**:

1. **Commit order = log order.** With one group there is a single total order
   of commits, which composes directly with VibeSQL's existing MVCC Phase 1
   machinery (xmin/xmax stamping): the Raft apply index maps onto the commit
   timestamp. No hybrid logical clocks, no cross-shard ordering questions.
2. **No sharding metadata, no distributed transactions.** Range descriptors,
   range splits/merges, intent resolution, and two-phase commit records all
   evaporate. The replication layer stays small enough for one person to own.
3. **Sharding solves a capacity problem VibeSQL doesn't have.** VibeSQL is
   in-memory-leaning and single-process; the working set fits one machine by
   design. Per-range groups exist to scale data and write throughput across
   machines — that is not this project's bottleneck.

**Known tradeoff (stated honestly)**: a single group caps write throughput at
what one leader on one machine can sequence. Multi-region **write** scaling
would require revisiting the sharded design (#5202 keeps that door open).
Multi-region **read** scaling remains in scope via leader leases +
bounded-staleness follower reads (#5200, Phase B2).

## Decision Outcome

**Chosen**: **`openraft`**, in a **single Raft group replicating the whole
database**, consumed exclusively through the engine-agnostic
`ConsensusBackend` adapter trait below.

`openraft` is the only candidate that is simultaneously async/tokio-native,
actively maintained, production-proven (Databend), and exposes the
snapshot / log-truncation / membership hooks VibeSQL needs. The adapter trait
keeps the decision reversible: consumers never see openraft types, so the
library — or even the topology — can change later without rewriting them.

**Note**: `openraft` is **not** added as a dependency in this phase. Phase A1
is decision + scaffolding; wiring openraft into the adapter is Phase A2/A3
work. This keeps the current PR purely additive.

## Adapter Trait Sketch

Landed in the new `vibesql-consensus` crate (this PR):

```rust
/// Index of an entry in the replicated log (1-based, Raft convention).
pub type LogIndex = u64;

/// Raft-style role of the local node.
pub enum Role { Leader, Follower, Candidate }

/// Opaque state-machine snapshot plus the last log index it covers.
pub struct Snapshot {
    pub last_included_index: LogIndex,
    pub data: Vec<u8>,
}

/// Engine-agnostic consensus adapter. Consumers depend on this trait,
/// never on the underlying library (openraft in Phase A2+).
pub trait ConsensusBackend: Send + Sync {
    type Entry: Serialize + DeserializeOwned + Send;

    /// Propose an entry; resolves once the entry is committed.
    async fn propose(&self, entry: Self::Entry) -> Result<LogIndex>;
    /// Read back a committed entry by index.
    async fn read_committed(&self, idx: LogIndex) -> Result<Self::Entry>;
    /// Capture a snapshot of committed state (for log truncation / catch-up).
    async fn snapshot(&self) -> Result<Snapshot>;
    /// Current role of this node.
    fn role(&self) -> Role;
}
```

What VibeSQL plugs in behind this trait in later phases: the database state
machine (apply committed entries), log persistence (WAL wiring), snapshot
build/install, and membership-change hooks — all of which openraft exposes
via its `RaftLogStorage` / `RaftStateMachine` traits.

**Async style**: the trait uses native `async fn` in traits (stable since
Rust 1.75; the workspace toolchain is far newer, so no MSRV impact). The
`async_fn_in_trait` lint is explicitly allowed at the trait definition: the
auto-trait (`Send`) bounds on returned futures and dyn-compatibility will be
revisited in Phase A2 when the first real (openraft) backend lands; if dyn
dispatch or stricter bounds are needed then, we will switch to explicit
`impl Future + Send` returns or boxed futures.

## Single-Node Loopback Plan

`vibesql-consensus` ships `SingleNodeBackend`, an in-memory implementation
that commits every proposal immediately (a `Vec`-backed log behind a mutex)
and always reports `Role::Leader`. Purpose:

- Dev and unit tests exercise the `ConsensusBackend` interface without
  standing up a multi-node cluster.
- Consumers of the trait (Phase B work) can be written and tested against it
  before the openraft backend exists.
- Its snapshot format (serde_json of the log, with an inherent
  `from_snapshot` constructor) is a stand-in only; real snapshot encoding is
  decided when WAL wiring lands.

## Deferred Non-Goals

Explicitly out of scope for the chosen design (not just this PR):

| Deferred item | Tracking | Why deferred |
|---------------|----------|--------------|
| Range sharding / per-range Raft groups | #5202 (Phase A0) | Solves a capacity problem VibeSQL doesn't have; revisit only if multi-region write scaling becomes a requirement |
| Distributed multi-shard transactions (intents + commit records) | #5201 (Phase C1) | Only needed with sharding; single group makes every transaction single-group |
| HLC (hybrid logical clock) timestamping | — | Single total log order makes commit ordering trivial; apply index maps onto commit timestamp |

Also out of scope for this PR specifically (next issues in the Raft track):
actual multi-node replication, log-persistence wiring to the WAL, membership
changes, and any Raft RPC / network code.

## Consequences

### Positive

* ✅ Consensus correctness outsourced to a production-proven library
* ✅ Replication layer composes with existing MVCC Phase 1 without redesign
* ✅ Adapter trait keeps both the library and the topology swappable
* ✅ `SingleNodeBackend` lets downstream work start immediately, testable
  without clusters
* ✅ Purely additive scaffolding — zero risk to existing crates

### Negative

* ❌ Write throughput capped at one leader / one machine
  - **Mitigation**: acceptable for VibeSQL's in-memory, single-process
    positioning; #5202 preserves the sharded option if that changes
* ❌ openraft API churn between minor versions
  - **Mitigation**: only `vibesql-consensus` internals touch openraft types
* ❌ Whole-database snapshots can be large
  - **Mitigation**: snapshot encoding/transport is deliberately deferred to
    the WAL-wiring phase, where incremental options can be evaluated

### Neutral

* The trait is intentionally minimal (4 methods); membership-change and
  log-truncation surface will be added when a real backend needs them

## Validation

Success criteria for this decision:

1. ✅ `ConsensusBackend` compiles in `vibesql-consensus` (this PR)
2. ✅ `SingleNodeBackend` passes propose/read roundtrip, role, and snapshot
   roundtrip unit tests (this PR)
3. ⏳ Phase A2: openraft backend implements the same trait with no consumer
   changes
4. ⏳ Phase B: replication consumers are written against the trait and pass
   against both backends

## References

* **openraft**: https://github.com/databendlabs/openraft
* **raft-rs**: https://github.com/tikv/raft-rs
* **Databend** (openraft in production): https://github.com/databendlabs/databend
* **rqlite** (single-group whole-DB Raft over SQLite): https://github.com/rqlite/rqlite
* **dqlite**: https://dqlite.io/
* **TigerBeetle VOPR** (deterministic simulation testing built before/with VSR): https://docs.tigerbeetle.com/about/vopr/
* **Viewstamped Replication Revisited** (Liskov & Cowling, 2012)
* **Raft paper**: Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (2014)

### Related Decisions

* ADR-0001: Language Choice (Rust) — ecosystem availability of openraft is a
  direct consequence

---

**Status**: ACCEPTED ✅

**Date Accepted**: 2026-06-11

**Next Steps**:
1. Phase A2/A3: add `openraft` dependency and implement `ConsensusBackend` over it
2. Wire log persistence to the WAL (separate issue)
3. Phase B2: leader leases + bounded-staleness follower reads (#5200)
