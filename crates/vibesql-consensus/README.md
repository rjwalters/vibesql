# vibesql-consensus

Consensus adapter layer for VibeSQL WAN replication.

## Overview

This crate is the stable seam between VibeSQL and the consensus engine that drives replication. Per [ADR-0004](../../docs/decisions/0004-consensus-library.md), VibeSQL runs `openraft` as a **single Raft group replicating the whole database** (the rqlite/dqlite/Turso model), but consumers of this crate depend only on the engine-agnostic `ConsensusBackend` trait — never on `openraft` types directly — so the consensus engine stays swappable.

## Features

- **`ConsensusBackend` trait**: engine-agnostic adapter (propose / read_committed / snapshot / role)
- **`SingleNodeBackend`**: in-memory loopback implementation that commits every proposal immediately, for dev and unit tests
- **`OpenraftBackend`**: the real engine — proposals flow through openraft's append → commit → apply pipeline, with optional on-disk durability (fsynced WAL-style log and vote records, torn-write tolerant recovery)
- **TCP transport**: length-prefixed frames on a dedicated consensus port, with static membership via `cluster.toml`
- **Durable snapshots**: CRC-framed, atomically-written snapshot files; recovery restores snapshot first, then replays the log
- **Replicated state machine**: applies committed transactions from the Raft log to a real VibeSQL database

All backend configurations pass the same behavioral conformance suite, so code written against the trait behaves identically on any of them.

### Cargo features

- `mvcc_enabled` — forwards the MVCC toggle to the storage/executor stack; enables commit-timestamp stamping (`xmin` = Raft log index) and the vacuum-horizon interlock

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-consensus = "0.2"
```

Spin up a local multi-node test cluster from the VibeSQL repository:

```bash
make test-cluster
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-consensus)
- [ADR-0004: Consensus Library Selection](../../docs/decisions/0004-consensus-library.md)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
