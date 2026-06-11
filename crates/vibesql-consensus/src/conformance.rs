//! Shared behavioral conformance suite for [`ConsensusBackend`]
//! implementations.
//!
//! Every backend must exhibit identical observable behavior: dense 1-based
//! log indices, `NotCommitted` for unproposed indices, snapshot roundtrips,
//! and rejection of corrupt snapshots. The same test bodies run against both
//! [`SingleNodeBackend`] and [`OpenraftBackend`] via the [`Harness`]
//! abstraction, so a future backend (or a durability change in an existing
//! one) only needs a new `Harness` impl plus one `conformance_suite!`
//! invocation to get full coverage.
//!
//! These tests were originally `SingleNodeBackend` unit tests (Phase A1) and
//! were extracted here when the openraft backend landed (Phase A2, #5196).
//! The durable openraft configuration joined the suite with PR 2 of #5196.

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::{
    ConsensusBackend, ConsensusError, LogIndex, OpenraftBackend, Role, SingleNodeBackend, Snapshot,
};

/// Stand-in for a replicated write (a serialized transaction or batch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestEntry {
    txn_id: u64,
    payload: String,
}

fn entry(txn_id: u64, payload: &str) -> TestEntry {
    TestEntry { txn_id, payload: payload.to_string() }
}

/// Backend-construction hooks for the conformance suite.
///
/// `restore` and `last_index` are inherent (non-trait) capabilities of both
/// current backends; they are surfaced here so the suite can verify snapshot
/// roundtrips behaviorally without those methods joining the
/// [`ConsensusBackend`] trait before later Raft phases need them.
trait Harness {
    type Backend: ConsensusBackend<Entry = TestEntry>;
    /// Keeps backing resources alive for the backend's lifetime (the durable
    /// harness holds the tempdir its Raft log lives in; in-memory harnesses
    /// use `()`).
    type Guard;

    async fn create() -> (Self::Backend, Self::Guard);
    async fn restore(snapshot: &Snapshot) -> crate::Result<(Self::Backend, Self::Guard)>;
    fn last_index(backend: &Self::Backend) -> LogIndex;
}

struct SingleNodeHarness;

impl Harness for SingleNodeHarness {
    type Backend = SingleNodeBackend<TestEntry>;
    type Guard = ();

    async fn create() -> (Self::Backend, Self::Guard) {
        (SingleNodeBackend::new(), ())
    }

    async fn restore(snapshot: &Snapshot) -> crate::Result<(Self::Backend, Self::Guard)> {
        Ok((SingleNodeBackend::from_snapshot(snapshot)?, ()))
    }

    fn last_index(backend: &Self::Backend) -> LogIndex {
        backend.last_index()
    }
}

struct OpenraftHarness;

impl Harness for OpenraftHarness {
    type Backend = OpenraftBackend<TestEntry>;
    type Guard = ();

    async fn create() -> (Self::Backend, Self::Guard) {
        (OpenraftBackend::new().await.expect("single-node openraft cluster should start"), ())
    }

    async fn restore(snapshot: &Snapshot) -> crate::Result<(Self::Backend, Self::Guard)> {
        Ok((OpenraftBackend::from_snapshot(snapshot).await?, ()))
    }

    fn last_index(backend: &Self::Backend) -> LogIndex {
        backend.last_index()
    }
}

/// The openraft backend with its Raft log persisted on disk (#5196, PR 2).
struct DurableOpenraftHarness;

impl Harness for DurableOpenraftHarness {
    type Backend = OpenraftBackend<TestEntry>;
    type Guard = TempDir;

    async fn create() -> (Self::Backend, Self::Guard) {
        let dir = TempDir::new().expect("create tempdir for durable raft log");
        let backend = OpenraftBackend::with_data_dir(dir.path())
            .await
            .expect("durable single-node openraft cluster should start");
        (backend, dir)
    }

    async fn restore(snapshot: &Snapshot) -> crate::Result<(Self::Backend, Self::Guard)> {
        let dir = TempDir::new().expect("create tempdir for durable raft log");
        let backend = OpenraftBackend::from_snapshot_with_data_dir(snapshot, dir.path()).await?;
        Ok((backend, dir))
    }

    fn last_index(backend: &Self::Backend) -> LogIndex {
        backend.last_index()
    }
}

// ---------------------------------------------------------------------------
// Test bodies (backend-generic)
// ---------------------------------------------------------------------------

async fn propose_read_committed_roundtrip<H: Harness>() {
    let (backend, _guard) = H::create().await;

    let first = entry(1, "INSERT INTO t VALUES (1)");
    let second = entry(2, "UPDATE t SET x = 2");

    let idx1 = backend.propose(first.clone()).await.unwrap();
    let idx2 = backend.propose(second.clone()).await.unwrap();

    // Log indices are 1-based and monotonically increasing.
    assert_eq!(idx1, 1);
    assert_eq!(idx2, 2);
    assert_eq!(H::last_index(&backend), 2);

    assert_eq!(backend.read_committed(idx1).await.unwrap(), first);
    assert_eq!(backend.read_committed(idx2).await.unwrap(), second);
}

async fn read_uncommitted_index_is_an_error<H: Harness>() {
    let (backend, _guard) = H::create().await;

    // Index 0 means "no entry" and is never readable.
    assert!(matches!(backend.read_committed(0).await, Err(ConsensusError::NotCommitted(0))));

    // Nothing proposed yet, so index 1 is not committed either.
    assert!(matches!(backend.read_committed(1).await, Err(ConsensusError::NotCommitted(1))));

    // The committed prefix ends exactly at the last proposed entry.
    backend.propose(entry(1, "write")).await.unwrap();
    assert!(matches!(backend.read_committed(2).await, Err(ConsensusError::NotCommitted(2))));
}

async fn single_node_is_always_leader<H: Harness>() {
    let (backend, _guard) = H::create().await;
    assert_eq!(backend.role(), Role::Leader);

    // Leadership is stable across proposals.
    backend.propose(entry(1, "write")).await.unwrap();
    assert_eq!(backend.role(), Role::Leader);
}

async fn proposals_are_ordered<H: Harness>() {
    let (backend, _guard) = H::create().await;

    for i in 1..=10u64 {
        let idx = backend.propose(entry(i, "ordered write")).await.unwrap();
        // Each proposal is assigned the next dense index, in submission
        // order.
        assert_eq!(idx, i);
    }
    assert_eq!(H::last_index(&backend), 10);

    for i in 1..=10u64 {
        assert_eq!(backend.read_committed(i).await.unwrap(), entry(i, "ordered write"));
    }
}

async fn snapshot_roundtrip_restores_the_log<H: Harness>() {
    let (backend, _guard) = H::create().await;
    for i in 1..=3 {
        backend.propose(entry(i, "write")).await.unwrap();
    }

    let snapshot = backend.snapshot().await.unwrap();
    assert_eq!(snapshot.last_included_index, 3);

    let (restored, _restored_guard) = H::restore(&snapshot).await.unwrap();
    assert_eq!(H::last_index(&restored), 3);
    for i in 1..=3u64 {
        assert_eq!(restored.read_committed(i).await.unwrap(), entry(i, "write"));
    }

    // New proposals continue numbering after the restored prefix.
    let idx = restored.propose(entry(4, "post-restore write")).await.unwrap();
    assert_eq!(idx, 4);
    assert_eq!(restored.read_committed(4).await.unwrap(), entry(4, "post-restore write"));
}

async fn snapshot_of_empty_log_is_valid<H: Harness>() {
    let (backend, _guard) = H::create().await;
    let snapshot = backend.snapshot().await.unwrap();
    assert_eq!(snapshot.last_included_index, 0);

    let (restored, _restored_guard) = H::restore(&snapshot).await.unwrap();
    assert_eq!(H::last_index(&restored), 0);
}

async fn corrupt_snapshot_is_rejected<H: Harness>() {
    // Garbage bytes are not a valid snapshot for any backend.
    let bogus = Snapshot { last_included_index: 5, data: b"not json".to_vec() };
    assert!(matches!(H::restore(&bogus).await, Err(ConsensusError::SnapshotCodec(_))));

    // A structurally valid snapshot whose index claim disagrees with its
    // contents must also be rejected.
    let (backend, _guard) = H::create().await;
    backend.propose(entry(1, "only one")).await.unwrap();
    let mut inconsistent = backend.snapshot().await.unwrap();
    inconsistent.last_included_index = 5;
    assert!(matches!(H::restore(&inconsistent).await, Err(ConsensusError::SnapshotCodec(_))));
}

// ---------------------------------------------------------------------------
// Suite instantiation, once per backend
// ---------------------------------------------------------------------------

macro_rules! conformance_suite {
    ($mod_name:ident, $harness:ty) => {
        mod $mod_name {
            #[tokio::test]
            async fn propose_read_committed_roundtrip() {
                super::propose_read_committed_roundtrip::<$harness>().await;
            }

            #[tokio::test]
            async fn read_uncommitted_index_is_an_error() {
                super::read_uncommitted_index_is_an_error::<$harness>().await;
            }

            #[tokio::test]
            async fn single_node_is_always_leader() {
                super::single_node_is_always_leader::<$harness>().await;
            }

            #[tokio::test]
            async fn proposals_are_ordered() {
                super::proposals_are_ordered::<$harness>().await;
            }

            #[tokio::test]
            async fn snapshot_roundtrip_restores_the_log() {
                super::snapshot_roundtrip_restores_the_log::<$harness>().await;
            }

            #[tokio::test]
            async fn snapshot_of_empty_log_is_valid() {
                super::snapshot_of_empty_log_is_valid::<$harness>().await;
            }

            #[tokio::test]
            async fn corrupt_snapshot_is_rejected() {
                super::corrupt_snapshot_is_rejected::<$harness>().await;
            }
        }
    };
}

conformance_suite!(single_node, super::SingleNodeHarness);
conformance_suite!(openraft_backend, super::OpenraftHarness);
conformance_suite!(openraft_durable, super::DurableOpenraftHarness);
