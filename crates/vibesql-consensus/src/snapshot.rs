//! Durable snapshot persistence, the snapshot payload codec, and the MVCC
//! vacuum-horizon pin for [`OpenraftBackend`] (Raft Phase A4, PR 1 of issue
//! #5198).
//!
//! Three concerns live here, all consumed by the state machine in
//! `openraft_backend.rs`:
//!
//! 1. **Payload codec** ([`encode_payload`] / [`decode_payload`]): the byte
//!    encoding of the state machine's applied state inside a snapshot.
//! 2. **Durable persistence** ([`SnapshotStore`]): snapshot blobs written to
//!    the data directory with tmp/rename atomicity, recovered on restart,
//!    and rejected **loudly** when corrupt.
//! 3. **GC interlock** ([`SnapshotHorizonPin`]): the hook through which an
//!    in-progress snapshot build pins the MVCC vacuum horizon, plus the
//!    no-op implementation for the current echo state machine.
//!
//! ## File format
//!
//! One file per snapshot, `snapshot-<raw raft index>.bin`, in the same data
//! directory as `raft.log`. The framing mirrors the conventions of
//! `crate::durable` (magic + version + CRC-32 over each frame):
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ Magic: "VSNP" (4 bytes)                     │
//! │ Version: u32 LE                             │
//! ├─────────────────────────────────────────────┤
//! │ Meta frame:  [len:u32][crc:u32][meta JSON]  │  SnapshotMeta
//! ├─────────────────────────────────────────────┤
//! │ Data frame:  [len:u64][crc:u32][data bytes] │  payload blob
//! └─────────────────────────────────────────────┘
//! ```
//!
//! Writes go to `snapshot-<index>.bin.tmp`, are fsynced, then renamed over
//! the final name (followed by a directory fsync), so a crash mid-write can
//! never leave a torn *final* file — only a `.tmp` leftover, which was never
//! acknowledged and is silently removed on the next open.
//!
//! ## Corruption handling
//!
//! A final `snapshot-<index>.bin` that fails validation (bad magic, bad
//! version, CRC mismatch, truncation, trailing bytes, undecodable meta) is
//! **real corruption**, not a crash artifact — the rename was atomic and the
//! contents were fsynced first. Following the same philosophy as
//! `crate::durable` (etcd's tail-only repair rule; see PR #5357): the open
//! fails loudly with `InvalidData` and the file is left byte-for-byte
//! untouched for inspection. There is **no silent fall back** to an older
//! snapshot or to a fresh state machine, because the raft log may already be
//! purged up to this snapshot's index — starting fresh would silently lose
//! acknowledged state.
//!
//! ## Purge safety
//!
//! [`DurableSnapshotWatermark`] is the bridge between the snapshot store and
//! the log store: it carries the raw raft index of the last snapshot that is
//! **durable on disk**. `DurableLogStore::purge` refuses to purge above it
//! (the Phase A4 safety rule: log entries may only be discarded once a
//! durable snapshot covers them). The watermark only advances *after* the
//! snapshot file is fsynced and renamed, so openraft can never be told about
//! — and can never purge against — a snapshot that might not survive a
//! crash.
//!
//! [`OpenraftBackend`]: crate::OpenraftBackend

use std::fmt::Debug;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::{BasicNode, SnapshotMeta};

use crate::durable::crc32;

/// Current snapshot file format version.
const SNAPSHOT_VERSION: u32 = 1;

/// Magic number for snapshot files: "VSNP" (cf. the raft log's "VRFT").
const SNAPSHOT_MAGIC: &[u8; 4] = b"VSNP";

/// `magic + version`.
const SNAPSHOT_HEADER_SIZE: usize = 8;

// ---------------------------------------------------------------------------
// Payload codec
// ---------------------------------------------------------------------------
//
// The snapshot *payload* is the serialized applied state of the state
// machine. Until Phase B1 (#5199) wires the real MVCC-backed state machine,
// that state is the echo machine's ordered list of applied entry payloads,
// and serde_json is its (test-scale) encoding. B1 replaces this codec with a
// streamed database-state encoding reusing the writers in
// `vibesql-storage::persistence::binary` — those writers serialize *tables*,
// which the echo machine does not have, so coupling to them now would be
// premature (see the curator re-scope on #5198). Keeping the codec isolated
// here means only this seam changes when B1 lands.

/// Encode the state machine's applied entries into a snapshot payload blob.
pub(crate) fn encode_payload(entries: &[Vec<u8>]) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(entries)
}

/// Decode a snapshot payload blob back into applied entries.
pub(crate) fn decode_payload(data: &[u8]) -> serde_json::Result<Vec<Vec<u8>>> {
    serde_json::from_slice(data)
}

// ---------------------------------------------------------------------------
// MVCC vacuum-horizon pin
// ---------------------------------------------------------------------------

/// Hook through which a snapshot build pins the MVCC vacuum horizon.
///
/// A snapshot is a consistent view of the state machine at a specific apply
/// index; row versions visible at that index must not be reclaimed while the
/// build reads them. [`RaftSnapshotBuilder::build_snapshot`] acquires a
/// guard from this hook **before** reading any state and holds it until the
/// snapshot is built, durably persisted, and registered — dropping the guard
/// is what releases the horizon.
///
/// The state machine is not MVCC-wired until Phase B1 (#5199), so the only
/// production implementation today is the no-op [`NoopHorizonPin`]. B1
/// implements this by registering the snapshot build as (or alongside) an
/// active read transaction in
/// `vibesql-storage::database::transaction_api` — the existing
/// `compute_gc_horizon` holdback for active transactions then covers the
/// build with no new watermark type (see
/// `vacuum_mvcc_horizon_held_back_by_active_transaction` in the storage
/// crate's vacuum tests).
///
/// [`RaftSnapshotBuilder::build_snapshot`]: openraft::RaftSnapshotBuilder::build_snapshot
pub(crate) trait SnapshotHorizonPin: Send + Sync + Debug {
    /// Acquire the pin. The returned guard releases it on drop.
    fn acquire(&self) -> Box<dyn Send>;
}

/// No-op pin for the pre-B1 echo state machine (nothing to hold back: its
/// state is an in-memory `Vec` read under the state-machine mutex).
#[derive(Debug, Default)]
pub(crate) struct NoopHorizonPin;

impl SnapshotHorizonPin for NoopHorizonPin {
    fn acquire(&self) -> Box<dyn Send> {
        Box::new(())
    }
}

// ---------------------------------------------------------------------------
// Durable snapshot watermark
// ---------------------------------------------------------------------------

/// The raw raft index of the last snapshot that is **durable on disk**
/// (`None` until one exists). Shared between [`SnapshotStore`] (which
/// advances it) and `DurableLogStore` (whose `purge` refuses to exceed it).
///
/// `0` is the "no snapshot" sentinel: raw raft log indices are 1-based, so
/// no real snapshot ever covers index 0 (a restore-seeded snapshot with no
/// raft meta reports index 0 and deliberately legalizes no purge).
#[derive(Debug, Default)]
pub(crate) struct DurableSnapshotWatermark(AtomicU64);

impl DurableSnapshotWatermark {
    /// The last durably snapshotted raw index, if any.
    pub(crate) fn get(&self) -> Option<u64> {
        match self.0.load(Ordering::SeqCst) {
            0 => None,
            index => Some(index),
        }
    }

    /// Monotonically advance the watermark (lower values are ignored).
    pub(crate) fn advance(&self, index: u64) {
        self.0.fetch_max(index, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// The durable snapshot store
// ---------------------------------------------------------------------------

/// A snapshot recovered from disk on open.
#[derive(Debug)]
pub(crate) struct LoadedSnapshot {
    pub(crate) meta: SnapshotMeta<u64, BasicNode>,
    pub(crate) data: Vec<u8>,
}

/// Durable snapshot persistence under a data directory (alongside
/// `raft.log`). See the module docs for format, atomicity, and corruption
/// rules.
#[derive(Debug)]
pub(crate) struct SnapshotStore {
    dir: PathBuf,
    watermark: Arc<DurableSnapshotWatermark>,
}

/// The raw raft index a snapshot file is named after (`0` for snapshots
/// whose meta carries no log id, e.g. restore seeds).
fn file_index(meta: &SnapshotMeta<u64, BasicNode>) -> u64 {
    meta.last_log_id.map_or(0, |id| id.index)
}

fn snapshot_file_name(index: u64) -> String {
    format!("snapshot-{index}.bin")
}

/// Parse `snapshot-<index>.bin` back into `<index>`; `None` for anything
/// else (foreign files are ignored, never deleted).
fn parse_snapshot_file_name(name: &str) -> Option<u64> {
    name.strip_prefix("snapshot-")?.strip_suffix(".bin")?.parse().ok()
}

fn corrupt(path: &Path, detail: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "durable raft snapshot {} is corrupt: {detail}; refusing to start — the file has \
             been left untouched for inspection (the raft log may be purged up to this \
             snapshot, so silently ignoring it could lose acknowledged state)",
            path.display()
        ),
    )
}

fn encode_snapshot_file(meta: &SnapshotMeta<u64, BasicNode>, data: &[u8]) -> io::Result<Vec<u8>> {
    let meta_json = serde_json::to_vec(meta).map_err(io::Error::other)?;
    let mut buf = Vec::with_capacity(SNAPSHOT_HEADER_SIZE + 8 + meta_json.len() + 12 + data.len());
    buf.extend_from_slice(SNAPSHOT_MAGIC);
    buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buf.extend_from_slice(&(meta_json.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crc32(&meta_json).to_le_bytes());
    buf.extend_from_slice(&meta_json);
    buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
    buf.extend_from_slice(&crc32(data).to_le_bytes());
    buf.extend_from_slice(data);
    Ok(buf)
}

fn decode_snapshot_file(
    path: &Path,
    buf: &[u8],
) -> io::Result<(SnapshotMeta<u64, BasicNode>, Vec<u8>)> {
    if buf.len() < SNAPSHOT_HEADER_SIZE {
        return Err(corrupt(path, "file is shorter than the 8-byte header"));
    }
    if &buf[0..4] != SNAPSHOT_MAGIC {
        return Err(corrupt(
            path,
            format!("expected magic 'VSNP', got '{}'", String::from_utf8_lossy(&buf[0..4])),
        ));
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().expect("4-byte slice"));
    if version > SNAPSHOT_VERSION {
        return Err(corrupt(
            path,
            format!("unsupported snapshot version {version} (current: {SNAPSHOT_VERSION})"),
        ));
    }

    // Meta frame.
    let mut offset = SNAPSHOT_HEADER_SIZE;
    if buf.len() - offset < 8 {
        return Err(corrupt(path, "truncated meta frame header"));
    }
    let meta_len =
        u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("4-byte slice")) as usize;
    let meta_crc =
        u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().expect("4-byte slice"));
    offset += 8;
    let Some(meta_end) = offset.checked_add(meta_len).filter(|&end| end <= buf.len()) else {
        return Err(corrupt(path, "meta frame length exceeds the file"));
    };
    let meta_json = &buf[offset..meta_end];
    if crc32(meta_json) != meta_crc {
        return Err(corrupt(path, "meta frame checksum mismatch"));
    }
    let meta: SnapshotMeta<u64, BasicNode> = serde_json::from_slice(meta_json)
        .map_err(|e| corrupt(path, format!("meta frame failed to decode: {e}")))?;
    offset = meta_end;

    // Data frame.
    if buf.len() - offset < 12 {
        return Err(corrupt(path, "truncated data frame header"));
    }
    let data_len =
        u64::from_le_bytes(buf[offset..offset + 8].try_into().expect("8-byte slice")) as usize;
    let data_crc =
        u32::from_le_bytes(buf[offset + 8..offset + 12].try_into().expect("4-byte slice"));
    offset += 12;
    let Some(data_end) = offset.checked_add(data_len).filter(|&end| end <= buf.len()) else {
        return Err(corrupt(path, "data frame length exceeds the file"));
    };
    let data = &buf[offset..data_end];
    if crc32(data) != data_crc {
        return Err(corrupt(path, "data frame checksum mismatch"));
    }
    if data_end != buf.len() {
        return Err(corrupt(
            path,
            format!("{} trailing bytes after the data frame", buf.len() - data_end),
        ));
    }

    Ok((meta, data.to_vec()))
}

/// fsync the directory containing `path` so renames/creations are durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_data()?;
    Ok(())
}

impl SnapshotStore {
    /// Open the snapshot store under `dir` (created if absent), recovering
    /// the latest snapshot if one exists.
    ///
    /// Crash leftovers (`*.tmp`, never acknowledged) are removed silently. A
    /// corrupt *final* snapshot file is a loud [`io::ErrorKind::InvalidData`]
    /// error — see the module docs for why there is no silent fallback.
    pub(crate) fn open(dir: &Path) -> io::Result<(Arc<Self>, Option<LoadedSnapshot>)> {
        std::fs::create_dir_all(dir)?;

        let mut latest: Option<(u64, PathBuf)> = None;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("snapshot-") && name.ends_with(".tmp") {
                // A tmp file is a crash mid-write: it was never renamed, so
                // it was never acknowledged. Removing it is safe.
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            if let Some(index) = parse_snapshot_file_name(&name) {
                if latest.as_ref().is_none_or(|(best, _)| index > *best) {
                    latest = Some((index, entry.path()));
                }
            }
        }

        let watermark = Arc::new(DurableSnapshotWatermark::default());
        let loaded = match latest {
            None => None,
            Some((index, path)) => {
                let buf = std::fs::read(&path)?;
                let (meta, data) = decode_snapshot_file(&path, &buf)?;
                if file_index(&meta) != index {
                    return Err(corrupt(
                        &path,
                        format!(
                            "file is named for index {index} but its meta covers index {}",
                            file_index(&meta)
                        ),
                    ));
                }
                watermark.advance(index);
                Some(LoadedSnapshot { meta, data })
            }
        };

        Ok((Arc::new(Self { dir: dir.to_path_buf(), watermark }), loaded))
    }

    /// Shared watermark handle for the log store's purge-safety check.
    pub(crate) fn watermark(&self) -> Arc<DurableSnapshotWatermark> {
        Arc::clone(&self.watermark)
    }

    /// Durably persist a snapshot: write to a tmp file, fsync, rename over
    /// the final name, fsync the directory, **then** advance the watermark
    /// (so purge can never get ahead of what is actually on disk). Older
    /// snapshot files are removed best-effort afterwards.
    pub(crate) fn save(&self, meta: &SnapshotMeta<u64, BasicNode>, data: &[u8]) -> io::Result<()> {
        let index = file_index(meta);
        let final_path = self.dir.join(snapshot_file_name(index));
        let tmp_path = self.dir.join(format!("snapshot-{index}.bin.tmp"));

        let buf = encode_snapshot_file(meta, data)?;
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&buf)?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.dir)?;

        self.watermark.advance(index);

        // Best-effort cleanup of superseded snapshots; failure leaves
        // harmless extra files (open always picks the highest index).
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(old) = parse_snapshot_file_name(&name) {
                    if old < index {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests: format, atomicity, and corruption handling at the store level
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use openraft::{CommittedLeaderId, LogId, StoredMembership};
    use tempfile::TempDir;

    use super::*;

    fn meta_at(index: u64) -> SnapshotMeta<u64, BasicNode> {
        SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), index)),
            last_membership: StoredMembership::default(),
            snapshot_id: format!("test-{index}"),
        }
    }

    #[test]
    fn watermark_starts_empty_and_advances_monotonically() {
        let watermark = DurableSnapshotWatermark::default();
        assert_eq!(watermark.get(), None);
        watermark.advance(5);
        assert_eq!(watermark.get(), Some(5));
        watermark.advance(3); // lower values are ignored
        assert_eq!(watermark.get(), Some(5));
        watermark.advance(9);
        assert_eq!(watermark.get(), Some(9));
    }

    #[test]
    fn empty_dir_has_no_snapshot() {
        let dir = TempDir::new().unwrap();
        let (store, loaded) = SnapshotStore::open(dir.path()).unwrap();
        assert!(loaded.is_none());
        assert_eq!(store.watermark().get(), None);
    }

    #[test]
    fn save_then_open_roundtrips_meta_and_data() {
        let dir = TempDir::new().unwrap();
        let meta = meta_at(7);
        {
            let (store, _) = SnapshotStore::open(dir.path()).unwrap();
            store.save(&meta, b"payload bytes").unwrap();
            assert_eq!(store.watermark().get(), Some(7));
        }

        let (store, loaded) = SnapshotStore::open(dir.path()).unwrap();
        let loaded = loaded.expect("snapshot should be recovered");
        assert_eq!(loaded.meta, meta);
        assert_eq!(loaded.data, b"payload bytes");
        assert_eq!(store.watermark().get(), Some(7));
    }

    #[test]
    fn newer_snapshot_supersedes_older() {
        let dir = TempDir::new().unwrap();
        let (store, _) = SnapshotStore::open(dir.path()).unwrap();
        store.save(&meta_at(3), b"old").unwrap();
        store.save(&meta_at(8), b"new").unwrap();

        // The superseded file is gone; only the latest remains.
        assert!(!dir.path().join(snapshot_file_name(3)).exists());
        assert!(dir.path().join(snapshot_file_name(8)).exists());

        let (store, loaded) = SnapshotStore::open(dir.path()).unwrap();
        assert_eq!(loaded.unwrap().data, b"new");
        assert_eq!(store.watermark().get(), Some(8));
    }

    #[test]
    fn restore_seed_snapshot_legalizes_no_purge() {
        let dir = TempDir::new().unwrap();
        // A restore-seeded snapshot has no raft log id: it covers no raft
        // entries, so the durable watermark must remain "none".
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "restore".to_string(),
        };
        let (store, _) = SnapshotStore::open(dir.path()).unwrap();
        store.save(&meta, b"seed").unwrap();
        assert_eq!(store.watermark().get(), None);

        let (store, loaded) = SnapshotStore::open(dir.path()).unwrap();
        assert_eq!(loaded.unwrap().data, b"seed");
        assert_eq!(store.watermark().get(), None);
    }

    #[test]
    fn crash_leftover_tmp_file_is_removed_silently() {
        let dir = TempDir::new().unwrap();
        {
            let (store, _) = SnapshotStore::open(dir.path()).unwrap();
            store.save(&meta_at(4), b"good").unwrap();
        }
        // Simulate a crash mid-write of the NEXT snapshot: a torn tmp file.
        std::fs::write(dir.path().join("snapshot-9.bin.tmp"), b"torn garbage").unwrap();

        let (store, loaded) = SnapshotStore::open(dir.path()).unwrap();
        assert_eq!(loaded.unwrap().data, b"good");
        assert_eq!(store.watermark().get(), Some(4));
        assert!(!dir.path().join("snapshot-9.bin.tmp").exists());
    }

    /// Every corruption of a *final* snapshot file must fail the open loudly
    /// and leave the file untouched — never silently fall back.
    #[test]
    fn corrupt_final_snapshot_is_rejected_loudly_and_left_untouched() {
        let make = |mutate: &dyn Fn(&mut Vec<u8>)| {
            let dir = TempDir::new().unwrap();
            let (store, _) = SnapshotStore::open(dir.path()).unwrap();
            store.save(&meta_at(5), b"snapshot payload").unwrap();
            drop(store);
            let path = dir.path().join(snapshot_file_name(5));
            let mut buf = std::fs::read(&path).unwrap();
            mutate(&mut buf);
            std::fs::write(&path, &buf).unwrap();
            (dir, path)
        };

        type Mutation = Box<dyn Fn(&mut Vec<u8>)>;
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "flipped data byte",
                Box::new(|buf: &mut Vec<u8>| {
                    let last = buf.len() - 1;
                    buf[last] ^= 0xFF;
                }),
            ),
            (
                "flipped meta byte",
                Box::new(|buf: &mut Vec<u8>| {
                    buf[SNAPSHOT_HEADER_SIZE + 8] ^= 0xFF;
                }),
            ),
            (
                "truncated file",
                Box::new(|buf: &mut Vec<u8>| {
                    buf.truncate(buf.len() - 4);
                }),
            ),
            (
                "trailing garbage",
                Box::new(|buf: &mut Vec<u8>| {
                    buf.extend_from_slice(b"junk");
                }),
            ),
            (
                "wrong magic",
                Box::new(|buf: &mut Vec<u8>| {
                    buf[0..4].copy_from_slice(b"XXXX");
                }),
            ),
            (
                "future version",
                Box::new(|buf: &mut Vec<u8>| {
                    buf[4..8].copy_from_slice(&(SNAPSHOT_VERSION + 1).to_le_bytes());
                }),
            ),
        ];

        for (label, mutate) in cases {
            let (dir, path) = make(&*mutate);
            let len_before = std::fs::metadata(&path).unwrap().len();

            let err = SnapshotStore::open(dir.path()).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{label}: wrong kind: {err}");
            assert!(err.to_string().contains("corrupt"), "{label}: unexpected error: {err}");
            // Left byte-for-byte untouched for inspection.
            assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before, "{label}");

            // The refusal is stable across attempts.
            let err = SnapshotStore::open(dir.path()).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{label}: second open: {err}");
        }
    }

    #[test]
    fn filename_and_meta_index_disagreement_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (store, _) = SnapshotStore::open(dir.path()).unwrap();
        store.save(&meta_at(5), b"payload").unwrap();
        drop(store);
        // Rename the file to claim a different index than its meta carries.
        std::fs::rename(
            dir.path().join(snapshot_file_name(5)),
            dir.path().join(snapshot_file_name(9)),
        )
        .unwrap();

        let err = SnapshotStore::open(dir.path()).unwrap_err();
        assert!(err.to_string().contains("named for index 9"), "unexpected error: {err}");
    }

    #[test]
    fn foreign_files_are_ignored() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        std::fs::write(dir.path().join("raft.log"), b"not a snapshot").unwrap();
        std::fs::write(dir.path().join("snapshot-abc.bin"), b"unparsable name").unwrap();

        let (store, loaded) = SnapshotStore::open(dir.path()).unwrap();
        assert!(loaded.is_none());
        assert_eq!(store.watermark().get(), None);
        // Foreign files were not deleted.
        assert!(dir.path().join("raft.log").exists());
        assert!(dir.path().join("snapshot-abc.bin").exists());
    }

    #[test]
    fn payload_codec_roundtrips() {
        let entries = vec![b"one".to_vec(), b"two".to_vec()];
        let blob = encode_payload(&entries).unwrap();
        assert_eq!(decode_payload(&blob).unwrap(), entries);

        assert!(decode_payload(b"not json").is_err());
    }
}
