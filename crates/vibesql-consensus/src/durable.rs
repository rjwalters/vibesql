//! Durable Raft log + vote storage for [`OpenraftBackend`] (Raft Phase A2,
//! PR 2 of issue #5196).
//!
//! [`DurableLogStore`] is a file-backed [`RaftLogStorage`] implementation. It
//! persists everything openraft requires to survive a restart with election
//! safety intact: log entries, the vote (term + voted_for), the saved commit
//! point, and the purge watermark.
//!
//! ## File format
//!
//! A single append-only file, `raft.log`, under the data directory. The
//! format deliberately mirrors the data-WAL conventions in
//! `vibesql-storage::wal` (`format.rs` / `writer.rs` / `reader.rs`) — same
//! header shape, same per-record framing, same CRC — without depending on
//! that crate: the WAL's framing helpers are private and entangled with
//! `WalEntry`/`Lsn`, so a minimal local copy keeps coupling lower than
//! exposing them would (see ADR-0004 and the curator re-scope on #5196).
//!
//! ```text
//! ┌────────────────────────────────────────┐
//! │ Header (32 bytes)                      │
//! │ - Magic: "VRFT" (4 bytes)              │
//! │ - Version: u32 LE                      │
//! │ - Created: u64 LE timestamp (ms)       │
//! │ - Reserved: 16 bytes                   │
//! ├────────────────────────────────────────┤
//! │ Record 1: [len:u32][crc:u32][data:...] │
//! ├────────────────────────────────────────┤
//! │ Record 2: [len:u32][crc:u32][data:...] │
//! ├────────────────────────────────────────┤
//! │ ...                                    │
//! └────────────────────────────────────────┘
//! ```
//!
//! Records are a [`LogRecord`] each: appends, vote saves, commit-point saves,
//! truncates (conflict rollback), and purges are all **logical records in one
//! ordered stream**. Recovery replays the stream to rebuild the in-memory
//! index; `truncate` and `purge` therefore never rewrite the file. Physical
//! space reclamation (compaction) is deferred to the snapshot/purge-policy
//! work in Phase A4 (#5198).
//!
//! ## Durability contract
//!
//! - `save_vote` fsyncs **before** returning — a vote that is not durable
//!   breaks election safety across restarts.
//! - `append` fsyncs the whole batch **before** invoking openraft's
//!   [`LogFlushed`] callback, honoring the storage-v2 contract that entries
//!   are acknowledged only once durable.
//! - `truncate` / `purge` / `save_committed` fsync before returning.
//!
//! ## Torn-tail tolerance vs. mid-file corruption
//!
//! On open, replay stops at the first record whose frame is incomplete or
//! whose CRC does not match. What happens next follows etcd's WAL repair
//! rule: **repair (truncation) is only permitted at the tail.**
//!
//! - **Torn tail** — no complete, CRC-valid record exists anywhere after the
//!   invalid frame. Appends are sequential and every batch is fsynced before
//!   the next write begins, so only the final (possibly never-acknowledged)
//!   batch can be in this state. The tail is physically truncated back to the
//!   end of the last valid record and the open succeeds — same recovery rule
//!   as `vibesql-storage::wal::reader::find_recovery_point`.
//! - **Mid-file corruption** — at least one complete, CRC-valid record
//!   follows the invalid frame ([`scan_for_valid_frame`]). The damage then
//!   sits inside the fsynced prefix; truncating there would silently drop
//!   entries already acknowledged via [`LogFlushed`] and roll back the
//!   fsynced vote (an election-safety violation once peers exist), while
//!   also destroying the forensic evidence. `open` instead fails loudly with
//!   an `InvalidData` error and leaves the file byte-for-byte untouched so an
//!   operator can intervene (post-A4, such a node can heal via snapshot).
//!
//! Boundary case: corruption confined to the *last* complete record is
//! indistinguishable from a torn write of that record at frame granularity
//! (nothing valid follows in either case), so it is treated as a torn tail.
//! That drops at most one record — possibly an acknowledged one — but the
//! alternative (erroring whenever the invalid region could hold a whole
//! frame) would reject every genuine torn tail longer than 8 bytes and
//! destroy ordinary crash tolerance. etcd's WAL makes the same trade.
//!
//! A torn *header* (file shorter than 32 bytes) means creation itself
//! crashed before any record could exist, so the file is reinitialized. A
//! CRC-valid record that fails to deserialize, or a wrong magic, is real
//! corruption and *is* an error.
//!
//! [`OpenraftBackend`]: crate::OpenraftBackend

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{Entry, LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use crate::openraft_backend::TypeConfig;

/// Name of the Raft log file inside the data directory.
const RAFT_LOG_FILE_NAME: &str = "raft.log";

/// Magic number for Raft log files: "VRFT" (cf. the data WAL's "VWAL").
const RAFT_LOG_MAGIC: &[u8; 4] = b"VRFT";

/// Current Raft log format version.
const RAFT_LOG_VERSION: u32 = 1;

/// Size of the file header in bytes (same layout as `WAL_HEADER_SIZE`).
const RAFT_LOG_HEADER_SIZE: usize = 32;

/// Per-record frame overhead: `[len:u32][crc:u32]`.
const RECORD_FRAME_SIZE: usize = 8;

/// CRC-32 (IEEE polynomial), identical to `vibesql-storage::wal::writer`.
fn crc32(data: &[u8]) -> u32 {
    const CRC32_TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 0;
            while j < 8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };

    let mut crc = 0xFFFFFFFF;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    crc ^ 0xFFFFFFFF
}

// ---------------------------------------------------------------------------
// Logical records
// ---------------------------------------------------------------------------

/// One logical record in the Raft log stream.
///
/// Everything openraft asks the log store to persist becomes a record in the
/// same ordered stream, so recovery is a single replay loop and mutations
/// never rewrite the file.
#[derive(Debug, Serialize, Deserialize)]
enum LogRecord {
    /// A log entry append (also overwrites a same-index entry that survived
    /// an earlier `Truncate`, which is how conflict rollback re-fills).
    Append(Entry<TypeConfig>),
    /// Vote state (term + voted_for). Must be fsynced before the save
    /// returns, or restarts can double-vote within a term.
    Vote(Vote<u64>),
    /// The last commit point openraft asked us to remember (optional API;
    /// persisting it lets startup re-apply committed entries immediately).
    Committed(Option<LogId<u64>>),
    /// Conflict rollback: remove all entries with `index >= .0`.
    Truncate(u64),
    /// Post-snapshot cleanup: remove all entries with `index <= .0.index`
    /// and advance the purge watermark.
    Purge(LogId<u64>),
}

/// In-memory image of the log, rebuilt from disk on open.
#[derive(Debug, Default)]
struct RaftLogImage {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged: Option<LogId<u64>>,
    /// Live entries keyed by raw raft index.
    entries: BTreeMap<u64, Entry<TypeConfig>>,
}

impl RaftLogImage {
    /// Apply one record. Shared by recovery replay and live mutation so both
    /// paths cannot drift.
    fn apply(&mut self, record: LogRecord) {
        match record {
            LogRecord::Append(entry) => {
                self.entries.insert(entry.log_id.index, entry);
            }
            LogRecord::Vote(vote) => self.vote = Some(vote),
            LogRecord::Committed(committed) => self.committed = committed,
            LogRecord::Truncate(since) => {
                self.entries.split_off(&since);
            }
            LogRecord::Purge(log_id) => {
                self.last_purged = Some(log_id);
                self.entries = self.entries.split_off(&(log_id.index + 1));
            }
        }
    }

    fn last_log_id(&self) -> Option<LogId<u64>> {
        self.entries.values().next_back().map(|e| e.log_id).or(self.last_purged)
    }

    /// Whether this log has any prior Raft state (drives the
    /// initialize-vs-recover decision in the backend).
    fn has_state(&self) -> bool {
        self.vote.is_some() || self.last_purged.is_some() || !self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// File layer
// ---------------------------------------------------------------------------

/// The open Raft log file, positioned at the end for appends.
#[derive(Debug)]
struct RaftLogFile {
    file: File,
}

impl RaftLogFile {
    /// Open (or create) the log file at `path`, replaying all valid records
    /// into a fresh [`RaftLogImage`].
    ///
    /// A torn tail (invalid frame with nothing valid after it) is physically
    /// truncated. Mid-file corruption (invalid frame with a valid record
    /// somewhere after it) is an error and leaves the file untouched — see
    /// the module docs for the rationale (etcd's tail-only repair rule).
    fn open(path: &Path) -> io::Result<(Self, RaftLogImage)> {
        let mut file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut image = RaftLogImage::default();

        if buf.len() < RAFT_LOG_HEADER_SIZE {
            // Empty file, or creation crashed mid-header: no record can have
            // been written yet (the header is synced before any record), so
            // reinitialize.
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            write_header(&mut file)?;
            file.sync_data()?;
            sync_parent_dir(path)?;
        } else {
            validate_header(&buf)?;
            let valid_end = replay_records(&buf, &mut image)?;
            if valid_end < buf.len() {
                // Replay stopped at an invalid frame. Truncation is only
                // legitimate if this is the tail: if any complete, CRC-valid
                // record exists after the invalid frame, the damage is in the
                // fsynced prefix and silently truncating would drop
                // acknowledged entries and roll back the fsynced vote.
                if let Some(later) = scan_for_valid_frame(&buf, valid_end + 1) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "raft log mid-file corruption: invalid record frame at offset \
                             {valid_end}, but a valid record follows at offset {later}; \
                             refusing to truncate acknowledged state — the file has been \
                             left untouched for inspection"
                        ),
                    ));
                }
                // True torn tail: discard it so future appends start from a
                // clean boundary.
                file.set_len(valid_end as u64)?;
                file.sync_data()?;
            }
            file.seek(SeekFrom::End(0))?;
        }

        Ok((Self { file }, image))
    }

    /// Frame, write, and fsync a batch of records (single fsync per batch).
    fn append_records(&mut self, records: &[LogRecord]) -> io::Result<()> {
        let mut buf = Vec::new();
        for record in records {
            let data = serde_json::to_vec(record).map_err(io::Error::other)?;
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32(&data).to_le_bytes());
            buf.extend_from_slice(&data);
        }
        self.file.write_all(&buf)?;
        // `sync_data` suffices: the file length only grows, and record
        // visibility is gated on the CRC of the data itself.
        self.file.sync_data()?;
        Ok(())
    }
}

fn write_header(file: &mut File) -> io::Result<()> {
    let mut buf = Vec::with_capacity(RAFT_LOG_HEADER_SIZE);
    buf.extend_from_slice(RAFT_LOG_MAGIC);
    buf.extend_from_slice(&RAFT_LOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&current_timestamp_ms().to_le_bytes());
    buf.extend_from_slice(&[0u8; 16]); // reserved
    debug_assert_eq!(buf.len(), RAFT_LOG_HEADER_SIZE);
    file.write_all(&buf)
}

fn validate_header(buf: &[u8]) -> io::Result<()> {
    if &buf[0..4] != RAFT_LOG_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid raft log file: expected magic 'VRFT', got '{}'",
                String::from_utf8_lossy(&buf[0..4])
            ),
        ));
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().expect("4-byte slice"));
    if version > RAFT_LOG_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported raft log version: {version} (current: {RAFT_LOG_VERSION})"),
        ));
    }
    Ok(())
}

/// Replay all valid records from `buf` into `image`.
///
/// Returns the byte offset just past the last valid record. Stops (without
/// error) at the first incomplete frame or CRC mismatch; the *caller*
/// decides whether that is a tolerable torn tail or mid-file corruption (via
/// [`scan_for_valid_frame`]). A CRC-valid record that fails to deserialize
/// is real corruption and returns an error.
fn replay_records(buf: &[u8], image: &mut RaftLogImage) -> io::Result<usize> {
    let mut offset = RAFT_LOG_HEADER_SIZE;
    while offset < buf.len() {
        if buf.len() - offset < RECORD_FRAME_SIZE {
            break; // torn frame
        }
        let len =
            u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("4-byte slice")) as usize;
        let crc = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().expect("4-byte slice"));
        let data_start = offset + RECORD_FRAME_SIZE;
        let Some(data_end) = data_start.checked_add(len).filter(|&end| end <= buf.len()) else {
            break; // torn data (or a torn length field pointing past EOF)
        };
        let data = &buf[data_start..data_end];
        if crc32(data) != crc {
            break; // torn or bit-flipped tail
        }
        let record: LogRecord = serde_json::from_slice(data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "raft log record at offset {offset} has a valid CRC but failed to decode: {e}"
                ),
            )
        })?;
        image.apply(record);
        offset = data_end;
    }
    Ok(offset)
}

/// Scan `buf[from..]` byte-by-byte for any complete, CRC-valid, decodable
/// record frame, returning the offset of the first one found.
///
/// Used by [`RaftLogFile::open`] to distinguish a torn tail (no valid record
/// after the invalid frame → truncation is safe) from mid-file corruption
/// (a valid record follows → truncating would destroy acknowledged state).
///
/// The scan advances one **byte** at a time rather than jumping frame
/// boundaries, because the corruption may be in the invalid frame's *length
/// field* — in that case the next frame boundary is unknowable, and only a
/// byte-forward scan can find the genuine record that starts right after the
/// corrupted frame's real extent. A candidate counts only if its length fits
/// inside the buffer, its CRC validates over the following bytes, *and* it
/// deserializes as a [`LogRecord`]; a random 8-byte window passing all three
/// is astronomically unlikely, and even a false positive errs in the safe
/// direction (refuse to open, operator inspects) rather than silently
/// truncating. The scan only runs on the already-exceptional invalid-frame
/// path, so its O(file bytes) cost is irrelevant.
fn scan_for_valid_frame(buf: &[u8], from: usize) -> Option<usize> {
    let mut offset = from;
    while offset + RECORD_FRAME_SIZE <= buf.len() {
        let len =
            u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("4-byte slice")) as usize;
        let data_start = offset + RECORD_FRAME_SIZE;
        if let Some(data_end) = data_start.checked_add(len).filter(|&end| end <= buf.len()) {
            let crc =
                u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().expect("4-byte slice"));
            let data = &buf[data_start..data_end];
            if crc32(data) == crc && serde_json::from_slice::<LogRecord>(data).is_ok() {
                return Some(offset);
            }
        }
        offset += 1;
    }
    None
}

/// fsync the directory containing `path` so the file's creation itself is
/// durable (POSIX requires syncing the directory entry separately).
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_data()?;
    }
    Ok(())
}

fn current_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The durable store
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct DurableInner {
    file: RaftLogFile,
    image: RaftLogImage,
}

/// File-backed [`RaftLogStorage`] for the durable
/// [`OpenraftBackend`](crate::OpenraftBackend) configuration.
///
/// Cloning shares the underlying store (it is a handle), which is what
/// openraft expects from `get_log_reader`.
///
/// The file I/O here is synchronous and runs inline on openraft's core task
/// (the same approach as openraft's own `rocksstore` example); a dedicated
/// I/O thread is not warranted until profiling says otherwise.
#[derive(Debug, Clone)]
pub(crate) struct DurableLogStore {
    inner: Arc<Mutex<DurableInner>>,
}

impl DurableLogStore {
    /// Open (or create) the Raft log under `dir`, replaying any existing
    /// state from disk.
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let (file, image) = RaftLogFile::open(&dir.join(RAFT_LOG_FILE_NAME))?;
        Ok(Self { inner: Arc::new(Mutex::new(DurableInner { file, image })) })
    }

    fn lock(&self) -> MutexGuard<'_, DurableInner> {
        self.inner.lock().expect("raft durable log store mutex poisoned")
    }

    /// `(has prior raft state, last raw log index)` — drives the backend's
    /// initialize-vs-recover decision and its recovery-apply wait.
    pub(crate) fn recovery_summary(&self) -> (bool, Option<u64>) {
        let inner = self.lock();
        (inner.image.has_state(), inner.image.last_log_id().map(|id| id.index))
    }

    /// Durably append one record and apply it to the in-memory image.
    fn append_and_apply(&self, record: LogRecord) -> io::Result<()> {
        let mut inner = self.lock();
        inner.file.append_records(std::slice::from_ref(&record))?;
        inner.image.apply(record);
        Ok(())
    }

    /// Durably append a batch of log entries (one fsync for the batch).
    ///
    /// This is the body of [`RaftLogStorage::append`], split out so tests can
    /// drive the durable path directly ([`LogFlushed`] is not constructible
    /// outside openraft).
    fn append_entries(&self, entries: Vec<Entry<TypeConfig>>) -> io::Result<()> {
        let records: Vec<LogRecord> = entries.into_iter().map(LogRecord::Append).collect();
        let mut inner = self.lock();
        inner.file.append_records(&records)?;
        for record in records {
            inner.image.apply(record);
        }
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for DurableLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.lock();
        Ok(inner.image.entries.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for DurableLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.lock();
        Ok(LogState {
            last_purged_log_id: inner.image.last_purged,
            last_log_id: inner.image.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        // fsyncs before returning: a non-durable vote breaks election safety.
        self.append_and_apply(LogRecord::Vote(*vote))
            .map_err(|e| StorageError::IO { source: StorageIOError::write_vote(&e) })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.lock().image.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.append_and_apply(LogRecord::Committed(committed))
            .map_err(|e| StorageError::IO { source: StorageIOError::write(&e) })
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.lock().image.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // The batch is written and fsynced before the callback fires: the
        // storage-v2 contract is that `log_io_completed(Ok)` means *durable*,
        // not merely accepted.
        match self.append_entries(entries.into_iter().collect()) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.log_io_completed(Err(io::Error::new(e.kind(), e.to_string())));
                Err(StorageError::IO { source: StorageIOError::write_logs(&e) })
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.append_and_apply(LogRecord::Truncate(log_id.index))
            .map_err(|e| StorageError::IO { source: StorageIOError::write_logs(&e) })
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.append_and_apply(LogRecord::Purge(log_id))
            .map_err(|e| StorageError::IO { source: StorageIOError::write_logs(&e) })
    }
}

// ---------------------------------------------------------------------------
// Tests: framing, recovery, and crash tolerance at the store level
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use openraft::{CommittedLeaderId, EntryPayload};
    use tempfile::TempDir;

    use super::*;

    fn entry(term: u64, index: u64, payload: &[u8]) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(payload.to_vec()),
        }
    }

    fn entry_indices(store: &DurableLogStore) -> Vec<u64> {
        store.lock().image.entries.keys().copied().collect()
    }

    fn log_file_path(dir: &TempDir) -> std::path::PathBuf {
        dir.path().join(RAFT_LOG_FILE_NAME)
    }

    #[test]
    fn fresh_store_is_empty() {
        let dir = TempDir::new().unwrap();
        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(store.recovery_summary(), (false, None));
        assert!(entry_indices(&store).is_empty());
    }

    #[test]
    fn entries_survive_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store
                .append_entries(vec![
                    entry(1, 1, b"one"),
                    entry(1, 2, b"two"),
                    entry(1, 3, b"three"),
                ])
                .unwrap();
        }

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(store.recovery_summary(), (true, Some(3)));
        assert_eq!(entry_indices(&store), vec![1, 2, 3]);
        let payload = match &store.lock().image.entries[&2].payload {
            EntryPayload::Normal(data) => data.clone(),
            other => panic!("expected Normal payload, got {other:?}"),
        };
        assert_eq!(payload, b"two");
    }

    #[test]
    fn vote_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let vote = Vote::new(7, 1);
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_and_apply(LogRecord::Vote(vote)).unwrap();
        }

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(store.lock().image.vote, Some(vote));
        // A vote alone counts as prior state: recovery must not re-initialize.
        assert_eq!(store.recovery_summary(), (true, None));
    }

    #[test]
    fn committed_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let committed = Some(LogId::new(CommittedLeaderId::new(1, 1), 2));
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries(vec![entry(1, 1, b"a"), entry(1, 2, b"b")]).unwrap();
            store.append_and_apply(LogRecord::Committed(committed)).unwrap();
        }

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(store.lock().image.committed, committed);
    }

    #[test]
    fn truncate_survives_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries((1..=5).map(|i| entry(1, i, b"x")).collect()).unwrap();
            // Conflict rollback: drop entries >= 3...
            store.append_and_apply(LogRecord::Truncate(3)).unwrap();
            // ...then refill index 3 at a later term, as a real conflict would.
            store.append_entries(vec![entry(2, 3, b"replacement")]).unwrap();
        }

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![1, 2, 3]);
        let replayed = store.lock().image.entries[&3].log_id;
        assert_eq!(replayed, LogId::new(CommittedLeaderId::new(2, 1), 3));
    }

    #[test]
    fn purge_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let purge_id = LogId::new(CommittedLeaderId::new(1, 1), 3);
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries((1..=5).map(|i| entry(1, i, b"x")).collect()).unwrap();
            store.append_and_apply(LogRecord::Purge(purge_id)).unwrap();
        }

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![4, 5]);
        assert_eq!(store.lock().image.last_purged, Some(purge_id));
        assert_eq!(store.recovery_summary(), (true, Some(5)));
    }

    #[test]
    fn fully_purged_log_reports_purge_watermark_as_last_log_id() {
        let dir = TempDir::new().unwrap();
        let purge_id = LogId::new(CommittedLeaderId::new(1, 1), 2);
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries(vec![entry(1, 1, b"a"), entry(1, 2, b"b")]).unwrap();
            store.append_and_apply(LogRecord::Purge(purge_id)).unwrap();
        }

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert!(entry_indices(&store).is_empty());
        assert_eq!(store.lock().image.last_log_id(), Some(purge_id));
    }

    #[test]
    fn torn_trailing_record_is_discarded() {
        let dir = TempDir::new().unwrap();
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store
                .append_entries(vec![
                    entry(1, 1, b"one"),
                    entry(1, 2, b"two"),
                    entry(1, 3, b"three"),
                ])
                .unwrap();
        }

        // Simulate a crash mid-write: chop bytes off the last record.
        let path = log_file_path(&dir);
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![1, 2]);
        assert_eq!(store.recovery_summary(), (true, Some(2)));

        // The torn tail was physically removed, so new appends land on a
        // clean boundary and survive another reopen.
        store.append_entries(vec![entry(1, 3, b"retry")]).unwrap();
        drop(store);

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![1, 2, 3]);
    }

    #[test]
    fn torn_frame_header_is_discarded() {
        let dir = TempDir::new().unwrap();
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries(vec![entry(1, 1, b"one")]).unwrap();
        }

        // Append 4 lonely bytes: a length field with no checksum or data.
        let path = log_file_path(&dir);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&42u32.to_le_bytes()).unwrap();
        drop(file);

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![1]);
    }

    /// Boundary case: corruption in the LAST complete frame, with nothing
    /// valid after it, is indistinguishable from a torn write of that frame
    /// at frame granularity — so it falls under the torn-tail rule (truncate
    /// and open) rather than the mid-file rule (refuse). This drops at most
    /// that one final record; see the module docs for why the alternative
    /// (erroring whenever the invalid region could hold a frame) would
    /// reject every genuine torn tail and destroy ordinary crash tolerance.
    #[test]
    fn corrupt_checksum_truncates_the_tail() {
        let dir = TempDir::new().unwrap();
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries(vec![entry(1, 1, b"one"), entry(1, 2, b"two")]).unwrap();
        }

        // Flip a bit in the last record's data.
        let path = log_file_path(&dir);
        let mut buf = std::fs::read(&path).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![1]);
    }

    /// Byte offsets of every complete record frame in `buf`, in order.
    fn record_offsets(buf: &[u8]) -> Vec<usize> {
        let mut offsets = Vec::new();
        let mut offset = RAFT_LOG_HEADER_SIZE;
        while offset < buf.len() {
            offsets.push(offset);
            let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += RECORD_FRAME_SIZE + len;
        }
        offsets
    }

    /// The reproduction from the PR #5357 judge review: 5 fsync-acknowledged
    /// entries plus a fsynced `Vote(term 7)` and `Committed(5)`, then a
    /// single bit flip inside **entry 2's** data. Valid records follow the
    /// damaged frame, so this is mid-file corruption, not a torn tail: open
    /// must refuse (no silent rollback of acknowledged entries or of the
    /// vote) and must not truncate a single byte (forensic evidence).
    #[test]
    fn mid_file_corruption_is_rejected_and_file_untouched() {
        let dir = TempDir::new().unwrap();
        {
            let store = DurableLogStore::open(dir.path()).unwrap();
            store.append_entries((1..=5).map(|i| entry(1, i, b"payload")).collect()).unwrap();
            store.append_and_apply(LogRecord::Vote(Vote::new(7, 1))).unwrap();
            store
                .append_and_apply(LogRecord::Committed(Some(LogId::new(
                    CommittedLeaderId::new(1, 1),
                    5,
                ))))
                .unwrap();
        }

        let path = log_file_path(&dir);
        let mut buf = std::fs::read(&path).unwrap();
        let len_before = buf.len() as u64;
        // Flip one byte inside entry 2's data.
        let frame2 = record_offsets(&buf)[1];
        buf[frame2 + RECORD_FRAME_SIZE + 2] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        let err = DurableLogStore::open(dir.path()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("mid-file corruption"), "unexpected error: {err}");
        // The file was left byte-for-byte untouched: nothing truncated.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before);

        // The refusal is stable: a second open attempt fails the same way.
        let err = DurableLogStore::open(dir.path()).unwrap_err();
        assert!(err.to_string().contains("mid-file corruption"), "unexpected error: {err}");
    }

    /// Corruption in a middle frame's LENGTH field — the nastiest case,
    /// because the next frame boundary becomes unknowable from the frame
    /// itself. The byte-forward scan still finds the genuine records that
    /// follow, so both an out-of-bounds corrupted length (`u32::MAX`) and a
    /// plausible in-bounds one (`1`, which misaligns the CRC check) are
    /// detected as mid-file corruption rather than a torn tail.
    #[test]
    fn mid_file_corruption_in_length_field_is_rejected() {
        for corrupt_len in [u32::MAX, 1u32] {
            let dir = TempDir::new().unwrap();
            {
                let store = DurableLogStore::open(dir.path()).unwrap();
                store.append_entries((1..=5).map(|i| entry(1, i, b"payload")).collect()).unwrap();
            }

            let path = log_file_path(&dir);
            let mut buf = std::fs::read(&path).unwrap();
            let len_before = buf.len() as u64;
            let frame2 = record_offsets(&buf)[1];
            buf[frame2..frame2 + 4].copy_from_slice(&corrupt_len.to_le_bytes());
            std::fs::write(&path, &buf).unwrap();

            let err = DurableLogStore::open(dir.path()).unwrap_err();
            assert!(
                err.to_string().contains("mid-file corruption"),
                "len={corrupt_len}: unexpected error: {err}"
            );
            assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before);
        }
    }

    #[test]
    fn torn_header_reinitializes_the_file() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        // Creation crashed mid-header: fewer than 32 bytes, no records.
        std::fs::write(log_file_path(&dir), b"VRFT\x01").unwrap();

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(store.recovery_summary(), (false, None));
        store.append_entries(vec![entry(1, 1, b"one")]).unwrap();
        drop(store);

        let store = DurableLogStore::open(dir.path()).unwrap();
        assert_eq!(entry_indices(&store), vec![1]);
    }

    #[test]
    fn foreign_file_is_rejected() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        std::fs::write(log_file_path(&dir), [b'X'; 64]).unwrap();

        let err = DurableLogStore::open(dir.path()).unwrap_err();
        assert!(err.to_string().contains("expected magic"), "unexpected error: {err}");
    }

    #[test]
    fn future_version_is_rejected() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(RAFT_LOG_MAGIC);
        buf.extend_from_slice(&(RAFT_LOG_VERSION + 1).to_le_bytes());
        buf.extend_from_slice(&[0u8; 24]);
        std::fs::write(log_file_path(&dir), &buf).unwrap();

        let err = DurableLogStore::open(dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("unsupported raft log version"),
            "unexpected error: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Backend-level crash recovery (OpenraftBackend::with_data_dir)
    // -----------------------------------------------------------------------

    use crate::{ConsensusBackend, OpenraftBackend};

    /// Replay the on-disk record stream into a fresh image (without opening
    /// the file for writing, so a live store can keep it).
    fn image_on_disk(dir: &TempDir) -> RaftLogImage {
        let buf = std::fs::read(log_file_path(dir)).unwrap();
        let mut image = RaftLogImage::default();
        replay_records(&buf, &mut image).unwrap();
        image
    }

    /// Proposals and the vote survive a clean shutdown + reopen: committed
    /// entries read back identically, numbering continues, and the term is
    /// not reset. (openraft restores a node whose persisted vote marks it
    /// leader directly into leadership at the *same* term — no new election
    /// — which only works because the vote was durably saved.)
    #[tokio::test]
    async fn backend_state_survives_restart() {
        let dir = TempDir::new().unwrap();

        let term_before = {
            let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
            for i in 1..=3u64 {
                let idx = backend.propose(format!("entry-{i}")).await.unwrap();
                assert_eq!(idx, i);
            }
            let term = backend.current_term();
            assert!(term >= 1, "an elected leader has a non-zero term");
            backend.shutdown().await.unwrap();
            term
        };

        // The vote (term + voted_for) reached disk before shutdown.
        let vote = image_on_disk(&dir).vote.expect("vote must be persisted");
        assert_eq!(vote.leader_id().term, term_before);
        assert_eq!(vote.leader_id().voted_for(), Some(1));

        let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
        assert_eq!(backend.last_index(), 3);
        for i in 1..=3u64 {
            assert_eq!(backend.read_committed(i).await.unwrap(), format!("entry-{i}"));
        }
        let term_after = backend.current_term();
        assert!(
            term_after >= term_before,
            "recovered term must not reset (before: {term_before}, after: {term_after})"
        );

        // Numbering continues after the recovered prefix.
        assert_eq!(backend.propose("entry-4".to_string()).await.unwrap(), 4);
        assert_eq!(backend.read_committed(4).await.unwrap(), "entry-4");
    }

    /// Walk the record stream and return the byte offset of the last
    /// [`LogRecord::Append`] frame. Panics if there is none.
    fn last_append_offset(buf: &[u8]) -> usize {
        let mut offset = RAFT_LOG_HEADER_SIZE;
        let mut last_append = None;
        while offset < buf.len() {
            let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
            let data_start = offset + RECORD_FRAME_SIZE;
            let data_end = data_start + len;
            let record: LogRecord = serde_json::from_slice(&buf[data_start..data_end]).unwrap();
            if matches!(record, LogRecord::Append(_)) {
                last_append = Some(offset);
            }
            offset = data_end;
        }
        last_append.expect("log contains at least one append record")
    }

    /// A crash mid-append leaves a torn trailing record; reopening the
    /// backend recovers the intact prefix (no error), and new proposals
    /// continue from there.
    #[tokio::test]
    async fn backend_recovers_from_torn_trailing_append() {
        let dir = TempDir::new().unwrap();
        {
            let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
            for i in 1..=3u64 {
                backend.propose(format!("entry-{i}")).await.unwrap();
            }
            backend.shutdown().await.unwrap();
        }

        // Simulate the crash: cut the file partway into the last appended
        // entry (entry-3). Everything from that frame on becomes a torn tail.
        let path = dir.path().join(RAFT_LOG_FILE_NAME);
        let buf = std::fs::read(&path).unwrap();
        let cut = last_append_offset(&buf) + RECORD_FRAME_SIZE + 1;
        OpenOptions::new().write(true).open(&path).unwrap().set_len(cut as u64).unwrap();

        let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
        assert_eq!(backend.last_index(), 2);
        assert_eq!(backend.read_committed(1).await.unwrap(), "entry-1");
        assert_eq!(backend.read_committed(2).await.unwrap(), "entry-2");

        // The torn entry was never acknowledged as committed, so its index
        // is simply reassigned to the next proposal.
        assert_eq!(backend.propose("entry-3-retry".to_string()).await.unwrap(), 3);
        assert_eq!(backend.read_committed(3).await.unwrap(), "entry-3-retry");
    }

    /// Restoring a snapshot into a directory that already holds Raft state
    /// would create two competing histories; it must be rejected.
    #[tokio::test]
    async fn backend_snapshot_restore_into_stateful_dir_is_rejected() {
        let dir = TempDir::new().unwrap();
        let snapshot = {
            let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
            backend.propose("entry-1".to_string()).await.unwrap();
            let snapshot = backend.snapshot().await.unwrap();
            backend.shutdown().await.unwrap();
            snapshot
        };

        let err =
            OpenraftBackend::<String>::from_snapshot_with_data_dir(&snapshot, dir.path()).await;
        assert!(
            matches!(err, Err(crate::ConsensusError::Backend(ref msg)) if msg.contains("already contains raft state")),
            "unexpected result: {err:?}"
        );
    }

    /// A durable backend that was shut down can be reopened from the same
    /// directory (shutdown releases nothing that recovery needs).
    #[tokio::test]
    async fn backend_shutdown_then_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
            backend.propose("entry-1".to_string()).await.unwrap();
            backend.shutdown().await.unwrap();
            // After shutdown the core is stopped: proposals fail fast.
            assert!(backend.propose("entry-2".to_string()).await.is_err());
        }

        let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
        assert_eq!(backend.last_index(), 1);
        assert_eq!(backend.read_committed(1).await.unwrap(), "entry-1");
    }
}
