// ============================================================================
// WAL Recovery System
// ============================================================================
//
// Implements crash recovery by loading the latest checkpoint and replaying
// WAL entries. This enables database durability across restarts.
//
// ## Recovery Process
//
// ```text
// ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
// │ Find        │────▶│ Load        │────▶│ Replay      │
// │ Checkpoint  │     │ Checkpoint  │     │ WAL         │
// └─────────────┘     └─────────────┘     └──────┬──────┘
//                                                │
//                            ┌───────────────────┘
//                            ▼
//                     ┌─────────────┐
//                     │ Ready       │
//                     └─────────────┘
// ```
//
// ## Transaction Recovery
//
// During WAL replay, we track transaction state:
// - TxnBegin: Add transaction to in-flight set
// - TxnCommit: Mark transaction as committed
// - TxnRollback: Mark transaction as rolled back
//
// Only operations from committed transactions are applied. Operations from
// uncommitted transactions (those without TxnCommit before crash) are ignored.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
};

use crate::{
    persistence::binary::{read_catalog_v, read_data, read_header},
    wal::{
        checkpoint::{read_checkpoint_data, CheckpointInfo, CheckpointWriter},
        entry::{Lsn, WalOp},
        reader::{ReadResult, WalReader},
    },
    Database, StorageError,
};

/// Recovery configuration options
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Whether to validate checkpoint checksums
    pub validate_checksums: bool,
    /// Whether to stop on first WAL corruption or continue with partial recovery
    pub stop_on_corruption: bool,
    /// Maximum number of checkpoints to try if the latest is corrupted
    pub max_checkpoint_retries: usize,
    /// Progress callback interval (number of entries between callbacks)
    pub progress_interval: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            validate_checksums: true,
            stop_on_corruption: true,
            max_checkpoint_retries: 3,
            progress_interval: 10000,
        }
    }
}

/// Statistics about a recovery operation
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Checkpoint LSN that was loaded (0 if no checkpoint)
    pub checkpoint_lsn: Lsn,
    /// Highest LSN observed anywhere during recovery — the loaded checkpoint LSN
    /// or any WAL entry read (including entries at/below the checkpoint LSN).
    ///
    /// Callers resume the live WAL's LSN counter at `last_lsn + 1` so that new
    /// entries — and the checkpoints stamped from them — keep advancing
    /// monotonically across process restarts (issue #5766).
    pub last_lsn: Lsn,
    /// Number of WAL entries replayed
    pub entries_replayed: u64,
    /// Number of entries skipped (before checkpoint LSN)
    pub entries_skipped: u64,
    /// Number of transactions committed during replay
    pub transactions_committed: u64,
    /// Number of transactions rolled back (explicit or incomplete)
    pub transactions_rolled_back: u64,
    /// Number of inserts applied
    pub inserts_applied: u64,
    /// Number of updates applied
    pub updates_applied: u64,
    /// Number of deletes applied
    pub deletes_applied: u64,
    /// Number of tables created
    pub tables_created: u64,
    /// Number of indexes created
    pub indexes_created: u64,
    /// Whether corruption was detected
    pub corruption_detected: bool,
    /// Position where corruption was detected (if any)
    pub corruption_position: Option<u64>,
}

/// Transaction state during recovery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    /// Transaction started but not yet committed
    InFlight,
    /// Transaction successfully committed
    Committed,
    /// Transaction explicitly rolled back
    RolledBack,
}

/// Tracks transaction state during WAL replay
struct TransactionTracker {
    /// State of each transaction
    states: HashMap<u64, TransactionState>,
    /// Operations buffered for each in-flight transaction
    /// Key: txn_id, Value: list of (lsn, op)
    buffered_ops: HashMap<u64, Vec<(Lsn, WalOp)>>,
}

impl TransactionTracker {
    fn new() -> Self {
        Self { states: HashMap::new(), buffered_ops: HashMap::new() }
    }

    /// Begin tracking a transaction
    fn begin_transaction(&mut self, txn_id: u64) {
        self.states.insert(txn_id, TransactionState::InFlight);
        self.buffered_ops.insert(txn_id, Vec::new());
    }

    /// Mark a transaction as committed and return its buffered operations
    fn commit_transaction(&mut self, txn_id: u64) -> Vec<(Lsn, WalOp)> {
        self.states.insert(txn_id, TransactionState::Committed);
        self.buffered_ops.remove(&txn_id).unwrap_or_default()
    }

    /// Mark a transaction as rolled back and discard its operations
    fn rollback_transaction(&mut self, txn_id: u64) {
        self.states.insert(txn_id, TransactionState::RolledBack);
        self.buffered_ops.remove(&txn_id);
    }

    /// Buffer an operation for a transaction
    fn buffer_op(&mut self, txn_id: u64, lsn: Lsn, op: WalOp) {
        if let Some(ops) = self.buffered_ops.get_mut(&txn_id) {
            ops.push((lsn, op));
        }
    }

    /// Check if a transaction is in-flight
    fn is_in_flight(&self, txn_id: u64) -> bool {
        matches!(self.states.get(&txn_id), Some(TransactionState::InFlight))
    }

    /// Get all in-flight transaction IDs (uncommitted at end of WAL)
    fn get_in_flight_transactions(&self) -> Vec<u64> {
        self.states
            .iter()
            .filter(|(_, state)| **state == TransactionState::InFlight)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get number of committed transactions
    fn committed_count(&self) -> u64 {
        self.states.values().filter(|s| **s == TransactionState::Committed).count() as u64
    }

    /// Get number of rolled back transactions (explicit + in-flight at end)
    fn rolled_back_count(&self) -> u64 {
        self.states
            .values()
            .filter(|s| **s == TransactionState::RolledBack || **s == TransactionState::InFlight)
            .count() as u64
    }
}

/// Recovery manager for restoring database state from checkpoint and WAL
pub struct RecoveryManager {
    /// Configuration
    config: RecoveryConfig,
    /// Checkpoint directory
    checkpoint_dir: PathBuf,
    /// WAL file path
    wal_path: Option<PathBuf>,
}

impl RecoveryManager {
    /// Create a new recovery manager
    pub fn new<P: AsRef<Path>>(checkpoint_dir: P) -> Self {
        Self {
            config: RecoveryConfig::default(),
            checkpoint_dir: checkpoint_dir.as_ref().to_path_buf(),
            wal_path: None,
        }
    }

    /// Create with custom configuration
    pub fn with_config<P: AsRef<Path>>(checkpoint_dir: P, config: RecoveryConfig) -> Self {
        Self { config, checkpoint_dir: checkpoint_dir.as_ref().to_path_buf(), wal_path: None }
    }

    /// Set the WAL file path
    pub fn with_wal<P: AsRef<Path>>(mut self, wal_path: P) -> Self {
        self.wal_path = Some(wal_path.as_ref().to_path_buf());
        self
    }

    /// Perform full recovery and return the recovered database
    pub fn recover(&self) -> Result<(Database, RecoveryStats), StorageError> {
        let mut stats = RecoveryStats::default();

        // Step 1: Find and load the latest valid checkpoint
        let (mut db, checkpoint_lsn) = self.load_latest_checkpoint(&mut stats)?;
        stats.checkpoint_lsn = checkpoint_lsn;
        stats.last_lsn = stats.last_lsn.max(checkpoint_lsn);

        // Step 2: Replay WAL entries after the checkpoint LSN
        if let Some(ref wal_path) = self.wal_path {
            if wal_path.exists() {
                self.replay_wal(&mut db, wal_path, checkpoint_lsn, &mut stats)?;
            }
        }

        Ok((db, stats))
    }

    /// Find and load the latest valid checkpoint
    fn load_latest_checkpoint(
        &self,
        stats: &mut RecoveryStats,
    ) -> Result<(Database, Lsn), StorageError> {
        // List all checkpoints
        let checkpoints = self.list_checkpoints()?;

        if checkpoints.is_empty() {
            // No checkpoints found - start with empty database
            log::info!("No checkpoints found, starting with empty database");
            return Ok((Database::new(), 0));
        }

        // Try checkpoints from newest to oldest
        let mut retries = 0;
        for checkpoint in checkpoints.into_iter().rev() {
            if retries >= self.config.max_checkpoint_retries {
                log::warn!(
                    "Exceeded max checkpoint retries ({}), starting with empty database",
                    self.config.max_checkpoint_retries
                );
                return Ok((Database::new(), 0));
            }

            match self.load_checkpoint(&checkpoint.path) {
                Ok((db, lsn)) => {
                    log::info!("Loaded checkpoint at LSN {} from {:?}", lsn, checkpoint.path);
                    return Ok((db, lsn));
                }
                Err(e) => {
                    log::warn!("Failed to load checkpoint {:?}: {}", checkpoint.path, e);
                    retries += 1;
                    stats.corruption_detected = true;
                }
            }
        }

        // All checkpoints failed - start with empty database
        log::warn!("All checkpoints failed to load, starting with empty database");
        Ok((Database::new(), 0))
    }

    /// List all checkpoint files sorted by LSN
    fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>, StorageError> {
        if !self.checkpoint_dir.exists() {
            return Ok(Vec::new());
        }

        let checkpoint_writer = CheckpointWriter::new(&self.checkpoint_dir)?;
        checkpoint_writer.list_checkpoints()
    }

    /// Load a specific checkpoint file and return the database and LSN
    fn load_checkpoint(&self, path: &Path) -> Result<(Database, Lsn), StorageError> {
        // Read checkpoint data (header + binary database data)
        let (header, data) = read_checkpoint_data(path)?;

        // Parse the binary database format
        let mut reader = BufReader::new(&data[..]);

        // Read and validate header, get version
        let version = read_header(&mut reader)?;

        // Read catalog section with version awareness
        let mut db = read_catalog_v(&mut reader, version)?;

        // Read data section (version-aware: v6 has no per-row MVCC prefix; v7+ does)
        read_data(&mut reader, &mut db, version)?;

        Ok((db, header.lsn))
    }

    /// Replay WAL entries after the given LSN
    fn replay_wal(
        &self,
        db: &mut Database,
        wal_path: &Path,
        start_lsn: Lsn,
        stats: &mut RecoveryStats,
    ) -> Result<(), StorageError> {
        let file = File::open(wal_path)
            .map_err(|e| StorageError::IoError(format!("Failed to open WAL: {}", e)))?;
        let reader = BufReader::new(file);

        let mut wal_reader = WalReader::open(reader)?;
        let mut tracker = TransactionTracker::new();
        let mut current_txn_id: Option<u64> = None;

        // First pass: read all entries and track transactions
        loop {
            match wal_reader.read_entry()? {
                ReadResult::Entry(entry) => {
                    // Track the highest LSN seen so callers can resume the live
                    // WAL counter past it (even for entries we skip below).
                    stats.last_lsn = stats.last_lsn.max(entry.lsn);

                    // Skip entries at or before checkpoint LSN
                    if entry.lsn <= start_lsn {
                        stats.entries_skipped += 1;
                        continue;
                    }

                    // Process the entry based on its operation type
                    match &entry.op {
                        WalOp::TxnBegin { txn_id } => {
                            tracker.begin_transaction(*txn_id);
                            current_txn_id = Some(*txn_id);
                        }
                        WalOp::TxnCommit { txn_id } => {
                            // Apply all buffered operations for this transaction
                            let ops = tracker.commit_transaction(*txn_id);
                            for (lsn, op) in ops {
                                self.apply_op(db, lsn, op, stats)?;
                            }
                            if current_txn_id == Some(*txn_id) {
                                current_txn_id = None;
                            }
                        }
                        WalOp::TxnRollback { txn_id } => {
                            tracker.rollback_transaction(*txn_id);
                            if current_txn_id == Some(*txn_id) {
                                current_txn_id = None;
                            }
                        }
                        WalOp::CheckpointBegin { .. } | WalOp::CheckpointComplete { .. } => {
                            // Skip checkpoint markers during replay
                        }
                        op => {
                            // DML/DDL operation
                            if let Some(txn_id) = current_txn_id {
                                // Part of a transaction - buffer it
                                if tracker.is_in_flight(txn_id) {
                                    tracker.buffer_op(txn_id, entry.lsn, op.clone());
                                }
                            } else {
                                // Standalone operation (auto-commit mode) - apply immediately
                                self.apply_op(db, entry.lsn, op.clone(), stats)?;
                            }
                        }
                    }

                    stats.entries_replayed += 1;

                    // Progress callback
                    if stats.entries_replayed.is_multiple_of(self.config.progress_interval as u64) {
                        log::debug!(
                            "Recovery progress: {} entries replayed",
                            stats.entries_replayed
                        );
                    }
                }
                ReadResult::Eof => {
                    break;
                }
                ReadResult::Corruption { position } => {
                    stats.corruption_detected = true;
                    stats.corruption_position = Some(position);

                    if self.config.stop_on_corruption {
                        log::warn!(
                            "WAL corruption detected at position {}, stopping replay",
                            position
                        );
                        break;
                    } else {
                        log::warn!(
                            "WAL corruption detected at position {}, continuing with partial recovery",
                            position
                        );
                        break;
                    }
                }
            }
        }

        // Handle in-flight transactions (uncommitted at crash time)
        let in_flight = tracker.get_in_flight_transactions();
        if !in_flight.is_empty() {
            log::info!(
                "Rolling back {} uncommitted transactions: {:?}",
                in_flight.len(),
                in_flight
            );
            // Operations were buffered but never committed - just discard them
            // (they're already in buffered_ops which we don't apply)
        }

        stats.transactions_committed = tracker.committed_count();
        stats.transactions_rolled_back = tracker.rolled_back_count();

        log::info!(
            "WAL replay complete: {} entries, {} committed txns, {} rolled back txns",
            stats.entries_replayed,
            stats.transactions_committed,
            stats.transactions_rolled_back
        );

        Ok(())
    }

    /// Apply a single WAL operation to the database
    fn apply_op(
        &self,
        db: &mut Database,
        _lsn: Lsn,
        op: WalOp,
        stats: &mut RecoveryStats,
    ) -> Result<(), StorageError> {
        match op {
            WalOp::Insert { table_id: _, table_name, row_id, values } => {
                // WAL format v2+ carries the fully-qualified table_name so the
                // mutation can be routed to the correct table during replay.
                // (v1 logs serialize an empty name; such DML is unroutable and
                // is skipped — v1 DML replay was never functional.)
                if table_name.is_empty() {
                    log::warn!(
                        "Skipping Insert during recovery: WAL entry has no table_name \
                         (legacy v1 format, row_id={})",
                        row_id
                    );
                    return Ok(());
                }
                match db.get_table_mut(&table_name) {
                    Some(table) => {
                        let row = crate::row::Row::new(values);
                        match table.insert(row) {
                            Ok(()) => stats.inserts_applied += 1,
                            Err(e) => log::warn!(
                                "Failed to apply Insert to {} during recovery: {}",
                                table_name,
                                e
                            ),
                        }
                    }
                    None => log::warn!(
                        "Skipping Insert during recovery: table {} not found",
                        table_name
                    ),
                }
            }
            WalOp::Update { table_id: _, table_name, row_id, old_values: _, new_values } => {
                if table_name.is_empty() {
                    log::warn!(
                        "Skipping Update during recovery: WAL entry has no table_name \
                         (legacy v1 format, row_id={})",
                        row_id
                    );
                    return Ok(());
                }
                match db.get_table_mut(&table_name) {
                    Some(table) => {
                        let row = crate::row::Row::new(new_values);
                        match table.update_row(row_id as usize, row) {
                            Ok(()) => stats.updates_applied += 1,
                            Err(e) => log::warn!(
                                "Failed to apply Update to {} (row {}) during recovery: {}",
                                table_name,
                                row_id,
                                e
                            ),
                        }
                    }
                    None => log::warn!(
                        "Skipping Update during recovery: table {} not found",
                        table_name
                    ),
                }
            }
            WalOp::Delete { table_id: _, table_name, row_id, old_values: _ } => {
                if table_name.is_empty() {
                    log::warn!(
                        "Skipping Delete during recovery: WAL entry has no table_name \
                         (legacy v1 format, row_id={})",
                        row_id
                    );
                    return Ok(());
                }
                match db.get_table_mut(&table_name) {
                    Some(table) => {
                        if table.mark_deleted_inplace(row_id as usize) {
                            stats.deletes_applied += 1;
                        } else {
                            log::warn!(
                                "Delete during recovery did not mark a row in {} (row {} \
                                 missing or already deleted)",
                                table_name,
                                row_id
                            );
                        }
                    }
                    None => log::warn!(
                        "Skipping Delete during recovery: table {} not found",
                        table_name
                    ),
                }
            }
            WalOp::CreateTable { table_id: _, table_name, schema_data } => {
                // Deserialize schema and create table
                match deserialize_table_schema(&schema_data) {
                    Ok(schema) => {
                        if db.get_table(&table_name).is_none() {
                            if let Err(e) = db.create_table(schema) {
                                log::warn!(
                                    "Failed to create table {} during recovery: {}",
                                    table_name,
                                    e
                                );
                            } else {
                                stats.tables_created += 1;
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("Failed to deserialize schema for table {}: {}", table_name, e);
                    }
                }
            }
            WalOp::DropTable { table_id: _, table_name } => {
                if let Err(e) = db.drop_table(&table_name) {
                    log::warn!("Failed to drop table {} during recovery: {}", table_name, e);
                }
            }
            WalOp::CreateIndex {
                index_id: _,
                index_name,
                table_id: _,
                column_indices,
                is_unique,
            } => {
                // Index creation during recovery
                // For now, just log - full implementation would need table name resolution
                log::trace!(
                    "Would create index {} on columns {:?}, unique={}",
                    index_name,
                    column_indices,
                    is_unique
                );
                stats.indexes_created += 1;
            }
            WalOp::DropIndex { index_id: _, index_name } => {
                log::trace!("Would drop index {}", index_name);
            }
            WalOp::TxnBegin { .. } | WalOp::TxnCommit { .. } | WalOp::TxnRollback { .. } => {
                // These are handled by the transaction tracker
            }
            WalOp::CheckpointBegin { .. } | WalOp::CheckpointComplete { .. } => {
                // Skip checkpoint markers
            }
        }
        Ok(())
    }
}

/// Deserialize a table schema from the WAL format
fn deserialize_table_schema(data: &[u8]) -> Result<vibesql_catalog::TableSchema, StorageError> {
    use vibesql_catalog::{ColumnSchema, TableSchema};

    let mut pos = 0;

    // Read table name (null-terminated)
    let name_end = data[pos..].iter().position(|&b| b == 0).ok_or_else(|| {
        StorageError::IoError("Invalid schema data: missing table name".to_string())
    })?;
    let table_name = String::from_utf8(data[pos..pos + name_end].to_vec())
        .map_err(|e| StorageError::IoError(format!("Invalid UTF-8 in table name: {}", e)))?;
    pos += name_end + 1;

    // Read column count
    if pos + 4 > data.len() {
        return Err(StorageError::IoError("Invalid schema data: missing column count".to_string()));
    }
    let column_count =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    // Read columns
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        // Column name (null-terminated)
        let name_end = data[pos..].iter().position(|&b| b == 0).ok_or_else(|| {
            StorageError::IoError("Invalid schema data: missing column name".to_string())
        })?;
        let column_name = String::from_utf8(data[pos..pos + name_end].to_vec())
            .map_err(|e| StorageError::IoError(format!("Invalid UTF-8 in column name: {}", e)))?;
        pos += name_end + 1;

        // Data type (null-terminated string representation)
        let type_end = data[pos..].iter().position(|&b| b == 0).ok_or_else(|| {
            StorageError::IoError("Invalid schema data: missing data type".to_string())
        })?;
        let type_str = String::from_utf8(data[pos..pos + type_end].to_vec())
            .map_err(|e| StorageError::IoError(format!("Invalid UTF-8 in data type: {}", e)))?;
        pos += type_end + 1;

        // Parse data type from debug string
        let data_type = parse_data_type(&type_str)?;

        // Nullable flag
        if pos >= data.len() {
            return Err(StorageError::IoError(
                "Invalid schema data: missing nullable flag".to_string(),
            ));
        }
        let nullable = data[pos] != 0;
        pos += 1;

        columns.push(ColumnSchema::new(column_name, data_type, nullable));
    }

    Ok(TableSchema::new(table_name, columns))
}

/// Parse a DataType from its debug string representation
fn parse_data_type(s: &str) -> Result<vibesql_types::DataType, StorageError> {
    use vibesql_types::DataType;

    // Handle common types
    let s = s.trim();

    if s == "Integer" {
        return Ok(DataType::Integer);
    }
    if s == "Bigint" {
        return Ok(DataType::Bigint);
    }
    if s == "Smallint" {
        return Ok(DataType::Smallint);
    }
    if s == "Unsigned" {
        return Ok(DataType::Unsigned);
    }
    if s == "Real" {
        return Ok(DataType::Real);
    }
    if s == "DoublePrecision" {
        return Ok(DataType::DoublePrecision);
    }
    if s == "Boolean" {
        return Ok(DataType::Boolean);
    }
    if s == "Date" {
        return Ok(DataType::Date);
    }
    if s == "Null" {
        return Ok(DataType::Null);
    }
    if s == "CharacterLargeObject" {
        return Ok(DataType::CharacterLargeObject);
    }
    if s == "BinaryLargeObject" {
        return Ok(DataType::BinaryLargeObject);
    }
    if s == "Name" {
        return Ok(DataType::Name);
    }

    // Handle Time { with_timezone: bool }
    if s.starts_with("Time") {
        let with_timezone = s.contains("with_timezone: true");
        return Ok(DataType::Time { with_timezone });
    }

    // Handle Timestamp { with_timezone: bool }
    if s.starts_with("Timestamp") {
        let with_timezone = s.contains("with_timezone: true");
        return Ok(DataType::Timestamp { with_timezone });
    }

    // Handle parameterized types
    if s.starts_with("Varchar") {
        // Varchar { max_length: Some(100) } or Varchar { max_length: None }
        if s.contains("None") {
            return Ok(DataType::Varchar { max_length: None });
        }
        if let Some(start) = s.find("Some(") {
            if let Some(end) = s[start..].find(')') {
                if let Ok(len) = s[start + 5..start + end].parse() {
                    return Ok(DataType::Varchar { max_length: Some(len) });
                }
            }
        }
        return Ok(DataType::Varchar { max_length: None });
    }

    // Handle Character { length: N }
    if s.starts_with("Character") {
        if let Some(start) = s.find("length:") {
            let len_str = &s[start + 7..];
            if let Some(len_end) = len_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(len) = len_str[..len_end].trim().parse() {
                    return Ok(DataType::Character { length: len });
                }
            } else if let Ok(len) = len_str.trim().trim_end_matches('}').trim().parse() {
                return Ok(DataType::Character { length: len });
            }
        }
        return Ok(DataType::Character { length: 1 });
    }

    // Handle Float { precision: N }
    if s.starts_with("Float") {
        if let Some(start) = s.find("precision:") {
            let prec_str = &s[start + 10..];
            if let Some(prec_end) = prec_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(p) = prec_str[..prec_end].trim().parse() {
                    return Ok(DataType::Float { precision: p });
                }
            } else if let Ok(p) = prec_str.trim().trim_end_matches('}').trim().parse() {
                return Ok(DataType::Float { precision: p });
            }
        }
        return Ok(DataType::Float { precision: 53 }); // Default double precision
    }

    if s.starts_with("Decimal") {
        // Decimal { precision: 10, scale: 2 }
        let mut precision = 38;
        let mut scale = 0;
        if let Some(prec_start) = s.find("precision:") {
            let prec_str = &s[prec_start + 10..];
            if let Some(prec_end) = prec_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(p) = prec_str[..prec_end].trim().parse() {
                    precision = p;
                }
            }
        }
        if let Some(scale_start) = s.find("scale:") {
            let scale_str = &s[scale_start + 6..];
            if let Some(scale_end) = scale_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(sc) = scale_str[..scale_end].trim().parse() {
                    scale = sc;
                }
            }
        }
        return Ok(DataType::Decimal { precision, scale });
    }

    if s.starts_with("Numeric") {
        let mut precision = 38;
        let mut scale = 0;
        if let Some(prec_start) = s.find("precision:") {
            let prec_str = &s[prec_start + 10..];
            if let Some(prec_end) = prec_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(p) = prec_str[..prec_end].trim().parse() {
                    precision = p;
                }
            }
        }
        if let Some(scale_start) = s.find("scale:") {
            let scale_str = &s[scale_start + 6..];
            if let Some(scale_end) = scale_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(sc) = scale_str[..scale_end].trim().parse() {
                    scale = sc;
                }
            }
        }
        return Ok(DataType::Numeric { precision, scale });
    }

    // Handle Vector { dimensions: N }
    if s.starts_with("Vector") {
        if let Some(start) = s.find("dimensions:") {
            let dim_str = &s[start + 11..];
            if let Some(dim_end) = dim_str.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                if let Ok(d) = dim_str[..dim_end].trim().parse() {
                    return Ok(DataType::Vector { dimensions: d });
                }
            } else if let Ok(d) = dim_str.trim().trim_end_matches('}').trim().parse() {
                return Ok(DataType::Vector { dimensions: d });
            }
        }
        return Ok(DataType::Vector { dimensions: 128 }); // Default dimensions
    }

    // Handle Bit { length: N }
    if s.starts_with("Bit") {
        if s.contains("None") {
            return Ok(DataType::Bit { length: None });
        }
        if let Some(start) = s.find("Some(") {
            if let Some(end) = s[start..].find(')') {
                if let Ok(len) = s[start + 5..start + end].parse() {
                    return Ok(DataType::Bit { length: Some(len) });
                }
            }
        }
        return Ok(DataType::Bit { length: Some(1) });
    }

    // Default to Varchar for unknown types
    log::warn!("Unknown data type '{}', defaulting to Varchar", s);
    Ok(DataType::Varchar { max_length: None })
}

/// Recover a database from checkpoint and WAL files
///
/// This is the main entry point for database recovery.
///
/// # Arguments
/// * `checkpoint_dir` - Directory containing checkpoint files
/// * `wal_path` - Path to the WAL file (optional)
///
/// # Returns
/// A tuple of (Database, RecoveryStats) on success
///
/// # Example
/// ```text
/// use vibesql_storage::wal::recovery::recover;
///
/// let (db, stats) = recover("/path/to/checkpoints", Some("/path/to/wal.log"))?;
/// println!("Recovered database with {} entries replayed", stats.entries_replayed);
/// ```
pub fn recover<P: AsRef<Path>>(
    checkpoint_dir: P,
    wal_path: Option<P>,
) -> Result<(Database, RecoveryStats), StorageError> {
    let mut manager = RecoveryManager::new(checkpoint_dir);
    if let Some(wal) = wal_path {
        manager = manager.with_wal(wal);
    }
    manager.recover()
}

/// Check if recovery is needed
///
/// Returns true if there are checkpoint files or a WAL file that could be used
/// to recover database state.
pub fn needs_recovery<P1: AsRef<Path>, P2: AsRef<Path>>(
    checkpoint_dir: P1,
    wal_path: Option<P2>,
) -> bool {
    let checkpoint_dir = checkpoint_dir.as_ref();
    let has_checkpoints = checkpoint_dir.exists()
        && fs::read_dir(checkpoint_dir)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .any(|e| e.path().extension().is_some_and(|ext| ext == "vchk"))
            })
            .unwrap_or(false);

    let has_wal = wal_path.is_some_and(|p| p.as_ref().exists());

    has_checkpoints || has_wal
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::wal::checkpoint::CheckpointWriter;

    #[test]
    fn test_recovery_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        fs::create_dir_all(&checkpoint_dir).unwrap();

        let manager = RecoveryManager::new(&checkpoint_dir);
        let (db, stats) = manager.recover().unwrap();

        assert_eq!(stats.checkpoint_lsn, 0);
        assert_eq!(stats.entries_replayed, 0);
        assert!(db.list_tables().is_empty());
    }

    #[test]
    fn test_recovery_config_defaults() {
        let config = RecoveryConfig::default();
        assert!(config.validate_checksums);
        assert!(config.stop_on_corruption);
        assert_eq!(config.max_checkpoint_retries, 3);
        assert_eq!(config.progress_interval, 10000);
    }

    #[test]
    fn test_transaction_tracker_basic() {
        let mut tracker = TransactionTracker::new();

        // Begin transaction
        tracker.begin_transaction(1);
        assert!(tracker.is_in_flight(1));

        // Buffer some operations
        tracker.buffer_op(
            1,
            10,
            WalOp::Insert {
                table_id: 1,
                table_name: "main.t".to_string(),
                row_id: 0,
                values: vec![SqlValue::Integer(42)],
            },
        );

        // Commit
        let ops = tracker.commit_transaction(1);
        assert_eq!(ops.len(), 1);
        assert!(!tracker.is_in_flight(1));
    }

    #[test]
    fn test_transaction_tracker_rollback() {
        let mut tracker = TransactionTracker::new();

        tracker.begin_transaction(1);
        tracker.buffer_op(
            1,
            10,
            WalOp::Insert {
                table_id: 1,
                table_name: "main.t".to_string(),
                row_id: 0,
                values: vec![SqlValue::Integer(42)],
            },
        );

        // Rollback discards operations
        tracker.rollback_transaction(1);
        assert!(!tracker.is_in_flight(1));
        assert_eq!(tracker.rolled_back_count(), 1);
    }

    #[test]
    fn test_transaction_tracker_in_flight_at_end() {
        let mut tracker = TransactionTracker::new();

        tracker.begin_transaction(1);
        tracker.begin_transaction(2);
        tracker.commit_transaction(1);
        // Transaction 2 left in-flight

        let in_flight = tracker.get_in_flight_transactions();
        assert_eq!(in_flight, vec![2]);
    }

    #[test]
    fn test_deserialize_table_schema() {
        // Create a simple schema
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    true,
                ),
            ],
        );

        // Serialize it (using the same format as core.rs)
        let mut data = Vec::new();
        data.extend_from_slice(schema.name.as_bytes());
        data.push(0);
        data.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());
        for col in &schema.columns {
            data.extend_from_slice(col.name.as_bytes());
            data.push(0);
            let type_str = format!("{:?}", col.data_type);
            data.extend_from_slice(type_str.as_bytes());
            data.push(0);
            data.push(if col.nullable { 1 } else { 0 });
        }

        // Deserialize and verify
        let result = deserialize_table_schema(&data).unwrap();
        assert_eq!(result.name, "test");
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert!(!result.columns[0].nullable);
        assert_eq!(result.columns[1].name, "name");
        assert!(result.columns[1].nullable);
    }

    #[test]
    fn test_parse_data_type() {
        assert!(matches!(parse_data_type("Integer").unwrap(), DataType::Integer));
        assert!(matches!(parse_data_type("Bigint").unwrap(), DataType::Bigint));
        assert!(matches!(parse_data_type("Boolean").unwrap(), DataType::Boolean));
        assert!(matches!(
            parse_data_type("CharacterLargeObject").unwrap(),
            DataType::CharacterLargeObject
        ));

        // Varchar with length
        match parse_data_type("Varchar { max_length: Some(100) }").unwrap() {
            DataType::Varchar { max_length: Some(100) } => {}
            other => panic!("Expected Varchar(100), got {:?}", other),
        }

        // Varchar without length
        match parse_data_type("Varchar { max_length: None }").unwrap() {
            DataType::Varchar { max_length: None } => {}
            other => panic!("Expected Varchar(None), got {:?}", other),
        }
    }

    #[test]
    fn test_needs_recovery_empty() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        assert!(!needs_recovery(checkpoint_dir, None::<PathBuf>));
    }

    #[test]
    fn test_needs_recovery_with_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        // Create a checkpoint
        let mut writer = CheckpointWriter::new(&checkpoint_dir).unwrap();
        writer.create_checkpoint(10, b"test data", 1).unwrap();

        assert!(needs_recovery(checkpoint_dir, None::<PathBuf>));
    }

    #[test]
    fn test_recovery_stats_default() {
        let stats = RecoveryStats::default();
        assert_eq!(stats.checkpoint_lsn, 0);
        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(stats.transactions_committed, 0);
        assert_eq!(stats.transactions_rolled_back, 0);
        assert!(!stats.corruption_detected);
    }

    // ------------------------------------------------------------------
    // Phase 2 (#5698): DML replay during recovery
    // ------------------------------------------------------------------
    //
    // These tests drive a real `Database` with the persistence engine
    // enabled so the WAL is populated with genuine ops (carrying the
    // qualified `table_name` introduced in WAL format v2), then recover
    // from the on-disk WAL and assert that row *data* — not just schema —
    // is restored.

    use crate::wal::{PersistenceConfig, PersistenceEngine};
    use crate::Database;

    /// Build a simple `id INTEGER, name VARCHAR` schema.
    fn simple_schema(name: &str) -> TableSchema {
        TableSchema::new(
            name.to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    true,
                ),
            ],
        )
    }

    /// Count live rows in a recovered table by qualified name.
    fn live_row_count(db: &Database, qualified: &str) -> usize {
        db.get_table(qualified).map(|t| t.scan_live().count()).unwrap_or(0)
    }

    /// Serialize a schema into the WAL `CreateTable.schema_data` layout that
    /// `deserialize_table_schema` (in this module) expects. Mirrors the
    /// production `serialize_table_schema` helper, kept local to avoid reaching
    /// into the private `database::table_api` module from a test.
    fn serialize_schema_for_test(schema: &TableSchema) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(schema.name.as_bytes());
        data.push(0);
        data.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());
        for col in &schema.columns {
            data.extend_from_slice(col.name.as_bytes());
            data.push(0);
            let type_str = format!("{:?}", col.data_type);
            data.extend_from_slice(type_str.as_bytes());
            data.push(0);
            data.push(if col.nullable { 1 } else { 0 });
        }
        data
    }

    #[test]
    fn test_recovery_replays_dml_inserts() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("test.wal");

        // 1. Write a table + rows through a persistence-enabled Database so the
        //    WAL captures real CreateTable + Insert ops.
        {
            let mut db = Database::new();
            let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
            db.enable_persistence(engine);

            db.create_table(simple_schema("people")).unwrap();
            db.insert_row(
                "main.people",
                crate::row::Row::new(vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar(arcstr::ArcStr::from("alice")),
                ]),
            )
            .unwrap();
            db.insert_row(
                "main.people",
                crate::row::Row::new(vec![
                    SqlValue::Integer(2),
                    SqlValue::Varchar(arcstr::ArcStr::from("bob")),
                ]),
            )
            .unwrap();

            // Flush everything to the WAL, then simulate a crash by dropping the
            // Database WITHOUT writing a checkpoint.
            db.sync_persistence().unwrap();
        }

        // 2. Recover from the WAL alone (no checkpoint).
        let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
        let (db, stats) = manager.recover().unwrap();

        assert_eq!(stats.tables_created, 1, "table schema should be replayed");
        assert_eq!(stats.inserts_applied, 2, "both inserts should be applied");
        assert_eq!(live_row_count(&db, "main.people"), 2, "both rows must survive recovery");

        let table = db.get_table("main.people").expect("table exists after recovery");
        let rows: Vec<_> = table.scan_live().map(|(_, r)| r.clone()).collect();
        assert_eq!(rows[0].values[0], SqlValue::Integer(1));
        assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    }

    #[test]
    fn test_recovery_replays_update_and_delete() {
        use crate::wal::entry::WalEntry;
        use crate::wal::writer::WalWriter;

        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("test.wal");

        // Build the schema-serialized bytes for a CreateTable op using the same
        // null-terminated layout `serialize_table_schema` / `deserialize_table_schema`
        // agree on, so recovery can rebuild the table.
        let schema = simple_schema("t");
        let schema_data = serialize_schema_for_test(&schema);

        // Hand-author a WAL: CreateTable, three Inserts, one Update (row 0), one
        // Delete (row 1) — all as standalone (auto-commit) ops carrying the
        // qualified table_name. This directly exercises apply_op's DML arms.
        {
            let file = std::fs::File::create(&wal_path).unwrap();
            let mut writer = WalWriter::create(file).unwrap();
            let mut lsn = 1u64;
            let mut append = |writer: &mut WalWriter<std::fs::File>, op: WalOp, lsn: &mut u64| {
                writer.append(&WalEntry::new(*lsn, 0, op)).unwrap();
                *lsn += 1;
            };

            append(
                &mut writer,
                WalOp::CreateTable { table_id: 0, table_name: "main.t".into(), schema_data },
                &mut lsn,
            );
            for i in 1..=3i64 {
                append(
                    &mut writer,
                    WalOp::Insert {
                        table_id: 0,
                        table_name: "main.t".into(),
                        row_id: (i - 1) as u64,
                        values: vec![
                            SqlValue::Integer(i),
                            SqlValue::Varchar(arcstr::ArcStr::from(format!("v{i}"))),
                        ],
                    },
                    &mut lsn,
                );
            }
            append(
                &mut writer,
                WalOp::Update {
                    table_id: 0,
                    table_name: "main.t".into(),
                    row_id: 0,
                    old_values: vec![
                        SqlValue::Integer(1),
                        SqlValue::Varchar(arcstr::ArcStr::from("v1")),
                    ],
                    new_values: vec![
                        SqlValue::Integer(1),
                        SqlValue::Varchar(arcstr::ArcStr::from("updated")),
                    ],
                },
                &mut lsn,
            );
            append(
                &mut writer,
                WalOp::Delete {
                    table_id: 0,
                    table_name: "main.t".into(),
                    row_id: 1,
                    old_values: vec![
                        SqlValue::Integer(2),
                        SqlValue::Varchar(arcstr::ArcStr::from("v2")),
                    ],
                },
                &mut lsn,
            );
            writer.flush().unwrap();
        }

        let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
        let (db, stats) = manager.recover().unwrap();

        assert_eq!(stats.inserts_applied, 3);
        assert_eq!(stats.updates_applied, 1);
        assert_eq!(stats.deletes_applied, 1);

        // 3 inserted, 1 deleted => 2 live rows.
        assert_eq!(live_row_count(&db, "main.t"), 2);

        let table = db.get_table("main.t").unwrap();
        // Row 0 was updated.
        assert_eq!(
            table.get_row(0).unwrap().values[1],
            SqlValue::Varchar(arcstr::ArcStr::from("updated"))
        );
        // Row 1 was deleted.
        assert!(table.is_row_deleted(1));
        // Row 2 untouched.
        assert_eq!(table.get_row(2).unwrap().values[0], SqlValue::Integer(3));
    }

    #[test]
    fn test_committed_dml_survives_cross_restart_via_resumed_lsn() {
        // Regression for #5766. This mirrors what the CLI's `WalState` does across
        // *separate process invocations* against a file-backed DB: each "process"
        // recovers, resumes the live WAL LSN counter past everything recovery saw
        // (`last_lsn + 1`), applies a committed mutation, writes a checkpoint at
        // `next_lsn - 1`, and exits.
        //
        // Before the fix the LSN counter reset to 1 every open, so a later
        // process's checkpoint could carry a *lower* LSN than an earlier one.
        // Recovery selects the highest-LSN checkpoint, so it resurrected the stale
        // pre-mutation state and silently dropped the newest committed write.
        // Resuming the LSN keeps checkpoint LSNs monotonic so the newest state wins.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("test.wal");

        // Mirror of `WalState::checkpoint`: stamp a checkpoint at next_lsn - 1.
        fn write_checkpoint(db: &Database, dir: &Path) {
            db.sync_persistence().unwrap();
            let lsn = db.persistence_next_lsn().unwrap_or(1).saturating_sub(1);
            let data = db.to_uncompressed_bytes().unwrap();
            let mut writer = CheckpointWriter::new(dir).unwrap();
            writer.create_checkpoint(lsn, &data, db.list_tables().len() as u32).unwrap();
        }

        // Mirror of `WalState::open`: recover, then resume the LSN at last_lsn + 1.
        fn open(dir: &Path, wal: &Path) -> Database {
            let manager = RecoveryManager::new(dir).with_wal(wal);
            let (mut db, stats) = manager.recover().unwrap();
            let resume = stats.last_lsn.saturating_add(1);
            let engine =
                PersistenceEngine::open_with_start_lsn(wal, PersistenceConfig::default(), resume)
                    .unwrap();
            db.enable_persistence(engine);
            db
        }

        let row = |id: i64, name: &str| {
            crate::row::Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(arcstr::ArcStr::from(name)),
            ])
        };

        // --- Cycle 1: create table + insert 3 committed rows, checkpoint, exit.
        {
            let mut db = open(&checkpoint_dir, &wal_path);
            db.create_table(simple_schema("t")).unwrap();
            db.insert_row("main.t", row(1, "a")).unwrap();
            db.insert_row("main.t", row(2, "b")).unwrap();
            db.insert_row("main.t", row(3, "c")).unwrap();
            write_checkpoint(&db, &checkpoint_dir);
        }

        // --- Cycle 2: reopen, delete row 0 (a committed mutation that *advances*
        // the LSN), checkpoint, exit. This is the post-delete state that must win.
        {
            let mut db = open(&checkpoint_dir, &wal_path);
            assert_eq!(live_row_count(&db, "main.t"), 3, "cycle 2 must see all 3 rows");
            // Emit a WAL delete (advances the engine LSN, exactly like the DELETE
            // executor) and mark the row deleted so the checkpoint captures it.
            db.emit_wal_delete("main.t", 0, vec![SqlValue::Integer(1)]);
            db.get_table_mut("main.t").unwrap().mark_deleted_inplace(0);
            write_checkpoint(&db, &checkpoint_dir);
        }

        // --- Cycle 3: reopen and assert the delete persisted (2 live rows), i.e.
        // recovery picked the post-delete checkpoint, not the stale 3-row one.
        {
            let db = open(&checkpoint_dir, &wal_path);
            assert_eq!(
                live_row_count(&db, "main.t"),
                2,
                "committed DELETE must survive the restart; recovery must not resurrect \
                 the pre-delete checkpoint (#5766)"
            );
        }

        // The checkpoint LSNs must be strictly monotonic across the two cycles so
        // that highest-LSN selection lands on the newest state.
        let writer = CheckpointWriter::new(&checkpoint_dir).unwrap();
        let checkpoints = writer.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2, "one checkpoint per write cycle");
        assert!(
            checkpoints[1].lsn > checkpoints[0].lsn,
            "later checkpoint must carry a higher LSN (got {} then {})",
            checkpoints[0].lsn,
            checkpoints[1].lsn
        );
    }

    #[test]
    fn test_recovery_discards_uncommitted_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("test.wal");

        {
            let mut db = Database::new();
            let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
            db.enable_persistence(engine);

            db.create_table(simple_schema("t")).unwrap();
            // One committed (auto-commit) insert.
            db.insert_row(
                "main.t",
                crate::row::Row::new(vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar(arcstr::ArcStr::from("committed")),
                ]),
            )
            .unwrap();

            // Begin an explicit transaction, insert, then crash WITHOUT
            // committing. The TxnBegin op is emitted to the WAL; the buffered
            // insert must NOT be applied on recovery.
            db.begin_transaction().unwrap();
            db.insert_row(
                "main.t",
                crate::row::Row::new(vec![
                    SqlValue::Integer(2),
                    SqlValue::Varchar(arcstr::ArcStr::from("uncommitted")),
                ]),
            )
            .unwrap();

            db.sync_persistence().unwrap();
            // Drop without commit_transaction() => simulated crash mid-txn.
        }

        let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
        let (db, _stats) = manager.recover().unwrap();

        // Only the committed row should survive.
        assert_eq!(live_row_count(&db, "main.t"), 1, "uncommitted row must be discarded");
        let table = db.get_table("main.t").unwrap();
        let rows: Vec<_> = table.scan_live().map(|(_, r)| r.clone()).collect();
        assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    }

    #[test]
    fn test_recovery_tolerates_truncated_wal_tail() {
        // A WAL whose final entry was only partially written (e.g. the process
        // died mid-flush) must recover every complete entry before the
        // truncation point and stop cleanly at the corruption — without losing
        // the earlier committed rows.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("test.wal");

        // 1. Write a valid WAL: CreateTable + two Inserts.
        {
            let mut db = Database::new();
            let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
            db.enable_persistence(engine);

            db.create_table(simple_schema("t")).unwrap();
            db.insert_row(
                "main.t",
                crate::row::Row::new(vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar(arcstr::ArcStr::from("one")),
                ]),
            )
            .unwrap();
            db.insert_row(
                "main.t",
                crate::row::Row::new(vec![
                    SqlValue::Integer(2),
                    SqlValue::Varchar(arcstr::ArcStr::from("two")),
                ]),
            )
            .unwrap();
            db.sync_persistence().unwrap();
        }

        // 2. Simulate a torn final write by chopping the last byte off the WAL.
        let original_len = std::fs::metadata(&wal_path).unwrap().len();
        assert!(original_len > 1);
        let file = std::fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.set_len(original_len - 1).unwrap();
        drop(file);

        // 3. Recovery must tolerate the truncated tail: it detects corruption,
        //    stops, and keeps the rows from the complete entries. The last
        //    (now-torn) Insert is dropped, leaving exactly one row.
        let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
        let (db, stats) = manager.recover().unwrap();

        assert!(stats.corruption_detected, "truncated tail should be flagged as corruption");
        assert_eq!(
            live_row_count(&db, "main.t"),
            1,
            "rows before the torn tail must survive; the torn entry is dropped"
        );
        let table = db.get_table("main.t").unwrap();
        let rows: Vec<_> = table.scan_live().map(|(_, r)| r.clone()).collect();
        assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    }

    #[test]
    fn test_recovery_v1_dml_skipped_gracefully() {
        // A WAL Insert serialized under format v1 (no inline table_name) must be
        // parsed without error and skipped during replay rather than corrupting
        // recovery. We hand-build a v1 op and deserialize it with version 1.
        use crate::wal::entry::WalEntry;

        // Serialize a current (v2) Insert, then re-read it as v1: the v1 reader
        // stops before the table_name field, so we instead construct the raw v1
        // byte layout directly: tag + table_id + row_id + values.
        let mut buf = Vec::new();
        // tag = Insert (0x01)
        buf.push(0x01);
        // table_id: u32 = 7
        buf.extend_from_slice(&7u32.to_le_bytes());
        // row_id: u64 = 0
        buf.extend_from_slice(&0u64.to_le_bytes());
        // values: len=1, then one Integer
        buf.extend_from_slice(&1u32.to_le_bytes());
        // Encode SqlValue::Integer(99) via the same value writer path.
        let mut value_buf = Vec::new();
        crate::persistence::binary::value::write_sql_value(&mut value_buf, &SqlValue::Integer(99))
            .unwrap();
        buf.extend_from_slice(&value_buf);

        // Prepend lsn + timestamp for a full WalEntry (v1 entry layout is
        // identical except the op body).
        let mut entry_buf = Vec::new();
        entry_buf.extend_from_slice(&1u64.to_le_bytes()); // lsn
        entry_buf.extend_from_slice(&0u64.to_le_bytes()); // timestamp_ms
        entry_buf.extend_from_slice(&buf);

        let mut reader = &entry_buf[..];
        let entry = WalEntry::deserialize_versioned(&mut reader, 1).unwrap();
        match entry.op {
            WalOp::Insert { table_id, table_name, row_id, values } => {
                assert_eq!(table_id, 7);
                assert!(table_name.is_empty(), "v1 op has no inline table_name");
                assert_eq!(row_id, 0);
                assert_eq!(values, vec![SqlValue::Integer(99)]);
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }
}
