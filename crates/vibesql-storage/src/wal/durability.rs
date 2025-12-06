// ============================================================================
// Durability Modes
// ============================================================================
//
// This module provides configurable durability modes for the WAL system.
// Different modes offer trade-offs between performance and data safety.
//
// ## Mode Behaviors
//
// | Mode     | WAL Write | WAL Sync | Checkpoint | Use Case               |
// |----------|-----------|----------|------------|------------------------|
// | Volatile | Never     | Never    | Never      | Testing, ephemeral     |
// | Lazy     | Batched   | Periodic | Periodic   | Default, good balance  |
// | Durable  | On commit | On commit| Periodic   | Important transactions |
// | Paranoid | Every op  | Every op | Frequent   | Critical data          |
//
// ## Performance Characteristics
//
// | Mode     | Single INSERT | Bulk 1M rows | Data Loss Window |
// |----------|---------------|--------------|------------------|
// | Volatile | ~1μs          | ~500ms       | All              |
// | Lazy     | ~1μs          | ~600ms       | ~50-100ms        |
// | Durable  | ~100μs        | ~1s          | None (committed) |
// | Paranoid | ~200μs        | ~2s          | None             |

use std::time::Duration;

use super::scheduler::CheckpointConfig;

/// Durability mode for the database
///
/// Controls how aggressively changes are persisted to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DurabilityMode {
    /// No persistence - changes are kept in memory only.
    ///
    /// Use cases:
    /// - Testing and development
    /// - Ephemeral/temporary databases
    /// - WASM environments without storage
    ///
    /// Characteristics:
    /// - Maximum performance
    /// - All data lost on shutdown/crash
    /// - WAL is never written
    Volatile,

    /// Batched WAL writes with periodic sync (default).
    ///
    /// Use cases:
    /// - Most production workloads
    /// - Good balance of speed and safety
    ///
    /// Characteristics:
    /// - WAL entries batched for efficiency
    /// - Sync every 50-100ms or N entries
    /// - Up to ~100ms of data could be lost on crash
    #[default]
    Lazy,

    /// WAL sync on transaction commit.
    ///
    /// Use cases:
    /// - Financial transactions
    /// - Important user data
    /// - When committed = durable guarantee needed
    ///
    /// Characteristics:
    /// - WAL written immediately on commit
    /// - fsync after each commit
    /// - Committed transactions are safe
    /// - Slower than Lazy mode
    Durable,

    /// WAL sync on every write operation.
    ///
    /// Use cases:
    /// - Critical systems where any data loss is unacceptable
    /// - Audit logs
    /// - Compliance requirements
    ///
    /// Characteristics:
    /// - Every operation synced to disk
    /// - Highest durability guarantee
    /// - Significantly slower than other modes
    Paranoid,
}

impl DurabilityMode {
    /// Returns true if this mode writes to WAL
    pub fn writes_wal(&self) -> bool {
        !matches!(self, DurabilityMode::Volatile)
    }

    /// Returns true if this mode syncs on commit
    pub fn sync_on_commit(&self) -> bool {
        matches!(self, DurabilityMode::Durable | DurabilityMode::Paranoid)
    }

    /// Returns true if this mode syncs on every operation
    pub fn sync_on_every_op(&self) -> bool {
        matches!(self, DurabilityMode::Paranoid)
    }

    /// Returns true if this mode uses batched writes
    pub fn uses_batching(&self) -> bool {
        matches!(self, DurabilityMode::Lazy)
    }

    /// Returns true if checkpointing is enabled for this mode
    pub fn enables_checkpointing(&self) -> bool {
        !matches!(self, DurabilityMode::Volatile)
    }

    /// Get recommended flush interval for this mode
    pub fn recommended_flush_interval_ms(&self) -> u64 {
        match self {
            DurabilityMode::Volatile => 0,
            DurabilityMode::Lazy => 50,
            DurabilityMode::Durable => 0, // Sync immediately on commit
            DurabilityMode::Paranoid => 0, // Sync on every op
        }
    }

    /// Get recommended flush count for this mode
    pub fn recommended_flush_count(&self) -> usize {
        match self {
            DurabilityMode::Volatile => usize::MAX,
            DurabilityMode::Lazy => 1000,
            DurabilityMode::Durable => 1, // Flush every entry
            DurabilityMode::Paranoid => 1, // Flush every entry
        }
    }

    /// Get recommended checkpoint interval for this mode
    pub fn recommended_checkpoint_interval(&self) -> Duration {
        match self {
            DurabilityMode::Volatile => Duration::from_secs(u64::MAX),
            DurabilityMode::Lazy => Duration::from_secs(30),
            DurabilityMode::Durable => Duration::from_secs(30),
            DurabilityMode::Paranoid => Duration::from_secs(10),
        }
    }

    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "volatile" => Some(DurabilityMode::Volatile),
            "lazy" => Some(DurabilityMode::Lazy),
            "durable" => Some(DurabilityMode::Durable),
            "paranoid" => Some(DurabilityMode::Paranoid),
            _ => None,
        }
    }

    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            DurabilityMode::Volatile => "volatile",
            DurabilityMode::Lazy => "lazy",
            DurabilityMode::Durable => "durable",
            DurabilityMode::Paranoid => "paranoid",
        }
    }
}

impl std::fmt::Display for DurabilityMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Per-transaction durability hint
///
/// Allows individual transactions to override the database's default
/// durability mode for their specific operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionDurability {
    /// Use the database's default durability mode
    #[default]
    Default,

    /// Force durable commit regardless of database mode
    ///
    /// Useful for critical operations in an otherwise Lazy database.
    /// Example: `BEGIN TRANSACTION WITH DURABILITY = DURABLE`
    ForceDurable,

    /// Allow lazy commit even in a Durable database
    ///
    /// Useful for bulk imports where some data loss is acceptable.
    /// Example: `BEGIN TRANSACTION WITH DURABILITY = LAZY`
    AllowLazy,

    /// Force volatile (no WAL) for this transaction
    ///
    /// Use for temporary operations that don't need persistence.
    /// Example: `BEGIN TRANSACTION WITH DURABILITY = VOLATILE`
    ForceVolatile,
}

impl TransactionDurability {
    /// Resolve the effective durability mode given database and transaction hints
    pub fn resolve(&self, database_mode: DurabilityMode) -> DurabilityMode {
        match self {
            TransactionDurability::Default => database_mode,
            TransactionDurability::ForceDurable => DurabilityMode::Durable,
            TransactionDurability::AllowLazy => {
                // Only downgrade to Lazy if database is Durable/Paranoid
                match database_mode {
                    DurabilityMode::Volatile => DurabilityMode::Volatile,
                    DurabilityMode::Lazy => DurabilityMode::Lazy,
                    DurabilityMode::Durable | DurabilityMode::Paranoid => DurabilityMode::Lazy,
                }
            }
            TransactionDurability::ForceVolatile => DurabilityMode::Volatile,
        }
    }

    /// Parse from SQL hint string
    pub fn from_sql_hint(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "DEFAULT" => Some(TransactionDurability::Default),
            "DURABLE" => Some(TransactionDurability::ForceDurable),
            "LAZY" => Some(TransactionDurability::AllowLazy),
            "VOLATILE" => Some(TransactionDurability::ForceVolatile),
            _ => None,
        }
    }
}

/// Complete persistence configuration including durability mode
#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    /// The durability mode
    pub mode: DurabilityMode,

    /// WAL flush interval in milliseconds (for Lazy mode)
    pub wal_flush_interval_ms: u64,

    /// WAL flush batch size (for Lazy mode)
    pub wal_flush_batch_size: usize,

    /// Checkpoint configuration
    pub checkpoint: CheckpointConfig,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self::lazy()
    }
}

impl DurabilityConfig {
    /// Create a volatile (no persistence) configuration
    pub fn volatile() -> Self {
        Self {
            mode: DurabilityMode::Volatile,
            wal_flush_interval_ms: 0,
            wal_flush_batch_size: usize::MAX,
            checkpoint: CheckpointConfig {
                auto_checkpoint: false,
                ..Default::default()
            },
        }
    }

    /// Create a lazy (batched) configuration - the default
    pub fn lazy() -> Self {
        Self {
            mode: DurabilityMode::Lazy,
            wal_flush_interval_ms: 50,
            wal_flush_batch_size: 1000,
            checkpoint: CheckpointConfig::default(),
        }
    }

    /// Create a durable (sync on commit) configuration
    pub fn durable() -> Self {
        Self {
            mode: DurabilityMode::Durable,
            wal_flush_interval_ms: 0,
            wal_flush_batch_size: 1,
            checkpoint: CheckpointConfig::default(),
        }
    }

    /// Create a paranoid (sync on every op) configuration
    pub fn paranoid() -> Self {
        Self {
            mode: DurabilityMode::Paranoid,
            wal_flush_interval_ms: 0,
            wal_flush_batch_size: 1,
            checkpoint: CheckpointConfig {
                interval: Duration::from_secs(10),
                ..Default::default()
            },
        }
    }

    /// Create configuration from a mode with recommended settings
    pub fn from_mode(mode: DurabilityMode) -> Self {
        match mode {
            DurabilityMode::Volatile => Self::volatile(),
            DurabilityMode::Lazy => Self::lazy(),
            DurabilityMode::Durable => Self::durable(),
            DurabilityMode::Paranoid => Self::paranoid(),
        }
    }

    /// Builder: set custom WAL flush interval
    pub fn with_flush_interval_ms(mut self, ms: u64) -> Self {
        self.wal_flush_interval_ms = ms;
        self
    }

    /// Builder: set custom WAL flush batch size
    pub fn with_flush_batch_size(mut self, size: usize) -> Self {
        self.wal_flush_batch_size = size;
        self
    }

    /// Builder: set custom checkpoint interval
    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint.interval = interval;
        self
    }

    /// Builder: set custom WAL size threshold for checkpoint
    pub fn with_checkpoint_wal_size_threshold(mut self, bytes: u64) -> Self {
        self.checkpoint.wal_size_threshold = bytes;
        self
    }

    /// Builder: disable auto checkpointing
    pub fn with_manual_checkpoint_only(mut self) -> Self {
        self.checkpoint.auto_checkpoint = false;
        self
    }

    /// Recommended configuration for browser/WASM environments
    ///
    /// WASM cannot spawn threads, so volatile mode is typically best.
    /// If OPFS persistence is available, lazy mode with manual sync can be used.
    pub fn browser_default() -> Self {
        Self::volatile()
    }

    /// Recommended configuration for development/testing
    pub fn development() -> Self {
        Self::volatile()
    }

    /// Recommended configuration for production servers
    pub fn production() -> Self {
        Self::lazy()
    }

    /// Recommended configuration for critical data
    pub fn critical() -> Self {
        Self::durable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_durability_mode_defaults() {
        assert_eq!(DurabilityMode::default(), DurabilityMode::Lazy);
    }

    #[test]
    fn test_durability_mode_properties() {
        // Volatile
        assert!(!DurabilityMode::Volatile.writes_wal());
        assert!(!DurabilityMode::Volatile.sync_on_commit());
        assert!(!DurabilityMode::Volatile.enables_checkpointing());

        // Lazy
        assert!(DurabilityMode::Lazy.writes_wal());
        assert!(!DurabilityMode::Lazy.sync_on_commit());
        assert!(DurabilityMode::Lazy.uses_batching());
        assert!(DurabilityMode::Lazy.enables_checkpointing());

        // Durable
        assert!(DurabilityMode::Durable.writes_wal());
        assert!(DurabilityMode::Durable.sync_on_commit());
        assert!(!DurabilityMode::Durable.sync_on_every_op());
        assert!(DurabilityMode::Durable.enables_checkpointing());

        // Paranoid
        assert!(DurabilityMode::Paranoid.writes_wal());
        assert!(DurabilityMode::Paranoid.sync_on_commit());
        assert!(DurabilityMode::Paranoid.sync_on_every_op());
        assert!(DurabilityMode::Paranoid.enables_checkpointing());
    }

    #[test]
    fn test_durability_mode_parsing() {
        assert_eq!(DurabilityMode::parse("volatile"), Some(DurabilityMode::Volatile));
        assert_eq!(DurabilityMode::parse("LAZY"), Some(DurabilityMode::Lazy));
        assert_eq!(DurabilityMode::parse("Durable"), Some(DurabilityMode::Durable));
        assert_eq!(DurabilityMode::parse("PARANOID"), Some(DurabilityMode::Paranoid));
        assert_eq!(DurabilityMode::parse("invalid"), None);
    }

    #[test]
    fn test_transaction_durability_resolve() {
        // Default follows database mode
        assert_eq!(
            TransactionDurability::Default.resolve(DurabilityMode::Lazy),
            DurabilityMode::Lazy
        );
        assert_eq!(
            TransactionDurability::Default.resolve(DurabilityMode::Durable),
            DurabilityMode::Durable
        );

        // ForceDurable always returns Durable
        assert_eq!(
            TransactionDurability::ForceDurable.resolve(DurabilityMode::Volatile),
            DurabilityMode::Durable
        );
        assert_eq!(
            TransactionDurability::ForceDurable.resolve(DurabilityMode::Lazy),
            DurabilityMode::Durable
        );

        // AllowLazy downgrades Durable/Paranoid to Lazy
        assert_eq!(
            TransactionDurability::AllowLazy.resolve(DurabilityMode::Durable),
            DurabilityMode::Lazy
        );
        assert_eq!(
            TransactionDurability::AllowLazy.resolve(DurabilityMode::Paranoid),
            DurabilityMode::Lazy
        );
        assert_eq!(
            TransactionDurability::AllowLazy.resolve(DurabilityMode::Volatile),
            DurabilityMode::Volatile
        );

        // ForceVolatile always returns Volatile
        assert_eq!(
            TransactionDurability::ForceVolatile.resolve(DurabilityMode::Paranoid),
            DurabilityMode::Volatile
        );
    }

    #[test]
    fn test_durability_config_presets() {
        let volatile = DurabilityConfig::volatile();
        assert_eq!(volatile.mode, DurabilityMode::Volatile);
        assert!(!volatile.checkpoint.auto_checkpoint);

        let lazy = DurabilityConfig::lazy();
        assert_eq!(lazy.mode, DurabilityMode::Lazy);
        assert_eq!(lazy.wal_flush_interval_ms, 50);

        let durable = DurabilityConfig::durable();
        assert_eq!(durable.mode, DurabilityMode::Durable);
        assert_eq!(durable.wal_flush_batch_size, 1);

        let paranoid = DurabilityConfig::paranoid();
        assert_eq!(paranoid.mode, DurabilityMode::Paranoid);
        assert_eq!(paranoid.checkpoint.interval, Duration::from_secs(10));
    }

    #[test]
    fn test_durability_config_builder() {
        let config = DurabilityConfig::lazy()
            .with_flush_interval_ms(100)
            .with_flush_batch_size(500)
            .with_checkpoint_interval(Duration::from_secs(60));

        assert_eq!(config.wal_flush_interval_ms, 100);
        assert_eq!(config.wal_flush_batch_size, 500);
        assert_eq!(config.checkpoint.interval, Duration::from_secs(60));
    }
}
