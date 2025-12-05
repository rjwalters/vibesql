// ============================================================================
// Database Configuration
// ============================================================================

#![allow(clippy::identity_op)]

/// Default columnar cache budget (256MB)
pub const DEFAULT_COLUMNAR_CACHE_BUDGET: usize = 256 * 1024 * 1024;

/// Configuration for database resource budgets
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Maximum memory for indexes and buffer pools (bytes)
    pub memory_budget: usize,

    /// Maximum disk space for database files (bytes)
    pub disk_budget: usize,

    /// Policy for handling memory budget violations
    pub spill_policy: SpillPolicy,

    /// SQL dialect compatibility mode (MySQL, SQLite, etc.)
    pub sql_mode: vibesql_types::SqlMode,

    /// Maximum memory for columnar cache (bytes)
    /// Set to 0 to disable the columnar cache
    pub columnar_cache_budget: usize,
}

/// Policy for what to do when memory budget is exceeded
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPolicy {
    /// Reject CREATE INDEX if it would exceed budget
    Reject,

    /// Automatically spill cold indexes from memory to disk
    SpillToDisk,

    /// Best effort - try to allocate, graceful degradation
    BestEffort,
}

impl DatabaseConfig {
    /// Default configuration for browser/WASM environments
    /// - 512MB memory budget (conservative for browsers)
    /// - 2GB disk budget (typical OPFS quota)
    /// - SpillToDisk policy (automatic eviction)
    /// - MySQL mode (default)
    /// - 64MB columnar cache (modest for browser memory constraints)
    pub fn browser_default() -> Self {
        DatabaseConfig {
            memory_budget: 512 * 1024 * 1024,    // 512MB
            disk_budget: 2 * 1024 * 1024 * 1024, // 2GB
            spill_policy: SpillPolicy::SpillToDisk,
            sql_mode: vibesql_types::SqlMode::default(),
            columnar_cache_budget: 64 * 1024 * 1024, // 64MB
        }
    }

    /// Default configuration for server environments
    /// - 16GB memory budget (abundant server RAM)
    /// - 1TB disk budget (generous server storage)
    /// - BestEffort policy (prefer memory, fall back to disk)
    /// - MySQL mode (default)
    /// - 256MB columnar cache (can cache all TPC-H tables at SF 1.0)
    pub fn server_default() -> Self {
        DatabaseConfig {
            memory_budget: (16u64 * 1024 * 1024 * 1024) as usize, // 16GB
            disk_budget: (1024u64 * 1024 * 1024 * 1024) as usize, // 1TB
            spill_policy: SpillPolicy::BestEffort,
            sql_mode: vibesql_types::SqlMode::default(),
            columnar_cache_budget: 256 * 1024 * 1024, // 256MB
        }
    }

    /// Minimal configuration for testing
    /// - 10MB memory budget (force eviction quickly)
    /// - 100MB disk budget
    /// - SpillToDisk policy
    /// - MySQL mode (default)
    /// - 1MB columnar cache (tiny for testing eviction)
    pub fn test_default() -> Self {
        DatabaseConfig {
            memory_budget: 10 * 1024 * 1024, // 10MB
            disk_budget: 100 * 1024 * 1024,  // 100MB
            spill_policy: SpillPolicy::SpillToDisk,
            sql_mode: vibesql_types::SqlMode::default(),
            columnar_cache_budget: 1 * 1024 * 1024, // 1MB
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        // Default to server configuration (most permissive)
        Self::server_default()
    }
}
