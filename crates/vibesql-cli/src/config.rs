use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::formatter::OutputFormat;

/// VibeSQL configuration loaded from ~/.vibesqlrc
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub display: DisplayConfig,

    #[serde(default)]
    pub database: DatabaseConfig,

    #[serde(default)]
    pub history: HistoryConfig,

    #[serde(default)]
    pub query: QueryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayConfig {
    /// Default output format: table, json, csv
    #[serde(default = "default_format")]
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Default database path to load on startup
    #[serde(default)]
    pub default_path: Option<String>,

    /// Whether to auto-save database on exit
    #[serde(default = "default_true")]
    pub auto_save: bool,

    /// SQL compatibility mode (mysql, sqlite)
    #[serde(default = "default_sql_mode")]
    pub sql_mode: String,

    /// Write-Ahead Log (WAL) durability for file-backed databases.
    ///
    /// Defaults to `true`. When a database file path is given, the CLI
    /// initializes the WAL persistence engine on open (replaying the last
    /// checkpoint + WAL via `RecoveryManager::recover`) and replaces
    /// snapshot-on-save with a checkpoint + WAL truncate.
    ///
    /// Both DDL (table schemas) and committed DML (row data) survive an unclean
    /// shutdown: `RecoveryManager::apply_op` replays Insert/Update/Delete from
    /// the WAL, routed by the inline `table_name` carried in WAL format v2 ops.
    /// Uncommitted transactions at crash time are discarded.
    ///
    /// Opt out with `[database] wal = false` to fall back to the snapshot-only
    /// path (in-memory + SQL-dump snapshot on save/exit, no WAL sibling files).
    #[serde(default = "default_true")]
    pub wal: bool,

    /// Busy timeout (milliseconds) for the exclusive inter-process database
    /// lock (issue #5808). While another CLI session holds the same
    /// file-backed database open, a new session retries acquiring the
    /// `<stem>.lock` sibling for up to this long before failing with the
    /// SQLite-compatible error `database is locked`. `0` = fail immediately.
    #[serde(default = "default_lock_timeout_ms")]
    pub lock_timeout_ms: u64,

    /// Number of checkpoint files to retain in `<stem>-checkpoints/` after a
    /// successful `\save` / clean-exit checkpoint (issue #6023).
    ///
    /// Each checkpoint on save writes a new `checkpoint_*.vchk`; without
    /// pruning the archive grows unboundedly (thousands of files over a long
    /// session). After the new checkpoint is durably written and the WAL is
    /// truncated, the CLI removes the oldest checkpoints so at most
    /// `keep_checkpoints` (plus the one just written) remain.
    ///
    /// Defaults to `2` (matching
    /// `vibesql_storage::wal::scheduler::DEFAULT_KEEP_CHECKPOINTS`). Keeping
    /// more than 1 lets `--recover-fallback` fall back to an older checkpoint
    /// when the newest is unreadable. A configured value of `0` is clamped to
    /// `1` so the newest checkpoint always survives.
    #[serde(default = "default_keep_checkpoints")]
    pub keep_checkpoints: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryConfig {
    /// History file path
    #[serde(default = "default_history_file")]
    pub file: String,

    /// Maximum number of history entries
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryConfig {
    /// Query timeout in seconds (0 = no timeout)
    #[serde(default)]
    pub timeout_seconds: u64,
}

// Default value functions
fn default_format() -> String {
    "table".to_string()
}

fn default_true() -> bool {
    true
}

fn default_history_file() -> String {
    if let Some(home) = dirs::home_dir() {
        home.join(".vibesql_history").to_string_lossy().to_string()
    } else {
        ".vibesql_history".to_string()
    }
}

fn default_max_entries() -> usize {
    10000
}

fn default_sql_mode() -> String {
    "mysql".to_string()
}

fn default_lock_timeout_ms() -> u64 {
    5000
}

fn default_keep_checkpoints() -> usize {
    // Matches vibesql_storage::wal::scheduler::DEFAULT_KEEP_CHECKPOINTS.
    2
}

impl Default for DisplayConfig {
    fn default() -> Self {
        DisplayConfig { format: default_format() }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            default_path: None,
            auto_save: default_true(),
            sql_mode: default_sql_mode(),
            wal: default_true(),
            lock_timeout_ms: default_lock_timeout_ms(),
            keep_checkpoints: default_keep_checkpoints(),
        }
    }
}

impl Default for HistoryConfig {
    fn default() -> Self {
        HistoryConfig { file: default_history_file(), max_entries: default_max_entries() }
    }
}

impl Config {
    /// Load configuration from ~/.vibesqlrc
    pub fn load() -> anyhow::Result<Self> {
        let config_path = Self::config_path()?;

        if !config_path.exists() {
            // No config file, use defaults
            return Ok(Config::default());
        }

        let content = fs::read_to_string(&config_path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file: {}", e))?;

        let config: Config = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file: {}", e))?;

        Ok(config)
    }

    /// Get the configuration file path (~/.vibesqlrc)
    pub fn config_path() -> anyhow::Result<PathBuf> {
        let home = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?;
        Ok(home.join(".vibesqlrc"))
    }

    /// Get the output format as OutputFormat enum
    pub fn get_output_format(&self) -> Option<OutputFormat> {
        match self.display.format.as_str() {
            "table" => Some(OutputFormat::Table),
            "json" => Some(OutputFormat::Json),
            "csv" => Some(OutputFormat::Csv),
            _ => None,
        }
    }

    /// Get the SQL mode as SqlMode enum
    #[allow(dead_code)]
    pub fn get_sql_mode(&self) -> Option<vibesql_types::SqlMode> {
        match self.database.sql_mode.to_lowercase().as_str() {
            "mysql" => Some(vibesql_types::SqlMode::MySQL {
                flags: vibesql_types::MySqlModeFlags::default(),
            }),
            "sqlite" => Some(vibesql_types::SqlMode::SQLite),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.display.format, "table");
        assert!(config.database.auto_save);
        assert_eq!(config.database.sql_mode, "mysql");
        assert_eq!(config.history.max_entries, 10000);
        assert_eq!(config.query.timeout_seconds, 0);
        // WAL is on by default (Phase 2, #5698): committed rows survive crashes.
        assert!(config.database.wal);
    }

    #[test]
    fn test_wal_defaults_on_when_unspecified() {
        // A config that omits `wal` must parse with wal = true (the new default).
        let toml_str = r#"
[database]
auto_save = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.database.wal);
    }

    #[test]
    fn test_lock_timeout_defaults_when_unspecified() {
        // A config that omits `lock_timeout_ms` must parse with the 5000 ms
        // default (issue #5808).
        let toml_str = r#"
[database]
wal = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.database.lock_timeout_ms, 5000);
        assert_eq!(Config::default().database.lock_timeout_ms, 5000);
    }

    #[test]
    fn test_lock_timeout_override_parses() {
        let toml_str = r#"
[database]
lock_timeout_ms = 250
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.database.lock_timeout_ms, 250);
    }

    #[test]
    fn test_keep_checkpoints_defaults_when_unspecified() {
        // A config that omits `keep_checkpoints` must parse with the default of
        // 2 (issue #6023), matching DEFAULT_KEEP_CHECKPOINTS in vibesql-storage.
        let toml_str = r#"
[database]
wal = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.database.keep_checkpoints, 2);
        assert_eq!(Config::default().database.keep_checkpoints, 2);
    }

    #[test]
    fn test_keep_checkpoints_override_parses() {
        let toml_str = r#"
[database]
keep_checkpoints = 5
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.database.keep_checkpoints, 5);
    }

    #[test]
    fn test_wal_opt_out_parses() {
        // Users can still disable WAL explicitly to get the snapshot-only path.
        let toml_str = r#"
[database]
wal = false
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(!config.database.wal);
    }

    #[test]
    fn test_parse_toml_config() {
        let toml_str = r#"
[display]
format = "json"

[database]
default_path = "~/databases/main.sql"
auto_save = false

[history]
file = "~/.my_history"
max_entries = 5000

[query]
timeout_seconds = 30
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.display.format, "json");
        assert_eq!(config.database.default_path, Some("~/databases/main.sql".to_string()));
        assert!(!config.database.auto_save);
        assert_eq!(config.history.file, "~/.my_history");
        assert_eq!(config.history.max_entries, 5000);
        assert_eq!(config.query.timeout_seconds, 30);
    }

    #[test]
    fn test_get_output_format() {
        let mut config = Config::default();

        config.display.format = "table".to_string();
        assert!(matches!(config.get_output_format(), Some(OutputFormat::Table)));

        config.display.format = "json".to_string();
        assert!(matches!(config.get_output_format(), Some(OutputFormat::Json)));

        config.display.format = "csv".to_string();
        assert!(matches!(config.get_output_format(), Some(OutputFormat::Csv)));

        config.display.format = "invalid".to_string();
        assert!(config.get_output_format().is_none());
    }

    #[test]
    fn test_get_sql_mode() {
        let mut config = Config::default();

        config.database.sql_mode = "mysql".to_string();
        assert_eq!(
            config.get_sql_mode(),
            Some(vibesql_types::SqlMode::MySQL { flags: vibesql_types::MySqlModeFlags::default() })
        );

        config.database.sql_mode = "MySQL".to_string(); // case insensitive
        assert_eq!(
            config.get_sql_mode(),
            Some(vibesql_types::SqlMode::MySQL { flags: vibesql_types::MySqlModeFlags::default() })
        );

        config.database.sql_mode = "sqlite".to_string();
        assert_eq!(config.get_sql_mode(), Some(vibesql_types::SqlMode::SQLite));

        config.database.sql_mode = "invalid".to_string();
        assert!(config.get_sql_mode().is_none());
    }
}
