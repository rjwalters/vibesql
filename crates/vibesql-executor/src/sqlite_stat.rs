//! SQLite Statistics Virtual Tables
//!
//! Implements `sqlite_stat1`, `sqlite_stat2`, `sqlite_stat3`, and `sqlite_stat4` for SQLite
//! compatibility. These tables store index statistics computed by the ANALYZE command.
//!
//! Currently, only `sqlite_stat1` is fully implemented, which is the most commonly used.
//!
//! Schema:
//! ```sql
//! CREATE TABLE sqlite_stat1 (
//!   tbl TEXT,   -- table name
//!   idx TEXT,   -- index name (NULL for table statistics)
//!   stat TEXT   -- space-separated statistics (row_count selectivity...)
//! );
//! ```
//!
//! Reference: https://www.sqlite.org/fileformat2.html#stat1tab

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, select::SelectResult};

/// Check if a table reference is a sqlite_stat table
pub fn is_sqlite_stat_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    matches!(normalized.as_str(), "sqlite_stat1" | "sqlite_stat2" | "sqlite_stat3" | "sqlite_stat4")
}

/// Check if a table reference is specifically sqlite_stat1
pub fn is_sqlite_stat1_table(table_name: &str) -> bool {
    table_name.eq_ignore_ascii_case("sqlite_stat1")
}

/// Get the schema for sqlite_stat1
pub fn get_sqlite_stat1_table_schema() -> TableSchema {
    TableSchema::new(
        "sqlite_stat1".to_string(),
        vec![
            ColumnSchema::new("tbl".to_string(), DataType::Varchar { max_length: None }, false),
            ColumnSchema::new("idx".to_string(), DataType::Varchar { max_length: None }, true),
            ColumnSchema::new("stat".to_string(), DataType::Varchar { max_length: None }, false),
        ],
    )
}

/// Execute a sqlite_stat1 query
///
/// This returns statistics that were manually inserted via INSERT INTO sqlite_stat1.
/// Unlike SQLite where ANALYZE populates sqlite_stat1 automatically, VibeSQL stores
/// statistics internally and uses sqlite_stat1 only for manual overrides.
///
/// To use statistics in VibeSQL:
/// 1. Run ANALYZE to compute internal statistics
/// 2. Optionally INSERT INTO sqlite_stat1 to override specific statistics
pub fn execute_sqlite_stat1_query(
    _catalog: &vibesql_catalog::Catalog,
    database: &vibesql_storage::Database,
) -> Result<SelectResult, ExecutorError> {
    let schema = get_sqlite_stat1_table_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    // Return manually inserted rows
    // SQLite compatibility: allow users to INSERT statistics for optimizer tuning
    for ((tbl, idx), stat) in database.get_all_sqlite_stat1() {
        rows.push(Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from(tbl.as_str())),
            match idx {
                Some(i) => SqlValue::Varchar(arcstr::ArcStr::from(i.as_str())),
                None => SqlValue::Null,
            },
            SqlValue::Varchar(arcstr::ArcStr::from(stat.as_str())),
        ]));
    }

    Ok(SelectResult { columns: column_names, rows })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_sqlite_stat_table() {
        assert!(is_sqlite_stat_table("sqlite_stat1"));
        assert!(is_sqlite_stat_table("SQLITE_STAT1"));
        assert!(is_sqlite_stat_table("Sqlite_Stat1"));
        assert!(is_sqlite_stat_table("sqlite_stat2"));
        assert!(is_sqlite_stat_table("sqlite_stat3"));
        assert!(is_sqlite_stat_table("sqlite_stat4"));
        assert!(!is_sqlite_stat_table("sqlite_master"));
        assert!(!is_sqlite_stat_table("users"));
    }

    #[test]
    fn test_get_sqlite_stat1_table_schema() {
        let schema = get_sqlite_stat1_table_schema();

        assert_eq!(schema.name, "sqlite_stat1");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "tbl");
        assert_eq!(schema.columns[1].name, "idx");
        assert_eq!(schema.columns[2].name, "stat");
    }
}
