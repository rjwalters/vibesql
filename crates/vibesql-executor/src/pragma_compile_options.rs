//! `pragma_compile_options` eponymous system table
//!
//! SQLite exposes each PRAGMA as an eponymous virtual table under the
//! `pragma_<name>` prefix, queryable from the FROM clause with bare-identifier
//! syntax (`SELECT * FROM pragma_compile_options`), *not* table-valued-function
//! call syntax. `pragma_compile_options` lists the compile-time options the
//! SQLite library was built with, one option string per row:
//!
//! ```sql
//! CREATE TABLE pragma_compile_options (
//!   compile_options TEXT
//! );
//! ```
//!
//! VibeSQL has no compile-time option flags to advertise, so the table is
//! synthesized with the correct single-column shape and **zero rows**. This is
//! sufficient for the SQLite conformance suite (json101.test's tail queries
//! `pragma_compile_options` via `db exists {SELECT 1 FROM pragma_compile_options
//! WHERE compile_options='...'}`; an empty result routes those checks to their
//! `else` branch, which is the expected behavior since VibeSQL does not define
//! the legacy JSON compile options). See issue #6019.
//!
//! Reference: <https://www.sqlite.org/pragma.html#pragma_compile_options>

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::DataType;

use crate::{errors::ExecutorError, select::SelectResult};

/// Check if a table reference is the `pragma_compile_options` eponymous table.
///
/// The match is case-insensitive, matching SQLite's identifier folding.
pub fn is_pragma_compile_options_table(table_name: &str) -> bool {
    table_name.eq_ignore_ascii_case("pragma_compile_options")
}

/// Get the schema for `pragma_compile_options`: a single `compile_options TEXT`
/// column.
pub fn get_pragma_compile_options_table_schema() -> TableSchema {
    TableSchema::new(
        "pragma_compile_options".to_string(),
        vec![ColumnSchema::new(
            "compile_options".to_string(),
            DataType::Varchar { max_length: None },
            true,
        )],
    )
}

/// Execute a `pragma_compile_options` query.
///
/// VibeSQL advertises no compile-time options, so this returns the correct
/// single-column shape with zero rows.
pub fn execute_pragma_compile_options_query() -> Result<SelectResult, ExecutorError> {
    let schema = get_pragma_compile_options_table_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    Ok(SelectResult { columns: column_names, rows: Vec::new() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_pragma_compile_options_table() {
        assert!(is_pragma_compile_options_table("pragma_compile_options"));
        assert!(is_pragma_compile_options_table("PRAGMA_COMPILE_OPTIONS"));
        assert!(is_pragma_compile_options_table("Pragma_Compile_Options"));
        assert!(!is_pragma_compile_options_table("compile_options"));
        assert!(!is_pragma_compile_options_table("pragma_table_info"));
        assert!(!is_pragma_compile_options_table("users"));
    }

    #[test]
    fn test_get_pragma_compile_options_table_schema() {
        let schema = get_pragma_compile_options_table_schema();
        assert_eq!(schema.name, "pragma_compile_options");
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].name, "compile_options");
    }

    #[test]
    fn test_execute_pragma_compile_options_query_is_empty() {
        let result = execute_pragma_compile_options_query().unwrap();
        assert_eq!(result.columns, vec!["compile_options"]);
        assert!(result.rows.is_empty());
    }
}
