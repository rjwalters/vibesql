// ============================================================================
// Database Debug and Utility Functions
// ============================================================================

use super::core::Database;
use crate::StorageError;

impl Database {
    // ============================================================================
    // Debug and Utilities
    // ============================================================================

    /// Get debug information about database state
    pub fn debug_info(&self) -> String {
        let mut output = String::new();
        output.push_str("=== Database Debug Info ===\n");
        output.push_str(&format!("Tables: {}\n", self.list_tables().len()));
        for table_name in self.list_tables() {
            if let Some(table) = self.get_table(&table_name) {
                output.push_str(&format!(
                    "  - {} ({} rows, {} columns)\n",
                    table_name,
                    table.row_count(),
                    table.schema.column_count()
                ));
            }
        }
        output
    }

    /// Dump all table contents in readable format
    pub fn dump_tables(&self) -> String {
        let mut output = String::new();
        for table_name in self.list_tables() {
            if let Ok(dump) = self.dump_table(&table_name) {
                output.push_str(&dump);
                output.push('\n');
            }
        }
        output
    }

    /// Dump a specific table's contents
    pub fn dump_table(&self, name: &str) -> Result<String, StorageError> {
        let table =
            self.get_table(name).ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;

        let mut output = String::new();
        output.push_str(&format!("=== Table: {} ===\n", name));

        let col_names: Vec<String> = table.schema.columns.iter().map(|c| c.name.clone()).collect();
        output.push_str(&format!("{}\n", col_names.join(" | ")));
        output.push_str(&format!("{}\n", "-".repeat(col_names.join(" | ").len())));

        for row in table.scan() {
            let values: Vec<String> = row.values.iter().map(|v| format!("{}", v)).collect();
            output.push_str(&format!("{}\n", values.join(" | ")));
        }

        output.push_str(&format!("({} rows)\n", table.row_count()));
        Ok(output)
    }
}
