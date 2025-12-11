use super::{validation, SqlExecutor};
use crate::{
    commands::{CopyDirection, CopyFormat},
    data_io::DataIO,
};

impl SqlExecutor {
    pub fn handle_copy(
        &mut self,
        table: &str,
        file_path: &str,
        direction: CopyDirection,
        format: CopyFormat,
    ) -> anyhow::Result<()> {
        // Validate table name to prevent SQL injection
        validation::validate_table_name(&self.db, table)?;

        match direction {
            CopyDirection::Export => {
                // Execute SELECT * FROM table to get all data
                let query = format!("SELECT * FROM {}", table);
                let result = self.execute(&query)?;

                // Export based on format
                match format {
                    CopyFormat::Csv => DataIO::export_csv(&result, file_path)?,
                    CopyFormat::Json => DataIO::export_json(&result, file_path)?,
                }
            }
            CopyDirection::Import => {
                // Import based on format
                match format {
                    CopyFormat::Csv => {
                        // Validate CSV columns before import
                        validation::validate_csv_columns(&self.db, file_path, table)?;

                        // Import CSV - generates INSERT statements
                        let insert_statements = DataIO::import_csv(file_path, table)?;

                        // Execute each INSERT statement
                        let mut success_count = 0;
                        for stmt in &insert_statements {
                            match self.execute(stmt) {
                                Ok(_) => success_count += 1,
                                Err(e) => {
                                    eprintln!("Warning: Failed to insert row: {}", e);
                                    // Continue with remaining rows
                                }
                            }
                        }
                        println!("Imported {} rows into '{}'", success_count, table);
                    }
                    CopyFormat::Json => {
                        // Validate JSON columns before import
                        validation::validate_json_columns(&self.db, file_path, table)?;

                        // Import JSON - generates INSERT statements
                        let insert_statements = DataIO::import_json(file_path, table)?;

                        // Execute each INSERT statement
                        let mut success_count = 0;
                        for stmt in &insert_statements {
                            match self.execute(stmt) {
                                Ok(_) => success_count += 1,
                                Err(e) => {
                                    eprintln!("Warning: Failed to insert row: {}", e);
                                    // Continue with remaining rows
                                }
                            }
                        }
                        println!("Imported {} rows into '{}'", success_count, table);
                    }
                }
            }
        }
        Ok(())
    }
}
