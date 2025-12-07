// ============================================================================
// Table Data Serialization
// ============================================================================
//
// Handles reading and writing table row data.

use std::io::{Read, Write};

use super::{
    io::*,
    value::{read_sql_value, write_sql_value},
};
use crate::{Database, StorageError};

pub fn write_data<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
    let table_names = db.catalog.list_tables();

    for table_name in table_names {
        if let Some(table) = db.get_table(&table_name) {
            // Write table name
            write_string(writer, &table_name)?;

            // Write row count
            write_u64(writer, table.row_count() as u64)?;

            // Write each row
            for row in table.scan() {
                for value in &row.values {
                    write_sql_value(writer, value)?;
                }
            }
        }
    }

    Ok(())
}

pub fn read_data<R: Read>(reader: &mut R, db: &mut Database) -> Result<(), StorageError> {
    let table_count = db.catalog.list_tables().len();

    // Track tables that were loaded so we can rebuild their indexes
    let mut loaded_tables = Vec::with_capacity(table_count);

    for _ in 0..table_count {
        // Read table name from file (don't rely on list_tables() ordering)
        let table_name = read_string(reader)?;
        let row_count = read_u64(reader)?;

        // Get column count first
        let column_count = db
            .get_table(&table_name)
            .map(|t| t.schema.columns.len())
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Now get mutable reference and insert rows
        if let Some(table) = db.get_table_mut(&table_name) {
            for _ in 0..row_count {
                let mut values = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    values.push(read_sql_value(reader)?);
                }

                let row = crate::Row::from_vec(values);
                table.insert(row).map_err(|e| {
                    StorageError::NotImplemented(format!("Failed to insert row: {}", e))
                })?;
            }
        }

        loaded_tables.push(table_name);
    }

    // Rebuild database-level indexes for all loaded tables.
    // This is necessary because indexes are created during catalog loading when tables
    // are empty, so the index data structures have no entries. After loading rows,
    // we need to populate the indexes with the actual data.
    // This fixes the bug where ORDER BY on indexed columns returns empty results
    // after loading a database from disk (see issue #3602).
    for table_name in loaded_tables {
        db.rebuild_indexes(&table_name);
    }

    Ok(())
}
