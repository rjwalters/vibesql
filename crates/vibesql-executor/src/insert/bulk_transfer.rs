//! Bulk INSERT-SELECT optimization with schema-compatible transfer fast path
//!
//! This module implements SQLite-style transfer optimization for INSERT INTO ... SELECT
//! queries, achieving 10-50x performance improvement by bypassing unnecessary
//! serialization/deserialization cycles when schemas are compatible.

use vibesql_ast::{FromClause, SelectItem, SelectStmt};
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Attempt bulk transfer optimization for INSERT INTO ... SELECT
///
/// Returns Some(row_count) if optimization succeeded, None to fall back to normal path.
pub fn try_bulk_transfer(
    db: &mut Database,
    dest_table: &str,
    select_stmt: &SelectStmt,
) -> Result<Option<usize>, ExecutorError> {
    // Phase 1: Detect eligible pattern
    let source_table = match extract_simple_table_select(select_stmt) {
        Some(table) => table,
        None => return Ok(None), // Fall back to normal path
    };

    // Don't transfer to self (would create infinite loop or duplicates)
    if source_table == dest_table {
        return Ok(None);
    }

    // Phase 2: Check schema compatibility
    let dest_schema = db
        .catalog
        .get_table(dest_table)
        .ok_or_else(|| ExecutorError::TableNotFound(dest_table.to_string()))?
        .clone();

    let src_schema = db
        .catalog
        .get_table(&source_table)
        .ok_or_else(|| ExecutorError::TableNotFound(source_table.clone()))?
        .clone();

    let compat_result = check_schema_compatibility(&dest_schema, &src_schema)?;

    if !compat_result.compatible {
        return Ok(None); // Fall back to normal path
    }

    // AUTOINCREMENT tables (issue #6173) need their `sqlite_sequence`
    // high-water mark bumped as part of the INSERT statement (matching
    // sqlite3's "xfer optimization" — the `sqlite_sequence` write-back still
    // happens even when the row data itself is bulk-copied). Rather than
    // duplicating that bookkeeping here, fall back to the normal per-row path
    // (`execute_insert_internal`), which already has it (autoinc-10.1).
    if dest_schema.is_autoincrement {
        return Ok(None);
    }

    // Phase 3: Execute optimized transfer
    execute_bulk_transfer(db, dest_table, &source_table, &dest_schema, &compat_result)
}

/// Extract table name from simple SELECT * FROM table pattern
fn extract_simple_table_select(select_stmt: &SelectStmt) -> Option<String> {
    // A WITH clause on the source SELECT disables the bulk-transfer fast path:
    // the `FROM <name>` might resolve to a CTE rather than a catalog table, so
    // reading it directly from storage would raise a spurious "no such table"
    // (with2.test 5.7/5.8/7.1 — `INSERT INTO t WITH cte AS (...) SELECT * FROM
    // cte`). This also matches SQLite, which disables the xfer optimization
    // whenever a WITH clause is attached (with2.test 5.5-5.8 expect no xfer).
    if select_stmt.with_clause.is_some() {
        return None;
    }

    // Must have: SELECT * or SELECT table.*
    let is_wildcard =
        select_stmt.select_list.iter().any(|item| matches!(item, SelectItem::Wildcard { .. }));

    if !is_wildcard {
        return None;
    }

    // Must be single table (no joins)
    let from = select_stmt.from.as_ref()?;
    let table_name = match from {
        FromClause::Table { name, .. } => name.clone(),
        FromClause::Join { .. } => return None, // No joins
        FromClause::Subquery { .. } => return None, // No subqueries
        FromClause::Values { .. } => return None, // No VALUES clauses
        FromClause::TableFunction { .. } => return None, // No table functions
    };

    // No WHERE, GROUP BY, HAVING, DISTINCT, LIMIT, OFFSET
    if select_stmt.where_clause.is_some()
        || select_stmt.group_by.is_some()
        || select_stmt.having.is_some()
        || select_stmt.distinct
        || select_stmt.limit.is_some()
        || select_stmt.offset.is_some()
    {
        return None;
    }

    // No set operations (UNION, etc.)
    if select_stmt.set_operation.is_some() {
        return None;
    }

    Some(table_name)
}

/// Schema compatibility result
#[derive(Debug)]
struct CompatibilityResult {
    compatible: bool,
    /// Constraints that must be validated even with compatible schemas
    validate_unique: bool,
    validate_primary_key: bool,
    validate_foreign_keys: bool,
    validate_check: bool,
}

/// Check if source and destination schemas are compatible for bulk transfer
fn check_schema_compatibility(
    dest: &TableSchema,
    src: &TableSchema,
) -> Result<CompatibilityResult, ExecutorError> {
    let mut result = CompatibilityResult {
        compatible: true,
        validate_unique: false,
        validate_primary_key: false,
        validate_foreign_keys: false,
        validate_check: false,
    };

    // 1. Must have same column count
    if dest.columns.len() != src.columns.len() {
        result.compatible = false;
        return Ok(result);
    }

    // 2. Column-by-column type compatibility
    for (dest_col, src_col) in dest.columns.iter().zip(src.columns.iter()) {
        // Exact type match required
        if dest_col.data_type != src_col.data_type {
            result.compatible = false;
            return Ok(result);
        }

        // NOT NULL: dest NOT NULL requires src NOT NULL
        // Note: nullable=true means NULL allowed, nullable=false means NOT NULL
        if !dest_col.nullable && src_col.nullable {
            result.compatible = false;
            return Ok(result);
        }
    }

    // 3. Determine which constraints need validation
    // Even with compatible schemas, dest might have constraints source doesn't

    // Check if dest has UNIQUE constraints
    if !dest.get_unique_constraint_indices().is_empty() {
        result.validate_unique = true;
    }

    // Check if dest has PRIMARY KEY
    if dest.get_primary_key_indices().is_some() {
        result.validate_primary_key = true;
    }

    // Check if dest has FOREIGN KEY constraints
    if !dest.foreign_keys.is_empty() {
        result.validate_foreign_keys = true;
    }

    // Check if dest has CHECK constraints
    if !dest.check_constraints.is_empty() {
        result.validate_check = true;
    }

    Ok(result)
}

/// Execute the bulk transfer with selective constraint validation
fn execute_bulk_transfer(
    db: &mut Database,
    dest_table: &str,
    source_table: &str,
    dest_schema: &TableSchema,
    compat_result: &CompatibilityResult,
) -> Result<Option<usize>, ExecutorError> {
    // Get source rows
    let source_rows = {
        let src_table = db
            .get_table(source_table)
            .ok_or_else(|| ExecutorError::TableNotFound(source_table.to_string()))?;

        // Use scan_live() to skip rows that have been deleted but not yet
        // compacted. `scan()` returns raw storage including deleted rows; an
        // AFTER DELETE trigger body running `INSERT INTO log SELECT * FROM tbl`
        // must not observe the row that was just deleted mid-statement
        // (trigger2-2.14-after). The non-bulk SELECT path already filters
        // deleted rows, so this keeps the fast path consistent.
        src_table.scan_live().map(|(_, row)| row.values.clone()).collect::<Vec<_>>()
    };

    let mut pk_values_seen = Vec::new();
    let mut unique_values_seen = if !dest_schema.get_unique_constraint_indices().is_empty() {
        vec![Vec::new(); dest_schema.get_unique_constraint_indices().len()]
    } else {
        Vec::new()
    };

    // Expression / partial indexes on the destination require the
    // evaluation-time non-deterministic date/time check per row (issue
    // #5313); gate the per-row call on a single catalog lookup.
    let has_schema_expr_indexes = db
        .catalog
        .get_table_indexes(dest_table)
        .iter()
        .any(|idx| idx.is_partial() || idx.columns.iter().any(|c| c.is_expression()));

    // Collect all validated rows for batch insert
    let mut validated_rows: Vec<vibesql_storage::Row> = Vec::with_capacity(source_rows.len());

    for row_values in source_rows {
        // Validate only the constraints that differ between schemas

        // Primary key uniqueness (if dest has PK)
        if compat_result.validate_primary_key {
            super::constraints::enforce_primary_key_constraint(
                db,
                dest_schema,
                dest_table,
                &row_values,
                &pk_values_seen,
            )?;
        }

        // UNIQUE constraints (if dest has them)
        if compat_result.validate_unique {
            super::constraints::enforce_unique_constraints(
                db,
                dest_schema,
                dest_table,
                &row_values,
                &unique_values_seen,
            )?;
        }

        // CHECK constraints (if dest has them)
        if compat_result.validate_check {
            super::constraints::enforce_check_constraints(dest_schema, &row_values)?;
        }

        // Non-deterministic date/time uses in index expressions /
        // partial-index predicates (evaluation-time, SQLite semantics)
        if has_schema_expr_indexes {
            super::constraints::enforce_index_expression_determinism(
                db,
                dest_schema,
                dest_table,
                &row_values,
            )?;
        }

        // Foreign key constraints (if dest has them)
        if compat_result.validate_foreign_keys {
            super::foreign_keys::validate_foreign_key_constraints(db, dest_table, &row_values)?;
        }

        // Track constraint values for batch validation
        if compat_result.validate_primary_key {
            if let Some(pk_cols) = dest_schema.get_primary_key_indices() {
                let pk_vals: Vec<SqlValue> =
                    pk_cols.iter().map(|&col_idx| row_values[col_idx].clone()).collect();
                pk_values_seen.push(pk_vals);
            }
        }

        if compat_result.validate_unique {
            for (constraint_idx, constraint_col_indices) in
                dest_schema.get_unique_constraint_indices().iter().enumerate()
            {
                let unique_vals: Vec<SqlValue> = constraint_col_indices
                    .iter()
                    .map(|&col_idx| row_values[col_idx].clone())
                    .collect();
                unique_values_seen[constraint_idx].push(unique_vals);
            }
        }

        // Collect validated row for batch insert
        validated_rows.push(vibesql_storage::Row::new(row_values));
    }

    // last_insert_rowid() support. SQLite's transfer optimization scans the
    // source b-tree in ascending rowid order and inserts each row, so the value
    // it reports afterwards is the rowid of the highest source row copied. For a
    // rowid destination whose INTEGER PRIMARY KEY is the copied rowid, that is
    // the maximum IPK value among the transferred rows. We compute the max here
    // (rather than the last scanned row) because VibeSQL scans the source in
    // physical order, which may differ from rowid order. Mirror the row-by-row
    // INSERT path, which only tracks the rowid for rowid tables with an INTEGER
    // PRIMARY KEY. Capture before the batch insert consumes `validated_rows`.
    let last_rowid = if dest_schema.without_rowid {
        None
    } else {
        dest_schema.get_integer_primary_key_index().and_then(|idx| {
            validated_rows
                .iter()
                .filter_map(|row| match row.values.get(idx) {
                    Some(SqlValue::Integer(v)) => Some(*v),
                    _ => None,
                })
                .max()
        })
    };

    // Whether the destination is a rowid table WITHOUT an INTEGER PRIMARY KEY.
    // For those, the implicit rowid is allocated by the storage layer during the
    // batch insert below, so `last_rowid` above is None — we read the max rowid
    // back after the insert instead (#5944).
    let dest_is_implicit_rowid =
        !dest_schema.without_rowid && dest_schema.get_integer_primary_key_index().is_none();

    // Batch insert all rows at once (much faster than row-by-row)
    // This reduces WAL operations, index rebuilds, and cache invalidations
    let inserted_count = if !validated_rows.is_empty() {
        db.insert_rows_batch(dest_table, validated_rows)
            .map_err(|e| ExecutorError::UnsupportedExpression(format!("Storage error: {}", e)))?
    } else {
        0
    };

    // Update last_insert_rowid() only when rows were actually inserted.
    if inserted_count > 0 {
        if let Some(rowid) = last_rowid {
            db.set_last_insert_rowid(rowid);
        } else if dest_is_implicit_rowid {
            // Non-IPK rowid destination: implicit rowids ascend, so the max
            // rowid after the batch equals the last inserted row's rowid,
            // matching SQLite's transfer-optimization last_insert_rowid() (#5944).
            if let Some(rowid) = db.get_table(dest_table).and_then(|t| t.max_rowid_signed()) {
                db.set_last_insert_rowid(rowid);
            }
        }
    }

    // Invalidate the database-level columnar cache since table data changed.
    // Note: The table-level cache is already invalidated by insert_rows_batch().
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    if inserted_count > 0 {
        db.invalidate_columnar_cache(dest_table);
    }

    Ok(Some(inserted_count))
}

#[cfg(test)]
mod tests {
    use vibesql_ast::*;
    use vibesql_types::DataType;

    use super::*;

    #[test]
    fn test_extract_simple_table_select_valid() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "source".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert_eq!(extract_simple_table_select(&stmt), Some("source".to_string()));
    }

    #[test]
    fn test_extract_simple_table_select_with_where() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "source".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(Expression::Literal(SqlValue::Integer(1))),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert_eq!(extract_simple_table_select(&stmt), None);
    }

    #[test]
    fn test_extract_simple_table_select_with_distinct() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: true,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "source".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert_eq!(extract_simple_table_select(&stmt), None);
    }

    #[test]
    fn test_schema_compatibility_same_columns() {
        let schema1 = TableSchema::new(
            "t1".to_string(),
            vec![
                vibesql_catalog::ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    collation: None,
                },
                vibesql_catalog::ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    collation: None,
                },
            ],
        );

        let schema2 = schema1.clone();

        let result = check_schema_compatibility(&schema1, &schema2).unwrap();
        assert!(result.compatible);
    }

    #[test]
    fn test_schema_compatibility_different_column_count() {
        let schema1 = TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            }],
        );

        let schema2 = TableSchema::new(
            "t2".to_string(),
            vec![
                vibesql_catalog::ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    collation: None,
                },
                vibesql_catalog::ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    collation: None,
                },
            ],
        );

        let result = check_schema_compatibility(&schema1, &schema2).unwrap();
        assert!(!result.compatible);
    }

    #[test]
    fn test_schema_compatibility_different_types() {
        let schema1 = TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let schema2 = TableSchema::new(
            "t2".to_string(),
            vec![vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                DataType::Varchar { max_length: None },
                false,
            )],
        );

        let result = check_schema_compatibility(&schema1, &schema2).unwrap();
        assert!(!result.compatible);
    }

    #[test]
    fn test_schema_compatibility_not_null_mismatch() {
        let schema1 = TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false)], /* NOT NULL */
        );

        let schema2 = TableSchema::new(
            "t2".to_string(),
            vec![vibesql_catalog::ColumnSchema::new("id".to_string(), DataType::Integer, true)], /* nullable */
        );

        let result = check_schema_compatibility(&schema1, &schema2).unwrap();
        assert!(!result.compatible); // Dest NOT NULL requires src NOT NULL
    }
}
