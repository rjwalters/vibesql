//! Information Schema Virtual Tables
//!
//! Implements PostgreSQL-compatible information_schema views for drizzle-kit integration.
//! These virtual tables return metadata about the database catalog.
//!
//! Supported views:
//! - information_schema.tables - Table metadata
//! - information_schema.columns - Column metadata
//! - information_schema.table_constraints - Constraint metadata
//! - information_schema.key_column_usage - Key column mappings
//! - information_schema.schemata - Schema/database list

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::errors::ExecutorError;
use crate::select::SelectResult;

/// Check if a table reference is an information_schema table
pub fn is_information_schema_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    normalized.starts_with("information_schema.")
        || matches!(
            normalized.as_str(),
            "tables" | "columns" | "table_constraints" | "key_column_usage" | "schemata"
        )
}

/// Parse schema-qualified table name (e.g., "information_schema.tables")
/// Returns (schema_name, table_name)
pub fn parse_qualified_name(qualified_name: &str) -> (&str, &str) {
    if let Some(dot_pos) = qualified_name.find('.') {
        let (schema, table) = qualified_name.split_at(dot_pos);
        (schema, &table[1..]) // Skip the dot
    } else {
        ("public", qualified_name)
    }
}

/// Execute an information_schema query
pub fn execute_information_schema_query(
    table_name: &str,
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let table_lower = table_name.to_lowercase();

    match table_lower.as_str() {
        "tables" => execute_tables_query(catalog),
        "columns" => execute_columns_query(catalog),
        "table_constraints" => execute_table_constraints_query(catalog),
        "key_column_usage" => execute_key_column_usage_query(catalog),
        "schemata" => execute_schemata_query(catalog),
        _ => Err(ExecutorError::TableNotFound(format!("information_schema.{}", table_name))),
    }
}

/// Get the schema for an information_schema table
pub fn get_information_schema_table_schema(table_name: &str) -> Option<TableSchema> {
    let table_lower = table_name.to_lowercase();

    match table_lower.as_str() {
        "tables" => Some(tables_schema()),
        "columns" => Some(columns_schema()),
        "table_constraints" => Some(table_constraints_schema()),
        "key_column_usage" => Some(key_column_usage_schema()),
        "schemata" => Some(schemata_schema()),
        _ => None,
    }
}

// ============================================================================
// Schema Definitions
// ============================================================================

/// Schema for information_schema.tables
fn tables_schema() -> TableSchema {
    TableSchema::new(
        "tables".to_string(),
        vec![
            ColumnSchema::new(
                "table_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_type".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "self_referencing_column_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "reference_generation".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "user_defined_type_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "user_defined_type_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "user_defined_type_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_insertable_into".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("is_typed".to_string(), DataType::Varchar { max_length: None }, true),
            ColumnSchema::new(
                "commit_action".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
        ],
    )
}

/// Schema for information_schema.columns
fn columns_schema() -> TableSchema {
    TableSchema::new(
        "columns".to_string(),
        vec![
            ColumnSchema::new(
                "table_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "column_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("ordinal_position".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "column_default".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_nullable".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "data_type".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("character_maximum_length".to_string(), DataType::Integer, true),
            ColumnSchema::new("character_octet_length".to_string(), DataType::Integer, true),
            ColumnSchema::new("numeric_precision".to_string(), DataType::Integer, true),
            ColumnSchema::new("numeric_precision_radix".to_string(), DataType::Integer, true),
            ColumnSchema::new("numeric_scale".to_string(), DataType::Integer, true),
            ColumnSchema::new("datetime_precision".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "interval_type".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("interval_precision".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "character_set_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "character_set_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "character_set_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "collation_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "collation_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "collation_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "domain_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "domain_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "domain_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "udt_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "udt_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("udt_name".to_string(), DataType::Varchar { max_length: None }, true),
            ColumnSchema::new(
                "scope_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "scope_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "scope_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("maximum_cardinality".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "dtd_identifier".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_self_referencing".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_identity".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "identity_generation".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "identity_start".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "identity_increment".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "identity_maximum".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "identity_minimum".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "identity_cycle".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_generated".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "generation_expression".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_updatable".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
        ],
    )
}

/// Schema for information_schema.table_constraints
fn table_constraints_schema() -> TableSchema {
    TableSchema::new(
        "table_constraints".to_string(),
        vec![
            ColumnSchema::new(
                "constraint_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "constraint_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "constraint_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "constraint_type".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "is_deferrable".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "initially_deferred".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("enforced".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    )
}

/// Schema for information_schema.key_column_usage
fn key_column_usage_schema() -> TableSchema {
    TableSchema::new(
        "key_column_usage".to_string(),
        vec![
            ColumnSchema::new(
                "constraint_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "constraint_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "constraint_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "table_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "column_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("ordinal_position".to_string(), DataType::Integer, true),
            ColumnSchema::new("position_in_unique_constraint".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "referenced_table_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "referenced_table_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "referenced_column_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
        ],
    )
}

/// Schema for information_schema.schemata
fn schemata_schema() -> TableSchema {
    TableSchema::new(
        "schemata".to_string(),
        vec![
            ColumnSchema::new(
                "catalog_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "schema_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "schema_owner".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "default_character_set_catalog".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "default_character_set_schema".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new(
                "default_character_set_name".to_string(),
                DataType::Varchar { max_length: None },
                true,
            ),
            ColumnSchema::new("sql_path".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    )
}

// ============================================================================
// Query Implementations
// ============================================================================

/// Execute information_schema.tables query
fn execute_tables_query(catalog: &vibesql_catalog::Catalog) -> Result<SelectResult, ExecutorError> {
    let schema = tables_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    // Iterate over all schemas
    for schema_name in catalog.list_schemas() {
        // Get tables in this schema
        for table_name in catalog.list_tables() {
            // Check if table belongs to this schema (for now all tables are in public)
            let table_schema_name = &schema_name;

            rows.push(Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                SqlValue::Varchar(arcstr::ArcStr::from(table_schema_name.clone())), // table_schema
                SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                SqlValue::Varchar(arcstr::ArcStr::from("BASE TABLE")), // table_type
                SqlValue::Null,                           // self_referencing_column_name
                SqlValue::Null,                           // reference_generation
                SqlValue::Null,                           // user_defined_type_catalog
                SqlValue::Null,                           // user_defined_type_schema
                SqlValue::Null,                           // user_defined_type_name
                SqlValue::Varchar(arcstr::ArcStr::from("YES")),     // is_insertable_into
                SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_typed
                SqlValue::Null,                           // commit_action
            ]));
        }
    }

    // Add views
    for view_name in catalog.list_views() {
        rows.push(Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
            SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
            SqlValue::Varchar(arcstr::ArcStr::from(view_name.clone())),     // table_name
            SqlValue::Varchar(arcstr::ArcStr::from("VIEW")),    // table_type
            SqlValue::Null,                           // self_referencing_column_name
            SqlValue::Null,                           // reference_generation
            SqlValue::Null,                           // user_defined_type_catalog
            SqlValue::Null,                           // user_defined_type_schema
            SqlValue::Null,                           // user_defined_type_name
            SqlValue::Varchar(arcstr::ArcStr::from("NO")), // is_insertable_into (views typically not insertable)
            SqlValue::Varchar(arcstr::ArcStr::from("NO")), // is_typed
            SqlValue::Null,                      // commit_action
        ]));
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Execute information_schema.columns query
fn execute_columns_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = columns_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    // Iterate over all tables
    for table_name in catalog.list_tables() {
        if let Some(table_schema) = catalog.get_table(&table_name) {
            for (ordinal, column) in table_schema.columns.iter().enumerate() {
                let (char_max_len, char_octet_len) = get_character_lengths(&column.data_type);
                let (num_precision, num_scale, num_radix) =
                    get_numeric_precision(&column.data_type);
                let datetime_precision = get_datetime_precision(&column.data_type);

                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")),    // table_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),     // table_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),       // table_name
                    SqlValue::Varchar(arcstr::ArcStr::from(column.name.clone())),      // column_name
                    SqlValue::Integer((ordinal + 1) as i64),     // ordinal_position
                    format_default_value(&column.default_value), // column_default
                    SqlValue::Varchar(arcstr::ArcStr::from(if column.nullable { "YES" } else { "NO" })), // is_nullable
                    SqlValue::Varchar(arcstr::ArcStr::from(format_data_type(&column.data_type))), // data_type
                    char_max_len,                             // character_maximum_length
                    char_octet_len,                           // character_octet_length
                    num_precision,                            // numeric_precision
                    num_radix,                                // numeric_precision_radix
                    num_scale,                                // numeric_scale
                    datetime_precision,                       // datetime_precision
                    SqlValue::Null,                           // interval_type
                    SqlValue::Null,                           // interval_precision
                    SqlValue::Null,                           // character_set_catalog
                    SqlValue::Null,                           // character_set_schema
                    SqlValue::Null,                           // character_set_name
                    SqlValue::Null,                           // collation_catalog
                    SqlValue::Null,                           // collation_schema
                    SqlValue::Null,                           // collation_name
                    SqlValue::Null,                           // domain_catalog
                    SqlValue::Null,                           // domain_schema
                    SqlValue::Null,                           // domain_name
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // udt_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("pg_catalog")), // udt_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(format_udt_name(&column.data_type))), // udt_name
                    SqlValue::Null,                           // scope_catalog
                    SqlValue::Null,                           // scope_schema
                    SqlValue::Null,                           // scope_name
                    SqlValue::Null,                           // maximum_cardinality
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("{}", ordinal + 1))), // dtd_identifier
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_self_referencing
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_identity
                    SqlValue::Null,                           // identity_generation
                    SqlValue::Null,                           // identity_start
                    SqlValue::Null,                           // identity_increment
                    SqlValue::Null,                           // identity_maximum
                    SqlValue::Null,                           // identity_minimum
                    SqlValue::Null,                           // identity_cycle
                    SqlValue::Varchar(arcstr::ArcStr::from("NEVER")),   // is_generated
                    SqlValue::Null,                           // generation_expression
                    SqlValue::Varchar(arcstr::ArcStr::from("YES")),     // is_updatable
                ]));
            }
        }
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Execute information_schema.table_constraints query
fn execute_table_constraints_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = table_constraints_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    for table_name in catalog.list_tables() {
        if let Some(table_schema) = catalog.get_table(&table_name) {
            // Primary key constraint
            if table_schema.primary_key.is_some() {
                let constraint_name = format!("{}_pkey", table_name);
                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(constraint_name)),       // constraint_name
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                    SqlValue::Varchar(arcstr::ArcStr::from("PRIMARY KEY")), // constraint_type
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_deferrable
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // initially_deferred
                    SqlValue::Varchar(arcstr::ArcStr::from("YES")),     // enforced
                ]));
            }

            // Unique constraints
            for (idx, _unique_cols) in table_schema.unique_constraints.iter().enumerate() {
                let constraint_name = format!("{}_{}_key", table_name, idx);
                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(constraint_name)),       // constraint_name
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                    SqlValue::Varchar(arcstr::ArcStr::from("UNIQUE")),  // constraint_type
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_deferrable
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // initially_deferred
                    SqlValue::Varchar(arcstr::ArcStr::from("YES")),     // enforced
                ]));
            }

            // Foreign key constraints
            for fk in &table_schema.foreign_keys {
                let constraint_name = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_{}_fkey", table_name, fk.parent_table));
                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(constraint_name)),       // constraint_name
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                    SqlValue::Varchar(arcstr::ArcStr::from("FOREIGN KEY")), // constraint_type
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_deferrable
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // initially_deferred
                    SqlValue::Varchar(arcstr::ArcStr::from("YES")),     // enforced
                ]));
            }

            // Check constraints
            for (check_name, _check_expr) in &table_schema.check_constraints {
                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(check_name.clone())),    // constraint_name
                    SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                    SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                    SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                    SqlValue::Varchar(arcstr::ArcStr::from("CHECK")),   // constraint_type
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // is_deferrable
                    SqlValue::Varchar(arcstr::ArcStr::from("NO")),      // initially_deferred
                    SqlValue::Varchar(arcstr::ArcStr::from("YES")),     // enforced
                ]));
            }
        }
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Execute information_schema.key_column_usage query
fn execute_key_column_usage_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = key_column_usage_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    for table_name in catalog.list_tables() {
        if let Some(table_schema) = catalog.get_table(&table_name) {
            // Primary key columns
            if let Some(pk_cols) = &table_schema.primary_key {
                let constraint_name = format!("{}_pkey", table_name);
                for (ordinal, col_name) in pk_cols.iter().enumerate() {
                    rows.push(Row::new(vec![
                        SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(constraint_name.clone())), // constraint_name
                        SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                        SqlValue::Varchar(arcstr::ArcStr::from(col_name.clone())),      // column_name
                        SqlValue::Integer((ordinal + 1) as i64),  // ordinal_position
                        SqlValue::Null,                           // position_in_unique_constraint
                        SqlValue::Null,                           // referenced_table_schema
                        SqlValue::Null,                           // referenced_table_name
                        SqlValue::Null,                           // referenced_column_name
                    ]));
                }
            }

            // Unique constraint columns
            for (idx, unique_cols) in table_schema.unique_constraints.iter().enumerate() {
                let constraint_name = format!("{}_{}_key", table_name, idx);
                for (ordinal, col_name) in unique_cols.iter().enumerate() {
                    rows.push(Row::new(vec![
                        SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(constraint_name.clone())), // constraint_name
                        SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                        SqlValue::Varchar(arcstr::ArcStr::from(col_name.clone())),      // column_name
                        SqlValue::Integer((ordinal + 1) as i64),  // ordinal_position
                        SqlValue::Null,                           // position_in_unique_constraint
                        SqlValue::Null,                           // referenced_table_schema
                        SqlValue::Null,                           // referenced_table_name
                        SqlValue::Null,                           // referenced_column_name
                    ]));
                }
            }

            // Foreign key columns
            for fk in &table_schema.foreign_keys {
                let constraint_name = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_{}_fkey", table_name, fk.parent_table));
                for (ordinal, (col_name, ref_col_name)) in
                    fk.column_names.iter().zip(fk.parent_column_names.iter()).enumerate()
                {
                    rows.push(Row::new(vec![
                        SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // constraint_catalog
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // constraint_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(constraint_name.clone())), // constraint_name
                        SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // table_catalog
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // table_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(table_name.clone())),    // table_name
                        SqlValue::Varchar(arcstr::ArcStr::from(col_name.clone())),      // column_name
                        SqlValue::Integer((ordinal + 1) as i64),  // ordinal_position
                        SqlValue::Integer((ordinal + 1) as i64),  // position_in_unique_constraint
                        SqlValue::Varchar(arcstr::ArcStr::from("public")),  // referenced_table_schema
                        SqlValue::Varchar(arcstr::ArcStr::from(fk.parent_table.clone())), // referenced_table_name
                        SqlValue::Varchar(arcstr::ArcStr::from(ref_col_name.clone())),  // referenced_column_name
                    ]));
                }
            }
        }
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Execute information_schema.schemata query
fn execute_schemata_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = schemata_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    for schema_name in catalog.list_schemas() {
        rows.push(Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("vibesql")), // catalog_name
            SqlValue::Varchar(arcstr::ArcStr::from(schema_name.clone())),   // schema_name
            SqlValue::Null,                           // schema_owner
            SqlValue::Null,                           // default_character_set_catalog
            SqlValue::Null,                           // default_character_set_schema
            SqlValue::Varchar(arcstr::ArcStr::from("UTF8")),    // default_character_set_name
            SqlValue::Null,                           // sql_path
        ]));
    }

    Ok(SelectResult { columns: column_names, rows })
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Format data type for PostgreSQL-style display
fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Integer => "integer".to_string(),
        DataType::Bigint => "bigint".to_string(),
        DataType::Smallint => "smallint".to_string(),
        DataType::Float { precision } => {
            if *precision <= 24 {
                "real".to_string()
            } else {
                "double precision".to_string()
            }
        }
        DataType::DoublePrecision => "double precision".to_string(),
        DataType::Real => "real".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Varchar { max_length } => match max_length {
            Some(len) => format!("character varying({})", len),
            None => "character varying".to_string(),
        },
        DataType::Character { length } => format!("character({})", length),
        DataType::CharacterLargeObject => "text".to_string(),
        DataType::Name => "name".to_string(),
        DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale } => {
            if *scale > 0 {
                format!("numeric({},{})", precision, scale)
            } else {
                format!("numeric({})", precision)
            }
        }
        DataType::Date => "date".to_string(),
        DataType::Time { with_timezone } => {
            if *with_timezone {
                "time with time zone".to_string()
            } else {
                "time without time zone".to_string()
            }
        }
        DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "timestamp with time zone".to_string()
            } else {
                "timestamp without time zone".to_string()
            }
        }
        DataType::BinaryLargeObject => "bytea".to_string(),
        DataType::Interval { .. } => "interval".to_string(),
        DataType::Bit { length } => match length {
            Some(len) => format!("bit({})", len),
            None => "bit(1)".to_string(),
        },
        DataType::Unsigned => "bigint".to_string(), // No unsigned in PostgreSQL, use bigint
        DataType::UserDefined { type_name } => type_name.clone(),
        DataType::Vector { dimensions } => format!("vector({})", dimensions),
        DataType::Null => "unknown".to_string(),
    }
}

/// Format UDT name for PostgreSQL-style display
fn format_udt_name(dt: &DataType) -> String {
    match dt {
        DataType::Integer => "int4".to_string(),
        DataType::Bigint => "int8".to_string(),
        DataType::Smallint => "int2".to_string(),
        DataType::Float { precision } => {
            if *precision <= 24 {
                "float4".to_string()
            } else {
                "float8".to_string()
            }
        }
        DataType::DoublePrecision => "float8".to_string(),
        DataType::Real => "float4".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Varchar { .. } | DataType::CharacterLargeObject | DataType::Name => {
            "varchar".to_string()
        }
        DataType::Character { .. } => "bpchar".to_string(),
        DataType::Numeric { .. } | DataType::Decimal { .. } => "numeric".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time { .. } => "time".to_string(),
        DataType::Timestamp { .. } => "timestamp".to_string(),
        DataType::BinaryLargeObject => "bytea".to_string(),
        DataType::Interval { .. } => "interval".to_string(),
        DataType::Bit { .. } => "bit".to_string(),
        DataType::Unsigned => "int8".to_string(),
        DataType::UserDefined { type_name } => type_name.clone(),
        DataType::Vector { .. } => "vector".to_string(),
        DataType::Null => "unknown".to_string(),
    }
}

/// Get character length information for a data type
fn get_character_lengths(dt: &DataType) -> (SqlValue, SqlValue) {
    match dt {
        DataType::Varchar { max_length: Some(len) } => {
            (SqlValue::Integer(*len as i64), SqlValue::Integer(*len as i64 * 4))
            // UTF-8 max 4 bytes/char
        }
        DataType::Varchar { max_length: None } | DataType::CharacterLargeObject => {
            (SqlValue::Null, SqlValue::Null)
        }
        DataType::Character { length } => {
            (SqlValue::Integer(*length as i64), SqlValue::Integer(*length as i64 * 4))
        }
        DataType::Name => {
            (SqlValue::Integer(128), SqlValue::Integer(512)) // NAME is VARCHAR(128)
        }
        _ => (SqlValue::Null, SqlValue::Null),
    }
}

/// Get numeric precision, scale, and radix for a data type
fn get_numeric_precision(dt: &DataType) -> (SqlValue, SqlValue, SqlValue) {
    match dt {
        DataType::Smallint => (SqlValue::Integer(16), SqlValue::Integer(0), SqlValue::Integer(2)),
        DataType::Integer | DataType::Unsigned => {
            (SqlValue::Integer(32), SqlValue::Integer(0), SqlValue::Integer(2))
        }
        DataType::Bigint => (SqlValue::Integer(64), SqlValue::Integer(0), SqlValue::Integer(2)),
        DataType::Float { precision } => {
            (SqlValue::Integer(*precision as i64), SqlValue::Null, SqlValue::Integer(2))
        }
        DataType::Real => (SqlValue::Integer(24), SqlValue::Null, SqlValue::Integer(2)),
        DataType::DoublePrecision => (SqlValue::Integer(53), SqlValue::Null, SqlValue::Integer(2)),
        DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale } => (
            SqlValue::Integer(*precision as i64),
            SqlValue::Integer(*scale as i64),
            SqlValue::Integer(10),
        ),
        _ => (SqlValue::Null, SqlValue::Null, SqlValue::Null),
    }
}

/// Get datetime precision for a data type
fn get_datetime_precision(dt: &DataType) -> SqlValue {
    match dt {
        DataType::Date => SqlValue::Integer(0),
        DataType::Time { .. } | DataType::Timestamp { .. } => SqlValue::Integer(6), // Default precision
        DataType::Interval { .. } => SqlValue::Integer(6),
        _ => SqlValue::Null,
    }
}

/// Format default value for display
fn format_default_value(default: &Option<vibesql_ast::Expression>) -> SqlValue {
    match default {
        Some(expr) => SqlValue::Varchar(arcstr::ArcStr::from(format!("{:?}", expr))), // Simple debug representation
        None => SqlValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_information_schema_table() {
        assert!(is_information_schema_table("information_schema.tables"));
        assert!(is_information_schema_table("INFORMATION_SCHEMA.TABLES"));
        assert!(is_information_schema_table("information_schema.columns"));
        assert!(!is_information_schema_table("users"));
        assert!(!is_information_schema_table("public.users"));
    }

    #[test]
    fn test_parse_qualified_name() {
        assert_eq!(
            parse_qualified_name("information_schema.tables"),
            ("information_schema", "tables")
        );
        assert_eq!(parse_qualified_name("public.users"), ("public", "users"));
        assert_eq!(parse_qualified_name("users"), ("public", "users"));
    }

    #[test]
    fn test_format_data_type() {
        assert_eq!(format_data_type(&DataType::Integer), "integer");
        assert_eq!(
            format_data_type(&DataType::Varchar { max_length: Some(255) }),
            "character varying(255)"
        );
        assert_eq!(
            format_data_type(&DataType::Numeric { precision: 10, scale: 2 }),
            "numeric(10,2)"
        );
        assert_eq!(format_data_type(&DataType::Boolean), "boolean");
        assert_eq!(format_data_type(&DataType::Date), "date");
    }

    #[test]
    fn test_schemata_query() {
        let catalog = vibesql_catalog::Catalog::new();
        let result = execute_schemata_query(&catalog).unwrap();

        assert_eq!(result.columns[0], "catalog_name");
        assert_eq!(result.columns[1], "schema_name");
        assert!(!result.rows.is_empty()); // Should have at least "public" schema
    }

    #[test]
    fn test_tables_query_empty() {
        let catalog = vibesql_catalog::Catalog::new();
        let result = execute_tables_query(&catalog).unwrap();

        assert_eq!(result.columns[0], "table_catalog");
        assert_eq!(result.columns[1], "table_schema");
        assert_eq!(result.columns[2], "table_name");
    }

    #[test]
    fn test_columns_query_empty() {
        let catalog = vibesql_catalog::Catalog::new();
        let result = execute_columns_query(&catalog).unwrap();

        assert_eq!(result.columns[0], "table_catalog");
        assert_eq!(result.columns[3], "column_name");
        assert!(result.rows.is_empty()); // No tables = no columns
    }
}
