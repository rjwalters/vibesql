//! SQLite Schema Virtual Table
//!
//! Implements `sqlite_master` (and its alias `sqlite_schema`) for SQLite compatibility.
//! These virtual tables return metadata about database objects in the standard SQLite format.
//!
//! Schema:
//! ```sql
//! CREATE TABLE sqlite_master (
//!   type TEXT,      -- 'table', 'index', 'view', 'trigger'
//!   name TEXT,      -- name of the object
//!   tbl_name TEXT,  -- table the object is associated with
//!   rootpage INT,   -- internal (always 0 for VibeSQL)
//!   sql TEXT        -- CREATE statement that created the object
//! );
//! ```
//!
//! Reference: https://www.sqlite.org/schematab.html

use vibesql_ast::pretty_print::ToSql;
use vibesql_catalog::{ColumnSchema, ReferentialAction, SortOrder, TableSchema};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, select::SelectResult};

/// Check if a table reference is sqlite_master or sqlite_schema
pub fn is_sqlite_schema_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    matches!(normalized.as_str(), "sqlite_master" | "sqlite_schema")
}

/// Check if a table reference is sqlite_temp_master or sqlite_temp_schema.
///
/// These are the temp-schema introspection views: they list objects that live
/// in the session temp schema (temp tables, and indexes on temp tables), and
/// are the temp-schema counterpart to `sqlite_master`. See issue #5513.
pub fn is_sqlite_temp_schema_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    matches!(normalized.as_str(), "sqlite_temp_master" | "sqlite_temp_schema")
}

/// Get the schema for sqlite_master/sqlite_schema
pub fn get_sqlite_schema_table_schema() -> TableSchema {
    TableSchema::new(
        "sqlite_master".to_string(),
        vec![
            ColumnSchema::new("type".to_string(), DataType::Varchar { max_length: None }, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: None }, false),
            ColumnSchema::new(
                "tbl_name".to_string(),
                DataType::Varchar { max_length: None },
                false,
            ),
            ColumnSchema::new("rootpage".to_string(), DataType::Integer, false),
            ColumnSchema::new("sql".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    )
}

/// Build a single sqlite_master-format row.
fn schema_row(obj_type: &str, name: &str, tbl_name: &str, sql: String) -> Row {
    Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from(obj_type)),
        SqlValue::Varchar(arcstr::ArcStr::from(name)),
        SqlValue::Varchar(arcstr::ArcStr::from(tbl_name)),
        SqlValue::Integer(0), // rootpage - always 0 for VibeSQL
        SqlValue::Varchar(arcstr::ArcStr::from(sql)),
    ])
}

/// Execute a `sqlite_master` / `sqlite_schema` query.
///
/// Lists **main-schema** objects only. Temp-schema objects (temp tables and
/// indexes on temp tables) are reported by `sqlite_temp_master`, matching
/// SQLite 3.51.0 where a temp-table index appears in `sqlite_temp_master` and
/// never in `sqlite_master`. See issue #5513.
pub fn execute_sqlite_schema_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = get_sqlite_schema_table_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    // Add tables. `list_tables()` returns the current (main) schema only, so
    // temp tables are already excluded here.
    for table_name in catalog.list_tables() {
        if let Some(table) = catalog.get_table(&table_name) {
            let sql = generate_create_table_sql(table);
            rows.push(schema_row("table", &table_name, &table_name, sql));
        }
    }

    // Add indexes owned by the main schema. Temp-table indexes are tagged with
    // their temp schema (#5513) and surface only via sqlite_temp_master.
    for index in catalog.get_schema_indexes(vibesql_catalog::DEFAULT_SCHEMA) {
        let sql = generate_create_index_sql(index);
        rows.push(schema_row("index", &index.name, &index.table_name, sql));
    }

    // Add views
    for view_name in catalog.list_views() {
        if let Some(view) = catalog.get_view(&view_name) {
            let sql = generate_create_view_sql(view);
            // tbl_name is same as name for views
            rows.push(schema_row("view", &view.name, &view.name, sql));
        }
    }

    // Add triggers
    for trigger_name in catalog.list_triggers() {
        if let Some(trigger) = catalog.get_trigger(&trigger_name) {
            let sql = generate_create_trigger_sql(trigger);
            rows.push(schema_row("trigger", &trigger.name, &trigger.table_name, sql));
        }
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Execute a `sqlite_temp_master` / `sqlite_temp_schema` query.
///
/// Lists the session's **temp-schema** objects: temp tables and indexes on temp
/// tables. Mirrors SQLite 3.51.0, where:
///   CREATE TEMP TABLE t(a); CREATE INDEX i ON t(a);
///   SELECT name,type FROM sqlite_temp_master;  -- lists t and i
/// while `sqlite_master` lists neither. See issue #5513.
///
/// Scope note: VibeSQL does not yet schema-tag temp **views** or temp
/// **triggers** (tracked separately — see follow-ons), so this view currently
/// surfaces temp tables and temp-table indexes. That is sufficient for the
/// index introspection parity this issue targets.
pub fn execute_sqlite_temp_schema_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = get_sqlite_schema_table_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    let temp_schema = catalog.temp_schema_name();

    // Add temp tables (objects living in the session temp schema).
    for table_name in catalog.list_tables_in_schema(temp_schema) {
        // Look up via the temp-qualified name so we read the temp copy even when
        // a same-named main table shadows it for unqualified lookups.
        let qualified = format!("{}.{}", temp_schema, table_name);
        if let Some(table) = catalog.get_table(&qualified) {
            let sql = generate_create_table_sql(table);
            rows.push(schema_row("table", &table_name, &table_name, sql));
        }
    }

    // Add indexes owned by the temp schema.
    for index in catalog.get_schema_indexes(temp_schema) {
        let sql = generate_create_index_sql(index);
        rows.push(schema_row("index", &index.name, &index.table_name, sql));
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Generate CREATE TABLE SQL statement for a table
fn generate_create_table_sql(table: &TableSchema) -> String {
    let mut sql = format!("CREATE TABLE {} (\n", table.name);
    let mut definitions: Vec<String> = Vec::new();

    // Add column definitions
    for col in &table.columns {
        let mut col_def = format!("  {} {}", col.name, format_data_type(&col.data_type));

        // Handle generated columns (AS expression syntax)
        if let Some(ref generated_expr) = col.generated_expr {
            col_def.push_str(&format!(" AS ({})", format_expression(generated_expr)));
        } else {
            // Only non-generated columns can have NOT NULL and DEFAULT
            if !col.nullable {
                col_def.push_str(" NOT NULL");
            }

            if let Some(ref default) = col.default_value {
                col_def.push_str(&format!(" DEFAULT {}", format_expression(default)));
            }
        }

        definitions.push(col_def);
    }

    // Add PRIMARY KEY constraint
    if let Some(ref pk_cols) = table.primary_key {
        definitions.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
    }

    // Add UNIQUE constraints
    for unique_cols in &table.unique_constraints {
        definitions.push(format!("  UNIQUE ({})", unique_cols.join(", ")));
    }

    // Add FOREIGN KEY constraints
    for fk in &table.foreign_keys {
        let mut fk_def = format!(
            "  FOREIGN KEY ({}) REFERENCES {} ({})",
            fk.column_names.join(", "),
            fk.parent_table,
            fk.parent_column_names.join(", ")
        );

        // Add ON DELETE action if not NO ACTION
        if fk.on_delete != ReferentialAction::NoAction {
            fk_def.push_str(&format!(" ON DELETE {}", format_referential_action(&fk.on_delete)));
        }

        // Add ON UPDATE action if not NO ACTION
        if fk.on_update != ReferentialAction::NoAction {
            fk_def.push_str(&format!(" ON UPDATE {}", format_referential_action(&fk.on_update)));
        }

        // Emit DEFERRABLE clause when non-default. Matches SQLite's
        // sqlite_master format and the `introspection.rs` SHOW CREATE
        // TABLE wiring landed in Phase C3 of #5085.
        if fk.is_deferrable {
            if fk.initially_deferred {
                fk_def.push_str(" DEFERRABLE INITIALLY DEFERRED");
            } else {
                fk_def.push_str(" DEFERRABLE INITIALLY IMMEDIATE");
            }
        }

        definitions.push(fk_def);
    }

    // Add CHECK constraints
    for (name, expr) in &table.check_constraints {
        definitions.push(format!("  CONSTRAINT {} CHECK ({})", name, format_expression(expr)));
    }

    sql.push_str(&definitions.join(",\n"));
    sql.push_str("\n)");

    sql
}

/// Generate CREATE INDEX SQL statement for an index
fn generate_create_index_sql(index: &vibesql_catalog::IndexMetadata) -> String {
    let unique_str = if index.is_unique { "UNIQUE " } else { "" };

    let columns: Vec<String> = index
        .columns
        .iter()
        .map(|col| {
            let order_str = match col.order() {
                SortOrder::Ascending => "",
                SortOrder::Descending => " DESC",
            };
            match col {
                vibesql_catalog::IndexedColumn::Column { column_name, prefix_length, .. } => {
                    let prefix_str = prefix_length.map(|l| format!("({})", l)).unwrap_or_default();
                    format!("{}{}{}", column_name, prefix_str, order_str)
                }
                vibesql_catalog::IndexedColumn::Expression { expr, .. } => {
                    // For expression indexes, wrap the expression in parentheses
                    format!("({}){}", expr.to_sql(), order_str)
                }
            }
        })
        .collect();

    format!(
        "CREATE {}INDEX {} ON {}({})",
        unique_str,
        index.name,
        index.table_name,
        columns.join(", ")
    )
}

/// Generate CREATE VIEW SQL statement for a view
fn generate_create_view_sql(view: &vibesql_catalog::ViewDefinition) -> String {
    // Use stored SQL definition if available
    if let Some(ref sql) = view.sql_definition {
        return sql.clone();
    }

    // Otherwise generate from the view definition
    let columns_str =
        view.columns.as_ref().map(|cols| format!(" ({})", cols.join(", "))).unwrap_or_default();

    // Use the ToSql trait to generate valid SQL from the AST
    use vibesql_ast::pretty_print::ToSql;
    format!("CREATE VIEW {}{} AS {}", view.name, columns_str, view.query.to_sql())
}

/// Generate CREATE TRIGGER SQL statement for a trigger
fn generate_create_trigger_sql(trigger: &vibesql_catalog::TriggerDefinition) -> String {
    use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};

    // Prefer the original SQL text when available so `sqlite_master.sql` matches
    // SQLite's verbatim form (no injected BEFORE/FOR EACH ROW, original spacing
    // and quoting preserved). This mirrors `generate_create_view_sql` and is the
    // form kept consistent by `crate::trigger_rename` across ALTER TABLE RENAME.
    // SQLite stores the statement text without the trailing `;`, so strip one if
    // present.
    if let Some(ref sql) = trigger.sql_definition {
        let trimmed = sql.trim_end();
        let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed);
        return trimmed.trim_end().to_string();
    }

    let timing = match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };

    let granularity = match trigger.granularity {
        TriggerGranularity::Row => "ROW",
        TriggerGranularity::Statement => "STATEMENT",
    };

    let event = match &trigger.event {
        TriggerEvent::Insert => "INSERT".to_string(),
        TriggerEvent::Update(None) => "UPDATE".to_string(),
        TriggerEvent::Update(Some(cols)) => format!("UPDATE OF {}", cols.join(", ")),
        TriggerEvent::Delete => "DELETE".to_string(),
    };

    // Include WHEN clause if present
    let when_clause = trigger
        .when_condition
        .as_ref()
        .map(|expr| format!(" WHEN ({})", format_expression(expr)))
        .unwrap_or_default();

    // Include trigger body from TriggerAction
    let body = match &trigger.triggered_action {
        TriggerAction::RawSql(sql) => sql.clone(),
    };

    format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH {}{}{}",
        trigger.name, timing, event, trigger.table_name, granularity, when_clause, body
    )
}

/// Format a DataType as a SQL string
fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Integer => "INTEGER".to_string(),
        DataType::Smallint => "SMALLINT".to_string(),
        DataType::Bigint => "BIGINT".to_string(),
        DataType::Unsigned => "UNSIGNED BIGINT".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::Float { precision } => format!("FLOAT({})", precision),
        DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        DataType::Numeric { precision, scale } => format!("NUMERIC({}, {})", precision, scale),
        DataType::Decimal { precision, scale } => format!("DECIMAL({}, {})", precision, scale),
        DataType::Varchar { max_length } => {
            if let Some(len) = max_length {
                format!("VARCHAR({})", len)
            } else {
                "TEXT".to_string()
            }
        }
        DataType::Character { length } => format!("CHAR({})", length),
        DataType::CharacterLargeObject => "TEXT".to_string(),
        DataType::Name => "TEXT".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time { with_timezone } => {
            if *with_timezone {
                "TIME WITH TIME ZONE".to_string()
            } else {
                "TIME".to_string()
            }
        }
        DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "TIMESTAMP WITH TIME ZONE".to_string()
            } else {
                "TIMESTAMP".to_string()
            }
        }
        DataType::Interval { start_field, end_field } => {
            if let Some(end) = end_field {
                format!("INTERVAL {:?} TO {:?}", start_field, end)
            } else {
                format!("INTERVAL {:?}", start_field)
            }
        }
        DataType::BinaryLargeObject => "BLOB".to_string(),
        DataType::Bit { length } => {
            if let Some(len) = length {
                format!("BIT({})", len)
            } else {
                "BIT".to_string()
            }
        }
        DataType::UserDefined { type_name } => type_name.clone(),
        DataType::Vector { dimensions } => format!("VECTOR({})", dimensions),
        DataType::Null => "NULL".to_string(),
    }
}

/// Format a referential action as a SQL string
fn format_referential_action(action: &ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "NO ACTION",
        ReferentialAction::Restrict => "RESTRICT",
        ReferentialAction::Cascade => "CASCADE",
        ReferentialAction::SetNull => "SET NULL",
        ReferentialAction::SetDefault => "SET DEFAULT",
    }
}

/// Format an expression for SQL output
fn format_expression(expr: &vibesql_ast::Expression) -> String {
    use vibesql_ast::pretty_print::ToSql;
    expr.to_sql()
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{Catalog, IndexMetadata, IndexType, IndexedColumn};

    use super::*;

    #[test]
    fn test_is_sqlite_schema_table() {
        assert!(is_sqlite_schema_table("sqlite_master"));
        assert!(is_sqlite_schema_table("sqlite_schema"));
        assert!(is_sqlite_schema_table("SQLITE_MASTER"));
        assert!(is_sqlite_schema_table("SQLITE_SCHEMA"));
        assert!(is_sqlite_schema_table("Sqlite_Master"));
        assert!(!is_sqlite_schema_table("users"));
        assert!(!is_sqlite_schema_table("sqlite_stat1"));
        assert!(!is_sqlite_schema_table("information_schema.tables"));
        // sqlite_temp_master is a distinct view, not sqlite_master.
        assert!(!is_sqlite_schema_table("sqlite_temp_master"));
    }

    #[test]
    fn test_is_sqlite_temp_schema_table() {
        assert!(is_sqlite_temp_schema_table("sqlite_temp_master"));
        assert!(is_sqlite_temp_schema_table("sqlite_temp_schema"));
        assert!(is_sqlite_temp_schema_table("SQLITE_TEMP_MASTER"));
        assert!(is_sqlite_temp_schema_table("Sqlite_Temp_Schema"));
        assert!(!is_sqlite_temp_schema_table("sqlite_master"));
        assert!(!is_sqlite_temp_schema_table("users"));
    }

    #[test]
    fn test_temp_index_separated_from_main_in_schema_views() {
        // #5513: a temp-schema index appears in sqlite_temp_master and is
        // excluded from sqlite_master; a main index is the reverse.
        let mut catalog = Catalog::new();

        // main.t with a main index.
        let main_table = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table(main_table).unwrap();
        catalog
            .add_index(IndexMetadata::new(
                "main_i".to_string(),
                "t".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
                false,
            ))
            .unwrap();

        // temp.tt with a temp index.
        let temp_schema = catalog.temp_schema_name().to_string();
        let temp_table = TableSchema::new(
            "tt".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table_in_schema(&temp_schema, temp_table).unwrap();
        catalog
            .add_index(
                IndexMetadata::new(
                    "temp_i".to_string(),
                    "tt".to_string(),
                    IndexType::BTree,
                    vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
                    false,
                )
                .with_schema(temp_schema.clone()),
            )
            .unwrap();

        let master = execute_sqlite_schema_query(&catalog).unwrap();
        let master_names: Vec<String> = master
            .rows
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                _ => String::new(),
            })
            .collect();
        assert!(master_names.contains(&"main_i".to_string()), "master should list main index");
        assert!(!master_names.contains(&"temp_i".to_string()), "master must exclude temp index");

        let temp_master = execute_sqlite_temp_schema_query(&catalog).unwrap();
        let temp_names: Vec<String> = temp_master
            .rows
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                _ => String::new(),
            })
            .collect();
        assert!(temp_names.contains(&"temp_i".to_string()), "temp_master should list temp index");
        assert!(temp_names.contains(&"tt".to_string()), "temp_master should list temp table");
        assert!(!temp_names.contains(&"main_i".to_string()), "temp_master must exclude main index");
    }

    #[test]
    fn test_sqlite_schema_empty_db() {
        let catalog = Catalog::new();
        let result = execute_sqlite_schema_query(&catalog).unwrap();

        assert_eq!(result.columns, vec!["type", "name", "tbl_name", "rootpage", "sql"]);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_get_sqlite_schema_table_schema() {
        let schema = get_sqlite_schema_table_schema();

        assert_eq!(schema.name, "sqlite_master");
        assert_eq!(schema.columns.len(), 5);
        assert_eq!(schema.columns[0].name, "type");
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[2].name, "tbl_name");
        assert_eq!(schema.columns[3].name, "rootpage");
        assert_eq!(schema.columns[4].name, "sql");
    }

    #[test]
    fn test_format_data_type() {
        assert_eq!(format_data_type(&DataType::Integer), "INTEGER");
        assert_eq!(format_data_type(&DataType::Varchar { max_length: Some(255) }), "VARCHAR(255)");
        assert_eq!(format_data_type(&DataType::Varchar { max_length: None }), "TEXT");
        assert_eq!(format_data_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(
            format_data_type(&DataType::Numeric { precision: 10, scale: 2 }),
            "NUMERIC(10, 2)"
        );
    }

    #[test]
    fn test_sqlite_schema_with_table() {
        let mut catalog = Catalog::new();

        // Create a simple table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let table =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        catalog.create_table(table).unwrap();

        let result = execute_sqlite_schema_query(&catalog).unwrap();

        // Should have one row for the table
        assert_eq!(result.rows.len(), 1);

        // Check the type column
        let type_val = &result.rows[0].values[0];
        assert_eq!(type_val, &SqlValue::Varchar(arcstr::ArcStr::from("table")));

        // Check the name column
        let name_val = &result.rows[0].values[1];
        assert_eq!(name_val, &SqlValue::Varchar(arcstr::ArcStr::from("users")));

        // Check the tbl_name column (same as name for tables)
        let tbl_name_val = &result.rows[0].values[2];
        assert_eq!(tbl_name_val, &SqlValue::Varchar(arcstr::ArcStr::from("users")));

        // Check the rootpage column (always 0)
        let rootpage_val = &result.rows[0].values[3];
        assert_eq!(rootpage_val, &SqlValue::Integer(0));

        // Check the sql column contains CREATE TABLE
        let sql_val = &result.rows[0].values[4];
        if let SqlValue::Varchar(sql) = sql_val {
            assert!(sql.contains("CREATE TABLE users"));
            assert!(sql.contains("id INTEGER"));
            assert!(sql.contains("name VARCHAR(100)"));
            assert!(sql.contains("PRIMARY KEY"));
        } else {
            panic!("Expected VARCHAR for sql column");
        }
    }

    #[test]
    fn test_sqlite_schema_with_index() {
        let mut catalog = Catalog::new();

        // Create a table first
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false,
            ),
        ];
        let table =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        catalog.create_table(table).unwrap();

        // Create an index
        let index = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true, // unique
        );
        catalog.add_index(index).unwrap();

        let result = execute_sqlite_schema_query(&catalog).unwrap();

        // Should have two rows: one for table, one for index
        assert_eq!(result.rows.len(), 2);

        // Find the index row
        let index_row = result
            .rows
            .iter()
            .find(|r| matches!(&r.values[0], SqlValue::Varchar(s) if s.as_str() == "index"))
            .expect("Should have an index row");

        // Check index name
        assert_eq!(index_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("idx_email")));

        // Check tbl_name for index
        assert_eq!(index_row.values[2], SqlValue::Varchar(arcstr::ArcStr::from("users")));

        // Check sql contains CREATE INDEX
        if let SqlValue::Varchar(sql) = &index_row.values[4] {
            assert!(sql.contains("CREATE UNIQUE INDEX idx_email ON users"));
        } else {
            panic!("Expected VARCHAR for sql column");
        }
    }

    #[test]
    fn test_generate_create_index_sql() {
        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![
                IndexedColumn::new_column("last_name".to_string(), SortOrder::Ascending),
                IndexedColumn::Column {
                    column_name: "first_name".to_string(),
                    order: SortOrder::Descending,
                    prefix_length: None,
                },
            ],
            false, // not unique
        );

        let sql = generate_create_index_sql(&index);
        assert_eq!(sql, "CREATE INDEX idx_name ON users(last_name, first_name DESC)");
    }

    #[test]
    fn test_generate_create_index_sql_with_prefix() {
        let index = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column_with_prefix(
                "email".to_string(),
                SortOrder::Ascending,
                50,
            )],
            true, // unique
        );

        let sql = generate_create_index_sql(&index);
        assert_eq!(sql, "CREATE UNIQUE INDEX idx_email ON users(email(50))");
    }

    #[test]
    fn test_generate_create_trigger_sql_basic() {
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let trigger = TriggerDefinition {
            name: "audit_insert".to_string(),
            schema: None,
            table_name: "users".to_string(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql(
                " BEGIN INSERT INTO audit VALUES (NEW.id); END".to_string(),
            ),
            enabled: true,
            sql_definition: None,
        };

        let sql = generate_create_trigger_sql(&trigger);
        assert_eq!(
            sql,
            "CREATE TRIGGER audit_insert AFTER INSERT ON users FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id); END"
        );
    }

    #[test]
    fn test_generate_create_trigger_sql_with_update_of() {
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let trigger = TriggerDefinition {
            name: "track_status_change".to_string(),
            schema: None,
            table_name: "orders".to_string(),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Update(Some(vec!["status".to_string(), "updated_at".to_string()])),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql(" BEGIN SELECT 1; END".to_string()),
            enabled: true,
            sql_definition: None,
        };

        let sql = generate_create_trigger_sql(&trigger);
        assert!(sql.contains("UPDATE OF status, updated_at"));
        assert!(sql.contains("CREATE TRIGGER track_status_change BEFORE UPDATE OF"));
    }

    #[test]
    fn test_generate_create_trigger_sql_instead_of() {
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let trigger = TriggerDefinition {
            name: "instead_delete".to_string(),
            schema: None,
            table_name: "my_view".to_string(),
            timing: TriggerTiming::InsteadOf,
            event: TriggerEvent::Delete,
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql(
                " BEGIN DELETE FROM base_table WHERE id = OLD.id; END".to_string(),
            ),
            enabled: true,
            sql_definition: None,
        };

        let sql = generate_create_trigger_sql(&trigger);
        assert!(sql.contains("INSTEAD OF DELETE"));
        assert!(sql.contains("FOR EACH ROW"));
    }

    /// Create a minimal SelectStmt for testing purposes
    fn create_minimal_select_stmt() -> vibesql_ast::SelectStmt {
        vibesql_ast::SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    #[test]
    fn test_generate_create_view_sql_with_definition() {
        use vibesql_catalog::ViewDefinition;

        // Create a view with sql_definition set
        let view = ViewDefinition::new_with_sql(
            "active_users".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            create_minimal_select_stmt(),
            false,
            "CREATE VIEW active_users (id, name) AS SELECT id, name FROM users WHERE active = 1"
                .to_string(),
        );

        let sql = generate_create_view_sql(&view);
        assert_eq!(
            sql,
            "CREATE VIEW active_users (id, name) AS SELECT id, name FROM users WHERE active = 1"
        );
    }

    #[test]
    fn test_generate_create_view_sql_fallback() {
        use vibesql_catalog::ViewDefinition;

        // Create a view without sql_definition (fallback path)
        let view = ViewDefinition::new(
            "test_view".to_string(),
            Some(vec!["col1".to_string()]),
            create_minimal_select_stmt(),
            false,
        );

        let sql = generate_create_view_sql(&view);
        // Should contain CREATE VIEW and the view name
        assert!(sql.contains("CREATE VIEW test_view"));
        // With column list specified
        assert!(sql.contains("(col1)"));
    }
}
