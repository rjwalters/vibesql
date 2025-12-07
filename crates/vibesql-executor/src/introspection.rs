//! Database introspection command execution (SHOW, DESCRIBE)
//!
//! This module implements MySQL-style introspection commands that query
//! database metadata and return result sets.

use vibesql_ast::{
    DescribeStmt, ShowColumnsStmt, ShowCreateTableStmt, ShowDatabasesStmt, ShowIndexStmt,
    ShowTablesStmt,
};
use vibesql_catalog::{IndexType, ReferentialAction, SortOrder};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::select::SelectResult;

/// Executor for database introspection commands
pub struct IntrospectionExecutor<'a> {
    db: &'a Database,
}

impl<'a> IntrospectionExecutor<'a> {
    /// Create a new introspection executor
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Execute SHOW TABLES statement
    ///
    /// Returns a result set with a single column "Tables_in_<database>" containing
    /// the names of all tables in the specified (or current) schema.
    pub fn execute_show_tables(
        &self,
        stmt: &ShowTablesStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // Get the schema to query (default to current schema)
        let schema_name =
            stmt.database.as_deref().unwrap_or_else(|| self.db.catalog.get_current_schema());

        // Get tables from the schema
        let tables = if let Some(schema) = self.db.catalog.get_schema(schema_name) {
            schema.list_tables()
        } else {
            // Try case-insensitive lookup
            let upper_name = schema_name.to_uppercase();
            self.db
                .catalog
                .list_schemas()
                .into_iter()
                .find(|s| s.to_uppercase() == upper_name)
                .and_then(|actual_name| self.db.catalog.get_schema(&actual_name))
                .map(|schema| schema.list_tables())
                .unwrap_or_default()
        };

        // Filter by LIKE pattern if provided
        let filtered_tables: Vec<String> = if let Some(ref pattern) = stmt.like_pattern {
            tables.into_iter().filter(|t| like_match(pattern, t)).collect()
        } else {
            tables
        };

        // Build result set
        let column_name = format!("Tables_in_{}", schema_name);
        let rows: Vec<Row> = filtered_tables
            .into_iter()
            .map(|name| Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(name))]))
            .collect();

        Ok(SelectResult { columns: vec![column_name], rows })
    }

    /// Execute SHOW DATABASES statement
    ///
    /// Returns a result set with a single column "Database" containing
    /// the names of all schemas (databases) in the catalog.
    pub fn execute_show_databases(
        &self,
        stmt: &ShowDatabasesStmt,
    ) -> Result<SelectResult, ExecutorError> {
        let schemas = self.db.catalog.list_schemas();

        // Filter by LIKE pattern if provided
        let filtered_schemas: Vec<String> = if let Some(ref pattern) = stmt.like_pattern {
            schemas.into_iter().filter(|s| like_match(pattern, s)).collect()
        } else {
            schemas
        };

        let rows: Vec<Row> = filtered_schemas
            .into_iter()
            .map(|name| Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(name))]))
            .collect();

        Ok(SelectResult { columns: vec!["Database".to_string()], rows })
    }

    /// Execute SHOW COLUMNS statement
    ///
    /// Returns a result set with columns: Field, Type, Null, Key, Default, Extra
    pub fn execute_show_columns(
        &self,
        stmt: &ShowColumnsStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // Get the table
        let table_name = if let Some(ref db) = stmt.database {
            format!("{}.{}", db, stmt.table_name)
        } else {
            stmt.table_name.clone()
        };

        let table = self
            .db
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Determine which columns are part of the primary key
        let pk_columns: std::collections::HashSet<String> =
            table.primary_key.as_ref().map(|pk| pk.iter().cloned().collect()).unwrap_or_default();

        // Determine which columns are part of unique constraints
        let unique_columns: std::collections::HashSet<String> =
            table.unique_constraints.iter().flatten().cloned().collect();

        let mut rows: Vec<Row> = Vec::with_capacity(table.columns.len());

        for col in &table.columns {
            // Check if column matches LIKE pattern
            if let Some(ref pattern) = stmt.like_pattern {
                if !like_match(pattern, &col.name) {
                    continue;
                }
            }

            // Field: column name
            let field = SqlValue::Varchar(arcstr::ArcStr::from(col.name.clone()));

            // Type: data type as string
            let type_str = format_data_type(&col.data_type);
            let col_type = SqlValue::Varchar(arcstr::ArcStr::from(type_str));

            // Null: YES or NO
            let nullable = SqlValue::Varchar(if col.nullable { "YES" } else { "NO" }.into());

            // Key: PRI for primary key, UNI for unique, empty otherwise
            let key = if pk_columns.contains(&col.name) {
                SqlValue::Varchar("PRI".into())
            } else if unique_columns.contains(&col.name) {
                SqlValue::Varchar("UNI".into())
            } else {
                SqlValue::Varchar("".into())
            };

            // Default: default value or NULL
            let default = col
                .default_value
                .as_ref()
                .map(|expr| SqlValue::Varchar(arcstr::ArcStr::from(format!("{:?}", expr))))
                .unwrap_or(SqlValue::Null);

            // Extra: auto_increment, etc. (we don't have auto_increment info yet)
            let extra = SqlValue::Varchar("".into());

            if stmt.full {
                // SHOW FULL COLUMNS includes: Collation, Privileges, Comment
                rows.push(Row::new(vec![
                    field,
                    col_type,
                    SqlValue::Null, // Collation
                    nullable,
                    key,
                    default,
                    extra,
                    SqlValue::Varchar("select,insert,update,references".into()), // Privileges
                    SqlValue::Varchar("".into()),                                // Comment
                ]));
            } else {
                rows.push(Row::new(vec![field, col_type, nullable, key, default, extra]));
            }
        }

        let columns = if stmt.full {
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Collation".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
                "Privileges".to_string(),
                "Comment".to_string(),
            ]
        } else {
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
            ]
        };

        Ok(SelectResult { columns, rows })
    }

    /// Execute DESCRIBE statement (alias for SHOW COLUMNS)
    pub fn execute_describe(&self, stmt: &DescribeStmt) -> Result<SelectResult, ExecutorError> {
        // DESCRIBE is equivalent to SHOW COLUMNS FROM table
        let show_columns = ShowColumnsStmt {
            table_name: stmt.table_name.clone(),
            database: None,
            full: false,
            like_pattern: stmt.column_pattern.clone(),
            where_clause: None,
        };
        self.execute_show_columns(&show_columns)
    }

    /// Execute SHOW INDEX statement
    ///
    /// Returns a result set with index information in MySQL format.
    pub fn execute_show_index(&self, stmt: &ShowIndexStmt) -> Result<SelectResult, ExecutorError> {
        // Get the table name (with optional database qualifier)
        let table_name = if let Some(ref db) = stmt.database {
            format!("{}.{}", db, stmt.table_name)
        } else {
            stmt.table_name.clone()
        };

        // Verify table exists
        let table = self
            .db
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        let mut rows: Vec<Row> = Vec::new();

        // Add primary key as an index (if present)
        if let Some(ref pk_cols) = table.primary_key {
            for (seq, col_name) in pk_cols.iter().enumerate() {
                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from(stmt.table_name.clone())), // Table
                    SqlValue::Integer(0),                       // Non_unique (0 = unique)
                    SqlValue::Varchar("PRIMARY".into()),        // Key_name
                    SqlValue::Integer((seq + 1) as i64),        // Seq_in_index
                    SqlValue::Varchar(arcstr::ArcStr::from(col_name.clone())),        // Column_name
                    SqlValue::Varchar("A".into()),              // Collation (A = ascending)
                    SqlValue::Null,                             // Cardinality
                    SqlValue::Null,                             // Sub_part
                    SqlValue::Null,                             // Packed
                    SqlValue::Varchar("".into()),               // Null
                    SqlValue::Varchar("BTREE".into()),          // Index_type
                    SqlValue::Varchar("".into()),               // Comment
                    SqlValue::Varchar("".into()),               // Index_comment
                    SqlValue::Varchar("YES".into()),            // Visible
                ]));
            }
        }

        // Add explicit indexes from catalog
        let indexes = self.db.catalog.get_table_indexes(&stmt.table_name);
        for index in indexes {
            for (seq, col) in index.columns.iter().enumerate() {
                let non_unique = if index.is_unique { 0 } else { 1 };
                let collation = match col.order {
                    SortOrder::Ascending => "A",
                    SortOrder::Descending => "D",
                };
                let index_type = match index.index_type {
                    IndexType::BTree => "BTREE",
                    IndexType::Hash => "HASH",
                    IndexType::RTree => "RTREE",
                    IndexType::Fulltext => "FULLTEXT",
                    IndexType::IVFFlat { .. } => "IVFFLAT",
                    IndexType::Hnsw { .. } => "HNSW",
                };

                rows.push(Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from(stmt.table_name.clone())), // Table
                    SqlValue::Integer(non_unique),              // Non_unique
                    SqlValue::Varchar(arcstr::ArcStr::from(index.name.clone())),      // Key_name
                    SqlValue::Integer((seq + 1) as i64),        // Seq_in_index
                    SqlValue::Varchar(arcstr::ArcStr::from(col.column_name.clone())), // Column_name
                    SqlValue::Varchar(collation.into()),        // Collation
                    SqlValue::Null,                             // Cardinality
                    col.prefix_length
                        .map(|l| SqlValue::Integer(l as i64))
                        .unwrap_or(SqlValue::Null), // Sub_part
                    SqlValue::Null,                             // Packed
                    SqlValue::Varchar("".into()),               // Null
                    SqlValue::Varchar(index_type.into()),       // Index_type
                    SqlValue::Varchar("".into()),               // Comment
                    SqlValue::Varchar("".into()),               // Index_comment
                    SqlValue::Varchar("YES".into()),            // Visible
                ]));
            }
        }

        Ok(SelectResult {
            columns: vec![
                "Table".to_string(),
                "Non_unique".to_string(),
                "Key_name".to_string(),
                "Seq_in_index".to_string(),
                "Column_name".to_string(),
                "Collation".to_string(),
                "Cardinality".to_string(),
                "Sub_part".to_string(),
                "Packed".to_string(),
                "Null".to_string(),
                "Index_type".to_string(),
                "Comment".to_string(),
                "Index_comment".to_string(),
                "Visible".to_string(),
            ],
            rows,
        })
    }

    /// Execute SHOW CREATE TABLE statement
    ///
    /// Returns a result set with columns: Table, Create Table
    pub fn execute_show_create_table(
        &self,
        stmt: &ShowCreateTableStmt,
    ) -> Result<SelectResult, ExecutorError> {
        let table = self
            .db
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Build the CREATE TABLE statement
        let mut sql = format!("CREATE TABLE {} (\n", table.name);
        let mut definitions: Vec<String> = Vec::new();

        // Add column definitions
        for col in &table.columns {
            let mut col_def = format!("  {} {}", col.name, format_data_type(&col.data_type));

            if !col.nullable {
                col_def.push_str(" NOT NULL");
            }

            if let Some(ref default) = col.default_value {
                col_def.push_str(&format!(" DEFAULT {:?}", default));
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

            // Add ON DELETE action
            fk_def.push_str(&format!(" ON DELETE {}", format_referential_action(&fk.on_delete)));

            // Add ON UPDATE action
            fk_def.push_str(&format!(" ON UPDATE {}", format_referential_action(&fk.on_update)));

            definitions.push(fk_def);
        }

        // Add CHECK constraints
        for (name, _expr) in &table.check_constraints {
            definitions.push(format!("  CONSTRAINT {} CHECK (...)", name));
        }

        sql.push_str(&definitions.join(",\n"));
        sql.push_str("\n)");

        let rows =
            vec![Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(table.name.clone())), SqlValue::Varchar(arcstr::ArcStr::from(sql))])];

        Ok(SelectResult { columns: vec!["Table".to_string(), "Create Table".to_string()], rows })
    }
}

/// Match a SQL LIKE pattern against a string (case-insensitive)
///
/// Supports % (match zero or more characters) and _ (match exactly one character)
fn like_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let text = text.to_lowercase();

    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    like_match_impl(&pattern_chars, &text_chars)
}

/// Recursive implementation of LIKE pattern matching
fn like_match_impl(pattern: &[char], text: &[char]) -> bool {
    match (pattern.first(), text.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some('%'), _) => {
            // % matches zero or more characters
            // Try matching 0 characters (skip %) or matching 1 character
            like_match_impl(&pattern[1..], text)
                || (!text.is_empty() && like_match_impl(pattern, &text[1..]))
        }
        (Some('_'), Some(_)) => {
            // _ matches exactly one character
            like_match_impl(&pattern[1..], &text[1..])
        }
        (Some('_'), None) => false,
        (Some('\\'), _) if pattern.len() > 1 => {
            // Escape sequence - match the next character literally
            if text.first() == Some(&pattern[1]) {
                like_match_impl(&pattern[2..], &text[1..])
            } else {
                false
            }
        }
        (Some(p), Some(t)) if *p == *t => like_match_impl(&pattern[1..], &text[1..]),
        _ => false,
    }
}

/// Format a DataType as a SQL string
fn format_data_type(dt: &vibesql_types::DataType) -> String {
    use vibesql_types::DataType;

    match dt {
        DataType::Integer => "INT".to_string(),
        DataType::Smallint => "SMALLINT".to_string(),
        DataType::Bigint => "BIGINT".to_string(),
        DataType::Unsigned => "UNSIGNED BIGINT".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::Float { precision } => format!("FLOAT({})", precision),
        DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        DataType::Numeric { precision, scale } => {
            format!("NUMERIC({}, {})", precision, scale)
        }
        DataType::Decimal { precision, scale } => {
            format!("DECIMAL({}, {})", precision, scale)
        }
        DataType::Varchar { max_length } => {
            if let Some(len) = max_length {
                format!("VARCHAR({})", len)
            } else {
                "VARCHAR".to_string()
            }
        }
        DataType::Character { length } => {
            format!("CHAR({})", length)
        }
        DataType::CharacterLargeObject => "CLOB".to_string(),
        DataType::Name => "NAME".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn create_test_db() -> Database {
        let mut db = Database::new();
        db.catalog.set_case_sensitive_identifiers(false);

        // Create a users table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false,
            ),
        ];
        let schema =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        db.create_table(schema).unwrap();

        // Create an orders table
        let order_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "total".to_string(),
                DataType::Decimal { precision: 10, scale: 2 },
                true,
            ),
        ];
        let order_schema = TableSchema::with_primary_key(
            "orders".to_string(),
            order_columns,
            vec!["id".to_string()],
        );
        db.create_table(order_schema).unwrap();

        db
    }

    #[test]
    fn test_show_tables() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowTablesStmt { database: None, like_pattern: None, where_clause: None };

        let result = executor.execute_show_tables(&stmt).unwrap();
        assert_eq!(result.columns.len(), 1);
        assert!(result.columns[0].starts_with("Tables_in_"));

        // Should have 2 tables
        assert_eq!(result.rows.len(), 2);

        // Extract table names
        let table_names: Vec<&str> = result
            .rows
            .iter()
            .filter_map(|r| match &r.values[0] {
                SqlValue::Varchar(s) => Some(s.as_ref()),
                _ => None,
            })
            .collect();

        assert!(table_names.contains(&"users") || table_names.contains(&"USERS"));
        assert!(table_names.contains(&"orders") || table_names.contains(&"ORDERS"));
    }

    #[test]
    fn test_show_tables_with_like() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowTablesStmt {
            database: None,
            like_pattern: Some("user%".to_string()),
            where_clause: None,
        };

        let result = executor.execute_show_tables(&stmt).unwrap();
        // Only 'users' should match
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_show_databases() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowDatabasesStmt { like_pattern: None, where_clause: None };

        let result = executor.execute_show_databases(&stmt).unwrap();
        assert_eq!(result.columns, vec!["Database"]);

        // Should have at least the 'public' schema
        assert!(!result.rows.is_empty());

        let schema_names: Vec<&str> = result
            .rows
            .iter()
            .filter_map(|r| match &r.values[0] {
                SqlValue::Varchar(s) => Some(s.as_ref()),
                _ => None,
            })
            .collect();

        assert!(schema_names.contains(&"public"));
    }

    #[test]
    fn test_show_columns() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowColumnsStmt {
            table_name: "users".to_string(),
            database: None,
            full: false,
            like_pattern: None,
            where_clause: None,
        };

        let result = executor.execute_show_columns(&stmt).unwrap();
        assert_eq!(result.columns, vec!["Field", "Type", "Null", "Key", "Default", "Extra"]);
        assert_eq!(result.rows.len(), 3); // id, name, email
    }

    #[test]
    fn test_show_columns_full() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowColumnsStmt {
            table_name: "users".to_string(),
            database: None,
            full: true,
            like_pattern: None,
            where_clause: None,
        };

        let result = executor.execute_show_columns(&stmt).unwrap();
        assert_eq!(result.columns.len(), 9);
        assert!(result.columns.contains(&"Privileges".to_string()));
        assert!(result.columns.contains(&"Comment".to_string()));
    }

    #[test]
    fn test_describe() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = DescribeStmt { table_name: "users".to_string(), column_pattern: None };

        let result = executor.execute_describe(&stmt).unwrap();
        assert_eq!(result.columns, vec!["Field", "Type", "Null", "Key", "Default", "Extra"]);
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_show_index() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowIndexStmt { table_name: "users".to_string(), database: None };

        let result = executor.execute_show_index(&stmt).unwrap();

        // Should have at least the PRIMARY key index
        assert!(!result.rows.is_empty());

        // Check that PRIMARY KEY index is present
        let has_primary = result.rows.iter().any(|r| {
            match &r.values[2] {
                // Key_name column
                SqlValue::Varchar(s) => s.as_str() == "PRIMARY",
                _ => false,
            }
        });
        assert!(has_primary);
    }

    #[test]
    fn test_show_create_table() {
        let db = create_test_db();
        let executor = IntrospectionExecutor::new(&db);

        let stmt = ShowCreateTableStmt { table_name: "users".to_string() };

        let result = executor.execute_show_create_table(&stmt).unwrap();
        assert_eq!(result.columns, vec!["Table", "Create Table"]);
        assert_eq!(result.rows.len(), 1);

        // Check the CREATE TABLE statement contains expected elements
        if let SqlValue::Varchar(sql) = &result.rows[0].values[1] {
            assert!(sql.contains("CREATE TABLE"));
            assert!(sql.contains("users"));
            assert!(sql.contains("id"));
            assert!(sql.contains("name"));
            assert!(sql.contains("email"));
            assert!(sql.contains("PRIMARY KEY"));
        } else {
            panic!("Expected VARCHAR for Create Table");
        }
    }

    #[test]
    fn test_like_match() {
        // Exact match
        assert!(like_match("test", "test"));
        assert!(like_match("test", "TEST")); // case-insensitive
        assert!(!like_match("test", "testing"));

        // % matches zero or more characters
        assert!(like_match("test%", "test"));
        assert!(like_match("test%", "testing"));
        assert!(like_match("%test", "test"));
        assert!(like_match("%test", "mytest"));
        assert!(like_match("%test%", "test"));
        assert!(like_match("%test%", "mytesting"));

        // _ matches exactly one character
        assert!(like_match("te_t", "test"));
        assert!(!like_match("te_t", "tet"));
        assert!(!like_match("te_t", "testt"));

        // Escape sequences
        assert!(like_match("te\\_t", "te_t"));
        assert!(!like_match("te\\_t", "test"));
    }
}
