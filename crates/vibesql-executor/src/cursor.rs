//! Cursor execution (SQL:1999 Feature E121)
//!
//! This module provides execution support for SQL cursor operations:
//! - DECLARE CURSOR: Declare a cursor for a query
//! - OPEN cursor: Execute the query and materialize results
//! - FETCH: Retrieve rows from an open cursor
//! - CLOSE cursor: Close and release cursor resources
//!
//! Cursors provide a way to iterate over query results row by row, which is
//! useful for processing large result sets without loading everything into memory.

use std::collections::HashMap;

use vibesql_ast::{
    CloseCursorStmt, DeclareCursorStmt, FetchOrientation, FetchStmt, OpenCursorStmt, SelectStmt,
};
use vibesql_storage::{Database, Row};

use crate::errors::ExecutorError;
use crate::SelectExecutor;

/// A cursor holding query definition and materialized results
#[derive(Debug, Clone)]
pub struct Cursor {
    /// Cursor name
    pub name: String,
    /// The SELECT query for this cursor
    pub query: Box<SelectStmt>,
    /// Materialized results (populated on OPEN)
    pub result: Option<CursorResult>,
    /// Current position (0 = before first row, 1 = first row, etc.)
    pub position: usize,
    /// Whether cursor supports backward movement
    pub scroll: bool,
    /// Whether cursor survives transaction commit (WITH HOLD)
    pub holdable: bool,
    /// Whether cursor uses snapshot isolation (INSENSITIVE)
    pub insensitive: bool,
}

/// Materialized cursor result set
#[derive(Debug, Clone)]
pub struct CursorResult {
    /// Column names
    pub columns: Vec<String>,
    /// All rows from the query
    pub rows: Vec<Row>,
}

/// Result of a FETCH operation
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// Column names
    pub columns: Vec<String>,
    /// Fetched rows (0 or 1 for single-row fetch, potentially more for FETCH ALL)
    pub rows: Vec<Row>,
}

impl FetchResult {
    /// Create an empty fetch result (no row at current position)
    pub fn empty(columns: Vec<String>) -> Self {
        Self { columns, rows: vec![] }
    }

    /// Create a fetch result with a single row
    pub fn single(columns: Vec<String>, row: Row) -> Self {
        Self { columns, rows: vec![row] }
    }
}

/// Storage for cursors within a session
#[derive(Debug, Default)]
pub struct CursorStore {
    cursors: HashMap<String, Cursor>,
}

impl CursorStore {
    /// Create a new empty cursor store
    pub fn new() -> Self {
        Self { cursors: HashMap::new() }
    }

    /// Declare a new cursor
    pub fn declare(&mut self, stmt: &DeclareCursorStmt) -> Result<(), ExecutorError> {
        let name = stmt.cursor_name.to_uppercase();

        if self.cursors.contains_key(&name) {
            return Err(ExecutorError::CursorAlreadyExists(name));
        }

        let cursor = Cursor {
            name: name.clone(),
            query: stmt.query.clone(),
            result: None,
            position: 0,
            scroll: stmt.scroll,
            holdable: stmt.hold.unwrap_or(false),
            insensitive: stmt.insensitive,
        };

        self.cursors.insert(name, cursor);
        Ok(())
    }

    /// Open a cursor (execute query and materialize results)
    pub fn open(&mut self, stmt: &OpenCursorStmt, db: &Database) -> Result<(), ExecutorError> {
        let name = stmt.cursor_name.to_uppercase();

        let cursor = self
            .cursors
            .get_mut(&name)
            .ok_or_else(|| ExecutorError::CursorNotFound(name.clone()))?;

        if cursor.result.is_some() {
            return Err(ExecutorError::CursorAlreadyOpen(name));
        }

        // Execute the query
        let executor = SelectExecutor::new(db);
        let select_result = executor.execute_with_columns(&cursor.query)?;

        cursor.result =
            Some(CursorResult { columns: select_result.columns, rows: select_result.rows });
        cursor.position = 0; // Before first row

        Ok(())
    }

    /// Fetch from a cursor
    pub fn fetch(&mut self, stmt: &FetchStmt) -> Result<FetchResult, ExecutorError> {
        let name = stmt.cursor_name.to_uppercase();

        let cursor = self
            .cursors
            .get_mut(&name)
            .ok_or_else(|| ExecutorError::CursorNotFound(name.clone()))?;

        let result =
            cursor.result.as_ref().ok_or_else(|| ExecutorError::CursorNotOpen(name.clone()))?;

        let row_count = result.rows.len();

        // Calculate new position based on fetch orientation
        let new_position = match &stmt.orientation {
            FetchOrientation::Next => cursor.position.saturating_add(1),
            FetchOrientation::Prior => {
                if !cursor.scroll {
                    return Err(ExecutorError::CursorNotScrollable(name));
                }
                cursor.position.saturating_sub(1)
            }
            FetchOrientation::First => {
                if !cursor.scroll && cursor.position > 0 {
                    return Err(ExecutorError::CursorNotScrollable(name));
                }
                1
            }
            FetchOrientation::Last => {
                if !cursor.scroll {
                    return Err(ExecutorError::CursorNotScrollable(name));
                }
                row_count
            }
            FetchOrientation::Absolute(n) => {
                if !cursor.scroll {
                    return Err(ExecutorError::CursorNotScrollable(name));
                }
                if *n >= 0 {
                    *n as usize
                } else {
                    // Negative: count from end
                    row_count.saturating_sub((-*n - 1) as usize)
                }
            }
            FetchOrientation::Relative(n) => {
                if !cursor.scroll && *n < 0 {
                    return Err(ExecutorError::CursorNotScrollable(name));
                }
                if *n >= 0 {
                    cursor.position.saturating_add(*n as usize)
                } else {
                    cursor.position.saturating_sub((-*n) as usize)
                }
            }
        };

        cursor.position = new_position;

        // Return row at position (1-indexed, 0 = before first)
        if new_position > 0 && new_position <= row_count {
            Ok(FetchResult::single(result.columns.clone(), result.rows[new_position - 1].clone()))
        } else {
            // No row at this position
            Ok(FetchResult::empty(result.columns.clone()))
        }
    }

    /// Close a cursor
    pub fn close(&mut self, stmt: &CloseCursorStmt) -> Result<(), ExecutorError> {
        let name = stmt.cursor_name.to_uppercase();

        if self.cursors.remove(&name).is_none() {
            return Err(ExecutorError::CursorNotFound(name));
        }

        Ok(())
    }

    /// Check if a cursor exists
    pub fn exists(&self, name: &str) -> bool {
        self.cursors.contains_key(&name.to_uppercase())
    }

    /// Check if a cursor is open
    pub fn is_open(&self, name: &str) -> bool {
        self.cursors.get(&name.to_uppercase()).map(|c| c.result.is_some()).unwrap_or(false)
    }

    /// Get the number of declared cursors
    pub fn count(&self) -> usize {
        self.cursors.len()
    }

    /// Clear all non-holdable cursors (called on transaction commit/rollback)
    pub fn clear_non_holdable(&mut self) {
        self.cursors.retain(|_, cursor| cursor.holdable);
    }

    /// Clear all cursors
    pub fn clear(&mut self) {
        self.cursors.clear();
    }
}

/// Executor for cursor operations
pub struct CursorExecutor;

impl CursorExecutor {
    /// Execute DECLARE CURSOR statement
    pub fn declare(store: &mut CursorStore, stmt: &DeclareCursorStmt) -> Result<(), ExecutorError> {
        store.declare(stmt)
    }

    /// Execute OPEN CURSOR statement
    pub fn open(
        store: &mut CursorStore,
        stmt: &OpenCursorStmt,
        db: &Database,
    ) -> Result<(), ExecutorError> {
        store.open(stmt, db)
    }

    /// Execute FETCH statement
    pub fn fetch(store: &mut CursorStore, stmt: &FetchStmt) -> Result<FetchResult, ExecutorError> {
        store.fetch(stmt)
    }

    /// Execute CLOSE CURSOR statement
    pub fn close(store: &mut CursorStore, stmt: &CloseCursorStmt) -> Result<(), ExecutorError> {
        store.close(stmt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{
        CloseCursorStmt, CursorUpdatability, DeclareCursorStmt, FetchOrientation, FetchStmt,
        FromClause, OpenCursorStmt, SelectItem, SelectStmt,
    };
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    fn create_test_db() -> Database {
        let mut db = Database::new();
        db.catalog.set_case_sensitive_identifiers(false);

        // Create employees table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ];
        let schema =
            TableSchema::with_primary_key("employees".to_string(), columns, vec!["id".to_string()]);
        db.create_table(schema).unwrap();

        // Insert test data
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(50000),
            ]),
        )
        .unwrap();
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Integer(60000),
            ]),
        )
        .unwrap();
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Varchar(arcstr::ArcStr::from("Carol")),
                SqlValue::Integer(55000),
            ]),
        )
        .unwrap();

        db
    }

    fn create_select_stmt() -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "employees".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    #[test]
    fn test_declare_cursor() {
        let mut store = CursorStore::new();

        let stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };

        assert!(store.declare(&stmt).is_ok());
        assert!(store.exists("emp_cursor"));
        assert!(!store.is_open("emp_cursor"));
    }

    #[test]
    fn test_declare_cursor_already_exists() {
        let mut store = CursorStore::new();

        let stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };

        store.declare(&stmt).unwrap();
        let result = store.declare(&stmt);
        assert!(matches!(result, Err(ExecutorError::CursorAlreadyExists(_))));
    }

    #[test]
    fn test_open_cursor() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let open_stmt = OpenCursorStmt { cursor_name: "emp_cursor".to_string() };
        assert!(store.open(&open_stmt, &db).is_ok());
        assert!(store.is_open("emp_cursor"));
    }

    #[test]
    fn test_open_cursor_not_found() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let open_stmt = OpenCursorStmt { cursor_name: "nonexistent".to_string() };
        let result = store.open(&open_stmt, &db);
        assert!(matches!(result, Err(ExecutorError::CursorNotFound(_))));
    }

    #[test]
    fn test_open_cursor_already_open() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let open_stmt = OpenCursorStmt { cursor_name: "emp_cursor".to_string() };
        store.open(&open_stmt, &db).unwrap();

        let result = store.open(&open_stmt, &db);
        assert!(matches!(result, Err(ExecutorError::CursorAlreadyOpen(_))));
    }

    #[test]
    fn test_fetch_next() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let open_stmt = OpenCursorStmt { cursor_name: "emp_cursor".to_string() };
        store.open(&open_stmt, &db).unwrap();

        // Fetch first row
        let fetch_stmt = FetchStmt {
            cursor_name: "emp_cursor".to_string(),
            orientation: FetchOrientation::Next,
            into_variables: None,
        };
        let result = store.fetch(&fetch_stmt).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(1));

        // Fetch second row
        let result = store.fetch(&fetch_stmt).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(2));

        // Fetch third row
        let result = store.fetch(&fetch_stmt).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(3));

        // Fetch past end - should return empty
        let result = store.fetch(&fetch_stmt).unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_fetch_from_unopened_cursor() {
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let fetch_stmt = FetchStmt {
            cursor_name: "emp_cursor".to_string(),
            orientation: FetchOrientation::Next,
            into_variables: None,
        };
        let result = store.fetch(&fetch_stmt);
        assert!(matches!(result, Err(ExecutorError::CursorNotOpen(_))));
    }

    #[test]
    fn test_fetch_prior_non_scrollable() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false, // Non-scrollable
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let open_stmt = OpenCursorStmt { cursor_name: "emp_cursor".to_string() };
        store.open(&open_stmt, &db).unwrap();

        // Move to first row
        let fetch_next = FetchStmt {
            cursor_name: "emp_cursor".to_string(),
            orientation: FetchOrientation::Next,
            into_variables: None,
        };
        store.fetch(&fetch_next).unwrap();

        // Try PRIOR on non-scrollable cursor
        let fetch_prior = FetchStmt {
            cursor_name: "emp_cursor".to_string(),
            orientation: FetchOrientation::Prior,
            into_variables: None,
        };
        let result = store.fetch(&fetch_prior);
        assert!(matches!(result, Err(ExecutorError::CursorNotScrollable(_))));
    }

    #[test]
    fn test_scroll_cursor() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "scroll_cursor".to_string(),
            insensitive: false,
            scroll: true, // Scrollable
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let open_stmt = OpenCursorStmt { cursor_name: "scroll_cursor".to_string() };
        store.open(&open_stmt, &db).unwrap();

        // Fetch LAST
        let fetch_last = FetchStmt {
            cursor_name: "scroll_cursor".to_string(),
            orientation: FetchOrientation::Last,
            into_variables: None,
        };
        let result = store.fetch(&fetch_last).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(3));

        // Fetch FIRST
        let fetch_first = FetchStmt {
            cursor_name: "scroll_cursor".to_string(),
            orientation: FetchOrientation::First,
            into_variables: None,
        };
        let result = store.fetch(&fetch_first).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(1));

        // Fetch ABSOLUTE 2
        let fetch_abs = FetchStmt {
            cursor_name: "scroll_cursor".to_string(),
            orientation: FetchOrientation::Absolute(2),
            into_variables: None,
        };
        let result = store.fetch(&fetch_abs).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(2));
    }

    #[test]
    fn test_close_cursor() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "emp_cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        let open_stmt = OpenCursorStmt { cursor_name: "emp_cursor".to_string() };
        store.open(&open_stmt, &db).unwrap();

        let close_stmt = CloseCursorStmt { cursor_name: "emp_cursor".to_string() };
        assert!(store.close(&close_stmt).is_ok());
        assert!(!store.exists("emp_cursor"));
    }

    #[test]
    fn test_close_nonexistent_cursor() {
        let mut store = CursorStore::new();

        let close_stmt = CloseCursorStmt { cursor_name: "nonexistent".to_string() };
        let result = store.close(&close_stmt);
        assert!(matches!(result, Err(ExecutorError::CursorNotFound(_))));
    }

    #[test]
    fn test_case_insensitive_cursor_names() {
        let db = create_test_db();
        let mut store = CursorStore::new();

        let declare_stmt = DeclareCursorStmt {
            cursor_name: "My_Cursor".to_string(),
            insensitive: false,
            scroll: false,
            hold: None,
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&declare_stmt).unwrap();

        // Open with different case
        let open_stmt = OpenCursorStmt { cursor_name: "MY_CURSOR".to_string() };
        assert!(store.open(&open_stmt, &db).is_ok());

        // Close with different case
        let close_stmt = CloseCursorStmt { cursor_name: "my_cursor".to_string() };
        assert!(store.close(&close_stmt).is_ok());
    }

    #[test]
    fn test_holdable_cursor() {
        let mut store = CursorStore::new();

        // Create holdable cursor
        let holdable_stmt = DeclareCursorStmt {
            cursor_name: "holdable".to_string(),
            insensitive: false,
            scroll: false,
            hold: Some(true),
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&holdable_stmt).unwrap();

        // Create non-holdable cursor
        let non_holdable_stmt = DeclareCursorStmt {
            cursor_name: "non_holdable".to_string(),
            insensitive: false,
            scroll: false,
            hold: Some(false),
            query: Box::new(create_select_stmt()),
            updatability: CursorUpdatability::Unspecified,
        };
        store.declare(&non_holdable_stmt).unwrap();

        assert_eq!(store.count(), 2);

        // Clear non-holdable cursors (simulating transaction commit)
        store.clear_non_holdable();

        assert_eq!(store.count(), 1);
        assert!(store.exists("holdable"));
        assert!(!store.exists("non_holdable"));
    }
}
