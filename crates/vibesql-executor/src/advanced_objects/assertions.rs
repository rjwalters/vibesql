//! Executor for ASSERTION objects (SQL:1999 Feature F671/F672)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE ASSERTION statement (SQL:1999 Feature F671/F672)
pub fn execute_create_assertion(
    stmt: &CreateAssertionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::Assertion;

    let assertion = Assertion::new(stmt.assertion_name.clone(), (*stmt.check_condition).clone());

    db.catalog.create_assertion(assertion)?;
    Ok(())
}

/// Execute DROP ASSERTION statement (SQL:1999 Feature F671/F672)
pub fn execute_drop_assertion(
    stmt: &DropAssertionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_assertion(&stmt.assertion_name, stmt.cascade)?;
    Ok(())
}

/// Assertion checker for runtime constraint enforcement (SQL:1999 Feature F671/F672)
///
/// This struct provides methods to check all database assertions after DML operations.
/// Assertions are schema-level constraints that can span multiple tables.
pub struct AssertionChecker;

impl AssertionChecker {
    /// Check all assertions in the database
    ///
    /// This should be called after INSERT, UPDATE, or DELETE operations to ensure
    /// that no assertion constraints are violated.
    ///
    /// # Arguments
    /// * `db` - Database reference (immutable, we only read data)
    ///
    /// # Returns
    /// * `Ok(())` if all assertions pass
    /// * `Err(ExecutorError::AssertionViolation)` if any assertion is violated
    pub fn check_all_assertions(db: &Database) -> Result<(), ExecutorError> {
        // Collect assertions first to avoid borrowing issues
        let assertions: Vec<_> = db
            .catalog
            .get_all_assertions()
            .map(|a| (a.name.clone(), a.check_condition.clone()))
            .collect();

        // If no assertions, return early
        if assertions.is_empty() {
            return Ok(());
        }

        // Check each assertion
        for (assertion_name, check_condition) in assertions {
            Self::check_assertion(db, &assertion_name, &check_condition)?;
        }

        Ok(())
    }

    /// Check a single assertion
    ///
    /// Evaluates the assertion's CHECK condition and returns an error if it evaluates to FALSE.
    fn check_assertion(
        db: &Database,
        assertion_name: &str,
        check_condition: &Expression,
    ) -> Result<(), ExecutorError> {
        // Build a SELECT statement that evaluates the check condition
        // SELECT (check_condition) -- should return TRUE if assertion holds
        let select_stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: check_condition.clone(),
                alias: None,
                source_text: None,
            }],
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
        };

        // Execute the SELECT
        let executor = crate::SelectExecutor::new(db);
        let rows = executor.execute(&select_stmt)?;

        // Check the result
        if rows.is_empty() {
            // No rows returned - this shouldn't happen for a scalar expression
            // Treat as violation to be safe
            return Err(ExecutorError::AssertionViolation {
                assertion_name: assertion_name.to_string(),
            });
        }

        // Get the result value
        let result = &rows[0].values[0];

        // Check if the result is TRUE
        match result {
            vibesql_types::SqlValue::Boolean(true) => Ok(()),
            vibesql_types::SqlValue::Boolean(false) => Err(ExecutorError::AssertionViolation {
                assertion_name: assertion_name.to_string(),
            }),
            vibesql_types::SqlValue::Null => {
                // NULL is treated as unknown, which in SQL standard means the assertion passes
                // (only FALSE triggers a violation)
                Ok(())
            }
            vibesql_types::SqlValue::Integer(0) => {
                // SQLite compatibility: 0 is FALSE
                Err(ExecutorError::AssertionViolation {
                    assertion_name: assertion_name.to_string(),
                })
            }
            vibesql_types::SqlValue::Integer(_) => {
                // SQLite compatibility: non-zero is TRUE
                Ok(())
            }
            _ => {
                // Other types - treat as TRUE if not explicitly false
                Ok(())
            }
        }
    }
}
