//! AST-level parameter binding for prepared statements
//!
//! This module provides functions to bind parameters to prepared statements
//! by replacing Placeholder expressions in the AST with Literal values,
//! avoiding the overhead of re-parsing SQL strings.
//!
//! ## Module Organization
//!
//! - `mutation` - In-place mutation-based binding (no cloning)
//! - `visitor` - AST traversal for counting and collecting placeholders
//!
//! ## Performance
//!
//! The mutation-based approach (`bind_statement_mut`) is preferred because it
//! avoids cloning the entire AST. Instead of:
//! - Clone 100 nodes, then modify 5 placeholders = O(100) allocations
//!
//! We do:
//! - Clone once (caller), modify 5 placeholders in-place = O(5) operations

mod mutation;
mod visitor;

use vibesql_ast::{Expression, Statement};
use vibesql_types::SqlValue;

// Re-export mutation-based functions
pub use mutation::bind_statement_mut;
#[cfg(test)]
use mutation::bind_statement_named_mut;

// Re-export visitor for placeholder counting
pub use visitor::visit_statement;

/// Count the number of placeholder parameters in a statement
pub fn count_placeholders(stmt: &Statement) -> usize {
    let mut count = 0;
    visit_statement(stmt, &mut |expr| {
        if matches!(expr, Expression::Placeholder(_)) {
            count += 1;
        }
    });
    count
}

/// Get the maximum numbered placeholder index in a statement.
/// Returns None if no numbered placeholders are found.
/// Returns Some(max) where max is the highest $N value (1-indexed).
#[cfg(test)]
fn max_numbered_placeholder(stmt: &Statement) -> Option<usize> {
    let mut max: Option<usize> = None;
    visit_statement(stmt, &mut |expr| {
        if let Expression::NumberedPlaceholder(n) = expr {
            max = Some(max.map_or(*n, |m| m.max(*n)));
        }
    });
    max
}

/// Collect all named placeholder names in a statement.
/// Returns a vector of unique parameter names.
#[cfg(test)]
fn collect_named_placeholders(stmt: &Statement) -> Vec<String> {
    let mut names = Vec::new();
    visit_statement(stmt, &mut |expr| {
        if let Expression::NamedPlaceholder(name) = expr {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
    });
    names
}

/// Bind parameters to a statement by replacing Placeholder expressions with Literal values
///
/// Returns a new statement with all placeholders replaced. The params slice must have
/// exactly the right number of parameters (one for each placeholder index).
///
/// This function uses mutation-based binding internally: it clones the statement once,
/// then mutates placeholders in-place, avoiding the O(n) cloning overhead of recursively
/// copying every AST node.
pub fn bind_parameters(stmt: &Statement, params: &[SqlValue]) -> Statement {
    let mut result = stmt.clone();
    bind_statement_mut(&mut result, params);
    result
}

/// Bind named parameters to a statement by replacing NamedPlaceholder expressions with Literal values
///
/// Returns a new statement with all named placeholders replaced. The params HashMap must
/// contain values for all named placeholders in the statement.
///
/// This function uses mutation-based binding internally: it clones the statement once,
/// then mutates placeholders in-place, avoiding the O(n) cloning overhead of recursively
/// copying every AST node.
#[cfg(test)]
fn bind_parameters_named(
    stmt: &Statement,
    params: &std::collections::HashMap<String, SqlValue>,
) -> Statement {
    let mut result = stmt.clone();
    bind_statement_named_mut(&mut result, params);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::InsertSource;

    #[test]
    fn test_count_placeholders_simple() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 1);
    }

    #[test]
    fn test_count_placeholders_multiple() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ? AND age > ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 3);
    }

    #[test]
    fn test_count_placeholders_none() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 0);
    }

    #[test]
    fn test_bind_parameters_select() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let bound = bind_parameters(&stmt, &[SqlValue::Integer(42)]);

        // Verify the placeholder was replaced with literal
        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { right, .. }) = &select.where_clause {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_parameters_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (?, ?)";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let params = vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))];
        let bound = bind_parameters(&stmt, &params);

        if let Statement::Insert(insert) = bound {
            if let InsertSource::Values(rows) = &insert.source {
                assert_eq!(rows[0][0], Expression::Literal(SqlValue::Integer(1)));
                assert_eq!(rows[0][1], Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
            } else {
                panic!("Expected VALUES insert source");
            }
        } else {
            panic!("Expected INSERT statement");
        }
    }

    // ============================================================================
    // Tests for numbered placeholders ($1, $2, etc.)
    // ============================================================================

    #[test]
    fn test_max_numbered_placeholder() {
        let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(max_numbered_placeholder(&stmt), Some(2));
    }

    #[test]
    fn test_max_numbered_placeholder_out_of_order() {
        // Out of order numbered placeholders
        let sql = "SELECT * FROM users WHERE id = $3 AND name = $1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(max_numbered_placeholder(&stmt), Some(3));
    }

    #[test]
    fn test_max_numbered_placeholder_none() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(max_numbered_placeholder(&stmt), None);
    }

    #[test]
    fn test_bind_numbered_placeholders_simple() {
        let sql = "SELECT * FROM users WHERE id = $1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let bound = bind_parameters(&stmt, &[SqlValue::Integer(42)]);

        // Verify the placeholder was replaced with literal
        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { right, .. }) = &select.where_clause {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_numbered_placeholders_multiple() {
        let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let params = vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))];
        let bound = bind_parameters(&stmt, &params);

        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { left, right, .. }) = &select.where_clause {
                // left is: id = $1
                if let Expression::BinaryOp { right: left_right, .. } = left.as_ref() {
                    assert_eq!(**left_right, Expression::Literal(SqlValue::Integer(1)));
                }
                // right is: name = $2
                if let Expression::BinaryOp { right: right_right, .. } = right.as_ref() {
                    assert_eq!(
                        **right_right,
                        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice")))
                    );
                }
            } else {
                panic!("Expected AND BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_numbered_placeholders_out_of_order() {
        // Test that $2 before $1 still binds correctly based on the number
        let sql = "SELECT * FROM users WHERE name = $2 AND id = $1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let params = vec![
            SqlValue::Integer(42),                // $1
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")), // $2
        ];
        let bound = bind_parameters(&stmt, &params);

        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { left, right, .. }) = &select.where_clause {
                // left is: name = $2 (should be "Bob")
                if let Expression::BinaryOp { right: left_right, .. } = left.as_ref() {
                    assert_eq!(
                        **left_right,
                        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Bob")))
                    );
                }
                // right is: id = $1 (should be 42)
                if let Expression::BinaryOp { right: right_right, .. } = right.as_ref() {
                    assert_eq!(**right_right, Expression::Literal(SqlValue::Integer(42)));
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_numbered_placeholders_reuse() {
        // Test that the same placeholder can be used multiple times
        let sql = "SELECT * FROM users WHERE id = $1 OR parent_id = $1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let bound = bind_parameters(&stmt, &[SqlValue::Integer(42)]);

        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { left, right, .. }) = &select.where_clause {
                // Both sides should have $1 bound to 42
                if let Expression::BinaryOp { right: left_right, .. } = left.as_ref() {
                    assert_eq!(**left_right, Expression::Literal(SqlValue::Integer(42)));
                }
                if let Expression::BinaryOp { right: right_right, .. } = right.as_ref() {
                    assert_eq!(**right_right, Expression::Literal(SqlValue::Integer(42)));
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    // ============================================================================
    // Tests for named placeholders (:name)
    // ============================================================================

    #[test]
    fn test_collect_named_placeholders() {
        let sql = "SELECT * FROM users WHERE id = :user_id AND name = :name";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let names = collect_named_placeholders(&stmt);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"user_id".to_string()));
        assert!(names.contains(&"name".to_string()));
    }

    #[test]
    fn test_collect_named_placeholders_with_reuse() {
        let sql = "SELECT * FROM users WHERE id = :id OR parent_id = :id";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let names = collect_named_placeholders(&stmt);
        // Should only contain one unique name
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "id");
    }

    #[test]
    fn test_bind_named_placeholders_simple() {
        let sql = "SELECT * FROM users WHERE id = :user_id";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("user_id".to_string(), SqlValue::Integer(42));

        let bound = bind_parameters_named(&stmt, &params);

        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { right, .. }) = &select.where_clause {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_named_placeholders_multiple() {
        let sql = "SELECT * FROM users WHERE id = :user_id AND name = :user_name";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("user_id".to_string(), SqlValue::Integer(1));
        params.insert("user_name".to_string(), SqlValue::Varchar(arcstr::ArcStr::from("Alice")));

        let bound = bind_parameters_named(&stmt, &params);

        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { left, right, .. }) = &select.where_clause {
                if let Expression::BinaryOp { right: left_right, .. } = left.as_ref() {
                    assert_eq!(**left_right, Expression::Literal(SqlValue::Integer(1)));
                }
                if let Expression::BinaryOp { right: right_right, .. } = right.as_ref() {
                    assert_eq!(
                        **right_right,
                        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice")))
                    );
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_named_placeholders_reuse() {
        let sql = "SELECT * FROM users WHERE id = :id OR parent_id = :id";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("id".to_string(), SqlValue::Integer(42));

        let bound = bind_parameters_named(&stmt, &params);

        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { left, right, .. }) = &select.where_clause {
                if let Expression::BinaryOp { right: left_right, .. } = left.as_ref() {
                    assert_eq!(**left_right, Expression::Literal(SqlValue::Integer(42)));
                }
                if let Expression::BinaryOp { right: right_right, .. } = right.as_ref() {
                    assert_eq!(**right_right, Expression::Literal(SqlValue::Integer(42)));
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
}
