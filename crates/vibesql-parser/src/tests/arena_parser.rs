//! Tests for arena-allocated parser.
//!
//! Note: The lexer normalizes identifiers to uppercase, so tests use uppercase
//! strings for identifier comparisons.

use bumpalo::Bump;
use vibesql_ast::arena::Converter;
use vibesql_ast::{DeleteStmt, InsertSource, InsertStmt, UpdateStmt, WhereClause};

use crate::arena_parser::ArenaParser;

// ============================================================================
// DELETE Tests
// ============================================================================

#[test]
fn test_arena_parse_delete_simple() {
    let arena = Bump::new();
    let sql = "DELETE FROM users";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "USERS");
    assert!(!stmt.only);
    assert!(stmt.where_clause.is_none());
}

#[test]
fn test_arena_parse_delete_with_where() {
    let arena = Bump::new();
    let sql = "DELETE FROM users WHERE id = 1";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "USERS");
    assert!(stmt.where_clause.is_some());
}

#[test]
fn test_arena_parse_delete_with_only() {
    let arena = Bump::new();
    let sql = "DELETE FROM ONLY users WHERE id = 1";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert!(stmt.only);
    assert_eq!(interner.resolve(stmt.table_name), "USERS");
}

#[test]
fn test_arena_parse_delete_convert_to_standard() {
    let arena = Bump::new();
    let sql = "DELETE FROM users WHERE id = 1";
    let (arena_stmt, interner) = ArenaParser::parse_delete_with_interner(sql, &arena).unwrap();

    // Convert to standard AST using the Converter
    let converter = Converter::new(&interner);
    let std_stmt: DeleteStmt = converter.convert_delete(arena_stmt);
    assert_eq!(std_stmt.table_name, "USERS");
    assert!(matches!(std_stmt.where_clause, Some(WhereClause::Condition(_))));
}

// ============================================================================
// UPDATE Tests
// ============================================================================

#[test]
fn test_arena_parse_update_simple() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John'";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "USERS");
    assert_eq!(stmt.assignments.len(), 1);
    assert_eq!(interner.resolve(stmt.assignments[0].column), "NAME");
    assert!(stmt.where_clause.is_none());
}

#[test]
fn test_arena_parse_update_multiple_assignments() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John', age = 30";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(stmt.assignments.len(), 2);
    assert_eq!(interner.resolve(stmt.assignments[0].column), "NAME");
    assert_eq!(interner.resolve(stmt.assignments[1].column), "AGE");
}

#[test]
fn test_arena_parse_update_with_where() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John' WHERE id = 1";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(stmt.where_clause.is_some());
}

#[test]
fn test_arena_parse_update_convert_to_standard() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = 'John', age = 30 WHERE id = 1";
    let (arena_stmt, interner) = ArenaParser::parse_update_with_interner(sql, &arena).unwrap();

    // Convert to standard AST using the Converter
    let converter = Converter::new(&interner);
    let std_stmt: UpdateStmt = converter.convert_update(arena_stmt);
    assert_eq!(std_stmt.table_name, "USERS");
    assert_eq!(std_stmt.assignments.len(), 2);
    assert!(matches!(std_stmt.where_clause, Some(WhereClause::Condition(_))));
}

// ============================================================================
// INSERT Tests
// ============================================================================

#[test]
fn test_arena_parse_insert_simple() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES ('John', 30)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, interner) = result.unwrap();
    assert_eq!(interner.resolve(stmt.table_name), "USERS");
    assert_eq!(stmt.columns.len(), 2);
    assert_eq!(interner.resolve(stmt.columns[0]), "NAME");
    assert_eq!(interner.resolve(stmt.columns[1]), "AGE");

    match &stmt.source {
        vibesql_ast::arena::InsertSource::Values(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
        }
        _ => panic!("Expected Values source"),
    }
}

#[test]
fn test_arena_parse_insert_multiple_rows() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES ('John', 30), ('Jane', 25)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    match &stmt.source {
        vibesql_ast::arena::InsertSource::Values(rows) => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("Expected Values source"),
    }
}

#[test]
fn test_arena_parse_insert_no_columns() {
    let arena = Bump::new();
    let sql = "INSERT INTO users VALUES ('John', 30)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert_eq!(stmt.columns.len(), 0);
}

#[test]
fn test_arena_parse_insert_or_replace() {
    let arena = Bump::new();
    let sql = "INSERT OR REPLACE INTO users (name) VALUES ('John')";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(matches!(stmt.conflict_clause, Some(vibesql_ast::arena::ConflictClause::Replace)));
}

#[test]
fn test_arena_parse_insert_or_ignore() {
    let arena = Bump::new();
    let sql = "INSERT OR IGNORE INTO users (name) VALUES ('John')";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(matches!(stmt.conflict_clause, Some(vibesql_ast::arena::ConflictClause::Ignore)));
}

#[test]
fn test_arena_parse_replace() {
    let arena = Bump::new();
    let sql = "REPLACE INTO users (name) VALUES ('John')";
    let result = ArenaParser::parse_replace_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(matches!(stmt.conflict_clause, Some(vibesql_ast::arena::ConflictClause::Replace)));
}

#[test]
fn test_arena_parse_insert_with_select() {
    let arena = Bump::new();
    let sql = "INSERT INTO users_backup (name, age) SELECT name, age FROM users";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    match &stmt.source {
        vibesql_ast::arena::InsertSource::Select(query) => {
            assert!(query.from.is_some());
        }
        _ => panic!("Expected Select source"),
    }
}

#[test]
fn test_arena_parse_insert_convert_to_standard() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES ('John', 30)";
    let (arena_stmt, interner) = ArenaParser::parse_insert_with_interner(sql, &arena).unwrap();

    // Convert to standard AST using the Converter
    let converter = Converter::new(&interner);
    let std_stmt: InsertStmt = converter.convert_insert(arena_stmt);
    assert_eq!(std_stmt.table_name, "USERS");
    assert_eq!(std_stmt.columns.len(), 2);
    assert!(matches!(std_stmt.source, InsertSource::Values(_)));
}

// ============================================================================
// Placeholder Tests
// ============================================================================

#[test]
fn test_arena_parse_delete_with_placeholder() {
    let arena = Bump::new();
    let sql = "DELETE FROM users WHERE id = ?";
    let result = ArenaParser::parse_delete_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    assert!(stmt.where_clause.is_some());
    if let Some(vibesql_ast::arena::WhereClause::Condition(
        vibesql_ast::arena::Expression::BinaryOp { right, .. },
    )) = &stmt.where_clause
    {
        // The right side should be a placeholder
        assert!(matches!(right, vibesql_ast::arena::Expression::Placeholder(_)));
    }
}

#[test]
fn test_arena_parse_update_with_placeholder() {
    let arena = Bump::new();
    let sql = "UPDATE users SET name = ? WHERE id = ?";
    let result = ArenaParser::parse_update_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    // First placeholder in SET
    assert!(matches!(stmt.assignments[0].value, vibesql_ast::arena::Expression::Placeholder(0)));
}

#[test]
fn test_arena_parse_insert_with_placeholder() {
    let arena = Bump::new();
    let sql = "INSERT INTO users (name, age) VALUES (?, ?)";
    let result = ArenaParser::parse_insert_with_interner(sql, &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (stmt, _interner) = result.unwrap();
    match &stmt.source {
        vibesql_ast::arena::InsertSource::Values(rows) => {
            assert!(matches!(rows[0][0], vibesql_ast::arena::Expression::Placeholder(0)));
            assert!(matches!(rows[0][1], vibesql_ast::arena::Expression::Placeholder(1)));
        }
        _ => panic!("Expected Values source"),
    }
}
