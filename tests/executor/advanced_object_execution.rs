//! Integration tests for advanced SQL object executors.
//!
//! Tests that the wired advanced object executors (EXPLAIN, SEQUENCE, PROCEDURE,
//! FUNCTION, COLLATION, CHARACTER SET, TRANSLATION) work correctly through
//! the full parse-execute pipeline.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{advanced_objects, ExplainExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

/// Create a test database with a users table.
fn create_test_db() -> Database {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(StringValue::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(StringValue::from("Bob"))]),
    )
    .unwrap();

    db
}

// ============================================================================
// EXPLAIN Tests
// ============================================================================

#[test]
fn test_explain_select_basic() {
    let db = create_test_db();
    let sql = "EXPLAIN SELECT * FROM users";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse EXPLAIN");

    match stmt {
        vibesql_ast::Statement::Explain(explain_stmt) => {
            let result =
                ExplainExecutor::execute(&explain_stmt, &db).expect("EXPLAIN execution failed");

            let output = result.to_text();
            assert!(output.contains("Select"), "Expected 'Select' in plan output");
            assert!(
                output.contains("Seq Scan") || output.contains("Index Scan"),
                "Expected scan type in plan output"
            );
        }
        _ => panic!("Expected Explain statement"),
    }
}

#[test]
fn test_explain_select_with_where() {
    let db = create_test_db();
    let sql = "EXPLAIN SELECT * FROM users WHERE id = 1";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse EXPLAIN");

    match stmt {
        vibesql_ast::Statement::Explain(explain_stmt) => {
            let result =
                ExplainExecutor::execute(&explain_stmt, &db).expect("EXPLAIN execution failed");

            let output = result.to_text();
            assert!(output.contains("Filter"), "Expected filter info in plan");
        }
        _ => panic!("Expected Explain statement"),
    }
}

#[test]
fn test_explain_json_format() {
    let db = create_test_db();
    let sql = "EXPLAIN FORMAT JSON SELECT * FROM users";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse EXPLAIN FORMAT JSON");

    match stmt {
        vibesql_ast::Statement::Explain(explain_stmt) => {
            let result =
                ExplainExecutor::execute(&explain_stmt, &db).expect("EXPLAIN execution failed");

            let output = result.to_json();
            assert!(output.starts_with("{"), "JSON should start with '{{': {}", output);
            assert!(output.contains("operation"), "JSON should contain 'operation'");
        }
        _ => panic!("Expected Explain statement"),
    }
}

// ============================================================================
// Sequence Tests
// ============================================================================

#[test]
fn test_create_sequence_basic() {
    let mut db = Database::new();
    let sql = "CREATE SEQUENCE test_seq";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SEQUENCE");

    match stmt {
        vibesql_ast::Statement::CreateSequence(create_stmt) => {
            // Should execute without error
            advanced_objects::execute_create_sequence(&create_stmt, &mut db)
                .expect("CREATE SEQUENCE execution failed");
        }
        _ => panic!("Expected CreateSequence statement"),
    }
}

#[test]
fn test_create_sequence_with_options() {
    let mut db = Database::new();
    let sql = "CREATE SEQUENCE user_id_seq START WITH 100 INCREMENT BY 10";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SEQUENCE with options");

    match stmt {
        vibesql_ast::Statement::CreateSequence(create_stmt) => {
            advanced_objects::execute_create_sequence(&create_stmt, &mut db)
                .expect("CREATE SEQUENCE execution failed");
        }
        _ => panic!("Expected CreateSequence statement"),
    }
}

#[test]
fn test_sequence_lifecycle() {
    let mut db = Database::new();

    // Create a sequence
    let create_sql = "CREATE SEQUENCE lifecycle_seq START WITH 1";
    let create_stmt = Parser::parse_sql(create_sql).expect("Parse failed");
    if let vibesql_ast::Statement::CreateSequence(stmt) = create_stmt {
        advanced_objects::execute_create_sequence(&stmt, &mut db).expect("CREATE SEQUENCE failed");
    }

    // Alter the sequence
    let alter_sql = "ALTER SEQUENCE lifecycle_seq RESTART WITH 100";
    let alter_stmt = Parser::parse_sql(alter_sql).expect("Parse failed");
    if let vibesql_ast::Statement::AlterSequence(stmt) = alter_stmt {
        advanced_objects::execute_alter_sequence(&stmt, &mut db).expect("ALTER SEQUENCE failed");
    }

    // Drop the sequence
    let drop_sql = "DROP SEQUENCE lifecycle_seq";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Parse failed");
    if let vibesql_ast::Statement::DropSequence(stmt) = drop_stmt {
        advanced_objects::execute_drop_sequence(&stmt, &mut db).expect("DROP SEQUENCE failed");
    }
}

// ============================================================================
// Collation Tests
// ============================================================================

#[test]
fn test_collation_lifecycle() {
    let mut db = Database::new();

    // Create a collation
    let create_sql = "CREATE COLLATION test_coll";
    let create_stmt = Parser::parse_sql(create_sql).expect("Failed to parse CREATE COLLATION");
    if let vibesql_ast::Statement::CreateCollation(stmt) = create_stmt {
        advanced_objects::execute_create_collation(&stmt, &mut db)
            .expect("CREATE COLLATION failed");
    }

    // Drop the collation
    let drop_sql = "DROP COLLATION test_coll";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP COLLATION");
    if let vibesql_ast::Statement::DropCollation(stmt) = drop_stmt {
        advanced_objects::execute_drop_collation(&stmt, &mut db).expect("DROP COLLATION failed");
    }
}

// ============================================================================
// Character Set Tests
// ============================================================================

#[test]
fn test_character_set_lifecycle() {
    let mut db = Database::new();

    // Create a character set
    let create_sql = "CREATE CHARACTER SET test_charset";
    let create_stmt = Parser::parse_sql(create_sql).expect("Failed to parse CREATE CHARACTER SET");
    if let vibesql_ast::Statement::CreateCharacterSet(stmt) = create_stmt {
        advanced_objects::execute_create_character_set(&stmt, &mut db)
            .expect("CREATE CHARACTER SET failed");
    }

    // Drop the character set
    let drop_sql = "DROP CHARACTER SET test_charset";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP CHARACTER SET");
    if let vibesql_ast::Statement::DropCharacterSet(stmt) = drop_stmt {
        advanced_objects::execute_drop_character_set(&stmt, &mut db)
            .expect("DROP CHARACTER SET failed");
    }
}

// ============================================================================
// Translation Tests
// ============================================================================

#[test]
fn test_translation_lifecycle() {
    let mut db = Database::new();

    // Create a translation
    let create_sql = "CREATE TRANSLATION test_trans";
    let create_stmt = Parser::parse_sql(create_sql).expect("Failed to parse CREATE TRANSLATION");
    if let vibesql_ast::Statement::CreateTranslation(stmt) = create_stmt {
        advanced_objects::execute_create_translation(&stmt, &mut db)
            .expect("CREATE TRANSLATION failed");
    }

    // Drop the translation
    let drop_sql = "DROP TRANSLATION test_trans";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP TRANSLATION");
    if let vibesql_ast::Statement::DropTranslation(stmt) = drop_stmt {
        advanced_objects::execute_drop_translation(&stmt, &mut db)
            .expect("DROP TRANSLATION failed");
    }
}

// ============================================================================
// Procedure Tests
// ============================================================================

#[test]
fn test_procedure_lifecycle() {
    let mut db = Database::new();

    // Create a procedure
    let create_sql = "CREATE PROCEDURE test_proc() BEGIN SELECT 1; END";
    let create_stmt = Parser::parse_sql(create_sql).expect("Failed to parse CREATE PROCEDURE");
    if let vibesql_ast::Statement::CreateProcedure(stmt) = create_stmt {
        advanced_objects::execute_create_procedure(&stmt, &mut db)
            .expect("CREATE PROCEDURE failed");
    }

    // Verify procedure exists via function_exists (they share the namespace)
    assert!(db.catalog.procedure_exists("test_proc"), "Procedure should exist after creation");

    // Drop the procedure
    let drop_sql = "DROP PROCEDURE test_proc";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP PROCEDURE");
    if let vibesql_ast::Statement::DropProcedure(stmt) = drop_stmt {
        advanced_objects::execute_drop_procedure(&stmt, &mut db).expect("DROP PROCEDURE failed");
    }

    // Verify procedure was dropped
    assert!(!db.catalog.procedure_exists("test_proc"), "Procedure should not exist after drop");
}

#[test]
fn test_drop_procedure_if_exists() {
    let mut db = Database::new();

    // Drop non-existent procedure with IF EXISTS should not error
    let drop_sql = "DROP PROCEDURE IF EXISTS nonexistent_proc";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP PROCEDURE IF EXISTS");
    if let vibesql_ast::Statement::DropProcedure(stmt) = drop_stmt {
        advanced_objects::execute_drop_procedure(&stmt, &mut db)
            .expect("DROP PROCEDURE IF EXISTS should not fail for non-existent procedure");
    }
}

// ============================================================================
// Function Tests
// ============================================================================

#[test]
fn test_function_lifecycle() {
    let mut db = Database::new();

    // Create a function using SQL standard syntax
    let create_sql = "CREATE FUNCTION add_one(x INT) RETURNS INT BEGIN RETURN x + 1; END";
    let create_stmt = Parser::parse_sql(create_sql).expect("Failed to parse CREATE FUNCTION");
    if let vibesql_ast::Statement::CreateFunction(stmt) = create_stmt {
        advanced_objects::execute_create_function(&stmt, &mut db).expect("CREATE FUNCTION failed");
    }

    // Verify function exists
    assert!(db.catalog.function_exists("add_one"), "Function should exist after creation");

    // Drop the function
    let drop_sql = "DROP FUNCTION add_one";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP FUNCTION");
    if let vibesql_ast::Statement::DropFunction(stmt) = drop_stmt {
        advanced_objects::execute_drop_function(&stmt, &mut db).expect("DROP FUNCTION failed");
    }

    // Verify function was dropped
    assert!(!db.catalog.function_exists("add_one"), "Function should not exist after drop");
}

#[test]
fn test_drop_function_if_exists() {
    let mut db = Database::new();

    // Drop non-existent function with IF EXISTS should not error
    let drop_sql = "DROP FUNCTION IF EXISTS nonexistent_func";
    let drop_stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP FUNCTION IF EXISTS");
    if let vibesql_ast::Statement::DropFunction(stmt) = drop_stmt {
        advanced_objects::execute_drop_function(&stmt, &mut db)
            .expect("DROP FUNCTION IF EXISTS should not fail for non-existent function");
    }
}
