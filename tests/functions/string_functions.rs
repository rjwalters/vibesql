use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn execute_query(db: &Database, query: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(query).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// Helper to create a dummy table with one row for testing functions without FROM
fn create_dummy_table(db: &mut Database) {
    let schema = TableSchema::new(
        "DUAL".to_string(),
        vec![ColumnSchema::new("dummy".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("DUAL", Row::new(vec![SqlValue::Integer(1)])).unwrap();
}

#[test]
fn test_upper_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('hello') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("HELLO")));
}

#[test]
fn test_upper_already_uppercase() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('WORLD') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("WORLD")));
}

#[test]
fn test_upper_mixed_case() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('HeLLo WoRLd') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("HELLO WORLD")));
}

#[test]
fn test_upper_with_numbers() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('test123') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("TEST123")));
}

#[test]
fn test_lower_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('HELLO') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello")));
}

#[test]
fn test_lower_already_lowercase() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("world")));
}

#[test]
fn test_lower_mixed_case() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('HeLLo WoRLd') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello world")));
}

#[test]
fn test_lower_with_numbers() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('TEST123') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("test123")));
}

#[test]
fn test_upper_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER(NULL) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

#[test]
fn test_lower_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER(NULL) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

#[test]
fn test_upper_with_table_data() {
    let mut db = Database::new();

    // Create a test table with string data
    let schema = TableSchema::new(
        "USERS".to_string(),
        vec![ColumnSchema::new(
            "NAME".to_string(),
            DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row("USERS", Row::new(vec![SqlValue::Varchar(std::sync::Arc::from("alice"))])).unwrap();
    db.insert_row("USERS", Row::new(vec![SqlValue::Varchar(std::sync::Arc::from("bob"))])).unwrap();

    let results = execute_query(&db, "SELECT UPPER(name) AS upper_name FROM users;").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("ALICE")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(std::sync::Arc::from("BOB")));
}

#[test]
fn test_case_insensitive_function_names() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Test that function names are case-insensitive
    let results1 = execute_query(&db, "SELECT upper('test') AS result FROM dual;").unwrap();
    let results2 = execute_query(&db, "SELECT Upper('test') AS result FROM dual;").unwrap();
    let results3 = execute_query(&db, "SELECT UPPER('test') AS result FROM dual;").unwrap();

    assert_eq!(results1[0].values[0], results2[0].values[0]);
    assert_eq!(results2[0].values[0], results3[0].values[0]);
    assert_eq!(results1[0].values[0], SqlValue::Varchar(std::sync::Arc::from("TEST")));
}

// --- SUBSTRING tests ---

#[test]
fn test_substring_basic_with_length() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT SUBSTRING('hello world', 1, 5) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello")));
}

#[test]
fn test_substring_without_length() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Without length, should extract to end of string
    let results =
        execute_query(&db, "SELECT SUBSTRING('hello world', 7) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("world")));
}

#[test]
fn test_substring_middle_of_string() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT SUBSTRING('hello world', 7, 5) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("world")));
}

#[test]
fn test_substring_single_char() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT SUBSTRING('hello', 1, 1) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("h")));
}

#[test]
fn test_substring_length_exceeds_string() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Length exceeds remaining string, should return what's available
    let results =
        execute_query(&db, "SELECT SUBSTRING('hello', 3, 100) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("llo")));
}

#[test]
fn test_substring_start_exceeds_length() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Start position exceeds string length, should return empty string
    let results =
        execute_query(&db, "SELECT SUBSTRING('hello', 100, 5) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("")));
}

#[test]
fn test_substring_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT SUBSTRING(NULL, 1, 5) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

#[test]
fn test_substring_zero_length() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Length of 0 should return empty string
    let results =
        execute_query(&db, "SELECT SUBSTRING('hello', 1, 0) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("")));
}

// --- TRIM tests ---

#[test]
fn test_trim_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT TRIM('  hello  ') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello")));
}

#[test]
fn test_trim_leading_only() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT TRIM('  hello') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello")));
}

#[test]
fn test_trim_trailing_only() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT TRIM('hello  ') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello")));
}

#[test]
fn test_trim_no_spaces() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT TRIM('hello') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello")));
}

#[test]
fn test_trim_only_spaces() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT TRIM('   ') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("")));
}

#[test]
fn test_trim_preserves_internal_spaces() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT TRIM('  hello world  ') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("hello world")));
}

#[test]
fn test_trim_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT TRIM(NULL) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

// --- CHAR_LENGTH / CHARACTER_LENGTH tests ---

#[test]
fn test_char_length_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT CHAR_LENGTH('hello') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(5));
}

#[test]
fn test_character_length_alias() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // CHARACTER_LENGTH is an alias for CHAR_LENGTH
    let results =
        execute_query(&db, "SELECT CHARACTER_LENGTH('hello') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(5));
}

#[test]
fn test_char_length_empty_string() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT CHAR_LENGTH('') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(0));
}

#[test]
fn test_char_length_with_spaces() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT CHAR_LENGTH('hello world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(11));
}

#[test]
fn test_char_length_leading_trailing_spaces() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Spaces count as characters
    let results =
        execute_query(&db, "SELECT CHAR_LENGTH('  hello  ') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(9));
}

#[test]
fn test_char_length_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT CHAR_LENGTH(NULL) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}
