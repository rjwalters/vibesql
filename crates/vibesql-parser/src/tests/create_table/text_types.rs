use super::super::*;

// ========================================================================
// Text and Character Data Types - CHAR, VARCHAR, TEXT Types
// ========================================================================

#[test]
fn test_parse_create_table_text_type() {
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, b TEXT);");
    assert!(result.is_ok(), "Should parse TEXT type");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            // TEXT should map to Varchar with no length limit
            match &create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: None } => {} // Success
                _ => {
                    panic!("Expected TEXT to map to VARCHAR, got {:?}", create.columns[0].data_type)
                }
            }

            match &create.columns[1].data_type {
                vibesql_types::DataType::Varchar { max_length: None } => {} // Success
                _ => {
                    panic!("Expected TEXT to map to VARCHAR, got {:?}", create.columns[1].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_text_varchar_compatibility() {
    // Verify TEXT and VARCHAR can be used interchangeably
    let text_result = Parser::parse_sql("CREATE TABLE t1(a TEXT);");
    let varchar_result = Parser::parse_sql("CREATE TABLE t2(a VARCHAR);");

    assert!(text_result.is_ok(), "TEXT should parse successfully");
    assert!(varchar_result.is_ok(), "VARCHAR should parse successfully");

    // Both should produce Varchar { max_length: None }
    if let Ok(vibesql_ast::Statement::CreateTable(text_table)) = text_result {
        if let Ok(vibesql_ast::Statement::CreateTable(varchar_table)) = varchar_result {
            assert_eq!(
                text_table.columns[0].data_type, varchar_table.columns[0].data_type,
                "TEXT and VARCHAR should be equivalent types"
            );
        }
    }
}

#[test]
fn test_parse_create_table_all_phase2_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE comprehensive (
            tiny SMALLINT,
            normal INTEGER,
            huge BIGINT,
            approximate FLOAT,
            precise_float REAL,
            very_precise DOUBLE PRECISION,
            money NUMERIC(10, 2),
            fixed_char CHAR(10),
            var_char VARCHAR(255),
            flag BOOLEAN,
            birth DATE,
            meeting TIME WITH TIME ZONE,
            created TIMESTAMP,
            age INTERVAL YEAR TO MONTH
        );",
    );
    assert!(result.is_ok(), "Should parse all Phase 2 types together");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 14, "Should have 14 columns");
            assert_eq!(create.table_name, "comprehensive");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
