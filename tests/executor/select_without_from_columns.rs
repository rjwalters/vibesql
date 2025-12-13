//! Test SELECT without FROM clause - with column names

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_select_1_with_columns() {
    let db = Database::new();

    // Parse SELECT 1;
    let stmt = Parser::parse_sql("SELECT 1;").expect("Failed to parse");

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute_with_columns(&select_stmt).expect("Failed to execute");

            eprintln!("Columns: {:?}", result.columns);
            eprintln!("Rows: {:?}", result.rows);

            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(1));

            // Check that the column name is the literal value, not the debug format
            assert_eq!(result.columns.len(), 1);
            assert_eq!(result.columns[0], "1", "Column name should be '1', not 'Integer(1)'");
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_1_with_alias() {
    let db = Database::new();

    // Parse SELECT 1 AS result;
    let stmt = Parser::parse_sql("SELECT 1 AS result;").expect("Failed to parse");

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute_with_columns(&select_stmt).expect("Failed to execute");

            eprintln!("Columns: {:?}", result.columns);
            eprintln!("Rows: {:?}", result.rows);

            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(1));

            // Check that the alias is used (parser preserves case for unquoted identifiers)
            assert_eq!(result.columns.len(), 1);
            assert_eq!(result.columns[0], "result");
        }
        _ => panic!("Expected SELECT statement"),
    }
}
