//! Test for issue #2998: Correlated subqueries don't resolve outer table columns
//!
//! When a correlated subquery references an outer table column (t1.b) where the
//! subquery has aliased the same table (FROM t1 AS x), the column resolution fails.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};

fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql)
        .map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("CreateTable error: {:?}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)
                .map_err(|e| format!("Insert error: {:?}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt)
                .map_err(|e| format!("Select error: {:?}", e))
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

#[test]
fn test_correlated_subquery_outer_table_reference() {
    let mut db = Database::new();

    // Create table t1 with columns a, b, c, d, e
    execute_sql(&mut db, "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
        .expect("Failed to create table");

    // Insert some data
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1,2,3,4,5)")
        .expect("Failed to insert row 1");
    execute_sql(&mut db, "INSERT INTO t1 VALUES(2,3,4,5,6)")
        .expect("Failed to insert row 2");

    // This is the failing query from issue #2998
    // The subquery references t1.b from the outer query while using alias x for its own t1
    let result = execute_sql(&mut db, "SELECT a, (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) FROM t1");

    match result {
        Ok(rows) => {
            println!("Success!");
            for row in &rows {
                println!("{:?}", row);
            }
            // For row (1,2,...): count of rows where x.b < 2 = 0 (no rows have b < 2)
            // For row (2,3,...): count of rows where x.b < 3 = 1 (row with b=2)
            assert_eq!(rows.len(), 2);
        }
        Err(e) => {
            panic!("Error: {}", e);
        }
    }
}
