/// Tests for set operations (UNION, INTERSECT, EXCEPT) associativity
///
/// These tests verify that set operations evaluate left-to-right, not right-to-left.
/// For example: (A EXCEPT B) EXCEPT C, not A EXCEPT (B EXCEPT C)
#[cfg(test)]
mod tests {
    use crate::SelectExecutor;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    #[test]
    fn test_except_left_to_right_evaluation() {
        let mut db = Database::new();

        // Create test tables
        let create_t1 = "CREATE TABLE t1 (a INT)";
        let create_t2 = "CREATE TABLE t2 (a INT)";
        let create_t3 = "CREATE TABLE t3 (a INT)";

        for sql in &[create_t1, create_t2, create_t3] {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
                crate::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
            }
        }

        // Insert data
        let insert_t1 = "INSERT INTO t1 VALUES (1), (2), (3)";
        let insert_t2 = "INSERT INTO t2 VALUES (2), (3), (4)";
        let insert_t3 = "INSERT INTO t3 VALUES (3), (4), (5)";

        for sql in &[insert_t1, insert_t2, insert_t3] {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
                crate::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
        }

        // Test: (t1 EXCEPT t2) EXCEPT t3
        // t1 EXCEPT t2 = {1} (since 2, 3 are in t2)
        // {1} EXCEPT t3 = {1} (since 1 is not in t3)
        // Expected result: [1]
        let query = "SELECT a FROM t1 EXCEPT SELECT a FROM t2 EXCEPT SELECT a FROM t3";
        let stmt = Parser::parse_sql(query).unwrap();

        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            assert_eq!(rows.len(), 1, "Should have exactly 1 row");
            assert_eq!(rows[0].values.len(), 1, "Should have exactly 1 column");

            // Extract the value
            if let vibesql_types::SqlValue::Integer(val) = rows[0].values[0] {
                assert_eq!(val, 1, "Result should be 1");
            } else {
                panic!("Expected Integer value");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_union_except_left_to_right() {
        let mut db = Database::new();

        // Create test tables
        let create_t1 = "CREATE TABLE t1 (a INT)";
        let create_t2 = "CREATE TABLE t2 (a INT)";
        let create_t3 = "CREATE TABLE t3 (a INT)";

        for sql in &[create_t1, create_t2, create_t3] {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
                crate::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
            }
        }

        // Insert data
        let insert_t1 = "INSERT INTO t1 VALUES (1), (2), (3)";
        let insert_t2 = "INSERT INTO t2 VALUES (2), (3), (4)";
        let insert_t3 = "INSERT INTO t3 VALUES (3), (4), (5)";

        for sql in &[insert_t1, insert_t2, insert_t3] {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
                crate::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
        }

        // Test: (t1 UNION t2) EXCEPT t3
        // t1 UNION t2 = {1, 2, 3, 4}
        // {1, 2, 3, 4} EXCEPT t3 = {1, 2} (remove 3, 4, 5)
        // Expected result: [1, 2]
        let query = "SELECT a FROM t1 UNION SELECT a FROM t2 EXCEPT SELECT a FROM t3";
        let stmt = Parser::parse_sql(query).unwrap();

        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            assert_eq!(rows.len(), 2, "Should have exactly 2 rows");

            // Extract values and sort them for comparison
            let mut values: Vec<i64> = rows
                .iter()
                .map(|row| {
                    if let vibesql_types::SqlValue::Integer(val) = row.values[0] {
                        val
                    } else {
                        panic!("Expected Integer value");
                    }
                })
                .collect();
            values.sort();

            assert_eq!(values, vec![1, 2], "Result should be [1, 2]");
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_multiple_except_operations() {
        let mut db = Database::new();

        // Create test tables
        let sqls = vec![
            "CREATE TABLE t1 (a INT)",
            "CREATE TABLE t2 (a INT)",
            "CREATE TABLE t3 (a INT)",
            "CREATE TABLE t4 (a INT)",
            "INSERT INTO t1 VALUES (1), (2), (3)",
            "INSERT INTO t2 VALUES (2), (3), (4)",
            "INSERT INTO t3 VALUES (3), (4), (5)",
            "INSERT INTO t4 VALUES (1), (5)",
        ];

        for sql in sqls {
            let stmt = Parser::parse_sql(sql).unwrap();
            match stmt {
                vibesql_ast::Statement::CreateTable(create_stmt) => {
                    crate::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
                }
                vibesql_ast::Statement::Insert(insert_stmt) => {
                    crate::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
                }
                _ => {}
            }
        }

        // Test: (((t1 UNION t2) EXCEPT t3) EXCEPT t4)
        // t1 UNION t2 = {1, 2, 3, 4}
        // {1, 2, 3, 4} EXCEPT t3 = {1, 2}
        // {1, 2} EXCEPT t4 = {2} (remove 1)
        let query = "SELECT a FROM t1 UNION SELECT a FROM t2 EXCEPT SELECT a FROM t3 EXCEPT SELECT a FROM t4";
        let stmt = Parser::parse_sql(query).unwrap();

        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            assert_eq!(rows.len(), 1, "Should have exactly 1 row");

            if let vibesql_types::SqlValue::Integer(val) = rows[0].values[0] {
                assert_eq!(val, 2, "Result should be 2");
            } else {
                panic!("Expected Integer value");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
}
