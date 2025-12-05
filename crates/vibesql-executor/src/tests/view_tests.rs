//! Tests for VIEW support (CREATE VIEW, DROP VIEW, SELECT from views)

#[cfg(test)]
mod tests {
    use crate::{advanced_objects, CreateTableExecutor, InsertExecutor, SelectExecutor};
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    fn create_test_db() -> Database {
        let mut db = Database::new();
        // Create a test table
        let create_table_sql = "CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(100), status VARCHAR(20))";
        let stmt = Parser::parse_sql(create_table_sql).expect("Failed to parse CREATE TABLE");
        if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
            CreateTableExecutor::execute(&create_stmt, &mut db).expect("Failed to create table");
        }

        // Insert some test data
        let insert_sqls = vec![
            "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 'active')",
            "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 'inactive')",
            "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 'active')",
        ];

        for sql in insert_sqls {
            let stmt = Parser::parse_sql(sql).expect("Failed to parse INSERT");
            if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
                InsertExecutor::execute(&mut db, &insert_stmt).expect("Failed to insert data");
            }
        }

        db
    }

    #[test]
    fn test_create_view() {
        let mut db = create_test_db();
        let sql =
            "CREATE VIEW active_users AS SELECT id, name, email FROM users WHERE status = 'active'";
        let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE VIEW");

        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create view");

            // Verify view was created (use uppercase since parser normalizes identifiers)
            assert!(db.catalog.get_view("ACTIVE_USERS").is_some());
        } else {
            panic!("Expected CreateView statement");
        }
    }

    #[test]
    fn test_create_or_replace_view() {
        let mut db = create_test_db();

        // Create initial view
        let sql1 = "CREATE VIEW test_view AS SELECT id, name FROM users";
        let stmt1 = Parser::parse_sql(sql1).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt1 {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create initial view");
        }

        // Replace the view with CREATE OR REPLACE
        let sql2 = "CREATE OR REPLACE VIEW test_view AS SELECT id, name, email FROM users";
        let stmt2 = Parser::parse_sql(sql2).expect("Failed to parse CREATE OR REPLACE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt2 {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create or replace view");

            // Verify view still exists (use uppercase)
            assert!(db.catalog.get_view("TEST_VIEW").is_some());
        }
    }

    #[test]
    fn test_select_from_view() {
        let mut db = create_test_db();

        // Create view
        let create_view_sql =
            "CREATE VIEW active_users AS SELECT id, name, email FROM users WHERE status = 'active'";
        let stmt = Parser::parse_sql(create_view_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create view");
        }

        // Select from view
        let select_sql = "SELECT * FROM active_users";
        let stmt = Parser::parse_sql(select_sql).expect("Failed to parse SELECT");
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute(&select_stmt).expect("Failed to select from view");

            // Should have 2 active users (Alice and Charlie)
            assert_eq!(result.len(), 2);
        }
    }

    #[test]
    fn test_select_from_view_with_where() {
        let mut db = create_test_db();

        // Create view
        let create_view_sql = "CREATE VIEW all_users AS SELECT id, name, email, status FROM users";
        let stmt = Parser::parse_sql(create_view_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create view");
        }

        // Select from view with WHERE clause
        let select_sql = "SELECT * FROM all_users WHERE status = 'active'";
        let stmt = Parser::parse_sql(select_sql).expect("Failed to parse SELECT");
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let result =
                executor.execute(&select_stmt).expect("Failed to select from view with WHERE");

            // Should have 2 active users
            assert_eq!(result.len(), 2);
        }
    }

    #[test]
    fn test_drop_view() {
        let mut db = create_test_db();

        // Create view
        let create_view_sql = "CREATE VIEW test_view AS SELECT id, name FROM users";
        let stmt = Parser::parse_sql(create_view_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create view");
        }

        assert!(db.catalog.get_view("TEST_VIEW").is_some());

        // Drop view
        let drop_view_sql = "DROP VIEW test_view";
        let stmt = Parser::parse_sql(drop_view_sql).expect("Failed to parse DROP VIEW");
        if let vibesql_ast::Statement::DropView(drop_stmt) = stmt {
            advanced_objects::execute_drop_view(&drop_stmt, &mut db).expect("Failed to drop view");
        }

        // Verify view was dropped
        assert!(db.catalog.get_view("TEST_VIEW").is_none());
    }

    #[test]
    fn test_drop_view_if_exists() {
        let mut db = create_test_db();

        // Drop non-existent view with IF EXISTS (should not error)
        let sql = "DROP VIEW IF EXISTS nonexistent_view";
        let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP VIEW IF EXISTS");
        if let vibesql_ast::Statement::DropView(drop_stmt) = stmt {
            let result = advanced_objects::execute_drop_view(&drop_stmt, &mut db);
            assert!(result.is_ok(), "DROP VIEW IF EXISTS should not error");
        }
    }

    #[test]
    fn test_create_view_with_column_list() {
        let mut db = create_test_db();

        let sql = "CREATE VIEW user_summary (user_id, full_name) AS SELECT id, name FROM users";
        let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE VIEW with column list");

        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create view");

            let view = db.catalog.get_view("USER_SUMMARY").expect("View not found");
            assert!(view.columns.is_some());
            let cols = view.columns.as_ref().unwrap();
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0], "USER_ID");
            assert_eq!(cols[1], "FULL_NAME");
        }
    }

    #[test]
    fn test_view_with_multiple_conditions() {
        let mut db = create_test_db();

        // Create view with WHERE clause
        let create_view_sql = "CREATE VIEW active_emails AS SELECT email FROM users WHERE status = 'active' AND id > 1";
        let stmt = Parser::parse_sql(create_view_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create view");
        }

        // Select from view
        let select_sql = "SELECT * FROM active_emails";
        let stmt = Parser::parse_sql(select_sql).expect("Failed to parse SELECT");
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let result = executor.execute(&select_stmt).expect("Failed to select from view");

            // Should have 1 result (Charlie, id=3, active)
            assert_eq!(result.len(), 1);
        }
    }

    #[test]
    fn test_view_on_view() {
        let mut db = create_test_db();

        // Create first view
        let view1_sql = "CREATE VIEW all_users AS SELECT id, name, email, status FROM users";
        let stmt1 = Parser::parse_sql(view1_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt1 {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create first view");
        }

        // Create second view that selects from first view
        let view2_sql =
            "CREATE VIEW active_users_v2 AS SELECT id, name FROM all_users WHERE status = 'active'";
        let stmt2 = Parser::parse_sql(view2_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt2 {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create second view");
        }

        // Select from second view
        let select_sql = "SELECT * FROM active_users_v2";
        let stmt = Parser::parse_sql(select_sql).expect("Failed to parse SELECT");
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(&db);
            let result =
                executor.execute(&select_stmt).expect("Failed to select from view on view");

            // Should have 2 active users
            assert_eq!(result.len(), 2);
        }
    }

    #[test]
    fn test_drop_view_cascade() {
        let mut db = create_test_db();

        // Create first view
        let view1_sql = "CREATE VIEW all_users AS SELECT id, name FROM users";
        let stmt1 = Parser::parse_sql(view1_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt1 {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create first view");
        }

        // Create second view that depends on first
        let view2_sql = "CREATE VIEW active_users AS SELECT * FROM all_users";
        let stmt2 = Parser::parse_sql(view2_sql).expect("Failed to parse CREATE VIEW");
        if let vibesql_ast::Statement::CreateView(view_stmt) = stmt2 {
            advanced_objects::execute_create_view(&view_stmt, &mut db)
                .expect("Failed to create second view");
        }

        // DROP CASCADE should drop all dependent views
        let drop_sql = "DROP VIEW all_users CASCADE";
        let stmt = Parser::parse_sql(drop_sql).expect("Failed to parse DROP VIEW CASCADE");
        if let vibesql_ast::Statement::DropView(drop_stmt) = stmt {
            advanced_objects::execute_drop_view(&drop_stmt, &mut db)
                .expect("Failed to drop view with CASCADE");
        }

        // Both views should be dropped
        assert!(db.catalog.get_view("ALL_USERS").is_none());
        assert!(db.catalog.get_view("ACTIVE_USERS").is_none());
    }
}
