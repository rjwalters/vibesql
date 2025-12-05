#[cfg(test)]
mod catalog_tests {
    use crate::{Catalog, CatalogError, ColumnSchema, TableSchema};

    #[test]
    fn test_column_schema_creation() {
        let col = ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, vibesql_types::DataType::Integer);
        assert!(!col.nullable);
    }

    #[test]
    fn test_table_schema_creation() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);
        assert_eq!(schema.name, "users");
        assert_eq!(schema.column_count(), 2);
    }

    #[test]
    fn test_table_schema_get_column() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        let id_col = schema.get_column("id");
        assert!(id_col.is_some());
        assert_eq!(id_col.unwrap().data_type, vibesql_types::DataType::Integer);

        let missing_col = schema.get_column("missing");
        assert!(missing_col.is_none());
    }

    #[test]
    fn test_table_schema_get_column_index() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        assert_eq!(schema.get_column_index("id"), Some(0));
        assert_eq!(schema.get_column_index("name"), Some(1));
        assert_eq!(schema.get_column_index("missing"), None);
    }

    #[test]
    fn test_catalog_create_table() {
        let mut catalog = Catalog::new();
        let columns =
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        let result = catalog.create_table(schema);
        assert!(result.is_ok());
        assert!(catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_create_duplicate_table() {
        let mut catalog = Catalog::new();
        let columns =
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)];
        let schema1 = TableSchema::new("users".to_string(), columns.clone());
        let schema2 = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema1).unwrap();
        let result = catalog.create_table(schema2);

        assert!(result.is_err());
        match result.unwrap_err() {
            CatalogError::TableAlreadyExists(name) => assert_eq!(name, "users"),
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_catalog_get_table() {
        let mut catalog = Catalog::new();
        let columns =
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema).unwrap();

        let retrieved = catalog.get_table("users");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "users");

        let missing = catalog.get_table("missing");
        assert!(missing.is_none());
    }

    #[test]
    fn test_catalog_drop_table() {
        let mut catalog = Catalog::new();
        let columns =
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema).unwrap();
        assert!(catalog.table_exists("users"));

        let result = catalog.drop_table("users");
        assert!(result.is_ok());
        assert!(!catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_drop_nonexistent_table() {
        let mut catalog = Catalog::new();
        let result = catalog.drop_table("missing");

        assert!(result.is_err());
        match result.unwrap_err() {
            CatalogError::TableNotFound { table_name } => assert_eq!(table_name, "missing"),
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_catalog_list_tables() {
        let mut catalog = Catalog::new();

        let schema1 = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let schema2 = TableSchema::new(
            "orders".to_string(),
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
        );

        catalog.create_table(schema1).unwrap();
        catalog.create_table(schema2).unwrap();

        let mut tables = catalog.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["orders".to_string(), "users".to_string()]);
    }

    #[test]
    fn test_error_display_table_already_exists() {
        let error = CatalogError::TableAlreadyExists("customers".to_string());
        let error_msg = format!("{}", error);
        assert_eq!(error_msg, "Table 'customers' already exists");
    }

    #[test]
    fn test_error_display_table_not_found() {
        let error = CatalogError::TableNotFound { table_name: "products".to_string() };
        let error_msg = format!("{}", error);
        assert_eq!(error_msg, "Table 'products' not found");
    }

    #[test]
    fn test_catalog_default() {
        let catalog = Catalog::default();
        assert_eq!(catalog.list_tables().len(), 0);
        assert!(!catalog.table_exists("any_table"));
    }

    #[test]
    fn test_column_schema_clone() {
        let col1 = ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false);
        let col2 = col1.clone();
        assert_eq!(col1, col2);
        assert_eq!(col1.name, col2.name);
        assert_eq!(col1.data_type, col2.data_type);
        assert_eq!(col1.nullable, col2.nullable);
    }

    #[test]
    fn test_table_schema_clone() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(255) },
                true,
            ),
        ];
        let schema1 = TableSchema::new("accounts".to_string(), columns);
        let schema2 = schema1.clone();
        assert_eq!(schema1, schema2);
        assert_eq!(schema1.name, schema2.name);
        assert_eq!(schema1.columns.len(), schema2.columns.len());
    }

    #[test]
    fn test_catalog_clone() {
        let mut catalog1 = Catalog::new();
        let columns =
            vec![ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);
        catalog1.create_table(schema).unwrap();

        let catalog2 = catalog1.clone();
        assert!(catalog2.table_exists("users"));
        assert_eq!(catalog1.list_tables(), catalog2.list_tables());
    }

    #[test]
    fn test_remove_column_removes_foreign_keys() {
        use crate::foreign_key::{ForeignKeyConstraint, ReferentialAction};

        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];

        // Create a foreign key on user_id
        let fk = ForeignKeyConstraint {
            name: Some("fk_user".to_string()),
            column_names: vec!["user_id".to_string()],
            column_indices: vec![1],
            parent_table: "users".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        };

        let mut schema = TableSchema::with_foreign_keys("orders".to_string(), columns, vec![fk]);

        // Verify foreign key exists
        assert_eq!(schema.foreign_keys.len(), 1);

        // Remove the user_id column (index 1)
        let result = schema.remove_column(1);
        assert!(result.is_ok());

        // Verify foreign key was removed
        assert_eq!(schema.foreign_keys.len(), 0);
    }

    #[test]
    fn test_remove_column_keeps_unrelated_foreign_keys() {
        use crate::foreign_key::{ForeignKeyConstraint, ReferentialAction};

        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];

        // Create a foreign key on user_id
        let fk = ForeignKeyConstraint {
            name: Some("fk_user".to_string()),
            column_names: vec!["user_id".to_string()],
            column_indices: vec![1],
            parent_table: "users".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        };

        let mut schema = TableSchema::with_foreign_keys("orders".to_string(), columns, vec![fk]);

        // Verify foreign key exists
        assert_eq!(schema.foreign_keys.len(), 1);

        // Remove the name column (index 2) - not referenced by FK
        let result = schema.remove_column(2);
        assert!(result.is_ok());

        // Verify foreign key still exists
        assert_eq!(schema.foreign_keys.len(), 1);
    }

    #[test]
    fn test_remove_column_removes_check_constraints() {
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;

        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("age".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];

        // Create a check constraint: age >= 18
        let check_expr = Expression::BinaryOp {
            op: BinaryOperator::GreaterThanOrEqual,
            left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(18))),
        };

        let check_constraints = vec![("age_check".to_string(), check_expr)];

        let mut schema = TableSchema::with_all_constraint_types(
            "users".to_string(),
            columns,
            None,
            vec![],
            check_constraints,
            vec![],
        );

        // Verify check constraint exists
        assert_eq!(schema.check_constraints.len(), 1);

        // Remove the age column (index 1)
        let result = schema.remove_column(1);
        assert!(result.is_ok());

        // Verify check constraint was removed
        assert_eq!(schema.check_constraints.len(), 0);
    }

    #[test]
    fn test_remove_column_keeps_unrelated_check_constraints() {
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;

        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("age".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];

        // Create a check constraint: age >= 18
        let check_expr = Expression::BinaryOp {
            op: BinaryOperator::GreaterThanOrEqual,
            left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(18))),
        };

        let check_constraints = vec![("age_check".to_string(), check_expr)];

        let mut schema = TableSchema::with_all_constraint_types(
            "users".to_string(),
            columns,
            None,
            vec![],
            check_constraints,
            vec![],
        );

        // Verify check constraint exists
        assert_eq!(schema.check_constraints.len(), 1);

        // Remove the name column (index 2) - not referenced by check constraint
        let result = schema.remove_column(2);
        assert!(result.is_ok());

        // Verify check constraint still exists
        assert_eq!(schema.check_constraints.len(), 1);
    }

    #[test]
    fn test_remove_column_handles_complex_check_constraints() {
        use vibesql_ast::{BinaryOperator, Expression};

        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("min_value".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new("max_value".to_string(), vibesql_types::DataType::Integer, true),
        ];

        // Create a check constraint: min_value < max_value
        let check_expr = Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::ColumnRef { table: None, column: "min_value".to_string() }),
            right: Box::new(Expression::ColumnRef { table: None, column: "max_value".to_string() }),
        };

        let check_constraints = vec![("range_check".to_string(), check_expr)];

        let mut schema = TableSchema::with_all_constraint_types(
            "ranges".to_string(),
            columns,
            None,
            vec![],
            check_constraints,
            vec![],
        );

        // Verify check constraint exists
        assert_eq!(schema.check_constraints.len(), 1);

        // Remove the min_value column (index 1) - referenced by check constraint
        let result = schema.remove_column(1);
        assert!(result.is_ok());

        // Verify check constraint was removed
        assert_eq!(schema.check_constraints.len(), 0);
    }

    #[test]
    fn test_remove_column_handles_all_constraints() {
        use crate::foreign_key::{ForeignKeyConstraint, ReferentialAction};
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;

        let columns = vec![
            ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new("age".to_string(), vibesql_types::DataType::Integer, true),
            ColumnSchema::new(
                "email".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(255) },
                true,
            ),
        ];

        // Create a foreign key on user_id
        let fk = ForeignKeyConstraint {
            name: Some("fk_user".to_string()),
            column_names: vec!["user_id".to_string()],
            column_indices: vec![1],
            parent_table: "users".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        };

        // Create a check constraint: age >= 18
        let check_expr = Expression::BinaryOp {
            op: BinaryOperator::GreaterThanOrEqual,
            left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(18))),
        };

        let mut schema = TableSchema::with_all_constraint_types(
            "profiles".to_string(),
            columns,
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
            vec![("age_check".to_string(), check_expr)],
            vec![fk],
        );

        // Verify initial state
        assert_eq!(schema.foreign_keys.len(), 1);
        assert_eq!(schema.check_constraints.len(), 1);
        assert_eq!(schema.unique_constraints.len(), 1);
        assert!(schema.primary_key.is_some());

        // Remove user_id column (index 1) - has FK but no check constraint
        let result = schema.remove_column(1);
        assert!(result.is_ok());

        // Verify FK was removed but check constraint remains
        assert_eq!(schema.foreign_keys.len(), 0);
        assert_eq!(schema.check_constraints.len(), 1);

        // Remove age column (index 1 again, since user_id was removed) - has check constraint
        let result = schema.remove_column(1);
        assert!(result.is_ok());

        // Verify check constraint was removed
        assert_eq!(schema.check_constraints.len(), 0);

        // Verify unique constraint on email still exists
        assert_eq!(schema.unique_constraints.len(), 1);
    }
}
