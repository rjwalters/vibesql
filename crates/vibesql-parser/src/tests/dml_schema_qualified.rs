//! Tests for schema-qualified table names in DML statements

use crate::Parser;
use vibesql_ast::Statement;

#[test]
fn test_insert_schema_qualified_quoted() {
    let sql = r#"INSERT INTO "mySchema"."users" VALUES (1)"#;
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "mySchema.users");
            assert!(insert.quoted);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_insert_schema_qualified_unquoted() {
    let sql = r#"INSERT INTO myschema.users VALUES (1)"#;
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "myschema.users");
            assert!(!insert.quoted);
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_update_schema_qualified_quoted() {
    let sql = r#"UPDATE "mySchema"."users" SET id = 2"#;
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Update(update) => {
            assert_eq!(update.table_name, "mySchema.users");
            assert!(update.quoted);
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_delete_schema_qualified_quoted() {
    let sql = r#"DELETE FROM "mySchema"."users""#;
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Delete(delete) => {
            assert_eq!(delete.table_name, "mySchema.users");
            assert!(delete.quoted);
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_insert_mixed_quoted_unquoted() {
    // Quoted schema, unquoted table
    let sql = r#"INSERT INTO "mySchema".users VALUES (1)"#;
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "mySchema.users");
            assert!(insert.quoted); // quoted if any part is quoted
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_replace_schema_qualified() {
    let sql = r#"REPLACE INTO "mySchema"."users" VALUES (1)"#;
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "mySchema.users");
            assert!(insert.quoted);
        }
        _ => panic!("Expected INSERT statement"),
    }
}
