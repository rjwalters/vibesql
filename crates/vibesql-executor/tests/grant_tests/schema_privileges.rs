//! Tests for schema-level privilege grants (USAGE, CREATE, ALL PRIVILEGES)

use vibesql_executor::GrantExecutor;
use vibesql_storage::Database;

#[test]
fn test_grant_usage_on_schema() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("app_schema".to_string()).unwrap();

    // Create the role
    db.catalog.create_role("user_role".to_string()).unwrap();

    // Grant USAGE privilege on schema
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Usage],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "app_schema".to_string(),
        for_type_name: None,
        grantees: vec!["user_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT USAGE: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("user_role", "app_schema", &vibesql_ast::PrivilegeType::Usage),
        "user_role should have USAGE privilege on app_schema schema"
    );
}

#[test]
fn test_grant_create_on_schema() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("admin_schema".to_string()).unwrap();

    // Create the role
    db.catalog.create_role("admin_role".to_string()).unwrap();

    // Grant CREATE privilege on schema
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Create],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "admin_schema".to_string(),
        for_type_name: None,
        grantees: vec!["admin_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT CREATE: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("admin_role", "admin_schema", &vibesql_ast::PrivilegeType::Create),
        "admin_role should have CREATE privilege on admin_schema schema"
    );
}

#[test]
fn test_grant_all_privileges_on_schema_expands_correctly() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("myschema".to_string()).unwrap();

    // Create the role
    db.catalog.create_role("developer".to_string()).unwrap();

    // Grant ALL PRIVILEGES on schema
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::AllPrivileges],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "myschema".to_string(),
        for_type_name: None,
        grantees: vec!["developer".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT ALL: {:?}", result.err());

    // Verify ALL PRIVILEGES expands to [Usage, Create] for schemas
    assert!(
        db.catalog.has_privilege("developer", "myschema", &vibesql_ast::PrivilegeType::Usage),
        "developer should have USAGE privilege from ALL PRIVILEGES"
    );
    assert!(
        db.catalog.has_privilege("developer", "myschema", &vibesql_ast::PrivilegeType::Create),
        "developer should have CREATE privilege from ALL PRIVILEGES"
    );

    // Verify only schema-specific privileges were granted (not table privileges)
    assert!(
        !db.catalog.has_privilege(
            "developer",
            "myschema",
            &vibesql_ast::PrivilegeType::Select(None)
        ),
        "developer should NOT have SELECT privilege (table-only)"
    );
}

#[test]
fn test_grant_schema_with_grant_option() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("power_schema".to_string()).unwrap();

    // Create the role
    db.catalog.create_role("power_user".to_string()).unwrap();

    // Grant USAGE privilege with GRANT OPTION
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Usage],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "power_schema".to_string(),
        for_type_name: None,
        grantees: vec!["power_user".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT with GRANT OPTION: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("power_user", "power_schema", &vibesql_ast::PrivilegeType::Usage),
        "power_user should have USAGE privilege"
    );

    // Verify with_grant_option flag is set
    let grants = db.catalog.get_grants_for_grantee("power_user");
    assert!(!grants.is_empty(), "Should have at least one grant");

    let grant = grants
        .iter()
        .find(|g| g.object == "power_schema" && g.privilege == vibesql_ast::PrivilegeType::Usage)
        .expect("Should find the USAGE grant on power_schema");

    assert!(grant.with_grant_option, "Grant should have with_grant_option set to true");
}

#[test]
fn test_grant_multiple_schema_privileges_to_multiple_grantees() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("shared".to_string()).unwrap();

    // Create the roles
    db.catalog.create_role("dev1".to_string()).unwrap();
    db.catalog.create_role("dev2".to_string()).unwrap();

    // Grant both USAGE and CREATE to multiple grantees
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Usage, vibesql_ast::PrivilegeType::Create],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "shared".to_string(),
        for_type_name: None,
        grantees: vec!["dev1".to_string(), "dev2".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all 4 combinations (2 privileges × 2 grantees)
    assert!(
        db.catalog.has_privilege("dev1", "shared", &vibesql_ast::PrivilegeType::Usage),
        "dev1 should have USAGE privilege"
    );
    assert!(
        db.catalog.has_privilege("dev1", "shared", &vibesql_ast::PrivilegeType::Create),
        "dev1 should have CREATE privilege"
    );
    assert!(
        db.catalog.has_privilege("dev2", "shared", &vibesql_ast::PrivilegeType::Usage),
        "dev2 should have USAGE privilege"
    );
    assert!(
        db.catalog.has_privilege("dev2", "shared", &vibesql_ast::PrivilegeType::Create),
        "dev2 should have CREATE privilege"
    );
}
