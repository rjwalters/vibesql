//! Error display and conversion tests
//!
//! Tests for error message formatting and error type conversions between
//! catalog, executor, and storage layers.

use std::error::Error;

use vibesql_catalog::CatalogError;
use vibesql_executor::ExecutorError;

#[test]
fn test_catalog_error_display_formats() {
    // Test all CatalogError variants display correctly

    let errors = vec![
        (
            CatalogError::TableAlreadyExists("mytable".to_string()),
            vec!["Table", "mytable", "already exists"],
        ),
        (
            CatalogError::TableNotFound { table_name: "missing".to_string() },
            vec!["Table", "missing", "not found"],
        ),
        (
            CatalogError::ColumnAlreadyExists("mycolumn".to_string()),
            vec!["Column", "mycolumn", "already exists"],
        ),
        (
            CatalogError::ColumnNotFound {
                column_name: "badcol".to_string(),
                table_name: "test_table".to_string(),
            },
            vec!["Column", "badcol", "not found"],
        ),
        (
            CatalogError::SchemaAlreadyExists("myschema".to_string()),
            vec!["Schema", "myschema", "already exists"],
        ),
        (
            CatalogError::SchemaNotFound("noschema".to_string()),
            vec!["Schema", "noschema", "not found"],
        ),
        (
            CatalogError::SchemaNotEmpty("fullschema".to_string()),
            vec!["Schema", "fullschema", "not empty"],
        ),
        (
            CatalogError::RoleAlreadyExists("admin".to_string()),
            vec!["Role", "admin", "already exists"],
        ),
        (CatalogError::RoleNotFound("norole".to_string()), vec!["Role", "norole", "not found"]),
        (
            CatalogError::DomainAlreadyExists("mydomain".to_string()),
            vec!["Domain", "mydomain", "already exists"],
        ),
        (
            CatalogError::DomainNotFound("nodomain".to_string()),
            vec!["Domain", "nodomain", "not found"],
        ),
        (
            CatalogError::SequenceAlreadyExists("myseq".to_string()),
            vec!["Sequence", "myseq", "already exists"],
        ),
        (
            CatalogError::SequenceNotFound("noseq".to_string()),
            vec!["Sequence", "noseq", "not found"],
        ),
        (
            CatalogError::TypeAlreadyExists("mytype".to_string()),
            vec!["Type", "mytype", "already exists"],
        ),
        (CatalogError::TypeNotFound("notype".to_string()), vec!["Type", "notype", "not found"]),
        (CatalogError::TypeInUse("busytype".to_string()), vec!["Type", "busytype", "still in use"]),
        (
            CatalogError::CollationAlreadyExists("mycoll".to_string()),
            vec!["Collation", "mycoll", "already exists"],
        ),
        (
            CatalogError::CollationNotFound("nocoll".to_string()),
            vec!["Collation", "nocoll", "not found"],
        ),
        (
            CatalogError::CharacterSetAlreadyExists("mycharset".to_string()),
            vec!["Character set", "mycharset", "already exists"],
        ),
        (
            CatalogError::CharacterSetNotFound("nocharset".to_string()),
            vec!["Character set", "nocharset", "not found"],
        ),
        (
            CatalogError::TranslationAlreadyExists("mytrans".to_string()),
            vec!["Translation", "mytrans", "already exists"],
        ),
        (
            CatalogError::TranslationNotFound("notrans".to_string()),
            vec!["Translation", "notrans", "not found"],
        ),
        (
            CatalogError::ViewAlreadyExists("myview".to_string()),
            vec!["View", "myview", "already exists"],
        ),
        (CatalogError::ViewNotFound("noview".to_string()), vec!["View", "noview", "not found"]),
    ];

    for (error, expected_parts) in errors {
        let error_msg = format!("{}", error);
        for part in expected_parts {
            assert!(
                error_msg.contains(part),
                "Error message '{}' should contain '{}'",
                error_msg,
                part
            );
        }
    }
}

#[test]
fn test_catalog_to_executor_error_conversion() {
    // Test From<CatalogError> for ExecutorError conversion

    let catalog_err = CatalogError::TableNotFound { table_name: "test".to_string() };
    let executor_err: ExecutorError = catalog_err.into();

    match executor_err {
        ExecutorError::TableNotFound(name) => {
            assert_eq!(name, "test");
        }
        other => panic!("Expected TableNotFound, got: {:?}", other),
    }

    // Test advanced SQL:1999 objects conversion
    let catalog_err = CatalogError::DomainNotFound("mydomain".to_string());
    let executor_err: ExecutorError = catalog_err.into();

    match executor_err {
        ExecutorError::Other(msg) => {
            assert!(msg.contains("Domain"));
            assert!(msg.contains("mydomain"));
            assert!(msg.contains("not found"));
        }
        other => panic!("Expected Other error for DomainNotFound, got: {:?}", other),
    }
}

#[test]
fn test_error_implements_std_error_trait() {
    // Verify all error types implement std::error::Error trait
    let catalog_err: Box<dyn Error> =
        Box::new(CatalogError::TableNotFound { table_name: "test".to_string() });
    assert!(catalog_err.to_string().contains("not found"));

    let executor_err: Box<dyn Error> = Box::new(ExecutorError::DivisionByZero);
    assert!(executor_err.to_string().contains("Division by zero"));

    let storage_err: Box<dyn Error> = Box::new(vibesql_storage::StorageError::RowNotFound);
    assert!(storage_err.to_string().contains("Row not found"));
}
