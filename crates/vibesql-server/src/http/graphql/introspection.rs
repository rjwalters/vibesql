//! GraphQL schema introspection support
//!
//! Handles __schema and __type queries for GraphQL introspection.

use std::sync::Arc;

use serde_json::{json, Value as JsonValue};
use vibesql_storage::Database;

/// Try to handle introspection queries (__schema, __type)
/// Returns Some(result) if this was an introspection query, None otherwise
pub fn try_introspection_query(db: &Arc<Database>, query: &str) -> Option<JsonValue> {
    let query = query.trim();

    // Check for __schema query
    if query.contains("__schema") {
        return try_schema_query(db, query);
    }

    // Check for __type query
    if query.contains("__type") {
        return try_type_query(db, query);
    }

    None
}

/// Handle __schema introspection query
fn try_schema_query(db: &Arc<Database>, _query: &str) -> Option<JsonValue> {
    let table_names = db.list_tables();
    let mut types = Vec::new();

    // Add built-in scalar types
    for scalar in &["String", "Int", "Float", "Boolean", "ID"] {
        types.push(json!({
            "kind": "SCALAR",
            "name": scalar,
            "fields": null,
            "possibleTypes": null,
        }));
    }

    // Add table types
    for table_name in &table_names {
        let fields = get_table_fields(db, table_name);
        types.push(json!({
            "kind": "OBJECT",
            "name": table_name,
            "fields": fields,
            "possibleTypes": null,
        }));
    }

    // Add __Schema and __Type types
    types.push(json!({
        "kind": "OBJECT",
        "name": "__Schema",
        "fields": vec![
            json!({"name": "types", "type": "[__Type!]!"}),
            json!({"name": "queryType", "type": "__Type"}),
        ],
        "possibleTypes": null,
    }));

    types.push(json!({
        "kind": "OBJECT",
        "name": "__Type",
        "fields": vec![
            json!({"name": "name", "type": "String"}),
            json!({"name": "kind", "type": "String"}),
            json!({"name": "fields", "type": "[__Field!]"}),
        ],
        "possibleTypes": null,
    }));

    types.push(json!({
        "kind": "OBJECT",
        "name": "__Field",
        "fields": vec![
            json!({"name": "name", "type": "String!"}),
            json!({"name": "type", "type": "__Type!"}),
        ],
        "possibleTypes": null,
    }));

    Some(json!({
        "__schema": {
            "types": types,
            "queryType": {
                "name": "Query"
            }
        }
    }))
}

/// Handle __type(name: "...") introspection query
fn try_type_query(db: &Arc<Database>, query: &str) -> Option<JsonValue> {
    // Simple pattern matching for __type(name: "TypeName")
    let type_name = extract_type_name(query)?;

    // Check if it's a built-in scalar type
    match type_name.as_str() {
        "String" | "Int" | "Float" | "Boolean" | "ID" => {
            return Some(json!({
                "__type": {
                    "kind": "SCALAR",
                    "name": type_name,
                    "fields": null,
                }
            }));
        }
        _ => {}
    }

    // Check if it's a table
    let table_names = db.list_tables();
    if table_names.contains(&type_name) {
        let fields = get_table_fields(db, &type_name);
        return Some(json!({
            "__type": {
                "kind": "OBJECT",
                "name": type_name,
                "fields": fields,
            }
        }));
    }

    // Type not found
    Some(json!({
        "__type": null
    }))
}

/// Extract type name from __type(name: "TypeName") query
pub fn extract_type_name(query: &str) -> Option<String> {
    // Look for pattern: __type(name: "TypeName")
    let start = query.find("__type(name:")?;
    let substring = &query[start + 11..]; // Skip "__type(name:"

    // Find the quoted string
    let first_quote = substring.find('"')?;
    let remaining = &substring[first_quote + 1..];
    let closing_quote = remaining.find('"')?;

    Some(remaining[..closing_quote].to_string())
}

/// Get fields for a table (map SQLite columns to GraphQL fields)
fn get_table_fields(db: &Arc<Database>, table_name: &str) -> Vec<JsonValue> {
    let fields = Vec::new();

    // Get table schema from database
    let table_names = db.list_tables();
    if !table_names.iter().any(|t| t == table_name) {
        return fields;
    }

    // Try to get table schema from database metadata
    // Use a basic introspection approach that queries the database
    // to determine column names

    // Note: Table introspection disabled for now - requires async context.
    // In a real implementation, this should query the database catalog to get column information.
    // For now, return generic fields based on table existence check above.
    let _ = table_name; // Silence unused variable warning

    // If we couldn't get fields from introspection, return an empty list
    // This prevents errors when a table exists but has no schema info
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_type_name() {
        let query = r#"query { __type(name: "users") { kind name } }"#;
        assert_eq!(extract_type_name(query), Some("users".to_string()));

        let query = r#"{ __type(name: "Post") { fields { name } } }"#;
        assert_eq!(extract_type_name(query), Some("Post".to_string()));
    }

    #[test]
    fn test_schema_query_detection() {
        let db = Arc::new(Database::new());
        let query = "query { __schema { types { name } } }";
        assert!(try_introspection_query(&db, query).is_some());
    }

    #[test]
    fn test_type_query_detection() {
        let db = Arc::new(Database::new());
        let query = r#"{ __type(name: "String") { kind } }"#;
        assert!(try_introspection_query(&db, query).is_some());
    }

    #[test]
    fn test_builtin_scalar_type() {
        let db = Arc::new(Database::new());
        let query = r#"{ __type(name: "Int") { kind name fields } }"#;
        let result = try_type_query(&db, query).unwrap();
        assert_eq!(result["__type"]["kind"], "SCALAR");
        assert_eq!(result["__type"]["name"], "Int");
        assert!(result["__type"]["fields"].is_null());
    }
}
