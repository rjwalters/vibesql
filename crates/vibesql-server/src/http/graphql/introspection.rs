//! GraphQL schema introspection support
//!
//! Handles __schema and __type queries for GraphQL introspection.
//!
//! Introspection is driven by a schema map (table name → [`TableSchema`])
//! rather than the live database handle, so the same code path serves both
//! standalone mode (the map is built from the local registry catalog) and
//! replicated mode (the map is built from the applied consensus catalog via
//! [`ReplicationHandle::schema_snapshot`](crate::replication::ReplicationHandle::schema_snapshot)),
//! #5421. Reading the catalog through the map keeps the GraphQL surface from
//! ever introspecting the unreplicated local database on a replicated node.

use std::collections::HashMap;

use serde_json::{json, Value as JsonValue};
use vibesql_catalog::TableSchema;

/// Try to handle introspection queries (__schema, __type)
/// Returns Some(result) if this was an introspection query, None otherwise
pub fn try_introspection_query(
    schemas: &HashMap<String, TableSchema>,
    query: &str,
) -> Option<JsonValue> {
    let query = query.trim();

    // Check for __schema query
    if query.contains("__schema") {
        return try_schema_query(schemas, query);
    }

    // Check for __type query
    if query.contains("__type") {
        return try_type_query(schemas, query);
    }

    None
}

/// Look up a table schema in the map case-insensitively.
fn find_schema<'a>(
    schemas: &'a HashMap<String, TableSchema>,
    table_name: &str,
) -> Option<&'a TableSchema> {
    schemas.iter().find(|(name, _)| name.eq_ignore_ascii_case(table_name)).map(|(_, schema)| schema)
}

/// Handle __schema introspection query
fn try_schema_query(schemas: &HashMap<String, TableSchema>, _query: &str) -> Option<JsonValue> {
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

    // Add table types (sorted for deterministic output across runs).
    let mut table_names: Vec<&String> = schemas.keys().collect();
    table_names.sort();
    for table_name in table_names {
        let fields = get_table_fields(schemas, table_name);
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
fn try_type_query(schemas: &HashMap<String, TableSchema>, query: &str) -> Option<JsonValue> {
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
    if find_schema(schemas, &type_name).is_some() {
        let fields = get_table_fields(schemas, &type_name);
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

/// Get fields for a table (map columns to GraphQL fields), driven by the
/// resolved [`TableSchema`]. In replicated mode the schema comes from the
/// applied consensus catalog, so the fields are no longer the empty list the
/// pre-#5421 local-only path returned for an empty local database.
fn get_table_fields(schemas: &HashMap<String, TableSchema>, table_name: &str) -> Vec<JsonValue> {
    let Some(schema) = find_schema(schemas, table_name) else {
        return Vec::new();
    };

    schema
        .columns
        .iter()
        .map(|col| {
            json!({
                "name": col.name,
                "type": graphql_type_name(&col.data_type, col.nullable),
            })
        })
        .collect()
}

/// Map a SQL [`DataType`](vibesql_types::DataType) onto a GraphQL scalar type
/// name, appending `!` for NOT NULL columns (GraphQL non-null marker).
fn graphql_type_name(data_type: &vibesql_types::DataType, nullable: bool) -> String {
    use vibesql_types::DataType;
    let base = match data_type {
        DataType::Boolean => "Boolean",
        DataType::Smallint | DataType::Integer | DataType::Bigint | DataType::Unsigned => "Int",
        DataType::Real
        | DataType::DoublePrecision
        | DataType::Float { .. }
        | DataType::Numeric { .. }
        | DataType::Decimal { .. } => "Float",
        _ => "String",
    };
    if nullable {
        base.to_string()
    } else {
        format!("{base}!")
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    use super::*;

    fn sample_schemas() -> HashMap<String, TableSchema> {
        let mut schemas = HashMap::new();
        let users = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    true,
                ),
            ],
        );
        schemas.insert("users".to_string(), users);
        schemas
    }

    #[test]
    fn test_extract_type_name() {
        let query = r#"query { __type(name: "users") { kind name } }"#;
        assert_eq!(extract_type_name(query), Some("users".to_string()));

        let query = r#"{ __type(name: "Post") { fields { name } } }"#;
        assert_eq!(extract_type_name(query), Some("Post".to_string()));
    }

    #[test]
    fn test_schema_query_detection() {
        let schemas = HashMap::new();
        let query = "query { __schema { types { name } } }";
        assert!(try_introspection_query(&schemas, query).is_some());
    }

    #[test]
    fn test_type_query_detection() {
        let schemas = HashMap::new();
        let query = r#"{ __type(name: "String") { kind } }"#;
        assert!(try_introspection_query(&schemas, query).is_some());
    }

    #[test]
    fn test_builtin_scalar_type() {
        let schemas = HashMap::new();
        let query = r#"{ __type(name: "Int") { kind name fields } }"#;
        let result = try_type_query(&schemas, query).unwrap();
        assert_eq!(result["__type"]["kind"], "SCALAR");
        assert_eq!(result["__type"]["name"], "Int");
        assert!(result["__type"]["fields"].is_null());
    }

    #[test]
    fn test_table_type_fields_from_schema() {
        let schemas = sample_schemas();
        let query = r#"{ __type(name: "users") { kind name fields { name } } }"#;
        let result = try_type_query(&schemas, query).unwrap();
        assert_eq!(result["__type"]["kind"], "OBJECT");
        assert_eq!(result["__type"]["name"], "users");
        let fields = result["__type"]["fields"].as_array().expect("fields array");
        // The schema's columns are reflected as GraphQL fields (no longer the
        // empty list the local-only path returned).
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[0]["type"], "Int!"); // NOT NULL → non-null marker
        assert_eq!(fields[1]["name"], "name");
        assert_eq!(fields[1]["type"], "String"); // nullable
    }

    #[test]
    fn test_schema_query_includes_tables() {
        let schemas = sample_schemas();
        let query = "query { __schema { types { name } } }";
        let result = try_schema_query(&schemas, query).unwrap();
        let types = result["__schema"]["types"].as_array().unwrap();
        assert!(types.iter().any(|t| t["name"] == "users"));
    }
}
