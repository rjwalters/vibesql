//! GraphQL API implementation for VibeSQL HTTP interface
//!
//! This provides a lightweight GraphQL-like interface over HTTP without a full GraphQL library.
//! It supports queries and mutations on database tables with structured filtering,
//! as well as schema introspection (__schema, __type queries).
//! Supports nested queries with automatic relationship resolution via foreign keys.
//!
//! # WHERE Clause Operators
//!
//! The GraphQL API supports structured WHERE clauses with the following operators:
//!
//! ## Comparison Operators
//! - `eq`: Equal to (=)
//! - `ne`: Not equal to (<>)
//! - `gt`: Greater than (>)
//! - `gte`: Greater than or equal (>=)
//! - `lt`: Less than (<)
//! - `lte`: Less than or equal (<=)
//!
//! ## String Operators
//! - `like`: SQL LIKE pattern matching
//! - `ilike`: Case-insensitive LIKE (uses LOWER())
//! - `contains`: Contains substring
//! - `startsWith`: Starts with prefix
//! - `endsWith`: Ends with suffix
//!
//! ## List Operators
//! - `in`: Value in list
//! - `notIn`: Value not in list
//!
//! ## Null Operators
//! - `isNull`: Check for NULL (true/false)
//!
//! ## Logical Combinators
//! - `AND`: Array of conditions combined with AND
//! - `OR`: Array of conditions combined with OR
//! - `NOT`: Negate a condition
//!
//! # Example
//! ```graphql
//! query {
//!   users(where: {
//!     age: { gte: 18 },
//!     OR: [
//!       { name: { contains: "smith" } },
//!       { email: { endsWith: "@company.com" } }
//!     ]
//!   }) {
//!     id
//!     name
//!     email
//!   }
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use vibesql_catalog::TableSchema;
use vibesql_storage::Database;

use super::types::json_to_sql_value;

/// GraphQL request body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    /// GraphQL query string
    pub query: String,
    /// Optional query variables
    #[serde(default)]
    pub variables: Option<serde_json::Map<String, JsonValue>>,
    /// Optional operation name
    #[serde(default)]
    pub operation_name: Option<String>,
}

/// GraphQL response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    /// Query result data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
    /// Query errors (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<GraphQLError>>,
}

/// GraphQL error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<serde_json::Map<String, JsonValue>>,
}

impl GraphQLError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), extensions: None }
    }
}

// ============================================================================
// WHERE Clause Types and Parsing
// ============================================================================

/// Comparison operators for WHERE clause filtering
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOp {
    /// Equal to (=)
    Eq,
    /// Not equal to (<>)
    Ne,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Gte,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    Lte,
    /// SQL LIKE pattern matching
    Like,
    /// Case-insensitive LIKE
    ILike,
    /// Contains substring
    Contains,
    /// Starts with prefix
    StartsWith,
    /// Ends with suffix
    EndsWith,
    /// Value in list
    In,
    /// Value not in list
    NotIn,
    /// Is NULL (true) or IS NOT NULL (false)
    IsNull,
}

/// A single field condition in a WHERE clause
#[derive(Debug, Clone)]
pub struct FieldCondition {
    /// Field name
    pub field: String,
    /// Comparison operator
    pub op: ComparisonOp,
    /// Value to compare against
    pub value: JsonValue,
}

/// A structured WHERE clause with logical combinators
#[derive(Debug, Clone)]
pub enum WhereClause {
    /// A simple field condition
    Condition(FieldCondition),
    /// AND combinator - all conditions must be true
    And(Vec<WhereClause>),
    /// OR combinator - at least one condition must be true
    Or(Vec<WhereClause>),
    /// NOT combinator - negate the condition
    Not(Box<WhereClause>),
}

/// Parse a structured WHERE clause from JSON
fn parse_where_clause(value: &JsonValue) -> Result<Option<WhereClause>, String> {
    match value {
        JsonValue::Object(obj) if obj.is_empty() => Ok(None),
        JsonValue::Object(obj) => {
            let clause = parse_where_object(obj)?;
            Ok(Some(clause))
        }
        JsonValue::Null => Ok(None),
        _ => Err("WHERE clause must be an object".to_string()),
    }
}

/// Parse a WHERE clause from a JSON object
fn parse_where_object(obj: &serde_json::Map<String, JsonValue>) -> Result<WhereClause, String> {
    let mut conditions: Vec<WhereClause> = Vec::new();

    for (key, value) in obj {
        match key.as_str() {
            "AND" => {
                let and_conditions = parse_logical_array(value, "AND")?;
                conditions.push(WhereClause::And(and_conditions));
            }
            "OR" => {
                let or_conditions = parse_logical_array(value, "OR")?;
                conditions.push(WhereClause::Or(or_conditions));
            }
            "NOT" => {
                let not_clause = parse_not_clause(value)?;
                conditions.push(WhereClause::Not(Box::new(not_clause)));
            }
            field_name => {
                let field_conditions = parse_field_conditions(field_name, value)?;
                conditions.extend(field_conditions);
            }
        }
    }

    // Combine multiple top-level conditions with AND
    if conditions.is_empty() {
        Err("WHERE clause cannot be empty".to_string())
    } else if conditions.len() == 1 {
        Ok(conditions.remove(0))
    } else {
        Ok(WhereClause::And(conditions))
    }
}

/// Parse an array of conditions for AND/OR
fn parse_logical_array(value: &JsonValue, op_name: &str) -> Result<Vec<WhereClause>, String> {
    match value {
        JsonValue::Array(arr) => {
            let mut clauses = Vec::new();
            for item in arr {
                match item {
                    JsonValue::Object(obj) => {
                        clauses.push(parse_where_object(obj)?);
                    }
                    _ => return Err(format!("{} array must contain objects", op_name)),
                }
            }
            if clauses.is_empty() {
                return Err(format!("{} array cannot be empty", op_name));
            }
            Ok(clauses)
        }
        _ => Err(format!("{} must be an array", op_name)),
    }
}

/// Parse a NOT clause
fn parse_not_clause(value: &JsonValue) -> Result<WhereClause, String> {
    match value {
        JsonValue::Object(obj) => parse_where_object(obj),
        _ => Err("NOT must contain an object".to_string()),
    }
}

/// Parse field conditions from a field value
fn parse_field_conditions(field_name: &str, value: &JsonValue) -> Result<Vec<WhereClause>, String> {
    match value {
        // Direct equality: { field: "value" }
        JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_) | JsonValue::Null => {
            Ok(vec![WhereClause::Condition(FieldCondition {
                field: field_name.to_string(),
                op: ComparisonOp::Eq,
                value: value.clone(),
            })])
        }
        // Operator object: { field: { op: value } }
        JsonValue::Object(ops) => {
            let mut conditions = Vec::new();
            for (op_name, op_value) in ops {
                let op = match op_name.as_str() {
                    "eq" => ComparisonOp::Eq,
                    "ne" => ComparisonOp::Ne,
                    "gt" => ComparisonOp::Gt,
                    "gte" => ComparisonOp::Gte,
                    "lt" => ComparisonOp::Lt,
                    "lte" => ComparisonOp::Lte,
                    "like" => ComparisonOp::Like,
                    "ilike" => ComparisonOp::ILike,
                    "contains" => ComparisonOp::Contains,
                    "startsWith" => ComparisonOp::StartsWith,
                    "endsWith" => ComparisonOp::EndsWith,
                    "in" => ComparisonOp::In,
                    "notIn" => ComparisonOp::NotIn,
                    "isNull" => ComparisonOp::IsNull,
                    unknown => {
                        return Err(format!("Unknown operator: {}", unknown));
                    }
                };
                conditions.push(WhereClause::Condition(FieldCondition {
                    field: field_name.to_string(),
                    op,
                    value: op_value.clone(),
                }));
            }
            Ok(conditions)
        }
        JsonValue::Array(_) => {
            // Direct array means IN: { field: [1, 2, 3] }
            Ok(vec![WhereClause::Condition(FieldCondition {
                field: field_name.to_string(),
                op: ComparisonOp::In,
                value: value.clone(),
            })])
        }
    }
}

/// Convert a WHERE clause to SQL with parameterized values
pub fn where_clause_to_sql(
    clause: &WhereClause,
    params: &mut Vec<vibesql_types::SqlValue>,
) -> Result<String, String> {
    match clause {
        WhereClause::Condition(cond) => condition_to_sql(cond, params),
        WhereClause::And(clauses) => {
            let sql_parts: Result<Vec<String>, String> =
                clauses.iter().map(|c| where_clause_to_sql(c, params)).collect();
            let parts = sql_parts?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        WhereClause::Or(clauses) => {
            let sql_parts: Result<Vec<String>, String> =
                clauses.iter().map(|c| where_clause_to_sql(c, params)).collect();
            let parts = sql_parts?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        WhereClause::Not(inner) => {
            let inner_sql = where_clause_to_sql(inner, params)?;
            Ok(format!("NOT {}", inner_sql))
        }
    }
}

/// Convert a field condition to SQL
fn condition_to_sql(
    cond: &FieldCondition,
    params: &mut Vec<vibesql_types::SqlValue>,
) -> Result<String, String> {
    let field = escape_identifier(&cond.field);

    match &cond.op {
        ComparisonOp::Eq => {
            if cond.value.is_null() {
                Ok(format!("{} IS NULL", field))
            } else {
                let param_idx = params.len() + 1;
                params.push(json_to_sql_value(&cond.value)?);
                Ok(format!("{} = ${}", field, param_idx))
            }
        }
        ComparisonOp::Ne => {
            if cond.value.is_null() {
                Ok(format!("{} IS NOT NULL", field))
            } else {
                let param_idx = params.len() + 1;
                params.push(json_to_sql_value(&cond.value)?);
                Ok(format!("{} <> ${}", field, param_idx))
            }
        }
        ComparisonOp::Gt => {
            let param_idx = params.len() + 1;
            params.push(json_to_sql_value(&cond.value)?);
            Ok(format!("{} > ${}", field, param_idx))
        }
        ComparisonOp::Gte => {
            let param_idx = params.len() + 1;
            params.push(json_to_sql_value(&cond.value)?);
            Ok(format!("{} >= ${}", field, param_idx))
        }
        ComparisonOp::Lt => {
            let param_idx = params.len() + 1;
            params.push(json_to_sql_value(&cond.value)?);
            Ok(format!("{} < ${}", field, param_idx))
        }
        ComparisonOp::Lte => {
            let param_idx = params.len() + 1;
            params.push(json_to_sql_value(&cond.value)?);
            Ok(format!("{} <= ${}", field, param_idx))
        }
        ComparisonOp::Like => {
            let param_idx = params.len() + 1;
            params.push(json_to_sql_value(&cond.value)?);
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::ILike => {
            let param_idx = params.len() + 1;
            params.push(json_to_sql_value(&cond.value)?);
            Ok(format!("LOWER({}) LIKE LOWER(${})", field, param_idx))
        }
        ComparisonOp::Contains => {
            let value_str = cond.value.as_str().ok_or("contains requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("%{}%", value_str))));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::StartsWith => {
            let value_str = cond.value.as_str().ok_or("startsWith requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("{}%", value_str))));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::EndsWith => {
            let value_str = cond.value.as_str().ok_or("endsWith requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("%{}", value_str))));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::In => {
            let arr = cond.value.as_array().ok_or("IN requires an array value")?;
            if arr.is_empty() {
                // Empty IN list is always false
                return Ok("FALSE".to_string());
            }
            let mut placeholders = Vec::new();
            for item in arr {
                let param_idx = params.len() + 1;
                params.push(json_to_sql_value(item)?);
                placeholders.push(format!("${}", param_idx));
            }
            Ok(format!("{} IN ({})", field, placeholders.join(", ")))
        }
        ComparisonOp::NotIn => {
            let arr = cond.value.as_array().ok_or("NOT IN requires an array value")?;
            if arr.is_empty() {
                // Empty NOT IN list is always true
                return Ok("TRUE".to_string());
            }
            let mut placeholders = Vec::new();
            for item in arr {
                let param_idx = params.len() + 1;
                params.push(json_to_sql_value(item)?);
                placeholders.push(format!("${}", param_idx));
            }
            Ok(format!("{} NOT IN ({})", field, placeholders.join(", ")))
        }
        ComparisonOp::IsNull => {
            let is_null = cond.value.as_bool().ok_or("isNull requires a boolean value")?;
            if is_null {
                Ok(format!("{} IS NULL", field))
            } else {
                Ok(format!("{} IS NOT NULL", field))
            }
        }
    }
}

/// Escape an identifier to prevent SQL injection
fn escape_identifier(identifier: &str) -> String {
    // Basic identifier validation - only allow alphanumeric and underscore
    if identifier.chars().all(|c| c.is_alphanumeric() || c == '_') {
        identifier.to_string()
    } else {
        // Quote the identifier if it contains special characters
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }
}

// ============================================================================
// Schema Introspection Support
// ============================================================================

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
fn extract_type_name(query: &str) -> Option<String> {
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

// ============================================================================
// Query/Mutation Parsing and SQL Generation
// ============================================================================

/// Parse a simple GraphQL query and convert to SQL
pub fn parse_graphql_query(query_str: &str) -> Result<GraphQLQueryInfo, String> {
    let trimmed = query_str.trim();

    // Check if it's a query operation
    if trimmed.starts_with("query {") || trimmed.starts_with('{') {
        parse_graphql_select_query(trimmed)
    } else if trimmed.starts_with("mutation {") {
        parse_graphql_mutation(trimmed)
    } else {
        Err("Invalid GraphQL query format".to_string())
    }
}

#[derive(Debug, Clone)]
pub enum GraphQLQueryInfo {
    Query {
        table_name: String,
        /// Simple field names (backwards compatible)
        fields: Vec<String>,
        /// Nested field structure for relationship queries
        nested_fields: Vec<GraphQLField>,
        /// Structured WHERE clause with operators
        where_clause: Option<WhereClause>,
        /// Legacy string-based WHERE clause (for backwards compatibility)
        where_clause_raw: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Mutation {
        operation_type: MutationType,
        table_name: String,
        data: Option<serde_json::Map<String, JsonValue>>,
        /// Structured WHERE clause with operators
        where_clause: Option<WhereClause>,
        /// Legacy string-based WHERE clause (for backwards compatibility)
        where_clause_raw: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum MutationType {
    Insert,
    Update,
    Delete,
}

/// A field in a GraphQL selection set, which may have nested selections
#[derive(Debug, Clone)]
pub struct GraphQLField {
    /// The field name (column name or relationship name)
    pub name: String,
    /// Nested field selections (for relationships)
    pub nested: Option<Vec<GraphQLField>>,
    /// WHERE clause for this level (for filtering nested relations)
    pub where_clause: Option<String>,
    /// Limit for nested queries
    pub limit: Option<usize>,
    /// Offset for nested queries
    pub offset: Option<usize>,
}

impl GraphQLField {
    /// Create a simple field without nesting
    pub fn simple(name: String) -> Self {
        Self { name, nested: None, where_clause: None, limit: None, offset: None }
    }

    /// Create a nested field with sub-selections
    pub fn nested(name: String, fields: Vec<GraphQLField>) -> Self {
        Self { name, nested: Some(fields), where_clause: None, limit: None, offset: None }
    }
}

/// Describes a relationship between two tables
#[derive(Debug, Clone)]
pub struct TableRelationship {
    /// The related table name
    pub related_table: String,
    /// The foreign key column(s) in the child table
    pub fk_columns: Vec<String>,
    /// The referenced column(s) in the parent table
    pub pk_columns: Vec<String>,
    /// Direction of the relationship from the perspective of the current table
    pub direction: RelationshipDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationshipDirection {
    /// Current table has FK pointing to related table (many-to-one)
    ManyToOne,
    /// Related table has FK pointing to current table (one-to-many)
    OneToMany,
}

/// Build a relationship map from all table schemas
/// Returns a map where key is table name, value is list of relationships
pub fn build_relationship_map(
    schemas: &HashMap<String, TableSchema>,
) -> HashMap<String, Vec<TableRelationship>> {
    let mut relationships: HashMap<String, Vec<TableRelationship>> = HashMap::new();

    for (table_name, schema) in schemas {
        // Initialize entry for this table
        relationships.entry(table_name.clone()).or_default();

        // Process foreign keys in this table (many-to-one relationships)
        for fk in &schema.foreign_keys {
            // This table has FK -> many-to-one relationship to parent
            let rel = TableRelationship {
                related_table: fk.parent_table.clone(),
                fk_columns: fk.column_names.clone(),
                pk_columns: fk.parent_column_names.clone(),
                direction: RelationshipDirection::ManyToOne,
            };
            relationships.entry(table_name.clone()).or_default().push(rel);

            // Parent table has one-to-many relationship back to this table
            let reverse_rel = TableRelationship {
                related_table: table_name.clone(),
                fk_columns: fk.column_names.clone(),
                pk_columns: fk.parent_column_names.clone(),
                direction: RelationshipDirection::OneToMany,
            };
            relationships.entry(fk.parent_table.clone()).or_default().push(reverse_rel);
        }
    }

    relationships
}

/// Find a relationship between two tables
pub fn find_relationship(
    relationships: &HashMap<String, Vec<TableRelationship>>,
    from_table: &str,
    to_table: &str,
) -> Option<TableRelationship> {
    relationships
        .get(from_table)?
        .iter()
        .find(|r| r.related_table.eq_ignore_ascii_case(to_table))
        .cloned()
}

/// Parse a GraphQL select-style query
fn parse_graphql_select_query(query: &str) -> Result<GraphQLQueryInfo, String> {
    // Simple parser for: { users { id name email } }
    // Or with filters: { users(where: {id: 1}) { id name } }
    // Or with string filter: { users(where: "id = 1") { id name } }
    // Or with nested: { users { id name posts { id title } } }

    let start = query.find('{').ok_or("Missing opening brace")?;
    let content = &query[start + 1..];

    // Find the table name (first word after opening brace)
    let table_part = content.trim_start();
    let table_name = table_part
        .split('(')
        .next()
        .and_then(|s| s.split_whitespace().next())
        .ok_or("Could not find table name")?
        .to_string();

    // Try to find fields between inner braces
    // Need to skip any braces inside the args (e.g., WHERE clause JSON)
    // Look for the field list brace after the args (after the closing paren)
    let fields_start = if let Some(args_start) = content.find('(') {
        // Has args, find the matching close paren first
        let mut paren_count = 0;
        let mut in_string = false;
        let mut escape_next = false;
        let mut close_paren_idx = None;
        for (i, ch) in content[args_start..].char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }
            match ch {
                '\\' => escape_next = true,
                '"' => in_string = !in_string,
                '(' if !in_string => paren_count += 1,
                ')' if !in_string => {
                    paren_count -= 1;
                    if paren_count == 0 {
                        close_paren_idx = Some(args_start + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        // Find the field list brace after the close paren
        if let Some(close_idx) = close_paren_idx {
            content[close_idx..].find('{').map(|i| close_idx + i)
        } else {
            content.find('{')
        }
    } else {
        content.find('{')
    }
    .ok_or("Missing field list")?;

    // Find the matching closing brace for the field list
    let fields_content = find_matching_braces(&content[fields_start..])?;

    // Parse fields (may include nested selections)
    let nested_fields = parse_field_selections(&fields_content)?;

    // Extract simple field names for backwards compatibility
    let fields: Vec<String> =
        nested_fields.iter().filter(|f| f.nested.is_none()).map(|f| f.name.clone()).collect();

    // Try to extract structured where clause (JSON object)
    let (where_clause, where_clause_raw) = extract_where_clauses(content)?;

    // Extract limit and offset
    let limit = extract_numeric_param(content, "limit");
    let offset = extract_numeric_param(content, "offset");

    Ok(GraphQLQueryInfo::Query {
        table_name,
        fields,
        nested_fields,
        where_clause,
        where_clause_raw,
        limit,
        offset,
    })
}

/// Extract numeric parameter value
fn extract_numeric_param(query: &str, param_name: &str) -> Option<usize> {
    let pattern = format!("{}:", param_name);
    let start = query.find(&pattern)?;
    let after_pattern = &query[start + pattern.len()..];
    let content = after_pattern.trim_start();

    // Find the number
    let end = content.find(|c: char| !c.is_ascii_digit()).unwrap_or(content.len());
    if end == 0 {
        return None;
    }
    content[..end].parse().ok()
}

/// Find content between matching braces, handling nested braces
fn find_matching_braces(content: &str) -> Result<String, String> {
    if !content.starts_with('{') {
        return Err("Expected opening brace".to_string());
    }

    let mut brace_count = 0;
    let mut end_idx = 0;

    for (i, ch) in content.char_indices() {
        match ch {
            '{' => brace_count += 1,
            '}' => {
                brace_count -= 1;
                if brace_count == 0 {
                    end_idx = i;
                    break;
                }
            }
            _ => {}
        }
    }

    if brace_count != 0 {
        return Err("Unmatched braces".to_string());
    }

    // Return content between the braces (excluding the braces themselves)
    Ok(content[1..end_idx].to_string())
}

/// Parse field selections, supporting nested queries
fn parse_field_selections(content: &str) -> Result<Vec<GraphQLField>, String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let bytes = content.as_bytes();
    let mut idx = 0;

    while idx < bytes.len() {
        let ch = bytes[idx] as char;

        match ch {
            '{' => {
                // Start of nested selection
                let field_name = current_field.trim().to_string();
                if field_name.is_empty() {
                    return Err("Nested selection without field name".to_string());
                }

                // Find the matching closing brace
                let nested_content = find_matching_braces(&content[idx..])?;
                let nested_fields = parse_field_selections(&nested_content)?;

                fields.push(GraphQLField::nested(field_name, nested_fields));
                current_field.clear();

                // Skip past the closing brace
                idx += nested_content.len() + 2; // +2 for { and }
            }
            ',' | '\n' => {
                // Explicit field separator
                let field_name = current_field.trim().to_string();
                if !field_name.is_empty() {
                    fields.push(GraphQLField::simple(field_name));
                    current_field.clear();
                }
                idx += 1;
            }
            ' ' | '\t' => {
                // Whitespace - could be separator or just before a nested selection
                // Look ahead to see if next non-whitespace is '{'
                let mut lookahead = idx + 1;
                while lookahead < bytes.len()
                    && (bytes[lookahead] == b' ' || bytes[lookahead] == b'\t')
                {
                    lookahead += 1;
                }

                if lookahead < bytes.len() && bytes[lookahead] == b'{' {
                    // Next is '{', so this field will become nested - don't commit yet
                    idx += 1;
                } else {
                    // Not followed by '{', so whitespace is a separator
                    let field_name = current_field.trim().to_string();
                    if !field_name.is_empty() {
                        fields.push(GraphQLField::simple(field_name));
                        current_field.clear();
                    }
                    idx += 1;
                }
            }
            '}' => {
                // Should not happen at this level (handled by find_matching_braces)
                break;
            }
            _ => {
                current_field.push(ch);
                idx += 1;
            }
        }
    }

    // Handle last field
    let field_name = current_field.trim().to_string();
    if !field_name.is_empty() {
        fields.push(GraphQLField::simple(field_name));
    }

    Ok(fields)
}

/// Parse a GraphQL mutation
fn parse_graphql_mutation(mutation: &str) -> Result<GraphQLQueryInfo, String> {
    let trimmed = mutation.trim();

    // Look for mutation { insert|update|delete...
    if trimmed.contains("insertInto") || trimmed.contains("insert") {
        parse_graphql_insert_mutation(trimmed)
    } else if trimmed.contains("update") {
        parse_graphql_update_mutation(trimmed)
    } else if trimmed.contains("delete") {
        parse_graphql_delete_mutation(trimmed)
    } else {
        Err("Unknown mutation type".to_string())
    }
}

fn parse_graphql_insert_mutation(mutation: &str) -> Result<GraphQLQueryInfo, String> {
    // Simple format: mutation { insertInto(table: "users", values: {...}) { id } }
    let table_part =
        extract_quoted_value(mutation, "table").ok_or("Missing table name in insert mutation")?;

    let data = extract_json_value(mutation, "values");

    Ok(GraphQLQueryInfo::Mutation {
        operation_type: MutationType::Insert,
        table_name: table_part,
        data,
        where_clause: None,
        where_clause_raw: None,
    })
}

fn parse_graphql_update_mutation(mutation: &str) -> Result<GraphQLQueryInfo, String> {
    let table_part =
        extract_quoted_value(mutation, "table").ok_or("Missing table name in update mutation")?;

    let data = extract_json_value(mutation, "values");
    let (where_clause, where_clause_raw) = extract_where_clauses(mutation)?;

    Ok(GraphQLQueryInfo::Mutation {
        operation_type: MutationType::Update,
        table_name: table_part,
        data,
        where_clause,
        where_clause_raw,
    })
}

fn parse_graphql_delete_mutation(mutation: &str) -> Result<GraphQLQueryInfo, String> {
    let table_part =
        extract_quoted_value(mutation, "table").ok_or("Missing table name in delete mutation")?;

    let (where_clause, where_clause_raw) = extract_where_clauses(mutation)?;

    Ok(GraphQLQueryInfo::Mutation {
        operation_type: MutationType::Delete,
        table_name: table_part,
        data: None,
        where_clause,
        where_clause_raw,
    })
}

/// Extract a quoted value from a parameter
fn extract_quoted_value(input: &str, param_name: &str) -> Option<String> {
    let pattern = format!("{}:", param_name);
    let start = input.find(&pattern)?;
    let after_pattern = &input[start + pattern.len()..];

    // Skip whitespace
    let content = after_pattern.trim_start();

    // Find quoted string
    if let Some(stripped) = content.strip_prefix('"') {
        let end = stripped.find('"')?;
        Some(stripped[..end].to_string())
    } else {
        None
    }
}

/// Extract a JSON value from a parameter
fn extract_json_value(input: &str, param_name: &str) -> Option<serde_json::Map<String, JsonValue>> {
    let pattern = format!("{}:", param_name);
    let start = input.find(&pattern)?;
    let after_pattern = &input[start + pattern.len()..];

    let content = after_pattern.trim_start();

    if content.starts_with('{') {
        // Find the matching closing brace
        let mut brace_count = 0;
        // Use char_indices() to get byte positions for correct string slicing
        for (i, ch) in content.char_indices() {
            match ch {
                '{' => brace_count += 1,
                '}' => {
                    brace_count -= 1;
                    if brace_count == 0 {
                        let json_str = &content[..=i];
                        if let Ok(JsonValue::Object(map)) = serde_json::from_str(json_str) {
                            return Some(map);
                        }
                        return None;
                    }
                }
                _ => {}
            }
        }
    }
    None
}

/// Extract WHERE clause from a query - supports both structured (JSON) and raw (string) formats
fn extract_where_clauses(query: &str) -> Result<(Option<WhereClause>, Option<String>), String> {
    let pattern = "where:";
    let start = match query.find(pattern) {
        Some(s) => s,
        None => return Ok((None, None)),
    };
    let after_pattern = &query[start + pattern.len()..];
    let content = after_pattern.trim_start();

    // Check if it's a structured WHERE (JSON object)
    if content.starts_with('{') {
        // Find the matching closing brace
        let mut brace_count = 0;
        for (i, ch) in content.char_indices() {
            match ch {
                '{' => brace_count += 1,
                '}' => {
                    brace_count -= 1;
                    if brace_count == 0 {
                        let json_str = &content[..=i];
                        match serde_json::from_str::<JsonValue>(json_str) {
                            Ok(value) => {
                                let clause = parse_where_clause(&value)?;
                                return Ok((clause, None));
                            }
                            Err(e) => {
                                return Err(format!("Invalid WHERE clause JSON: {}", e));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Err("Unmatched brace in WHERE clause".to_string())
    } else if let Some(stripped) = content.strip_prefix('"') {
        // Raw string WHERE clause (legacy format)
        if let Some(end) = stripped.find('"') {
            Ok((None, Some(stripped[..end].to_string())))
        } else {
            Err("Unclosed quote in WHERE clause".to_string())
        }
    } else {
        Err("WHERE clause must be a JSON object or quoted string".to_string())
    }
}

/// Convert GraphQL query info to SQL (simple version without relationships)
pub fn graphql_to_sql(
    query_info: &GraphQLQueryInfo,
) -> Result<(String, Vec<vibesql_types::SqlValue>), String> {
    match query_info {
        GraphQLQueryInfo::Query {
            table_name,
            fields,
            nested_fields: _,
            where_clause,
            where_clause_raw,
            limit,
            offset,
        } => {
            let select_list = if fields.is_empty() || fields.contains(&"*".to_string()) {
                "*".to_string()
            } else {
                fields.iter().map(|f| escape_identifier(f)).collect::<Vec<_>>().join(", ")
            };

            let table = escape_identifier(table_name);
            let mut sql = format!("SELECT {} FROM {}", select_list, table);
            let mut params = Vec::new();

            // Handle structured WHERE clause
            if let Some(clause) = where_clause {
                let where_sql = where_clause_to_sql(clause, &mut params)?;
                sql.push_str(&format!(" WHERE {}", where_sql));
            } else if let Some(raw_where) = where_clause_raw {
                // Fall back to raw WHERE clause (legacy)
                sql.push_str(&format!(" WHERE {}", raw_where));
            }

            if let Some(limit) = limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }

            if let Some(offset) = offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }

            Ok((sql, params))
        }
        GraphQLQueryInfo::Mutation {
            operation_type,
            table_name,
            data,
            where_clause,
            where_clause_raw,
        } => {
            let table = escape_identifier(table_name);

            match operation_type {
                MutationType::Insert => {
                    if let Some(data) = data {
                        let columns: Vec<String> = data.keys().cloned().collect();
                        let placeholders = (0..columns.len())
                            .map(|i| format!("${}", i + 1))
                            .collect::<Vec<_>>()
                            .join(", ");

                        let column_list = columns
                            .iter()
                            .map(|c| escape_identifier(c))
                            .collect::<Vec<_>>()
                            .join(", ");

                        let sql = format!(
                            "INSERT INTO {} ({}) VALUES ({})",
                            table, column_list, placeholders
                        );

                        // Convert values to SqlValue
                        let mut params = Vec::new();
                        for col in columns {
                            if let Some(val) = data.get(&col) {
                                params.push(json_to_sql_value(val)?);
                            }
                        }

                        Ok((sql, params))
                    } else {
                        Err("INSERT requires data".to_string())
                    }
                }
                MutationType::Update => {
                    if let Some(data) = data {
                        let mut params = Vec::new();

                        let set_clause = data
                            .keys()
                            .enumerate()
                            .map(|(i, col)| format!("{} = ${}", escape_identifier(col), i + 1))
                            .collect::<Vec<_>>()
                            .join(", ");

                        let mut sql = format!("UPDATE {} SET {}", table, set_clause);

                        // Collect data params first
                        for col in data.keys() {
                            if let Some(val) = data.get(col) {
                                params.push(json_to_sql_value(val)?);
                            }
                        }

                        // Handle structured WHERE clause
                        if let Some(clause) = where_clause {
                            let where_sql = where_clause_to_sql(clause, &mut params)?;
                            sql.push_str(&format!(" WHERE {}", where_sql));
                        } else if let Some(raw_where) = where_clause_raw {
                            // Fall back to raw WHERE clause (legacy)
                            sql.push_str(&format!(" WHERE {}", raw_where));
                        }

                        Ok((sql, params))
                    } else {
                        Err("UPDATE requires data".to_string())
                    }
                }
                MutationType::Delete => {
                    let mut sql = format!("DELETE FROM {}", table);
                    let mut params = Vec::new();

                    // Handle structured WHERE clause
                    if let Some(clause) = where_clause {
                        let where_sql = where_clause_to_sql(clause, &mut params)?;
                        sql.push_str(&format!(" WHERE {}", where_sql));
                    } else if let Some(raw_where) = where_clause_raw {
                        // Fall back to raw WHERE clause (legacy)
                        sql.push_str(&format!(" WHERE {}", raw_where));
                    } else {
                        return Err("DELETE requires WHERE clause".to_string());
                    }

                    Ok((sql, params))
                }
            }
        }
    }
}

/// Context for executing GraphQL queries with relationship resolution
pub struct GraphQLExecutionContext<'a> {
    /// Relationship map built from all table schemas
    pub relationships: HashMap<String, Vec<TableRelationship>>,
    /// Table schemas for column validation
    pub schemas: &'a HashMap<String, TableSchema>,
}

impl<'a> GraphQLExecutionContext<'a> {
    /// Create a new execution context from table schemas
    pub fn new(schemas: &'a HashMap<String, TableSchema>) -> Self {
        let relationships = build_relationship_map(schemas);
        Self { relationships, schemas }
    }

    /// Check if a field is a relationship (refers to another table)
    pub fn is_relationship(&self, table: &str, field: &str) -> bool {
        find_relationship(&self.relationships, table, field).is_some()
    }

    /// Get the relationship for a field if it exists
    pub fn get_relationship(&self, table: &str, field: &str) -> Option<TableRelationship> {
        find_relationship(&self.relationships, table, field)
    }
}

/// Information about a nested query to execute
#[derive(Debug, Clone)]
pub struct NestedQueryInfo {
    /// The nested field name (relationship name)
    pub field_name: String,
    /// The related table name
    pub related_table: String,
    /// Columns to select from the related table
    pub select_columns: Vec<String>,
    /// The FK columns in the child table
    pub fk_columns: Vec<String>,
    /// The PK columns in the parent table
    pub pk_columns: Vec<String>,
    /// Direction of the relationship
    pub direction: RelationshipDirection,
    /// Further nested queries
    pub nested: Vec<NestedQueryInfo>,
    /// Optional WHERE clause for filtering
    pub where_clause: Option<String>,
    /// Optional limit
    pub limit: Option<usize>,
    /// Optional offset
    pub offset: Option<usize>,
}

/// Build the list of nested queries from parsed fields
pub fn build_nested_queries(
    ctx: &GraphQLExecutionContext,
    table_name: &str,
    fields: &[GraphQLField],
) -> Vec<NestedQueryInfo> {
    let mut nested_queries = Vec::new();

    for field in fields {
        if field.nested.is_some() {
            // This is a relationship field
            if let Some(rel) = ctx.get_relationship(table_name, &field.name) {
                let sub_fields = field.nested.as_ref().unwrap();
                let select_columns: Vec<String> = sub_fields
                    .iter()
                    .filter(|f| f.nested.is_none())
                    .map(|f| f.name.clone())
                    .collect();

                // Recursively build nested queries for deeper levels
                let deeper_nested = build_nested_queries(ctx, &rel.related_table, sub_fields);

                nested_queries.push(NestedQueryInfo {
                    field_name: field.name.clone(),
                    related_table: rel.related_table.clone(),
                    select_columns,
                    fk_columns: rel.fk_columns.clone(),
                    pk_columns: rel.pk_columns.clone(),
                    direction: rel.direction.clone(),
                    nested: deeper_nested,
                    where_clause: field.where_clause.clone(),
                    limit: field.limit,
                    offset: field.offset,
                });
            }
        }
    }

    nested_queries
}

/// Generate SQL for the main query (without JOINs - we'll use separate queries for nested data)
pub fn generate_main_query_sql(
    query_info: &GraphQLQueryInfo,
) -> Result<(String, Vec<vibesql_types::SqlValue>), String> {
    // Just delegate to the simple version for the main query
    graphql_to_sql(query_info)
}

/// Generate SQL for a nested query based on parent row values
/// Uses IN clause for batching to avoid N+1 problem
pub fn generate_nested_query_sql(
    nested: &NestedQueryInfo,
    parent_values: &[JsonValue],
) -> Result<String, String> {
    if parent_values.is_empty() {
        return Ok(String::new());
    }

    // Build select list - include FK/PK columns for grouping
    let mut select_columns = nested.select_columns.clone();

    // For one-to-many, include FK column if not already present
    if nested.direction == RelationshipDirection::OneToMany {
        for fk_col in &nested.fk_columns {
            if !select_columns.iter().any(|c| c.eq_ignore_ascii_case(fk_col)) {
                select_columns.push(fk_col.clone());
            }
        }
    }

    let select_list =
        if select_columns.is_empty() { "*".to_string() } else { select_columns.join(", ") };

    let mut sql = format!("SELECT {} FROM {}", select_list, nested.related_table);

    // Build WHERE clause based on relationship direction
    let where_column = match nested.direction {
        RelationshipDirection::OneToMany => {
            // Child table has FK pointing to parent
            // WHERE child.fk_col IN (parent_pk_values)
            nested.fk_columns.first().ok_or("Missing FK column")?
        }
        RelationshipDirection::ManyToOne => {
            // Parent table has PK that child FK points to
            // WHERE parent.pk_col IN (child_fk_values)
            nested.pk_columns.first().ok_or("Missing PK column")?
        }
    };

    // Extract values for IN clause
    let values: Vec<String> = parent_values
        .iter()
        .filter_map(|v| match v {
            JsonValue::Number(n) => Some(n.to_string()),
            JsonValue::String(s) => Some(format!("'{}'", s.replace('\'', "''"))),
            _ => None,
        })
        .collect();

    if !values.is_empty() {
        sql.push_str(&format!(" WHERE {} IN ({})", where_column, values.join(", ")));

        // Add any additional WHERE clause from the nested query
        if let Some(where_clause) = &nested.where_clause {
            sql.push_str(&format!(" AND ({})", where_clause));
        }
    }

    // Add limit/offset if specified
    if let Some(limit) = nested.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = nested.offset {
        sql.push_str(&format!(" OFFSET {}", offset));
    }

    Ok(sql)
}

/// Group nested query results by their parent FK value
pub fn group_nested_results(
    rows: Vec<serde_json::Map<String, JsonValue>>,
    nested: &NestedQueryInfo,
) -> HashMap<String, Vec<serde_json::Map<String, JsonValue>>> {
    let mut grouped: HashMap<String, Vec<serde_json::Map<String, JsonValue>>> = HashMap::new();

    let group_column = match nested.direction {
        RelationshipDirection::OneToMany => {
            // Group by FK column in child rows
            nested.fk_columns.first().map(|s| s.as_str())
        }
        RelationshipDirection::ManyToOne => {
            // Group by PK column in parent rows
            nested.pk_columns.first().map(|s| s.as_str())
        }
    };

    if let Some(col) = group_column {
        for row in rows {
            // Find the grouping value (case-insensitive)
            let key = row
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(col))
                .map(|(_, v)| value_to_string(v))
                .unwrap_or_default();

            grouped.entry(key).or_default().push(row);
        }
    }

    grouped
}

/// Convert a JSON value to a string key for grouping
fn value_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        _ => format!("{}", v),
    }
}

/// Attach nested results to parent rows
pub fn attach_nested_results(
    parent_rows: &mut [serde_json::Map<String, JsonValue>],
    nested: &NestedQueryInfo,
    nested_grouped: HashMap<String, Vec<serde_json::Map<String, JsonValue>>>,
) {
    let key_column = match nested.direction {
        RelationshipDirection::OneToMany => {
            // Parent's PK matches child's FK
            nested.pk_columns.first().map(|s| s.as_str())
        }
        RelationshipDirection::ManyToOne => {
            // Parent's FK matches child's PK
            nested.fk_columns.first().map(|s| s.as_str())
        }
    };

    if let Some(col) = key_column {
        for row in parent_rows.iter_mut() {
            // Find the key value in the parent row (case-insensitive)
            let key = row
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(col))
                .map(|(_, v)| value_to_string(v))
                .unwrap_or_default();

            // Get the nested rows for this key
            let nested_rows = nested_grouped.get(&key).cloned().unwrap_or_default();

            // Attach based on relationship direction
            match nested.direction {
                RelationshipDirection::OneToMany => {
                    // One parent has many children - attach as array
                    let nested_array: Vec<JsonValue> =
                        nested_rows.into_iter().map(JsonValue::Object).collect();
                    row.insert(nested.field_name.clone(), JsonValue::Array(nested_array));
                }
                RelationshipDirection::ManyToOne => {
                    // Many children have one parent - attach as single object or null
                    let nested_value = nested_rows
                        .into_iter()
                        .next()
                        .map(JsonValue::Object)
                        .unwrap_or(JsonValue::Null);
                    row.insert(nested.field_name.clone(), nested_value);
                }
            }
        }
    }
}

/// Check if a query has nested fields that need relationship resolution
pub fn has_nested_fields(query_info: &GraphQLQueryInfo) -> bool {
    match query_info {
        GraphQLQueryInfo::Query { nested_fields, .. } => {
            nested_fields.iter().any(|f| f.nested.is_some())
        }
        GraphQLQueryInfo::Mutation { .. } => false,
    }
}

/// Get simple column names from nested fields (excluding relationship fields)
pub fn get_simple_columns(fields: &[GraphQLField]) -> Vec<String> {
    fields.iter().filter(|f| f.nested.is_none()).map(|f| f.name.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::ForeignKeyConstraint;

    #[test]
    fn test_parse_simple_query() {
        let query = "{ users { id, name } }";
        let result = parse_graphql_query(query);
        assert!(result.is_ok());
        if let GraphQLQueryInfo::Query { table_name, fields, nested_fields, .. } = result.unwrap() {
            assert_eq!(table_name, "users");
            assert!(fields.contains(&"id".to_string()));
            assert!(fields.contains(&"name".to_string()));
            assert_eq!(nested_fields.len(), 2);
            assert!(nested_fields.iter().all(|f| f.nested.is_none()));
        }
    }

    #[test]
    fn test_parse_query_with_raw_where() {
        let query = r#"{ users(where: "id = 1") { id, name } }"#;
        let result = parse_graphql_query(query);
        assert!(result.is_ok());
        if let GraphQLQueryInfo::Query { where_clause_raw, .. } = result.unwrap() {
            assert_eq!(where_clause_raw, Some("id = 1".to_string()));
        }
    }

    #[test]
    fn test_parse_nested_query() {
        let query = "{ users { id name posts { id title } } }";
        let result = parse_graphql_query(query);
        assert!(result.is_ok());

        if let GraphQLQueryInfo::Query { nested_fields, .. } = result.unwrap() {
            // Should have 3 fields: id, name, posts
            assert_eq!(nested_fields.len(), 3);

            // Find the posts field
            let posts_field = nested_fields.iter().find(|f| f.name == "posts").unwrap();
            assert!(posts_field.nested.is_some());

            let posts_nested = posts_field.nested.as_ref().unwrap();
            assert_eq!(posts_nested.len(), 2);
            assert_eq!(posts_nested[0].name, "id");
            assert_eq!(posts_nested[1].name, "title");
        }
    }

    // Comparison operator tests
    #[test]
    fn test_where_clause_eq() {
        let json: JsonValue = serde_json::json!({"id": {"eq": 1}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "id = $1");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_where_clause_ne() {
        let json: JsonValue = serde_json::json!({"status": {"ne": "inactive"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "status <> $1");
    }

    #[test]
    fn test_where_clause_gt() {
        let json: JsonValue = serde_json::json!({"age": {"gt": 18}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "age > $1");
    }

    #[test]
    fn test_where_clause_gte() {
        let json: JsonValue = serde_json::json!({"age": {"gte": 21}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "age >= $1");
    }

    #[test]
    fn test_where_clause_lt() {
        let json: JsonValue = serde_json::json!({"price": {"lt": 100}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "price < $1");
    }

    #[test]
    fn test_where_clause_lte() {
        let json: JsonValue = serde_json::json!({"quantity": {"lte": 50}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "quantity <= $1");
    }

    // String operator tests
    #[test]
    fn test_where_clause_like() {
        let json: JsonValue = serde_json::json!({"name": {"like": "%john%"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "name LIKE $1");
    }

    #[test]
    fn test_where_clause_ilike() {
        let json: JsonValue = serde_json::json!({"name": {"ilike": "%JOHN%"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "LOWER(name) LIKE LOWER($1)");
    }

    #[test]
    fn test_where_clause_contains() {
        let json: JsonValue = serde_json::json!({"email": {"contains": "smith"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "email LIKE $1");
        if let vibesql_types::SqlValue::Varchar(s) = &params[0] {
            assert_eq!(s.as_str(), "%smith%");
        }
    }

    #[test]
    fn test_where_clause_starts_with() {
        let json: JsonValue = serde_json::json!({"name": {"startsWith": "Dr."}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "name LIKE $1");
        if let vibesql_types::SqlValue::Varchar(s) = &params[0] {
            assert_eq!(s.as_str(), "Dr.%");
        }
    }

    #[test]
    fn test_where_clause_ends_with() {
        let json: JsonValue = serde_json::json!({"email": {"endsWith": "@example.com"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "email LIKE $1");
        if let vibesql_types::SqlValue::Varchar(s) = &params[0] {
            assert_eq!(s.as_str(), "%@example.com");
        }
    }

    // List operator tests
    #[test]
    fn test_where_clause_in() {
        let json: JsonValue = serde_json::json!({"status": {"in": ["active", "pending"]}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "status IN ($1, $2)");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_where_clause_not_in() {
        let json: JsonValue = serde_json::json!({"id": {"notIn": [1, 2, 3]}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "id NOT IN ($1, $2, $3)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_where_clause_in_empty() {
        let json: JsonValue = serde_json::json!({"id": {"in": []}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "FALSE");
    }

    #[test]
    fn test_where_clause_not_in_empty() {
        let json: JsonValue = serde_json::json!({"id": {"notIn": []}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "TRUE");
    }

    // Null operator tests
    #[test]
    fn test_where_clause_is_null_true() {
        let json: JsonValue = serde_json::json!({"deleted_at": {"isNull": true}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "deleted_at IS NULL");
    }

    #[test]
    fn test_where_clause_is_null_false() {
        let json: JsonValue = serde_json::json!({"updated_at": {"isNull": false}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "updated_at IS NOT NULL");
    }

    #[test]
    fn test_where_clause_eq_null() {
        let json: JsonValue = serde_json::json!({"field": null});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "field IS NULL");
    }

    #[test]
    fn test_where_clause_ne_null() {
        let json: JsonValue = serde_json::json!({"field": {"ne": null}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "field IS NOT NULL");
    }

    // Logical combinator tests
    #[test]
    fn test_where_clause_and() {
        let json: JsonValue = serde_json::json!({
            "AND": [
                {"age": {"gte": 18}},
                {"status": "active"}
            ]
        });
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "(age >= $1 AND status = $2)");
    }

    #[test]
    fn test_where_clause_or() {
        let json: JsonValue = serde_json::json!({
            "OR": [
                {"name": {"contains": "smith"}},
                {"email": {"endsWith": "@company.com"}}
            ]
        });
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "(name LIKE $1 OR email LIKE $2)");
    }

    #[test]
    fn test_where_clause_not() {
        let json: JsonValue = serde_json::json!({
            "NOT": {"status": "deleted"}
        });
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "NOT status = $1");
    }

    #[test]
    fn test_where_clause_complex_nested() {
        let json: JsonValue = serde_json::json!({
            "age": {"gte": 18},
            "OR": [
                {"name": {"contains": "smith"}},
                {"email": {"endsWith": "@company.com"}}
            ]
        });
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        // Multiple top-level conditions are combined with AND
        // Due to non-deterministic JSON object ordering, check for presence of expected patterns
        assert!(sql.contains("age >="), "SQL should contain 'age >='");
        assert!(sql.contains(" AND "), "SQL should contain ' AND '");
        assert!(sql.contains(" OR "), "SQL should contain ' OR '");
        assert!(params.len() >= 3, "Should have at least 3 parameters");
    }

    // Direct equality tests
    #[test]
    fn test_where_clause_direct_string() {
        let json: JsonValue = serde_json::json!({"status": "active"});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "status = $1");
    }

    #[test]
    fn test_where_clause_direct_number() {
        let json: JsonValue = serde_json::json!({"id": 42});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "id = $1");
    }

    #[test]
    fn test_where_clause_direct_array_as_in() {
        let json: JsonValue = serde_json::json!({"id": [1, 2, 3]});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let sql = where_clause_to_sql(&clause, &mut params).unwrap();
        assert_eq!(sql, "id IN ($1, $2, $3)");
    }

    // GraphQL to SQL integration tests
    #[test]
    fn test_graphql_to_sql_with_structured_where() {
        let query = r#"{ users(where: {"age": {"gte": 18}, "status": "active"}) { id, name } }"#;
        let result = parse_graphql_query(query).unwrap();
        let (sql, params) = graphql_to_sql(&result).unwrap();
        // Check key parts are present (order may vary due to HashMap)
        assert!(sql.starts_with("SELECT"), "SQL should start with SELECT");
        assert!(sql.contains("FROM users"), "SQL should contain 'FROM users'");
        assert!(sql.contains("WHERE"), "SQL should contain WHERE");
        assert!(sql.contains("age >="), "SQL should contain 'age >='");
        assert!(sql.contains("status ="), "SQL should contain 'status ='");
        assert_eq!(params.len(), 2, "Should have 2 parameters");
    }

    #[test]
    fn test_graphql_to_sql_with_or() {
        let query = r#"{ users(where: {"OR": [{"name": {"contains": "john"}}, {"email": {"endsWith": "@test.com"}}]}) { id, name } }"#;
        let result = parse_graphql_query(query).unwrap();
        let (sql, _) = graphql_to_sql(&result).unwrap();
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_graphql_with_limit_offset() {
        let query = r#"{ users(limit: 10, offset: 20) { id, name } }"#;
        let result = parse_graphql_query(query).unwrap();
        let (sql, _) = graphql_to_sql(&result).unwrap();
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
    }

    // Error handling tests
    #[test]
    fn test_unknown_operator_error() {
        let json: JsonValue = serde_json::json!({"id": {"unknownOp": 1}});
        let result = parse_where_clause(&json);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown operator"));
    }

    #[test]
    fn test_in_requires_array() {
        let json: JsonValue = serde_json::json!({"id": {"in": "not_an_array"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let result = where_clause_to_sql(&clause, &mut params);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_null_requires_boolean() {
        let json: JsonValue = serde_json::json!({"field": {"isNull": "not_a_boolean"}});
        let clause = parse_where_clause(&json).unwrap().unwrap();
        let mut params = Vec::new();
        let result = where_clause_to_sql(&clause, &mut params);
        assert!(result.is_err());
    }

    // Identifier escaping tests
    #[test]
    fn test_escape_simple_identifier() {
        assert_eq!(escape_identifier("user_name"), "user_name");
    }

    #[test]
    fn test_escape_identifier_with_special_chars() {
        assert_eq!(escape_identifier("user-name"), "\"user-name\"");
    }

    #[test]
    fn test_escape_identifier_with_quotes() {
        assert_eq!(escape_identifier("user\"name"), "\"user\"\"name\"");
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

    // Nested query tests (relationship resolution)
    #[test]
    fn test_parse_deep_nested_query() {
        let query = "{ users { id posts { id comments { id body } } } }";
        let result = parse_graphql_query(query);
        assert!(result.is_ok());

        if let GraphQLQueryInfo::Query { nested_fields, .. } = result.unwrap() {
            let posts_field = nested_fields.iter().find(|f| f.name == "posts").unwrap();
            let posts_nested = posts_field.nested.as_ref().unwrap();

            let comments_field = posts_nested.iter().find(|f| f.name == "comments").unwrap();
            assert!(comments_field.nested.is_some());

            let comments_nested = comments_field.nested.as_ref().unwrap();
            assert_eq!(comments_nested.len(), 2);
        }
    }

    #[test]
    fn test_build_relationship_map() {
        use vibesql_catalog::{ColumnSchema, ReferentialAction};
        use vibesql_types::DataType;

        let mut schemas = HashMap::new();

        // Create users table
        let users_schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    false,
                ),
            ],
        );
        schemas.insert("users".to_string(), users_schema);

        // Create posts table with FK to users
        let mut posts_schema = TableSchema::new(
            "posts".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "title".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    false,
                ),
                ColumnSchema::new("user_id".to_string(), DataType::Integer, false),
            ],
        );
        posts_schema.foreign_keys.push(ForeignKeyConstraint {
            name: Some("fk_posts_user".to_string()),
            column_names: vec!["user_id".to_string()],
            column_indices: vec![2],
            parent_table: "users".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        });
        schemas.insert("posts".to_string(), posts_schema);

        let relationships = build_relationship_map(&schemas);

        // Users should have one-to-many relationship to posts
        let user_rels = relationships.get("users").unwrap();
        assert_eq!(user_rels.len(), 1);
        assert_eq!(user_rels[0].related_table, "posts");
        assert_eq!(user_rels[0].direction, RelationshipDirection::OneToMany);

        // Posts should have many-to-one relationship to users
        let post_rels = relationships.get("posts").unwrap();
        assert_eq!(post_rels.len(), 1);
        assert_eq!(post_rels[0].related_table, "users");
        assert_eq!(post_rels[0].direction, RelationshipDirection::ManyToOne);
    }

    #[test]
    fn test_generate_nested_query_sql() {
        let nested = NestedQueryInfo {
            field_name: "posts".to_string(),
            related_table: "posts".to_string(),
            select_columns: vec!["id".to_string(), "title".to_string()],
            fk_columns: vec!["user_id".to_string()],
            pk_columns: vec!["id".to_string()],
            direction: RelationshipDirection::OneToMany,
            nested: vec![],
            where_clause: None,
            limit: None,
            offset: None,
        };

        let parent_values = vec![
            JsonValue::Number(serde_json::Number::from(1)),
            JsonValue::Number(serde_json::Number::from(2)),
            JsonValue::Number(serde_json::Number::from(3)),
        ];

        let sql = generate_nested_query_sql(&nested, &parent_values).unwrap();
        assert!(sql.contains("SELECT id, title, user_id FROM posts"));
        assert!(sql.contains("WHERE user_id IN (1, 2, 3)"));
    }

    #[test]
    fn test_has_nested_fields() {
        let simple_query = parse_graphql_query("{ users { id name } }").unwrap();
        assert!(!has_nested_fields(&simple_query));

        let nested_query = parse_graphql_query("{ users { id posts { id } } }").unwrap();
        assert!(has_nested_fields(&nested_query));
    }
}
