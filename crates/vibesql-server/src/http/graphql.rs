//! GraphQL API implementation for VibeSQL HTTP interface
//!
//! This provides a lightweight GraphQL-like interface over HTTP without a full GraphQL library.
//! It supports queries and mutations on database tables with structured filtering.
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

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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
        Self {
            message: message.into(),
            extensions: None,
        }
    }
}

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
        JsonValue::String(_)
        | JsonValue::Number(_)
        | JsonValue::Bool(_)
        | JsonValue::Null => {
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
            let sql_parts: Result<Vec<String>, String> = clauses
                .iter()
                .map(|c| where_clause_to_sql(c, params))
                .collect();
            let parts = sql_parts?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        WhereClause::Or(clauses) => {
            let sql_parts: Result<Vec<String>, String> = clauses
                .iter()
                .map(|c| where_clause_to_sql(c, params))
                .collect();
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
            let value_str = cond
                .value
                .as_str()
                .ok_or("contains requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(format!("%{}%", value_str)));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::StartsWith => {
            let value_str = cond
                .value
                .as_str()
                .ok_or("startsWith requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(format!("{}%", value_str)));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::EndsWith => {
            let value_str = cond
                .value
                .as_str()
                .ok_or("endsWith requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(format!("%{}", value_str)));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::In => {
            let arr = cond
                .value
                .as_array()
                .ok_or("IN requires an array value")?;
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
            let arr = cond
                .value
                .as_array()
                .ok_or("NOT IN requires an array value")?;
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
            let is_null = cond
                .value
                .as_bool()
                .ok_or("isNull requires a boolean value")?;
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
    if identifier
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        identifier.to_string()
    } else {
        // Quote the identifier if it contains special characters
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }
}

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
        fields: Vec<String>,
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

/// Parse a GraphQL select-style query
fn parse_graphql_select_query(query: &str) -> Result<GraphQLQueryInfo, String> {
    // Simple parser for: { users { id name email } }
    // Or with filters: { users(where: {id: 1}) { id name } }
    // Or with string filter: { users(where: "id = 1") { id name } }

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
    let fields_start = content.find('{').ok_or("Missing field list")?;
    let fields_end = content[fields_start + 1..]
        .find('}')
        .ok_or("Missing closing brace for fields")?;

    let fields_content = &content[fields_start + 1..fields_start + 1 + fields_end];
    let fields: Vec<String> = fields_content
        .split(',')
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect();

    // Try to extract structured where clause (JSON object)
    let (where_clause, where_clause_raw) = extract_where_clauses(content)?;

    // Extract limit and offset
    let limit = extract_numeric_param(content, "limit");
    let offset = extract_numeric_param(content, "offset");

    Ok(GraphQLQueryInfo::Query {
        table_name,
        fields,
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
    let table_part = extract_quoted_value(mutation, "table")
        .ok_or("Missing table name in insert mutation")?;

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
    let table_part = extract_quoted_value(mutation, "table")
        .ok_or("Missing table name in update mutation")?;

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
    let table_part = extract_quoted_value(mutation, "table")
        .ok_or("Missing table name in delete mutation")?;

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
fn extract_json_value(
    input: &str,
    param_name: &str,
) -> Option<serde_json::Map<String, JsonValue>> {
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
    } else if content.starts_with('"') {
        // Raw string WHERE clause (legacy format)
        let stripped = &content[1..];
        if let Some(end) = stripped.find('"') {
            Ok((None, Some(stripped[..end].to_string())))
        } else {
            Err("Unclosed quote in WHERE clause".to_string())
        }
    } else {
        Err("WHERE clause must be a JSON object or quoted string".to_string())
    }
}

/// Convert GraphQL query info to SQL
pub fn graphql_to_sql(
    query_info: &GraphQLQueryInfo,
) -> Result<(String, Vec<vibesql_types::SqlValue>), String> {
    match query_info {
        GraphQLQueryInfo::Query {
            table_name,
            fields,
            where_clause,
            where_clause_raw,
            limit,
            offset,
        } => {
            let select_list = if fields.contains(&"*".to_string()) {
                "*".to_string()
            } else {
                fields
                    .iter()
                    .map(|f| escape_identifier(f))
                    .collect::<Vec<_>>()
                    .join(", ")
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
                            .map(|(i, col)| {
                                format!("{} = ${}", escape_identifier(col), i + 1)
                            })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let query = "{ users { id, name } }";
        let result = parse_graphql_query(query);
        assert!(result.is_ok());
        if let GraphQLQueryInfo::Query { table_name, fields, .. } = result.unwrap() {
            assert_eq!(table_name, "users");
            assert!(fields.contains(&"id".to_string()));
            assert!(fields.contains(&"name".to_string()));
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
            assert_eq!(s, "%smith%");
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
            assert_eq!(s, "Dr.%");
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
            assert_eq!(s, "%@example.com");
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
}
