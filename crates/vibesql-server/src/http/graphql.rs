//! GraphQL API implementation for VibeSQL HTTP interface
//!
//! This provides a lightweight GraphQL-like interface over HTTP without a full GraphQL library.
//! It supports queries and mutations on database tables with basic filtering.

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
        where_clause: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Mutation {
        operation_type: MutationType,
        table_name: String,
        data: Option<serde_json::Map<String, JsonValue>>,
        where_clause: Option<String>,
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

    // Try to extract where clause
    let where_clause = extract_where_clause(content);

    Ok(GraphQLQueryInfo::Query {
        table_name,
        fields,
        where_clause,
        limit: None,
        offset: None,
    })
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
    })
}

fn parse_graphql_update_mutation(mutation: &str) -> Result<GraphQLQueryInfo, String> {
    let table_part = extract_quoted_value(mutation, "table")
        .ok_or("Missing table name in update mutation")?;

    let data = extract_json_value(mutation, "values");
    let where_clause = extract_quoted_value(mutation, "where");

    Ok(GraphQLQueryInfo::Mutation {
        operation_type: MutationType::Update,
        table_name: table_part,
        data,
        where_clause,
    })
}

fn parse_graphql_delete_mutation(mutation: &str) -> Result<GraphQLQueryInfo, String> {
    let table_part = extract_quoted_value(mutation, "table")
        .ok_or("Missing table name in delete mutation")?;

    let where_clause = extract_quoted_value(mutation, "where");

    Ok(GraphQLQueryInfo::Mutation {
        operation_type: MutationType::Delete,
        table_name: table_part,
        data: None,
        where_clause,
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

/// Extract WHERE clause from a query
fn extract_where_clause(query: &str) -> Option<String> {
    extract_quoted_value(query, "where")
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
            limit,
            offset,
        } => {
            let select_list = if fields.contains(&"*".to_string()) {
                "*".to_string()
            } else {
                fields.join(", ")
            };

            let mut sql = format!("SELECT {} FROM {}", select_list, table_name);

            if let Some(where_clause) = where_clause {
                sql.push_str(&format!(" WHERE {}", where_clause));
            }

            if let Some(limit) = limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }

            if let Some(offset) = offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }

            Ok((sql, vec![]))
        }
        GraphQLQueryInfo::Mutation {
            operation_type,
            table_name,
            data,
            where_clause,
        } => match operation_type {
            MutationType::Insert => {
                if let Some(data) = data {
                    let columns: Vec<String> = data.keys().cloned().collect();
                    let placeholders = (0..columns.len())
                        .map(|i| format!("${}", i + 1))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let sql = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        table_name,
                        columns.join(", "),
                        placeholders
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
                    let set_clause = data
                        .keys()
                        .enumerate()
                        .map(|(i, col)| format!("{} = ${}", col, i + 1))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let mut sql = format!("UPDATE {} SET {}", table_name, set_clause);

                    if let Some(where_clause) = where_clause {
                        sql.push_str(&format!(" WHERE {}", where_clause));
                    }

                    let mut params = Vec::new();
                    for col in data.keys() {
                        if let Some(val) = data.get(col) {
                            params.push(json_to_sql_value(val)?);
                        }
                    }

                    Ok((sql, params))
                } else {
                    Err("UPDATE requires data".to_string())
                }
            }
            MutationType::Delete => {
                let mut sql = format!("DELETE FROM {}", table_name);

                if let Some(where_clause) = where_clause {
                    sql.push_str(&format!(" WHERE {}", where_clause));
                } else {
                    return Err("DELETE requires WHERE clause".to_string());
                }

                Ok((sql, vec![]))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let query = "{ users { id name } }";
        let result = parse_graphql_query(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_query_with_where() {
        let query = r#"{ users(where: "id = 1") { id name } }"#;
        let result = parse_graphql_query(query);
        assert!(result.is_ok());
    }
}
