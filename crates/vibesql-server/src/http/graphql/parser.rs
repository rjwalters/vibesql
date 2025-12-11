//! GraphQL query and mutation parsing
//!
//! This module handles parsing GraphQL query strings into structured representations
//! and converting them to SQL.

use serde_json::Value as JsonValue;

use super::where_clause::{escape_identifier, parse_where_clause, where_clause_to_sql, WhereClause};
use crate::http::types::json_to_sql_value;

/// Parsed GraphQL query information
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

/// Mutation operation type
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

/// Generate SQL for the main query (without JOINs - we'll use separate queries for nested data)
pub fn generate_main_query_sql(
    query_info: &GraphQLQueryInfo,
) -> Result<(String, Vec<vibesql_types::SqlValue>), String> {
    // Just delegate to the simple version for the main query
    graphql_to_sql(query_info)
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

    #[test]
    fn test_has_nested_fields() {
        let simple_query = parse_graphql_query("{ users { id name } }").unwrap();
        assert!(!has_nested_fields(&simple_query));

        let nested_query = parse_graphql_query("{ users { id posts { id } } }").unwrap();
        assert!(has_nested_fields(&nested_query));
    }
}
