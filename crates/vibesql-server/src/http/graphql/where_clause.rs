//! WHERE clause types, parsing, and SQL generation
//!
//! # WHERE Clause Operators
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

use serde_json::Value as JsonValue;

use crate::http::types::json_to_sql_value;

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
pub fn parse_where_clause(value: &JsonValue) -> Result<Option<WhereClause>, String> {
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
            params.push(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!(
                "%{}%",
                value_str
            ))));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::StartsWith => {
            let value_str = cond.value.as_str().ok_or("startsWith requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!(
                "{}%",
                value_str
            ))));
            Ok(format!("{} LIKE ${}", field, param_idx))
        }
        ComparisonOp::EndsWith => {
            let value_str = cond.value.as_str().ok_or("endsWith requires a string value")?;
            let param_idx = params.len() + 1;
            params.push(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!(
                "%{}",
                value_str
            ))));
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
pub fn escape_identifier(identifier: &str) -> String {
    // Basic identifier validation - only allow alphanumeric and underscore
    if identifier.chars().all(|c| c.is_alphanumeric() || c == '_') {
        identifier.to_string()
    } else {
        // Quote the identifier if it contains special characters
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
