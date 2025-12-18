//! Query execution for WASM bindings
//!
//! This module handles SELECT query execution and result serialization.

use wasm_bindgen::prelude::*;

use crate::{Database, QueryResult};

/// Executes a SELECT query and returns results as JSON
pub fn execute_query(db: &Database, sql: &str) -> Result<JsValue, JsValue> {
    // Parse the SQL
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;

    // Ensure it's a SELECT statement
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => return Err(JsValue::from_str("query() method requires a SELECT statement")),
    };

    // Execute the query with column metadata
    let select_executor = vibesql_executor::SelectExecutor::new(&db.db);
    let result = select_executor
        .execute_with_columns(&select_stmt)
        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

    let columns = result.columns;
    let rows = result.rows;

    // Convert rows to JSON strings
    let row_strings: Vec<String> = rows
        .iter()
        .map(|row| {
            // Convert each SqlValue to a JSON-compatible representation
            let json_values: Vec<serde_json::Value> = row
                .values
                .iter()
                .map(|v| match v {
                    vibesql_types::SqlValue::Integer(i) => serde_json::Value::Number((*i).into()),
                    vibesql_types::SqlValue::Smallint(i) => serde_json::Value::Number((*i).into()),
                    vibesql_types::SqlValue::Bigint(i) => serde_json::Value::Number((*i).into()),
                    vibesql_types::SqlValue::Unsigned(u) => serde_json::Value::Number((*u).into()),
                    vibesql_types::SqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    vibesql_types::SqlValue::Real(f) => serde_json::Number::from_f64(*f as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    vibesql_types::SqlValue::Double(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        serde_json::Value::String(s.to_string())
                    }
                    vibesql_types::SqlValue::Boolean(b) => serde_json::Value::Bool(*b),
                    vibesql_types::SqlValue::Numeric(n) => serde_json::Number::from_f64(*n)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    vibesql_types::SqlValue::Date(d) => serde_json::Value::String(d.to_string()),
                    vibesql_types::SqlValue::Time(t) => serde_json::Value::String(t.to_string()),
                    vibesql_types::SqlValue::Timestamp(ts) => {
                        serde_json::Value::String(ts.to_string())
                    }
                    vibesql_types::SqlValue::Interval(i) => {
                        serde_json::Value::String(i.to_string())
                    }
                    vibesql_types::SqlValue::Vector(v) => {
                        // Serialize vector as JSON array of floats
                        serde_json::Value::Array(
                            v.iter()
                                .map(|f| {
                                    serde_json::Number::from_f64(*f as f64)
                                        .map(serde_json::Value::Number)
                                        .unwrap_or(serde_json::Value::Null)
                                })
                                .collect(),
                        )
                    }
                    vibesql_types::SqlValue::Blob(b) => {
                        // Serialize blob as hex string with x'' prefix
                        let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                        serde_json::Value::String(format!("x'{}'", hex))
                    }
                    vibesql_types::SqlValue::Null => serde_json::Value::Null,
                })
                .collect();

            serde_json::to_string(&json_values).unwrap_or_else(|_| "[]".to_string())
        })
        .collect();

    let result = QueryResult { columns, rows: row_strings, row_count: rows.len() };

    serde_wasm_bindgen::to_value(&result)
        .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
}
