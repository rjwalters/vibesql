//! Extract table dependencies from SQL queries

use std::collections::HashSet;

/// Extract all table names referenced in a SQL query
///
/// This is a simplified implementation that handles common SQL patterns.
/// For a production system, this should use the full SQL parser.
pub fn extract_table_dependencies(query: &str) -> Result<HashSet<String>, String> {
    let query_lower = query.to_lowercase();
    let mut tables = HashSet::new();

    // Simple regex-like pattern matching for common SQL keywords
    // This is a basic implementation; a real implementation should use a proper SQL parser

    // Find FROM clauses
    let parts: Vec<&str> = query_lower.split_whitespace().collect();
    let mut i = 0;
    while i < parts.len() {
        match parts[i] {
            "from" | "join" | "inner" | "left" | "right" | "full" | "cross" => {
                // Try to extract table name after FROM/JOIN
                if i + 1 < parts.len() {
                    let next_word = parts[i + 1];

                    // Skip if it's a keyword
                    if !is_sql_keyword(next_word) {
                        // Extract table name (remove schema prefix, alias, etc.)
                        let table_name = extract_table_name(next_word);
                        if !table_name.is_empty() {
                            tables.insert(table_name);
                        }
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }

    // If we couldn't extract any tables, return an error
    if tables.is_empty() {
        // Try to give a more useful error
        if query_lower.contains("from") || query_lower.contains("join") {
            // We found FROM/JOIN but couldn't parse it
            return Err("Could not parse table dependencies from query".to_string());
        } else {
            // Might be a statement without FROM (like INSERT, UPDATE, DELETE, etc.)
            // Try to extract from those
            tables = extract_from_dml(query)?;
        }
    }

    Ok(tables)
}

/// Extract table name from a potentially quoted or aliased identifier
fn extract_table_name(ident: &str) -> String {
    // Remove trailing commas, parentheses, etc.
    let cleaned = ident
        .trim_end_matches(',')
        .trim_end_matches('(')
        .trim_end_matches(')')
        .trim_end_matches(';');

    // Remove quotes
    let unquoted = cleaned
        .trim_start_matches('"')
        .trim_end_matches('"')
        .trim_start_matches('`')
        .trim_end_matches('`')
        .trim_start_matches('\'')
        .trim_end_matches('\'');

    // Extract schema.table (take just the table part if schema is present)
    if let Some(dot_idx) = unquoted.rfind('.') {
        unquoted[dot_idx + 1..].to_string()
    } else {
        unquoted.to_string()
    }
}

/// Extract table names from DML statements (INSERT, UPDATE, DELETE)
fn extract_from_dml(query: &str) -> Result<HashSet<String>, String> {
    let query_lower = query.to_lowercase().trim().to_string();
    let mut tables = HashSet::new();

    if query_lower.starts_with("insert into") {
        // INSERT INTO table_name ...
        let parts: Vec<&str> = query_lower.split_whitespace().collect();
        if parts.len() >= 3 {
            let table = extract_table_name(parts[2]);
            if !table.is_empty() {
                tables.insert(table);
                return Ok(tables);
            }
        }
    } else if query_lower.starts_with("update") {
        // UPDATE table_name ...
        let parts: Vec<&str> = query_lower.split_whitespace().collect();
        if parts.len() >= 2 {
            let table = extract_table_name(parts[1]);
            if !table.is_empty() {
                tables.insert(table);
                return Ok(tables);
            }
        }
    } else if query_lower.starts_with("delete from") {
        // DELETE FROM table_name ...
        let parts: Vec<&str> = query_lower.split_whitespace().collect();
        if parts.len() >= 3 {
            let table = extract_table_name(parts[2]);
            if !table.is_empty() {
                tables.insert(table);
                return Ok(tables);
            }
        }
    }

    if tables.is_empty() {
        Err("Could not extract table names from query".to_string())
    } else {
        Ok(tables)
    }
}

/// Check if a word is a SQL keyword
fn is_sql_keyword(word: &str) -> bool {
    // Check for JOIN-related keywords
    if word == "join" || word == "left" || word == "right" || word == "inner" || word == "full" 
        || word == "cross" || word == "outer" || word == "natural" {
        return true;
    }
    
    // Check for other SQL keywords
    matches!(
        word,
        "where" | "and" | "or" | "not" | "in" | "on" | "as" | "group" | "by" | "order"
            | "having" | "limit" | "offset" | "union" | "intersect" | "except" | "select"
            | "using" | "case" | "when" | "then" | "else" | "end" | "between" | "like"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let deps = extract_table_dependencies("SELECT * FROM users").unwrap();
        let expected: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_select_with_alias() {
        let deps = extract_table_dependencies("SELECT * FROM users u").unwrap();
        let expected: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_select_with_schema() {
        let deps = extract_table_dependencies("SELECT * FROM public.users").unwrap();
        let expected: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_join() {
        let deps =
            extract_table_dependencies("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
                .unwrap();
        assert_eq!(
            deps,
            vec!["users".to_string(), "orders".to_string()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn test_multiple_joins() {
        let deps = extract_table_dependencies(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id"
        ).unwrap();
        let expected: HashSet<_> =
            vec!["users".to_string(), "orders".to_string(), "products".to_string()]
                .into_iter()
                .collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_insert_statement() {
        let deps = extract_table_dependencies("INSERT INTO users (name) VALUES ('John')").unwrap();
        let expected: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_update_statement() {
        let deps = extract_table_dependencies("UPDATE users SET name = 'Jane'").unwrap();
        let expected: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_delete_statement() {
        let deps = extract_table_dependencies("DELETE FROM users WHERE id = 1").unwrap();
        let expected: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_quoted_table_names() {
        let deps = extract_table_dependencies("SELECT * FROM \"Users\"").unwrap();
        assert_eq!(deps.len(), 1);
        // Should handle quoted names
        assert!(deps.iter().any(|t| t.to_lowercase() == "users"));
    }

    #[test]
    fn test_left_join() {
        let deps = extract_table_dependencies(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        )
        .unwrap();
        assert_eq!(
            deps,
            vec!["users".to_string(), "orders".to_string()]
                .into_iter()
                .collect()
        );
    }
}
