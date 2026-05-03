//! GraphQL relationship resolution and nested query handling
//!
//! This module handles building relationship maps from table schemas,
//! generating SQL for nested queries, and attaching nested results to parent rows.

use std::collections::HashMap;

use serde_json::Value as JsonValue;
use vibesql_catalog::TableSchema;

use super::parser::GraphQLField;

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

/// Direction of a table relationship
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

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, ForeignKeyConstraint, ReferentialAction};
    use vibesql_types::DataType;

    use super::*;

    #[test]
    fn test_build_relationship_map() {
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
            is_deferrable: false,
            initially_deferred: false,
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
}
