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

mod introspection;
mod parser;
mod relationships;
mod types;
mod where_clause;

// Re-export public types and functions
pub use introspection::try_introspection_query;
pub use parser::{
    generate_main_query_sql, get_simple_columns, graphql_to_sql, has_nested_fields,
    parse_graphql_query, GraphQLField, GraphQLQueryInfo, MutationType,
};
pub use relationships::{
    attach_nested_results, build_nested_queries, build_relationship_map, find_relationship,
    generate_nested_query_sql, group_nested_results, GraphQLExecutionContext, NestedQueryInfo,
    RelationshipDirection, TableRelationship,
};
pub use types::{GraphQLError, GraphQLRequest, GraphQLResponse};
pub use where_clause::{
    escape_identifier, parse_where_clause, where_clause_to_sql, ComparisonOp, FieldCondition,
    WhereClause,
};
