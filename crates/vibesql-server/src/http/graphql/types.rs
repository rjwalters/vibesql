//! GraphQL request and response types

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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
