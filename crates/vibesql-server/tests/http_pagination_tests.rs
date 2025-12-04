//! Integration tests for HTTP API pagination support
//!
//! Tests pagination with limit and offset parameters on HTTP API queries.

use std::sync::Arc;
use vibesql_server::http::{create_http_router, types::QueryRequest};
use vibesql_storage::Database;
use axum_test::TestServer;

/// Helper to create a test database with sample data
async fn setup_test_database() -> Arc<Database> {
    let db = Arc::new(Database::new_memory().expect("Failed to create database"));
    
    // Create a test table with sample data
    let mut session = vibesql_server::session::Session::new("test".to_string(), "test_user".to_string())
        .expect("Failed to create session");
    
    // Create table
    let _ = session.execute("CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(100))")
        .expect("Failed to create table");
    
    // Insert sample data
    for i in 1..=25 {
        let name = format!("User{}", i);
        let email = format!("user{}@example.com", i);
        let query = format!(
            "INSERT INTO users VALUES ({}, '{}', '{}')",
            i, name, email
        );
        let _ = session.execute(&query).expect("Failed to insert");
    }
    
    db
}

#[tokio::test]
async fn test_pagination_with_limit() {
    let db = setup_test_database().await;
    let app = create_http_router(db);
    let server = TestServer::new(app).expect("Failed to create test server");
    
    // Query with limit of 5
    let response = server
        .post("/api/query")
        .json(&QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            limit: Some(5),
            offset: None,
        })
        .await;
    
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    
    let body: serde_json::Value = response.json();
    let row_count = body["row_count"].as_u64().expect("Missing row_count");
    let total_count = body["total_count"].as_u64().expect("Missing total_count");
    
    assert_eq!(row_count, 5, "Should return 5 rows");
    assert_eq!(total_count, 25, "Total count should be 25");
}

#[tokio::test]
async fn test_pagination_with_offset() {
    let db = setup_test_database().await;
    let app = create_http_router(db);
    let server = TestServer::new(app).expect("Failed to create test server");
    
    // Query with offset of 20
    let response = server
        .post("/api/query")
        .json(&QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            limit: None,
            offset: Some(20),
        })
        .await;
    
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    
    let body: serde_json::Value = response.json();
    let row_count = body["row_count"].as_u64().expect("Missing row_count");
    let offset = body["offset"].as_u64().expect("Missing offset");
    
    assert_eq!(row_count, 5, "Should return 5 rows (25 - 20 offset)");
    assert_eq!(offset, 20, "Offset should be 20");
}

#[tokio::test]
async fn test_pagination_with_limit_and_offset() {
    let db = setup_test_database().await;
    let app = create_http_router(db);
    let server = TestServer::new(app).expect("Failed to create test server");
    
    // Query with limit 10 and offset 5
    let response = server
        .post("/api/query")
        .json(&QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            limit: Some(10),
            offset: Some(5),
        })
        .await;
    
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    
    let body: serde_json::Value = response.json();
    let row_count = body["row_count"].as_u64().expect("Missing row_count");
    let total_count = body["total_count"].as_u64().expect("Missing total_count");
    let limit = body["limit"].as_u64().expect("Missing limit");
    let offset = body["offset"].as_u64().expect("Missing offset");
    
    assert_eq!(row_count, 10, "Should return 10 rows");
    assert_eq!(total_count, 25, "Total count should be 25");
    assert_eq!(limit, 10, "Limit should be 10");
    assert_eq!(offset, 5, "Offset should be 5");
}

#[tokio::test]
async fn test_pagination_offset_exceeds_total() {
    let db = setup_test_database().await;
    let app = create_http_router(db);
    let server = TestServer::new(app).expect("Failed to create test server");
    
    // Query with offset greater than total rows
    let response = server
        .post("/api/query")
        .json(&QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            limit: Some(10),
            offset: Some(100),
        })
        .await;
    
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    
    let body: serde_json::Value = response.json();
    let row_count = body["row_count"].as_u64().expect("Missing row_count");
    let total_count = body["total_count"].as_u64().expect("Missing total_count");
    
    assert_eq!(row_count, 0, "Should return 0 rows");
    assert_eq!(total_count, 25, "Total count should still be 25");
}

#[tokio::test]
async fn test_pagination_without_params() {
    let db = setup_test_database().await;
    let app = create_http_router(db);
    let server = TestServer::new(app).expect("Failed to create test server");
    
    // Query without pagination parameters
    let response = server
        .post("/api/query")
        .json(&QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            limit: None,
            offset: None,
        })
        .await;
    
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    
    let body: serde_json::Value = response.json();
    let row_count = body["row_count"].as_u64().expect("Missing row_count");
    let total_count = body["total_count"].as_u64().expect("Missing total_count");
    
    assert_eq!(row_count, 25, "Should return all 25 rows");
    assert_eq!(total_count, 25, "Total count should be 25");
    
    // offset and limit should not be present when not provided
    assert!(body["offset"].is_null(), "offset should be None when not provided");
    assert!(body["limit"].is_null(), "limit should be None when not provided");
}
