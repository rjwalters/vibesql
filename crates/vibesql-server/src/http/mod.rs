//! HTTP REST and GraphQL API endpoints for VibeSQL

pub mod crud;
pub mod rest;
pub mod types;

pub use rest::create_http_router;
