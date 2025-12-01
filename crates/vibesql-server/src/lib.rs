//! VibeSQL Server Library
//!
//! This module provides the core server components for the VibeSQL database server,
//! including PostgreSQL wire protocol support, authentication, and session management.

pub mod auth;
pub mod config;
pub mod connection;
pub mod observability;
pub mod protocol;
pub mod session;

pub use auth::PasswordStore;
pub use config::{AuthConfig, Config, LoggingConfig, ServerConfig};
pub use connection::ConnectionHandler;
pub use observability::ObservabilityProvider;
pub use protocol::{BackendMessage, FieldDescription, FrontendMessage, TransactionStatus};
pub use session::{Column, ExecutionResult, Row, Session};
