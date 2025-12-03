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
pub mod subscription;

pub use auth::PasswordStore;
pub use config::{AuthConfig, Config, LoggingConfig, ServerConfig};
pub use connection::ConnectionHandler;
pub use observability::ObservabilityProvider;
pub use protocol::{
    BackendMessage, FieldDescription, FrontendMessage, SubscriptionUpdateType, TransactionStatus,
};
pub use session::{Column, ExecutionResult, Row, Session};
pub use subscription::{
    extract_table_dependencies, extract_table_refs, ChangeEvent, SubscriptionError,
    SubscriptionId, SubscriptionManager, SubscriptionUpdate,
};
