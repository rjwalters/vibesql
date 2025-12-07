//! VibeSQL Server Library
//!
//! This module provides the core server components for the VibeSQL database server,
//! including PostgreSQL wire protocol support, authentication, and session management.

pub mod auth;
pub mod config;
pub mod connection;
pub mod http;
pub mod observability;
pub mod protocol;
pub mod registry;
pub mod scheduler;
pub mod session;
pub mod subscription;
pub mod transaction;

pub use auth::PasswordStore;
pub use config::{
    ApiKeyConfig, AuthConfig, Config, HttpAuthConfig, HttpAuthMethod, HttpConfig, JwtConfig,
    LoggingConfig, ServerConfig,
};
pub use connection::{ConnectionHandler, TableMutationNotification};
pub use observability::ObservabilityProvider;
pub use protocol::{
    BackendMessage, FieldDescription, FrontendMessage, SubscriptionUpdateType, TransactionStatus,
};
pub use scheduler::{
    ScheduleExecutor, ScheduleExecutorConfig, SchedulerManager, SchedulerManagerConfig,
};
pub use registry::{DatabaseRegistry, SharedDatabase};
pub use session::{Column, ExecutionResult, Row, Session};
pub use subscription::{
    create_partial_row_update, extract_table_dependencies, extract_table_refs,
    SelectiveColumnConfig, Subscription, SubscriptionConfig, SubscriptionError, SubscriptionId,
    SubscriptionManager, SubscriptionUpdate,
};
pub use transaction::{
    SessionTransactionManager, TransactionChange, TransactionError, TransactionState,
};
// Re-export ChangeEvent from storage layer for consistency
pub use vibesql_storage::ChangeEvent;
