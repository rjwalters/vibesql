//! PostgreSQL wire protocol implementation
//!
//! This module implements the PostgreSQL wire protocol for client-server communication.
//!
//! # Module Organization
//!
//! - [`types`]: Core type definitions (errors, configs, field descriptions)
//! - [`backend`]: Backend messages (server -> client)
//! - [`frontend`]: Frontend messages (client -> server)
//! - [`helpers`]: Encoding/decoding utility functions

mod backend;
mod frontend;
mod helpers;
mod types;

#[cfg(test)]
mod tests;

// Re-export all public types to maintain API compatibility
pub use backend::BackendMessage;
pub use frontend::FrontendMessage;
pub use types::{
    FieldDescription, PartialRowUpdate, ProtocolError, SelectiveUpdatesConfig,
    SubscriptionUpdateType, TransactionStatus,
};
