//! Real-time reactive query subscriptions
//!
//! This module provides subscription management for reactive queries.
//! Subscriptions track the results of queries and notify clients when those results change.

pub mod dependencies;
pub mod manager;
pub mod router;

pub use dependencies::extract_table_dependencies;
pub use manager::{Subscription, SubscriptionId, SubscriptionManager};
pub use router::{ChangeRouter, SubscriptionUpdate};
