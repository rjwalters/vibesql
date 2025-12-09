//! Subscription manager for tracking and notifying query subscriptions
//!
//! The SubscriptionManager is the central component of the subscription system.
//! It maintains the registry of active subscriptions, indexes them by table
//! dependencies, and handles change event notifications.
//!
//! # Module Organization
//!
//! This module is split into focused submodules:
//!
//! - [`lifecycle`]: Subscribe/unsubscribe operations
//! - [`wire`]: Wire protocol integration
//! - [`query`]: Query execution and retry logic
//! - [`events`]: Change event handling and notification
//! - [`metrics`]: Metrics and configuration accessors

mod events;
mod lifecycle;
mod metrics;
mod query;
mod wire;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;

use dashmap::DashMap;

use super::{SubscriptionConfig, SubscriptionId};
use crate::subscription::Subscription;

// ============================================================================
// Subscription Manager
// ============================================================================

/// Manager for query subscriptions
///
/// Tracks all active subscriptions, indexes them by table dependencies,
/// and handles notifications when data changes.
///
/// # Thread Safety
///
/// The manager uses `DashMap` for lock-free concurrent access to subscriptions.
/// Multiple threads can subscribe, unsubscribe, and process change events
/// concurrently without explicit locking.
///
/// # Performance
///
/// The manager uses a table-based index to quickly find subscriptions affected
/// by a change event. This allows O(1) lookup of subscriptions by table name,
/// rather than scanning all subscriptions.
///
/// # Connection Tracking
///
/// The manager supports tracking subscriptions by connection/session ID.
/// This enables efficient cleanup when a connection closes, and allows
/// both HTTP SSE and wire protocol clients to use the same subscription
/// infrastructure.
pub struct SubscriptionManager {
    /// All active subscriptions, indexed by ID
    pub(crate) subscriptions: DashMap<SubscriptionId, Subscription>,

    /// Index: table_name -> subscription IDs that depend on it
    /// This enables fast lookup of affected subscriptions when a table changes
    pub(crate) table_index: DashMap<String, HashSet<SubscriptionId>>,

    /// Index: connection_id -> subscription IDs belonging to that connection
    /// Used for connection-level subscription tracking and cleanup
    pub(crate) connection_index: DashMap<String, HashSet<SubscriptionId>>,

    /// Index: wire_subscription_id -> internal SubscriptionId
    /// Used to bridge wire protocol UUIDs to internal u64 IDs
    pub(crate) wire_id_index: DashMap<[u8; 16], SubscriptionId>,

    /// Configuration for limits and quotas
    pub(crate) config: SubscriptionConfig,

    /// Counter for global limit exceeded events (for metrics)
    pub(crate) limit_exceeded_count: AtomicUsize,

    /// Counter for result set too large events (for metrics)
    pub(crate) result_set_exceeded_count: AtomicUsize,

    /// Atomic counter for current subscription count (for lock-free limit checking)
    pub(crate) subscription_count_atomic: AtomicUsize,

    /// Per-connection subscription counts (for per-connection limit enforcement)
    pub(crate) connection_subscription_counts: DashMap<String, AtomicUsize>,
}

impl SubscriptionManager {
    /// Create a new subscription manager with default configuration
    pub fn new() -> Self {
        Self::with_config(SubscriptionConfig::default())
    }

    /// Create a new subscription manager with custom configuration
    pub fn with_config(config: SubscriptionConfig) -> Self {
        Self {
            subscriptions: DashMap::new(),
            table_index: DashMap::new(),
            connection_index: DashMap::new(),
            wire_id_index: DashMap::new(),
            config,
            limit_exceeded_count: AtomicUsize::new(0),
            result_set_exceeded_count: AtomicUsize::new(0),
            subscription_count_atomic: AtomicUsize::new(0),
            connection_subscription_counts: DashMap::new(),
        }
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}
