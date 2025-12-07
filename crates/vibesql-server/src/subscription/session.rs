//! Session-level subscription types
//!
//! **DEPRECATED**: This module is deprecated and no longer contains any public types.
//! Use `SubscriptionManager` with connection tracking methods instead:
//! - `subscribe_for_connection()` - Subscribe for a specific connection
//! - `unsubscribe_by_wire_id()` - Unsubscribe by wire protocol ID
//! - `unsubscribe_all_for_connection()` - Clean up all subscriptions for a connection
//! - `connection_subscription_count()` - Get subscription count for a connection
