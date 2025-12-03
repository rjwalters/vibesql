//! Subscription management for reactive query subscriptions.
//!
//! This module provides components for managing real-time query subscriptions,
//! including extracting table dependencies from SQL queries.

mod table_dependencies;

pub use table_dependencies::extract_table_dependencies;
