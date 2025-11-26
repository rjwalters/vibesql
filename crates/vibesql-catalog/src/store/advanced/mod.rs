//! Advanced SQL:1999 object management.
//!
//! This module handles user-defined types, domains, sequences, collations,
//! character sets, translations, views, triggers, assertions, and routines.
//!
//! ## Module Organization
//!
//! This module is organized by SQL object type for better maintainability:
//!
//! - [`types`] - User-defined type management
//! - [`domains`] - Domain management
//! - [`sequences`] - Sequence management
//! - [`collations`] - Collation management
//! - [`charsets`] - Character set management
//! - [`translations`] - Translation management
//! - [`views`] - View management and dependency tracking
//! - [`triggers`] - Trigger management
//! - [`assertions`] - Assertion management
//! - [`routines`] - Function and procedure management

// Module declarations - each handles a specific SQL object type
mod assertions;
mod charsets;
mod collations;
mod domains;
mod routines;
mod sequences;
mod translations;
mod triggers;
mod types;
mod views;

// Re-export all modules for internal use
// Note: All methods are defined as `impl Catalog` in each module,
// so they are automatically available on the Catalog type.
// No explicit re-exports are needed since the impls are applied directly.

// Re-export ViewDropBehavior for use by external code
pub use views::ViewDropBehavior;
