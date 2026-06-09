// ============================================================================
// Index Maintenance - CRUD operations for indexes
// ============================================================================
//
// This module has been split into focused submodules for better maintainability:
//
// - prefix: Prefix truncation utilities for string indexing
// - create: Index creation and drop operations (B-tree, IVFFlat, HNSW)
// - insert: Row insertion into indexes
// - delete: Row deletion from indexes
// - update: Row updates and index rebuilding

#![allow(clippy::doc_overindented_list_items)]

mod create;
mod delete;
mod insert;
mod partial;
mod prefix;
mod update;

// Re-export the prefix truncation function for use by sibling modules
pub(super) use prefix::apply_prefix_truncation;
