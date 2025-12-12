// ============================================================================
// Persistence Tests Module
// ============================================================================
//
// This module organizes persistence tests into logical categories:
//
// - sql_dump: Tests for SQL dump save/load functionality
// - json_persistence: Tests for JSON serialization/deserialization
// - binary_persistence: Tests for binary format serialization/deserialization
//
// Tests are split from the original monolithic tests.rs file (1,211 lines)
// into smaller, focused modules for improved maintainability and navigation.
// ============================================================================

pub mod binary_persistence;
pub mod json_persistence;
pub mod sql_dump;
