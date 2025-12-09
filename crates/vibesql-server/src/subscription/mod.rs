//! Query subscription management for real-time reactive updates
//!
//! This module provides the infrastructure for tracking active query subscriptions,
//! receiving change events from the storage layer, and determining which subscriptions
//! need to be notified when data changes.
//!
//! # Overview
//!
//! The subscription system allows clients to register queries for real-time updates.
//! When the underlying data changes, subscriptions are automatically re-evaluated
//! and clients are notified if their results have changed.
//!
//! # Architecture
//!
//! - [`SubscriptionId`]: Unique identifier for each subscription
//! - [`Subscription`]: Individual subscription with query and notification channel
//! - [`SubscriptionManager`]: Central manager tracking all subscriptions
//! - [`SubscriptionUpdate`]: Update notifications sent to subscribers
//! - [`ChangeEvent`]: Events from the storage layer indicating data changes
//!
//! # Example
//!
//! ```text
//! use vibesql_server::subscription::{SubscriptionManager, ChangeEvent};
//! use tokio::sync::mpsc;
//!
//! let manager = SubscriptionManager::new();
//! let (tx, mut rx) = mpsc::channel(16);
//!
//! // Subscribe to a query
//! let id = manager.subscribe("SELECT * FROM users WHERE active = true".to_string(), tx)?;
//!
//! // When data changes, the manager checks affected subscriptions
//! manager.handle_change(ChangeEvent::Insert {
//!     table_name: "users".to_string(),
//!     row_id: 42,
//! }).await;
//!
//! // Subscriber receives update if results changed
//! if let Some(update) = rx.recv().await {
//!     println!("Results updated: {:?}", update);
//! }
//! ```

// ============================================================================
// Submodules
// ============================================================================

mod config;
mod delta;
pub mod error;
pub mod filter;
mod hash;
mod manager;
pub mod pk_detector;
mod router;
mod selective;
pub mod session;
mod table_dependencies;
mod table_extract;
mod types;

// ============================================================================
// Re-exports from submodules
// ============================================================================

// Config types
pub use config::{SubscriptionConfig, SubscriptionRetryPolicy};

// Delta computation
pub use delta::{compute_delta, compute_delta_with_pk, PartialRowDelta};

// Error handling
pub use error::{classify_error, classify_error_str, SubscriptionErrorKind};

// Hash utilities
pub use hash::hash_rows;

// Manager
pub use manager::SubscriptionManager;

// PK detection
pub use pk_detector::{
    detect_pk_columns, detect_pk_columns_from_stmt, PkDetectionFailureReason, PkDetectionResult,
};

// Router
pub use router::{ChangeRouter, SubscriptionUpdate as RouterUpdate};

// Selective column updates
pub use selective::{
    compute_column_diff, create_partial_row_update, create_partial_row_update_with_metrics,
    should_use_selective_update, should_use_selective_update_with_metrics, ColumnDiff,
    SelectiveColumnConfig,
};

// Table analysis
pub use table_dependencies::extract_table_dependencies;
pub use table_extract::extract_table_refs;

// Core types
pub use types::{
    Subscription, SubscriptionError, SubscriptionId, SubscriptionMetrics, SubscriptionUpdate,
};

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_id_uniqueness() {
        let id1 = SubscriptionId::new();
        let id2 = SubscriptionId::new();
        let id3 = SubscriptionId::new();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_subscription_id_display() {
        // Note: We can't construct SubscriptionId(42) directly anymore since the field is private
        // We'll test the display format differently
        let id = SubscriptionId::new();
        let display = format!("{}", id);
        assert!(display.starts_with("sub-"));
    }

    #[test]
    fn test_hash_rows_empty() {
        let rows: Vec<crate::Row> = vec![];
        let hash = hash_rows(&rows);
        // Empty rows should produce a consistent hash
        assert_eq!(hash, hash_rows(&[]));
    }

    #[test]
    fn test_hash_rows_different_content() {
        use vibesql_types::SqlValue;

        let rows1 = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("hello"))],
        }];

        let rows2 = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("hello"))],
        }];

        let hash1 = hash_rows(&rows1);
        let hash2 = hash_rows(&rows2);

        // Different content should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_rows_same_content() {
        use vibesql_types::SqlValue;

        let rows1 = vec![crate::Row {
            values: vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("test"))],
        }];

        let rows2 = vec![crate::Row {
            values: vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("test"))],
        }];

        let hash1 = hash_rows(&rows1);
        let hash2 = hash_rows(&rows2);

        // Same content should produce same hash
        assert_eq!(hash1, hash2);
    }

    // ========================================================================
    // Tests for compute_delta
    // ========================================================================

    #[test]
    fn test_compute_delta_no_changes() {
        use vibesql_types::SqlValue;

        let rows = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];

        // Same old and new should return None
        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &rows, &rows);
        assert!(delta.is_none());
    }

    #[test]
    fn test_compute_delta_single_insert() {
        use vibesql_types::SqlValue;

        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(inserts[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_single_delete() {
        use vibesql_types::SqlValue;

        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
                assert_eq!(deletes[0].values[0], SqlValue::Integer(2));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_insert_and_delete() {
        use vibesql_types::SqlValue;

        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(deletes.len(), 1);
                assert!(updates.is_empty());
                // The old row was deleted, new row was inserted
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(deletes[0].values[0], SqlValue::Integer(1));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_empty_to_rows() {
        use vibesql_types::SqlValue;

        let old: Vec<crate::Row> = vec![];
        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 2);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_rows_to_empty() {
        use vibesql_types::SqlValue;

        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];
        let new: Vec<crate::Row> = vec![];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 2);
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_duplicate_rows() {
        use vibesql_types::SqlValue;

        // Test handling of duplicate rows
        let old = vec![
            crate::Row { values: vec![SqlValue::Integer(1)] },
            crate::Row { values: vec![SqlValue::Integer(1)] },
        ];

        let new = vec![
            crate::Row { values: vec![SqlValue::Integer(1)] },
            crate::Row { values: vec![SqlValue::Integer(1)] },
            crate::Row { values: vec![SqlValue::Integer(1)] },
        ];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // One additional duplicate row was inserted
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    // ========================================================================
    // Tests for PK-based Delta Computation
    // ========================================================================

    #[test]
    fn test_compute_delta_with_pk_detects_update() {
        use vibesql_types::SqlValue;

        // Same PK (1), different name value - should be detected as UPDATE
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0]; // First column is PK
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // With PK matching, this should be an UPDATE, not insert+delete
                assert!(inserts.is_empty());
                assert_eq!(updates.len(), 1);
                assert!(deletes.is_empty());

                // Verify the update contains old and new row
                let (old_row, new_row) = &updates[0];
                assert_eq!(old_row.values[0], SqlValue::Integer(1));
                assert_eq!(old_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
                assert_eq!(new_row.values[0], SqlValue::Integer(1));
                assert_eq!(new_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_insert_and_delete() {
        use vibesql_types::SqlValue;

        // Different PKs - should be insert + delete
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(deletes[0].values[0], SqlValue::Integer(1));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_no_changes() {
        use vibesql_types::SqlValue;

        let rows = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &rows, &rows, &pk_columns);
        assert!(delta.is_none());
    }

    #[test]
    fn test_compute_delta_with_pk_multiple_updates() {
        use vibesql_types::SqlValue;

        // Multiple rows with updates
        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
        ];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("ALICE"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("BOB"))] },
        ];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert_eq!(updates.len(), 2);
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_composite_pk() {
        use vibesql_types::SqlValue;

        // Composite PK (order_id, user_id)
        let old = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("pending")),
            ],
        }];

        let new = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("shipped")),
            ],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0, 1]; // Composite PK
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert_eq!(updates.len(), 1);
                assert!(deletes.is_empty());

                let (_, new_row) = &updates[0];
                assert_eq!(new_row.values[2], SqlValue::Varchar(arcstr::ArcStr::from("shipped")));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_empty_fallback() {
        use vibesql_types::SqlValue;

        // With empty pk_columns, should fall back to hash-based and detect as insert+delete
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let delta = compute_delta_with_pk(test_id, &old, &new, &[]);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // Hash-based: different content = insert + delete, no update detection
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_mixed_operations() {
        use vibesql_types::SqlValue;

        // Mix of insert, update, and delete
        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))] },
            crate::Row {
                values: vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
            },
        ];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("ALICE"))],
            }, // Update
            // Row 2 deleted
            crate::Row {
                values: vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
            }, // Unchanged
            crate::Row {
                values: vec![SqlValue::Integer(4), SqlValue::Varchar(arcstr::ArcStr::from("Diana"))],
            }, // Insert
        ];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(updates.len(), 1);
                assert_eq!(deletes.len(), 1);

                // Verify insert
                assert_eq!(inserts[0].values[0], SqlValue::Integer(4));

                // Verify update
                let (old_row, new_row) = &updates[0];
                assert_eq!(old_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
                assert_eq!(new_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("ALICE")));

                // Verify delete
                assert_eq!(deletes[0].values[0], SqlValue::Integer(2));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_out_of_bounds_fallback() {
        use vibesql_types::SqlValue;

        // PK column index out of bounds - should fall back to hash-based
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![5]; // Out of bounds
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        // Should fall back to hash-based matching
        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
            }
            _ => panic!("Expected Delta update"),
        }
    }

    // ========================================================================
    // Tests for Selective Column Updates
    // ========================================================================

    #[test]
    fn test_compute_column_diff_no_changes() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]);
        assert!(diff.is_none());
    }

    #[test]
    fn test_compute_column_diff_single_column_change() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        // Included columns should be PK (0) + changed (1)
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_compute_column_diff_multiple_columns_change() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("active")),
            ],
        };
        let new = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("inactive")),
            ],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1, 3]);
        // Included columns should be PK (0) + changed (1, 3)
        assert_eq!(diff.included_columns, vec![0, 1, 3]);
    }

    #[test]
    fn test_compute_column_diff_pk_column_changed() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new = crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![0]);
        // PK is already changed, so included = just [0]
        assert_eq!(diff.included_columns, vec![0]);
    }

    #[test]
    fn test_compute_column_diff_null_handling() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_should_use_selective_update_enabled() {
        let diff = ColumnDiff { changed_columns: vec![1], included_columns: vec![0, 1] };

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        assert!(should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_disabled() {
        let diff = ColumnDiff { changed_columns: vec![1], included_columns: vec![0, 1] };

        let config =
            SelectiveColumnConfig { enabled: false, pk_columns: vec![0], ..Default::default() };

        assert!(!should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_too_many_changes() {
        // 6 columns changed out of 10 = 60%, exceeds 50% threshold
        let diff = ColumnDiff {
            changed_columns: vec![1, 2, 3, 4, 5, 6],
            included_columns: vec![0, 1, 2, 3, 4, 5, 6],
        };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            max_changed_columns_ratio: 0.5,
            ..Default::default()
        };

        assert!(!should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_create_partial_row_update() {
        let old_row =
            vec![Some(b"1".to_vec()), Some(b"Alice".to_vec()), Some(b"100".to_vec())];
        let new_row =
            vec![Some(b"1".to_vec()), Some(b"Bob".to_vec()), Some(b"100".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            max_changed_columns_ratio: 0.5,
            ..Default::default()
        };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config).unwrap();

        assert_eq!(partial.total_columns, 3);
        // Should include columns 0 (PK) and 1 (changed)
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert!(!partial.is_column_present(2));
        assert_eq!(partial.present_column_count(), 2);
        // Values should be the new values for included columns
        assert_eq!(partial.values.len(), 2);
        assert_eq!(partial.values[0], Some(b"1".to_vec()));
        assert_eq!(partial.values[1], Some(b"Bob".to_vec()));
    }

    #[test]
    fn test_create_partial_row_update_null_change() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), None];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config).unwrap();

        assert_eq!(partial.total_columns, 2);
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert_eq!(partial.values.len(), 2);
        assert_eq!(partial.values[0], Some(b"1".to_vec()));
        assert_eq!(partial.values[1], None); // NULL value
    }

    #[test]
    fn test_create_partial_row_update_no_changes() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_partial_row_update_column_mask() {
        use crate::protocol::messages::PartialRowUpdate;

        // Test with 10 columns, columns 0, 3, 7 present
        let partial = PartialRowUpdate::new(
            10,
            &[0, 3, 7],
            vec![Some(b"a".to_vec()), Some(b"b".to_vec()), Some(b"c".to_vec())],
        );

        assert_eq!(partial.total_columns, 10);
        assert_eq!(partial.column_mask.len(), 2); // ceil(10/8) = 2 bytes

        // Check column presence
        assert!(partial.is_column_present(0));
        assert!(!partial.is_column_present(1));
        assert!(!partial.is_column_present(2));
        assert!(partial.is_column_present(3));
        assert!(!partial.is_column_present(4));
        assert!(!partial.is_column_present(5));
        assert!(!partial.is_column_present(6));
        assert!(partial.is_column_present(7));
        assert!(!partial.is_column_present(8));
        assert!(!partial.is_column_present(9));
        assert!(!partial.is_column_present(10)); // Out of range

        assert_eq!(partial.present_column_count(), 3);
    }

    #[test]
    fn test_delta_updates_produce_partial_row_updates() {
        use vibesql_types::SqlValue;

        // Test that delta computation with updates can produce partial row updates
        // This verifies the integration between compute_delta_with_pk and create_partial_row_update

        let test_id = SubscriptionId::new();

        // Create old and new rows where only one column changes
        // Row format: [id, name, balance]
        // id=1: name unchanged, balance changes from 100 to 150
        let old = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(100),
            ],
        }];
        let new = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(150),
            ],
        }];

        let pk_columns = vec![0]; // First column is PK
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);

        // Verify we got an update (not delete+insert)
        if let SubscriptionUpdate::Delta { updates, inserts, deletes, .. } = delta.unwrap() {
            assert!(inserts.is_empty(), "Should not have inserts");
            assert!(deletes.is_empty(), "Should not have deletes");
            assert_eq!(updates.len(), 1, "Should have one update");

            // Now verify that create_partial_row_update works with this update
            let (old_row, new_row) = &updates[0];

            // Convert to wire format (as connection.rs does)
            let old_wire: Vec<Option<Vec<u8>>> =
                old_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();
            let new_wire: Vec<Option<Vec<u8>>> =
                new_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();

            let config =
                SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

            let partial = create_partial_row_update(&old_wire, &new_wire, &[0], &config);

            // Should produce a partial update since only 1 of 3 columns changed
            assert!(partial.is_some(), "Should produce partial row update");

            let partial = partial.unwrap();
            assert_eq!(partial.total_columns, 3);

            // Should include PK (column 0) and changed column (column 2)
            assert!(partial.is_column_present(0), "PK column should be present");
            assert!(!partial.is_column_present(1), "Unchanged column should not be present");
            assert!(partial.is_column_present(2), "Changed column should be present");

            // Verify values
            assert_eq!(partial.values.len(), 2); // PK + changed column
            assert_eq!(partial.values[0], Some(b"1".to_vec())); // PK value
            assert_eq!(partial.values[1], Some(b"150".to_vec())); // New balance
        } else {
            panic!("Expected Delta, got something else");
        }
    }

    #[test]
    fn test_delta_updates_fallback_to_full_row_when_too_many_changes() {
        use vibesql_types::SqlValue;

        // Test that when too many columns change, create_partial_row_update returns None

        let test_id = SubscriptionId::new();

        // Create old and new rows where most non-PK columns change
        // Row format: [id, name, email]
        let old = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Varchar(arcstr::ArcStr::from("alice@old.com")),
            ],
        }];
        let new = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Varchar(arcstr::ArcStr::from("bob@new.com")),
            ],
        }];

        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);

        if let SubscriptionUpdate::Delta { updates, .. } = delta.unwrap() {
            assert_eq!(updates.len(), 1);

            let (old_row, new_row) = &updates[0];

            let old_wire: Vec<Option<Vec<u8>>> =
                old_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();
            let new_wire: Vec<Option<Vec<u8>>> =
                new_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();

            // Use config with low threshold (max 30% of columns can change)
            let config = SelectiveColumnConfig {
                enabled: true,
                pk_columns: vec![0],
                max_changed_columns_ratio: 0.3,
                ..Default::default()
            };

            // 2 of 3 columns changed (66%), which exceeds 30% threshold
            let partial = create_partial_row_update(&old_wire, &new_wire, &[0], &config);

            // Should NOT produce partial update due to too many changes
            assert!(partial.is_none(), "Should fall back to full row when too many columns change");
        } else {
            panic!("Expected Delta");
        }
    }

    // ========================================================================
    // Additional Tests for Selective Column Updates (Issue #3924)
    // ========================================================================

    #[test]
    fn test_should_use_selective_update_below_min_changed_columns() {
        // Only 1 column changed, but min_changed_columns is 2
        let diff = ColumnDiff { changed_columns: vec![1], included_columns: vec![0, 1] };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 2, // Require at least 2 columns to change
            max_changed_columns_ratio: 0.5,
        };

        // Should return false because only 1 column changed
        assert!(!should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_at_min_changed_columns() {
        // Exactly 2 columns changed, min_changed_columns is 2
        let diff = ColumnDiff { changed_columns: vec![1, 2], included_columns: vec![0, 1, 2] };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 2, // Require at least 2 columns to change
            max_changed_columns_ratio: 0.5,
        };

        // Should return true because exactly min_changed_columns changed (2 of 10 = 20%)
        assert!(should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_at_max_ratio() {
        // 5 of 10 columns changed = 50%, exactly at max_changed_columns_ratio
        let diff = ColumnDiff {
            changed_columns: vec![1, 2, 3, 4, 5],
            included_columns: vec![0, 1, 2, 3, 4, 5],
        };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5, // Allow up to 50%
        };

        // Should return true because exactly at threshold (not over)
        assert!(should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_create_partial_row_update_all_columns_changed() {
        // All 3 columns change - should fall back (ratio = 100% > 50%)
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec()), Some(b"100".to_vec())];
        let new_row = vec![Some(b"2".to_vec()), Some(b"Bob".to_vec()), Some(b"200".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        // Should return None because all columns changed (100% > 50%)
        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_create_partial_row_update_empty_pk_columns() {
        // Empty PK columns - should still work, just won't include extra columns
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec()), Some(b"100".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Bob".to_vec()), Some(b"100".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        // Should work - only column 1 changed (33% < 50%)
        let partial = create_partial_row_update(&old_row, &new_row, &[], &config).unwrap();

        assert_eq!(partial.total_columns, 3);
        // Only column 1 is present (no PK to force-include)
        assert!(!partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert!(!partial.is_column_present(2));
        assert_eq!(partial.present_column_count(), 1);
        assert_eq!(partial.values.len(), 1);
        assert_eq!(partial.values[0], Some(b"Bob".to_vec()));
    }

    #[test]
    fn test_create_partial_row_update_disabled_returns_none() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Bob".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: false, // Disabled
            pk_columns: vec![0],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        // Should return None because selective updates are disabled
        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_create_partial_row_update_different_row_lengths() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Bob".to_vec()), Some(b"extra".to_vec())];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        // Should return None because row lengths differ
        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_compute_column_diff_different_row_lengths() {
        use vibesql_types::SqlValue;

        let old = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))] };
        let new = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(100),
            ],
        };

        // Should return None because row lengths differ
        let diff = compute_column_diff(&old, &new, &[0]);
        assert!(diff.is_none());
    }

    #[test]
    fn test_compute_column_diff_composite_pk() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![
                SqlValue::Integer(1),   // PK col 0
                SqlValue::Integer(100), // PK col 1
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(50),
            ],
        };
        let new = crate::Row {
            values: vec![
                SqlValue::Integer(1),   // PK col 0 unchanged
                SqlValue::Integer(100), // PK col 1 unchanged
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")), // Changed
                SqlValue::Integer(50), // Unchanged
            ],
        };

        // Composite PK: columns 0 and 1
        let diff = compute_column_diff(&old, &new, &[0, 1]).unwrap();
        assert_eq!(diff.changed_columns, vec![2]); // Only column 2 changed
        // Included columns should be PK (0, 1) + changed (2)
        assert_eq!(diff.included_columns, vec![0, 1, 2]);
    }

    #[test]
    fn test_create_partial_row_update_composite_pk() {
        let old_row = vec![
            Some(b"1".to_vec()),     // PK col 0
            Some(b"100".to_vec()),   // PK col 1
            Some(b"Alice".to_vec()), // Data
            Some(b"50".to_vec()),    // Data
        ];
        let new_row = vec![
            Some(b"1".to_vec()),   // PK col 0 unchanged
            Some(b"100".to_vec()), // PK col 1 unchanged
            Some(b"Bob".to_vec()), // Changed
            Some(b"50".to_vec()),  // Unchanged
        ];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0, 1],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        let partial = create_partial_row_update(&old_row, &new_row, &[0, 1], &config).unwrap();

        assert_eq!(partial.total_columns, 4);
        // Columns 0, 1 (PK) and 2 (changed) should be present
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert!(partial.is_column_present(2));
        assert!(!partial.is_column_present(3));
        assert_eq!(partial.present_column_count(), 3);
    }

    #[test]
    fn test_partial_row_update_large_column_count() {
        use crate::protocol::messages::PartialRowUpdate;

        // Test with 20 columns (requires 3 bytes for column mask)
        let partial = PartialRowUpdate::new(
            20,
            &[0, 7, 8, 15, 16], // Spread across multiple bytes
            vec![
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"c".to_vec()),
                Some(b"d".to_vec()),
                Some(b"e".to_vec()),
            ],
        );

        assert_eq!(partial.total_columns, 20);
        assert_eq!(partial.column_mask.len(), 3); // ceil(20/8) = 3 bytes

        // Check column presence across bytes
        assert!(partial.is_column_present(0));  // Byte 0, bit 0
        assert!(partial.is_column_present(7));  // Byte 0, bit 7
        assert!(partial.is_column_present(8));  // Byte 1, bit 0
        assert!(partial.is_column_present(15)); // Byte 1, bit 7
        assert!(partial.is_column_present(16)); // Byte 2, bit 0
        assert!(!partial.is_column_present(19)); // Not present
        assert_eq!(partial.present_column_count(), 5);
    }

    #[test]
    fn test_compute_column_diff_null_to_value() {
        use vibesql_types::SqlValue;

        // Test NULL -> value transition
        let old = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };
        let new = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_compute_column_diff_value_to_null() {
        use vibesql_types::SqlValue;

        // Test value -> NULL transition
        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_create_partial_row_update_null_to_value() {
        let old_row = vec![Some(b"1".to_vec()), None]; // NULL in column 1
        let new_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config).unwrap();

        assert_eq!(partial.total_columns, 2);
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert_eq!(partial.values.len(), 2);
        assert_eq!(partial.values[0], Some(b"1".to_vec()));
        assert_eq!(partial.values[1], Some(b"Alice".to_vec())); // Changed from NULL
    }

    // ========================================================================
    // Tests for PartialRowDelta
    // ========================================================================

    #[test]
    fn test_partial_row_delta_from_rows_single_column_change() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(100),
            ],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(150),
            ],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK (0) + changed column (2)
        assert_eq!(delta.column_indices, vec![0, 2]);
        assert_eq!(delta.old_values, vec![SqlValue::Integer(1), SqlValue::Integer(100)]);
        assert_eq!(delta.new_values, vec![SqlValue::Integer(1), SqlValue::Integer(150)]);
    }

    #[test]
    fn test_partial_row_delta_from_rows_multiple_column_changes() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("active")),
            ],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("inactive")),
            ],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK (0) + changed columns (1, 3)
        assert_eq!(delta.column_indices, vec![0, 1, 3]);
        assert_eq!(
            delta.old_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Varchar(arcstr::ArcStr::from("active"))
            ]
        );
        assert_eq!(
            delta.new_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Varchar(arcstr::ArcStr::from("inactive"))
            ]
        );
    }

    #[test]
    fn test_partial_row_delta_from_rows_no_changes() {
        use vibesql_types::SqlValue;

        let row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&row, &row, &pk_columns);

        assert!(delta.is_none(), "Should return None when rows are identical");
    }

    #[test]
    fn test_partial_row_delta_from_rows_pk_column_changed() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new_row = crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // PK column changed, should only include column 0
        assert_eq!(delta.column_indices, vec![0]);
        assert_eq!(delta.old_values, vec![SqlValue::Integer(1)]);
        assert_eq!(delta.new_values, vec![SqlValue::Integer(2)]);
    }

    #[test]
    fn test_partial_row_delta_from_rows_null_handling() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new_row = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK (0) + changed column (1)
        assert_eq!(delta.column_indices, vec![0, 1]);
        assert_eq!(delta.new_values, vec![SqlValue::Integer(1), SqlValue::Null]);
    }

    #[test]
    fn test_partial_row_delta_from_rows_composite_pk() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("old")),
            ],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("new")),
            ],
        };

        let pk_columns = vec![0, 1]; // Composite PK
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK columns (0, 1) + changed column (2)
        assert_eq!(delta.column_indices, vec![0, 1, 2]);
        assert_eq!(
            delta.old_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("old"))
            ]
        );
        assert_eq!(
            delta.new_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(arcstr::ArcStr::from("new"))
            ]
        );
    }

    #[test]
    fn test_partial_row_delta_from_rows_different_column_count() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(100),
            ],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_none(), "Should return None when column counts differ");
    }

    #[test]
    fn test_subscription_update_partial_subscription_id() {
        let test_id = SubscriptionId::new();
        let update = SubscriptionUpdate::Partial { subscription_id: test_id, updates: vec![] };

        assert_eq!(update.subscription_id(), test_id);
    }
}
