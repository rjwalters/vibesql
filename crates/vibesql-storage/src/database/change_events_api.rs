// ============================================================================
// Change Event Broadcasting API (Reactive Subscriptions)
// ============================================================================
//
// This module provides change event broadcasting methods for the Database struct.
// Enables reactive subscriptions for data change notifications.

use super::Database;
use crate::change_events::{ChangeEvent, ChangeEventReceiver};

impl Database {
    // ============================================================================
    // Change Event Broadcasting (Reactive Subscriptions)
    // ============================================================================

    /// Enable change event broadcasting
    ///
    /// Creates a broadcast channel for notifying subscribers when data changes.
    /// Returns a receiver for the channel.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of events to buffer before old events are overwritten
    ///
    /// # Example
    /// ```text
    /// let mut db = Database::new();
    /// let mut rx = db.enable_change_events(1024);
    ///
    /// // Insert some data
    /// db.insert_row("users", row)?;
    ///
    /// // Receive change events
    /// for event in rx.recv_all() {
    ///     println!("Change: {:?}", event);
    /// }
    /// ```
    pub fn enable_change_events(&mut self, capacity: usize) -> ChangeEventReceiver {
        let (sender, receiver) = crate::change_events::channel(capacity);
        self.change_sender = Some(sender);
        receiver
    }

    /// Subscribe to change events
    ///
    /// Returns a new receiver for change events if broadcasting is enabled,
    /// or None if `enable_change_events()` has not been called.
    ///
    /// # Example
    /// ```text
    /// // Enable broadcasting
    /// db.enable_change_events(1024);
    ///
    /// // Create additional subscribers
    /// let rx1 = db.subscribe_changes().unwrap();
    /// let rx2 = db.subscribe_changes().unwrap();
    /// ```
    pub fn subscribe_changes(&self) -> Option<ChangeEventReceiver> {
        self.change_sender.as_ref().map(|s| s.subscribe())
    }

    /// Check if change event broadcasting is enabled
    pub fn change_events_enabled(&self) -> bool {
        self.change_sender.is_some()
    }

    /// Broadcast a change event to all subscribers (internal use)
    pub(super) fn broadcast_change(&self, event: ChangeEvent) {
        if let Some(sender) = &self.change_sender {
            let _ = sender.send(event);
        }
    }

    /// Notify subscribers of an update event
    ///
    /// This should be called by the executor after successfully updating a row.
    /// The storage layer broadcasts the event to any subscribers.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table that was modified
    /// * `row_index` - Index of the row that was updated
    pub fn notify_update(&self, table_name: &str, row_index: usize) {
        self.broadcast_change(ChangeEvent::Update {
            table_name: table_name.to_string(),
            row_index,
        });
    }

    /// Notify subscribers of a delete event
    ///
    /// This should be called by the executor after successfully deleting rows.
    /// The storage layer broadcasts the event to any subscribers.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table that was modified
    /// * `row_indices` - Indices of rows that were deleted (before deletion)
    pub fn notify_deletes(&self, table_name: &str, row_indices: &[usize]) {
        for &row_index in row_indices {
            self.broadcast_change(ChangeEvent::Delete {
                table_name: table_name.to_string(),
                row_index,
            });
        }
    }
}
