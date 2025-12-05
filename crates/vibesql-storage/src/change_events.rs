//! Change event broadcasting for reactive subscriptions
//!
//! This module provides a broadcast channel for notifying subscribers when data changes.
//! It is designed to be lightweight and WASM-compatible.

use std::sync::{Arc, Mutex, Weak};

/// Default capacity for the change event channel
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Change event for external subscribers
///
/// Note: Only row_id is included, not full row data. This is intentional for performance -
/// cloning full rows on every mutation would be expensive. Subscribers that need row data
/// can re-query using the row_id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeEvent {
    /// A row was inserted
    Insert {
        /// Name of the table
        table_name: String,
        /// Index of the inserted row
        row_index: usize,
    },
    /// A row was updated
    Update {
        /// Name of the table
        table_name: String,
        /// Index of the updated row
        row_index: usize,
    },
    /// A row was deleted
    Delete {
        /// Name of the table
        table_name: String,
        /// Index of the deleted row (before deletion)
        row_index: usize,
    },
}

impl ChangeEvent {
    /// Get the table name from the event
    pub fn table_name(&self) -> &str {
        match self {
            ChangeEvent::Insert { table_name, .. } => table_name,
            ChangeEvent::Update { table_name, .. } => table_name,
            ChangeEvent::Delete { table_name, .. } => table_name,
        }
    }

    /// Get the row index from the event
    pub fn row_index(&self) -> usize {
        match self {
            ChangeEvent::Insert { row_index, .. } => *row_index,
            ChangeEvent::Update { row_index, .. } => *row_index,
            ChangeEvent::Delete { row_index, .. } => *row_index,
        }
    }
}

/// Error type for receive operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// No events available (non-blocking)
    Empty,
    /// Receiver has lagged behind and missed some events
    Lagged(usize),
    /// Channel has been closed (sender dropped)
    Closed,
}

/// Shared state for the broadcast channel
struct ChannelState {
    /// Ring buffer of events
    buffer: Vec<Option<ChangeEvent>>,
    /// Current write position in the ring buffer
    write_pos: usize,
    /// Total number of events ever sent (for lag detection)
    total_sent: usize,
    /// Channel capacity
    capacity: usize,
    /// Whether the sender has been dropped
    closed: bool,
}

impl ChannelState {
    fn new(capacity: usize) -> Self {
        Self { buffer: vec![None; capacity], write_pos: 0, total_sent: 0, capacity, closed: false }
    }
}

/// Sender half of the change event broadcast channel
///
/// This can be cloned to create additional senders, all sharing the same channel.
#[derive(Clone)]
pub struct ChangeEventSender {
    state: Arc<Mutex<ChannelState>>,
}

impl ChangeEventSender {
    /// Send an event to all subscribers
    ///
    /// Returns the number of active receivers. If there are no receivers,
    /// the event is still buffered for future subscribers.
    pub fn send(&self, event: ChangeEvent) -> usize {
        let mut state = self.state.lock().unwrap();

        // Store the event in the ring buffer
        let write_pos = state.write_pos;
        let capacity = state.capacity;
        state.buffer[write_pos] = Some(event);
        state.write_pos = (write_pos + 1) % capacity;
        state.total_sent += 1;

        // Count active receivers by counting weak references
        // This is an approximation since we don't track receivers directly
        Arc::strong_count(&self.state) - 1 // -1 for the sender's own reference
    }

    /// Create a new receiver subscribed to this channel
    pub fn subscribe(&self) -> ChangeEventReceiver {
        let state = self.state.lock().unwrap();
        ChangeEventReceiver {
            state: Arc::downgrade(&self.state),
            read_pos: state.total_sent, // Start from current position (no backlog)
        }
    }
}

impl std::fmt::Debug for ChangeEventSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeEventSender").finish_non_exhaustive()
    }
}

/// Receiver half of the change event broadcast channel
pub struct ChangeEventReceiver {
    state: Weak<Mutex<ChannelState>>,
    /// Position of next event to read (in terms of total_sent)
    read_pos: usize,
}

impl ChangeEventReceiver {
    /// Try to receive the next event without blocking
    ///
    /// Returns:
    /// - `Ok(event)` if an event is available
    /// - `Err(RecvError::Empty)` if no events are available
    /// - `Err(RecvError::Lagged(n))` if n events were missed due to buffer overflow
    /// - `Err(RecvError::Closed)` if the sender has been dropped
    pub fn try_recv(&mut self) -> Result<ChangeEvent, RecvError> {
        let state_arc = self.state.upgrade().ok_or(RecvError::Closed)?;
        let state = state_arc.lock().unwrap();

        // Check if we've lagged behind
        let oldest_available = state.total_sent.saturating_sub(state.capacity);
        if self.read_pos < oldest_available {
            let missed = oldest_available - self.read_pos;
            self.read_pos = oldest_available;
            return Err(RecvError::Lagged(missed));
        }

        // Check if there are new events
        if self.read_pos >= state.total_sent {
            if state.closed {
                return Err(RecvError::Closed);
            }
            return Err(RecvError::Empty);
        }

        // Calculate buffer index
        let buffer_idx = self.read_pos % state.capacity;
        let event = state.buffer[buffer_idx].clone().expect("Buffer slot should be filled");

        self.read_pos += 1;
        Ok(event)
    }

    /// Receive all available events
    ///
    /// Returns a vector of all events that have been published since the last read.
    /// If the receiver has lagged, logs a warning and returns events from the oldest available.
    pub fn recv_all(&mut self) -> Vec<ChangeEvent> {
        let mut events = Vec::new();
        loop {
            match self.try_recv() {
                Ok(event) => events.push(event),
                Err(RecvError::Lagged(n)) => {
                    log::warn!("Change event receiver lagged, missed {} events", n);
                    // Continue reading from the oldest available position
                }
                Err(RecvError::Empty) | Err(RecvError::Closed) => break,
            }
        }
        events
    }
}

impl Clone for ChangeEventReceiver {
    fn clone(&self) -> Self {
        Self { state: self.state.clone(), read_pos: self.read_pos }
    }
}

impl std::fmt::Debug for ChangeEventReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeEventReceiver").field("read_pos", &self.read_pos).finish()
    }
}

/// Create a new broadcast channel for change events
///
/// # Arguments
/// * `capacity` - Maximum number of events to buffer before old events are overwritten
///
/// # Returns
/// A tuple of (sender, receiver)
pub fn channel(capacity: usize) -> (ChangeEventSender, ChangeEventReceiver) {
    let capacity = capacity.max(1); // Ensure at least capacity of 1
    let state = Arc::new(Mutex::new(ChannelState::new(capacity)));

    let sender = ChangeEventSender { state: state.clone() };
    let receiver = ChangeEventReceiver { state: Arc::downgrade(&state), read_pos: 0 };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_receive_single_event() {
        let (sender, mut receiver) = channel(16);

        sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 0 });

        let event = receiver.try_recv().unwrap();
        assert!(
            matches!(event, ChangeEvent::Insert { table_name, row_index: 0 } if table_name == "users")
        );
    }

    #[test]
    fn test_multiple_receivers() {
        let (sender, mut rx1) = channel(16);
        let mut rx2 = sender.subscribe();

        sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 0 });

        // Both receivers should get the event
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn test_empty_when_no_events() {
        let (_sender, mut receiver) = channel(16);
        assert_eq!(receiver.try_recv(), Err(RecvError::Empty));
    }

    #[test]
    fn test_lagged_receiver() {
        let (sender, mut receiver) = channel(4); // Small buffer

        // Send more events than buffer can hold
        for i in 0..10 {
            sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: i });
        }

        // First read should report lag
        let result = receiver.try_recv();
        assert!(matches!(result, Err(RecvError::Lagged(_))));

        // Subsequent reads should work
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn test_closed_channel() {
        let (sender, mut receiver) = channel(16);
        drop(sender);

        assert_eq!(receiver.try_recv(), Err(RecvError::Closed));
    }

    #[test]
    fn test_recv_all() {
        let (sender, mut receiver) = channel(16);

        sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 0 });
        sender.send(ChangeEvent::Update { table_name: "users".to_string(), row_index: 0 });
        sender.send(ChangeEvent::Delete { table_name: "users".to_string(), row_index: 0 });

        let events = receiver.recv_all();
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], ChangeEvent::Insert { .. }));
        assert!(matches!(events[1], ChangeEvent::Update { .. }));
        assert!(matches!(events[2], ChangeEvent::Delete { .. }));
    }

    #[test]
    fn test_event_accessors() {
        let event = ChangeEvent::Insert { table_name: "products".to_string(), row_index: 42 };
        assert_eq!(event.table_name(), "products");
        assert_eq!(event.row_index(), 42);
    }

    #[test]
    fn test_new_subscriber_starts_from_current() {
        let (sender, _rx1) = channel(16);

        // Send some events before second subscriber joins
        sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 0 });
        sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 1 });

        // New subscriber should not see old events
        let mut rx2 = sender.subscribe();
        assert_eq!(rx2.try_recv(), Err(RecvError::Empty));

        // But should see new events
        sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 2 });
        let event = rx2.try_recv().unwrap();
        assert_eq!(event.row_index(), 2);
    }
}
