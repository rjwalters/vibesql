// ============================================================================
// Append Mode Tracker
// ============================================================================
//
// Tracks sequential insert patterns to optimize constraint validation.
// When sequential inserts are detected (append mode), constraint checks
// can skip duplicate lookups for better INSERT performance.

use vibesql_types::SqlValue;

/// Threshold for consecutive sequential inserts before enabling append mode
const APPEND_MODE_THRESHOLD: usize = 3;

/// Tracks append mode optimization state for a table
///
/// Append mode is activated when detecting sequential primary key inserts,
/// allowing constraint validation to skip expensive duplicate lookups.
/// This optimization was added in #840 to improve INSERT performance.
#[derive(Debug, Clone)]
pub struct AppendModeTracker {
    /// Last inserted primary key value (None if no inserts yet)
    last_pk_value: Option<Vec<SqlValue>>,

    /// Whether append mode is currently active
    append_mode: bool,

    /// Count of consecutive sequential inserts
    append_streak: usize,
}

impl AppendModeTracker {
    /// Create a new append mode tracker
    pub fn new() -> Self {
        Self { last_pk_value: None, append_mode: false, append_streak: 0 }
    }

    /// Update append mode tracking based on current primary key value
    ///
    /// Detects sequential inserts and enables append mode after reaching
    /// the threshold (3 consecutive sequential inserts).
    ///
    /// # Arguments
    /// * `pk_values` - Primary key values of the row being inserted
    ///
    /// # Behavior
    /// - First insert: records PK, no streak yet
    /// - Sequential insert (PK > last PK): increments streak
    /// - Non-sequential insert (PK <= last PK): resets append mode
    /// - After threshold consecutive sequential inserts: activates append mode
    pub fn update(&mut self, pk_values: &[SqlValue]) {
        if let Some(last_pk) = &self.last_pk_value {
            // Check if current PK is greater than last PK (sequential)
            if pk_values > last_pk.as_slice() {
                self.append_streak += 1;
                // Enable append mode after threshold consecutive sequential inserts
                if self.append_streak >= APPEND_MODE_THRESHOLD {
                    self.append_mode = true;
                }
            } else {
                // Non-sequential insert - reset append mode
                self.append_mode = false;
                self.append_streak = 0;
            }
        }
        // Update last PK value for next comparison
        self.last_pk_value = Some(pk_values.to_vec());
    }

    /// Check if table is in append mode (sequential inserts detected)
    ///
    /// When true, constraint checks can skip duplicate lookups for optimization.
    pub fn is_active(&self) -> bool {
        self.append_mode
    }

    /// Reset append mode tracking
    ///
    /// Should be called when table is cleared or truncated.
    pub fn reset(&mut self) {
        self.last_pk_value = None;
        self.append_mode = false;
        self.append_streak = 0;
    }

    // Expose internal state for testing
    #[cfg(test)]
    pub(crate) fn append_streak(&self) -> usize {
        self.append_streak
    }

    #[cfg(test)]
    pub(crate) fn last_pk_value(&self) -> Option<&Vec<SqlValue>> {
        self.last_pk_value.as_ref()
    }
}

impl Default for AppendModeTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tracker_not_active() {
        let tracker = AppendModeTracker::new();
        assert!(!tracker.is_active());
        assert_eq!(tracker.append_streak(), 0);
        assert_eq!(tracker.last_pk_value(), None);
    }

    #[test]
    fn test_first_insert_no_streak() {
        let mut tracker = AppendModeTracker::new();

        tracker.update(&[SqlValue::Integer(1)]);

        assert!(!tracker.is_active());
        assert_eq!(tracker.append_streak(), 0);
        assert_eq!(tracker.last_pk_value(), Some(&vec![SqlValue::Integer(1)]));
    }

    #[test]
    fn test_sequential_inserts_build_streak() {
        let mut tracker = AppendModeTracker::new();

        // First insert
        tracker.update(&[SqlValue::Integer(1)]);
        assert_eq!(tracker.append_streak(), 0);
        assert!(!tracker.is_active());

        // Second sequential
        tracker.update(&[SqlValue::Integer(2)]);
        assert_eq!(tracker.append_streak(), 1);
        assert!(!tracker.is_active());

        // Third sequential
        tracker.update(&[SqlValue::Integer(3)]);
        assert_eq!(tracker.append_streak(), 2);
        assert!(!tracker.is_active());

        // Fourth sequential - NOW append mode activates
        tracker.update(&[SqlValue::Integer(4)]);
        assert_eq!(tracker.append_streak(), 3);
        assert!(tracker.is_active());
    }

    #[test]
    fn test_non_sequential_resets_append_mode() {
        let mut tracker = AppendModeTracker::new();

        // Build up to append mode
        tracker.update(&[SqlValue::Integer(1)]);
        tracker.update(&[SqlValue::Integer(2)]);
        tracker.update(&[SqlValue::Integer(3)]);
        tracker.update(&[SqlValue::Integer(4)]);
        assert!(tracker.is_active());
        assert_eq!(tracker.append_streak(), 3);

        // Non-sequential insert (goes backward)
        tracker.update(&[SqlValue::Integer(2)]);
        assert!(!tracker.is_active());
        assert_eq!(tracker.append_streak(), 0);
    }

    #[test]
    fn test_equal_pk_resets_append_mode() {
        let mut tracker = AppendModeTracker::new();

        // Build up to append mode
        tracker.update(&[SqlValue::Integer(1)]);
        tracker.update(&[SqlValue::Integer(2)]);
        tracker.update(&[SqlValue::Integer(3)]);
        tracker.update(&[SqlValue::Integer(4)]);
        assert!(tracker.is_active());

        // Equal PK (duplicate) - should reset
        tracker.update(&[SqlValue::Integer(4)]);
        assert!(!tracker.is_active());
        assert_eq!(tracker.append_streak(), 0);
    }

    #[test]
    fn test_reset_clears_state() {
        let mut tracker = AppendModeTracker::new();

        // Build up to append mode
        tracker.update(&[SqlValue::Integer(1)]);
        tracker.update(&[SqlValue::Integer(2)]);
        tracker.update(&[SqlValue::Integer(3)]);
        tracker.update(&[SqlValue::Integer(4)]);
        assert!(tracker.is_active());

        // Reset
        tracker.reset();

        assert!(!tracker.is_active());
        assert_eq!(tracker.append_streak(), 0);
        assert_eq!(tracker.last_pk_value(), None);
    }

    #[test]
    fn test_composite_primary_key() {
        let mut tracker = AppendModeTracker::new();

        // Composite PK: (department, employee_id)
        tracker.update(&[SqlValue::Integer(1), SqlValue::Integer(100)]);
        tracker.update(&[SqlValue::Integer(1), SqlValue::Integer(101)]);
        tracker.update(&[SqlValue::Integer(1), SqlValue::Integer(102)]);
        tracker.update(&[SqlValue::Integer(1), SqlValue::Integer(103)]);

        assert!(tracker.is_active());
        assert_eq!(tracker.append_streak(), 3);
    }

    #[test]
    fn test_string_primary_keys() {
        let mut tracker = AppendModeTracker::new();

        tracker.update(&[SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        tracker.update(&[SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);
        tracker.update(&[SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]);
        tracker.update(&[SqlValue::Varchar(arcstr::ArcStr::from("David"))]);

        assert!(tracker.is_active());
    }

    #[test]
    fn test_continues_after_activation() {
        let mut tracker = AppendModeTracker::new();

        // Activate append mode
        tracker.update(&[SqlValue::Integer(1)]);
        tracker.update(&[SqlValue::Integer(2)]);
        tracker.update(&[SqlValue::Integer(3)]);
        tracker.update(&[SqlValue::Integer(4)]);
        assert!(tracker.is_active());

        // Continue in append mode
        tracker.update(&[SqlValue::Integer(100)]);
        assert!(tracker.is_active());
        assert_eq!(tracker.append_streak(), 4); // Streak continues
    }
}
