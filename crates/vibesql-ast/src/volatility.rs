//! Volatile (non-deterministic) function classification.
//!
//! Single source of truth for which SQL function names are — or may be —
//! non-deterministic. Two consumers with different conservatism needs
//! share this list:
//!
//! - The optimizer (`vibesql-executor::optimizer::window_pushdown`) must not push predicates
//!   containing functions whose value can change between evaluations; it uses the coarse
//!   [`is_volatile_function`] union.
//! - Raft replication (`vibesql-consensus::freeze`) must freeze or reject non-deterministic
//!   expressions before a statement is logged; it additionally needs the finer-grained category
//!   predicates to decide *how* a volatile call site can be made deterministic.
//!
//! Names are matched against [`FunctionIdentifier::canonical`] output
//! (lowercase for unquoted identifiers).
//!
//! [`FunctionIdentifier::canonical`]: crate::FunctionIdentifier::canonical

/// Functions whose value differs on every call: `random()`,
/// `randomblob(n)`, and the MySQL-style `rand()`. Per-row volatile — two
/// evaluations within the same statement yield different values.
pub fn is_random_function(canonical_name: &str) -> bool {
    matches!(canonical_name, "rand" | "random" | "randomblob")
}

/// Functions that read connection/session state: `changes()`,
/// `total_changes()`, `last_insert_rowid()`. Deterministic within one
/// session's view, but the value depends on session history that is not
/// part of replicated database state.
pub fn is_session_state_function(canonical_name: &str) -> bool {
    matches!(canonical_name, "changes" | "total_changes" | "last_insert_rowid")
}

/// Zero-argument clock readings: `now()` and the function-call forms of
/// `current_date`/`current_time`/`current_timestamp`. Always
/// non-deterministic (wall clock), but stable within a statement under
/// SQLite semantics.
pub fn is_clock_function(canonical_name: &str) -> bool {
    matches!(canonical_name, "now" | "current_date" | "current_time" | "current_timestamp")
}

/// SQLite date/time functions that are non-deterministic **only when
/// invoked with `'now'`** (explicitly, or implicitly by omitting the
/// time-value argument) or with the timezone-dependent
/// `'localtime'`/`'utc'` modifiers. With an explicit time value and no
/// timezone modifier they are pure functions of their arguments.
pub fn is_datetime_family_function(canonical_name: &str) -> bool {
    matches!(
        canonical_name,
        "date" | "time" | "datetime" | "julianday" | "unixepoch" | "strftime" | "timediff"
    )
}

/// Coarse union: `true` if the function is (or may be, depending on its
/// arguments) non-deterministic. This is the conservative classification
/// the optimizer uses to refuse predicate push-down.
pub fn is_volatile_function(canonical_name: &str) -> bool {
    is_random_function(canonical_name)
        || is_session_state_function(canonical_name)
        || is_clock_function(canonical_name)
        || is_datetime_family_function(canonical_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_categories_are_disjoint_and_unioned() {
        let all = [
            "rand",
            "random",
            "randomblob",
            "changes",
            "total_changes",
            "last_insert_rowid",
            "now",
            "current_date",
            "current_time",
            "current_timestamp",
            "date",
            "time",
            "datetime",
            "julianday",
            "unixepoch",
            "strftime",
            "timediff",
        ];
        for name in all {
            assert!(is_volatile_function(name), "{name} must be volatile");
            let categories = [
                is_random_function(name),
                is_session_state_function(name),
                is_clock_function(name),
                is_datetime_family_function(name),
            ];
            assert_eq!(
                categories.iter().filter(|c| **c).count(),
                1,
                "{name} must be in exactly one category"
            );
        }
        for name in ["abs", "upper", "coalesce", "length", "substr"] {
            assert!(!is_volatile_function(name), "{name} must not be volatile");
        }
    }
}
