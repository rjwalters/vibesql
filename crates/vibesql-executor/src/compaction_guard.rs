//! Deferred-compaction coordination for interleaved per-row DML loops.
//!
//! Issue #5486: the UPDATE/DELETE executors fire row triggers interleaved per
//! row (BEFORE -> apply -> AFTER) while iterating over a table's *physical* row
//! indices. A trigger body may recursively DELETE from that same table (e.g.
//! fkey2-4.3's recursive AFTER DELETE cascade, or trigger1-1.10). If such a
//! nested DELETE compacts the table, every physical index shifts and the
//! ancestor loop's not-yet-processed indices become invalid.
//!
//! To prevent that, an interleaved DML loop registers the table it is iterating
//! over via [`IterationGuard`]. While a table is registered, nested DELETEs on
//! that table defer compaction ([`is_iterating`] returns `true`); the
//! outermost loop performs a single compaction once it finishes.
//!
//! Deletes on a table that is *not* currently under iteration compact normally
//! — including a nested DELETE on a different table (e.g. an INSTEAD OF DELETE
//! trigger on a view whose body deletes from a base table that no ancestor is
//! iterating).

use std::cell::RefCell;

thread_local! {
    /// Tables currently being iterated by an interleaved per-row DML loop on
    /// this thread. A multiset (Vec) so nested loops on the same table register
    /// independently and each unregisters exactly one entry on drop.
    static ITERATING_TABLES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// RAII guard that registers `table_name` as under interleaved iteration for
/// its lifetime. Nested DELETEs on the same table will defer compaction while
/// this guard is alive.
pub struct IterationGuard {
    table_name: String,
}

impl IterationGuard {
    /// Register `table_name` as under interleaved iteration.
    pub fn new(table_name: &str) -> Self {
        ITERATING_TABLES.with(|tables| tables.borrow_mut().push(table_name.to_string()));
        IterationGuard { table_name: table_name.to_string() }
    }
}

impl Drop for IterationGuard {
    fn drop(&mut self) {
        ITERATING_TABLES.with(|tables| {
            let mut tables = tables.borrow_mut();
            // Remove one matching entry (the most recent), leaving any
            // outer-loop registration of the same table intact.
            if let Some(pos) = tables.iter().rposition(|t| t == &self.table_name) {
                tables.remove(pos);
            }
        });
    }
}

/// Returns `true` if `table_name` is currently being iterated by an interleaved
/// per-row DML loop on this thread (case-sensitive match on the canonical
/// table name the loops register). Callers use this to decide whether to defer
/// compaction of that table.
pub fn is_iterating(table_name: &str) -> bool {
    ITERATING_TABLES.with(|tables| tables.borrow().iter().any(|t| t == table_name))
}
