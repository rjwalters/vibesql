//! Arena-scoped string interning for efficient identifier storage.
//!
//! This module provides string interning to deduplicate identifiers in the AST.
//! Instead of storing full `&'arena str` slices (16 bytes), we use `Symbol`
//! indices (4 bytes) that reference interned strings.
//!
//! # Example
//!
//! ```text
//! use bumpalo::Bump;
//! use vibesql_ast::arena::{ArenaInterner, Symbol};
//!
//! let arena = Bump::new();
//! let mut interner = ArenaInterner::new(&arena);
//!
//! let sym1 = interner.intern("users");
//! let sym2 = interner.intern("users"); // Returns same symbol
//! assert_eq!(sym1, sym2);
//!
//! let name = interner.resolve(sym1);
//! assert_eq!(name, "users");
//! ```

use bumpalo::collections::Vec as BumpVec;
use bumpalo::Bump;
use std::collections::HashMap;

/// A symbol representing an interned string.
///
/// This is a compact 4-byte identifier that can be used to look up the actual
/// string value in the interner. Using symbols instead of string slices reduces
/// memory usage from 16 bytes to 4 bytes per identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Symbol(u32);

impl Symbol {
    /// Creates a new symbol from a raw index.
    ///
    /// This is primarily for internal use. Prefer using `ArenaInterner::intern`.
    #[inline]
    pub const fn from_raw(index: u32) -> Self {
        Symbol(index)
    }

    /// Returns the raw index of this symbol.
    #[inline]
    pub const fn as_raw(self) -> u32 {
        self.0
    }
}

/// Arena-scoped string interner for efficient identifier deduplication.
///
/// The interner stores strings in an arena and maintains a mapping from
/// string content to symbol indices. Interning the same string twice
/// returns the same symbol.
pub struct ArenaInterner<'arena> {
    /// Arena-allocated storage for interned strings.
    arena: &'arena Bump,
    /// Maps string content to symbol indices for deduplication.
    map: HashMap<&'arena str, Symbol>,
    /// Vector of interned strings, indexed by symbol.
    strings: BumpVec<'arena, &'arena str>,
}

impl<'arena> ArenaInterner<'arena> {
    /// Creates a new interner using the given arena for string storage.
    pub fn new(arena: &'arena Bump) -> Self {
        ArenaInterner { arena, map: HashMap::new(), strings: BumpVec::new_in(arena) }
    }

    /// Creates a new interner with pre-allocated capacity.
    pub fn with_capacity(arena: &'arena Bump, capacity: usize) -> Self {
        let mut strings = BumpVec::new_in(arena);
        strings.reserve(capacity);
        ArenaInterner { arena, map: HashMap::with_capacity(capacity), strings }
    }

    /// Interns a string, returning a symbol that can be used to retrieve it.
    ///
    /// If the string was already interned, returns the existing symbol.
    /// Otherwise, allocates the string in the arena and creates a new symbol.
    #[inline]
    pub fn intern(&mut self, s: &str) -> Symbol {
        // Check if already interned
        if let Some(&sym) = self.map.get(s) {
            return sym;
        }

        // Allocate string in arena
        let interned: &'arena str = self.arena.alloc_str(s);

        // Create new symbol
        let sym = Symbol(self.strings.len() as u32);
        self.strings.push(interned);
        self.map.insert(interned, sym);

        sym
    }

    /// Resolves a symbol to its string value.
    ///
    /// # Panics
    ///
    /// Panics if the symbol was not created by this interner.
    #[inline]
    pub fn resolve(&self, sym: Symbol) -> &'arena str {
        self.strings[sym.0 as usize]
    }

    /// Tries to resolve a symbol, returning `None` if invalid.
    #[inline]
    pub fn try_resolve(&self, sym: Symbol) -> Option<&'arena str> {
        self.strings.get(sym.0 as usize).copied()
    }

    /// Returns the number of unique strings interned.
    #[inline]
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Returns true if no strings have been interned.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    /// Returns an iterator over all interned strings and their symbols.
    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &'arena str)> + '_ {
        self.strings.iter().enumerate().map(|(i, s)| (Symbol(i as u32), *s))
    }
}

impl std::fmt::Debug for ArenaInterner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaInterner").field("len", &self.strings.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_size() {
        // Verify Symbol is exactly 4 bytes
        assert_eq!(std::mem::size_of::<Symbol>(), 4);
    }

    #[test]
    fn test_intern_and_resolve() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);

        let sym = interner.intern("hello");
        assert_eq!(interner.resolve(sym), "hello");
    }

    #[test]
    fn test_intern_deduplication() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);

        let sym1 = interner.intern("users");
        let sym2 = interner.intern("users");
        let sym3 = interner.intern("orders");

        assert_eq!(sym1, sym2);
        assert_ne!(sym1, sym3);
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_symbol_equality() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);

        let id1 = interner.intern("id");
        let id2 = interner.intern("id");
        let name = interner.intern("name");

        // Same string -> same symbol -> fast O(1) comparison
        assert_eq!(id1, id2);
        assert_ne!(id1, name);
    }

    #[test]
    fn test_empty_string() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);

        let sym = interner.intern("");
        assert_eq!(interner.resolve(sym), "");
    }

    #[test]
    fn test_iter() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);

        interner.intern("a");
        interner.intern("b");
        interner.intern("c");

        let collected: Vec<_> = interner.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].1, "a");
        assert_eq!(collected[1].1, "b");
        assert_eq!(collected[2].1, "c");
    }

    #[test]
    fn test_try_resolve() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);

        let sym = interner.intern("test");
        assert_eq!(interner.try_resolve(sym), Some("test"));

        // Invalid symbol (larger than any interned)
        let invalid = Symbol::from_raw(999);
        assert_eq!(interner.try_resolve(invalid), None);
    }
}
