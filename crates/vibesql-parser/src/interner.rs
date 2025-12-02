//! String interning for SQL identifiers.
//!
//! This module provides string interning to deduplicate identical identifier strings,
//! reducing memory usage and enabling fast pointer-based equality comparisons.

use string_interner::{backend::StringBackend, DefaultSymbol, StringInterner};

/// Symbol representing an interned string.
///
/// This is a lightweight handle (usize) that can be used to retrieve the original string
/// from the interner. Equality comparisons are O(1) pointer comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StringSymbol(DefaultSymbol);

impl StringSymbol {
    /// Create a new StringSymbol from a DefaultSymbol.
    #[inline]
    pub(crate) fn new(symbol: DefaultSymbol) -> Self {
        StringSymbol(symbol)
    }

    /// Get the underlying symbol.
    #[inline]
    pub(crate) fn inner(self) -> DefaultSymbol {
        self.0
    }
}

/// Interner for SQL identifiers.
///
/// Deduplicates identical strings and provides fast symbol-based equality.
/// The interner is owned by the lexer and passed to the parser for resolution.
#[derive(Debug, Default)]
pub struct IdentifierInterner {
    interner: StringInterner<StringBackend>,
}

impl IdentifierInterner {
    /// Create a new empty interner.
    #[inline]
    pub fn new() -> Self {
        IdentifierInterner { interner: StringInterner::new() }
    }

    /// Intern a string and return its symbol.
    ///
    /// If the string was already interned, returns the existing symbol.
    /// This is O(1) for cache hits and O(n) for new strings.
    #[inline]
    pub fn get_or_intern(&mut self, string: impl AsRef<str>) -> StringSymbol {
        StringSymbol::new(self.interner.get_or_intern(string))
    }

    /// Resolve a symbol back to its string.
    ///
    /// Returns None if the symbol is not from this interner.
    #[inline]
    pub fn resolve(&self, symbol: StringSymbol) -> Option<&str> {
        self.interner.resolve(symbol.inner())
    }

    /// Resolve a symbol, panicking if not found.
    ///
    /// Use this when you're certain the symbol came from this interner.
    #[inline]
    pub fn resolve_unchecked(&self, symbol: StringSymbol) -> &str {
        self.interner.resolve(symbol.inner()).expect("symbol not found in interner")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_and_resolve() {
        let mut interner = IdentifierInterner::new();
        let sym1 = interner.get_or_intern("hello");
        let sym2 = interner.get_or_intern("world");
        let sym3 = interner.get_or_intern("hello"); // duplicate

        assert_eq!(sym1, sym3); // same symbol for same string
        assert_ne!(sym1, sym2); // different symbols for different strings

        assert_eq!(interner.resolve(sym1), Some("hello"));
        assert_eq!(interner.resolve(sym2), Some("world"));
    }

    #[test]
    fn test_symbol_equality_is_fast() {
        let mut interner = IdentifierInterner::new();
        let sym1 = interner.get_or_intern("a_very_long_identifier_name");
        let sym2 = interner.get_or_intern("a_very_long_identifier_name");

        // This is an O(1) comparison, not O(n) string comparison
        assert_eq!(sym1, sym2);
    }
}
