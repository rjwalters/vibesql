/// Check if a database path represents an in-memory database.
/// SQLite uses ":memory:" as a special value for in-memory databases.
pub fn is_memory_database(path: &str) -> bool {
    path == ":memory:" || path == "file::memory:" || path.starts_with("file::memory:?")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_database_detection() {
        // Standard :memory: format
        assert!(is_memory_database(":memory:"));

        // URI format
        assert!(is_memory_database("file::memory:"));

        // URI format with parameters
        assert!(is_memory_database("file::memory:?cache=shared"));

        // Regular file paths should not be detected as memory
        assert!(!is_memory_database("test.db"));
        assert!(!is_memory_database("/path/to/database.db"));
        assert!(!is_memory_database("memory.db"));
        assert!(!is_memory_database(":memory")); // Missing trailing colon
    }
}
