//! Location tracking for sqllogictest files.

use std::{fmt, sync::Arc};

/// The location in source file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Location {
    pub(crate) file: Arc<str>,
    pub(crate) line: u32,
    pub(crate) upper: Option<Arc<Location>>,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)?;
        if let Some(upper) = &self.upper {
            write!(f, "\nat {upper}")?;
        }
        Ok(())
    }
}

impl Location {
    /// File path.
    pub fn file(&self) -> &str {
        &self.file
    }

    /// Line number.
    pub fn line(&self) -> u32 {
        self.line
    }

    /// Creates a new location.
    pub(crate) fn new(file: impl Into<Arc<str>>, line: u32) -> Self {
        Self { file: file.into(), line, upper: None }
    }

    /// Returns the location of next line.
    #[must_use]
    pub(crate) fn next_line(mut self) -> Self {
        self.line += 1;
        self
    }

    /// Returns the location of next level file.
    pub(crate) fn include(&self, file: &str) -> Self {
        Self { file: file.into(), line: 0, upper: Some(Arc::new(self.clone())) }
    }
}
