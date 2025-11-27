//! Output handling and validation for sqllogictest execution.

#![allow(clippy::redundant_closure)]

use itertools::Itertools;

use crate::error_handling::AnyError;
use crate::ColumnType;

/// Output of a record.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RecordOutput<T: ColumnType> {
    /// No output. Occurs when the record is skipped or not a `query`, `statement`, or `system`
    /// command.
    Nothing,
    /// The output of a `query`.
    Query {
        types: Vec<T>,
        rows: Vec<Vec<String>>,
        error: Option<AnyError>,
    },
    /// The output of a `statement`.
    Statement { count: u64, error: Option<AnyError> },
    /// The output of a `system` command.
    #[non_exhaustive]
    System {
        stdout: Option<String>,
        error: Option<AnyError>,
    },
}

#[non_exhaustive]
pub enum DBOutput<T: ColumnType> {
    Rows {
        types: Vec<T>,
        rows: Vec<Vec<String>>,
    },
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    ///
    /// If the test case doesn't specify `statement count <n>`, the number is simply ignored.
    StatementComplete(u64),
}

/// Normalizer will be used by [`Runner`] to normalize the result values
///
/// # Default
///
/// By default, the ([`default_normalizer`]) will be used to normalize values.
pub type Normalizer = fn(s: &String) -> String;

/// Trim and replace multiple whitespaces with one.
#[allow(clippy::ptr_arg)]
pub fn default_normalizer(s: &String) -> String {
    s.trim().split_ascii_whitespace().join(" ")
}

/// Validator will be used by [`Runner`] to validate the output.
///
/// # Default
///
/// By default, the ([`default_validator`]) will be used compare normalized results.
pub type Validator =
    fn(normalizer: Normalizer, actual: &[Vec<String>], expected: &[String]) -> bool;

pub fn default_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    // Support ignore marker <slt:ignore> to skip volatile parts of output.
    const IGNORE_MARKER: &str = "<slt:ignore>";
    let contains_ignore_marker = expected.iter().any(|line| line.contains(IGNORE_MARKER));

    // Normalize expected lines.
    // If ignore marker present, perform fragment-based matching on the full snapshot.
    if contains_ignore_marker {
        // If ignore marker present, perform fragment-based matching on the full snapshot.
        // The actual results might contain \n, and may not be a normal "row", which is not suitable to normalize.
        let expected_results = expected;
        // Flatten the rows so each column value becomes its own line
        let actual_rows: Vec<String> = actual
            .iter()
            .flat_map(|strs| strs.iter().map(|s| s.to_string()))
            .collect_vec();

        let expected_snapshot = expected_results.join("\n");
        let actual_snapshot = actual_rows.join("\n");
        let fragments: Vec<&str> = expected_snapshot.split(IGNORE_MARKER).collect();
        let mut pos = 0;
        for frag in fragments {
            if frag.is_empty() {
                continue;
            }
            if let Some(idx) = actual_snapshot[pos..].find(frag) {
                pos += idx + frag.len();
            } else {
                tracing::error!(
                    "mismatch at: {}\nexpected: {}\nactual: {}",
                    pos,
                    frag,
                    &actual_snapshot[pos..]
                );
                return false;
            }
        }
        return true;
    }

    let expected_results = expected.iter().map(normalizer).collect_vec();
    // Default, we compare normalized results. Whitespace characters are ignored.

    // Determine if expected format is flattened (one value per line) or joined (one row per line)
    // by checking if the number of expected entries matches the total number of column values
    let row_width = actual.first().map(|row| row.len()).unwrap_or(0);
    let total_values = actual.len() * row_width;
    let is_flattened = expected.len() == total_values && row_width > 1;

    let normalized_rows: Vec<String> = if is_flattened {
        // Expected format is flattened: each column value on its own line
        actual
            .iter()
            .flat_map(|strs| strs.iter().map(normalizer))
            .collect_vec()
    } else {
        // Expected format is joined: each row with space-separated columns
        actual
            .iter()
            .map(|row_cols| {
                let joined = row_cols.iter().map(|s| normalizer(s)).collect::<Vec<_>>().join(" ");
                normalizer(&joined)
            })
            .collect_vec()
    };

    normalized_rows == expected_results
}

/// [`Runner`] uses this validator to check that the expected column types match an actual output.
///
/// # Default
///
/// By default ([`default_column_validator`]), columns are not validated.
pub type ColumnTypeValidator<T> = fn(actual: &Vec<T>, expected: &Vec<T>) -> bool;

/// The default validator always returns success for any inputs of expected and actual sets of
/// columns.
pub fn default_column_validator<T: ColumnType>(_: &Vec<T>, _: &Vec<T>) -> bool {
    true
}

/// The strict validator checks:
/// - the number of columns is as expected
/// - each column has the same type as expected
#[allow(clippy::ptr_arg)]
pub fn strict_column_validator<T: ColumnType>(actual: &Vec<T>, expected: &Vec<T>) -> bool {
    actual.len() == expected.len()
        && !actual
            .iter()
            .zip(expected.iter())
            .any(|(actual_column, expected_column)| actual_column != expected_column)
}
