use prettytable::{Cell, Row, Table};
use vibesql_l10n::vibe_msg;

use crate::executor::QueryResult;

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
    Markdown,
    Html,
    /// Raw output format: values framed with ASCII 31/30 control bytes (see
    /// `print_raw`). NULL values are represented by a dedicated sentinel byte,
    /// distinct from an actual empty-string value.
    /// Compatible with SQLite TCL test harness expectations.
    Raw,
}

pub struct ResultFormatter {
    format: OutputFormat,
}

impl ResultFormatter {
    pub fn new() -> Self {
        ResultFormatter { format: OutputFormat::Table }
    }

    pub fn set_format(&mut self, format: OutputFormat) {
        self.format = format;
    }

    pub fn print_result(&self, result: &QueryResult) {
        match self.format {
            OutputFormat::Table => self.print_table(result),
            OutputFormat::Json => self.print_json(result),
            OutputFormat::Csv => self.print_csv(result),
            OutputFormat::Markdown => self.print_markdown(result),
            OutputFormat::Html => self.print_html(result),
            OutputFormat::Raw => {
                // Raw format: no timing info, no DDL messages, just raw values
                // This is for scripted/automated use (e.g., TCL test harness)
                self.print_raw(result);
                return;
            }
        }

        // Print DDL messages for non-raw formats (interactive use)
        if let Some(ref msg) = result.message {
            println!("{}", msg);
        }

        // Print timing if available (not for raw format)
        if let Some(time_ms) = result.execution_time_ms {
            println!(
                "{}",
                vibe_msg!(
                    "rows-with-time",
                    count = result.row_count as i64,
                    time = format!("{:.3}", time_ms / 1000.0)
                )
            );
        } else {
            println!("{}", vibe_msg!("rows-count", count = result.row_count as i64));
        }
    }

    fn print_table(&self, result: &QueryResult) {
        if result.columns.is_empty() {
            return;
        }

        let mut table = Table::new();

        // Add header
        let header_cells: Vec<Cell> = result.columns.iter().map(|col| Cell::new(col)).collect();
        table.add_row(Row::new(header_cells));

        // Add rows
        for row in &result.rows {
            let cells: Vec<Cell> = row
                .iter()
                .map(|val| {
                    // For table display, show "NULL" for actual NULL values
                    Cell::new(val.as_ref().map(|s| s.as_str()).unwrap_or("NULL"))
                })
                .collect();
            table.add_row(Row::new(cells));
        }

        table.printstd();
    }

    fn print_json(&self, result: &QueryResult) {
        let mut json_rows = Vec::new();
        for row in &result.rows {
            let mut json_obj = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                if i < row.len() {
                    // For JSON, use proper null for NULL values
                    let value = match &row[i] {
                        Some(s) => serde_json::Value::String(s.clone()),
                        None => serde_json::Value::Null,
                    };
                    json_obj.insert(col.clone(), value);
                }
            }
            json_rows.push(serde_json::Value::Object(json_obj));
        }

        let output = serde_json::to_string_pretty(&json_rows).unwrap_or_else(|_| "[]".to_string());
        println!("{}", output);
    }

    fn print_csv(&self, result: &QueryResult) {
        // Print header
        println!("{}", result.columns.join(","));

        // Print rows
        for row in &result.rows {
            // For CSV, NULL values are empty
            let values: Vec<&str> =
                row.iter().map(|val| val.as_ref().map(|s| s.as_str()).unwrap_or("")).collect();
            println!("{}", values.join(","));
        }
    }

    fn print_markdown(&self, result: &QueryResult) {
        if result.columns.is_empty() {
            return;
        }

        // Print header row
        print!("|");
        for col in &result.columns {
            print!(" {} |", col);
        }
        println!();

        // Print separator row
        print!("|");
        for _ in &result.columns {
            print!("---|");
        }
        println!();

        // Print data rows
        for row in &result.rows {
            print!("|");
            for val in row {
                // For markdown, show "NULL" for actual NULL values
                print!(" {} |", val.as_ref().map(|s| s.as_str()).unwrap_or("NULL"));
            }
            println!();
        }
    }

    fn print_html(&self, result: &QueryResult) {
        if result.columns.is_empty() {
            println!("<table></table>");
            return;
        }

        println!("<table>");

        // Print header
        println!("  <thead>");
        println!("    <tr>");
        for col in &result.columns {
            println!("      <th>{}</th>", Self::escape_html(col));
        }
        println!("    </tr>");
        println!("  </thead>");

        // Print body
        println!("  <tbody>");
        for row in &result.rows {
            println!("    <tr>");
            for val in row {
                // For HTML, show "NULL" for actual NULL values
                let display = val.as_ref().map(|s| s.as_str()).unwrap_or("NULL");
                println!("      <td>{}</td>", Self::escape_html(display));
            }
            println!("    </tr>");
        }
        println!("  </tbody>");

        println!("</table>");
    }

    fn escape_html(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    }

    /// Print results in raw format for SQLite TCL test compatibility.
    /// - ASCII 31 (Unit Separator, `\x1f`) between values within each row
    /// - ASCII 30 (Record Separator, `\x1e`) terminating each row
    /// - NULL values output as the ASCII 1 (`\x01`) sentinel (see [`Self::NULL_SENTINEL`]); an
    ///   actual empty-string value is emitted as literal empty text, distinct from NULL
    /// - No headers, no borders, no row count
    ///
    /// We use ASCII 31 between values (instead of pipe) because pipe can appear
    /// in SQL string values. We terminate rows with ASCII 30 (instead of a plain
    /// newline) because a single column VALUE may itself contain an embedded
    /// newline (e.g. the verbatim multi-line `CREATE TABLE` text stored in
    /// `sqlite_master.sql`). Using `\n` as the row terminator made such values
    /// ambiguous with the row boundary, mis-splitting one row into several.
    /// ASCII 30/31 are the canonical control characters for record/field
    /// framing and cannot occur in ordinary SQL values, so embedded newlines are
    /// preserved verbatim. The TCL shim (scripts/tester_vibesql.tcl,
    /// `parse_raw_result`) splits on these same separators.
    fn print_raw(&self, result: &QueryResult) {
        // Single write; no trailing newline. The harness frames rows on \x1e.
        print!("{}", Self::format_raw(result));
    }

    /// ASCII 1 (Start of Heading). Emitted in place of a value to mean "this
    /// column is actually NULL" — distinct from an actual empty-string value,
    /// which is emitted as literal empty text. Chosen for the same reason as
    /// the `\x1e`/`\x1f` framing bytes: a control character that cannot occur
    /// in ordinary SQL text, so it is unambiguous on the wire (#6175).
    ///
    /// Without this sentinel, `None` (SQL NULL) and `Some("")` (an empty
    /// string) both serialize to the same empty slot between separators, so a
    /// consumer with a customized NULL representation (SQLite TCL's `db
    /// nullvalue`) cannot tell them apart — e.g. `PRAGMA table_info` reporting
    /// a NULL `dflt_value` (no DEFAULT clause) vs. an empty-string
    /// `dflt_value` (`DEFAULT ''`).
    const NULL_SENTINEL: &'static str = "\x01";

    /// Build the raw-format payload for `result` (see `print_raw`).
    ///
    /// Columns are joined with ASCII 31 (`\x1f`) and each row is terminated with
    /// ASCII 30 (`\x1e`). Zero rows yields an empty string. Extracted as a pure
    /// function so the exact framing can be unit-tested.
    fn format_raw(result: &QueryResult) -> String {
        let mut out = String::new();
        for row in &result.rows {
            let values: Vec<&str> = row
                .iter()
                .map(|val| {
                    // None represents actual SQL NULL -> the NULL sentinel.
                    // Some(s) is a real value (including an empty string,
                    // which must round-trip as empty text, not NULL).
                    val.as_ref().map(|s| s.as_str()).unwrap_or(Self::NULL_SENTINEL)
                })
                .collect();
            out.push_str(&values.join("\x1f"));
            out.push('\x1e');
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_result() -> QueryResult {
        QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![Some("1".to_string()), Some("Alice".to_string())],
                vec![Some("2".to_string()), Some("Bob".to_string())],
            ],
            row_count: 2,
            execution_time_ms: None,
            message: None,
        }
    }

    #[test]
    fn test_html_escaping() {
        assert_eq!(
            ResultFormatter::escape_html("<script>alert('xss')</script>"),
            "&lt;script&gt;alert(&#39;xss&#39;)&lt;/script&gt;"
        );
        assert_eq!(ResultFormatter::escape_html("A & B"), "A &amp; B");
    }

    #[test]
    fn test_markdown_format() {
        let formatter = ResultFormatter::new();
        let result = create_test_result();

        // Just verify it doesn't panic - output goes to stdout
        formatter.print_markdown(&result);
    }

    #[test]
    fn test_html_format() {
        let formatter = ResultFormatter::new();
        let result = create_test_result();

        // Just verify it doesn't panic - output goes to stdout
        formatter.print_html(&result);
    }

    #[test]
    fn test_empty_result() {
        let formatter = ResultFormatter::new();
        let result = QueryResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };

        // These should handle empty results gracefully
        formatter.print_markdown(&result);
        formatter.print_html(&result);
    }

    #[test]
    fn test_raw_format() {
        let mut formatter = ResultFormatter::new();
        formatter.set_format(OutputFormat::Raw);
        let result = create_test_result();

        // Just verify it doesn't panic - output goes to stdout
        formatter.print_raw(&result);
    }

    #[test]
    fn test_raw_format_with_nulls() {
        let mut formatter = ResultFormatter::new();
        formatter.set_format(OutputFormat::Raw);
        let result = QueryResult {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            rows: vec![
                // First row: value, NULL (actual), value
                vec![Some("1".to_string()), None, Some("3".to_string())],
                // Second row: empty string (not NULL), value, NULL (actual)
                vec![Some(String::new()), Some("test".to_string()), None],
            ],
            row_count: 2,
            execution_time_ms: None,
            message: None,
        };

        // Just verify it doesn't panic - output goes to stdout
        // NULL values (None) should be converted to the NULL_SENTINEL byte,
        // distinct from an actual empty-string value.
        formatter.print_raw(&result);
    }

    #[test]
    fn test_raw_format_null_vs_null_string() {
        let mut formatter = ResultFormatter::new();
        formatter.set_format(OutputFormat::Raw);
        let result = QueryResult {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            rows: vec![
                // Row: actual NULL, literal string "NULL", regular value
                vec![None, Some("NULL".to_string()), Some("hello".to_string())],
            ],
            row_count: 1,
            execution_time_ms: None,
            message: None,
        };

        // NULL (None) should become the sentinel, but "NULL" string
        // (Some("NULL")) should stay "NULL"
        formatter.print_raw(&result);
    }

    #[test]
    fn test_raw_format_empty_result() {
        let mut formatter = ResultFormatter::new();
        formatter.set_format(OutputFormat::Raw);
        let result = QueryResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };

        // Should handle empty results gracefully
        formatter.print_raw(&result);
    }

    #[test]
    fn test_format_raw_framing() {
        // Rows are terminated with \x1e (Record Separator) and columns are
        // joined with \x1f (Unit Separator). Zero rows yields empty output.
        let result = create_test_result();
        assert_eq!(ResultFormatter::format_raw(&result), "1\x1fAlice\x1e2\x1fBob\x1e");

        // NULL (None) renders as the NULL_SENTINEL byte; a real "NULL" string stays.
        let nulls = QueryResult {
            columns: vec!["a".to_string(), "b".to_string()],
            rows: vec![vec![None, Some("NULL".to_string())]],
            row_count: 1,
            execution_time_ms: None,
            message: None,
        };
        assert_eq!(ResultFormatter::format_raw(&nulls), "\x01\x1fNULL\x1e");

        // An actual empty-string value (Some("")) is distinct from NULL: it
        // renders as literal empty text, not the sentinel.
        let empty_string_value = QueryResult {
            columns: vec!["a".to_string(), "b".to_string()],
            rows: vec![vec![Some(String::new()), None]],
            row_count: 1,
            execution_time_ms: None,
            message: None,
        };
        assert_eq!(ResultFormatter::format_raw(&empty_string_value), "\x1f\x01\x1e");

        // Zero rows => empty string.
        let empty = QueryResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };
        assert_eq!(ResultFormatter::format_raw(&empty), "");
    }

    #[test]
    fn test_format_raw_preserves_embedded_newline() {
        // A single column value containing an embedded newline (e.g. the
        // verbatim multi-line CREATE TABLE text from sqlite_master.sql) must be
        // emitted verbatim, framed by a single trailing \x1e — NOT split into
        // multiple rows. This is the bug fixed by issue #5630.
        let multiline = "CREATE TABLE t(\n  a INT\n)";
        let result = QueryResult {
            columns: vec!["sql".to_string()],
            rows: vec![vec![Some(multiline.to_string())]],
            row_count: 1,
            execution_time_ms: None,
            message: None,
        };
        let out = ResultFormatter::format_raw(&result);
        assert_eq!(out, format!("{}\x1e", multiline));
        // Exactly one row terminator: the embedded \n did not create a boundary.
        assert_eq!(out.matches('\x1e').count(), 1);
    }
}
