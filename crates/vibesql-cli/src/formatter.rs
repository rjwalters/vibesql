use prettytable::{Cell, Row, Table};

use crate::executor::QueryResult;

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
    Markdown,
    Html,
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
        }

        // Print timing if available
        if let Some(time_ms) = result.execution_time_ms {
            println!("{} rows in set ({:.3}s)", result.row_count, time_ms / 1000.0);
        } else {
            println!("{} rows", result.row_count);
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
            let cells: Vec<Cell> = row.iter().map(|val| Cell::new(val)).collect();
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
                    json_obj.insert(col.clone(), serde_json::Value::String(row[i].clone()));
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
            println!("{}", row.join(","));
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
                print!(" {} |", val);
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
                println!("      <td>{}</td>", Self::escape_html(val));
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_result() -> QueryResult {
        QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
            row_count: 2,
            execution_time_ms: None,
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
        let result =
            QueryResult { columns: vec![], rows: vec![], row_count: 0, execution_time_ms: None };

        // These should handle empty results gracefully
        formatter.print_markdown(&result);
        formatter.print_html(&result);
    }
}
