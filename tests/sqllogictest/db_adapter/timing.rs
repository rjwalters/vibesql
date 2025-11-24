//! Statement timing and performance instrumentation.
//!
//! This module tracks execution times for different statement types (INSERT, CREATE INDEX,
//! SELECT, etc.) and provides statistics about query performance including average times,
//! slowest queries, and detailed timing summaries.

use std::time::Duration;

/// Safely truncate a string to a maximum byte length, respecting UTF-8 character boundaries.
/// Returns the truncated string with "..." appended if truncation occurred.
pub fn truncate_sql(sql: &str, max_bytes: usize) -> String {
    if sql.len() <= max_bytes {
        return sql.to_string();
    }

    // Find the last valid UTF-8 character boundary at or before max_bytes
    let mut boundary = max_bytes;
    while boundary > 0 && !sql.is_char_boundary(boundary) {
        boundary -= 1;
    }

    format!("{}...", &sql[..boundary])
}

/// Statement timing statistics for a category of statements.
#[derive(Debug, Default)]
pub struct StatementTimings {
    pub count: usize,
    pub total_duration: Duration,
    pub max_duration: Duration,
    pub max_sql: Option<String>,
}

impl StatementTimings {
    /// Record a statement execution time.
    pub fn record(&mut self, duration: Duration, sql: &str) {
        self.count += 1;
        self.total_duration += duration;
        if duration > self.max_duration {
            self.max_duration = duration;
            // Store truncated SQL for the slowest statement
            self.max_sql = Some(truncate_sql(sql, 100));
        }
    }
}

/// Timing instrumentation tracker for all statement types.
#[derive(Debug, Default)]
pub struct TimingTracker {
    pub enabled: bool,
    pub slow_query_threshold_ms: u64,
    pub insert_timings: StatementTimings,
    pub create_index_timings: StatementTimings,
    pub select_timings: StatementTimings,
    pub other_timings: StatementTimings,
}

impl TimingTracker {
    /// Create a new timing tracker with the given settings.
    pub fn new(enabled: bool, slow_query_threshold_ms: u64) -> Self {
        Self {
            enabled,
            slow_query_threshold_ms,
            insert_timings: StatementTimings::default(),
            create_index_timings: StatementTimings::default(),
            select_timings: StatementTimings::default(),
            other_timings: StatementTimings::default(),
        }
    }

    /// Record timing for a statement based on its type.
    pub fn record(&mut self, stmt_type: &str, duration: Duration, sql: &str) {
        if !self.enabled {
            return;
        }

        match stmt_type {
            "INSERT" => self.insert_timings.record(duration, sql),
            "CREATE_INDEX" => self.create_index_timings.record(duration, sql),
            "SELECT" => self.select_timings.record(duration, sql),
            _ => self.other_timings.record(duration, sql),
        }

        // Log slow queries
        let elapsed_ms = duration.as_millis() as u64;
        if elapsed_ms >= self.slow_query_threshold_ms {
            eprintln!(
                "[SLOW QUERY] {:.3}s - {} - {}",
                duration.as_secs_f64(),
                stmt_type,
                truncate_sql(sql, 100)
            );
        }
    }

    /// Print timing summary to stderr.
    pub fn print_summary(&self) {
        if !self.enabled {
            return;
        }

        let total_stmts = self.insert_timings.count
            + self.create_index_timings.count
            + self.select_timings.count
            + self.other_timings.count;

        if total_stmts == 0 {
            return;
        }

        eprintln!("\n📊 Statement Timing Summary:");
        eprintln!("  Total statements: {}", total_stmts);

        let total_time = self.insert_timings.total_duration
            + self.create_index_timings.total_duration
            + self.select_timings.total_duration
            + self.other_timings.total_duration;
        eprintln!("  Total time: {:.2}s", total_time.as_secs_f64());

        // INSERT statistics
        if self.insert_timings.count > 0 {
            self.print_category_stats("INSERT statements", &self.insert_timings);
        }

        // CREATE INDEX statistics
        if self.create_index_timings.count > 0 {
            self.print_category_stats("CREATE INDEX statements", &self.create_index_timings);
        }

        // SELECT statistics
        if self.select_timings.count > 0 {
            self.print_category_stats("SELECT queries", &self.select_timings);
        }

        // OTHER statistics
        if self.other_timings.count > 0 {
            self.print_category_stats("Other statements", &self.other_timings);
        }

        eprintln!();
    }

    fn print_category_stats(&self, category: &str, timings: &StatementTimings) {
        let avg_ms = timings.total_duration.as_millis() as f64 / timings.count as f64;
        eprintln!("\n  {}: {}", category, timings.count);
        eprintln!("    Total time: {:.2}s", timings.total_duration.as_secs_f64());
        eprintln!("    Average: {:.2}ms", avg_ms);
        eprintln!("    Slowest: {:.2}s", timings.max_duration.as_secs_f64());
        if let Some(ref sql) = timings.max_sql {
            eprintln!("      SQL: {}", sql);
        }
    }
}

/// Detect statement type from SQL for timing categorization.
pub fn detect_statement_type(sql: &str) -> &'static str {
    let sql_upper = sql.trim().to_uppercase();
    if sql_upper.starts_with("INSERT") {
        "INSERT"
    } else if sql_upper.starts_with("CREATE INDEX") || sql_upper.starts_with("CREATE UNIQUE INDEX") {
        "CREATE_INDEX"
    } else if sql_upper.starts_with("SELECT") {
        "SELECT"
    } else {
        "OTHER"
    }
}
