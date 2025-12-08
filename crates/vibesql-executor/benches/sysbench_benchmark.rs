// The drop() calls below are deliberate to release borrowed connections before acquiring new ones
#![allow(dropping_copy_types, clippy::drop_non_drop)]

//! Sysbench OLTP Benchmark Runner
//!
//! WARNING: BENCHMARK INTEGRITY REQUIREMENT
//! =========================================
//! All database operations in this benchmark MUST go through the SQL execution path.
//! Direct API calls (e.g., Table::get_by_pk, Table::insert_row) that bypass SQL parsing
//! and planning would produce misleading benchmark results since they skip the query
//! processing overhead that real users experience.
//!
//! Acceptable patterns:
//! - SelectExecutor::new(db).execute(&stmt) - SQL execution
//! - UpdateExecutor::execute(&stmt, db) - SQL execution
//! - InsertExecutor::execute(db, &stmt) - SQL execution
//! - DeleteExecutor::execute(&stmt, db) - SQL execution
//! - session.execute_prepared() / session.execute() - SQL execution
//!
//! NOT acceptable in benchmark hot path:
//! - db.get_table().get_by_pk() - bypasses SQL
//! - table.insert_row() - bypasses SQL (OK for setup only)
//! - table.scan_all() - bypasses SQL
//!
//! A standalone benchmark runner for Sysbench OLTP workloads, comparing
//! VibeSQL against SQLite, DuckDB, and MySQL.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run (VibeSQL only)
//! cargo bench --package vibesql-executor --bench sysbench_benchmark --no-run
//! ./target/release/deps/sysbench_benchmark-*
//!
//! # With comparisons (SQLite, DuckDB, MySQL)
//! cargo bench --package vibesql-executor --bench sysbench_benchmark --features benchmark-comparison --no-run
//! ./target/release/deps/sysbench_benchmark-*
//!
//! # With MySQL comparison
//! MYSQL_URL=mysql://user:pass@localhost:3306/sysbench \
//! ./target/release/deps/sysbench_benchmark-*
//! ```
//!
//! ## Environment Variables
//!
//! - `SYSBENCH_TABLE_SIZE` - Number of rows (default: 10000)
//! - `SYSBENCH_DURATION_SECS` - Benchmark duration in seconds (default: 30)
//! - `SYSBENCH_WARMUP_SECS` - Warmup duration in seconds (default: 5)
//! - `MYSQL_URL` - MySQL connection string (optional, e.g., mysql://user:pass@localhost:3306/sysbench)
//!
//! ## Workload Types
//!
//! ```bash
//! ./target/release/deps/sysbench_benchmark-* point-select
//! ./target/release/deps/sysbench_benchmark-* insert
//! ./target/release/deps/sysbench_benchmark-* update-index
//! ./target/release/deps/sysbench_benchmark-* update-non-index
//! ./target/release/deps/sysbench_benchmark-* delete
//! ./target/release/deps/sysbench_benchmark-* range
//! ./target/release/deps/sysbench_benchmark-* all  # Run all workloads (default)
//! ```

mod sysbench;

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysbench::schema::load_vibesql;
use vibesql_ast::{Assignment, DeleteStmt, Expression, SelectStmt, UpdateStmt, WhereClause};
use vibesql_executor::{DeleteExecutor, InsertExecutor, SelectExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;
use vibesql_types::SqlValue;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::PooledConn;
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;

/// Default table size for sysbench tests
const DEFAULT_TABLE_SIZE: usize = 10_000;

/// Range size for range queries (sysbench default is 100)
const RANGE_SIZE: usize = 100;

// =============================================================================
// Parameter Binding Helpers
// =============================================================================

/// Bind values to placeholders in an expression
fn bind_expression(expr: &Expression, params: &[SqlValue]) -> Expression {
    match expr {
        Expression::Placeholder(idx) => {
            Expression::Literal(params.get(*idx).cloned().unwrap_or(SqlValue::Null))
        }
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: *op,
            left: Box::new(bind_expression(left, params)),
            right: Box::new(bind_expression(right, params)),
        },
        Expression::UnaryOp { op, expr } => {
            Expression::UnaryOp { op: *op, expr: Box::new(bind_expression(expr, params)) }
        }
        Expression::Between { expr, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(bind_expression(expr, params)),
            low: Box::new(bind_expression(low, params)),
            high: Box::new(bind_expression(high, params)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| bind_expression(a, params)).collect(),
            character_unit: character_unit.clone(),
        },
        Expression::AggregateFunction { name, distinct, args } => Expression::AggregateFunction {
            name: name.clone(),
            distinct: *distinct,
            args: args.iter().map(|a| bind_expression(a, params)).collect(),
        },
        // Pass through expressions that don't contain placeholders
        _ => expr.clone(),
    }
}

/// Bind values to placeholders in a WHERE clause
fn bind_where_clause(
    where_clause: &Option<WhereClause>,
    params: &[SqlValue],
) -> Option<WhereClause> {
    where_clause.as_ref().map(|wc| match wc {
        WhereClause::Condition(expr) => WhereClause::Condition(bind_expression(expr, params)),
        WhereClause::CurrentOf(cursor) => WhereClause::CurrentOf(cursor.clone()),
    })
}

/// Bind values to placeholders in a SelectStmt
fn bind_select(stmt: &SelectStmt, params: &[SqlValue]) -> SelectStmt {
    SelectStmt {
        with_clause: stmt.with_clause.clone(),
        distinct: stmt.distinct,
        select_list: stmt.select_list.clone(),
        into_table: stmt.into_table.clone(),
        into_variables: stmt.into_variables.clone(),
        from: stmt.from.clone(),
        where_clause: stmt.where_clause.as_ref().map(|e| bind_expression(e, params)),
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
        order_by: stmt.order_by.clone(),
        limit: stmt.limit,
        offset: stmt.offset,
        set_operation: stmt.set_operation.clone(),
    }
}

/// Bind values to placeholders in a DeleteStmt
fn bind_delete(stmt: &DeleteStmt, params: &[SqlValue]) -> DeleteStmt {
    DeleteStmt {
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        where_clause: bind_where_clause(&stmt.where_clause, params),
    }
}

/// Bind values to placeholders in an UpdateStmt
fn bind_update(stmt: &UpdateStmt, params: &[SqlValue]) -> UpdateStmt {
    UpdateStmt {
        table_name: stmt.table_name.clone(),
        assignments: stmt
            .assignments
            .iter()
            .map(|a| Assignment {
                column: a.column.clone(),
                value: bind_expression(&a.value, params),
            })
            .collect(),
        where_clause: bind_where_clause(&stmt.where_clause, params),
    }
}

// =============================================================================
// Pre-parsed Query Templates
// =============================================================================

/// Pre-parsed SQL query templates for sysbench operations
/// These are parsed once at setup time and reused with parameter binding
///
/// Note: INSERT is not included here because INSERT VALUES with placeholders
/// requires prepared statement infrastructure. For INSERT, we format the SQL
/// with actual values and parse per-execution (consistent with the short-term
/// approach in issue #3204).
struct PreparedQueries {
    point_select: SelectStmt,
    update_index: UpdateStmt,
    update_non_index: UpdateStmt,
    delete: DeleteStmt,
    simple_range: SelectStmt,
    sum_range: SelectStmt,
    order_range: SelectStmt,
    distinct_range: SelectStmt,
}

impl PreparedQueries {
    fn new() -> Self {
        // Parse query templates with ? placeholders
        // Note: Statement::Select wraps SelectStmt in a Box, so we dereference it
        let point_select = match Parser::parse_sql("SELECT c FROM sbtest1 WHERE id = ?") {
            Ok(vibesql_ast::Statement::Select(s)) => *s,
            _ => panic!("Failed to parse point_select template"),
        };

        let update_index = match Parser::parse_sql("UPDATE sbtest1 SET k = k + 1 WHERE id = ?") {
            Ok(vibesql_ast::Statement::Update(s)) => s,
            _ => panic!("Failed to parse update_index template"),
        };

        let update_non_index = match Parser::parse_sql("UPDATE sbtest1 SET c = ? WHERE id = ?") {
            Ok(vibesql_ast::Statement::Update(s)) => s,
            _ => panic!("Failed to parse update_non_index template"),
        };

        let delete = match Parser::parse_sql("DELETE FROM sbtest1 WHERE id = ?") {
            Ok(vibesql_ast::Statement::Delete(s)) => s,
            _ => panic!("Failed to parse delete template"),
        };

        let simple_range = match Parser::parse_sql("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?")
        {
            Ok(vibesql_ast::Statement::Select(s)) => *s,
            _ => panic!("Failed to parse simple_range template"),
        };

        let sum_range =
            match Parser::parse_sql("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?") {
                Ok(vibesql_ast::Statement::Select(s)) => *s,
                _ => panic!("Failed to parse sum_range template"),
            };

        let order_range =
            match Parser::parse_sql("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c") {
                Ok(vibesql_ast::Statement::Select(s)) => *s,
                _ => panic!("Failed to parse order_range template"),
            };

        let distinct_range = match Parser::parse_sql(
            "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
        ) {
            Ok(vibesql_ast::Statement::Select(s)) => *s,
            _ => panic!("Failed to parse distinct_range template"),
        };

        Self {
            point_select,
            update_index,
            update_non_index,
            delete,
            simple_range,
            sum_range,
            order_range,
            distinct_range,
        }
    }
}

/// Workload type enum
#[derive(Debug, Clone, Copy, PartialEq)]
enum WorkloadType {
    PointSelect,
    Insert,
    UpdateIndex,
    UpdateNonIndex,
    Delete,
    Range,
    All,
}

impl WorkloadType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "point-select" | "pointselect" | "ps" => Some(Self::PointSelect),
            "insert" | "ins" | "i" => Some(Self::Insert),
            "update-index" | "updateindex" | "ui" => Some(Self::UpdateIndex),
            "update-non-index" | "updatenonindex" | "uni" => Some(Self::UpdateNonIndex),
            "delete" | "del" | "d" => Some(Self::Delete),
            "range" | "r" => Some(Self::Range),
            "all" | "mix" | "mixed" => Some(Self::All),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::PointSelect => "Point Select",
            Self::Insert => "Insert",
            Self::UpdateIndex => "Update Index",
            Self::UpdateNonIndex => "Update Non-Index",
            Self::Delete => "Delete",
            Self::Range => "Range",
            Self::All => "All Workloads",
        }
    }
}

/// Benchmark results for a single workload
#[derive(Debug, Default, Clone)]
struct WorkloadResults {
    workload_name: String,
    client_id: Option<usize>, // None for single-client or aggregate results
    operations: u64,
    #[allow(dead_code)]
    total_time_us: u64,
    avg_latency_us: f64,
    ops_per_second: f64,
}

impl WorkloadResults {
    /// Aggregate results from multiple clients into a single summary
    fn aggregate(results: &[WorkloadResults], workload_name: &str) -> WorkloadResults {
        if results.is_empty() {
            return WorkloadResults {
                workload_name: workload_name.to_string(),
                ..Default::default()
            };
        }

        let total_ops: u64 = results.iter().map(|r| r.operations).sum();
        let total_time_us: u64 = results.iter().map(|r| r.total_time_us).sum();

        // Weighted average latency (weighted by operation count)
        let weighted_latency_sum: f64 =
            results.iter().map(|r| r.avg_latency_us * r.operations as f64).sum();
        let avg_latency_us =
            if total_ops > 0 { weighted_latency_sum / total_ops as f64 } else { 0.0 };

        // Total throughput is sum of individual throughputs
        let total_ops_per_second: f64 = results.iter().map(|r| r.ops_per_second).sum();

        WorkloadResults {
            workload_name: workload_name.to_string(),
            client_id: None, // Aggregate result
            operations: total_ops,
            total_time_us,
            avg_latency_us,
            ops_per_second: total_ops_per_second,
        }
    }
}

/// Generate a 120-char 'c' column value
fn generate_c_string(rng: &mut ChaCha8Rng) -> String {
    let mut s = String::with_capacity(120);
    for i in 0..11 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 10 {
            s.push('-');
        }
    }
    s
}

/// Generate a 60-char 'pad' column value
fn generate_pad_string(rng: &mut ChaCha8Rng) -> String {
    let mut s = String::with_capacity(60);
    for i in 0..5 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 4 {
            s.push('-');
        }
    }
    while s.len() < 60 {
        s.push(' ');
    }
    s
}

// =============================================================================
// VibeSQL Executor Trait and Implementation
// =============================================================================

trait SysbenchExecutor {
    fn point_select(&mut self, id: i64) -> usize;
    fn insert(&mut self, id: i64, k: i64, c: &str, pad: &str);
    fn update_index(&mut self, id: i64);
    fn update_non_index(&mut self, id: i64, c: &str);
    fn delete(&mut self, id: i64);
    fn simple_range(&mut self, start: i64, end: i64) -> usize;
    fn sum_range(&mut self, start: i64, end: i64) -> usize;
    fn order_range(&mut self, start: i64, end: i64) -> usize;
    fn distinct_range(&mut self, start: i64, end: i64) -> usize;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

/// VibeSQL executor using SQL path consistently with pre-parsed query templates
/// SQL is parsed once at setup time and reused with parameter binding for each execution.
/// This provides an apples-to-apples comparison with SQLite/DuckDB which use prepare_cached().
struct VibesqlExecutor<'a> {
    db: &'a mut VibeDB,
    queries: &'a PreparedQueries,
}

impl<'a> VibesqlExecutor<'a> {
    fn new(db: &'a mut VibeDB, queries: &'a PreparedQueries) -> Self {
        Self { db, queries }
    }
}

impl<'a> SysbenchExecutor for VibesqlExecutor<'a> {
    fn point_select(&mut self, id: i64) -> usize {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(id)];
        let bound = bind_select(&self.queries.point_select, &params);
        let executor = SelectExecutor::new(self.db);
        match executor.execute(&bound) {
            Ok(result) => result.len(),
            Err(_) => 0,
        }
    }

    fn insert(&mut self, id: i64, k: i64, c: &str, pad: &str) {
        // Use SQL path - format SQL with actual values and parse
        // Note: INSERT with placeholders requires prepared statement infrastructure
        // which is not yet available. This approach still uses the SQL executor path
        // (no direct API bypass) for benchmark fairness.
        let sql = format!(
            "INSERT INTO sbtest1 (id, k, c, padding) VALUES ({}, {}, '{}', '{}')",
            id, k, c, pad
        );
        if let Ok(vibesql_ast::Statement::Insert(insert)) = Parser::parse_sql(&sql) {
            let _ = InsertExecutor::execute(self.db, &insert);
        }
    }

    fn update_index(&mut self, id: i64) {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(id)];
        let bound = bind_update(&self.queries.update_index, &params);
        let _ = UpdateExecutor::execute(&bound, self.db);
    }

    fn update_non_index(&mut self, id: i64, c: &str) {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Varchar(arcstr::ArcStr::from(c)), SqlValue::Integer(id)];
        let bound = bind_update(&self.queries.update_non_index, &params);
        let _ = UpdateExecutor::execute(&bound, self.db);
    }

    fn delete(&mut self, id: i64) {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(id)];
        let bound = bind_delete(&self.queries.delete, &params);
        let _ = DeleteExecutor::execute(&bound, self.db);
    }

    fn simple_range(&mut self, start: i64, end: i64) -> usize {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(start), SqlValue::Integer(end)];
        let bound = bind_select(&self.queries.simple_range, &params);
        let executor = SelectExecutor::new(self.db);
        match executor.execute(&bound) {
            Ok(result) => result.len(),
            Err(_) => 0,
        }
    }

    fn sum_range(&mut self, start: i64, end: i64) -> usize {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(start), SqlValue::Integer(end)];
        let bound = bind_select(&self.queries.sum_range, &params);
        let executor = SelectExecutor::new(self.db);
        match executor.execute(&bound) {
            Ok(result) => result.len(),
            Err(_) => 0,
        }
    }

    fn order_range(&mut self, start: i64, end: i64) -> usize {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(start), SqlValue::Integer(end)];
        let bound = bind_select(&self.queries.order_range, &params);
        let executor = SelectExecutor::new(self.db);
        match executor.execute(&bound) {
            Ok(result) => result.len(),
            Err(_) => 0,
        }
    }

    fn distinct_range(&mut self, start: i64, end: i64) -> usize {
        // Use SQL path with pre-parsed template
        let params = [SqlValue::Integer(start), SqlValue::Integer(end)];
        let bound = bind_select(&self.queries.distinct_range, &params);
        let executor = SelectExecutor::new(self.db);
        match executor.execute(&bound) {
            Ok(result) => result.len(),
            Err(_) => 0,
        }
    }

    fn name(&self) -> &'static str {
        "VibeSQL"
    }
}

// =============================================================================
// SQLite Executor
// =============================================================================

#[cfg(feature = "sqlite-comparison")]
struct SqliteExecutor<'a> {
    conn: &'a SqliteConn,
}

#[cfg(feature = "sqlite-comparison")]
impl<'a> SqliteExecutor<'a> {
    fn new(conn: &'a SqliteConn) -> Self {
        Self { conn }
    }
}

#[cfg(feature = "sqlite-comparison")]
impl<'a> SysbenchExecutor for SqliteExecutor<'a> {
    fn point_select(&mut self, id: i64) -> usize {
        let mut stmt = self.conn.prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1").unwrap();
        let mut rows = stmt.query([id]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn insert(&mut self, id: i64, k: i64, c: &str, pad: &str) {
        let mut stmt = self
            .conn
            .prepare_cached("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        let _ = stmt.execute(rusqlite::params![id, k, c, pad]);
    }

    fn update_index(&mut self, id: i64) {
        let mut stmt =
            self.conn.prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1").unwrap();
        let _ = stmt.execute(rusqlite::params![id]);
    }

    fn update_non_index(&mut self, id: i64, c: &str) {
        let mut stmt = self.conn.prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2").unwrap();
        let _ = stmt.execute(rusqlite::params![c, id]);
    }

    fn delete(&mut self, id: i64) {
        let mut stmt = self.conn.prepare_cached("DELETE FROM sbtest1 WHERE id = ?1").unwrap();
        let _ = stmt.execute(rusqlite::params![id]);
    }

    fn simple_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt =
            self.conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn sum_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?")
            .unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn order_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
            .unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn distinct_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
            .unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn name(&self) -> &'static str {
        "SQLite"
    }
}

// =============================================================================
// DuckDB Executor
// =============================================================================

#[cfg(feature = "duckdb-comparison")]
struct DuckdbExecutor<'a> {
    conn: &'a DuckDBConn,
}

#[cfg(feature = "duckdb-comparison")]
impl<'a> DuckdbExecutor<'a> {
    fn new(conn: &'a DuckDBConn) -> Self {
        Self { conn }
    }
}

#[cfg(feature = "duckdb-comparison")]
impl<'a> SysbenchExecutor for DuckdbExecutor<'a> {
    fn point_select(&mut self, id: i64) -> usize {
        let mut stmt = self.conn.prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1").unwrap();
        let mut rows = stmt.query([id]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn insert(&mut self, id: i64, k: i64, c: &str, pad: &str) {
        let mut stmt = self
            .conn
            .prepare_cached("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        let _ = stmt.execute(duckdb::params![id, k, c, pad]);
    }

    fn update_index(&mut self, id: i64) {
        let mut stmt =
            self.conn.prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1").unwrap();
        let _ = stmt.execute(duckdb::params![id]);
    }

    fn update_non_index(&mut self, id: i64, c: &str) {
        let mut stmt = self.conn.prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2").unwrap();
        let _ = stmt.execute(duckdb::params![c, id]);
    }

    fn delete(&mut self, id: i64) {
        let mut stmt = self.conn.prepare_cached("DELETE FROM sbtest1 WHERE id = ?1").unwrap();
        let _ = stmt.execute(duckdb::params![id]);
    }

    fn simple_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt =
            self.conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn sum_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?")
            .unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn order_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
            .unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn distinct_range(&mut self, start: i64, end: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
            .unwrap();
        let mut rows = stmt.query([start, end]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    }

    fn name(&self) -> &'static str {
        "DuckDB"
    }
}

// =============================================================================
// MySQL Executor
// =============================================================================

#[cfg(feature = "mysql-comparison")]
struct MysqlExecutor<'a> {
    conn: &'a mut PooledConn,
}

#[cfg(feature = "mysql-comparison")]
impl<'a> MysqlExecutor<'a> {
    fn new(conn: &'a mut PooledConn) -> Self {
        Self { conn }
    }
}

#[cfg(feature = "mysql-comparison")]
impl<'a> SysbenchExecutor for MysqlExecutor<'a> {
    fn point_select(&mut self, id: i64) -> usize {
        let result: Vec<mysql::Row> =
            self.conn.exec("SELECT c FROM sbtest1 WHERE id = ?", (id,)).unwrap();
        result.len()
    }

    fn insert(&mut self, id: i64, k: i64, c: &str, pad: &str) {
        let _ = self.conn.exec_drop(
            "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)",
            (id, k, c, pad),
        );
    }

    fn update_index(&mut self, id: i64) {
        let _ = self.conn.exec_drop("UPDATE sbtest1 SET k = k + 1 WHERE id = ?", (id,));
    }

    fn update_non_index(&mut self, id: i64, c: &str) {
        let _ = self.conn.exec_drop("UPDATE sbtest1 SET c = ? WHERE id = ?", (c, id));
    }

    fn delete(&mut self, id: i64) {
        let _ = self.conn.exec_drop("DELETE FROM sbtest1 WHERE id = ?", (id,));
    }

    fn simple_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> =
            self.conn.exec("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?", (start, end)).unwrap();
        result.len()
    }

    fn sum_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?", (start, end))
            .unwrap();
        result.len()
    }

    fn order_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c", (start, end))
            .unwrap();
        result.len()
    }

    fn distinct_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec(
                "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
                (start, end),
            )
            .unwrap();
        result.len()
    }

    fn name(&self) -> &'static str {
        "MySQL"
    }
}

// =============================================================================
// Benchmark Runner
// =============================================================================

fn run_point_select_benchmark<E: SysbenchExecutor>(
    executor: &mut E,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
    client_id: Option<usize>,
) -> WorkloadResults {
    // Use client_id as seed modifier for different random sequences per client
    let seed = 42 + client_id.unwrap_or(0) as u64 * 1000;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let id = rng.random_range(1..=table_size as i64);
        let _ = executor.point_select(id);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);
        let op_start = Instant::now();
        let _ = executor.point_select(id);
        total_time_us += op_start.elapsed().as_micros() as u64;
        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Point Select".to_string(),
        client_id,
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

fn run_insert_benchmark<E: SysbenchExecutor>(
    executor: &mut E,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
    client_id: Option<usize>,
    client_count: usize,
) -> WorkloadResults {
    // Use client_id as seed modifier for different random sequences per client
    let seed = 42 + client_id.unwrap_or(0) as u64 * 1000;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // For multi-client insert, partition ID space to avoid conflicts
    // Client N inserts IDs starting at: table_size + 1 + N * large_offset
    let client_offset = client_id.unwrap_or(0) as i64;
    let id_spacing = 10_000_000i64; // Large spacing to avoid overlaps during benchmark
    let mut next_id = (table_size + 1) as i64 + client_offset * id_spacing;

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let k = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        let pad = generate_pad_string(&mut rng);
        executor.insert(next_id, k, &c, &pad);
        next_id += client_count as i64; // Interleave IDs across clients
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let k = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        let pad = generate_pad_string(&mut rng);

        let op_start = Instant::now();
        executor.insert(next_id, k, &c, &pad);
        total_time_us += op_start.elapsed().as_micros() as u64;

        next_id += client_count as i64; // Interleave IDs across clients
        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Insert".to_string(),
        client_id,
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

fn run_update_index_benchmark<E: SysbenchExecutor>(
    executor: &mut E,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
    client_id: Option<usize>,
    client_count: usize,
) -> WorkloadResults {
    // Use client_id as seed modifier for different random sequences per client
    let seed = 42 + client_id.unwrap_or(0) as u64 * 1000;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // For multi-client update, partition key space to minimize conflicts
    // Each client operates on a subset of IDs: client N handles IDs where id % client_count == N
    let client_offset = client_id.unwrap_or(0);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        // Generate an ID in this client's partition
        let base_id = rng.random_range(0..(table_size / client_count.max(1)) as i64);
        let id = base_id * client_count as i64 + client_offset as i64 + 1;
        let id = id.min(table_size as i64); // Ensure within bounds
        executor.update_index(id);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        // Generate an ID in this client's partition
        let base_id = rng.random_range(0..(table_size / client_count.max(1)) as i64);
        let id = base_id * client_count as i64 + client_offset as i64 + 1;
        let id = id.min(table_size as i64); // Ensure within bounds

        let op_start = Instant::now();
        executor.update_index(id);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Update Index".to_string(),
        client_id,
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

fn run_update_non_index_benchmark<E: SysbenchExecutor>(
    executor: &mut E,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
    client_id: Option<usize>,
    client_count: usize,
) -> WorkloadResults {
    // Use client_id as seed modifier for different random sequences per client
    let seed = 42 + client_id.unwrap_or(0) as u64 * 1000;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // For multi-client update, partition key space to minimize conflicts
    let client_offset = client_id.unwrap_or(0);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let base_id = rng.random_range(0..(table_size / client_count.max(1)) as i64);
        let id = base_id * client_count as i64 + client_offset as i64 + 1;
        let id = id.min(table_size as i64);
        let c = generate_c_string(&mut rng);
        executor.update_non_index(id, &c);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let base_id = rng.random_range(0..(table_size / client_count.max(1)) as i64);
        let id = base_id * client_count as i64 + client_offset as i64 + 1;
        let id = id.min(table_size as i64);
        let c = generate_c_string(&mut rng);

        let op_start = Instant::now();
        executor.update_non_index(id, &c);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Update Non-Index".to_string(),
        client_id,
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

fn run_delete_benchmark<E: SysbenchExecutor>(
    executor: &mut E,
    table_size: usize,
    duration: Duration,
    _warmup: Duration,
    client_id: Option<usize>,
    client_count: usize,
) -> WorkloadResults {
    // Use client_id as seed modifier for different random sequences per client
    let seed = 42 + client_id.unwrap_or(0) as u64 * 1000;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // For multi-client delete, partition available IDs across clients
    // Each client gets a disjoint subset: client N gets IDs where (id - 1) % client_count == N
    let client_offset = client_id.unwrap_or(0);
    let mut available_ids: Vec<i64> = (1..=table_size as i64)
        .filter(|&id| ((id - 1) as usize % client_count.max(1)) == client_offset)
        .collect();

    // Warmup - delete at most 1% of this client's rows to preserve most data for benchmark
    // Unlike other benchmarks, DELETE has no cache warmup benefit, and with fast
    // deletion rates (200K+ ops/sec), unlimited warmup can exhaust all rows.
    let warmup_limit = (available_ids.len() / 100).max(10); // At least 10 rows
    for _ in 0..warmup_limit {
        if available_ids.is_empty() {
            break;
        }
        let idx = rng.random_range(0..available_ids.len());
        let id = available_ids.swap_remove(idx);
        executor.delete(id);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration && !available_ids.is_empty() {
        let idx = rng.random_range(0..available_ids.len());
        let id = available_ids.swap_remove(idx);

        let op_start = Instant::now();
        executor.delete(id);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Delete".to_string(),
        client_id,
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

fn run_range_benchmark<E: SysbenchExecutor>(
    executor: &mut E,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
    client_id: Option<usize>,
) -> WorkloadResults {
    // Use client_id to seed the data generator differently per client
    let seed = 42 + client_id.unwrap_or(0) as u64 * 1000;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let profile_breakdown = std::env::var("RANGE_QUERY_BREAKDOWN").is_ok();

    // Helper to generate random range using our seeded RNG
    let random_range = |rng: &mut ChaCha8Rng| -> (i64, i64) {
        let effective_range = RANGE_SIZE.min(table_size);
        let max_start = table_size.saturating_sub(effective_range).saturating_add(1).max(1);
        let start = rng.random_range(1..=max_start as i64);
        (start, start + effective_range as i64 - 1)
    };

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let (start, end) = random_range(&mut rng);
        let _ = executor.simple_range(start, end);
        let _ = executor.sum_range(start, end);
        let _ = executor.order_range(start, end);
        let _ = executor.distinct_range(start, end);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let mut simple_time_us = 0u64;
    let mut sum_time_us = 0u64;
    let mut order_time_us = 0u64;
    let mut distinct_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let (start, end) = random_range(&mut rng);

        let op_start = Instant::now();

        let t0 = Instant::now();
        let _ = executor.simple_range(start, end);
        simple_time_us += t0.elapsed().as_micros() as u64;

        let t1 = Instant::now();
        let _ = executor.sum_range(start, end);
        sum_time_us += t1.elapsed().as_micros() as u64;

        let t2 = Instant::now();
        let _ = executor.order_range(start, end);
        order_time_us += t2.elapsed().as_micros() as u64;

        let t3 = Instant::now();
        let _ = executor.distinct_range(start, end);
        distinct_time_us += t3.elapsed().as_micros() as u64;

        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 4; // 4 range queries per iteration
    }

    // Print breakdown if requested (only for single client or client 0)
    if profile_breakdown && client_id.unwrap_or(0) == 0 {
        let query_count = operations / 4;
        eprintln!(
            "\n  {} Query Breakdown (avg per query, {} queries):",
            executor.name(),
            query_count
        );
        eprintln!(
            "    simple_range:   {:.2} us",
            simple_time_us as f64 / query_count as f64
        );
        eprintln!(
            "    sum_range:      {:.2} us",
            sum_time_us as f64 / query_count as f64
        );
        eprintln!(
            "    order_range:    {:.2} us",
            order_time_us as f64 / query_count as f64
        );
        eprintln!(
            "    distinct_range: {:.2} us",
            distinct_time_us as f64 / query_count as f64
        );
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Range Queries".to_string(),
        client_id,
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

fn print_results(results: &[WorkloadResults], db_name: &str) {
    eprintln!("\n--- {} Results ---", db_name);
    eprintln!(
        "{:<20} {:>8} {:>12} {:>15} {:>12}",
        "Workload", "Client", "Operations", "Avg Latency", "Ops/sec"
    );
    eprintln!("{:-<20} {:->8} {:->12} {:->15} {:->12}", "", "", "", "", "");

    for result in results {
        let client_str = match result.client_id {
            Some(id) => format!("{}", id),
            None => "all".to_string(),
        };
        eprintln!(
            "{:<20} {:>8} {:>12} {:>12.2} us {:>12.0}",
            result.workload_name, client_str, result.operations, result.avg_latency_us, result.ops_per_second
        );
    }
}

/// Print results with per-client breakdown and aggregate summary
fn print_parallel_results(
    client_results: &[WorkloadResults],
    aggregate: &WorkloadResults,
    db_name: &str,
    show_per_client: bool,
) {
    eprintln!("\n--- {} Results ({} clients) ---", db_name, client_results.len());

    if show_per_client && client_results.len() > 1 {
        eprintln!(
            "{:<20} {:>8} {:>12} {:>15} {:>12}",
            "Workload", "Client", "Operations", "Avg Latency", "Ops/sec"
        );
        eprintln!("{:-<20} {:->8} {:->12} {:->15} {:->12}", "", "", "", "", "");

        for result in client_results {
            let client_str = match result.client_id {
                Some(id) => format!("{}", id),
                None => "-".to_string(),
            };
            eprintln!(
                "{:<20} {:>8} {:>12} {:>12.2} us {:>12.0}",
                result.workload_name, client_str, result.operations, result.avg_latency_us, result.ops_per_second
            );
        }
        eprintln!("{:-<20} {:->8} {:->12} {:->15} {:->12}", "", "", "", "", "");
    }

    // Always print aggregate
    eprintln!(
        "{:<20} {:>8} {:>12} {:>12.2} us {:>12.0}",
        aggregate.workload_name, "TOTAL", aggregate.operations, aggregate.avg_latency_us, aggregate.ops_per_second
    );
}

fn print_comparison_summary(all_results: &[(&str, Vec<WorkloadResults>)], client_count: usize) {
    if client_count > 1 {
        eprintln!("\n\n=== Comparison Summary ({} clients) ===", client_count);
    } else {
        eprintln!("\n\n=== Comparison Summary ===");
    }

    // Get all workload names
    let workload_names: Vec<&str> = if let Some((_, results)) = all_results.first() {
        results.iter().map(|r| r.workload_name.as_str()).collect()
    } else {
        return;
    };

    for workload_name in workload_names {
        eprintln!("\n{}", workload_name);
        eprintln!(
            "{:<12} {:>12} {:>15} {:>12}",
            "Database", "Operations", "Avg Latency", "Ops/sec"
        );
        eprintln!("{:-<12} {:->12} {:->15} {:->12}", "", "", "", "");

        for (db_name, results) in all_results {
            if let Some(result) = results.iter().find(|r| r.workload_name == workload_name) {
                eprintln!(
                    "{:<12} {:>12} {:>12.2} us {:>12.0}",
                    db_name, result.operations, result.avg_latency_us, result.ops_per_second
                );
            }
        }
    }
}

fn main() {
    eprintln!("=== Sysbench OLTP Benchmark Runner ===");

    // Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [WORKLOAD_TYPE]", args[0]);
        eprintln!("\nWorkload Types:");
        eprintln!("  point-select    Run only point select queries");
        eprintln!("  insert          Run only insert operations");
        eprintln!("  update-index    Run only indexed column updates");
        eprintln!("  update-non-index Run only non-indexed column updates");
        eprintln!("  delete          Run only delete operations");
        eprintln!("  range           Run only range queries");
        eprintln!("  all             Run all workloads (default)");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  SYSBENCH_TABLE_SIZE    Number of rows (default: 10000)");
        eprintln!("  SYSBENCH_DURATION_SECS Benchmark duration in seconds (default: 30)");
        eprintln!("  SYSBENCH_WARMUP_SECS   Warmup duration in seconds (default: 5)");
        eprintln!("  SYSBENCH_CLIENTS       Number of parallel clients (default: 1, sequential)");
        eprintln!("  SYSBENCH_SHOW_CLIENTS  Show per-client results (default: false)");
        eprintln!("  MYSQL_URL              MySQL connection string (optional)");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run all workloads", args[0]);
        eprintln!("  {} point-select              # Run only point selects", args[0]);
        eprintln!("  SYSBENCH_TABLE_SIZE=50000 {}  # Run with 50k rows", args[0]);
        eprintln!("  SYSBENCH_CLIENTS=4 {}         # Run with 4 parallel clients", args[0]);
        eprintln!("  MYSQL_URL=mysql://user:pass@localhost/sysbench {}", args[0]);
        std::process::exit(0);
    }

    // Get configuration from environment
    let table_size: usize = env::var("SYSBENCH_TABLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TABLE_SIZE);

    let duration_secs: u64 =
        env::var("SYSBENCH_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);

    let warmup_secs: u64 =
        env::var("SYSBENCH_WARMUP_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);

    // Number of parallel clients (1 = sequential, >1 = parallel)
    let client_count: usize =
        env::var("SYSBENCH_CLIENTS").ok().and_then(|s| s.parse().ok()).unwrap_or(1).max(1);

    // Whether to show per-client results (useful for debugging, noisy for many clients)
    let show_per_client: bool = env::var("SYSBENCH_SHOW_CLIENTS").is_ok();

    let duration = Duration::from_secs(duration_secs);
    let warmup = Duration::from_secs(warmup_secs);

    // Parse workload type (skip benchmark harness arguments like --bench)
    let user_args: Vec<_> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();
    let workload_type = if !user_args.is_empty() {
        match WorkloadType::from_str(user_args[0]) {
            Some(t) => t,
            None => {
                eprintln!("Error: Unknown workload type '{}'. Run with --help for usage.", user_args[0]);
                std::process::exit(1);
            }
        }
    } else {
        WorkloadType::All
    };

    eprintln!("Configuration:");
    eprintln!("  Table size: {} rows", table_size);
    eprintln!("  Duration: {} seconds", duration_secs);
    eprintln!("  Warmup: {} seconds", warmup_secs);
    eprintln!("  Clients: {}", client_count);
    eprintln!("  Workload: {}", workload_type.name());

    // Collect all results for comparison summary
    let mut all_results: Vec<(&str, Vec<WorkloadResults>)> = Vec::new();

    // ========================================
    // VibeSQL Benchmark
    // ========================================

    // Pre-parse SQL query templates (parsed once, reused for each execution)
    // Arc-wrap for sharing across parallel clients
    let queries = Arc::new(PreparedQueries::new());

    eprintln!("\nLoading VibeSQL database ({} rows)...", table_size);
    let load_start = Instant::now();

    // Helper to run a single-client benchmark
    let run_single_client_benchmark = |workload: WorkloadType,
                                        queries: &PreparedQueries|
     -> WorkloadResults {
        let mut db = load_vibesql(table_size);
        let mut executor = VibesqlExecutor::new(&mut db, queries);
        match workload {
            WorkloadType::PointSelect => {
                run_point_select_benchmark(&mut executor, table_size, duration, warmup, None)
            }
            WorkloadType::Insert => {
                run_insert_benchmark(&mut executor, table_size, duration, warmup, None, 1)
            }
            WorkloadType::UpdateIndex => {
                run_update_index_benchmark(&mut executor, table_size, duration, warmup, None, 1)
            }
            WorkloadType::UpdateNonIndex => {
                run_update_non_index_benchmark(&mut executor, table_size, duration, warmup, None, 1)
            }
            WorkloadType::Delete => {
                run_delete_benchmark(&mut executor, table_size, duration, warmup, None, 1)
            }
            WorkloadType::Range => {
                run_range_benchmark(&mut executor, table_size, duration, warmup, None)
            }
            WorkloadType::All => unreachable!(),
        }
    };

    // Helper to run multi-client parallel benchmark
    #[cfg(feature = "parallel")]
    let run_parallel_benchmark = |workload: WorkloadType,
                                   queries: &Arc<PreparedQueries>,
                                   client_count: usize|
     -> (Vec<WorkloadResults>, WorkloadResults) {
        let client_results: Vec<WorkloadResults> = (0..client_count)
            .into_par_iter()
            .map(|client_id| {
                // Each client gets its own database instance to avoid contention
                let mut db = load_vibesql(table_size);
                let mut executor = VibesqlExecutor::new(&mut db, queries.as_ref());
                match workload {
                    WorkloadType::PointSelect => run_point_select_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                    ),
                    WorkloadType::Insert => run_insert_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::UpdateIndex => run_update_index_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::UpdateNonIndex => run_update_non_index_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::Delete => run_delete_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::Range => run_range_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                    ),
                    WorkloadType::All => unreachable!(),
                }
            })
            .collect();

        let aggregate = WorkloadResults::aggregate(&client_results, workload.name());
        (client_results, aggregate)
    };

    // Non-parallel fallback
    #[cfg(not(feature = "parallel"))]
    let run_parallel_benchmark = |workload: WorkloadType,
                                   queries: &Arc<PreparedQueries>,
                                   client_count: usize|
     -> (Vec<WorkloadResults>, WorkloadResults) {
        // Run sequentially when parallel feature is disabled
        let client_results: Vec<WorkloadResults> = (0..client_count)
            .map(|client_id| {
                let mut db = load_vibesql(table_size);
                let mut executor = VibesqlExecutor::new(&mut db, queries.as_ref());
                match workload {
                    WorkloadType::PointSelect => run_point_select_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                    ),
                    WorkloadType::Insert => run_insert_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::UpdateIndex => run_update_index_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::UpdateNonIndex => run_update_non_index_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::Delete => run_delete_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                        client_count,
                    ),
                    WorkloadType::Range => run_range_benchmark(
                        &mut executor,
                        table_size,
                        duration,
                        warmup,
                        Some(client_id),
                    ),
                    WorkloadType::All => unreachable!(),
                }
            })
            .collect();

        let aggregate = WorkloadResults::aggregate(&client_results, workload.name());
        (client_results, aggregate)
    };

    // Run benchmark for a specific workload type
    let run_workload = |workload: WorkloadType| -> Vec<WorkloadResults> {
        if client_count == 1 {
            // Single client: simple execution
            vec![run_single_client_benchmark(workload, &queries)]
        } else {
            // Multi-client: parallel execution
            let (client_results, aggregate) = run_parallel_benchmark(workload, &queries, client_count);
            print_parallel_results(&client_results, &aggregate, "VibeSQL", show_per_client);
            vec![aggregate]
        }
    };

    // Load a reference DB for timing
    let _reference_db = load_vibesql(table_size);
    eprintln!("VibeSQL loaded in {:?}", load_start.elapsed());

    let mut vibesql_results = Vec::new();

    match workload_type {
        WorkloadType::PointSelect => {
            vibesql_results.extend(run_workload(WorkloadType::PointSelect));
        }
        WorkloadType::Insert => {
            vibesql_results.extend(run_workload(WorkloadType::Insert));
        }
        WorkloadType::UpdateIndex => {
            vibesql_results.extend(run_workload(WorkloadType::UpdateIndex));
        }
        WorkloadType::UpdateNonIndex => {
            vibesql_results.extend(run_workload(WorkloadType::UpdateNonIndex));
        }
        WorkloadType::Delete => {
            vibesql_results.extend(run_workload(WorkloadType::Delete));
        }
        WorkloadType::Range => {
            vibesql_results.extend(run_workload(WorkloadType::Range));
        }
        WorkloadType::All => {
            // Run all workloads sequentially (each with parallel clients if configured)
            for wl in &[
                WorkloadType::PointSelect,
                WorkloadType::Insert,
                WorkloadType::UpdateIndex,
                WorkloadType::UpdateNonIndex,
                WorkloadType::Delete,
                WorkloadType::Range,
            ] {
                vibesql_results.extend(run_workload(*wl));
            }
        }
    }

    // For single client, print results normally
    if client_count == 1 {
        print_results(&vibesql_results, "VibeSQL");
    }
    all_results.push(("VibeSQL", vibesql_results));

    // ========================================
    // SQLite Comparison (if feature enabled)
    // ========================================
    #[cfg(feature = "sqlite-comparison")]
    {
        use sysbench::schema::load_sqlite;

        eprintln!("\n\nLoading SQLite database ({} rows)...", table_size);
        let sqlite_load_start = Instant::now();
        let _sqlite_ref = load_sqlite(table_size);
        eprintln!("SQLite loaded in {:?}", sqlite_load_start.elapsed());

        // Helper to run SQLite single-client benchmark
        let run_sqlite_single = |workload: WorkloadType| -> WorkloadResults {
            let conn = load_sqlite(table_size);
            let mut executor = SqliteExecutor::new(&conn);
            match workload {
                WorkloadType::PointSelect => {
                    run_point_select_benchmark(&mut executor, table_size, duration, warmup, None)
                }
                WorkloadType::Insert => {
                    run_insert_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::UpdateIndex => {
                    run_update_index_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::UpdateNonIndex => {
                    run_update_non_index_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::Delete => {
                    run_delete_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::Range => {
                    run_range_benchmark(&mut executor, table_size, duration, warmup, None)
                }
                WorkloadType::All => unreachable!(),
            }
        };

        // Helper to run SQLite multi-client parallel benchmark
        #[cfg(feature = "parallel")]
        let run_sqlite_parallel = |workload: WorkloadType, client_count: usize| -> (Vec<WorkloadResults>, WorkloadResults) {
            let client_results: Vec<WorkloadResults> = (0..client_count)
                .into_par_iter()
                .map(|client_id| {
                    let conn = load_sqlite(table_size);
                    let mut executor = SqliteExecutor::new(&conn);
                    match workload {
                        WorkloadType::PointSelect => run_point_select_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::Insert => run_insert_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateIndex => run_update_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateNonIndex => run_update_non_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Delete => run_delete_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Range => run_range_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::All => unreachable!(),
                    }
                })
                .collect();
            let aggregate = WorkloadResults::aggregate(&client_results, workload.name());
            (client_results, aggregate)
        };

        #[cfg(not(feature = "parallel"))]
        let run_sqlite_parallel = |workload: WorkloadType, client_count: usize| -> (Vec<WorkloadResults>, WorkloadResults) {
            let client_results: Vec<WorkloadResults> = (0..client_count)
                .map(|client_id| {
                    let conn = load_sqlite(table_size);
                    let mut executor = SqliteExecutor::new(&conn);
                    match workload {
                        WorkloadType::PointSelect => run_point_select_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::Insert => run_insert_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateIndex => run_update_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateNonIndex => run_update_non_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Delete => run_delete_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Range => run_range_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::All => unreachable!(),
                    }
                })
                .collect();
            let aggregate = WorkloadResults::aggregate(&client_results, workload.name());
            (client_results, aggregate)
        };

        let run_sqlite_workload = |workload: WorkloadType| -> Vec<WorkloadResults> {
            if client_count == 1 {
                vec![run_sqlite_single(workload)]
            } else {
                let (client_results, aggregate) = run_sqlite_parallel(workload, client_count);
                print_parallel_results(&client_results, &aggregate, "SQLite", show_per_client);
                vec![aggregate]
            }
        };

        let mut sqlite_results = Vec::new();
        match workload_type {
            WorkloadType::All => {
                for wl in &[
                    WorkloadType::PointSelect, WorkloadType::Insert, WorkloadType::UpdateIndex,
                    WorkloadType::UpdateNonIndex, WorkloadType::Delete, WorkloadType::Range,
                ] {
                    sqlite_results.extend(run_sqlite_workload(*wl));
                }
            }
            wl => sqlite_results.extend(run_sqlite_workload(wl)),
        }

        if client_count == 1 {
            print_results(&sqlite_results, "SQLite");
        }
        all_results.push(("SQLite", sqlite_results));
    }

    // ========================================
    // DuckDB Comparison (if feature enabled)
    // ========================================
    #[cfg(feature = "duckdb-comparison")]
    {
        use sysbench::schema::load_duckdb;

        eprintln!("\n\nLoading DuckDB database ({} rows)...", table_size);
        let duckdb_load_start = Instant::now();
        let _duckdb_ref = load_duckdb(table_size);
        eprintln!("DuckDB loaded in {:?}", duckdb_load_start.elapsed());

        // Helper to run DuckDB single-client benchmark
        let run_duckdb_single = |workload: WorkloadType| -> WorkloadResults {
            let conn = load_duckdb(table_size);
            let mut executor = DuckdbExecutor::new(&conn);
            match workload {
                WorkloadType::PointSelect => {
                    run_point_select_benchmark(&mut executor, table_size, duration, warmup, None)
                }
                WorkloadType::Insert => {
                    run_insert_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::UpdateIndex => {
                    run_update_index_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::UpdateNonIndex => {
                    run_update_non_index_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::Delete => {
                    run_delete_benchmark(&mut executor, table_size, duration, warmup, None, 1)
                }
                WorkloadType::Range => {
                    run_range_benchmark(&mut executor, table_size, duration, warmup, None)
                }
                WorkloadType::All => unreachable!(),
            }
        };

        // Helper to run DuckDB multi-client parallel benchmark
        #[cfg(feature = "parallel")]
        let run_duckdb_parallel = |workload: WorkloadType, client_count: usize| -> (Vec<WorkloadResults>, WorkloadResults) {
            let client_results: Vec<WorkloadResults> = (0..client_count)
                .into_par_iter()
                .map(|client_id| {
                    let conn = load_duckdb(table_size);
                    let mut executor = DuckdbExecutor::new(&conn);
                    match workload {
                        WorkloadType::PointSelect => run_point_select_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::Insert => run_insert_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateIndex => run_update_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateNonIndex => run_update_non_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Delete => run_delete_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Range => run_range_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::All => unreachable!(),
                    }
                })
                .collect();
            let aggregate = WorkloadResults::aggregate(&client_results, workload.name());
            (client_results, aggregate)
        };

        #[cfg(not(feature = "parallel"))]
        let run_duckdb_parallel = |workload: WorkloadType, client_count: usize| -> (Vec<WorkloadResults>, WorkloadResults) {
            let client_results: Vec<WorkloadResults> = (0..client_count)
                .map(|client_id| {
                    let conn = load_duckdb(table_size);
                    let mut executor = DuckdbExecutor::new(&conn);
                    match workload {
                        WorkloadType::PointSelect => run_point_select_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::Insert => run_insert_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateIndex => run_update_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::UpdateNonIndex => run_update_non_index_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Delete => run_delete_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id), client_count,
                        ),
                        WorkloadType::Range => run_range_benchmark(
                            &mut executor, table_size, duration, warmup, Some(client_id),
                        ),
                        WorkloadType::All => unreachable!(),
                    }
                })
                .collect();
            let aggregate = WorkloadResults::aggregate(&client_results, workload.name());
            (client_results, aggregate)
        };

        let run_duckdb_workload = |workload: WorkloadType| -> Vec<WorkloadResults> {
            if client_count == 1 {
                vec![run_duckdb_single(workload)]
            } else {
                let (client_results, aggregate) = run_duckdb_parallel(workload, client_count);
                print_parallel_results(&client_results, &aggregate, "DuckDB", show_per_client);
                vec![aggregate]
            }
        };

        let mut duckdb_results = Vec::new();
        match workload_type {
            WorkloadType::All => {
                for wl in &[
                    WorkloadType::PointSelect, WorkloadType::Insert, WorkloadType::UpdateIndex,
                    WorkloadType::UpdateNonIndex, WorkloadType::Delete, WorkloadType::Range,
                ] {
                    duckdb_results.extend(run_duckdb_workload(*wl));
                }
            }
            wl => duckdb_results.extend(run_duckdb_workload(wl)),
        }

        if client_count == 1 {
            print_results(&duckdb_results, "DuckDB");
        }
        all_results.push(("DuckDB", duckdb_results));
    }

    // ========================================
    // MySQL Comparison (if feature enabled)
    // ========================================
    // Note: MySQL runs single-client only (connection pooling would require different setup)
    #[cfg(feature = "mysql-comparison")]
    {
        use sysbench::schema::load_mysql;

        if let Some(mut mysql_conn) = load_mysql(table_size) {
            eprintln!("\n\nMySQL database loaded");

            // Warn when parallel mode is enabled but MySQL runs single-threaded
            if client_count > 1 {
                eprintln!("\n⚠️  WARNING: Parallel mode enabled (SYSBENCH_CLIENTS={})", client_count);
                eprintln!("⚠️  MySQL runs single-threaded - results not directly comparable");
                eprintln!("⚠️  MySQL would require connection pooling for parallel clients");
            }

            let mut mysql_results = Vec::new();
            {
                let mut executor = MysqlExecutor::new(&mut mysql_conn);

                match workload_type {
                    WorkloadType::PointSelect => {
                        mysql_results.push(run_point_select_benchmark(
                            &mut executor, table_size, duration, warmup, None,
                        ));
                    }
                    WorkloadType::Insert => {
                        mysql_results.push(run_insert_benchmark(
                            &mut executor, table_size, duration, warmup, None, 1,
                        ));
                    }
                    WorkloadType::UpdateIndex => {
                        mysql_results.push(run_update_index_benchmark(
                            &mut executor, table_size, duration, warmup, None, 1,
                        ));
                    }
                    WorkloadType::UpdateNonIndex => {
                        mysql_results.push(run_update_non_index_benchmark(
                            &mut executor, table_size, duration, warmup, None, 1,
                        ));
                    }
                    WorkloadType::Delete => {
                        mysql_results.push(run_delete_benchmark(
                            &mut executor, table_size, duration, warmup, None, 1,
                        ));
                    }
                    WorkloadType::Range => {
                        mysql_results.push(run_range_benchmark(
                            &mut executor, table_size, duration, warmup, None,
                        ));
                    }
                    WorkloadType::All => {
                        mysql_results.push(run_point_select_benchmark(
                            &mut executor, table_size, duration, warmup, None,
                        ));
                        drop(executor);

                        if let Some(mut conn2) = load_mysql(table_size) {
                            let mut executor2 = MysqlExecutor::new(&mut conn2);
                            mysql_results.push(run_insert_benchmark(
                                &mut executor2, table_size, duration, warmup, None, 1,
                            ));
                        }

                        if let Some(mut conn3) = load_mysql(table_size) {
                            let mut executor3 = MysqlExecutor::new(&mut conn3);
                            mysql_results.push(run_update_index_benchmark(
                                &mut executor3, table_size, duration, warmup, None, 1,
                            ));
                            mysql_results.push(run_update_non_index_benchmark(
                                &mut executor3, table_size, duration, warmup, None, 1,
                            ));
                        }

                        if let Some(mut conn4) = load_mysql(table_size) {
                            let mut executor4 = MysqlExecutor::new(&mut conn4);
                            mysql_results.push(run_delete_benchmark(
                                &mut executor4, table_size, duration, warmup, None, 1,
                            ));
                        }

                        if let Some(mut conn5) = load_mysql(table_size) {
                            let mut executor5 = MysqlExecutor::new(&mut conn5);
                            mysql_results.push(run_range_benchmark(
                                &mut executor5, table_size, duration, warmup, None,
                            ));
                        }
                    }
                }
            }
            print_results(&mysql_results, "MySQL");
            all_results.push(("MySQL", mysql_results));
        } else {
            eprintln!("\n\nSkipping MySQL benchmark - MYSQL_URL not set");
        }
    }

    // Print comparison summary
    if all_results.len() > 1 {
        print_comparison_summary(&all_results, client_count);
    }

    eprintln!("\n=== Done ===");
}
