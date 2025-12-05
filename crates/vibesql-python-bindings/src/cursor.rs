//! Query cursor implementation
//!
//! This module provides the Cursor class for executing SQL statements,
//! managing cached results, and fetching query results following DB-API 2.0 conventions.

use std::{num::NonZeroUsize, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;
use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use crate::{
    conversions::{convert_params_to_sql_values, sqlvalue_to_py, substitute_placeholders},
    profiling, OperationalError, ProgrammingError,
};

/// Query result storage
///
/// Represents the result of a SQL query execution, either a SELECT result
/// with rows and columns, or a DML/DDL result with rows affected count.
enum QueryResultData {
    /// SELECT query result with columns and rows
    Select { columns: Vec<String>, rows: Vec<vibesql_storage::Row> },
    /// DML/DDL result with rows affected and message
    Execute {
        rows_affected: usize,
        #[allow(dead_code)]
        message: String,
    },
}

/// Cursor object for executing SQL statements
///
/// Follows DB-API 2.0 conventions for query execution and result fetching.
/// Maintains caches for parsed statements and table schemas to improve performance.
///
/// # Caches
///
/// - **Statement Cache**: LRU cache of up to 1000 parsed SQL statements
/// - **Schema Cache**: LRU cache of up to 100 table schemas
/// - **Statistics**: Tracks hit/miss ratios for both caches
#[pyclass]
pub struct Cursor {
    db: Arc<Mutex<vibesql_storage::Database>>,
    last_result: Option<QueryResultData>,
    /// LRU cache for parsed SQL statements (max 1000 entries)
    /// Key: SQL string with ? placeholders, Value: parsed AST
    stmt_cache: Arc<Mutex<LruCache<String, vibesql_ast::Statement>>>,
    /// Cache for table schemas (max 100 tables per cursor)
    /// Key: table name, Value: cached schema
    schema_cache: Arc<Mutex<LruCache<String, vibesql_catalog::TableSchema>>>,
    /// Cache statistics for monitoring
    cache_hits: Arc<Mutex<usize>>,
    cache_misses: Arc<Mutex<usize>>,
    /// Schema cache statistics
    schema_cache_hits: Arc<Mutex<usize>>,
    schema_cache_misses: Arc<Mutex<usize>>,
}

impl Cursor {
    /// Create a new cursor for the given database
    pub(crate) fn new(db: Arc<Mutex<vibesql_storage::Database>>) -> PyResult<Self> {
        Ok(Cursor {
            db,
            last_result: None,
            stmt_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            schema_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap()))),
            cache_hits: Arc::new(Mutex::new(0)),
            cache_misses: Arc::new(Mutex::new(0)),
            schema_cache_hits: Arc::new(Mutex::new(0)),
            schema_cache_misses: Arc::new(Mutex::new(0)),
        })
    }
}

#[pymethods]
impl Cursor {
    /// Execute a SQL statement with optional parameter binding
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute (may contain ? placeholders)
    /// * `params` - Optional tuple of parameter values to bind to ? placeholders
    ///
    /// # Returns
    /// None
    ///
    /// # Errors
    /// Returns ProgrammingError for SQL parse errors or OperationalError for execution errors.
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &mut self,
        py: Python,
        sql: &str,
        params: Option<&Bound<'_, PyTuple>>,
    ) -> PyResult<()> {
        let mut profiler = profiling::DetailedProfiler::new("execute()");

        // Use the original SQL (with ? placeholders) as the cache key
        let cache_key = sql.to_string();
        profiler.checkpoint("SQL string copied");

        // Check if we have a cached parsed AST for this SQL
        let mut cache = self.stmt_cache.lock();
        profiler.checkpoint("Acquired cache lock");
        let stmt = if let Some(cached_stmt) = cache.get(&cache_key) {
            // Cache hit! Clone the cached AST before releasing lock
            let cloned_stmt = cached_stmt.clone();
            drop(cache); // Release cache lock before updating stats
            *self.cache_hits.lock() += 1;
            profiler.checkpoint("Cache HIT - stmt cloned");
            cloned_stmt
        } else {
            // Cache miss - need to parse
            drop(cache); // Release cache lock before parsing
            *self.cache_misses.lock() += 1;
            profiler.checkpoint("Cache MISS - need to parse");

            // Process SQL with parameter substitution if params are provided
            let processed_sql = if let Some(params_tuple) = params {
                Self::bind_parameters(py, sql, params_tuple)?
            } else {
                // No parameters provided, use SQL as-is
                sql.to_string()
            };

            // Parse the processed SQL
            let stmt = vibesql_parser::Parser::parse_sql(&processed_sql)
                .map_err(|e| ProgrammingError::new_err(format!("Parse error: {:?}", e)))?;
            profiler.checkpoint("SQL parsed to AST");

            // Store the parsed AST in cache for future reuse
            let mut cache = self.stmt_cache.lock();
            cache.put(cache_key.clone(), stmt.clone());
            drop(cache);
            profiler.checkpoint("AST cached");

            stmt
        };

        profiler.checkpoint("Statement ready for execution");

        // Execute based on statement type
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let db = self.db.lock();
                profiler.checkpoint("Database lock acquired (SELECT)");
                let select_executor = vibesql_executor::SelectExecutor::new(&db);
                profiler.checkpoint("SelectExecutor created");
                let result = select_executor
                    .execute_with_columns(&select_stmt)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;
                profiler.checkpoint("SELECT executed in Rust");

                self.last_result =
                    Some(QueryResultData::Select { columns: result.columns, rows: result.rows });
                profiler.checkpoint("Result stored");

                Ok(())
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                let mut db = self.db.lock();
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                // Clear both statement and schema caches on schema change
                let mut cache = self.stmt_cache.lock();
                cache.clear();
                drop(cache);
                self.clear_schema_cache();

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: 0,
                    message: format!("Table '{}' created successfully", create_stmt.table_name),
                });

                Ok(())
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                let mut db = self.db.lock();
                let message = vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                // Clear both statement and schema caches on schema change
                let mut cache = self.stmt_cache.lock();
                cache.clear();
                drop(cache);
                self.clear_schema_cache();

                self.last_result = Some(QueryResultData::Execute { rows_affected: 0, message });

                Ok(())
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                let mut db = self.db.lock();
                profiler.checkpoint("Database lock acquired (INSERT)");
                let row_count = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;
                profiler.checkpoint("INSERT executed in Rust");

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} inserted into '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        insert_stmt.table_name
                    ),
                });
                profiler.checkpoint("Result message created");

                Ok(())
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                // Use cached schema to reduce catalog lookups
                let cached_schema = self.get_cached_schema(&update_stmt.table_name)?;
                profiler.checkpoint("Schema cache lookup (UPDATE)");

                let mut db = self.db.lock();
                profiler.checkpoint("Database lock acquired (UPDATE)");
                let row_count = vibesql_executor::UpdateExecutor::execute_with_schema(
                    &update_stmt,
                    &mut db,
                    Some(&cached_schema),
                )
                .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;
                profiler.checkpoint("UPDATE executed in Rust");

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} updated in '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        update_stmt.table_name
                    ),
                });
                profiler.checkpoint("Result message created");

                Ok(())
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                let mut db = self.db.lock();
                profiler.checkpoint("Database lock acquired (DELETE)");
                let row_count = vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;
                profiler.checkpoint("DELETE executed in Rust");

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} deleted from '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        delete_stmt.table_name
                    ),
                });
                profiler.checkpoint("Result message created");

                Ok(())
            }
            vibesql_ast::Statement::CreateView(view_stmt) => {
                let mut db = self.db.lock();
                vibesql_executor::advanced_objects::execute_create_view(&view_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                // Clear both statement and schema caches on schema change
                let mut cache = self.stmt_cache.lock();
                cache.clear();
                drop(cache);
                self.clear_schema_cache();

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: 0,
                    message: format!("View '{}' created successfully", view_stmt.view_name),
                });

                Ok(())
            }
            vibesql_ast::Statement::DropView(drop_stmt) => {
                let mut db = self.db.lock();
                vibesql_executor::advanced_objects::execute_drop_view(&drop_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                // Clear both statement and schema caches on schema change
                let mut cache = self.stmt_cache.lock();
                cache.clear();
                drop(cache);
                self.clear_schema_cache();

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: 0,
                    message: format!("View '{}' dropped successfully", drop_stmt.view_name),
                });

                Ok(())
            }
            _ => Err(ProgrammingError::new_err(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    /// Fetch all rows from the last query result
    ///
    /// # Returns
    /// A list of tuples, each representing a row from the result set.
    ///
    /// # Errors
    /// Returns ProgrammingError if the last result was not a SELECT query.
    fn fetchall(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        let mut profiler = profiling::DetailedProfiler::new("fetchall()");
        match &self.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                profiler.checkpoint(&format!("Starting fetch of {} rows", rows.len()));
                let result_list = PyList::empty(py);
                profiler.checkpoint("Empty PyList created");
                for row in rows {
                    let py_values: Vec<Py<PyAny>> =
                        row.values.iter().map(|v| sqlvalue_to_py(py, v).unwrap()).collect();
                    let py_row = PyTuple::new(py, py_values)?;
                    result_list.append(py_row)?;
                }
                profiler.checkpoint("All rows converted to Python");
                Ok(result_list.into())
            }
            Some(QueryResultData::Execute { .. }) => {
                Err(ProgrammingError::new_err("No SELECT result to fetch"))
            }
            None => Err(ProgrammingError::new_err("No query has been executed")),
        }
    }

    /// Fetch one row from the last query result
    ///
    /// # Returns
    /// A tuple representing the next row, or None if no more rows are available.
    ///
    /// # Errors
    /// Returns ProgrammingError if the last result was not a SELECT query.
    fn fetchone(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        match &mut self.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                if rows.is_empty() {
                    Ok(py.None())
                } else {
                    let row = rows.remove(0);
                    let py_values: Vec<Py<PyAny>> =
                        row.values.iter().map(|v| sqlvalue_to_py(py, v).unwrap()).collect();
                    let py_row = PyTuple::new(py, py_values)?;
                    Ok(py_row.into())
                }
            }
            Some(QueryResultData::Execute { .. }) => {
                Err(ProgrammingError::new_err("No SELECT result to fetch"))
            }
            None => Err(ProgrammingError::new_err("No query has been executed")),
        }
    }

    /// Fetch multiple rows from the last query result
    ///
    /// # Arguments
    /// * `size` - Number of rows to fetch
    ///
    /// # Returns
    /// A list of tuples, each representing a row from the result set.
    ///
    /// # Errors
    /// Returns ProgrammingError if the last result was not a SELECT query.
    fn fetchmany(&mut self, py: Python, size: usize) -> PyResult<Py<PyAny>> {
        match &mut self.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                let fetch_count = size.min(rows.len());
                let result_list = PyList::empty(py);

                for _ in 0..fetch_count {
                    if rows.is_empty() {
                        break;
                    }
                    let row = rows.remove(0);
                    let py_values: Vec<Py<PyAny>> =
                        row.values.iter().map(|v| sqlvalue_to_py(py, v).unwrap()).collect();
                    let py_row = PyTuple::new(py, py_values)?;
                    result_list.append(py_row)?;
                }

                Ok(result_list.into())
            }
            Some(QueryResultData::Execute { .. }) => {
                Err(ProgrammingError::new_err("No SELECT result to fetch"))
            }
            None => Err(ProgrammingError::new_err("No query has been executed")),
        }
    }

    /// Get the number of rows affected by the last DML operation
    ///
    /// # Returns
    /// The number of rows affected by the last INSERT, UPDATE, or DELETE operation,
    /// the number of rows in the result set for a SELECT, or -1 if not applicable.
    #[getter]
    fn rowcount(&self) -> PyResult<i64> {
        match &self.last_result {
            Some(QueryResultData::Execute { rows_affected, .. }) => Ok(*rows_affected as i64),
            Some(QueryResultData::Select { rows, .. }) => Ok(rows.len() as i64),
            None => Ok(-1),
        }
    }

    /// Get description of the columns in the last SELECT result
    ///
    /// # Returns
    /// A sequence of 7-item sequences describing each column:
    /// (name, type_code, display_size, internal_size, precision, scale, null_ok)
    /// Returns None if the last result was not a SELECT query.
    #[getter]
    fn description(&self, py: Python) -> PyResult<Py<PyAny>> {
        match &self.last_result {
            Some(QueryResultData::Select { columns, .. }) => {
                let desc_list = PyList::empty(py);
                for col_name in columns {
                    // Each column is a 7-tuple: (name, type_code, None, None, None, None, None)
                    let col_str = col_name.as_str().into_pyobject(py)?.into_any().unbind();
                    let col_tuple = PyTuple::new(
                        py,
                        [
                            col_str,
                            py.None(), // type_code - we don't have detailed type info
                            py.None(), // display_size
                            py.None(), // internal_size
                            py.None(), // precision
                            py.None(), // scale
                            py.None(), // null_ok
                        ],
                    )?;
                    desc_list.append(col_tuple)?;
                }
                Ok(desc_list.into())
            }
            Some(QueryResultData::Execute { .. }) => Ok(py.None()),
            None => Ok(py.None()),
        }
    }

    /// Get schema cache statistics
    ///
    /// # Returns
    /// A tuple of (hits, misses, hit_rate) for the schema cache.
    fn schema_cache_stats(&self) -> PyResult<(usize, usize, f64)> {
        let hits = *self.schema_cache_hits.lock();
        let misses = *self.schema_cache_misses.lock();
        let total = hits + misses;
        let hit_rate = if total > 0 { (hits as f64) / (total as f64) } else { 0.0 };
        Ok((hits, misses, hit_rate))
    }

    /// Get statement cache statistics
    ///
    /// # Returns
    /// A tuple of (hits, misses, hit_rate) for the statement cache.
    fn cache_stats(&self) -> PyResult<(usize, usize, f64)> {
        let hits = *self.cache_hits.lock();
        let misses = *self.cache_misses.lock();
        let total = hits + misses;
        let hit_rate = if total > 0 { (hits as f64) / (total as f64) } else { 0.0 };
        Ok((hits, misses, hit_rate))
    }

    /// Clear both statement and schema caches
    ///
    /// Useful for testing or when schema changes occur outside this cursor.
    fn clear_cache(&mut self) -> PyResult<()> {
        let mut cache = self.stmt_cache.lock();
        cache.clear();
        drop(cache);

        self.clear_schema_cache();
        Ok(())
    }

    /// Close the cursor
    ///
    /// For now, this is a no-op but provided for DB-API 2.0 compatibility.
    fn close(&self) -> PyResult<()> {
        // No cleanup needed
        Ok(())
    }

    /// Execute a SQL statement multiple times with different parameter sets
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute (may contain ? placeholders)
    /// * `seq_of_params` - A sequence of parameter sequences (list of tuples)
    ///
    /// # Returns
    /// None
    ///
    /// # Errors
    /// Returns ProgrammingError for SQL parse errors or OperationalError for execution errors.
    fn executemany(
        &mut self,
        py: Python,
        sql: &str,
        seq_of_params: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let mut total_rows_affected = 0;

        for item in seq_of_params.iter() {
            let params_tuple = item.cast::<PyTuple>()?;
            // Bind parameters for this iteration
            let processed_sql = Self::bind_parameters(py, sql, params_tuple)?;

            // Parse the SQL
            let stmt = vibesql_parser::Parser::parse_sql(&processed_sql)
                .map_err(|e| ProgrammingError::new_err(format!("Parse error: {:?}", e)))?;

            // Execute the statement
            match stmt {
                vibesql_ast::Statement::Insert(insert_stmt) => {
                    let mut db = self.db.lock();
                    let row_count =
                        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).map_err(
                            |e| OperationalError::new_err(format!("Execution error: {:?}", e)),
                        )?;
                    total_rows_affected += row_count;
                }
                vibesql_ast::Statement::Update(update_stmt) => {
                    let mut db = self.db.lock();
                    let row_count =
                        vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut db).map_err(
                            |e| OperationalError::new_err(format!("Execution error: {:?}", e)),
                        )?;
                    total_rows_affected += row_count;
                }
                vibesql_ast::Statement::Delete(delete_stmt) => {
                    let mut db = self.db.lock();
                    let row_count =
                        vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut db).map_err(
                            |e| OperationalError::new_err(format!("Execution error: {:?}", e)),
                        )?;
                    total_rows_affected += row_count;
                }
                _ => {
                    return Err(ProgrammingError::new_err(
                        "executemany only supports INSERT, UPDATE, and DELETE statements"
                            .to_string(),
                    ))
                }
            }
        }

        // Set the final result to the total rows affected
        self.last_result = Some(QueryResultData::Execute {
            rows_affected: total_rows_affected,
            message: format!("{} rows affected", total_rows_affected),
        });

        Ok(())
    }
}

impl Cursor {
    /// Bind parameters to SQL statement
    ///
    /// Takes SQL with ? placeholders and a tuple of parameters, validates
    /// parameter count, and returns SQL with parameters substituted as literals.
    fn bind_parameters(py: Python, sql: &str, params: &Bound<'_, PyTuple>) -> PyResult<String> {
        // Count placeholders in SQL
        let placeholder_count = sql.matches('?').count();
        let param_count = params.len();

        // Validate parameter count matches placeholder count
        if placeholder_count != param_count {
            return Err(ProgrammingError::new_err(format!(
                "Parameter count mismatch: SQL has {} placeholders but {} parameters provided",
                placeholder_count, param_count
            )));
        }

        // Convert Python parameters to SQL values
        let sql_values = convert_params_to_sql_values(py, params)?;

        // Replace placeholders with SQL literal values
        Ok(substitute_placeholders(sql, &sql_values))
    }

    /// Get table schema with caching
    ///
    /// First checks the schema cache, and only queries the database catalog on cache miss.
    /// This reduces redundant catalog lookups during repeated operations on the same table.
    fn get_cached_schema(&self, table_name: &str) -> PyResult<vibesql_catalog::TableSchema> {
        // Check cache first
        let mut cache = self.schema_cache.lock();
        if let Some(schema) = cache.get(table_name) {
            // Cache hit! Clone and return
            let schema = schema.clone();
            drop(cache);
            *self.schema_cache_hits.lock() += 1;
            return Ok(schema);
        }
        drop(cache);

        // Cache miss - look up in database catalog
        *self.schema_cache_misses.lock() += 1;
        let db = self.db.lock();
        let schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| OperationalError::new_err(format!("Table not found: {}", table_name)))?
            .clone();
        drop(db);

        // Store in cache for future use
        let mut cache = self.schema_cache.lock();
        cache.put(table_name.to_string(), schema.clone());
        drop(cache);

        Ok(schema)
    }

    /// Invalidate schema cache for a specific table
    ///
    /// Call this after DDL operations that modify table schema.
    #[allow(dead_code)]
    fn invalidate_schema_cache(&self, table_name: &str) {
        let mut cache = self.schema_cache.lock();
        cache.pop(table_name);
    }

    /// Clear all schema caches
    ///
    /// Call this after any DDL operation that could affect multiple tables.
    fn clear_schema_cache(&self) {
        let mut cache = self.schema_cache.lock();
        cache.clear();
    }
}
