//! Helper functions for WASM API tests

/// Helper to execute SQL (handles multi-statement SQL like loading database schemas)
pub fn execute_sql(db: &mut vibesql_storage::Database, sql: &str) -> Result<(), String> {
    // Split by semicolons to handle multiple statements
    for statement_text in sql.split(';') {
        // Remove comment lines and trim
        let cleaned: String = statement_text
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with("--"))
            .collect::<Vec<&str>>()
            .join("\n");

        let trimmed = cleaned.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Parse the SQL
        let stmt = vibesql_parser::Parser::parse_sql(trimmed).map_err(|e| {
            // Show first 100 chars of SQL for debugging
            let sql_preview = if trimmed.len() > 100 {
                format!("{}...", &trimmed[..100])
            } else {
                trimmed.to_string()
            };
            format!("Parse error in '{}': {:?}", sql_preview, e)
        })?;

        // Execute based on statement type
        match stmt {
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, db)
                    .map_err(|e| format!("CreateTable error: {:?}", e))?;
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                vibesql_executor::DropTableExecutor::execute(&drop_stmt, db)
                    .map_err(|e| format!("DropTable error: {:?}", e))?;
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                vibesql_executor::InsertExecutor::execute(db, &insert_stmt)
                    .map_err(|e| format!("Insert error: {:?}", e))?;
            }
            vibesql_ast::Statement::Select(select_stmt) => {
                let select_executor = vibesql_executor::SelectExecutor::new(db);
                select_executor
                    .execute(&select_stmt)
                    .map_err(|e| format!("Select error: {:?}", e))?;
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                vibesql_executor::UpdateExecutor::execute(&update_stmt, db)
                    .map_err(|e| format!("Update error: {:?}", e))?;
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                vibesql_executor::DeleteExecutor::execute(&delete_stmt, db)
                    .map_err(|e| format!("Delete error: {:?}", e))?;
            }
            vibesql_ast::Statement::BeginTransaction(begin_stmt) => {
                vibesql_executor::BeginTransactionExecutor::execute(&begin_stmt, db)
                    .map_err(|e| format!("Begin transaction error: {:?}", e))?;
            }
            vibesql_ast::Statement::Commit(commit_stmt) => {
                vibesql_executor::CommitExecutor::execute(&commit_stmt, db)
                    .map_err(|e| format!("Commit error: {:?}", e))?;
            }
            vibesql_ast::Statement::Rollback(rollback_stmt) => {
                vibesql_executor::RollbackExecutor::execute(&rollback_stmt, db)
                    .map_err(|e| format!("Rollback error: {:?}", e))?;
            }
            _ => return Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }
    Ok(())
}

/// Helper to create the appropriate database based on name
/// Returns (Database, Option<load_error>)
pub fn setup_example_database(db_name: &str) -> (vibesql_storage::Database, Option<String>) {
    match db_name {
        "northwind" => {
            let mut db = vibesql_storage::Database::new();
            let northwind_sql = include_str!("../../../../web-demo/examples/northwind.sql");
            match execute_sql(&mut db, northwind_sql) {
                Ok(_) => (db, None),
                Err(e) => {
                    (vibesql_storage::Database::new(), Some(format!("northwind DB load failed: {}", e)))
                }
            }
        }
        "employees" => {
            let mut db = vibesql_storage::Database::new();
            let employees_sql = include_str!("../../../../web-demo/examples/employees.sql");
            match execute_sql(&mut db, employees_sql) {
                Ok(_) => (db, None),
                Err(e) => {
                    (vibesql_storage::Database::new(), Some(format!("employees DB load failed: {}", e)))
                }
            }
        }
        // All other databases (empty, company, university, etc.) start as empty
        // Examples using these databases create their own tables
        _ => (vibesql_storage::Database::new(), None),
    }
}

/// Parse JSON example files to extract SQL queries for testing
/// Returns: Vec<(id, database, sql)>
pub fn parse_examples() -> Vec<(String, String, String)> {
    let mut all_examples = Vec::new();

    // Load all JSON example files
    let example_files = vec![
        ("basic", include_str!("../../../../web-demo/src/data/examples/basic.json")),
        ("joins", include_str!("../../../../web-demo/src/data/examples/joins.json")),
        ("subqueries", include_str!("../../../../web-demo/src/data/examples/subqueries.json")),
        ("aggregates", include_str!("../../../../web-demo/src/data/examples/aggregates.json")),
        ("window", include_str!("../../../../web-demo/src/data/examples/window.json")),
        ("datetime", include_str!("../../../../web-demo/src/data/examples/datetime.json")),
        ("string", include_str!("../../../../web-demo/src/data/examples/string.json")),
        ("math", include_str!("../../../../web-demo/src/data/examples/math.json")),
        ("case", include_str!("../../../../web-demo/src/data/examples/case.json")),
        (
            "null-handling",
            include_str!("../../../../web-demo/src/data/examples/null-handling.json"),
        ),
        ("set", include_str!("../../../../web-demo/src/data/examples/set.json")),
        ("recursive", include_str!("../../../../web-demo/src/data/examples/recursive.json")),
        ("ddl", include_str!("../../../../web-demo/src/data/examples/ddl.json")),
        ("dml", include_str!("../../../../web-demo/src/data/examples/dml.json")),
        ("patterns", include_str!("../../../../web-demo/src/data/examples/patterns.json")),
        ("data-quality", include_str!("../../../../web-demo/src/data/examples/data-quality.json")),
        (
            "performance-patterns",
            include_str!("../../../../web-demo/src/data/examples/performance-patterns.json"),
        ),
        (
            "business-intelligence",
            include_str!("../../../../web-demo/src/data/examples/business-intelligence.json"),
        ),
        (
            "report-templates",
            include_str!("../../../../web-demo/src/data/examples/report-templates.json"),
        ),
        (
            "advanced-multi-feature",
            include_str!("../../../../web-demo/src/data/examples/advanced-multi-feature.json"),
        ),
        (
            "sql1999-standards",
            include_str!("../../../../web-demo/src/data/examples/sql1999-standards.json"),
        ),
        ("company", include_str!("../../../../web-demo/src/data/examples/company.json")),
        ("university", include_str!("../../../../web-demo/src/data/examples/university.json")),
    ];

    for (_category_name, json_content) in example_files {
        if let Ok(examples) = parse_json_examples(json_content) {
            all_examples.extend(examples);
        }
    }

    all_examples
}

/// Parse a single JSON example file
#[allow(clippy::type_complexity)]
fn parse_json_examples(
    content: &str,
) -> Result<Vec<(String, String, String)>, Box<dyn std::error::Error>> {
    let mut examples = Vec::new();

    // Parse the JSON
    let json: serde_json::Value = serde_json::from_str(content)?;
    let root_obj = json.as_object().ok_or("JSON root must be an object")?;

    // Examples are nested under the "examples" key
    let examples_obj = root_obj
        .get("examples")
        .and_then(|v| v.as_object())
        .ok_or("JSON must have an 'examples' object")?;

    for (example_id, example_value) in examples_obj {
        let example_obj =
            example_value.as_object().ok_or(format!("Example {} must be an object", example_id))?;

        let sql = example_obj
            .get("sql")
            .and_then(|v| v.as_str())
            .ok_or(format!("Example {} missing sql field", example_id))?
            .to_string();

        let database = example_obj
            .get("database")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "northwind".to_string());

        // Clean SQL by removing expected results comments
        let cleaned_sql = extract_query(&sql);

        examples.push((example_id.clone(), database, cleaned_sql));
    }

    Ok(examples)
}

/// Extract just the SQL query without expected result comments
fn extract_query(sql: &str) -> String {
    sql.lines()
        .take_while(|line| !line.trim().contains("-- EXPECTED"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Check if an example uses known unsupported SQL features
pub fn uses_unsupported_features(sql: &str, example_id: &str) -> Option<&'static str> {
    let sql_upper = sql.to_uppercase();

    // DDL examples test CREATE TABLE with constraints (see issue #214)
    if example_id.starts_with("ddl-") {
        return Some("CREATE TABLE with constraints (PRIMARY KEY, UNIQUE, CHECK, etc.)");
    }

    // Check for known unsupported features
    if sql_upper.contains("WITH ") && (sql_upper.contains(" AS (") || sql_upper.contains(" AS(")) {
        return Some("Common Table Expressions (CTEs)");
    }
    if sql_upper.contains(" UNION ")
        || sql_upper.contains(" INTERSECT ")
        || sql_upper.contains(" EXCEPT ")
    {
        return Some("SET operations (UNION/INTERSECT/EXCEPT)");
    }
    if sql_upper.contains("ALTER TABLE") || sql_upper.contains("DROP INDEX") {
        return Some("DDL operations (ALTER TABLE/DROP INDEX)");
    }
    if sql.contains('|') && !sql.contains("'|'") {
        return Some("pipe character (possibly unsupported syntax)");
    }
    // Aggregate functions with GROUP BY or multi-column aggregates
    if (sql_upper.contains("COUNT(")
        || sql_upper.contains("AVG(")
        || sql_upper.contains("SUM(")
        || sql_upper.contains("MIN(")
        || sql_upper.contains("MAX("))
        && (sql_upper.contains("GROUP BY")
            || sql_upper.matches("COUNT(").count() > 1
            || sql_upper.contains("AVG(")
            || sql_upper.contains("SUM("))
    {
        return Some("Aggregate functions with GROUP BY or multiple aggregates");
    }
    // Subqueries
    if sql_upper.contains("SELECT") && sql_upper.matches("SELECT").count() > 1 {
        return Some("Subqueries");
    }
    // Recursive CTEs
    if sql_upper.contains("RECURSIVE") {
        return Some("Recursive CTEs");
    }
    if sql_upper.contains("EXTRACT(")
        || sql_upper.contains("DATE_PART(")
        || sql_upper.contains("YEAR(")
    {
        return Some("Date/time extraction functions");
    }

    None
}
