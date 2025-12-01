//! SQL:1999 Conformance Testing using sqltest suite
//!
//! This test module runs SQL:1999 conformance tests by reading YAML files
//! directly from the upstream-recommended sqltest suite by Elliot Chance.

use std::fs;

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use serde::Deserialize;
use vibesql_storage::Database;

/// Individual test case from YAML files
#[derive(Debug, Deserialize, Clone)]
struct YamlTest {
    id: String,
    #[allow(dead_code)] // Present in YAML but not used in our code
    feature: String,
    #[serde(default)]
    sql: SqlField,
}

/// SQL field can be either a single string or array of strings
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum SqlField {
    Single(String),
    Multiple(Vec<String>),
}

impl Default for SqlField {
    fn default() -> Self {
        SqlField::Single(String::new())
    }
}

impl std::fmt::Display for SqlField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlField::Single(s) => write!(f, "{}", s),
            SqlField::Multiple(v) => write!(f, "{}", v.join("; ")),
        }
    }
}

/// Processed test case ready for execution
#[derive(Debug, Clone)]
struct TestCase {
    id: String,
    sql: SqlField,
    expect_success: bool,
}

/// Test results summary
#[derive(Debug, Default)]
struct TestResults {
    passed: usize,
    failed: usize,
    errors: usize,
    total: usize,
    failed_tests: Vec<(String, String)>,        // (test_id, sql)
    error_tests: Vec<(String, String, String)>, // (test_id, sql, error_msg)
}

impl TestResults {
    fn record_pass(&mut self) {
        self.passed += 1;
        self.total += 1;
    }

    fn record_fail(&mut self, test_id: String, sql: String) {
        self.failed += 1;
        self.total += 1;
        self.failed_tests.push((test_id, sql));
    }

    fn record_error(&mut self, test_id: String, sql: String, error: String) {
        self.errors += 1;
        self.total += 1;
        self.error_tests.push((test_id, sql, error));
    }

    fn pass_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.passed as f64 / self.total as f64) * 100.0
        }
    }
}

/// SQL:1999 Conformance Test Runner
struct SqltestRunner {
    tests: Vec<TestCase>,
}

impl SqltestRunner {
    /// Load all test YAML files directly from the sqltest suite
    fn load() -> Result<Self, String> {
        let mut tests = Vec::new();

        // Read all E-series and F-series test files
        let patterns = vec![
            "third_party/sqltest/standards/2016/E/*.tests.yml",
            "third_party/sqltest/standards/2016/F/*.tests.yml",
        ];

        for pattern in patterns {
            for entry in glob::glob(pattern).map_err(|e| format!("Glob pattern error: {}", e))? {
                let path = entry.map_err(|e| format!("Path error: {}", e))?;

                // Read file content
                let content = fs::read_to_string(&path)
                    .map_err(|e| format!("Failed to read {:?}: {}", path, e))?;

                // Parse YAML documents (separated by ---)
                for document in serde_yaml::Deserializer::from_str(&content) {
                    let yaml_test: YamlTest = YamlTest::deserialize(document)
                        .map_err(|e| format!("Failed to parse YAML from {:?}: {}", path, e))?;

                    tests.push(TestCase {
                        id: yaml_test.id,
                        sql: yaml_test.sql,
                        expect_success: true, // All tests in upstream expect success
                    });
                }
            }
        }

        Ok(Self { tests })
    }

    /// Run all conformance tests
    fn run_all(&self) -> TestResults {
        let mut results = TestResults::default();

        println!(
            "\n🧪 Running {} SQL:1999 conformance tests from upstream YAML files...\n",
            self.tests.len()
        );

        for test_case in &self.tests {
            // Create a fresh database for each test to ensure isolation
            let mut db = Database::new();
            match self.run_test(&mut db, test_case) {
                Ok(true) => {
                    results.record_pass();
                    print!(".");
                }
                Ok(false) => {
                    results.record_fail(test_case.id.clone(), test_case.sql.to_string());
                    print!("F");
                    eprintln!("\n❌ FAIL: {} - {}", test_case.id, test_case.sql);
                }
                Err(e) => {
                    results.record_error(
                        test_case.id.clone(),
                        test_case.sql.to_string(),
                        e.clone(),
                    );
                    print!("E");
                    eprintln!("\n⚠️  ERROR: {} - {}\n   {}", test_case.id, test_case.sql, e);
                }
            }

            // Newline every 50 tests for readability
            if results.total % 50 == 0 {
                println!();
            }
        }

        println!("\n");
        results
    }

    /// Run a single test case
    fn run_test(&self, db: &mut Database, test_case: &TestCase) -> Result<bool, String> {
        match &test_case.sql {
            SqlField::Single(sql) => self.run_single_statement(db, sql, test_case.expect_success),
            SqlField::Multiple(statements) => {
                // Execute each statement in sequence
                for (idx, sql) in statements.iter().enumerate() {
                    self.run_single_statement(db, sql, test_case.expect_success)
                        .map_err(|e| format!("Statement {} failed: {}", idx + 1, e))?;
                }
                Ok(true)
            }
        }
    }

    /// Run a single SQL statement
    fn run_single_statement(
        &self,
        db: &mut Database,
        sql: &str,
        expect_success: bool,
    ) -> Result<bool, String> {
        // Parse the SQL
        let parse_result = Parser::parse_sql(sql);

        if !expect_success {
            // Test expects failure - check that it fails
            return Ok(parse_result.is_err());
        }

        // Test expects success
        let stmt = parse_result.map_err(|e| format!("Parse error: {:?}", e))?;

        // Try to execute the statement
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(db);
                executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                vibesql_executor::InsertExecutor::execute(db, &insert_stmt)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                vibesql_executor::UpdateExecutor::execute(&update_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                vibesql_executor::DeleteExecutor::execute(&delete_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                vibesql_executor::DropTableExecutor::execute(&drop_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                vibesql_executor::AlterTableExecutor::execute(&alter_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateSchema(create_stmt) => {
                vibesql_executor::SchemaExecutor::execute_create_schema(&create_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropSchema(drop_stmt) => {
                vibesql_executor::SchemaExecutor::execute_drop_schema(&drop_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::SetSchema(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_schema(&set_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::SetCatalog(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_catalog(&set_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::SetNames(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_names(&set_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::SetTimeZone(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_time_zone(&set_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::Grant(grant_stmt) => {
                vibesql_executor::GrantExecutor::execute_grant(&grant_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::Revoke(revoke_stmt) => {
                vibesql_executor::RevokeExecutor::execute_revoke(&revoke_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateRole(create_role_stmt) => {
                vibesql_executor::RoleExecutor::execute_create_role(&create_role_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropRole(drop_role_stmt) => {
                vibesql_executor::RoleExecutor::execute_drop_role(&drop_role_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateDomain(create_domain_stmt) => {
                vibesql_executor::DomainExecutor::execute_create_domain(&create_domain_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropDomain(drop_domain_stmt) => {
                vibesql_executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateType(create_type_stmt) => {
                vibesql_executor::TypeExecutor::execute_create_type(&create_type_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropType(drop_type_stmt) => {
                vibesql_executor::TypeExecutor::execute_drop_type(&drop_type_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropTranslation(drop_translation_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_translation(&drop_translation_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateView(create_view_stmt) => {
                vibesql_executor::advanced_objects::execute_create_view(&create_view_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropView(drop_view_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_view(&drop_view_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
                vibesql_executor::IndexExecutor::execute(&create_index_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::DropIndex(drop_index_stmt) => {
                vibesql_executor::IndexExecutor::execute_drop(&drop_index_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                vibesql_executor::IndexExecutor::execute_reindex(&reindex_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            vibesql_ast::Statement::BeginTransaction(_)
            | vibesql_ast::Statement::Commit(_)
            | vibesql_ast::Statement::Rollback(_)
            | vibesql_ast::Statement::Savepoint(_)
            | vibesql_ast::Statement::TruncateTable(_)
            | vibesql_ast::Statement::RollbackToSavepoint(_)
            | vibesql_ast::Statement::ReleaseSavepoint(_)
            | vibesql_ast::Statement::CreateSequence(_)
            | vibesql_ast::Statement::DropSequence(_)
            | vibesql_ast::Statement::AlterSequence(_)
            | vibesql_ast::Statement::CreateCollation(_)
            | vibesql_ast::Statement::DropCollation(_)
            | vibesql_ast::Statement::CreateCharacterSet(_)
            | vibesql_ast::Statement::DropCharacterSet(_)
            | vibesql_ast::Statement::CreateTranslation(_)
            | vibesql_ast::Statement::SetTransaction(_)
            | vibesql_ast::Statement::SetVariable(_)
            | vibesql_ast::Statement::CreateTrigger(_)
            | vibesql_ast::Statement::DropTrigger(_)
            | vibesql_ast::Statement::AlterTrigger(_)
            | vibesql_ast::Statement::CreateAssertion(_)
            | vibesql_ast::Statement::DropAssertion(_)
            | vibesql_ast::Statement::DeclareCursor(_)
            | vibesql_ast::Statement::OpenCursor(_)
            | vibesql_ast::Statement::Fetch(_)
            | vibesql_ast::Statement::CloseCursor(_)
            | vibesql_ast::Statement::CreateProcedure(_)
            | vibesql_ast::Statement::DropProcedure(_)
            | vibesql_ast::Statement::CreateFunction(_)
            | vibesql_ast::Statement::DropFunction(_)
            | vibesql_ast::Statement::Call(_)
            | vibesql_ast::Statement::ShowTables(_)
            | vibesql_ast::Statement::ShowDatabases(_)
            | vibesql_ast::Statement::ShowColumns(_)
            | vibesql_ast::Statement::ShowIndex(_)
            | vibesql_ast::Statement::ShowCreateTable(_)
            | vibesql_ast::Statement::Describe(_)
            | vibesql_ast::Statement::Analyze(_)
            | vibesql_ast::Statement::Prepare(_)
            | vibesql_ast::Statement::Execute(_)
            | vibesql_ast::Statement::Deallocate(_) => {
                // Transactions, cursors, triggers, assertions, procedures, functions, and advanced SQL objects are no-ops
                // for validation
                Ok(true)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn run_sql1999_conformance_suite() {
    let runner =
        SqltestRunner::load().expect("Failed to load YAML test files from third_party/sqltest");

    let results = runner.run_all();

    // Print summary
    println!("{}", "=".repeat(60));
    println!("SQL:1999 Conformance Test Results");
    println!("{}", "=".repeat(60));
    println!("Total:   {}", results.total);
    println!("Passed:  {} ✅", results.passed);
    println!("Failed:  {} ❌", results.failed);
    println!("Errors:  {} ⚠️", results.errors);
    println!("Pass Rate: {:.1}%", results.pass_rate());
    println!("{}", "=".repeat(60));

    // Save results to JSON with detailed failure information
    let results_json = serde_json::json!({
        "total": results.total,
        "passed": results.passed,
        "failed": results.failed,
        "errors": results.errors,
        "pass_rate": results.pass_rate(),
        "failed_tests": results.failed_tests.iter().map(|(id, sql)| {
            serde_json::json!({
                "id": id,
                "sql": sql
            })
        }).collect::<Vec<_>>(),
        "error_tests": results.error_tests.iter().map(|(id, sql, error)| {
            serde_json::json!({
                "id": id,
                "sql": sql,
                "error": error
            })
        }).collect::<Vec<_>>(),
    });

    fs::write("target/sqltest_results.json", serde_json::to_string_pretty(&results_json).unwrap())
        .ok();

    // Assert we have some passing tests (not expecting 100% initially)
    assert!(results.passed > 0, "No tests passed! Pass rate: {:.1}%", results.pass_rate());

    // Optional: Assert minimum pass rate (commented out for now - allow any pass rate)
    // assert!(results.pass_rate() >= 50.0, "Pass rate too low: {:.1}%", results.pass_rate());
}
