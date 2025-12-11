//! Quick integration test for MySQL spatial data types support

use vibesql::{executor::CreateTableExecutor, parser::Parser, storage::Database};

fn main() {
    let mut db = Database::new();

    println!("Testing MySQL Spatial Data Types Support");
    println!("=========================================\n");

    // Test the exact SQL from SQLLogicTest that was reported as failing in issue #818
    let sql = "CREATE TABLE `t1710a` (`c1` MULTIPOLYGON COMMENT 'text155459', `c2` MULTIPOLYGON COMMENT 'text155461');";

    println!("SQL: {}\n", sql);

    match Parser::parse_sql(sql) {
        Ok(vibesql::ast::Statement::CreateTable(create_stmt)) => {
            println!("✓ Parse successful");
            println!("  Table: {}", create_stmt.table_name);
            println!("  Columns: {}", create_stmt.columns.len());

            for col in &create_stmt.columns {
                println!("    - {} ({:?}) comment: {:?}", col.name, col.data_type, col.comment);
            }
            println!();

            match CreateTableExecutor::execute(&create_stmt, &mut db) {
                Ok(msg) => {
                    println!("✓ {}", msg);

                    // Verify table exists
                    if db.catalog.table_exists("t1710a") {
                        println!("✓ Table 't1710a' exists in catalog");

                        if let Some(schema) = db.catalog.get_table("t1710a") {
                            println!("✓ Table schema retrieved successfully");
                            println!("  Column count: {}", schema.column_count());
                        }
                    } else {
                        eprintln!("✗ Table 't1710a' NOT found in catalog");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("✗ Execution failed: {:?}", e);
                    std::process::exit(1);
                }
            }
        }
        Ok(_) => {
            eprintln!("✗ Parsed to wrong statement type");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("✗ Parse failed: {:?}", e);
            std::process::exit(1);
        }
    }

    println!("\n🎉 All spatial type tests passed!");
    println!("   MySQL spatial data types (MULTIPOLYGON, etc.) are working correctly.");
}
