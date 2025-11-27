//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! This crate supports multiple extensions beyond the original sqllogictest format.
//! See the [README](https://github.com/risinglightdb/sqllogictest-rs#slt-test-file-format-cookbook) for more information.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//!
//! # Usage
//!
//! For how to use the CLI tool backed by this library, see the [README](https://github.com/risinglightdb/sqllogictest-rs#use-the-cli-tool).
//!
//! For using the crate as a lib, and implement your custom driver, see below.
//!
//! Implement [`DB`] trait for your database structure:
//!
//! ```no_run
//! struct MyDatabase {
//!     // fields
//! }
//!
//! #[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
//! enum MyError {
//!     // variants
//! }
//!
//! impl sqllogictest::DB for MyDatabase {
//!     type Error = MyError;
//!     // Or define your own column type
//!     type ColumnType = sqllogictest::DefaultColumnType;
//!     fn run(
//!         &mut self,
//!         sql: &str,
//!     ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
//!         // TODO
//!         Ok(sqllogictest::DBOutput::StatementComplete(0))
//!     }
//! }
//!
//! // Then create a `Runner` on your database instance, and run the tests:
//! let mut tester = sqllogictest::Runner::new(|| async {
//!     let db = MyDatabase {
//!         // fields
//!     };
//!     Ok(db)
//! });
//! let _res = tester.run_file("../tests/slt/basic.slt");
//!
//! // You can also parse the script and execute the records separately:
//! let records = sqllogictest::parse_file("../tests/slt/basic.slt").unwrap();
//! for record in records {
//!     let _res = tester.run(record);
//! }
//! ```

pub mod column_type;
pub mod connection;
pub mod error_handling;
pub mod executor;
pub mod harness;
pub mod output;
pub mod parser;
pub mod result_updater;
pub mod runner;
pub mod substitution;

// Re-export parser sub-modules
pub use self::parser::location;
pub use self::parser::error_parser;
pub use self::parser::retry_parser;
pub use self::parser::directive_parser;
pub use self::parser::record_parser;

pub use self::column_type::*;
pub use self::connection::*;
pub use self::error_handling::*;
pub use self::executor::*;
pub use self::output::*;
pub use self::parser::*;
pub use self::result_updater::*;
