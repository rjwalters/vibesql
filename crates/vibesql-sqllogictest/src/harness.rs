use std::path::Path;

pub use glob::glob;
pub use libtest_mimic::{run, Arguments, Failed, Trial};

use crate::{MakeConnection, Runner};

/// * `db_fn`: `fn() -> sqllogictest::AsyncDB`
/// * `pattern`: The glob used to match against and select each file to be tested. It is relative to
///   the root of the crate.
#[macro_export]
macro_rules! harness {
    ($db_fn:path, $pattern:expr) => {
        fn main() {
            let paths = $crate::harness::glob($pattern).expect("failed to find test files");
            let mut tests = vec![];

            for entry in paths {
                let path = entry.expect("failed to read glob entry");
                tests.push($crate::harness::Trial::test(
                    path.to_str().unwrap().to_string(),
                    move || $crate::harness::test(&path, || async { Ok($db_fn()) }),
                ));
            }

            if tests.is_empty() {
                panic!("no test found for sqllogictest under: {}", $pattern);
            }

            $crate::harness::run(&$crate::harness::Arguments::from_args(), tests).exit();
        }
    };
}

pub fn test(filename: impl AsRef<Path>, make_conn: impl MakeConnection) -> Result<(), Failed> {
    let mut tester = Runner::new(make_conn);
    // Add "mysql" label for skipif/onlyif directives
    // VibeSQL uses MySQL-compatible division (returns REAL/DECIMAL for integer division)
    tester.add_label("mysql");
    // Enable auto-switching of SQL dialect based on skipif/onlyif conditions.
    // This allows tests with `skipif mysql` to run in sqlite mode and vice versa,
    // maximizing test coverage instead of skipping tests.
    tester.enable_auto_switch_dialect();
    tester.run_file(filename)?;
    tester.shutdown();
    Ok(())
}

/// Test with auto-dialect switching disabled.
/// Use this when you want the original skip behavior based on labels.
pub fn test_no_auto_switch(filename: impl AsRef<Path>, make_conn: impl MakeConnection) -> Result<(), Failed> {
    let mut tester = Runner::new(make_conn);
    tester.add_label("mysql");
    // Auto-switch is disabled by default, but we explicitly note it here
    tester.run_file(filename)?;
    tester.shutdown();
    Ok(())
}
