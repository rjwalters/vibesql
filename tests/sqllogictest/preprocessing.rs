//! Test file preprocessing for MySQL-specific directives.

/// Preprocess test file content to filter MySQL-specific directives
pub fn preprocess_for_mysql(content: &str) -> String {
    let mut output_lines = Vec::new();
    let mut skip_next_record = false;
    // Track if we've seen `onlyif mysql` for this record (separate from skip state)
    let mut seen_onlyif_mysql = false;
    // Track if we've seen ANY onlyif directive for this record
    let mut in_onlyif_block = false;

    for line in content.lines() {
        // Check for dialect directives
        if line.starts_with("onlyif ") {
            let dialect =
                line.trim_start_matches("onlyif ").split_whitespace().next().unwrap_or("");
            // Track that we're in an onlyif block
            in_onlyif_block = true;
            // For onlyif: run if ANY dialect matches mysql (OR logic for inclusion)
            if dialect == "mysql" {
                seen_onlyif_mysql = true;
            }
            // Don't set skip yet - wait until we've seen all consecutive onlyif directives
            continue; // Don't include the directive line
        } else if line.starts_with("skipif ") {
            let dialect =
                line.trim_start_matches("skipif ").split_whitespace().next().unwrap_or("");
            // Accumulate skip conditions with OR - if ANY skipif matches mysql, skip the record
            skip_next_record = skip_next_record || (dialect == "mysql");
            continue; // Don't include the directive line
        }

        // If we just finished processing onlyif directives, evaluate them
        if in_onlyif_block {
            // Skip if we saw onlyif directives but none matched mysql
            if !seen_onlyif_mysql {
                skip_next_record = true;
            }
            in_onlyif_block = false;
            seen_onlyif_mysql = false;
        }

        // If we're not skipping, include the line
        // The skip applies to the entire test record (until next blank line or new test)
        if skip_next_record {
            // Skip this line, but check if we've reached the end of the record
            if line.trim().is_empty() {
                skip_next_record = false;
                output_lines.push(line); // Include blank lines
            }
            // Continue skipping until blank line or new test starts (implicitly via next directive)
        } else {
            output_lines.push(line);
        }
    }

    output_lines.join("\n")
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_preprocess_onlyif_mysql() {
        let input = "statement ok\nCREATE TABLE t1 (x INT)\n\nonlyif mysql\nstatement ok\nINSERT INTO t1 VALUES (1)\n\nonlyif postgresql\nstatement ok\nINSERT INTO t1 VALUES (2)\n";
        let output = preprocess_for_mysql(input);

        // Should include MySQL-specific statement
        assert!(output.contains("INSERT INTO t1 VALUES (1)"));
        // Should exclude PostgreSQL-specific statement
        assert!(!output.contains("INSERT INTO t1 VALUES (2)"));
        // Should not include directive lines
        assert!(!output.contains("onlyif"));
    }

    #[test]
    fn test_preprocess_skipif_mysql() {
        let input = "statement ok\nCREATE TABLE t1 (x INT)\n\nskipif mysql\nstatement ok\nINSERT INTO t1 VALUES (1)\n\nskipif postgresql\nstatement ok\nINSERT INTO t1 VALUES (2)\n";
        let output = preprocess_for_mysql(input);

        // Should exclude MySQL-skipped statement
        assert!(!output.contains("INSERT INTO t1 VALUES (1)"));
        // Should include statement not skipped for MySQL
        assert!(output.contains("INSERT INTO t1 VALUES (2)"));
        // Should not include directive lines
        assert!(!output.contains("skipif"));
    }

    #[test]
    fn test_preprocess_directive_with_comment() {
        let input = "onlyif mysql # aggregate syntax:\nstatement ok\nSELECT SUM(x) FROM t1\n\nskipif mysql # unsupported feature\nstatement ok\nINSERT INTO t1 VALUES (99)\n";
        let output = preprocess_for_mysql(input);

        // MySQL directive with comment should include statement
        assert!(
            output.contains("SELECT SUM(x) FROM t1"),
            "onlyif mysql with comment should include MySQL statement"
        );
        // MySQL skipif with comment should exclude statement
        assert!(
            !output.contains("INSERT INTO t1 VALUES (99)"),
            "skipif mysql with comment should exclude MySQL statement"
        );
        // Directives should be removed
        assert!(!output.contains("onlyif"));
        assert!(!output.contains("skipif"));
    }

    #[test]
    fn test_preprocess_mixed_directives() {
        let input = r#"statement ok
CREATE TABLE t1 (x INT)

onlyif mysql
statement ok
INSERT INTO t1 VALUES (1)

skipif mysql
query I
SELECT * FROM t1 WHERE x > 10
----

onlyif postgresql
statement ok
INSERT INTO t1 VALUES (2)

statement ok
INSERT INTO t1 VALUES (3)
"#;
        let output = preprocess_for_mysql(input);

        // MySQL-only statement should be included
        assert!(output.contains("INSERT INTO t1 VALUES (1)"));
        // MySQL-skipped query should be excluded
        assert!(!output.contains("SELECT * FROM t1 WHERE x > 10"));
        // PostgreSQL-only statement should be excluded
        assert!(!output.contains("INSERT INTO t1 VALUES (2)"));
        // Universal statement should be included
        assert!(output.contains("INSERT INTO t1 VALUES (3)"));
        // No directives should remain
        assert!(!output.contains("onlyif"));
        assert!(!output.contains("skipif"));
    }

    #[test]
    fn test_preprocess_multiple_consecutive_skipif() {
        // This is the bug from issue #2632 - multiple consecutive skipif directives
        // should accumulate with OR logic, not overwrite each other
        let input = r#"skipif mysql # not compatible
skipif postgresql # PostgreSQL requires AS
query I rowsort label-1665
SELECT ( 41 ) / - 6 - - COUNT ( * ) col2 FROM tab0
----
4
"#;
        let output = preprocess_for_mysql(input);

        // The query should be EXCLUDED because `skipif mysql` matches
        // Even though `skipif postgresql` comes after, the mysql skip should still apply
        assert!(
            !output.contains("SELECT ( 41 )"),
            "Query with skipif mysql should be excluded, even with subsequent skipif postgresql"
        );
        assert!(!output.contains("skipif"), "Directives should be removed");
    }

    #[test]
    fn test_preprocess_multiple_consecutive_onlyif() {
        // Multiple onlyif should use OR logic - run if ANY matches mysql
        let input = r#"onlyif mysql # MySQL specific
onlyif sqlite # also works on sqlite
statement ok
INSERT INTO t1 VALUES (1)
"#;
        let output = preprocess_for_mysql(input);

        // Should INCLUDE because `onlyif mysql` matches
        assert!(
            output.contains("INSERT INTO t1 VALUES (1)"),
            "Statement with onlyif mysql should be included, even with other onlyif directives"
        );
        assert!(!output.contains("onlyif"), "Directives should be removed");
    }

    #[test]
    fn test_preprocess_multiple_onlyif_no_mysql() {
        // Multiple onlyif with no mysql match should skip
        let input = r#"onlyif postgresql
onlyif sqlite
statement ok
INSERT INTO t1 VALUES (1)
"#;
        let output = preprocess_for_mysql(input);

        // Should EXCLUDE because neither onlyif matches mysql
        assert!(
            !output.contains("INSERT INTO t1 VALUES (1)"),
            "Statement with onlyif postgresql+sqlite should be excluded for MySQL"
        );
    }
}
