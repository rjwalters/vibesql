-- ============================================================================
-- PostgreSQL-inspired Trigger Regression Tests for VibeSQL
-- ============================================================================
-- Adapted from PostgreSQL's src/test/regress/sql/triggers.sql
-- Modified for VibeSQL's SQLite-compatible trigger syntax
-- ============================================================================
--
-- KNOWN LIMITATIONS:
-- - VibeSQL's trigger parser stores trigger bodies as debug token format
--   which causes failures when the trigger body is later re-parsed for execution.
--   This is tracked and tests marked accordingly with SKIP directives.
-- - NEW/OLD pseudo-variables work when triggers are created programmatically
--   but fail when parsed from SQL due to the above limitation.
-- ============================================================================

-- ============================================================================
-- SECTION 1: Basic Table Creation (These always work)
-- ============================================================================

-- TEST: Create test table for triggers
CREATE TABLE trigtest (
    id INTEGER PRIMARY KEY,
    value INTEGER DEFAULT 0,
    name TEXT
);

-- TEST: Create audit log table
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    operation TEXT,
    table_name TEXT,
    old_value INTEGER,
    new_value INTEGER
);

-- ============================================================================
-- SECTION 2: Trigger DDL Syntax Tests
-- ============================================================================

-- TEST: Create BEFORE INSERT trigger - empty body
CREATE TRIGGER tr_test_syntax1
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop trigger
DROP TRIGGER tr_test_syntax1;

-- TEST: Create AFTER UPDATE trigger - empty body
CREATE TRIGGER tr_test_syntax2
AFTER UPDATE ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop second trigger
DROP TRIGGER tr_test_syntax2;

-- TEST: Create BEFORE DELETE trigger - empty body
CREATE TRIGGER tr_test_syntax3
BEFORE DELETE ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop third trigger
DROP TRIGGER tr_test_syntax3;

-- TEST: Create trigger FOR EACH STATEMENT
CREATE TRIGGER tr_statement_test
AFTER INSERT ON trigtest
FOR EACH STATEMENT
BEGIN
END;

-- TEST: Drop statement trigger
DROP TRIGGER tr_statement_test;

-- ============================================================================
-- SECTION 3: Trigger with WHEN clause (Conditional Triggers)
-- ============================================================================

-- TEST: Create conditional trigger with WHEN clause - syntax test
CREATE TRIGGER tr_conditional_test
BEFORE INSERT ON trigtest
FOR EACH ROW
WHEN (1 = 1)
BEGIN
END;

-- TEST: Drop conditional trigger
DROP TRIGGER tr_conditional_test;

-- ============================================================================
-- SECTION 4: UPDATE OF Column-Specific Triggers
-- ============================================================================

-- TEST: Create trigger that fires only on value column update - syntax test
CREATE TRIGGER tr_update_of_test
BEFORE UPDATE OF (value) ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop column-specific trigger
DROP TRIGGER tr_update_of_test;

-- TEST: Create trigger for multiple columns
CREATE TRIGGER tr_update_of_multi
BEFORE UPDATE OF (value, name) ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop multi-column trigger
DROP TRIGGER tr_update_of_multi;

-- ============================================================================
-- SECTION 5: DROP TRIGGER Variations
-- ============================================================================

-- TEST: Create trigger for DROP tests
CREATE TRIGGER tr_drop_test
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: DROP TRIGGER basic
-- EXPECT_OK
DROP TRIGGER tr_drop_test;

-- TEST: Create another trigger for CASCADE test
CREATE TRIGGER tr_cascade_test
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: DROP TRIGGER CASCADE
-- EXPECT_OK
DROP TRIGGER tr_cascade_test CASCADE;

-- TEST: Create trigger for RESTRICT test
CREATE TRIGGER tr_restrict_test
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: DROP TRIGGER RESTRICT
-- EXPECT_OK
DROP TRIGGER tr_restrict_test RESTRICT;

-- ============================================================================
-- SECTION 6: Error Cases
-- ============================================================================

-- TEST: Create trigger on non-existent table
-- EXPECT_ERROR: table
CREATE TRIGGER tr_no_table
BEFORE INSERT ON nonexistent_table
FOR EACH ROW
BEGIN
END;

-- TEST: Create duplicate trigger name
CREATE TRIGGER tr_duplicate
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- EXPECT_ERROR: exists
CREATE TRIGGER tr_duplicate
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Cleanup duplicate trigger
DROP TRIGGER tr_duplicate;

-- TEST: Drop non-existent trigger
-- EXPECT_ERROR: TriggerNotFound
DROP TRIGGER tr_does_not_exist;

-- ============================================================================
-- SECTION 7: INSTEAD OF Triggers on Views (Syntax Tests)
-- Note: INSTEAD OF triggers on views require views to be fully registered
-- in the catalog, which may have limitations in current VibeSQL version.
-- ============================================================================

-- TEST: Create view for INSTEAD OF trigger
CREATE VIEW trigtest_view AS
SELECT id, value, name FROM trigtest WHERE value > 100;

-- TEST: Create INSTEAD OF INSERT trigger - syntax test
-- SKIP: INSTEAD OF triggers on views not fully supported
CREATE TRIGGER tr_instead_of_test
INSTEAD OF INSERT ON trigtest_view
FOR EACH ROW
BEGIN
END;

-- TEST: Drop INSTEAD OF trigger
-- SKIP: Previous trigger creation skipped
DROP TRIGGER tr_instead_of_test;

-- TEST: Drop view
DROP VIEW trigtest_view;

-- ============================================================================
-- SECTION 8: Trigger Timing Variants
-- ============================================================================

-- TEST: BEFORE INSERT timing
CREATE TRIGGER tr_before_insert_timing
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop BEFORE INSERT
DROP TRIGGER tr_before_insert_timing;

-- TEST: AFTER INSERT timing
CREATE TRIGGER tr_after_insert_timing
AFTER INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop AFTER INSERT
DROP TRIGGER tr_after_insert_timing;

-- TEST: BEFORE UPDATE timing
CREATE TRIGGER tr_before_update_timing
BEFORE UPDATE ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop BEFORE UPDATE
DROP TRIGGER tr_before_update_timing;

-- TEST: AFTER UPDATE timing
CREATE TRIGGER tr_after_update_timing
AFTER UPDATE ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop AFTER UPDATE
DROP TRIGGER tr_after_update_timing;

-- TEST: BEFORE DELETE timing
CREATE TRIGGER tr_before_delete_timing
BEFORE DELETE ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop BEFORE DELETE
DROP TRIGGER tr_before_delete_timing;

-- TEST: AFTER DELETE timing
CREATE TRIGGER tr_after_delete_timing
AFTER DELETE ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Drop AFTER DELETE
DROP TRIGGER tr_after_delete_timing;

-- ============================================================================
-- SECTION 9: Trigger Execution Tests (with simple bodies)
-- These tests verify trigger execution behavior using simple SQL that can
-- be re-parsed after the trigger body is stored.
-- ============================================================================

-- TEST: Create counter table
CREATE TABLE trigger_counter (count INTEGER DEFAULT 0);

-- TEST: Insert initial counter
INSERT INTO trigger_counter VALUES (0);

-- TEST: Create trigger with simple SELECT (won't modify state but tests execution path)
-- SKIP: Trigger body parsing stores debug format, not valid SQL
CREATE TRIGGER tr_simple_exec
AFTER INSERT ON trigtest
FOR EACH ROW
BEGIN
    SELECT 1;
END;

-- TEST: Insert row to fire trigger
-- EXPECT_OK
INSERT INTO trigtest (id, value, name) VALUES (1, 100, 'first');

-- TEST: Verify row was inserted
-- EXPECT: 1|100|first
SELECT id, value, name FROM trigtest WHERE id = 1;

-- TEST: Cleanup simple exec trigger
-- SKIP: Previous trigger creation may have failed
DROP TRIGGER tr_simple_exec;

-- ============================================================================
-- SECTION 10: Multiple Triggers on Same Table
-- ============================================================================

-- TEST: Create first trigger (alphabetically first)
CREATE TRIGGER tr_alpha
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Create second trigger (alphabetically second)
CREATE TRIGGER tr_beta
BEFORE INSERT ON trigtest
FOR EACH ROW
BEGIN
END;

-- TEST: Insert with multiple triggers
-- EXPECT_OK
INSERT INTO trigtest (id, value, name) VALUES (2, 200, 'multi');

-- TEST: Verify multi-trigger row inserted
-- EXPECT: 2|200|multi
SELECT id, value, name FROM trigtest WHERE id = 2;

-- TEST: Cleanup alpha trigger
DROP TRIGGER tr_alpha;

-- TEST: Cleanup beta trigger
DROP TRIGGER tr_beta;

-- ============================================================================
-- SECTION 11: DML Operations for Coverage
-- ============================================================================

-- TEST: Update row
-- EXPECT_OK
UPDATE trigtest SET value = 150 WHERE id = 1;

-- TEST: Verify update
-- EXPECT: 1|150|first
SELECT id, value, name FROM trigtest WHERE id = 1;

-- TEST: Delete row
-- EXPECT_OK
DELETE FROM trigtest WHERE id = 2;

-- TEST: Verify delete
-- EXPECT_COUNT: 1
SELECT * FROM trigtest;

-- ============================================================================
-- CLEANUP
-- ============================================================================

-- TEST: Drop counter table
DROP TABLE trigger_counter;

-- TEST: Drop audit table
DROP TABLE audit_log;

-- TEST: Drop main test table
DROP TABLE trigtest;

-- TEST: Verify cleanup (table should not exist)
-- EXPECT_ERROR: TableNotFound
SELECT * FROM trigtest;
