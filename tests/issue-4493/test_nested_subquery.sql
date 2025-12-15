-- Simplified test case for nested correlated subquery column resolution
-- Based on issue #4493

CREATE TABLE t1(c TEXT);
CREATE TABLE t2(x TEXT);

INSERT INTO t1 VALUES('a'), ('b'), ('c');
INSERT INTO t2 VALUES('a'), ('b'), ('c');

-- Simple case: 2 levels (this should work)
SELECT x FROM t2, t1 WHERE x IN (
    SELECT x FROM t2, t1 WHERE x = c
);

-- Complex case: 3 levels (this is likely to fail)
-- The innermost SELECT should be able to see columns from all outer levels
SELECT x FROM t2, t1 WHERE x IN (
    SELECT x FROM t2, t1 WHERE x IN (
        SELECT x FROM t1 WHERE x = c
    )
);
