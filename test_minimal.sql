-- Absolute minimal test for nested subquery bug
CREATE TABLE t1(c TEXT);
CREATE TABLE t2(x TEXT);
INSERT INTO t1 VALUES('a');
INSERT INTO t2 VALUES('a');

-- Level 0: t2, t1
-- Level 1: t1 (needs to reference x from level 0's t2)
SELECT x FROM t2, t1 WHERE c IN (
    SELECT c FROM t1 WHERE c = x
);
