-- Very simple 2-level nested subquery test
CREATE TABLE t1(c TEXT);
CREATE TABLE t2(x TEXT);
INSERT INTO t1 VALUES('a');
INSERT INTO t2 VALUES('a');

-- This should work - 2 levels, column 'x' from outer level
SELECT * FROM t1 WHERE c IN (
    SELECT c FROM t1 WHERE c = 'a'
);

-- This might fail - 3 levels, column 'x' from outer level
SELECT * FROM t2, t1 WHERE x IN (
    SELECT x FROM t1 WHERE x = (SELECT x FROM t2 WHERE x = 'a')
);
