-- TPC-H Q9: Product Type Profit Measure (SQLite-compatible with strftime)
SELECT
    n_name as nation,
    strftime('%Y', o_orderdate) as o_year,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
GROUP BY n_name, strftime('%Y', o_orderdate)
ORDER BY nation, o_year DESC;
