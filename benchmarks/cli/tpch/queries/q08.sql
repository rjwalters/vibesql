-- TPC-H Q8: National Market Share (SQLite-compatible with strftime)
SELECT
    strftime('%Y', o_orderdate) as o_year,
    SUM(CASE WHEN n2.n_name = 'BRAZIL'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) as mkt_share
FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
WHERE p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate >= '1995-01-01'
    AND o_orderdate <= '1996-12-31'
    AND p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY strftime('%Y', o_orderdate)
ORDER BY o_year;
