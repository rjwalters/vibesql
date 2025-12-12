-- TPC-H Q7: Volume Shipping (SQLite-compatible with strftime)
SELECT
    n1.n_name as supp_nation,
    n2.n_name as cust_nation,
    strftime('%Y', l_shipdate) as l_year,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
         OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate >= '1995-01-01'
    AND l_shipdate <= '1996-12-31'
GROUP BY n1.n_name, n2.n_name, strftime('%Y', l_shipdate)
ORDER BY supp_nation, cust_nation, l_year;
