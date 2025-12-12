-- TPC-H Q13: Customer Distribution
SELECT
    c_count,
    COUNT(*) as custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey) as c_count
    FROM customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
