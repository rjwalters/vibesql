#!/usr/bin/env python3
"""
MySQL TPC-H Benchmark Runner

Generates TPC-H data matching the VibeSQL/DuckDB benchmarks and runs
all 22 TPC-H queries against MySQL, outputting timing results.

Usage:
    python bench_mysql_tpch.py [--scale-factor 0.01] [--iterations 3]

Requirements:
    pip install mysql-connector-python
"""

import argparse
import json
import random
import string
import sys
import time
from datetime import datetime, timedelta
from typing import Any

try:
    import mysql.connector
except ImportError:
    print("Error: mysql-connector-python not installed")
    print("Run: pip install mysql-connector-python")
    sys.exit(1)


# =============================================================================
# TPC-H Reference Data (matches Rust implementation)
# =============================================================================

NATIONS = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
    ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
    ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1),
]

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIP_MODES = ["AIR", "AIR REG", "MAIL", "RAIL", "SHIP", "TRUCK", "FOB"]
COLORS = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"]
TYPES = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
CONTAINERS = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "MED PKG", "MED PACK", "LG CASE", "LG BOX", "LG PACK", "LG PKG", "JUMBO BOX", "JUMBO CASE", "JUMBO PACK", "JUMBO PKG", "WRAP CASE", "WRAP BOX", "WRAP PACK", "WRAP PKG"]


# =============================================================================
# TPC-H Queries (MySQL-compatible versions)
# =============================================================================

TPCH_QUERIES = {
    "Q1": """
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity) as avg_qty,
    AVG(l_extendedprice) as avg_price,
    AVG(l_discount) as avg_disc,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
""",

    "Q2": """
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT MIN(ps_supplycost)
        FROM partsupp, supplier, nation, region
        WHERE p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
    )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
""",

    "Q3": """
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
""",

    "Q4": """
SELECT
    o_orderpriority,
    COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
    AND o_orderdate < '1993-10-01'
    AND EXISTS (
        SELECT *
        FROM lineitem
        WHERE l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
""",

    "Q5": """
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
""",

    "Q6": """
SELECT
    SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
""",

    "Q7": """
SELECT
    n1.n_name as supp_nation,
    n2.n_name as cust_nation,
    YEAR(l_shipdate) as l_year,
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
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
""",

    "Q8": """
SELECT
    YEAR(o_orderdate) as o_year,
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
GROUP BY o_year
ORDER BY o_year
""",

    "Q9": """
SELECT
    n_name as nation,
    YEAR(o_orderdate) as o_year,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
""",

    "Q10": """
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-01-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
""",

    "Q11": """
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) as value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
    FROM partsupp, supplier, nation
    WHERE ps_suppkey = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'GERMANY'
)
ORDER BY value DESC
""",

    "Q12": """
SELECT
    l_shipmode,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        THEN 1 ELSE 0 END) as high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        THEN 1 ELSE 0 END) as low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= '1994-01-01'
    AND l_receiptdate < '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
""",

    "Q13": """
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
ORDER BY custdist DESC, c_count DESC
""",

    "Q14": """
SELECT
    100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
    AND l_shipdate >= '1995-09-01'
    AND l_shipdate < '1995-10-01'
""",

    "Q15": """
WITH revenue AS (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem
    WHERE l_shipdate >= '1996-01-01'
        AND l_shipdate < '1996-04-01'
    GROUP BY l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier, revenue
WHERE s_suppkey = supplier_no
    AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY s_suppkey
""",

    "Q16": """
SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT s_suppkey
        FROM supplier
        WHERE s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
""",

    "Q17": """
SELECT
    SUM(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * AVG(l_quantity)
        FROM lineitem
        WHERE l_partkey = p_partkey
    )
""",

    "Q18": """
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) as total_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
        SELECT l_orderkey
        FROM lineitem
        GROUP BY l_orderkey
        HAVING SUM(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
""",

    "Q19": """
SELECT
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem, part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
""",

    "Q20": """
SELECT
    s_name,
    s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey
    FROM partsupp
    WHERE ps_partkey IN (
        SELECT p_partkey
        FROM part
        WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > (
        SELECT 0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= '1994-01-01'
            AND l_shipdate < '1995-01-01'
    )
)
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY s_name
""",

    "Q21": """
SELECT
    s_name,
    COUNT(*) as numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT *
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
""",

    "Q22": """
SELECT
    SUBSTRING(c_phone, 1, 2) as cntrycode,
    COUNT(*) as numcust,
    SUM(c_acctbal) as totacctbal
FROM customer
WHERE SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > (
        SELECT AVG(c_acctbal)
        FROM customer
        WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    )
    AND NOT EXISTS (
        SELECT *
        FROM orders
        WHERE o_custkey = c_custkey
    )
GROUP BY cntrycode
ORDER BY cntrycode
""",
}


# =============================================================================
# Data Generator (matches Rust ChaCha8Rng with seed 42)
# =============================================================================

class TPCHDataGenerator:
    """Generates TPC-H data matching the Rust implementation's deterministic pattern."""

    def __init__(self, scale_factor: float):
        self.scale_factor = scale_factor
        self.customer_count = max(int(150_000 * scale_factor), 100)
        self.orders_count = max(int(1_500_000 * scale_factor), 1000)
        self.lineitem_count = max(int(6_000_000 * scale_factor), 4000)
        self.supplier_count = max(int(10_000 * scale_factor), 10)
        self.part_count = max(int(200_000 * scale_factor), 200)

        # Use the same seed as Rust (42)
        random.seed(42)

    def random_varchar(self, max_len: int) -> str:
        length = random.randint(10, max_len - 1)
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def random_phone(self, nation_key: int) -> str:
        return f"{10 + nation_key:02d}-{random.randint(100, 999):03d}-{random.randint(100, 999):03d}-{random.randint(1000, 9999):04d}"

    def random_date(self) -> str:
        year = random.randint(1992, 1998)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        return f"{year:04d}-{month:02d}-{day:02d}"

    def get_valid_supplier_for_part(self, part_key: int, supplier_idx: int) -> int:
        j = supplier_idx % 4
        return ((part_key + (j * (self.supplier_count // 4 + (part_key - 1) // self.supplier_count))) % self.supplier_count) + 1


def load_data(cursor, generator: TPCHDataGenerator, verbose: bool = True):
    """Load all TPC-H data into MySQL."""

    if verbose:
        print(f"Loading TPC-H data (SF={generator.scale_factor})...")
        print(f"  Customers: {generator.customer_count}")
        print(f"  Orders: {generator.orders_count}")
        print(f"  Lineitems: {generator.lineitem_count}")
        print(f"  Suppliers: {generator.supplier_count}")
        print(f"  Parts: {generator.part_count}")

    # Region
    if verbose:
        print("  Loading regions...")
    for i, name in enumerate(REGIONS):
        cursor.execute(
            "INSERT INTO region VALUES (%s, %s, %s)",
            (i, name, "comment")
        )

    # Nation
    if verbose:
        print("  Loading nations...")
    for i, (name, region_key) in enumerate(NATIONS):
        cursor.execute(
            "INSERT INTO nation VALUES (%s, %s, %s, %s)",
            (i, name, region_key, "comment")
        )

    # Customer
    if verbose:
        print("  Loading customers...")
    for i in range(generator.customer_count):
        nation_key = i % 25
        acctbal = (i * 17.3) % 10000.0 - 999.99
        cursor.execute(
            "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                i + 1,
                f"Customer#{i + 1:09d}",
                generator.random_varchar(40),
                nation_key,
                generator.random_phone(nation_key),
                round(acctbal, 2),
                SEGMENTS[i % len(SEGMENTS)],
                generator.random_varchar(117)
            )
        )

    # Supplier
    if verbose:
        print("  Loading suppliers...")
    for i in range(generator.supplier_count):
        nation_key = i % 25
        acctbal = (i * 13.7) % 10000.0 - 999.99
        cursor.execute(
            "INSERT INTO supplier VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                i + 1,
                f"Supplier#{i + 1:09d}",
                generator.random_varchar(40),
                nation_key,
                generator.random_phone(nation_key),
                round(acctbal, 2),
                generator.random_varchar(101)
            )
        )

    # Part
    if verbose:
        print("  Loading parts...")
    for i in range(generator.part_count):
        color1 = COLORS[i % len(COLORS)]
        color2 = COLORS[(i * 7) % len(COLORS)]
        p_name = f"{color1} {TYPES[i % len(TYPES)]} {color2}"
        retailprice = (90000.0 + (i / 10.0) % 10000.0) / 100.0
        cursor.execute(
            "INSERT INTO part VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                i + 1,
                p_name,
                f"Manufacturer#{(i % 5) + 1}",
                f"Brand#{(i % 5) + 1}{(i // 5 % 5) + 1}",
                TYPES[i % len(TYPES)],
                (i % 50) + 1,
                CONTAINERS[i % len(CONTAINERS)],
                round(retailprice, 2),
                generator.random_varchar(23)
            )
        )

    # Partsupp
    if verbose:
        print("  Loading partsupp...")
    for part_key in range(1, generator.part_count + 1):
        for j in range(4):
            supp_key = generator.get_valid_supplier_for_part(part_key, j)
            availqty = ((part_key * 17 + j * 31) % 9999) + 1
            supplycost = ((part_key * 13 + j * 7) % 100000) / 100.0 + 1.0
            cursor.execute(
                "INSERT INTO partsupp VALUES (%s, %s, %s, %s, %s)",
                (
                    part_key,
                    supp_key,
                    availqty,
                    round(supplycost, 2),
                    generator.random_varchar(199)
                )
            )

    # Orders
    if verbose:
        print("  Loading orders...")
    for i in range(generator.orders_count):
        cust_key = (i % generator.customer_count) + 1
        totalprice = (i * 271.3) % 500000.0 + 1000.0
        order_date = generator.random_date()
        cursor.execute(
            "INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                i + 1,
                cust_key,
                ["O", "F", "P"][i % 3],
                round(totalprice, 2),
                order_date,
                PRIORITIES[i % len(PRIORITIES)],
                f"Clerk#{(i * 7) % 1000 + 1:09d}",
                0,
                generator.random_varchar(79)
            )
        )

    # Lineitem
    if verbose:
        print("  Loading lineitems...")
    line_id = 0
    for order_num in range(1, generator.orders_count + 1):
        num_lines = (order_num * 3 % 7) + 1

        for line_num in range(1, num_lines + 1):
            if line_id >= generator.lineitem_count:
                break

            part_key = (line_id * 13) % generator.part_count + 1
            supp_key = generator.get_valid_supplier_for_part(part_key, line_id)

            quantity = ((line_id * 11) % 50 + 1)
            extendedprice = quantity * ((line_id * 97) % 100000 + 900)
            discount = ((line_id * 7) % 10) / 100.0
            tax = ((line_id * 3) % 8) / 100.0
            ship_date = generator.random_date()
            commit_date = generator.random_date()
            receipt_date = generator.random_date()

            cursor.execute(
                "INSERT INTO lineitem VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    order_num,
                    part_key,
                    supp_key,
                    line_num,
                    float(quantity),
                    float(extendedprice),
                    round(discount, 2),
                    round(tax, 2),
                    ["N", "R", "A"][line_id % 3],
                    ["O", "F"][line_id % 2],
                    ship_date,
                    commit_date,
                    receipt_date,
                    "DELIVER IN PERSON",
                    SHIP_MODES[line_id % len(SHIP_MODES)],
                    generator.random_varchar(44)
                )
            )

            line_id += 1

        if line_id >= generator.lineitem_count:
            break

    if verbose:
        print("  Data loading complete!")


def run_benchmarks(
    cursor,
    iterations: int = 3,
    timeout: int = 60,
    verbose: bool = True
) -> dict[str, Any]:
    """Run all TPC-H queries and return timing results."""

    results = {}

    for query_name, query_sql in TPCH_QUERIES.items():
        if verbose:
            print(f"Running {query_name}...", end=" ", flush=True)

        times = []
        error = None

        for _ in range(iterations):
            try:
                start = time.perf_counter()
                cursor.execute(query_sql)
                # Fetch all results to ensure complete execution
                _ = cursor.fetchall()
                elapsed = time.perf_counter() - start
                times.append(elapsed)
            except Exception as e:
                error = str(e)
                break

        if error:
            results[query_name] = {
                "success": False,
                "error": error,
                "time_ms": None
            }
            if verbose:
                print(f"ERROR: {error[:50]}...")
        else:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            results[query_name] = {
                "success": True,
                "time_ms": round(avg_time * 1000, 2),
                "min_ms": round(min_time * 1000, 2),
                "max_ms": round(max_time * 1000, 2),
                "runs": [round(t * 1000, 2) for t in times]
            }
            if verbose:
                print(f"{avg_time * 1000:.2f}ms (min: {min_time * 1000:.2f}, max: {max_time * 1000:.2f})")

    return results


def main():
    parser = argparse.ArgumentParser(description="MySQL TPC-H Benchmark")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument("--scale-factor", type=float, default=0.01, help="TPC-H scale factor")
    parser.add_argument("--iterations", type=int, default=3, help="Benchmark iterations per query")
    parser.add_argument("--skip-load", action="store_true", help="Skip data loading (use existing data)")
    parser.add_argument("--output", default=None, help="Output JSON file")
    parser.add_argument("--quiet", action="store_true", help="Quiet mode")

    args = parser.parse_args()
    verbose = not args.quiet

    # Connect to MySQL
    if verbose:
        print(f"Connecting to MySQL at {args.host}:{args.port}...")

    try:
        conn = mysql.connector.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            autocommit=True
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        print("\nMake sure MySQL is running. You can start it with:")
        print("  docker run -d --name mysql-tpch -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 3306:3306 mysql:8")
        sys.exit(1)

    cursor = conn.cursor()

    # Create schema
    if not args.skip_load:
        if verbose:
            print("Creating schema...")

        # Create database and tables
        cursor.execute("DROP DATABASE IF EXISTS tpch")
        cursor.execute("CREATE DATABASE tpch")
        cursor.execute("USE tpch")

        # Create all tables
        cursor.execute("""
            CREATE TABLE region (
                r_regionkey INTEGER PRIMARY KEY,
                r_name VARCHAR(25) NOT NULL,
                r_comment VARCHAR(152)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE nation (
                n_nationkey INTEGER PRIMARY KEY,
                n_name VARCHAR(25) NOT NULL,
                n_regionkey INTEGER NOT NULL,
                n_comment VARCHAR(152),
                INDEX idx_nation_regionkey (n_regionkey)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE customer (
                c_custkey INTEGER PRIMARY KEY,
                c_name VARCHAR(25) NOT NULL,
                c_address VARCHAR(40) NOT NULL,
                c_nationkey INTEGER NOT NULL,
                c_phone VARCHAR(15) NOT NULL,
                c_acctbal DECIMAL(15,2) NOT NULL,
                c_mktsegment VARCHAR(10) NOT NULL,
                c_comment VARCHAR(117),
                INDEX idx_customer_nationkey (c_nationkey)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE supplier (
                s_suppkey INTEGER PRIMARY KEY,
                s_name VARCHAR(25) NOT NULL,
                s_address VARCHAR(40) NOT NULL,
                s_nationkey INTEGER NOT NULL,
                s_phone VARCHAR(15) NOT NULL,
                s_acctbal DECIMAL(15,2) NOT NULL,
                s_comment VARCHAR(101),
                INDEX idx_supplier_nationkey (s_nationkey)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE part (
                p_partkey INTEGER PRIMARY KEY,
                p_name VARCHAR(55) NOT NULL,
                p_mfgr VARCHAR(25) NOT NULL,
                p_brand VARCHAR(10) NOT NULL,
                p_type VARCHAR(25) NOT NULL,
                p_size INTEGER NOT NULL,
                p_container VARCHAR(10) NOT NULL,
                p_retailprice DECIMAL(15,2) NOT NULL,
                p_comment VARCHAR(23)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE partsupp (
                ps_partkey INTEGER NOT NULL,
                ps_suppkey INTEGER NOT NULL,
                ps_availqty INTEGER NOT NULL,
                ps_supplycost DECIMAL(15,2) NOT NULL,
                ps_comment VARCHAR(199),
                PRIMARY KEY (ps_partkey, ps_suppkey),
                INDEX idx_partsupp_suppkey (ps_suppkey)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE orders (
                o_orderkey INTEGER PRIMARY KEY,
                o_custkey INTEGER NOT NULL,
                o_orderstatus VARCHAR(1) NOT NULL,
                o_totalprice DECIMAL(15,2) NOT NULL,
                o_orderdate DATE NOT NULL,
                o_orderpriority VARCHAR(15) NOT NULL,
                o_clerk VARCHAR(15) NOT NULL,
                o_shippriority INTEGER NOT NULL,
                o_comment VARCHAR(79),
                INDEX idx_orders_custkey (o_custkey),
                INDEX idx_orders_orderdate (o_orderdate)
            ) ENGINE=InnoDB
        """)

        cursor.execute("""
            CREATE TABLE lineitem (
                l_orderkey INTEGER NOT NULL,
                l_partkey INTEGER NOT NULL,
                l_suppkey INTEGER NOT NULL,
                l_linenumber INTEGER NOT NULL,
                l_quantity DECIMAL(15,2) NOT NULL,
                l_extendedprice DECIMAL(15,2) NOT NULL,
                l_discount DECIMAL(15,2) NOT NULL,
                l_tax DECIMAL(15,2) NOT NULL,
                l_returnflag VARCHAR(1) NOT NULL,
                l_linestatus VARCHAR(1) NOT NULL,
                l_shipdate DATE NOT NULL,
                l_commitdate DATE NOT NULL,
                l_receiptdate DATE NOT NULL,
                l_shipinstruct VARCHAR(25) NOT NULL,
                l_shipmode VARCHAR(10) NOT NULL,
                l_comment VARCHAR(44),
                PRIMARY KEY (l_orderkey, l_linenumber),
                INDEX idx_lineitem_partkey (l_partkey),
                INDEX idx_lineitem_suppkey (l_suppkey),
                INDEX idx_lineitem_shipdate (l_shipdate)
            ) ENGINE=InnoDB
        """)

        # Load data
        generator = TPCHDataGenerator(args.scale_factor)
        load_data(cursor, generator, verbose)
        conn.commit()
    else:
        cursor.execute("USE tpch")

    # Run benchmarks
    if verbose:
        print("\n" + "=" * 60)
        print("Running TPC-H benchmarks...")
        print("=" * 60)

    results = run_benchmarks(cursor, args.iterations, verbose=verbose)

    # Calculate summary
    successful = [r for r in results.values() if r["success"]]
    total_time = sum(r["time_ms"] for r in successful)
    avg_time = total_time / len(successful) if successful else 0

    output = {
        "timestamp": datetime.now().isoformat(),
        "database": "MySQL 8",
        "scale_factor": args.scale_factor,
        "iterations": args.iterations,
        "summary": {
            "total_queries": len(results),
            "successful": len(successful),
            "failed": len(results) - len(successful),
            "total_time_ms": round(total_time, 2),
            "avg_time_ms": round(avg_time, 2)
        },
        "queries": results
    }

    # Print summary
    if verbose:
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"Successful queries: {len(successful)}/{len(results)}")
        print(f"Total time: {total_time:.2f}ms")
        print(f"Average time: {avg_time:.2f}ms")

        # Print table for easy comparison
        print("\n| Query | MySQL (ms) |")
        print("|-------|------------|")
        for q in sorted(results.keys(), key=lambda x: int(x[1:])):
            r = results[q]
            if r["success"]:
                print(f"| {q} | {r['time_ms']:.2f} |")
            else:
                print(f"| {q} | ERROR |")

    # Output JSON
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        if verbose:
            print(f"\nResults saved to {args.output}")
    else:
        if not verbose:
            print(json.dumps(output, indent=2))

    cursor.close()
    conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
