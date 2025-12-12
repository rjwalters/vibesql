#!/usr/bin/env python3
"""
TPC-H Data Generator for CLI Benchmarks

Generates TPC-H data as SQL INSERT statements with deterministic pseudo-random data
matching the Rust implementation for fair comparison.

Usage:
    python generate_data.py [--scale FACTOR] > data.sql

    FACTOR: Scale factor (default 0.01)
            0.01 = ~60K rows (lineitem)
            0.1  = ~600K rows
            1.0  = ~6M rows (standard TPC-H)
"""

import argparse
import random
import sys
from dataclasses import dataclass

# Reference data (matches Rust implementation)
NATIONS = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
    ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
    ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
]

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIP_MODES = ["AIR", "AIR REG", "MAIL", "RAIL", "SHIP", "TRUCK", "FOB"]

COLORS = [
    "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
    "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse",
    "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan",
    "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest",
    "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
    "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon",
    "light", "lime", "linen", "magenta", "maroon", "medium", "metallic",
    "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive",
    "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum",
    "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle",
    "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow",
    "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
    "wheat", "white", "yellow",
]

TYPES = [
    "STANDARD ANODIZED TIN", "STANDARD ANODIZED NICKEL", "STANDARD ANODIZED BRASS",
    "STANDARD ANODIZED STEEL", "STANDARD ANODIZED COPPER", "SMALL ANODIZED TIN",
    "SMALL ANODIZED NICKEL", "SMALL ANODIZED BRASS", "SMALL ANODIZED STEEL",
    "SMALL ANODIZED COPPER", "MEDIUM ANODIZED TIN", "MEDIUM ANODIZED NICKEL",
    "MEDIUM ANODIZED BRASS", "MEDIUM ANODIZED STEEL", "MEDIUM ANODIZED COPPER",
    "LARGE ANODIZED TIN", "LARGE ANODIZED NICKEL", "LARGE ANODIZED BRASS",
    "LARGE ANODIZED STEEL", "LARGE ANODIZED COPPER", "ECONOMY ANODIZED TIN",
    "ECONOMY ANODIZED NICKEL", "ECONOMY ANODIZED BRASS", "ECONOMY ANODIZED STEEL",
    "ECONOMY ANODIZED COPPER", "PROMO ANODIZED TIN", "PROMO ANODIZED NICKEL",
    "PROMO ANODIZED BRASS", "PROMO ANODIZED STEEL", "PROMO ANODIZED COPPER",
]

CONTAINERS = [
    "SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "MED PKG",
    "MED PACK", "LG CASE", "LG BOX", "LG PACK", "LG PKG", "JUMBO BOX",
    "JUMBO CASE", "JUMBO PACK", "JUMBO PKG", "WRAP CASE", "WRAP BOX",
    "WRAP PACK", "WRAP PKG",
]


@dataclass
class TPCHData:
    """TPC-H data generator with deterministic random data."""
    scale_factor: float
    customer_count: int
    orders_count: int
    lineitem_count: int
    supplier_count: int
    part_count: int
    rng: random.Random

    @classmethod
    def new(cls, scale_factor: float):
        """Create new data generator with specified scale factor."""
        customer_count = max(int(150_000 * scale_factor), 100)
        orders_count = max(int(1_500_000 * scale_factor), 1000)
        lineitem_count = max(int(6_000_000 * scale_factor), 4000)
        supplier_count = max(int(10_000 * scale_factor), 10)
        part_count = max(int(200_000 * scale_factor), 200)

        # Use fixed seed for deterministic data (same as Rust: 42)
        rng = random.Random(42)

        return cls(
            scale_factor=scale_factor,
            customer_count=customer_count,
            orders_count=orders_count,
            lineitem_count=lineitem_count,
            supplier_count=supplier_count,
            part_count=part_count,
            rng=rng,
        )

    def random_varchar(self, max_len: int) -> str:
        """Generate random alphanumeric string."""
        length = self.rng.randint(10, max_len - 1)
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        return ''.join(self.rng.choice(chars) for _ in range(length))

    def random_phone(self, nation_key: int) -> str:
        """Generate phone number for nation."""
        return f"{10 + nation_key:02d}-{self.rng.randint(100, 999)}-{self.rng.randint(100, 999)}-{self.rng.randint(1000, 9999)}"

    def random_date(self) -> str:
        """Generate random date between 1992-01-01 and 1998-12-31."""
        year = self.rng.randint(1992, 1998)
        month = self.rng.randint(1, 12)
        day = self.rng.randint(1, 28)  # Simplified
        return f"{year:04d}-{month:02d}-{day:02d}"


def escape_string(s: str) -> str:
    """Escape string for SQL."""
    return s.replace("'", "''")


def generate_region():
    """Generate REGION table data."""
    print("-- REGION table")
    for i, name in enumerate(REGIONS):
        print(f"INSERT INTO region VALUES ({i}, '{name}', 'comment');")


def generate_nation():
    """Generate NATION table data."""
    print("\n-- NATION table")
    for i, (name, region_key) in enumerate(NATIONS):
        print(f"INSERT INTO nation VALUES ({i}, '{name}', {region_key}, 'comment');")


def generate_customer(data: TPCHData):
    """Generate CUSTOMER table data."""
    print(f"\n-- CUSTOMER table ({data.customer_count} rows)")
    for i in range(data.customer_count):
        nation_key = i % 25
        # Use abs() to avoid negative literals (vibesql doesn't support them in INSERT)
        acctbal = abs((i * 17.3) % 10000.0 - 999.99)
        name = f"Customer#{i + 1:09d}"
        address = escape_string(data.random_varchar(40))
        phone = data.random_phone(nation_key)
        segment = SEGMENTS[i % len(SEGMENTS)]
        comment = escape_string(data.random_varchar(117))
        print(f"INSERT INTO customer VALUES ({i + 1}, '{name}', '{address}', {nation_key}, '{phone}', {acctbal:.2f}, '{segment}', '{comment}');")


def generate_supplier(data: TPCHData):
    """Generate SUPPLIER table data."""
    print(f"\n-- SUPPLIER table ({data.supplier_count} rows)")
    for i in range(data.supplier_count):
        nation_key = i % 25
        # Use abs() to avoid negative literals (vibesql doesn't support them in INSERT)
        acctbal = abs((i * 13.7) % 10000.0 - 999.99)
        name = f"Supplier#{i + 1:09d}"
        address = escape_string(data.random_varchar(40))
        phone = data.random_phone(nation_key)
        comment = escape_string(data.random_varchar(101))
        print(f"INSERT INTO supplier VALUES ({i + 1}, '{name}', '{address}', {nation_key}, '{phone}', {acctbal:.2f}, '{comment}');")


def generate_part(data: TPCHData):
    """Generate PART table data."""
    print(f"\n-- PART table ({data.part_count} rows)")
    for i in range(data.part_count):
        color1 = COLORS[i % len(COLORS)]
        color2 = COLORS[(i * 7) % len(COLORS)]
        p_name = f"{color1} {TYPES[i % len(TYPES)]} {color2}"
        mfgr = f"Manufacturer#{(i % 5) + 1}"
        brand = f"Brand#{(i % 5) + 1}{(i // 5 % 5) + 1}"
        p_type = TYPES[i % len(TYPES)]
        size = (i % 50) + 1
        container = CONTAINERS[i % len(CONTAINERS)]
        retailprice = (90000.0 + (i / 10.0) % 10000.0) / 100.0
        comment = escape_string(data.random_varchar(23))
        print(f"INSERT INTO part VALUES ({i + 1}, '{p_name}', '{mfgr}', '{brand}', '{p_type}', {size}, '{container}', {retailprice:.2f}, '{comment}');")


def get_valid_supplier_for_part(part_key: int, supplier_count: int, supplier_idx: int) -> int:
    """Get valid supplier key for a part (matches Rust implementation)."""
    j = supplier_idx % 4
    base = (part_key - 1) % supplier_count
    offset = (j * supplier_count) // 4
    return ((base + offset) % supplier_count) + 1


def generate_partsupp(data: TPCHData):
    """Generate PARTSUPP table data."""
    count = data.part_count * 4
    print(f"\n-- PARTSUPP table ({count} rows)")
    for part_key in range(1, data.part_count + 1):
        for j in range(4):
            supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, j)
            availqty = ((part_key * 17 + j * 31) % 9999) + 1
            supplycost = ((part_key * 13 + j * 7) % 100000) / 100.0 + 1.0
            comment = escape_string(data.random_varchar(199))
            print(f"INSERT INTO partsupp VALUES ({part_key}, {supp_key}, {availqty}, {supplycost:.2f}, '{comment}');")


def generate_orders(data: TPCHData):
    """Generate ORDERS table data."""
    print(f"\n-- ORDERS table ({data.orders_count} rows)")
    for i in range(data.orders_count):
        cust_key = (i % data.customer_count) + 1
        totalprice = (i * 271.3) % 500000.0 + 1000.0
        order_date = data.random_date()
        status = ["O", "F", "P"][i % 3]
        priority = PRIORITIES[i % len(PRIORITIES)]
        clerk = f"Clerk#{(i * 7) % 1000 + 1:09d}"
        comment = escape_string(data.random_varchar(79))
        print(f"INSERT INTO orders VALUES ({i + 1}, {cust_key}, '{status}', {totalprice:.2f}, '{order_date}', '{priority}', '{clerk}', 0, '{comment}');")


def generate_lineitem(data: TPCHData):
    """Generate LINEITEM table data."""
    print(f"\n-- LINEITEM table ({data.lineitem_count} rows)")
    line_id = 0
    for order_num in range(1, data.orders_count + 1):
        num_lines = (order_num * 3 % 7) + 1
        for line_num in range(1, num_lines + 1):
            if line_id >= data.lineitem_count:
                return

            part_key = (line_id * 13) % data.part_count + 1
            supp_key = get_valid_supplier_for_part(part_key, data.supplier_count, line_id)

            quantity = (line_id * 11) % 50 + 1
            extendedprice = quantity * ((line_id * 97) % 100000 + 900.0)
            discount = ((line_id * 7) % 10) / 100.0
            tax = ((line_id * 3) % 8) / 100.0
            ship_date = data.random_date()
            commit_date = data.random_date()
            receipt_date = data.random_date()
            returnflag = ["N", "R", "A"][line_id % 3]
            linestatus = ["O", "F"][line_id % 2]
            shipmode = SHIP_MODES[line_id % len(SHIP_MODES)]
            comment = escape_string(data.random_varchar(44))

            print(f"INSERT INTO lineitem VALUES ({order_num}, {part_key}, {supp_key}, {line_num}, {quantity:.2f}, {extendedprice:.2f}, {discount:.2f}, {tax:.2f}, '{returnflag}', '{linestatus}', '{ship_date}', '{commit_date}', '{receipt_date}', 'DELIVER IN PERSON', '{shipmode}', '{comment}');")
            line_id += 1


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H data as SQL INSERT statements")
    parser.add_argument("--scale", type=float, default=0.01, help="Scale factor (default: 0.01)")
    args = parser.parse_args()

    data = TPCHData.new(args.scale)

    print("-- TPC-H Data Generation")
    print(f"-- Scale Factor: {args.scale}")
    print(f"-- Customer: {data.customer_count} rows")
    print(f"-- Supplier: {data.supplier_count} rows")
    print(f"-- Part: {data.part_count} rows")
    print(f"-- Partsupp: {data.part_count * 4} rows")
    print(f"-- Orders: {data.orders_count} rows")
    print(f"-- Lineitem: {data.lineitem_count} rows")
    print()

    # Generate reference tables first
    generate_region()
    generate_nation()

    # Generate main tables
    generate_supplier(data)
    generate_customer(data)
    generate_part(data)
    generate_partsupp(data)
    generate_orders(data)
    generate_lineitem(data)

    print("\n-- Data generation complete")


if __name__ == "__main__":
    main()
