//! TPC-H Data Generator
//!
//! This module provides data generation utilities for TPC-H benchmark tables.
//! It includes constants for reference data (nations, regions) and a data
//! generator that produces deterministic pseudo-random data based on scale factor.

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

pub const NATIONS: &[(&str, usize)] = &[
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];

pub const REGIONS: &[&str] = &["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
pub const SEGMENTS: &[&str] = &["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"];
pub const PRIORITIES: &[&str] = &["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
pub const SHIP_MODES: &[&str] = &["AIR", "AIR REG", "MAIL", "RAIL", "SHIP", "TRUCK", "FOB"];
pub const COLORS: &[&str] = &[
    "almond",
    "antique",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanched",
    "blue",
    "blush",
    "brown",
    "burlywood",
    "burnished",
    "chartreuse",
    "chiffon",
    "chocolate",
    "coral",
    "cornflower",
    "cornsilk",
    "cream",
    "cyan",
    "dark",
    "deep",
    "dim",
    "dodger",
    "drab",
    "firebrick",
    "floral",
    "forest",
    "frosted",
    "gainsboro",
    "ghost",
    "goldenrod",
    "green",
    "grey",
    "honeydew",
    "hot",
    "indian",
    "ivory",
    "khaki",
    "lace",
    "lavender",
    "lawn",
    "lemon",
    "light",
    "lime",
    "linen",
    "magenta",
    "maroon",
    "medium",
    "metallic",
    "midnight",
    "mint",
    "misty",
    "moccasin",
    "navajo",
    "navy",
    "olive",
    "orange",
    "orchid",
    "pale",
    "papaya",
    "peach",
    "peru",
    "pink",
    "plum",
    "powder",
    "puff",
    "purple",
    "red",
    "rose",
    "rosy",
    "royal",
    "saddle",
    "salmon",
    "sandy",
    "seashell",
    "sienna",
    "sky",
    "slate",
    "smoke",
    "snow",
    "spring",
    "steel",
    "tan",
    "thistle",
    "tomato",
    "turquoise",
    "violet",
    "wheat",
    "white",
    "yellow",
];
pub const TYPES: &[&str] = &["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
pub const CONTAINERS: &[&str] = &[
    "SM CASE",
    "SM BOX",
    "SM PACK",
    "SM PKG",
    "MED BAG",
    "MED BOX",
    "MED PKG",
    "MED PACK",
    "LG CASE",
    "LG BOX",
    "LG PACK",
    "LG PKG",
    "JUMBO BOX",
    "JUMBO CASE",
    "JUMBO PACK",
    "JUMBO PKG",
    "WRAP CASE",
    "WRAP BOX",
    "WRAP PACK",
    "WRAP PKG",
];

pub struct TPCHData {
    pub scale_factor: f64,
    pub customer_count: usize,
    pub orders_count: usize,
    pub lineitem_count: usize,
    pub supplier_count: usize,
    pub part_count: usize,
    rng: ChaCha8Rng,
}

impl TPCHData {
    pub fn new(scale_factor: f64) -> Self {
        let customer_count = ((150_000.0 * scale_factor) as usize).max(100);
        let orders_count = ((1_500_000.0 * scale_factor) as usize).max(1000);
        let lineitem_count = ((6_000_000.0 * scale_factor) as usize).max(4000);
        let supplier_count = ((10_000.0 * scale_factor) as usize).max(10);
        let part_count = ((200_000.0 * scale_factor) as usize).max(200);

        Self {
            scale_factor,
            customer_count,
            orders_count,
            lineitem_count,
            supplier_count,
            part_count,
            rng: ChaCha8Rng::seed_from_u64(42), // Deterministic
        }
    }

    pub fn random_varchar(&mut self, max_len: usize) -> String {
        let len = self.rng.random_range(10..max_len);
        (0..len).map(|_| self.rng.sample(rand::distr::Alphanumeric) as char).collect()
    }

    pub fn random_phone(&mut self, nation_key: usize) -> String {
        format!(
            "{:02}-{:03}-{:03}-{:04}",
            10 + nation_key,
            self.rng.random_range(100..1000),
            self.rng.random_range(100..1000),
            self.rng.random_range(1000..10000)
        )
    }

    pub fn random_date(&mut self, _start: &str, _end: &str) -> String {
        // Simple date generation between start and end
        let year = self.rng.random_range(1992..1999);
        let month = self.rng.random_range(1..13);
        let day = self.rng.random_range(1..29); // Simplified
        format!("{:04}-{:02}-{:02}", year, month, day)
    }
}
