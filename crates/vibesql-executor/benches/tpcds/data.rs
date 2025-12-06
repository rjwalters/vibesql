//! TPC-DS Data Generator
//!
//! This module provides data generation utilities for TPC-DS benchmark tables.
//! It includes constants for reference data and a data generator that produces
//! deterministic pseudo-random data based on scale factor.
//!
//! TPC-DS Schema includes:
//! - 7 fact tables: store_sales, store_returns, catalog_sales, catalog_returns,
//!   web_sales, web_returns, inventory
//! - 18 dimension tables: date_dim, time_dim, item, customer, customer_address,
//!   customer_demographics, household_demographics, income_band, store,
//!   catalog_page, web_page, web_site, warehouse, ship_mode, reason,
//!   promotion, call_center

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

// Gender options
pub const GENDERS: &[&str] = &["M", "F"];

// Marital status options
pub const MARITAL_STATUS: &[&str] = &["S", "M", "D", "W", "U"];

// Education status options
pub const EDUCATION_STATUS: &[&str] = &[
    "Primary",
    "Secondary",
    "College",
    "2 yr Degree",
    "4 yr Degree",
    "Advanced Degree",
    "Unknown",
];

// Credit rating options
pub const CREDIT_RATINGS: &[&str] = &["Low Risk", "Good", "High Risk", "Unknown"];

// Buy potential options
pub const BUY_POTENTIALS: &[&str] =
    &["Unknown", "0-500", "501-1000", "1001-5000", "5001-10000", ">10000"];

// Dependents count (0-6)
pub const DEP_COUNTS: &[i32] = &[0, 1, 2, 3, 4, 5, 6];

// Employed count (0-6)
pub const EMP_COUNTS: &[i32] = &[0, 1, 2, 3, 4, 5, 6];

// Vehicle count (0-4)
pub const VEHICLE_COUNTS: &[i32] = &[-1, 0, 1, 2, 3, 4];

// Income bands (ranges from TPC-DS spec)
pub const INCOME_BANDS: &[(i32, i32)] = &[
    (0, 10000),
    (10001, 20000),
    (20001, 30000),
    (30001, 40000),
    (40001, 50000),
    (50001, 60000),
    (60001, 70000),
    (70001, 80000),
    (80001, 90000),
    (90001, 100000),
    (100001, 110000),
    (110001, 120000),
    (120001, 130000),
    (130001, 140000),
    (140001, 150000),
    (150001, 160000),
    (160001, 170000),
    (170001, 180000),
    (180001, 190000),
    (190001, 200000),
];

// Ship modes
pub const SHIP_MODES: &[&str] =
    &["REGULAR", "EXPRESS", "OVERNIGHT", "TWO DAY", "LIBRARY", "NEXT DAY"];

// States (US states for customer addresses)
pub const STATES: &[&str] = &[
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
];

// Countries
pub const COUNTRIES: &[&str] = &["United States"];

// Store names prefixes
pub const STORE_NAMES: &[&str] =
    &["able", "ation", "bar", "cally", "eing", "ese", "ought", "pri", "able", "eing"];

// Catalog page types
pub const CATALOG_PAGE_TYPES: &[&str] = &["bi-annual", "quarterly", "monthly"];

// Web page types
pub const WEB_PAGE_TYPES: &[&str] =
    &["welcome", "order", "protected", "feedback", "review", "dynamic"];

// Web site classes
pub const WEB_SITE_CLASSES: &[&str] = &["Unknown", "Unknown"];

// Return reasons
pub const REASONS: &[&str] = &[
    "Did not like the color",
    "Did not fit",
    "Found a better price",
    "Not the product I ordered",
    "Damaged product",
    "Quality problems",
    "Duplicate purchase",
    "Wrong item",
    "Changed my mind",
    "Gift exchange",
    "Defective item",
    "Part missing",
    "Other",
    "Lost my receipt",
    "Late delivery",
];

// Promotion purposes
pub const PROMO_PURPOSES: &[&str] = &["Unknown", "Other", "Clearance", "Sale", "New Product"];

// Promotion channel flags
pub const CHANNEL_FLAGS: &[&str] = &["N", "Y"];

// Item categories
pub const CATEGORIES: &[&str] = &[
    "Women",
    "Men",
    "Children",
    "Home",
    "Electronics",
    "Music",
    "Books",
    "Sports",
    "Shoes",
    "Jewelry",
];

// Item classes
pub const CLASSES: &[&str] = &[
    "shirts",
    "pants",
    "dresses",
    "accessories",
    "swimwear",
    "computers",
    "cameras",
    "stereo",
    "classical",
    "pop",
    "rock",
    "fiction",
    "mystery",
    "sports",
    "travel",
    "football",
    "baseball",
    "basketball",
    "tennis",
    "golf",
    "running",
    "hiking",
    "camping",
];

// Item brands
pub const BRANDS: &[&str] = &[
    "amalgimported",
    "edu packmaxi",
    "exportiexporti",
    "importoimport",
    "amalgexporti",
    "edu packamalg",
    "exportibrand",
    "importobrand",
    "amalgbrand",
    "edu packexporti",
    "exportischolar",
    "importoamalg",
    "amalgscholar",
    "edu packimporto",
    "exportiunivamalg",
    "importoimporto",
    "amalgunivamalg",
    "edu packbrand",
    "scholaramalgamalg",
    "importoexporti",
    "corpscholar",
    "brandcorp",
    "maxicorp",
    "univcorp",
];

// Item colors
pub const ITEM_COLORS: &[&str] = &[
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

// Item sizes
pub const ITEM_SIZES: &[&str] =
    &["small", "medium", "large", "extra large", "petite", "N/A", "economy", "economy"];

// Item units
pub const ITEM_UNITS: &[&str] = &[
    "Ounce", "Oz", "Bunch", "Gram", "Pound", "Lb", "Ton", "Carton", "Cup", "Dram", "Gross",
    "Bundle", "Tbl", "Box", "Case", "Dozen", "Each", "N/A", "Pallet", "Unknown",
];

// Item containers
pub const ITEM_CONTAINERS: &[&str] = &["Unknown", "Wrap", "Box"];

/// Time granularity for time_dim table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeGranularity {
    /// One row per second (86,400 rows) - TPC-DS spec
    Second,
    /// One row per minute (1,440 rows) - recommended default
    #[default]
    Minute,
    /// One row per hour (24 rows) - minimal for fast loading
    Hour,
}

impl TimeGranularity {
    /// Returns the number of seconds between each time_dim row
    pub fn seconds_per_row(&self) -> usize {
        match self {
            TimeGranularity::Second => 1,
            TimeGranularity::Minute => 60,
            TimeGranularity::Hour => 3600,
        }
    }

    /// Returns the number of rows in time_dim for this granularity
    pub fn row_count(&self) -> usize {
        86400 / self.seconds_per_row()
    }
}

/// Configuration for TPC-DS dimension table sizes
///
/// By default, uses reduced sizes for faster data loading while maintaining
/// meaningful benchmark results:
/// - date_dim: 6 years (1998-2003) instead of 200 years
/// - time_dim: minute granularity instead of second granularity
#[derive(Debug, Clone)]
pub struct TPCDSConfig {
    /// Start year for date_dim (default: 1998)
    pub date_start_year: i32,
    /// End year for date_dim (default: 2003)
    pub date_end_year: i32,
    /// Time granularity for time_dim (default: Minute)
    pub time_granularity: TimeGranularity,
}

impl Default for TPCDSConfig {
    fn default() -> Self {
        Self {
            date_start_year: 1998,
            date_end_year: 2003,
            time_granularity: TimeGranularity::Minute,
        }
    }
}

impl TPCDSConfig {
    /// Create a config with TPC-DS spec-compliant sizes (much larger)
    pub fn spec_compliant() -> Self {
        Self {
            date_start_year: 1900,
            date_end_year: 2100,
            time_granularity: TimeGranularity::Second,
        }
    }

    /// Create a minimal config for fastest loading (testing only)
    pub fn minimal() -> Self {
        Self { date_start_year: 1998, date_end_year: 2003, time_granularity: TimeGranularity::Hour }
    }

    /// Calculate the number of days in the date range
    /// Uses simplified calculation (365 days per year, ignoring leap years)
    pub fn date_dim_count(&self) -> usize {
        let years = (self.date_end_year - self.date_start_year + 1) as usize;
        years * 365
    }

    /// Calculate the number of rows in time_dim
    pub fn time_dim_count(&self) -> usize {
        self.time_granularity.row_count()
    }
}

pub struct TPCDSData {
    pub scale_factor: f64,
    pub config: TPCDSConfig,

    // Dimension table counts (fixed or scaled)
    pub date_dim_count: usize, // Configurable (default: ~2K days for 1998-2003)
    pub time_dim_count: usize, // Configurable (default: 1,440 minutes)
    pub item_count: usize,     // SF * 18,000
    pub customer_count: usize, // SF * 100,000
    pub customer_address_count: usize,
    pub customer_demographics_count: usize, // ~1,920 combinations
    pub household_demographics_count: usize,
    pub income_band_count: usize,  // 20 bands
    pub store_count: usize,        // 12 * SF (min 12)
    pub catalog_page_count: usize, // SF * 11,718
    pub web_page_count: usize,     // SF * 60
    pub web_site_count: usize,     // 30 * SF (min 6)
    pub warehouse_count: usize,    // 5 * SF (min 5)
    pub ship_mode_count: usize,    // 20
    pub reason_count: usize,       // 35
    pub promotion_count: usize,    // SF * 300
    pub call_center_count: usize,  // 6 * SF (min 6)

    // Fact table counts (scaled)
    pub store_sales_count: usize,
    pub store_returns_count: usize,
    pub catalog_sales_count: usize,
    pub catalog_returns_count: usize,
    pub web_sales_count: usize,
    pub web_returns_count: usize,
    pub inventory_count: usize,

    rng: ChaCha8Rng,
}

impl TPCDSData {
    /// Create a new TPCDSData with default configuration (reduced dimension sizes)
    pub fn new(scale_factor: f64) -> Self {
        Self::with_config(scale_factor, TPCDSConfig::default())
    }

    /// Create a new TPCDSData with custom configuration
    pub fn with_config(scale_factor: f64, config: TPCDSConfig) -> Self {
        // Dimension table sizes based on config
        let date_dim_count = config.date_dim_count();
        let time_dim_count = config.time_dim_count();
        let item_count = ((18_000.0 * scale_factor) as usize).max(1000);
        let customer_count = ((100_000.0 * scale_factor) as usize).max(1000);
        let customer_address_count = customer_count * 2; // ~2 addresses per customer
        let customer_demographics_count = 1920; // Fixed combinations
        let household_demographics_count = 7200; // Fixed combinations
        let income_band_count = 20; // Fixed
        let store_count = ((12.0 * scale_factor) as usize).max(12);
        let catalog_page_count = ((11_718.0 * scale_factor) as usize).max(100);
        let web_page_count = ((60.0 * scale_factor) as usize).max(60);
        let web_site_count = ((30.0 * scale_factor) as usize).max(6);
        let warehouse_count = ((5.0 * scale_factor) as usize).max(5);
        let ship_mode_count = 20; // Fixed
        let reason_count = 35; // Fixed
        let promotion_count = ((300.0 * scale_factor) as usize).max(100);
        let call_center_count = ((6.0 * scale_factor) as usize).max(6);

        // Fact table sizes (scaled significantly for benchmarking)
        // TPC-DS SF=1 has ~2.9M store_sales rows
        let store_sales_count = ((2_880_404.0 * scale_factor) as usize).max(10000);
        let store_returns_count = ((287_514.0 * scale_factor) as usize).max(1000);
        let catalog_sales_count = ((1_441_548.0 * scale_factor) as usize).max(5000);
        let catalog_returns_count = ((144_067.0 * scale_factor) as usize).max(500);
        let web_sales_count = ((719_384.0 * scale_factor) as usize).max(2500);
        let web_returns_count = ((71_763.0 * scale_factor) as usize).max(250);
        let inventory_count = ((11_745_000.0 * scale_factor) as usize).max(10000);

        Self {
            scale_factor,
            config,
            date_dim_count,
            time_dim_count,
            item_count,
            customer_count,
            customer_address_count,
            customer_demographics_count,
            household_demographics_count,
            income_band_count,
            store_count,
            catalog_page_count,
            web_page_count,
            web_site_count,
            warehouse_count,
            ship_mode_count,
            reason_count,
            promotion_count,
            call_center_count,
            store_sales_count,
            store_returns_count,
            catalog_sales_count,
            catalog_returns_count,
            web_sales_count,
            web_returns_count,
            inventory_count,
            rng: ChaCha8Rng::seed_from_u64(42), // Deterministic
        }
    }

    pub fn random_varchar(&mut self, max_len: usize) -> String {
        let len = self.rng.random_range(10..max_len.max(11));
        (0..len).map(|_| self.rng.sample(rand::distr::Alphanumeric) as char).collect()
    }

    pub fn random_i32(&mut self, min: i32, max: i32) -> i32 {
        self.rng.random_range(min..=max)
    }

    pub fn random_f64(&mut self, min: f64, max: f64) -> f64 {
        self.rng.random_range(min..=max)
    }

    /// Generate a random decimal value rounded to 2 decimal places
    /// Used for prices, costs, and monetary values
    pub fn random_decimal(&mut self, min: f64, max: f64) -> f64 {
        let value = self.rng.random_range(min..=max);
        (value * 100.0).round() / 100.0
    }

    pub fn random_bool(&mut self) -> bool {
        self.rng.random_bool(0.5)
    }

    /// Generate a date string in YYYY-MM-DD format for the TPC-DS date range (1998-2003)
    pub fn random_date(&mut self) -> String {
        let year = self.rng.random_range(1998..=2003);
        let month = self.rng.random_range(1..=12);
        let day = self.rng.random_range(1..=28); // Simplified
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    /// Generate a specific date for a given date_sk (surrogate key)
    /// TPC-DS dates span from 1998-01-01 to 2003-12-31 (~2191 days)
    pub fn date_sk_to_string(&self, date_sk: i32) -> String {
        // Base date is 1998-01-01
        let base_year = 1998;
        let days = date_sk as i64;

        // Simple calculation (ignoring leap years for simplicity in benchmark)
        let year = base_year + (days / 365) as i32;
        let day_of_year = (days % 365) as i32;

        let month = (day_of_year / 30).min(11) + 1;
        let day = (day_of_year % 30) + 1;

        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    /// Generate time string in HH:MM:SS format
    pub fn time_sk_to_string(&self, time_sk: i32) -> String {
        let hours = time_sk / 3600;
        let minutes = (time_sk % 3600) / 60;
        let seconds = time_sk % 60;
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    }

    /// Pick a random element from a slice
    pub fn random_choice<'a, T>(&mut self, choices: &'a [T]) -> &'a T {
        &choices[self.rng.random_range(0..choices.len())]
    }

    /// Pick a random index for a table
    pub fn random_key(&mut self, max: usize) -> i32 {
        self.rng.random_range(1..=max as i32)
    }

    /// Generate a street address
    pub fn random_street_address(&mut self) -> String {
        let number = self.rng.random_range(1..9999);
        let street = self.random_varchar(20);
        format!("{} {} Street", number, street)
    }

    /// Generate a city name
    pub fn random_city(&mut self) -> String {
        let prefixes = ["North", "South", "East", "West", "New", "Old", ""];
        let suffixes = ["ville", "town", "city", "burg", "port", "field", "dale"];
        let base = self.random_varchar(8);
        let prefix = prefixes[self.rng.random_range(0..prefixes.len())];
        let suffix = suffixes[self.rng.random_range(0..suffixes.len())];
        if prefix.is_empty() {
            format!("{}{}", base, suffix)
        } else {
            format!("{} {}{}", prefix, base, suffix)
        }
    }

    /// Generate a ZIP code
    pub fn random_zip(&mut self) -> String {
        format!("{:05}", self.rng.random_range(10000..99999))
    }

    /// Generate a phone number
    pub fn random_phone(&mut self) -> String {
        format!(
            "({:03}) {:03}-{:04}",
            self.rng.random_range(200..999),
            self.rng.random_range(100..999),
            self.rng.random_range(1000..9999)
        )
    }

    /// Generate email address
    pub fn random_email(&mut self) -> String {
        let user = self.random_varchar(10);
        let domains = ["example.com", "email.org", "mail.net", "web.com"];
        let domain = domains[self.rng.random_range(0..domains.len())];
        format!("{}@{}", user.to_lowercase(), domain)
    }
}
