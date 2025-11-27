//! TPC-DS Data Generator
//!
//! This module provides data generation utilities for TPC-DS benchmark tables.
//! It includes constants for reference data and a data generator that produces
//! deterministic pseudo-random data based on scale factor.

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

// =============================================================================
// TPC-DS Reference Data Constants
// =============================================================================

// Customer Demographics constants
pub const CD_GENDERS: &[&str] = &["M", "F"];
pub const CD_MARITAL_STATUS: &[&str] = &["M", "S", "D", "W", "U"]; // Married, Single, Divorced, Widowed, Unknown
pub const CD_EDUCATION_STATUS: &[&str] = &[
    "Primary",
    "Secondary",
    "College",
    "2 yr Degree",
    "4 yr Degree",
    "Advanced Degree",
    "Unknown",
];
pub const CD_CREDIT_RATINGS: &[&str] = &["Good", "Low Risk", "High Risk", "Unknown"];

// Household Demographics constants
pub const HD_BUY_POTENTIALS: &[&str] = &[
    "Unknown",
    "0-500",
    "501-1000",
    "1001-5000",
    "5001-10000",
    ">10000",
];

// Call Center constants
pub const CC_CLASSES: &[&str] = &["small", "medium", "large"];
pub const CC_HOURS: &[&str] = &["8AM-4PM", "8AM-12AM", "8AM-8AM"];

// =============================================================================
// TPC-DS Data Generator
// =============================================================================

pub struct TPCDSData {
    pub scale_factor: f64,
    // Dimension table counts (mostly fixed or small scale)
    pub income_band_count: usize,           // Fixed at 20 bands
    pub customer_demographics_count: usize, // ~1,920 combinations
    pub household_demographics_count: usize, // ~7,200 combinations
    pub call_center_count: usize,           // ~6 at SF=1
    pub inventory_count: usize,             // Based on items * warehouses * weeks
    // Related counts needed for inventory
    pub item_count: usize,
    pub warehouse_count: usize,
    pub date_count: usize, // ~1,825 days (5 years)
    rng: ChaCha8Rng,
}

impl TPCDSData {
    pub fn new(scale_factor: f64) -> Self {
        // TPC-DS dimension table sizes
        // income_band is fixed at 20 bands
        let income_band_count = 20;

        // customer_demographics: all combinations of attributes
        // 2 genders * 5 marital * 7 education * (varies) = ~1,920 rows
        let customer_demographics_count = 1920;

        // household_demographics: 20 income bands * 6 buy_potential * 10 dep_count * 6 vehicle_count
        // But TPC-DS uses ~7,200 rows
        let household_demographics_count = 7200;

        // call_center scales with SF but minimum is ~6
        let call_center_count = ((6.0 * scale_factor) as usize).max(6);

        // Item count for inventory reference
        let item_count = ((18000.0 * scale_factor) as usize).max(1000);
        let warehouse_count = ((5.0 * scale_factor) as usize).max(1);
        let date_count = 1825; // ~5 years of days

        // inventory: items * warehouses * weekly snapshots (~52 weeks)
        let inventory_count = ((item_count * warehouse_count * 52) as f64 * scale_factor.min(0.1))
            as usize;

        Self {
            scale_factor,
            income_band_count,
            customer_demographics_count,
            household_demographics_count,
            call_center_count,
            inventory_count,
            item_count,
            warehouse_count,
            date_count,
            rng: ChaCha8Rng::seed_from_u64(12345), // Different seed than TPC-H
        }
    }

    pub fn random_varchar(&mut self, max_len: usize) -> String {
        let len = self.rng.random_range(5..max_len.max(6));
        (0..len)
            .map(|_| self.rng.sample(rand::distr::Alphanumeric) as char)
            .collect()
    }

    pub fn random_date(&mut self, _start: &str, _end: &str) -> String {
        // Generate dates in TPC-DS range (1998-2003)
        let year = self.rng.random_range(1998..2004);
        let month = self.rng.random_range(1..13);
        let day = self.rng.random_range(1..29); // Simplified
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    pub fn random_time(&mut self) -> String {
        let hour = self.rng.random_range(0..24);
        let minute = self.rng.random_range(0..60);
        let second = self.rng.random_range(0..60);
        format!("{:02}:{:02}:{:02}", hour, minute, second)
    }

    pub fn random_phone(&mut self) -> String {
        format!(
            "{:03}-{:03}-{:04}",
            self.rng.random_range(200..999),
            self.rng.random_range(100..1000),
            self.rng.random_range(1000..10000)
        )
    }

    pub fn random_zip(&mut self) -> String {
        format!("{:05}", self.rng.random_range(10000..99999))
    }

    pub fn random_integer(&mut self, min: i32, max: i32) -> i32 {
        self.rng.random_range(min..max)
    }
}
