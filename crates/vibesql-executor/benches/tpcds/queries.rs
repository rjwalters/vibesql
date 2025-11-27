//! TPC-DS Query Definitions
//!
//! This module provides query definitions for TPC-DS benchmark testing.
//! Queries are organized by complexity and feature requirements.

// =============================================================================
// Sanity Queries - Basic table verification
// =============================================================================

/// Count rows in income_band table (should be 20)
pub const SANITY_INCOME_BAND_COUNT: &str = "SELECT COUNT(*) FROM income_band";

/// Verify income band ranges are correct
pub const SANITY_INCOME_BAND_RANGES: &str = r#"
SELECT ib_income_band_sk, ib_lower_bound, ib_upper_bound
FROM income_band
ORDER BY ib_income_band_sk
LIMIT 5
"#;

/// Count rows in customer_demographics table
pub const SANITY_CUSTOMER_DEMOGRAPHICS_COUNT: &str =
    "SELECT COUNT(*) FROM customer_demographics";

/// Verify customer demographics combinations
pub const SANITY_CUSTOMER_DEMOGRAPHICS_SAMPLE: &str = r#"
SELECT cd_demo_sk, cd_gender, cd_marital_status, cd_education_status
FROM customer_demographics
ORDER BY cd_demo_sk
LIMIT 10
"#;

/// Count rows in household_demographics table
pub const SANITY_HOUSEHOLD_DEMOGRAPHICS_COUNT: &str =
    "SELECT COUNT(*) FROM household_demographics";

/// Verify household demographics data
pub const SANITY_HOUSEHOLD_DEMOGRAPHICS_SAMPLE: &str = r#"
SELECT hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count
FROM household_demographics
ORDER BY hd_demo_sk
LIMIT 10
"#;

/// Count rows in call_center table
pub const SANITY_CALL_CENTER_COUNT: &str = "SELECT COUNT(*) FROM call_center";

/// Verify call center data
pub const SANITY_CALL_CENTER_SAMPLE: &str = r#"
SELECT cc_call_center_sk, cc_call_center_id, cc_name, cc_class, cc_employees
FROM call_center
ORDER BY cc_call_center_sk
LIMIT 5
"#;

/// Count rows in inventory table
pub const SANITY_INVENTORY_COUNT: &str = "SELECT COUNT(*) FROM inventory";

/// Verify inventory data
pub const SANITY_INVENTORY_SAMPLE: &str = r#"
SELECT inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand
FROM inventory
ORDER BY inv_date_sk, inv_item_sk, inv_warehouse_sk
LIMIT 10
"#;

// =============================================================================
// Simple Queries - Basic filtering and aggregation
// =============================================================================

/// Query demographics by gender
pub const SIMPLE_DEMOGRAPHICS_BY_GENDER: &str = r#"
SELECT cd_gender, COUNT(*) as cnt
FROM customer_demographics
GROUP BY cd_gender
ORDER BY cd_gender
"#;

/// Query demographics by education status
pub const SIMPLE_DEMOGRAPHICS_BY_EDUCATION: &str = r#"
SELECT cd_education_status, COUNT(*) as cnt
FROM customer_demographics
GROUP BY cd_education_status
ORDER BY cnt DESC
"#;

/// Query household demographics by income band
pub const SIMPLE_HOUSEHOLD_BY_INCOME: &str = r#"
SELECT hd_income_band_sk, COUNT(*) as households
FROM household_demographics
GROUP BY hd_income_band_sk
ORDER BY hd_income_band_sk
"#;

/// Query call centers by class
pub const SIMPLE_CALL_CENTERS_BY_CLASS: &str = r#"
SELECT cc_class, COUNT(*) as centers, SUM(cc_employees) as total_employees
FROM call_center
GROUP BY cc_class
ORDER BY cc_class
"#;

/// Query average inventory by warehouse
pub const SIMPLE_INVENTORY_BY_WAREHOUSE: &str = r#"
SELECT inv_warehouse_sk, AVG(inv_quantity_on_hand) as avg_quantity
FROM inventory
GROUP BY inv_warehouse_sk
ORDER BY inv_warehouse_sk
"#;

// =============================================================================
// Join Queries - Testing relationships between tables
// =============================================================================

/// Join household_demographics with income_band
pub const JOIN_HOUSEHOLD_INCOME: &str = r#"
SELECT
    income_band.ib_lower_bound,
    income_band.ib_upper_bound,
    COUNT(*) as household_count,
    AVG(household_demographics.hd_dep_count) as avg_dependents
FROM household_demographics
JOIN income_band ON household_demographics.hd_income_band_sk = income_band.ib_income_band_sk
GROUP BY income_band.ib_income_band_sk, income_band.ib_lower_bound, income_band.ib_upper_bound
ORDER BY ib_lower_bound
"#;

/// Query high-income households with vehicles
pub const JOIN_HIGH_INCOME_VEHICLES: &str = r#"
SELECT
    income_band.ib_lower_bound,
    household_demographics.hd_vehicle_count,
    COUNT(*) as cnt
FROM household_demographics
JOIN income_band ON household_demographics.hd_income_band_sk = income_band.ib_income_band_sk
WHERE income_band.ib_lower_bound >= 100000
GROUP BY income_band.ib_lower_bound, household_demographics.hd_vehicle_count
ORDER BY ib_lower_bound, hd_vehicle_count
"#;

// =============================================================================
// TPC-DS Style Queries - Simplified versions of actual TPC-DS queries
// =============================================================================

/// Simplified Q1-style: Return items with excess inventory
/// (Actual Q1 involves store_returns, store_sales, date_dim, store, customer)
pub const TPCDS_STYLE_Q1: &str = r#"
SELECT inv_item_sk, SUM(inv_quantity_on_hand) as total_qty
FROM inventory
GROUP BY inv_item_sk
HAVING SUM(inv_quantity_on_hand) > 500
ORDER BY total_qty DESC
LIMIT 100
"#;

/// Simplified Q4-style: Demographic analysis
/// (Actual Q4 involves catalog_sales, web_sales, customer, customer_address)
pub const TPCDS_STYLE_Q4: &str = r#"
SELECT
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_credit_rating,
    COUNT(*) as customer_count
FROM customer_demographics
WHERE cd_dep_count > 0
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_credit_rating
ORDER BY customer_count DESC
LIMIT 50
"#;

/// Simplified Q6-style: Call center performance by class
/// (Actual Q6 involves web_sales, customer_demographics, date_dim, item)
pub const TPCDS_STYLE_Q6: &str = r#"
SELECT
    cc_class,
    cc_name,
    cc_employees,
    cc_sq_ft,
    cc_employees * 1.0 / cc_sq_ft * 1000 as employees_per_1000_sqft
FROM call_center
WHERE cc_employees > 0 AND cc_sq_ft > 0
ORDER BY employees_per_1000_sqft DESC
"#;

/// Simplified Q11-style: Household analysis by income
/// (Actual Q11 involves customer, store_sales, web_sales, date_dim)
pub const TPCDS_STYLE_Q11: &str = r#"
SELECT
    income_band.ib_income_band_sk,
    income_band.ib_lower_bound,
    income_band.ib_upper_bound,
    household_demographics.hd_buy_potential,
    COUNT(*) as household_count,
    AVG(household_demographics.hd_dep_count) as avg_deps,
    AVG(household_demographics.hd_vehicle_count) as avg_vehicles
FROM household_demographics
JOIN income_band ON household_demographics.hd_income_band_sk = income_band.ib_income_band_sk
WHERE income_band.ib_lower_bound >= 50000
GROUP BY income_band.ib_income_band_sk, income_band.ib_lower_bound, income_band.ib_upper_bound, household_demographics.hd_buy_potential
ORDER BY ib_lower_bound, hd_buy_potential
"#;

// =============================================================================
// All sanity queries for testing
// =============================================================================

pub const ALL_SANITY_QUERIES: &[(&str, &str)] = &[
    ("income_band_count", SANITY_INCOME_BAND_COUNT),
    ("income_band_ranges", SANITY_INCOME_BAND_RANGES),
    (
        "customer_demographics_count",
        SANITY_CUSTOMER_DEMOGRAPHICS_COUNT,
    ),
    (
        "customer_demographics_sample",
        SANITY_CUSTOMER_DEMOGRAPHICS_SAMPLE,
    ),
    (
        "household_demographics_count",
        SANITY_HOUSEHOLD_DEMOGRAPHICS_COUNT,
    ),
    (
        "household_demographics_sample",
        SANITY_HOUSEHOLD_DEMOGRAPHICS_SAMPLE,
    ),
    ("call_center_count", SANITY_CALL_CENTER_COUNT),
    ("call_center_sample", SANITY_CALL_CENTER_SAMPLE),
    ("inventory_count", SANITY_INVENTORY_COUNT),
    ("inventory_sample", SANITY_INVENTORY_SAMPLE),
];

pub const ALL_SIMPLE_QUERIES: &[(&str, &str)] = &[
    ("demographics_by_gender", SIMPLE_DEMOGRAPHICS_BY_GENDER),
    (
        "demographics_by_education",
        SIMPLE_DEMOGRAPHICS_BY_EDUCATION,
    ),
    ("household_by_income", SIMPLE_HOUSEHOLD_BY_INCOME),
    ("call_centers_by_class", SIMPLE_CALL_CENTERS_BY_CLASS),
    ("inventory_by_warehouse", SIMPLE_INVENTORY_BY_WAREHOUSE),
];

pub const ALL_JOIN_QUERIES: &[(&str, &str)] = &[
    ("household_income", JOIN_HOUSEHOLD_INCOME),
    ("high_income_vehicles", JOIN_HIGH_INCOME_VEHICLES),
];

pub const ALL_TPCDS_STYLE_QUERIES: &[(&str, &str)] = &[
    ("tpcds_q1_style", TPCDS_STYLE_Q1),
    ("tpcds_q4_style", TPCDS_STYLE_Q4),
    ("tpcds_q6_style", TPCDS_STYLE_Q6),
    ("tpcds_q11_style", TPCDS_STYLE_Q11),
];
