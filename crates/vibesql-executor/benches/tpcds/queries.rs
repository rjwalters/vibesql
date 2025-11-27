//! TPC-DS Query Definitions
//!
//! This module contains TPC-DS benchmark queries adapted for the Phase 1 schema.
//! The original TPC-DS has 99 queries; we implement a subset that work with
//! our current tables: date_dim, time_dim, item, customer, customer_address,
//! store, and store_sales.
//!
//! Queries are numbered to match the official TPC-DS query numbers where possible,
//! with adaptations noted in comments.

// =============================================================================
// TPC-DS Q3: Report sales by brand for a given year and month
// =============================================================================
// Original: Uses catalog_sales. Adapted to use store_sales.
// Tests: 3-way join, date filtering, aggregation, ordering
pub const TPCDS_Q3: &str = r#"
SELECT
    d_year,
    i_brand_id,
    i_brand,
    SUM(ss_ext_sales_price) as sum_sales
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND i_manufact_id = 1
    AND d_moy = 11
GROUP BY d_year, i_brand_id, i_brand
ORDER BY d_year, sum_sales DESC, i_brand_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q7: Promotion impact analysis
// =============================================================================
// Original: Uses customer_demographics. Simplified to use base tables.
// Tests: 4-way join, aggregation with multiple measures, filtering
pub const TPCDS_Q7: &str = r#"
SELECT
    i_item_id,
    AVG(ss_quantity) as avg_quantity,
    AVG(ss_list_price) as avg_list_price,
    AVG(ss_coupon_amt) as avg_coupon_amt,
    AVG(ss_sales_price) as avg_sales_price
FROM store_sales, customer, date_dim, item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND c_birth_year BETWEEN 1970 AND 1980
    AND d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q19: Store sales by customer location
// =============================================================================
// Original: Uses catalog_returns. Adapted to store_sales only.
// Tests: 5-way join, geographic filtering, aggregation
pub const TPCDS_Q19: &str = r#"
SELECT
    i_brand_id,
    i_brand,
    i_manufact_id,
    i_manufact,
    SUM(ss_ext_sales_price) as ext_price
FROM date_dim, store_sales, item, customer, customer_address
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND d_moy = 11
    AND d_year = 1999
    AND ca_state = 'CA'
GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q42: Monthly store sales by item category
// =============================================================================
// Original query. Works with our Phase 1 schema.
// Tests: 3-way join, date filtering, GROUP BY, ORDER BY
pub const TPCDS_Q42: &str = r#"
SELECT
    d_year,
    i_category_id,
    i_category,
    SUM(ss_ext_sales_price) as total_sales
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND i_manager_id = 1
    AND d_moy = 12
    AND d_year = 2000
GROUP BY d_year, i_category_id, i_category
ORDER BY total_sales DESC, d_year, i_category_id, i_category
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q52: Weekly store sales
// =============================================================================
// Original: Uses brand filtering. Adapted to match our data.
// Tests: 3-way join, date arithmetic with week, aggregation
pub const TPCDS_Q52: &str = r#"
SELECT
    d_year,
    d_week_seq,
    SUM(ss_ext_sales_price) as weekly_sales
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND i_category_id = 1
    AND d_year = 2000
GROUP BY d_year, d_week_seq
ORDER BY d_year, d_week_seq, weekly_sales
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q55: Brand sales by store
// =============================================================================
// Adapted from original. Uses store dimension.
// Tests: 4-way join with store, month filtering
pub const TPCDS_Q55: &str = r#"
SELECT
    i_brand_id,
    i_brand,
    SUM(ss_ext_sales_price) as ext_price
FROM date_dim, store_sales, item, store
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND i_manager_id = 5
    AND d_moy = 11
    AND d_year = 2000
GROUP BY i_brand_id, i_brand
ORDER BY ext_price DESC, i_brand_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q68: Store sales by customer demographics
// =============================================================================
// Adapted to use customer birth year instead of demographics table.
// Tests: Multiple aggregations, customer filtering
pub const TPCDS_Q68: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    ca_city,
    c_birth_year,
    SUM(ss_ext_sales_price) as total_sales,
    SUM(ss_ext_list_price - ss_ext_discount_amt) as total_paid,
    SUM(ss_quantity) as items_bought
FROM store_sales, date_dim, customer, customer_address
WHERE ss_sold_date_sk = d_date_sk
    AND ss_customer_sk = c_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND d_year = 2000
    AND c_birth_year BETWEEN 1960 AND 1970
GROUP BY c_last_name, c_first_name, ca_city, c_birth_year
ORDER BY total_sales DESC, c_last_name, c_first_name
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q73: Store ticket analysis
// =============================================================================
// Adapted to use store table.
// Tests: Ticket-level grouping, HAVING clause
pub const TPCDS_Q73: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    c_customer_id,
    COUNT(*) as cnt
FROM store_sales, customer, store, date_dim
WHERE ss_customer_sk = c_customer_sk
    AND ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND d_year BETWEEN 1999 AND 2001
GROUP BY c_last_name, c_first_name, c_customer_id
HAVING COUNT(*) > 5
ORDER BY cnt DESC, c_last_name, c_first_name
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q89: Store profit analysis by category
// =============================================================================
// Simplified version testing profit calculations.
// Tests: Complex expressions in SELECT, profit margin calculation
pub const TPCDS_Q89: &str = r#"
SELECT
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy,
    SUM(ss_sales_price) as sum_sales,
    SUM(ss_net_profit) as sum_profit,
    AVG(ss_net_profit) as avg_profit
FROM store_sales, date_dim, item, store
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND d_year = 2000
GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
HAVING SUM(ss_sales_price) > 1000
ORDER BY sum_profit DESC, i_category, i_class
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q96: Store sales by time of day
// =============================================================================
// Uses time_dim for time-of-day analysis.
// Tests: Time dimension join, shift analysis
pub const TPCDS_Q96: &str = r#"
SELECT
    t_hour,
    t_am_pm,
    COUNT(*) as sales_count,
    SUM(ss_quantity) as total_quantity,
    SUM(ss_sales_price) as total_sales
FROM store_sales, time_dim, store
WHERE ss_sold_time_sk = t_time_sk
    AND ss_store_sk = s_store_sk
    AND s_store_name = 'Store#1'
GROUP BY t_hour, t_am_pm
ORDER BY t_hour
LIMIT 100
"#;

// =============================================================================
// Simple Sanity Queries for Testing
// =============================================================================

/// Simple date_dim count
pub const TPCDS_SANITY_DATE: &str = r#"
SELECT COUNT(*) as cnt, MIN(d_year) as min_year, MAX(d_year) as max_year
FROM date_dim
"#;

/// Simple store_sales aggregation
pub const TPCDS_SANITY_SALES: &str = r#"
SELECT
    COUNT(*) as num_sales,
    SUM(ss_ext_sales_price) as total_sales,
    AVG(ss_quantity) as avg_qty
FROM store_sales
"#;

/// Simple join test
pub const TPCDS_SANITY_JOIN: &str = r#"
SELECT
    d_year,
    COUNT(*) as cnt
FROM store_sales, date_dim
WHERE ss_sold_date_sk = d_date_sk
GROUP BY d_year
ORDER BY d_year
"#;

// =============================================================================
// Query Registry for Benchmark Iteration
// =============================================================================

/// All TPC-DS queries available for benchmarking
pub const TPCDS_QUERIES: &[(&str, &str)] = &[
    ("Q3", TPCDS_Q3),
    ("Q7", TPCDS_Q7),
    ("Q19", TPCDS_Q19),
    ("Q42", TPCDS_Q42),
    ("Q52", TPCDS_Q52),
    ("Q55", TPCDS_Q55),
    ("Q68", TPCDS_Q68),
    ("Q73", TPCDS_Q73),
    ("Q89", TPCDS_Q89),
    ("Q96", TPCDS_Q96),
];

/// Sanity check queries for validation
pub const TPCDS_SANITY_QUERIES: &[(&str, &str)] = &[
    ("sanity_date", TPCDS_SANITY_DATE),
    ("sanity_sales", TPCDS_SANITY_SALES),
    ("sanity_join", TPCDS_SANITY_JOIN),
];
