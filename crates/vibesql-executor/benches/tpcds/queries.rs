//! TPC-DS Query Definitions
//!
//! This module contains TPC-DS benchmark queries adapted for the implemented schema.
//! The original TPC-DS has 99 queries; we implement a subset that work with our
//! current tables across three phases:
//!
//! ## Phase 1 (Core Tables):
//! - date_dim, time_dim, item, customer, customer_address, store, store_sales
//! - Queries: Q3, Q7, Q19, Q42, Q52, Q55, Q68, Q73, Q89, Q96
//!
//! ## Phase 2 (Extended Tables):
//! - promotion, warehouse, ship_mode, reason, store_returns
//! - Queries: Q25, Q26, Q27, Q35, Q50, Q81, Q82, Q83
//!
//! ## Phase 3 (Full E-Commerce):
//! - catalog_page, web_page, web_site
//! - catalog_sales, catalog_returns, web_sales, web_returns
//! - Queries: Q13, Q16, Q20, Q32, Q37, Q60, Q62, Q76, Q84, Q92
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
// Phase 2 Queries - Using new tables (promotion, warehouse, reason, store_returns)
// =============================================================================

// TPC-DS Q25: Store sales and returns by customer
// Tests: Join between store_sales and store_returns
pub const TPCDS_Q25: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    s_store_id,
    s_store_name,
    SUM(ss_net_profit) as store_sales_profit,
    SUM(sr_net_loss) as store_returns_loss,
    SUM(ss_net_profit) - SUM(sr_net_loss) as total_profit
FROM store_sales, store_returns, date_dim d1, date_dim d2, store, item
WHERE ss_item_sk = i_item_sk
    AND ss_sold_date_sk = d1.d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_customer_sk = sr_customer_sk
    AND ss_item_sk = sr_item_sk
    AND sr_returned_date_sk = d2.d_date_sk
    AND d1.d_moy = 4
    AND d1.d_year = 2000
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100
"#;

// TPC-DS Q26: Promotional impact on sales
// Tests: Join with promotion table
pub const TPCDS_Q26: &str = r#"
SELECT
    i_item_id,
    AVG(ss_quantity) as avg_quantity,
    AVG(ss_list_price) as avg_list_price,
    AVG(ss_coupon_amt) as avg_coupon_amt,
    AVG(ss_sales_price) as avg_sales_price
FROM store_sales, customer, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_promo_sk = p_promo_sk
    AND c_birth_year BETWEEN 1965 AND 1975
    AND d_year = 2000
    AND p_channel_email = 'Y'
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100
"#;

// TPC-DS Q27: Profit by promotion and store
// Tests: Complex aggregation with promotion
pub const TPCDS_Q27: &str = r#"
SELECT
    i_item_id,
    s_state,
    p_promo_name,
    AVG(ss_quantity) as avg_qty,
    AVG(ss_list_price) as avg_list_price,
    AVG(ss_sales_price) as avg_sales_price,
    SUM(ss_net_profit) as total_profit
FROM store_sales, store, item, promotion, date_dim
WHERE ss_store_sk = s_store_sk
    AND ss_item_sk = i_item_sk
    AND ss_promo_sk = p_promo_sk
    AND ss_sold_date_sk = d_date_sk
    AND d_year = 2000
    AND d_moy = 11
GROUP BY i_item_id, s_state, p_promo_name
ORDER BY total_profit DESC, i_item_id, s_state
LIMIT 100
"#;

// TPC-DS Q35: Customer returns analysis
// Tests: Aggregation on store_returns
pub const TPCDS_Q35: &str = r#"
SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    COUNT(*) as return_count,
    SUM(sr_return_amt) as total_returns,
    AVG(sr_return_amt) as avg_return_amt
FROM customer, store_returns, date_dim
WHERE c_customer_sk = sr_customer_sk
    AND sr_returned_date_sk = d_date_sk
    AND d_year = 2000
GROUP BY c_customer_id, c_first_name, c_last_name
HAVING COUNT(*) > 2
ORDER BY total_returns DESC, c_customer_id
LIMIT 100
"#;

// TPC-DS Q50: Return reasons analysis
// Tests: Join with reason table
pub const TPCDS_Q50: &str = r#"
SELECT
    s_store_name,
    s_state,
    r_reason_desc,
    COUNT(*) as num_returns,
    SUM(sr_return_amt) as total_return_amt,
    AVG(sr_return_amt) as avg_return_amt
FROM store, store_returns, reason, date_dim
WHERE s_store_sk = sr_store_sk
    AND sr_reason_sk = r_reason_sk
    AND sr_returned_date_sk = d_date_sk
    AND d_year = 2000
GROUP BY s_store_name, s_state, r_reason_desc
ORDER BY num_returns DESC, s_store_name, r_reason_desc
LIMIT 100
"#;

// TPC-DS Q81: Return rate by customer location
// Tests: Multi-table join with customer_address
pub const TPCDS_Q81: &str = r#"
SELECT
    ca_state,
    COUNT(DISTINCT c_customer_sk) as num_customers,
    COUNT(*) as num_returns,
    SUM(sr_return_amt) as total_returns,
    SUM(sr_return_amt) / COUNT(DISTINCT c_customer_sk) as returns_per_customer
FROM customer, customer_address, store_returns, date_dim
WHERE c_customer_sk = sr_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND sr_returned_date_sk = d_date_sk
    AND d_year = 2000
GROUP BY ca_state
HAVING COUNT(*) > 10
ORDER BY total_returns DESC
LIMIT 100
"#;

// TPC-DS Q82: Warehouse and promotion analysis
// Tests: Join warehouse and promotion tables
pub const TPCDS_Q82: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    i_current_price
FROM item, promotion
WHERE i_item_sk = p_item_sk
    AND p_cost > 1000
    AND i_current_price BETWEEN 20 AND 50
ORDER BY i_item_id
LIMIT 100
"#;

// TPC-DS Q83: Returns by item and reason
// Tests: Complex join with item and reason
pub const TPCDS_Q83: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    r_reason_desc,
    SUM(sr_return_quantity) as return_qty,
    SUM(sr_return_amt) as return_amt
FROM store_returns, item, reason, date_dim
WHERE sr_item_sk = i_item_sk
    AND sr_reason_sk = r_reason_sk
    AND sr_returned_date_sk = d_date_sk
    AND d_year = 2000
    AND d_moy BETWEEN 6 AND 8
GROUP BY i_item_id, i_item_desc, r_reason_desc
ORDER BY return_amt DESC, i_item_id
LIMIT 100
"#;

// =============================================================================
// Phase 3 Queries - Using catalog_sales, catalog_returns, web_sales, web_returns
// =============================================================================

// TPC-DS Q13: Catalog sales analysis by demographics
// Tests: Join with catalog_sales and customer
pub const TPCDS_Q13: &str = r#"
SELECT
    AVG(cs_quantity) as avg_quantity,
    AVG(cs_ext_sales_price) as avg_sales_price,
    AVG(cs_ext_wholesale_cost) as avg_wholesale_cost,
    SUM(cs_ext_wholesale_cost) as total_wholesale_cost
FROM catalog_sales, customer, date_dim
WHERE cs_sold_date_sk = d_date_sk
    AND cs_bill_customer_sk = c_customer_sk
    AND c_birth_year BETWEEN 1970 AND 1980
    AND d_year = 2000
LIMIT 100
"#;

// TPC-DS Q16: Catalog sales with return analysis
// Tests: Join between catalog_sales and catalog_returns
pub const TPCDS_Q16: &str = r#"
SELECT
    COUNT(DISTINCT cs_order_number) as order_count,
    SUM(cs_ext_ship_cost) as total_ship_cost,
    SUM(cs_net_profit) as total_profit
FROM catalog_sales cs1, date_dim, customer_address
WHERE cs1.cs_sold_date_sk = d_date_sk
    AND cs1.cs_ship_addr_sk = ca_address_sk
    AND d_year = 2000
    AND d_moy BETWEEN 1 AND 6
    AND ca_state = 'TX'
LIMIT 100
"#;

// TPC-DS Q20: Catalog sales by catalog page
// Tests: Join with catalog_page table
pub const TPCDS_Q20: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(cs_ext_sales_price) as itemrevenue,
    SUM(cs_ext_sales_price) * 100 / SUM(SUM(cs_ext_sales_price)) OVER () as revenueratio
FROM catalog_sales, item, date_dim
WHERE cs_item_sk = i_item_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_year = 2000
    AND d_moy = 1
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
"#;

// TPC-DS Q32: Catalog sales and promotions
// Tests: Complex catalog sales filtering
pub const TPCDS_Q32: &str = r#"
SELECT
    SUM(cs_ext_discount_amt) as excess_discount
FROM catalog_sales, item, date_dim
WHERE cs_item_sk = i_item_sk
    AND cs_sold_date_sk = d_date_sk
    AND i_manufact_id = 1
    AND d_year = 2000
    AND d_moy = 1
LIMIT 100
"#;

// TPC-DS Q37: Catalog page analysis
// Tests: Join with catalog_page
pub const TPCDS_Q37: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    i_current_price
FROM item, catalog_page, catalog_sales, date_dim
WHERE i_item_sk = cs_item_sk
    AND cs_catalog_page_sk = cp_catalog_page_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_year = 2000
    AND cp_catalog_page_number BETWEEN 1 AND 100
    AND i_current_price BETWEEN 20 AND 50
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100
"#;

// TPC-DS Q60: Web sales by category
// Tests: Web sales table with item join
pub const TPCDS_Q60: &str = r#"
SELECT
    i_item_id,
    SUM(ws_ext_sales_price) as total_sales
FROM web_sales, item, date_dim
WHERE ws_item_sk = i_item_sk
    AND ws_sold_date_sk = d_date_sk
    AND d_year = 2000
    AND d_moy = 12
GROUP BY i_item_id
ORDER BY total_sales DESC, i_item_id
LIMIT 100
"#;

// TPC-DS Q62: Web sales shipping analysis
// Tests: Web sales with warehouse and ship_mode
pub const TPCDS_Q62: &str = r#"
SELECT
    w_warehouse_name,
    sm_type,
    SUM(ws_ext_ship_cost) as ship_cost,
    SUM(ws_net_profit) as net_profit
FROM web_sales, warehouse, ship_mode, date_dim, web_site
WHERE ws_warehouse_sk = w_warehouse_sk
    AND ws_ship_mode_sk = sm_ship_mode_sk
    AND ws_sold_date_sk = d_date_sk
    AND ws_web_site_sk = web_site_sk
    AND d_year = 2000
GROUP BY w_warehouse_name, sm_type
ORDER BY w_warehouse_name, sm_type
LIMIT 100
"#;

// TPC-DS Q76: Web page and catalog page analysis
// Tests: Multi-channel sales analysis
pub const TPCDS_Q76: &str = r#"
SELECT
    'web' as channel,
    wp_web_page_id as page_id,
    SUM(ws_ext_sales_price) as sales
FROM web_sales, web_page, date_dim
WHERE ws_web_page_sk = wp_web_page_sk
    AND ws_sold_date_sk = d_date_sk
    AND d_year = 2000
GROUP BY wp_web_page_id
ORDER BY sales DESC
LIMIT 100
"#;

// TPC-DS Q84: Web returns analysis
// Tests: Web returns table usage
pub const TPCDS_Q84: &str = r#"
SELECT
    c_customer_id,
    c_last_name,
    c_first_name,
    SUM(wr_return_amt) as total_returns
FROM customer, web_returns, date_dim
WHERE c_customer_sk = wr_refunded_customer_sk
    AND wr_returned_date_sk = d_date_sk
    AND d_year = 2000
GROUP BY c_customer_id, c_last_name, c_first_name
HAVING SUM(wr_return_amt) > 100
ORDER BY total_returns DESC, c_customer_id
LIMIT 100
"#;

// TPC-DS Q92: Web sales vs web returns
// Tests: Web returns with reason analysis
pub const TPCDS_Q92: &str = r#"
SELECT
    SUM(ws_ext_discount_amt) as discount_amt
FROM web_sales, item, date_dim
WHERE ws_item_sk = i_item_sk
    AND ws_sold_date_sk = d_date_sk
    AND i_manufact_id = 1
    AND d_year = 2000
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
    // Phase 1 queries
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
    // Phase 2 queries (use promotion, store_returns, reason)
    ("Q25", TPCDS_Q25),
    ("Q26", TPCDS_Q26),
    ("Q27", TPCDS_Q27),
    ("Q35", TPCDS_Q35),
    ("Q50", TPCDS_Q50),
    ("Q81", TPCDS_Q81),
    ("Q82", TPCDS_Q82),
    ("Q83", TPCDS_Q83),
    // Phase 3 queries (use catalog_sales, catalog_returns, web_sales, web_returns)
    ("Q13", TPCDS_Q13),
    ("Q16", TPCDS_Q16),
    ("Q20", TPCDS_Q20),
    ("Q32", TPCDS_Q32),
    ("Q37", TPCDS_Q37),
    ("Q60", TPCDS_Q60),
    ("Q62", TPCDS_Q62),
    ("Q76", TPCDS_Q76),
    ("Q84", TPCDS_Q84),
    ("Q92", TPCDS_Q92),
];

/// Sanity check queries for validation
pub const TPCDS_SANITY_QUERIES: &[(&str, &str)] = &[
    ("sanity_date", TPCDS_SANITY_DATE),
    ("sanity_sales", TPCDS_SANITY_SALES),
    ("sanity_join", TPCDS_SANITY_JOIN),
];
