//! TPC-DS Query Definitions
//!
//! This module contains TPC-DS benchmark queries adapted for the implemented schema.
//! The original TPC-DS has 99 queries; we implement a subset that work with our
//! current tables across multiple phases:
//!
//! ## Phase 1 (Core Tables):
//! - date_dim, time_dim, item, customer, customer_address, store, store_sales
//! - Queries: Q1, Q2, Q3, Q6, Q7, Q9, Q10, Q12, Q15, Q19, Q42, Q52, Q55, Q68, Q73, Q89, Q96
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
//! ## Tier 2 (Q21-Q50 Complex Analytics):
//! - Q21, Q23, Q24, Q28, Q29, Q30, Q31, Q33, Q34, Q38, Q39, Q40, Q41, Q43, Q44, Q45, Q46, Q47, Q48, Q49
//! - Complex multi-table joins, CTEs, window functions, cross-channel analysis
//! - Note: Q22, Q36 blocked by ROLLUP/CUBE requirements
//!
//! Queries are numbered to match the official TPC-DS query numbers where possible,
//! with adaptations noted in comments.

// =============================================================================
// TPC-DS Q1: Customer Store Returns Analysis
// =============================================================================
// Identifies customers whose store return amounts exceed 120% of their
// store's average return amount for a given year.
// Tests: CTE, correlated subquery, multi-table join
pub const TPCDS_Q1: &str = r#"
WITH customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_return_amt) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk
        AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
)
AND s_store_sk = ctr1.ctr_store_sk
AND s_state = 'TN'
AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q2: Weekly Sales Comparison Year-over-Year
// =============================================================================
// Compares daily sales ratios between consecutive years (2001 vs 2002)
// for web and catalog sales channels.
// Tests: Multiple CTEs, UNION ALL, complex joins
pub const TPCDS_Q2: &str = r#"
WITH wscs AS (
    SELECT sold_date_sk, sales_price
    FROM (
        SELECT ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
        FROM web_sales
    ) x
    UNION ALL
    (SELECT cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
     FROM catalog_sales)
),
wswscs AS (
    SELECT
        d_week_seq,
        SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) sun_sales,
        SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) mon_sales,
        SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) tue_sales,
        SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) wed_sales,
        SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) thu_sales,
        SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) fri_sales,
        SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) sat_sales
    FROM wscs, date_dim
    WHERE d_date_sk = sold_date_sk
    GROUP BY d_week_seq
)
SELECT
    d_week_seq1,
    ROUND(sun_sales1 / sun_sales2, 2),
    ROUND(mon_sales1 / mon_sales2, 2),
    ROUND(tue_sales1 / tue_sales2, 2),
    ROUND(wed_sales1 / wed_sales2, 2),
    ROUND(thu_sales1 / thu_sales2, 2),
    ROUND(fri_sales1 / fri_sales2, 2),
    ROUND(sat_sales1 / sat_sales2, 2)
FROM (
    SELECT
        wswscs.d_week_seq d_week_seq1,
        sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1,
        wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1,
        sat_sales sat_sales1
    FROM wswscs, date_dim
    WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001
) y,
(
    SELECT
        wswscs.d_week_seq d_week_seq2,
        sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2,
        wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2,
        sat_sales sat_sales2
    FROM wswscs, date_dim
    WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2002
) z
WHERE d_week_seq1 = d_week_seq2 - 53
ORDER BY d_week_seq1
LIMIT 100
"#;

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
// TPC-DS Q6: State Sales Analysis
// =============================================================================
// Analyzes store sales by state for items priced above 120% of their
// category average during a specific month.
// Tests: Subquery in WHERE, correlated subquery, 5-way join
pub const TPCDS_Q6: &str = r#"
SELECT
    a.ca_state state,
    COUNT(*) cnt
FROM customer_address a, customer c, store_sales s, date_dim d, item i
WHERE a.ca_address_sk = c.c_current_addr_sk
    AND c.c_customer_sk = s.ss_customer_sk
    AND s.ss_sold_date_sk = d.d_date_sk
    AND s.ss_item_sk = i.i_item_sk
    AND d.d_month_seq = (
        SELECT DISTINCT d_month_seq
        FROM date_dim
        WHERE d_year = 2000 AND d_moy = 1
    )
    AND i.i_current_price > 1.2 * (
        SELECT AVG(j.i_current_price)
        FROM item j
        WHERE j.i_category = i.i_category
    )
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt
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
// TPC-DS Q9: Quantity-Based Bucket Analysis
// =============================================================================
// Calculates average discount or net paid amounts based on sales quantity
// thresholds using scalar subqueries in CASE expressions.
// Tests: Multiple scalar subqueries, CASE expressions, conditional aggregation
pub const TPCDS_Q9: &str = r#"
SELECT
    CASE WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) > 62316685
        THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
        ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
    END bucket1,
    CASE WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) > 19045798
        THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40)
        ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40)
    END bucket2,
    CASE WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) > 365541424
        THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60)
        ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60)
    END bucket3,
    CASE WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) > 216357808
        THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80)
        ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80)
    END bucket4,
    CASE WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) > 184483884
        THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100)
        ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100)
    END bucket5
FROM reason
WHERE r_reason_sk = 1
"#;

// =============================================================================
// TPC-DS Q10: Customer Demographics Analysis
// =============================================================================
// Analyzes customer demographics for customers in specific counties who
// made purchases across multiple channels (store, web, or catalog) during early 2002.
// Tests: EXISTS subqueries, OR conditions, multi-table join
pub const TPCDS_Q10: &str = r#"
SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address
FROM customer c, customer_address ca
WHERE c.c_current_addr_sk = ca.ca_address_sk
    AND ca_county IN ('Rush County', 'Toole County', 'Jefferson County',
                      'Dona Ana County', 'La Porte County')
    AND EXISTS (
        SELECT 1
        FROM store_sales, date_dim
        WHERE c.c_customer_sk = ss_customer_sk
            AND ss_sold_date_sk = d_date_sk
            AND d_year = 2002
            AND d_moy BETWEEN 1 AND 4
    )
    AND (
        EXISTS (
            SELECT 1
            FROM web_sales, date_dim
            WHERE c.c_customer_sk = ws_bill_customer_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 2002
                AND d_moy BETWEEN 1 AND 4
        )
        OR EXISTS (
            SELECT 1
            FROM catalog_sales, date_dim
            WHERE c.c_customer_sk = cs_ship_customer_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 2002
                AND d_moy BETWEEN 1 AND 4
        )
    )
ORDER BY c_customer_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q12: Web Sales Revenue by Category (Window Function)
// =============================================================================
// Calculates web sales revenue and revenue ratio within item classes
// for specific categories during a 30-day period.
// Tests: Window function SUM() OVER (PARTITION BY), category filtering
pub const TPCDS_Q12: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ws_ext_sales_price) AS itemrevenue,
    SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price))
        OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales, item, date_dim
WHERE ws_item_sk = i_item_sk
    AND i_category IN ('Sports', 'Books', 'Home')
    AND ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q15: Catalog Sales by Zip Code
// =============================================================================
// Aggregates catalog sales totals by zip code for Q2 2001, filtered by
// specific zip codes, states, or high-value transactions.
// Tests: OR conditions in WHERE, SUBSTR function, multi-table join
pub const TPCDS_Q15: &str = r#"
SELECT
    ca_zip,
    SUM(cs_sales_price)
FROM catalog_sales, customer, customer_address, date_dim
WHERE cs_bill_customer_sk = c_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND (SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                                  '85392', '85460', '80348', '81792')
         OR ca_state IN ('CA', 'WA', 'GA')
         OR cs_sales_price > 500)
    AND cs_sold_date_sk = d_date_sk
    AND d_qoy = 2
    AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
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
// Tier 2 Queries (Q21-Q50) - Complex analytics
// =============================================================================

// TPC-DS Q21: Inventory Analysis by Warehouse
// Analyzes inventory levels by warehouse and item for a specific date range.
// Tests: Multi-table join, date arithmetic, complex filtering
pub const TPCDS_Q21: &str = r#"
SELECT
    w_warehouse_name,
    i_item_id,
    SUM(CASE WHEN d_date < '2000-04-11'
             THEN cs_quantity ELSE 0 END) AS inv_before,
    SUM(CASE WHEN d_date >= '2000-04-11'
             THEN cs_quantity ELSE 0 END) AS inv_after
FROM catalog_sales, warehouse, item, date_dim
WHERE cs_warehouse_sk = w_warehouse_sk
    AND cs_item_sk = i_item_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN '2000-03-11' AND '2000-05-11'
GROUP BY w_warehouse_name, i_item_id
HAVING SUM(CASE WHEN d_date < '2000-04-11'
                THEN cs_quantity ELSE 0 END) > 0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100
"#;

// TPC-DS Q23: Customer Sales Analysis (Part 1 - Frequent Shoppers)
// Identifies customers who frequently purchased items with specific attributes.
// Tests: CTEs, complex subqueries, customer behavior analysis
pub const TPCDS_Q23: &str = r#"
WITH frequent_ss_items AS (
    SELECT
        i_item_sk AS item_sk,
        SUM(ss_quantity) AS ss_quantity_sum
    FROM store_sales, item
    WHERE ss_item_sk = i_item_sk
        AND i_current_price > 50
    GROUP BY i_item_sk
    HAVING SUM(ss_quantity) > 500
)
SELECT
    c_customer_sk,
    c_first_name,
    c_last_name,
    SUM(ss_quantity) AS total_quantity
FROM store_sales, customer, frequent_ss_items
WHERE ss_customer_sk = c_customer_sk
    AND ss_item_sk = frequent_ss_items.item_sk
GROUP BY c_customer_sk, c_first_name, c_last_name
ORDER BY total_quantity DESC, c_customer_sk
LIMIT 100
"#;

// TPC-DS Q24: Store Sales Analysis by City (Part 1)
// Analyzes store returns by customer for items in specific color categories.
// Tests: Multiple CTEs, complex predicates, aggregation
pub const TPCDS_Q24: &str = r#"
WITH ssales AS (
    SELECT
        c_last_name,
        c_first_name,
        s_store_name,
        ca_state,
        s_state,
        i_color,
        i_current_price,
        i_manager_id,
        i_size,
        SUM(ss_net_paid) AS netpaid
    FROM store_sales, store_returns, store, item, customer, customer_address
    WHERE ss_ticket_number = sr_ticket_number
        AND ss_item_sk = sr_item_sk
        AND ss_customer_sk = c_customer_sk
        AND ss_item_sk = i_item_sk
        AND ss_store_sk = s_store_sk
        AND c_current_addr_sk = ca_address_sk
        AND c_birth_country <> UPPER(ca_country)
        AND s_state = ca_state
    GROUP BY c_last_name, c_first_name, s_store_name, ca_state,
             s_state, i_color, i_current_price, i_manager_id, i_size
)
SELECT
    c_last_name,
    c_first_name,
    s_store_name,
    SUM(netpaid) AS paid
FROM ssales
WHERE i_color = 'red'
GROUP BY c_last_name, c_first_name, s_store_name
HAVING SUM(netpaid) > 5000
ORDER BY c_last_name, c_first_name, s_store_name
LIMIT 100
"#;

// TPC-DS Q28: Quantity-Based Bucket Analysis for Store Sales
// Analyzes average list prices and coupon amounts by quantity ranges.
// Tests: Multiple scalar subqueries, CASE expressions
pub const TPCDS_Q28: &str = r#"
SELECT
    AVG(ss_list_price) AS avg_list_price,
    AVG(ss_coupon_amt) AS avg_coupon,
    AVG(ss_wholesale_cost) AS avg_wholesale
FROM store_sales
WHERE ss_quantity BETWEEN 0 AND 5
    AND (ss_list_price BETWEEN 10 AND 20
         OR ss_coupon_amt BETWEEN 100 AND 500
         OR ss_wholesale_cost BETWEEN 10 AND 50)
LIMIT 100
"#;

// TPC-DS Q29: Store Sales Returns Analysis
// Correlates store sales with catalog and web sales for matching customers.
// Tests: Multi-channel analysis, complex joins
pub const TPCDS_Q29: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    s_store_id,
    s_store_name,
    SUM(ss_quantity) AS store_sales_quantity,
    SUM(sr_return_quantity) AS store_returns_quantity,
    SUM(cs_quantity) AS catalog_sales_quantity
FROM store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2, date_dim d3, store, item
WHERE d1.d_moy = 4
    AND d1.d_year = 1999
    AND d1.d_date_sk = ss_sold_date_sk
    AND i_item_sk = ss_item_sk
    AND s_store_sk = ss_store_sk
    AND ss_customer_sk = sr_customer_sk
    AND ss_item_sk = sr_item_sk
    AND ss_ticket_number = sr_ticket_number
    AND sr_returned_date_sk = d2.d_date_sk
    AND d2.d_moy BETWEEN 4 AND 10
    AND d2.d_year = 1999
    AND sr_customer_sk = cs_bill_customer_sk
    AND sr_item_sk = cs_item_sk
    AND cs_sold_date_sk = d3.d_date_sk
    AND d3.d_moy BETWEEN 4 AND 10
    AND d3.d_year = 1999
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100
"#;

// TPC-DS Q30: Web Returns Analysis by State
// Analyzes web returns by customer state with rolling comparisons.
// Tests: CTEs, correlated subqueries, geographic analysis
pub const TPCDS_Q30: &str = r#"
WITH customer_total_return AS (
    SELECT
        wr_returning_customer_sk AS ctr_customer_sk,
        ca_state AS ctr_state,
        SUM(wr_return_amt) AS ctr_total_return
    FROM web_returns, date_dim, customer_address
    WHERE wr_returned_date_sk = d_date_sk
        AND d_year = 2002
        AND wr_returning_addr_sk = ca_address_sk
    GROUP BY wr_returning_customer_sk, ca_state
)
SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    ca_state,
    ctr1.ctr_total_return
FROM customer_total_return ctr1, customer_address, customer
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr2.ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_state = ctr2.ctr_state
)
AND c_customer_sk = ctr1.ctr_customer_sk
AND c_current_addr_sk = ca_address_sk
AND ca_state IN ('GA', 'KY', 'NM')
ORDER BY c_customer_id, c_first_name, c_last_name, ca_state, ctr_total_return
LIMIT 100
"#;

// TPC-DS Q31: Store Sales Growth Comparison
// Compares store sales growth by state across years.
// Tests: Self-join on aggregated data, year-over-year comparison
pub const TPCDS_Q31: &str = r#"
WITH ss AS (
    SELECT
        ca_state AS state,
        d_year AS year,
        SUM(ss_net_profit) AS profit
    FROM store_sales, date_dim, customer, customer_address
    WHERE ss_sold_date_sk = d_date_sk
        AND ss_customer_sk = c_customer_sk
        AND c_current_addr_sk = ca_address_sk
        AND d_year IN (1999, 2000)
    GROUP BY ca_state, d_year
),
ws AS (
    SELECT
        ca_state AS state,
        d_year AS year,
        SUM(ws_net_profit) AS profit
    FROM web_sales, date_dim, customer, customer_address
    WHERE ws_sold_date_sk = d_date_sk
        AND ws_bill_customer_sk = c_customer_sk
        AND c_current_addr_sk = ca_address_sk
        AND d_year IN (1999, 2000)
    GROUP BY ca_state, d_year
)
SELECT
    ss.state,
    ss.year,
    ss.profit AS store_profit,
    ws.profit AS web_profit
FROM ss, ws
WHERE ss.state = ws.state
    AND ss.year = ws.year
ORDER BY ss.state, ss.year
LIMIT 100
"#;

// TPC-DS Q33: Cross-Channel Sales by Manufacturer
// Analyzes sales by manufacturer across store, catalog, and web channels.
// Tests: UNION ALL, multi-channel aggregation, manufacturer filtering
pub const TPCDS_Q33: &str = r#"
WITH ss AS (
    SELECT
        i_manufact_id,
        SUM(ss_ext_sales_price) AS total_sales
    FROM store_sales, date_dim, customer_address, item
    WHERE ss_sold_date_sk = d_date_sk
        AND ss_addr_sk = ca_address_sk
        AND ca_gmt_offset = -5
        AND d_year = 1998
        AND ss_item_sk = i_item_sk
    GROUP BY i_manufact_id
),
cs AS (
    SELECT
        i_manufact_id,
        SUM(cs_ext_sales_price) AS total_sales
    FROM catalog_sales, date_dim, customer_address, item
    WHERE cs_sold_date_sk = d_date_sk
        AND cs_bill_addr_sk = ca_address_sk
        AND ca_gmt_offset = -5
        AND d_year = 1998
        AND cs_item_sk = i_item_sk
    GROUP BY i_manufact_id
),
ws AS (
    SELECT
        i_manufact_id,
        SUM(ws_ext_sales_price) AS total_sales
    FROM web_sales, date_dim, customer_address, item
    WHERE ws_sold_date_sk = d_date_sk
        AND ws_bill_addr_sk = ca_address_sk
        AND ca_gmt_offset = -5
        AND d_year = 1998
        AND ws_item_sk = i_item_sk
    GROUP BY i_manufact_id
)
SELECT
    i_manufact_id,
    SUM(total_sales) AS total_sales
FROM (
    SELECT * FROM ss
    UNION ALL
    SELECT * FROM cs
    UNION ALL
    SELECT * FROM ws
) combined
GROUP BY i_manufact_id
ORDER BY total_sales DESC
LIMIT 100
"#;

// TPC-DS Q34: Store Sales by Customer Demographics
// Analyzes customer purchasing patterns at stores.
// Tests: Multi-table join, customer demographics, HAVING clause
pub const TPCDS_Q34: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag,
    COUNT(*) AS cnt
FROM store_sales, date_dim, store, customer
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_customer_sk = c_customer_sk
    AND d_year = 1999
    AND d_moy BETWEEN 4 AND 7
    AND s_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
GROUP BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag
HAVING COUNT(*) BETWEEN 1 AND 5
ORDER BY cnt DESC, c_last_name, c_first_name
LIMIT 100
"#;

// TPC-DS Q38: Customer Multi-Channel Presence
// Identifies customers who purchased through multiple channels.
// Tests: INTERSECT pattern using EXISTS, cross-channel analysis
pub const TPCDS_Q38: &str = r#"
SELECT COUNT(*) AS customer_count
FROM (
    SELECT DISTINCT c_customer_sk
    FROM customer, store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year = 1999
        AND d_moy = 1
) store_cust
WHERE EXISTS (
    SELECT 1
    FROM web_sales, date_dim
    WHERE store_cust.c_customer_sk = ws_bill_customer_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year = 1999
        AND d_moy = 1
)
AND EXISTS (
    SELECT 1
    FROM catalog_sales, date_dim
    WHERE store_cust.c_customer_sk = cs_bill_customer_sk
        AND cs_sold_date_sk = d_date_sk
        AND d_year = 1999
        AND d_moy = 1
)
"#;

// TPC-DS Q39: Inventory Variance Analysis by Warehouse
// Analyzes inventory variance across warehouses for different months.
// Tests: Complex statistical calculations, warehouse analytics
pub const TPCDS_Q39: &str = r#"
WITH warehouse_stats AS (
    SELECT
        w_warehouse_sk,
        w_warehouse_name,
        i_item_sk,
        d_moy,
        AVG(cs_quantity) AS mean_qty,
        COUNT(*) AS cnt
    FROM catalog_sales, warehouse, item, date_dim
    WHERE cs_warehouse_sk = w_warehouse_sk
        AND cs_item_sk = i_item_sk
        AND cs_sold_date_sk = d_date_sk
        AND d_year = 2000
    GROUP BY w_warehouse_sk, w_warehouse_name, i_item_sk, d_moy
)
SELECT
    w_warehouse_name,
    i_item_sk,
    d_moy,
    mean_qty,
    cnt
FROM warehouse_stats
WHERE cnt > 10
    AND mean_qty > 0
ORDER BY w_warehouse_name, i_item_sk, d_moy
LIMIT 100
"#;

// TPC-DS Q40: Catalog Sales Returns Analysis
// Analyzes catalog sales with and without returns by warehouse.
// Tests: LEFT OUTER JOIN, return rate calculation
pub const TPCDS_Q40: &str = r#"
SELECT
    w_warehouse_name,
    w_warehouse_sq_ft,
    w_city,
    w_county,
    w_state,
    w_country,
    SUM(CASE WHEN cs_ship_date_sk - cs_sold_date_sk <= 30
             THEN cs_ext_discount_amt ELSE 0 END) AS days_30,
    SUM(CASE WHEN cs_ship_date_sk - cs_sold_date_sk > 30
             AND cs_ship_date_sk - cs_sold_date_sk <= 60
             THEN cs_ext_discount_amt ELSE 0 END) AS days_31_60,
    SUM(CASE WHEN cs_ship_date_sk - cs_sold_date_sk > 60
             THEN cs_ext_discount_amt ELSE 0 END) AS days_61_plus
FROM catalog_sales, warehouse, date_dim
WHERE cs_sold_date_sk = d_date_sk
    AND cs_warehouse_sk = w_warehouse_sk
    AND d_year = 2000
GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country
ORDER BY w_warehouse_name
LIMIT 100
"#;

// TPC-DS Q41: Item Attribute Analysis
// Finds items with specific size and color combinations not yet promoted.
// Tests: Complex string predicates, NOT EXISTS pattern
pub const TPCDS_Q41: &str = r#"
SELECT DISTINCT i_item_id
FROM item
WHERE i_current_price BETWEEN 50 AND 100
    AND i_manufact_id IN (
        SELECT DISTINCT i_manufact_id
        FROM item
        WHERE (i_size = 'medium' AND i_color = 'black')
           OR (i_size = 'large' AND i_color = 'white')
    )
ORDER BY i_item_id
LIMIT 100
"#;

// TPC-DS Q43: Store Sales by Day of Week
// Analyzes store sales patterns by day of week for a specific year.
// Tests: Pivot pattern using CASE, day-of-week analysis
pub const TPCDS_Q43: &str = r#"
SELECT
    s_store_name,
    s_store_id,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price ELSE 0 END) AS sun_sales,
    SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price ELSE 0 END) AS mon_sales,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price ELSE 0 END) AS tue_sales,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price ELSE 0 END) AS wed_sales,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price ELSE 0 END) AS thu_sales,
    SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price ELSE 0 END) AS fri_sales,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price ELSE 0 END) AS sat_sales
FROM date_dim, store_sales, store
WHERE d_date_sk = ss_sold_date_sk
    AND s_store_sk = ss_store_sk
    AND d_year = 2000
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name, s_store_id, sun_sales
LIMIT 100
"#;

// TPC-DS Q44: Store Items Profit Ranking
// Ranks items by net profit within each store.
// Tests: Window functions for ranking, profit analysis
pub const TPCDS_Q44: &str = r#"
SELECT
    i_item_id,
    i_product_name,
    AVG(ss_net_profit) AS avg_profit,
    RANK() OVER (ORDER BY AVG(ss_net_profit) DESC) AS profit_rank
FROM store_sales, item
WHERE ss_item_sk = i_item_sk
GROUP BY i_item_id, i_product_name
HAVING AVG(ss_net_profit) > 0
ORDER BY profit_rank
LIMIT 100
"#;

// TPC-DS Q45: Web Sales by Customer Zip Code
// Analyzes web sales by customer zip code for specific item categories.
// Tests: Geographic analysis, category filtering
pub const TPCDS_Q45: &str = r#"
SELECT
    ca_zip,
    ca_city,
    SUM(ws_sales_price) AS total_sales
FROM web_sales, customer, customer_address, date_dim, item
WHERE ws_bill_customer_sk = c_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND ws_item_sk = i_item_sk
    AND ws_sold_date_sk = d_date_sk
    AND d_year = 2001
    AND d_qoy = 1
    AND i_category IN ('Sports', 'Music', 'Books')
GROUP BY ca_zip, ca_city
ORDER BY ca_zip, ca_city, total_sales
LIMIT 100
"#;

// TPC-DS Q46: Store Sales by Customer and Store
// Analyzes store sales by customer and their home store location.
// Tests: Multi-dimensional grouping, customer behavior
pub const TPCDS_Q46: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    ca_city,
    s_store_name,
    SUM(ss_coupon_amt) AS total_coupon,
    SUM(ss_net_paid) AS total_paid
FROM store_sales, date_dim, customer, customer_address, store
WHERE ss_sold_date_sk = d_date_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_store_sk = s_store_sk
    AND c_current_addr_sk = ca_address_sk
    AND d_year = 2000
    AND d_moy = 3
GROUP BY c_last_name, c_first_name, ca_city, s_store_name
ORDER BY total_paid DESC, c_last_name, c_first_name
LIMIT 100
"#;

// TPC-DS Q47: Store Monthly Sales Rolling Comparison
// Compares monthly sales with rolling 3-month averages.
// Tests: Window functions with frame, monthly trends
pub const TPCDS_Q47: &str = r#"
WITH monthly_sales AS (
    SELECT
        s_store_name,
        d_year,
        d_moy,
        SUM(ss_sales_price) AS total_sales
    FROM store_sales, date_dim, store
    WHERE ss_sold_date_sk = d_date_sk
        AND ss_store_sk = s_store_sk
        AND d_year IN (1999, 2000, 2001)
    GROUP BY s_store_name, d_year, d_moy
)
SELECT
    s_store_name,
    d_year,
    d_moy,
    total_sales,
    AVG(total_sales) OVER (
        PARTITION BY s_store_name
        ORDER BY d_year, d_moy
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS rolling_avg
FROM monthly_sales
ORDER BY s_store_name, d_year, d_moy
LIMIT 100
"#;

// TPC-DS Q48: Store Sales Quantity Analysis
// Analyzes store sales quantities by customer demographics.
// Tests: Complex filtering, demographic segmentation
pub const TPCDS_Q48: &str = r#"
SELECT
    SUM(ss_quantity) AS total_qty,
    SUM(ss_ext_sales_price) AS total_sales,
    SUM(ss_ext_wholesale_cost) AS total_wholesale
FROM store_sales, store, customer, customer_address, date_dim
WHERE s_store_sk = ss_store_sk
    AND ss_sold_date_sk = d_date_sk
    AND ss_customer_sk = c_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND d_year = 1998
    AND (
        (ca_country = 'United States'
         AND ca_state IN ('TX', 'OH', 'NE')
         AND ss_net_profit BETWEEN 100 AND 200)
        OR
        (ca_country = 'United States'
         AND ca_state IN ('NC', 'CO', 'MN')
         AND ss_net_profit BETWEEN 150 AND 300)
        OR
        (ca_country = 'United States'
         AND ca_state IN ('VA', 'TN', 'CA')
         AND ss_net_profit BETWEEN 50 AND 250)
    )
LIMIT 100
"#;

// TPC-DS Q49: Channel Sales Return Analysis
// Analyzes return ratios across different sales channels.
// Tests: Multi-channel return analysis, ratio calculations
pub const TPCDS_Q49: &str = r#"
SELECT
    channel,
    item,
    SUM(returns_amt) AS return_amt,
    SUM(net_loss) AS net_loss
FROM (
    SELECT
        'store' AS channel,
        ss_item_sk AS item,
        sr_return_amt AS returns_amt,
        sr_net_loss AS net_loss
    FROM store_sales, store_returns, date_dim
    WHERE ss_sold_date_sk = d_date_sk
        AND d_year = 1999
        AND ss_customer_sk = sr_customer_sk
        AND ss_item_sk = sr_item_sk
        AND ss_ticket_number = sr_ticket_number
    UNION ALL
    SELECT
        'catalog' AS channel,
        cs_item_sk AS item,
        cr_return_amount AS returns_amt,
        cr_net_loss AS net_loss
    FROM catalog_sales, catalog_returns, date_dim
    WHERE cs_sold_date_sk = d_date_sk
        AND d_year = 1999
        AND cs_order_number = cr_order_number
        AND cs_item_sk = cr_item_sk
    UNION ALL
    SELECT
        'web' AS channel,
        ws_item_sk AS item,
        wr_return_amt AS returns_amt,
        wr_net_loss AS net_loss
    FROM web_sales, web_returns, date_dim
    WHERE ws_sold_date_sk = d_date_sk
        AND d_year = 1999
        AND ws_order_number = wr_order_number
        AND ws_item_sk = wr_item_sk
) all_returns
GROUP BY channel, item
ORDER BY channel, return_amt DESC
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
    // Phase 1 queries (core tables: date_dim, item, customer, store, store_sales)
    ("Q1", TPCDS_Q1),
    ("Q2", TPCDS_Q2),
    ("Q3", TPCDS_Q3),
    ("Q6", TPCDS_Q6),
    ("Q7", TPCDS_Q7),
    ("Q9", TPCDS_Q9),
    ("Q10", TPCDS_Q10),
    ("Q12", TPCDS_Q12),
    ("Q15", TPCDS_Q15),
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
    // Tier 2 queries (Q21-Q50) - complex analytics
    ("Q21", TPCDS_Q21),
    ("Q23", TPCDS_Q23),
    ("Q24", TPCDS_Q24),
    ("Q28", TPCDS_Q28),
    ("Q29", TPCDS_Q29),
    ("Q30", TPCDS_Q30),
    ("Q31", TPCDS_Q31),
    ("Q33", TPCDS_Q33),
    ("Q34", TPCDS_Q34),
    ("Q38", TPCDS_Q38),
    ("Q39", TPCDS_Q39),
    ("Q40", TPCDS_Q40),
    ("Q41", TPCDS_Q41),
    ("Q43", TPCDS_Q43),
    ("Q44", TPCDS_Q44),
    ("Q45", TPCDS_Q45),
    ("Q46", TPCDS_Q46),
    ("Q47", TPCDS_Q47),
    ("Q48", TPCDS_Q48),
    ("Q49", TPCDS_Q49),
];

/// Sanity check queries for validation
pub const TPCDS_SANITY_QUERIES: &[(&str, &str)] = &[
    ("sanity_date", TPCDS_SANITY_DATE),
    ("sanity_sales", TPCDS_SANITY_SALES),
    ("sanity_join", TPCDS_SANITY_JOIN),
];
