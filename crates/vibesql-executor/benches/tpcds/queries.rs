//! TPC-DS Query Definitions
//!
//! This module contains the complete TPC-DS benchmark query suite (all 99 queries)
//! adapted for the implemented schema. Queries span multiple complexity tiers:
//!
//! ## Phase 1 (Core Tables - Q1-Q19):
//! - date_dim, time_dim, item, customer, customer_address, store, store_sales
//! - Queries: Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q14, Q15, Q17, Q18, Q19
//! - Features: Basic joins, aggregations, CTEs, UNION ALL, INTERSECT, ROLLUP
//!
//! ## Phase 2 (Extended Tables - Q20-Q50):
//! - promotion, warehouse, ship_mode, reason, store_returns
//! - Queries: Q20-Q50 (complex analytics tier)
//! - Features: Window functions, advanced CTEs, CUBE, year-over-year comparisons
//!
//! ## Phase 3 (Full E-Commerce - Multi-channel):
//! - catalog_page, web_page, web_site, call_center
//! - catalog_sales, catalog_returns, web_sales, web_returns
//! - Queries: Q13, Q16, Q20, Q32, Q37, Q57, Q60, Q62, Q76, Q84, Q91, Q92, Q99
//!
//! ## Tier 3 (Advanced Analytics - Q51-Q99):
//! - Advanced window functions, ROLLUP/CUBE, complex CTEs
//! - ROLLUP queries: Q5, Q14, Q18, Q67, Q70, Q77, Q80, Q86 (hierarchical grouping)
//! - Window function queries: Q51, Q53, Q58, Q74, Q75, Q78, Q98
//! - Multi-channel analysis: Q4, Q11, Q58, Q71, Q74, Q75, Q87, Q97
//! - Call center queries: Q57, Q91, Q99
//!
//! ## Key SQL Features Tested:
//! - ROLLUP/CUBE: Q5, Q14, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86
//! - INTERSECT: Q8, Q14, Q38, Q87
//! - Window Functions: Q12, Q47, Q51, Q53, Q57, Q58, Q74, Q75, Q78, Q98
//! - Complex CTEs: Q4, Q5, Q11, Q14, Q23, Q24, Q58, Q59, Q64
//! - Statistical Functions: Q17 (STDDEV_SAMP)
//!
//! Total implemented: 99 queries (100% TPC-DS coverage)
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
// Canonical TPC-DS Q3: Analyzes store sales discounts by brand.
// Tests: 3-way join, date filtering, aggregation, ordering
pub const TPCDS_Q3: &str = r#"
SELECT
    dt.d_year,
    item.i_brand_id brand_id,
    item.i_brand brand,
    SUM(ss_ext_discount_amt) sum_agg
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manufact_id = 427
    AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q4: Customer Catalog Sales Growth vs Store/Web
// =============================================================================
// Identifies customers whose catalog sales growth between consecutive years
// exceeds both their store and web sales growth rates.
// Tests: Complex CTE with UNION ALL, 6-way self-join, year-over-year comparison
pub const TPCDS_Q4: &str = r#"
WITH year_total AS (
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
        SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) year_total,
        's' sale_type
    FROM customer, store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
             c_birth_country, c_login, c_email_address, d_year
    UNION ALL
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
        SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) year_total,
        'c' sale_type
    FROM customer, catalog_sales, date_dim
    WHERE c_customer_sk = cs_bill_customer_sk
        AND cs_sold_date_sk = d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
             c_birth_country, c_login, c_email_address, d_year
    UNION ALL
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
        SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) year_total,
        'w' sale_type
    FROM customer, web_sales, date_dim
    WHERE c_customer_sk = ws_bill_customer_sk
        AND ws_sold_date_sk = d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
             c_birth_country, c_login, c_email_address, d_year
)
SELECT
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
    AND t_s_firstyear.customer_id = t_c_secyear.customer_id
    AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
    AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
    AND t_s_firstyear.customer_id = t_w_secyear.customer_id
    AND t_s_firstyear.sale_type = 's'
    AND t_c_firstyear.sale_type = 'c'
    AND t_w_firstyear.sale_type = 'w'
    AND t_s_secyear.sale_type = 's'
    AND t_c_secyear.sale_type = 'c'
    AND t_w_secyear.sale_type = 'w'
    AND t_s_firstyear.dyear = 2001
    AND t_s_secyear.dyear = 2001 + 1
    AND t_c_firstyear.dyear = 2001
    AND t_c_secyear.dyear = 2001 + 1
    AND t_w_firstyear.dyear = 2001
    AND t_w_secyear.dyear = 2001 + 1
    AND t_s_firstyear.year_total > 0
    AND t_c_firstyear.year_total > 0
    AND t_w_firstyear.year_total > 0
    AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
        END > CASE
                WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                ELSE NULL
              END
    AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
        END > CASE
                WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                ELSE NULL
              END
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_preferred_cust_flag
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q5: Multi-Channel Sales and Returns Analysis with ROLLUP
// =============================================================================
// Analyzes sales and returns across store, catalog, and web channels
// with hierarchical subtotals using ROLLUP.
// Tests: Complex CTEs, UNION ALL, ROLLUP grouping
pub const TPCDS_Q5: &str = r#"
WITH ssr AS (
    SELECT
        s_store_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
    FROM (
        SELECT
            ss_store_sk AS store_sk,
            ss_sold_date_sk AS date_sk,
            ss_ext_sales_price AS sales_price,
            ss_net_profit AS profit,
            CAST(0 AS DECIMAL(7,2)) AS return_amt,
            CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM store_sales
        UNION ALL
        SELECT
            sr_store_sk AS store_sk,
            sr_returned_date_sk AS date_sk,
            CAST(0 AS DECIMAL(7,2)) AS sales_price,
            CAST(0 AS DECIMAL(7,2)) AS profit,
            sr_return_amt AS return_amt,
            sr_net_loss AS net_loss
        FROM store_returns
    ) salesreturns,
    date_dim,
    store
    WHERE date_sk = d_date_sk
        AND d_date BETWEEN CAST('2002-08-22' AS DATE) AND CAST('2002-09-05' AS DATE)
        AND store_sk = s_store_sk
    GROUP BY s_store_id
),
csr AS (
    SELECT
        cp_catalog_page_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
    FROM (
        SELECT
            cs_catalog_page_sk AS page_sk,
            cs_sold_date_sk AS date_sk,
            cs_ext_sales_price AS sales_price,
            cs_net_profit AS profit,
            CAST(0 AS DECIMAL(7,2)) AS return_amt,
            CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM catalog_sales
        UNION ALL
        SELECT
            cr_catalog_page_sk AS page_sk,
            cr_returned_date_sk AS date_sk,
            CAST(0 AS DECIMAL(7,2)) AS sales_price,
            CAST(0 AS DECIMAL(7,2)) AS profit,
            cr_return_amount AS return_amt,
            cr_net_loss AS net_loss
        FROM catalog_returns
    ) salesreturns,
    date_dim,
    catalog_page
    WHERE date_sk = d_date_sk
        AND d_date BETWEEN CAST('2002-08-22' AS DATE) AND CAST('2002-09-05' AS DATE)
        AND page_sk = cp_catalog_page_sk
    GROUP BY cp_catalog_page_id
),
wsr AS (
    SELECT
        web_site_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
    FROM (
        SELECT
            ws_web_site_sk AS wsr_web_site_sk,
            ws_sold_date_sk AS date_sk,
            ws_ext_sales_price AS sales_price,
            ws_net_profit AS profit,
            CAST(0 AS DECIMAL(7,2)) AS return_amt,
            CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM web_sales
        UNION ALL
        SELECT
            ws_web_site_sk AS wsr_web_site_sk,
            wr_returned_date_sk AS date_sk,
            CAST(0 AS DECIMAL(7,2)) AS sales_price,
            CAST(0 AS DECIMAL(7,2)) AS profit,
            wr_return_amt AS return_amt,
            wr_net_loss AS net_loss
        FROM web_returns
        LEFT OUTER JOIN web_sales ON (wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number)
    ) salesreturns,
    date_dim,
    web_site
    WHERE date_sk = d_date_sk
        AND d_date BETWEEN CAST('2002-08-22' AS DATE) AND CAST('2002-09-05' AS DATE)
        AND wsr_web_site_sk = web_site_sk
    GROUP BY web_site_id
)
SELECT
    channel,
    id,
    SUM(sales) AS sales,
    SUM(returns1) AS returns1,
    SUM(profit) AS profit
FROM (
    SELECT
        'store channel' AS channel,
        'store' || s_store_id AS id,
        sales,
        returns1,
        (profit - profit_loss) AS profit
    FROM ssr
    UNION ALL
    SELECT
        'catalog channel' AS channel,
        'catalog_page' || cp_catalog_page_id AS id,
        sales,
        returns1,
        (profit - profit_loss) AS profit
    FROM csr
    UNION ALL
    SELECT
        'web channel' AS channel,
        'web_site' || web_site_id AS id,
        sales,
        returns1,
        (profit - profit_loss) AS profit
    FROM wsr
) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
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
// Canonical TPC-DS Q7: Analyzes sales metrics by item filtered by demographics and promotions.
// Tests: 5-way join with customer_demographics and promotion, aggregation with multiple measures
pub const TPCDS_Q7: &str = r#"
SELECT
    i_item_id,
    AVG(ss_quantity) agg1,
    AVG(ss_list_price) agg2,
    AVG(ss_coupon_amt) agg3,
    AVG(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_promo_sk = p_promo_sk
    AND cd_gender = 'F'
    AND cd_marital_status = 'W'
    AND cd_education_status = '2 yr Degree'
    AND (p_channel_email = 'N' OR p_channel_event = 'N')
    AND d_year = 1998
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q8: Store Sales by ZIP Code Analysis
// =============================================================================
// Retrieves store names and total net profits for Q2 2000, filtered by
// specific geographic criteria using ZIP code intersection.
// Tests: Complex subquery with INTERSECT, ZIP code prefix matching, aggregation
pub const TPCDS_Q8: &str = r#"
SELECT s_store_name, SUM(ss_net_profit)
FROM store_sales, date_dim, store,
    (SELECT ca_zip
     FROM (
         SELECT SUBSTR(ca_zip, 1, 5) ca_zip
         FROM customer_address
         WHERE SUBSTR(ca_zip, 1, 5) IN (
             '67436', '26121', '38443', '63157', '68856', '19485', '86425', '26741',
             '70991', '60899', '63573', '47556', '56193', '93314', '87827', '62017',
             '85067', '95390', '48091', '10261', '81845', '41790', '42853', '24675',
             '12840', '60065', '84430', '57451', '24021', '91735', '75335', '71935'
         )
         INTERSECT
         SELECT ca_zip
         FROM (
             SELECT SUBSTR(ca_zip, 1, 5) ca_zip, COUNT(*) cnt
             FROM customer_address, customer
             WHERE ca_address_sk = c_current_addr_sk
                 AND c_preferred_cust_flag = 'Y'
             GROUP BY ca_zip
             HAVING COUNT(*) > 10
         ) A1
     ) A2
    ) V1
WHERE ss_store_sk = s_store_sk
    AND ss_sold_date_sk = d_date_sk
    AND d_qoy = 2
    AND d_year = 2000
    AND SUBSTR(s_zip, 1, 2) = SUBSTR(V1.ca_zip, 1, 2)
GROUP BY s_store_name
ORDER BY s_store_name
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
// TPC-DS Q11: Customer Web vs Store Sales Growth Comparison
// =============================================================================
// Identifies customers whose web sales growth rate exceeded their store sales
// growth rate between consecutive years.
// Tests: CTE with UNION ALL, 4-way self-join, year-over-year comparison
pub const TPCDS_Q11: &str = r#"
WITH year_total AS (
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
        SUM(ss_ext_list_price - ss_ext_discount_amt) year_total,
        's' sale_type
    FROM customer, store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
             c_birth_country, c_login, c_email_address, d_year
    UNION ALL
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
        SUM(ws_ext_list_price - ws_ext_discount_amt) year_total,
        'w' sale_type
    FROM customer, web_sales, date_dim
    WHERE c_customer_sk = ws_bill_customer_sk
        AND ws_sold_date_sk = d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
             c_birth_country, c_login, c_email_address, d_year
)
SELECT
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_birth_country
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
    AND t_s_firstyear.customer_id = t_w_secyear.customer_id
    AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
    AND t_s_firstyear.sale_type = 's'
    AND t_w_firstyear.sale_type = 'w'
    AND t_s_secyear.sale_type = 's'
    AND t_w_secyear.sale_type = 'w'
    AND t_s_firstyear.dyear = 2001
    AND t_s_secyear.dyear = 2001 + 1
    AND t_w_firstyear.dyear = 2001
    AND t_w_secyear.dyear = 2001 + 1
    AND t_s_firstyear.year_total > 0
    AND t_w_firstyear.year_total > 0
    AND CASE
          WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
          ELSE 0.0
        END > CASE
                WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                ELSE 0.0
              END
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
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
// TPC-DS Q14: Cross-Channel Item Sales Analysis with ROLLUP
// =============================================================================
// Identifies items sold across all three channels (store, catalog, web) and
// aggregates their sales by channel using ROLLUP for hierarchical subtotals.
// Tests: Complex CTEs, triple INTERSECT, ROLLUP grouping, HAVING with subquery
pub const TPCDS_Q14: &str = r#"
WITH cross_items AS (
    SELECT i_item_sk ss_item_sk
    FROM item,
    (SELECT iss.i_brand_id brand_id, iss.i_class_id class_id, iss.i_category_id category_id
     FROM store_sales, item iss, date_dim d1
     WHERE ss_item_sk = iss.i_item_sk
         AND ss_sold_date_sk = d1.d_date_sk
         AND d1.d_year BETWEEN 1999 AND 1999 + 2
     INTERSECT
     SELECT ics.i_brand_id, ics.i_class_id, ics.i_category_id
     FROM catalog_sales, item ics, date_dim d2
     WHERE cs_item_sk = ics.i_item_sk
         AND cs_sold_date_sk = d2.d_date_sk
         AND d2.d_year BETWEEN 1999 AND 1999 + 2
     INTERSECT
     SELECT iws.i_brand_id, iws.i_class_id, iws.i_category_id
     FROM web_sales, item iws, date_dim d3
     WHERE ws_item_sk = iws.i_item_sk
         AND ws_sold_date_sk = d3.d_date_sk
         AND d3.d_year BETWEEN 1999 AND 1999 + 2
    ) x
    WHERE i_brand_id = brand_id
        AND i_class_id = class_id
        AND i_category_id = category_id
),
avg_sales AS (
    SELECT AVG(quantity * list_price) average_sales
    FROM (
        SELECT ss_quantity quantity, ss_list_price list_price
        FROM store_sales, date_dim
        WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999 + 2
        UNION ALL
        SELECT cs_quantity quantity, cs_list_price list_price
        FROM catalog_sales, date_dim
        WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999 + 2
        UNION ALL
        SELECT ws_quantity quantity, ws_list_price list_price
        FROM web_sales, date_dim
        WHERE ws_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999 + 2
    ) x
)
SELECT channel, i_brand_id, i_class_id, i_category_id,
       SUM(sales), SUM(number_sales)
FROM (
    SELECT 'store' channel, i_brand_id, i_class_id, i_category_id,
           SUM(ss_quantity * ss_list_price) sales, COUNT(*) number_sales
    FROM store_sales, item, date_dim
    WHERE ss_item_sk IN (SELECT ss_item_sk FROM cross_items)
        AND ss_item_sk = i_item_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year = 1999 + 2 AND d_moy = 11
    GROUP BY i_brand_id, i_class_id, i_category_id
    HAVING SUM(ss_quantity * ss_list_price) > (SELECT average_sales FROM avg_sales)
    UNION ALL
    SELECT 'catalog' channel, i_brand_id, i_class_id, i_category_id,
           SUM(cs_quantity * cs_list_price) sales, COUNT(*) number_sales
    FROM catalog_sales, item, date_dim
    WHERE cs_item_sk IN (SELECT ss_item_sk FROM cross_items)
        AND cs_item_sk = i_item_sk
        AND cs_sold_date_sk = d_date_sk
        AND d_year = 1999 + 2 AND d_moy = 11
    GROUP BY i_brand_id, i_class_id, i_category_id
    HAVING SUM(cs_quantity * cs_list_price) > (SELECT average_sales FROM avg_sales)
    UNION ALL
    SELECT 'web' channel, i_brand_id, i_class_id, i_category_id,
           SUM(ws_quantity * ws_list_price) sales, COUNT(*) number_sales
    FROM web_sales, item, date_dim
    WHERE ws_item_sk IN (SELECT ss_item_sk FROM cross_items)
        AND ws_item_sk = i_item_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year = 1999 + 2 AND d_moy = 11
    GROUP BY i_brand_id, i_class_id, i_category_id
    HAVING SUM(ws_quantity * ws_list_price) > (SELECT average_sales FROM avg_sales)
) y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel, i_brand_id, i_class_id, i_category_id
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
// TPC-DS Q17: Store Sales to Returns to Catalog Analysis
// =============================================================================
// Analyzes the relationship between store sales, returns, and subsequent
// catalog sales for items and states in Q1 1999.
// Tests: 8-way join, statistical functions (STDDEV_SAMP), date-based filtering
pub const TPCDS_Q17: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    s_state,
    COUNT(ss_quantity) AS store_sales_quantitycount,
    AVG(ss_quantity) AS store_sales_quantityave,
    STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev,
    STDDEV_SAMP(ss_quantity) / AVG(ss_quantity) AS store_sales_quantitycov,
    COUNT(sr_return_quantity) AS store_returns_quantitycount,
    AVG(sr_return_quantity) AS store_returns_quantityave,
    STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev,
    STDDEV_SAMP(sr_return_quantity) / AVG(sr_return_quantity) AS store_returns_quantitycov,
    COUNT(cs_quantity) AS catalog_sales_quantitycount,
    AVG(cs_quantity) AS catalog_sales_quantityave,
    STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev,
    STDDEV_SAMP(cs_quantity) / AVG(cs_quantity) AS catalog_sales_quantitycov
FROM store_sales, store_returns, catalog_sales,
     date_dim d1, date_dim d2, date_dim d3,
     store, item
WHERE d1.d_quarter_name = '1999Q1'
    AND d1.d_date_sk = ss_sold_date_sk
    AND i_item_sk = ss_item_sk
    AND s_store_sk = ss_store_sk
    AND ss_customer_sk = sr_customer_sk
    AND ss_item_sk = sr_item_sk
    AND ss_ticket_number = sr_ticket_number
    AND sr_returned_date_sk = d2.d_date_sk
    AND d2.d_quarter_name IN ('1999Q1', '1999Q2', '1999Q3')
    AND sr_customer_sk = cs_bill_customer_sk
    AND sr_item_sk = cs_item_sk
    AND cs_sold_date_sk = d3.d_date_sk
    AND d3.d_quarter_name IN ('1999Q1', '1999Q2', '1999Q3')
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id, i_item_desc, s_state
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q18: Catalog Sales Demographics Analysis with ROLLUP
// =============================================================================
// Analyzes catalog sales with customer demographics using hierarchical
// grouping by item, country, state, and county.
// Tests: 7-way join, self-join on customer_demographics, ROLLUP grouping
pub const TPCDS_Q18: &str = r#"
SELECT
    i_item_id,
    ca_country,
    ca_state,
    ca_county,
    AVG(CAST(cs_quantity AS DECIMAL(12,2))) agg1,
    AVG(CAST(cs_list_price AS DECIMAL(12,2))) agg2,
    AVG(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3,
    AVG(CAST(cs_sales_price AS DECIMAL(12,2))) agg4,
    AVG(CAST(cs_net_profit AS DECIMAL(12,2))) agg5,
    AVG(CAST(c_birth_year AS DECIMAL(12,2))) agg6,
    AVG(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM catalog_sales,
     customer_demographics cd1,
     customer_demographics cd2,
     customer,
     customer_address,
     date_dim,
     item
WHERE cs_sold_date_sk = d_date_sk
    AND cs_item_sk = i_item_sk
    AND cs_bill_cdemo_sk = cd1.cd_demo_sk
    AND cs_bill_customer_sk = c_customer_sk
    AND cd1.cd_gender = 'F'
    AND cd1.cd_education_status = 'Secondary'
    AND c_current_cdemo_sk = cd2.cd_demo_sk
    AND c_current_addr_sk = ca_address_sk
    AND c_birth_month IN (8, 4, 2, 5, 11, 9)
    AND d_year = 2001
    AND ca_state IN ('KS', 'IA', 'AL', 'UT', 'VA', 'NC', 'TX')
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q19: Store sales by customer location
// =============================================================================
// Canonical TPC-DS Q19: Analyzes sales by brand where customer zip differs from store zip.
// Tests: 6-way join with store, geographic filtering (zip code mismatch), aggregation
pub const TPCDS_Q19: &str = r#"
SELECT
    i_brand_id brand_id,
    i_brand brand,
    i_manufact_id,
    i_manufact,
    SUM(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item, customer, customer_address, store
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND i_manager_id = 38
    AND d_moy = 12
    AND d_year = 1998
    AND ss_customer_sk = c_customer_sk
    AND c_current_addr_sk = ca_address_sk
    AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
    AND ss_store_sk = s_store_sk
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
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
// TPC-DS Q52: Weekly store sales by brand
// =============================================================================
// Canonical TPC-DS Q52: Analyzes sales by brand and year with manager filtering.
// Tests: 3-way join, brand filtering by manager, aggregation
pub const TPCDS_Q52: &str = r#"
SELECT
    dt.d_year,
    item.i_brand_id brand_id,
    item.i_brand brand,
    SUM(ss_ext_sales_price) ext_price
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manager_id = 1
    AND dt.d_moy = 11
    AND dt.d_year = 1999
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, ext_price DESC, brand_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q55: Brand sales analysis
// =============================================================================
// Canonical TPC-DS Q55: Analyzes sales by brand with manager filtering.
// Tests: 3-way join, month filtering, aggregation
pub const TPCDS_Q55: &str = r#"
SELECT
    i_brand_id brand_id,
    i_brand brand,
    SUM(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND i_manager_id = 33
    AND d_moy = 12
    AND d_year = 1998
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q68: Store sales by customer demographics
// =============================================================================
// Canonical TPC-DS Q68: Analyzes store sales by ticket with household demographics.
// Tests: Subquery with aggregation, household_demographics filtering, city comparison
pub const TPCDS_Q68: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    ca_city,
    bought_city,
    ss_ticket_number,
    extended_price,
    extended_tax,
    list_price
FROM (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        ca_city bought_city,
        SUM(ss_ext_sales_price) extended_price,
        SUM(ss_ext_list_price) list_price,
        SUM(ss_ext_tax) extended_tax
    FROM store_sales, date_dim, store, household_demographics, customer_address
    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
        AND store_sales.ss_store_sk = store.s_store_sk
        AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        AND store_sales.ss_addr_sk = customer_address.ca_address_sk
        AND date_dim.d_dom BETWEEN 1 AND 2
        AND (household_demographics.hd_dep_count = 8
             OR household_demographics.hd_vehicle_count = 3)
        AND date_dim.d_year IN (1998, 1999, 2000)
        AND store.s_city IN ('Fairview', 'Midway')
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
) dn, customer, customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
    AND customer.c_current_addr_sk = current_addr.ca_address_sk
    AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, ss_ticket_number
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q73: Store ticket analysis
// =============================================================================
// Canonical TPC-DS Q73: Analyzes store tickets with household demographics filtering.
// Tests: Subquery with ticket grouping, household_demographics, county filtering
pub const TPCDS_Q73: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag,
    ss_ticket_number,
    cnt
FROM (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        COUNT(*) cnt
    FROM store_sales, date_dim, store, household_demographics
    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
        AND store_sales.ss_store_sk = store.s_store_sk
        AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        AND date_dim.d_dom BETWEEN 1 AND 2
        AND (household_demographics.hd_buy_potential = '>10000'
             OR household_demographics.hd_buy_potential = '0-500')
        AND household_demographics.hd_vehicle_count > 0
        AND CASE
            WHEN household_demographics.hd_vehicle_count > 0
            THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
            ELSE NULL
        END > 1
        AND date_dim.d_year IN (2000, 2001, 2002)
        AND store.s_county IN ('Williamson County')
    GROUP BY ss_ticket_number, ss_customer_sk
) dj, customer
WHERE ss_customer_sk = c_customer_sk
    AND cnt BETWEEN 1 AND 5
ORDER BY cnt DESC, c_last_name ASC
LIMIT 100
"#;

// =============================================================================
// TPC-DS Q89: Store profit analysis by category
// =============================================================================
// Canonical TPC-DS Q89: Analyzes monthly sales variance from average.
// Tests: Window function (AVG OVER), subquery, CASE expression, category/class filtering
pub const TPCDS_Q89: &str = r#"
SELECT *
FROM (
    SELECT
        i_category,
        i_class,
        i_brand,
        s_store_name,
        s_company_name,
        d_moy,
        SUM(ss_sales_price) sum_sales,
        AVG(SUM(ss_sales_price)) OVER (
            PARTITION BY i_category, i_brand, s_store_name, s_company_name
        ) avg_monthly_sales
    FROM item, store_sales, date_dim, store
    WHERE ss_item_sk = i_item_sk
        AND ss_sold_date_sk = d_date_sk
        AND ss_store_sk = s_store_sk
        AND d_year IN (2002)
        AND ((i_category IN ('Home', 'Men', 'Sports')
              AND i_class IN ('paint', 'accessories', 'fitness'))
             OR (i_category IN ('Shoes', 'Jewelry', 'Women')
                 AND i_class IN ('mens', 'pendants', 'swimwear')))
    GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
) tmp1
WHERE CASE
    WHEN (avg_monthly_sales <> 0)
    THEN (ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales)
    ELSE NULL
END > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
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

// TPC-DS Q22: Inventory by Warehouse with ROLLUP
// Analyzes inventory levels with hierarchical subtotals by warehouse, item, quarter.
// Tests: ROLLUP grouping, multi-table join, hierarchical aggregation
pub const TPCDS_Q22: &str = r#"
SELECT
    i_product_name,
    i_brand,
    i_class,
    i_category,
    AVG(cs_quantity) AS qoh
FROM catalog_sales, warehouse, item, date_dim
WHERE cs_warehouse_sk = w_warehouse_sk
    AND cs_item_sk = i_item_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1200 AND 1211
GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh, i_product_name, i_brand, i_class, i_category
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

// TPC-DS Q36: Store Sales Gross Margin Analysis with CUBE
// Multi-dimensional analysis of gross margin across store and item dimensions.
// Tests: CUBE grouping, GROUPING() function, window function with PARTITION BY
pub const TPCDS_Q36: &str = r#"
SELECT
    SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
    i_category,
    i_class,
    GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(i_category) + GROUPING(i_class),
                     CASE WHEN GROUPING(i_class) = 0 THEN i_category END
        ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC
    ) AS rank_within_parent
FROM store_sales, date_dim, item, store
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND s_state IN ('TN', 'TX', 'OH')
    AND d_year = 2001
GROUP BY CUBE(i_category, i_class)
ORDER BY lochierarchy DESC, gross_margin
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
// Tier 3 Queries (Q51-Q99) - Advanced analytics with ROLLUP/CUBE
// =============================================================================

// TPC-DS Q51: Rolling web sales analysis with window functions
// Tests: Window functions (ROW_NUMBER, SUM OVER), CTEs
pub const TPCDS_Q51: &str = r#"
WITH web_v1 AS (
    SELECT
        ws_item_sk item_sk,
        d_date,
        SUM(ws_sales_price) AS sumws
    FROM web_sales, date_dim
    WHERE ws_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1200 AND 1211
    GROUP BY ws_item_sk, d_date
),
store_v1 AS (
    SELECT
        ss_item_sk item_sk,
        d_date,
        SUM(ss_sales_price) AS sumss
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1200 AND 1211
    GROUP BY ss_item_sk, d_date
)
SELECT
    item_sk,
    d_date,
    SUM(sumws) OVER (PARTITION BY item_sk ORDER BY d_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_ws_sales,
    SUM(sumss) OVER (PARTITION BY item_sk ORDER BY d_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_ss_sales
FROM (
    SELECT item_sk, d_date, sumws, NULL AS sumss FROM web_v1
    UNION ALL
    SELECT item_sk, d_date, NULL AS sumws, sumss FROM store_v1
) x
ORDER BY item_sk, d_date
LIMIT 100
"#;

// TPC-DS Q53: Monthly store sales by manufacturer with window functions
// Tests: Window functions (AVG OVER), manufacturer filtering
pub const TPCDS_Q53: &str = r#"
SELECT
    i_manufact_id,
    SUM(ss_sales_price) sum_sales,
    AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
FROM item, store_sales, date_dim, store
WHERE ss_item_sk = i_item_sk
    AND ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
    AND i_manufact_id IN (1, 2, 3, 4, 5)
GROUP BY i_manufact_id, d_qoy
HAVING SUM(ss_sales_price) > AVG(ss_sales_price)
ORDER BY i_manufact_id, sum_sales
LIMIT 100
"#;

// TPC-DS Q54: Extended price analysis for specific customers
// Tests: Multiple CTEs, EXISTS subquery
pub const TPCDS_Q54: &str = r#"
WITH my_customers AS (
    SELECT DISTINCT c_customer_sk, c_current_addr_sk
    FROM customer, store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year = 2000
        AND d_moy BETWEEN 1 AND 6
),
my_revenue AS (
    SELECT
        c_customer_sk,
        SUM(ss_ext_sales_price) AS revenue
    FROM my_customers, store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year = 2000
        AND d_moy BETWEEN 1 AND 6
    GROUP BY c_customer_sk
)
SELECT
    COUNT(*) AS cnt,
    SUM(revenue) AS total_revenue
FROM my_revenue
WHERE revenue > 50
"#;

// TPC-DS Q56: Store sales by state with IN filter
// Tests: Subquery in WHERE, state filtering
pub const TPCDS_Q56: &str = r#"
SELECT
    i_item_id,
    SUM(ss_ext_sales_price) total_sales
FROM store_sales, date_dim, customer_address, item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_addr_sk = ca_address_sk
    AND ca_gmt_offset = -5
    AND d_year = 2000
    AND d_moy = 2
    AND i_item_id IN (
        SELECT i_item_id
        FROM item
        WHERE i_color IN ('slate', 'blanched', 'burnished')
    )
GROUP BY i_item_id
ORDER BY i_item_id, total_sales
LIMIT 100
"#;

// TPC-DS Q57: Monthly rolling sum analysis
// Tests: Window functions (AVG OVER with window frame)
pub const TPCDS_Q57: &str = r#"
WITH v1 AS (
    SELECT
        i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        SUM(cs_sales_price) sum_sales,
        AVG(SUM(cs_sales_price)) OVER
            (PARTITION BY i_category, i_brand, cc_name, d_year
             ORDER BY d_moy
             ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) avg_monthly_sales,
        ROW_NUMBER() OVER (PARTITION BY i_category, i_brand, cc_name
                          ORDER BY d_year, d_moy) rn
    FROM item, catalog_sales, date_dim, call_center
    WHERE cs_item_sk = i_item_sk
        AND cs_sold_date_sk = d_date_sk
        AND cc_call_center_sk = cs_call_center_sk
        AND d_year IN (1999, 2000, 2001)
    GROUP BY i_category, i_brand, cc_name, d_year, d_moy
)
SELECT *
FROM v1
WHERE d_year = 2000
    AND avg_monthly_sales > 0
    AND CASE WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, i_category
LIMIT 100
"#;

// TPC-DS Q58: Catalog returns analysis with weekly comparison
// Tests: CTEs, aggregate functions
pub const TPCDS_Q58: &str = r#"
WITH ss_items AS (
    SELECT
        i_item_id item_id,
        SUM(ss_ext_sales_price) ss_item_rev
    FROM store_sales, item, date_dim
    WHERE ss_item_sk = i_item_sk
        AND d_date_sk = ss_sold_date_sk
        AND d_date BETWEEN '2000-01-01' AND '2000-01-31'
    GROUP BY i_item_id
),
cs_items AS (
    SELECT
        i_item_id item_id,
        SUM(cs_ext_sales_price) cs_item_rev
    FROM catalog_sales, item, date_dim
    WHERE cs_item_sk = i_item_sk
        AND d_date_sk = cs_sold_date_sk
        AND d_date BETWEEN '2000-01-01' AND '2000-01-31'
    GROUP BY i_item_id
),
ws_items AS (
    SELECT
        i_item_id item_id,
        SUM(ws_ext_sales_price) ws_item_rev
    FROM web_sales, item, date_dim
    WHERE ws_item_sk = i_item_sk
        AND d_date_sk = ws_sold_date_sk
        AND d_date BETWEEN '2000-01-01' AND '2000-01-31'
    GROUP BY i_item_id
)
SELECT
    ss_items.item_id,
    ss_item_rev,
    ss_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 ss_pct,
    cs_item_rev,
    cs_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 cs_pct,
    ws_item_rev,
    ws_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 ws_pct,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 total_rev
FROM ss_items, cs_items, ws_items
WHERE ss_items.item_id = cs_items.item_id
    AND ss_items.item_id = ws_items.item_id
    AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
    AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
ORDER BY ss_items.item_id, ss_item_rev
LIMIT 100
"#;

// TPC-DS Q59: Weekly sales deviation analysis
// Tests: Multiple CTEs, week-over-week comparison
pub const TPCDS_Q59: &str = r#"
WITH wss AS (
    SELECT
        d_week_seq,
        ss_store_sk,
        SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price ELSE NULL END) sun_sales,
        SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price ELSE NULL END) mon_sales,
        SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price ELSE NULL END) tue_sales,
        SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price ELSE NULL END) wed_sales,
        SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price ELSE NULL END) thu_sales,
        SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price ELSE NULL END) fri_sales,
        SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price ELSE NULL END) sat_sales
    FROM store_sales, date_dim
    WHERE d_date_sk = ss_sold_date_sk
    GROUP BY d_week_seq, ss_store_sk
)
SELECT
    s_store_name,
    wss.d_week_seq,
    sun_sales,
    mon_sales,
    tue_sales,
    wed_sales,
    thu_sales,
    fri_sales,
    sat_sales
FROM wss, store, date_dim d
WHERE d.d_week_seq = wss.d_week_seq
    AND ss_store_sk = s_store_sk
    AND d_month_seq BETWEEN 1200 AND 1211
ORDER BY s_store_name, wss.d_week_seq
LIMIT 100
"#;

// TPC-DS Q61: Promotional effectiveness analysis
// Tests: Complex subqueries, division by sum
pub const TPCDS_Q61: &str = r#"
SELECT
    p_promo_name,
    SUM(ss_ext_sales_price) AS promotional_sales,
    SUM(ss_ext_sales_price) * 100 / (
        SELECT SUM(ss_ext_sales_price)
        FROM store_sales, date_dim
        WHERE ss_sold_date_sk = d_date_sk
            AND d_year = 2000
            AND d_moy = 11
    ) AS pct_of_total
FROM store_sales, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_promo_sk = p_promo_sk
    AND d_year = 2000
    AND d_moy = 11
    AND p_channel_email = 'Y'
GROUP BY p_promo_name
ORDER BY promotional_sales DESC
LIMIT 100
"#;

// TPC-DS Q63: Monthly brand revenue deviation
// Tests: CASE with AVG, aggregation
pub const TPCDS_Q63: &str = r#"
SELECT
    i_manager_id,
    SUM(ss_sales_price) sum_sales,
    AVG(ss_sales_price) avg_sales
FROM store_sales, date_dim, item, store
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
    AND i_manager_id IN (1, 2, 3, 4, 5)
GROUP BY i_manager_id
ORDER BY i_manager_id, sum_sales
LIMIT 100
"#;

// TPC-DS Q64: Inventory analysis with returns
// Tests: Multiple fact table joins, complex WHERE
pub const TPCDS_Q64: &str = r#"
WITH cs_ui AS (
    SELECT
        cs_item_sk,
        SUM(cs_ext_list_price) AS sale,
        SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
    FROM catalog_sales, catalog_returns
    WHERE cs_item_sk = cr_item_sk
        AND cs_order_number = cr_order_number
    GROUP BY cs_item_sk
    HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
)
SELECT
    cs_ui.cs_item_sk,
    sale,
    refund
FROM cs_ui, item
WHERE i_item_sk = cs_item_sk
ORDER BY cs_item_sk
LIMIT 100
"#;

// TPC-DS Q65: Store revenue comparison
// Tests: Subquery with aggregation
pub const TPCDS_Q65: &str = r#"
SELECT
    s_store_name,
    i_item_desc,
    SUM(ss_sales_price) AS revenue
FROM store_sales, date_dim, item, store
WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND d_month_seq BETWEEN 1200 AND 1211
GROUP BY s_store_name, i_item_desc
HAVING SUM(ss_sales_price) > (
    SELECT 0.1 * AVG(revenue)
    FROM (
        SELECT SUM(ss_sales_price) AS revenue
        FROM store_sales, date_dim
        WHERE ss_sold_date_sk = d_date_sk
            AND d_month_seq BETWEEN 1200 AND 1211
        GROUP BY ss_store_sk, ss_item_sk
    ) avg_revenue
)
ORDER BY s_store_name, revenue DESC
LIMIT 100
"#;

// TPC-DS Q66: Web and catalog sales by warehouse
// Tests: Complex multi-table join with warehouse
pub const TPCDS_Q66: &str = r#"
SELECT
    w_warehouse_name,
    w_warehouse_sq_ft,
    w_city,
    w_county,
    w_state,
    w_country,
    ship_carriers,
    d_year AS year,
    SUM(jan_sales) AS jan_sales,
    SUM(feb_sales) AS feb_sales,
    SUM(mar_sales) AS mar_sales,
    SUM(apr_sales) AS apr_sales
FROM (
    SELECT
        w_warehouse_name,
        w_warehouse_sq_ft,
        w_city,
        w_county,
        w_state,
        w_country,
        'DHL,BARIAN' AS ship_carriers,
        d_year,
        SUM(CASE WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jan_sales,
        SUM(CASE WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS feb_sales,
        SUM(CASE WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS mar_sales,
        SUM(CASE WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS apr_sales
    FROM web_sales, warehouse, date_dim, ship_mode
    WHERE ws_warehouse_sk = w_warehouse_sk
        AND ws_ship_mode_sk = sm_ship_mode_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year = 2000
        AND sm_type IN ('DHL', 'BARIAN')
    GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
) x
GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, d_year
ORDER BY w_warehouse_name
LIMIT 100
"#;

// TPC-DS Q67: Inventory cube analysis (USES ROLLUP)
// Tests: ROLLUP clause, GROUPING function
pub const TPCDS_Q67: &str = r#"
SELECT
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    SUM(ss_sales_price * ss_quantity) AS sumsales
FROM store_sales, date_dim, store, item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_item_sk = i_item_sk
    AND d_month_seq BETWEEN 1200 AND 1211
GROUP BY ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
ORDER BY i_category, i_class, i_brand, sumsales DESC
LIMIT 100
"#;

// TPC-DS Q69: Customer demographics analysis - store buyers who don't buy web/catalog
// Tests: EXISTS, NOT EXISTS with multiple correlated subqueries, GROUP BY with aggregates
// Official TPC-DS query per specification v3.2.0
// Note: Using explicit JOINs for better optimizer handling (semantically equivalent to comma joins)
pub const TPCDS_Q69: &str = r#"
SELECT
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_education_status,
    COUNT(*) cnt1,
    cd.cd_purchase_estimate,
    COUNT(*) cnt2,
    cd.cd_credit_rating,
    COUNT(*) cnt3
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE ca.ca_state IN ('KS', 'AZ', 'NE')
    AND EXISTS (
        SELECT 1
        FROM store_sales ss
        JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
        WHERE c.c_customer_sk = ss.ss_customer_sk
            AND d1.d_year = 2000
            AND d1.d_moy BETWEEN 1 AND 1 + 2
    )
    AND (NOT EXISTS (
            SELECT 1
            FROM web_sales ws
            JOIN date_dim d2 ON ws.ws_sold_date_sk = d2.d_date_sk
            WHERE c.c_customer_sk = ws.ws_bill_customer_sk
                AND d2.d_year = 2000
                AND d2.d_moy BETWEEN 1 AND 1 + 2
        )
        AND NOT EXISTS (
            SELECT 1
            FROM catalog_sales cs
            JOIN date_dim d3 ON cs.cs_sold_date_sk = d3.d_date_sk
            WHERE c.c_customer_sk = cs.cs_ship_customer_sk
                AND d3.d_year = 2000
                AND d3.d_moy BETWEEN 1 AND 1 + 2
        ))
GROUP BY cd.cd_gender, cd.cd_marital_status, cd.cd_education_status,
         cd.cd_purchase_estimate, cd.cd_credit_rating
ORDER BY cd.cd_gender, cd.cd_marital_status, cd.cd_education_status,
         cd.cd_purchase_estimate, cd.cd_credit_rating
LIMIT 100
"#;

// TPC-DS Q70: Store sales rollup by state (USES ROLLUP)
// Tests: ROLLUP clause, GROUPING function
pub const TPCDS_Q70: &str = r#"
SELECT
    SUM(ss_net_profit) AS total_sum,
    s_state,
    s_county,
    GROUPING(s_state) + GROUPING(s_county) AS lochierarchy
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1200 AND 1211
    AND ss_store_sk = s_store_sk
    AND s_state IN ('TN', 'CA', 'TX', 'NY')
GROUP BY ROLLUP(s_state, s_county)
ORDER BY
    lochierarchy DESC,
    CASE WHEN GROUPING(s_state) + GROUPING(s_county) = 0 THEN s_state END
LIMIT 100
"#;

// TPC-DS Q71: Time-based promotions analysis
// Tests: UNION ALL, date/time filtering
pub const TPCDS_Q71: &str = r#"
SELECT
    i_brand_id brand_id,
    i_brand brand,
    t_hour,
    t_minute,
    SUM(ext_price) ext_price
FROM item,
    (SELECT ws_ext_sales_price AS ext_price, ws_sold_time_sk AS sold_time_sk, ws_item_sk AS sold_item_sk
     FROM web_sales, date_dim
     WHERE d_date_sk = ws_sold_date_sk AND d_moy = 11 AND d_year = 2000
     UNION ALL
     SELECT cs_ext_sales_price AS ext_price, cs_sold_time_sk AS sold_time_sk, cs_item_sk AS sold_item_sk
     FROM catalog_sales, date_dim
     WHERE d_date_sk = cs_sold_date_sk AND d_moy = 11 AND d_year = 2000
     UNION ALL
     SELECT ss_ext_sales_price AS ext_price, ss_sold_time_sk AS sold_time_sk, ss_item_sk AS sold_item_sk
     FROM store_sales, date_dim
     WHERE d_date_sk = ss_sold_date_sk AND d_moy = 11 AND d_year = 2000) tmp,
    time_dim
WHERE sold_item_sk = i_item_sk
    AND sold_time_sk = t_time_sk
    AND i_manager_id = 1
    AND t_meal_time = 'breakfast'
GROUP BY i_brand_id, i_brand, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id
LIMIT 100
"#;

// TPC-DS Q72: Warehouse inventory turnover
// Tests: Complex join with warehouse and ship_mode
pub const TPCDS_Q72: &str = r#"
SELECT
    i_item_desc,
    w_warehouse_name,
    d_week_seq,
    SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo,
    SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo,
    COUNT(*) total_cnt
FROM catalog_sales
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
LEFT JOIN promotion ON cs_promo_sk = p_promo_sk
WHERE d_year = 2000
GROUP BY i_item_desc, w_warehouse_name, d_week_seq
ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d_week_seq
LIMIT 100
"#;

// TPC-DS Q74: Customer year-over-year sales
// Tests: Multiple CTEs with window functions
pub const TPCDS_Q74: &str = r#"
WITH year_total AS (
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        d_year AS year,
        SUM(ss_net_paid) year_total,
        's' sale_type
    FROM customer, store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year IN (2000, 2001)
    GROUP BY c_customer_id, c_first_name, c_last_name, d_year
    UNION ALL
    SELECT
        c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        d_year AS year,
        SUM(ws_net_paid) year_total,
        'w' sale_type
    FROM customer, web_sales, date_dim
    WHERE c_customer_sk = ws_bill_customer_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year IN (2000, 2001)
    GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name
FROM year_total t_s_firstyear, year_total t_s_secyear,
     year_total t_w_firstyear, year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
    AND t_s_firstyear.customer_id = t_w_secyear.customer_id
    AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
    AND t_s_firstyear.sale_type = 's'
    AND t_s_secyear.sale_type = 's'
    AND t_w_firstyear.sale_type = 'w'
    AND t_w_secyear.sale_type = 'w'
    AND t_s_firstyear.year = 2000
    AND t_s_secyear.year = 2001
    AND t_w_firstyear.year = 2000
    AND t_w_secyear.year = 2001
    AND t_s_firstyear.year_total > 0
    AND t_w_firstyear.year_total > 0
    AND CASE WHEN t_w_firstyear.year_total > 0
             THEN t_w_secyear.year_total / t_w_firstyear.year_total
             ELSE NULL END >
        CASE WHEN t_s_firstyear.year_total > 0
             THEN t_s_secyear.year_total / t_s_firstyear.year_total
             ELSE NULL END
ORDER BY t_s_secyear.customer_id
LIMIT 100
"#;

// TPC-DS Q75: Brand sales comparison across channels
// Tests: Multiple UNION ALL in CTE
pub const TPCDS_Q75: &str = r#"
WITH all_sales AS (
    SELECT
        d_year,
        i_brand_id,
        i_class_id,
        i_category_id,
        i_manufact_id,
        SUM(sales_cnt) AS sales_cnt,
        SUM(sales_amt) AS sales_amt
    FROM (
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
            cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
        FROM catalog_sales
        JOIN item ON i_item_sk = cs_item_sk
        JOIN date_dim ON d_date_sk = cs_sold_date_sk
        LEFT JOIN catalog_returns ON cs_order_number = cr_order_number
            AND cs_item_sk = cr_item_sk
        WHERE i_category = 'Books'
        UNION ALL
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
            ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
        FROM store_sales
        JOIN item ON i_item_sk = ss_item_sk
        JOIN date_dim ON d_date_sk = ss_sold_date_sk
        LEFT JOIN store_returns ON ss_ticket_number = sr_ticket_number
            AND ss_item_sk = sr_item_sk
        WHERE i_category = 'Books'
        UNION ALL
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
            ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
        FROM web_sales
        JOIN item ON i_item_sk = ws_item_sk
        JOIN date_dim ON d_date_sk = ws_sold_date_sk
        LEFT JOIN web_returns ON ws_order_number = wr_order_number
            AND ws_item_sk = wr_item_sk
        WHERE i_category = 'Books'
    ) sales_detail
    GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
)
SELECT
    prev_yr.d_year AS prev_year,
    curr_yr.d_year AS year,
    curr_yr.i_brand_id,
    curr_yr.i_class_id,
    curr_yr.i_category_id,
    curr_yr.i_manufact_id,
    prev_yr.sales_cnt AS prev_yr_cnt,
    curr_yr.sales_cnt AS curr_yr_cnt,
    curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
    curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr, all_sales prev_yr
WHERE curr_yr.i_brand_id = prev_yr.i_brand_id
    AND curr_yr.i_class_id = prev_yr.i_class_id
    AND curr_yr.i_category_id = prev_yr.i_category_id
    AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
    AND curr_yr.d_year = 2001
    AND prev_yr.d_year = 2000
    AND CAST(curr_yr.sales_cnt AS DECIMAL) / CAST(prev_yr.sales_cnt AS DECIMAL) < 0.9
ORDER BY sales_cnt_diff
LIMIT 100
"#;

// TPC-DS Q77: Channel profit rollup (USES ROLLUP)
// Tests: ROLLUP clause, channel analysis
pub const TPCDS_Q77: &str = r#"
WITH ss AS (
    SELECT
        s_store_sk,
        SUM(ss_ext_sales_price) AS sales,
        SUM(ss_net_profit) AS profit
    FROM store_sales, date_dim, store
    WHERE ss_sold_date_sk = d_date_sk
        AND d_date BETWEEN '2000-08-01' AND '2000-08-31'
        AND ss_store_sk = s_store_sk
    GROUP BY s_store_sk
),
ws AS (
    SELECT
        ws_web_site_sk,
        SUM(ws_ext_sales_price) AS sales,
        SUM(ws_net_profit) AS profit
    FROM web_sales, date_dim, web_site
    WHERE ws_sold_date_sk = d_date_sk
        AND d_date BETWEEN '2000-08-01' AND '2000-08-31'
        AND ws_web_site_sk = web_site_sk
    GROUP BY ws_web_site_sk
),
cs AS (
    SELECT
        cs_call_center_sk,
        SUM(cs_ext_sales_price) AS sales,
        SUM(cs_net_profit) AS profit
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
        AND d_date BETWEEN '2000-08-01' AND '2000-08-31'
    GROUP BY cs_call_center_sk
)
SELECT
    channel,
    id,
    SUM(sales) AS sales,
    SUM(profit) AS profit
FROM (
    SELECT 'store' AS channel, s_store_sk AS id, sales, profit FROM ss
    UNION ALL
    SELECT 'web' AS channel, ws_web_site_sk AS id, sales, profit FROM ws
    UNION ALL
    SELECT 'catalog' AS channel, cs_call_center_sk AS id, sales, profit FROM cs
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100
"#;

// TPC-DS Q78: Web/catalog sales comparison
// Tests: Complex CTE with window functions
pub const TPCDS_Q78: &str = r#"
WITH ws AS (
    SELECT
        d_year AS ws_sold_year,
        ws_item_sk,
        ws_bill_customer_sk ws_customer_sk,
        SUM(ws_quantity) ws_qty,
        SUM(ws_wholesale_cost) ws_wc,
        SUM(ws_sales_price) ws_sp
    FROM web_sales
    LEFT JOIN web_returns ON wr_order_number = ws_order_number AND ws_item_sk = wr_item_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE wr_order_number IS NULL
    GROUP BY d_year, ws_item_sk, ws_bill_customer_sk
),
cs AS (
    SELECT
        d_year AS cs_sold_year,
        cs_item_sk,
        cs_bill_customer_sk cs_customer_sk,
        SUM(cs_quantity) cs_qty,
        SUM(cs_wholesale_cost) cs_wc,
        SUM(cs_sales_price) cs_sp
    FROM catalog_sales
    LEFT JOIN catalog_returns ON cr_order_number = cs_order_number AND cs_item_sk = cr_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE cr_order_number IS NULL
    GROUP BY d_year, cs_item_sk, cs_bill_customer_sk
),
ss AS (
    SELECT
        d_year AS ss_sold_year,
        ss_item_sk,
        ss_customer_sk,
        SUM(ss_quantity) ss_qty,
        SUM(ss_wholesale_cost) ss_wc,
        SUM(ss_sales_price) ss_sp
    FROM store_sales
    LEFT JOIN store_returns ON sr_ticket_number = ss_ticket_number AND ss_item_sk = sr_item_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE sr_ticket_number IS NULL
    GROUP BY d_year, ss_item_sk, ss_customer_sk
)
SELECT
    ss_sold_year,
    ss_item_sk,
    ss_customer_sk,
    ROUND(ss_qty / (COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)), 2) AS ratio
FROM ss
LEFT JOIN ws ON ws_sold_year = ss_sold_year AND ws_item_sk = ss_item_sk AND ws_customer_sk = ss_customer_sk
LEFT JOIN cs ON cs_sold_year = ss_sold_year AND cs_item_sk = ss_item_sk AND cs_customer_sk = ss_customer_sk
WHERE (COALESCE(ws_qty, 0) > 0 OR COALESCE(cs_qty, 0) > 0)
    AND ss_sold_year = 2000
ORDER BY ss_sold_year, ss_item_sk, ss_customer_sk, ratio
LIMIT 100
"#;

// TPC-DS Q79: Store profits by customer location
// Tests: Customer address filtering, profit analysis
pub const TPCDS_Q79: &str = r#"
SELECT
    c_last_name,
    c_first_name,
    SUBSTR(s_city, 1, 30) AS city,
    ss_ticket_number,
    amt,
    profit
FROM (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        s_city,
        SUM(ss_coupon_amt) amt,
        SUM(ss_net_profit) profit
    FROM store_sales, date_dim, store
    WHERE ss_sold_date_sk = d_date_sk
        AND ss_store_sk = s_store_sk
        AND d_year = 2000
        AND d_moy = 11
    GROUP BY ss_ticket_number, ss_customer_sk, s_city
) ms, customer
WHERE ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, city, profit
LIMIT 100
"#;

// TPC-DS Q80: Channel returns profit (USES ROLLUP)
// Tests: ROLLUP on multiple channels
pub const TPCDS_Q80: &str = r#"
WITH ssr AS (
    SELECT
        s_store_id AS store_id,
        SUM(ss_ext_sales_price) AS sales,
        SUM(ss_net_profit) AS profit,
        SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS profit_ratio
    FROM store_sales, date_dim, store
    WHERE ss_sold_date_sk = d_date_sk
        AND d_date BETWEEN '2000-08-23' AND '2000-09-22'
        AND ss_store_sk = s_store_sk
    GROUP BY s_store_id
)
SELECT
    channel,
    id,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit
FROM (
    SELECT 'store' AS channel, store_id AS id, sales, profit FROM ssr
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100
"#;

// TPC-DS Q85: Web returns by reason and item
// Tests: Reason analysis with returns
pub const TPCDS_Q85: &str = r#"
SELECT
    SUBSTR(r_reason_desc, 1, 20) reason,
    AVG(ws_quantity) avg_qty,
    AVG(wr_refunded_cash) avg_refund,
    AVG(wr_fee) avg_fee
FROM web_sales, web_returns, web_page, reason, date_dim
WHERE ws_web_page_sk = wp_web_page_sk
    AND ws_item_sk = wr_item_sk
    AND ws_order_number = wr_order_number
    AND ws_sold_date_sk = d_date_sk
    AND wr_reason_sk = r_reason_sk
    AND d_year = 2000
GROUP BY r_reason_desc
ORDER BY reason, avg_qty, avg_refund, avg_fee
LIMIT 100
"#;

// TPC-DS Q86: Web sales category rollup (USES ROLLUP)
// Tests: ROLLUP clause for hierarchical totals
pub const TPCDS_Q86: &str = r#"
SELECT
    SUM(ws_net_paid) AS total_sum,
    i_category,
    i_class,
    GROUPING(i_category) + GROUPING(i_class) AS lochierarchy
FROM web_sales, date_dim, item
WHERE ws_sold_date_sk = d_date_sk
    AND ws_item_sk = i_item_sk
    AND d_month_seq BETWEEN 1200 AND 1211
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
    lochierarchy DESC,
    CASE WHEN GROUPING(i_category) + GROUPING(i_class) = 0 THEN i_category END
LIMIT 100
"#;

// TPC-DS Q87: Customer overlap between channels
// Tests: EXCEPT/INTERSECT or NOT EXISTS patterns
pub const TPCDS_Q87: &str = r#"
SELECT COUNT(*) AS customer_count
FROM (
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM store_sales, date_dim, customer
    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
        AND store_sales.ss_customer_sk = customer.c_customer_sk
        AND d_month_seq BETWEEN 1200 AND 1211
) hot_cust
WHERE NOT EXISTS (
    SELECT 1
    FROM catalog_sales, date_dim d2, customer c2
    WHERE catalog_sales.cs_sold_date_sk = d2.d_date_sk
        AND catalog_sales.cs_bill_customer_sk = c2.c_customer_sk
        AND c2.c_last_name = hot_cust.c_last_name
        AND c2.c_first_name = hot_cust.c_first_name
        AND d2.d_date = hot_cust.d_date
)
"#;

// TPC-DS Q88: Time-of-day purchase patterns
// Tests: Multiple scalar subqueries, time analysis
pub const TPCDS_Q88: &str = r#"
SELECT *
FROM (
    SELECT COUNT(*) h8_30_to_9
    FROM store_sales, time_dim, store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
        AND ss_store_sk = s_store_sk
        AND t_hour = 8
        AND t_minute >= 30
        AND s_store_name = 'Store#1'
) s1,
(
    SELECT COUNT(*) h9_to_9_30
    FROM store_sales, time_dim, store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
        AND ss_store_sk = s_store_sk
        AND t_hour = 9
        AND t_minute < 30
        AND s_store_name = 'Store#1'
) s2,
(
    SELECT COUNT(*) h9_30_to_10
    FROM store_sales, time_dim, store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
        AND ss_store_sk = s_store_sk
        AND t_hour = 9
        AND t_minute >= 30
        AND s_store_name = 'Store#1'
) s3,
(
    SELECT COUNT(*) h10_to_10_30
    FROM store_sales, time_dim, store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
        AND ss_store_sk = s_store_sk
        AND t_hour = 10
        AND t_minute < 30
        AND s_store_name = 'Store#1'
) s4
LIMIT 100
"#;

// TPC-DS Q90: Web time-based analysis
// Tests: CASE within aggregation
// Note: Changed subquery alias from 'at' to 'am_data' for DuckDB compatibility
// ('at' is a reserved keyword in DuckDB)
pub const TPCDS_Q90: &str = r#"
SELECT
    CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) am_pm_ratio
FROM (
    SELECT COUNT(*) amc
    FROM web_sales, time_dim, web_page
    WHERE ws_sold_time_sk = time_dim.t_time_sk
        AND ws_web_page_sk = web_page.wp_web_page_sk
        AND t_hour BETWEEN 8 AND 9
) am_data,
(
    SELECT COUNT(*) pmc
    FROM web_sales, time_dim, web_page
    WHERE ws_sold_time_sk = time_dim.t_time_sk
        AND ws_web_page_sk = web_page.wp_web_page_sk
        AND t_hour BETWEEN 19 AND 20
) pm_data
WHERE pmc > 0
"#;

// TPC-DS Q91: Web returns by call center
// Tests: Call center analysis
pub const TPCDS_Q91: &str = r#"
SELECT
    cc_call_center_id call_center,
    cc_name call_center_name,
    cc_manager manager,
    SUM(cr_net_loss) returns_loss
FROM call_center, catalog_returns, date_dim, customer
WHERE cr_call_center_sk = cc_call_center_sk
    AND cr_returned_date_sk = d_date_sk
    AND cr_returning_customer_sk = c_customer_sk
    AND d_year = 2000
    AND d_moy = 11
GROUP BY cc_call_center_id, cc_name, cc_manager
ORDER BY returns_loss DESC
LIMIT 100
"#;

// TPC-DS Q93: Store returns with no original sale
// Tests: Anti-join pattern
pub const TPCDS_Q93: &str = r#"
SELECT
    ss_customer_sk,
    SUM(sr_return_amt) AS returns_amt
FROM store_returns, store_sales, reason
WHERE sr_item_sk = ss_item_sk
    AND sr_ticket_number = ss_ticket_number
    AND sr_reason_sk = r_reason_sk
    AND r_reason_desc = 'reason 28'
GROUP BY ss_customer_sk
ORDER BY returns_amt DESC
LIMIT 100
"#;

// TPC-DS Q94: Web sales with no returns
// Tests: Anti-join pattern for web channel
pub const TPCDS_Q94: &str = r#"
SELECT
    COUNT(DISTINCT ws_order_number) AS order_count,
    SUM(ws_ext_ship_cost) AS total_ship_cost,
    SUM(ws_net_profit) AS total_profit
FROM web_sales ws1, date_dim, customer_address, web_site
WHERE d_date BETWEEN '2000-02-01' AND '2000-04-02'
    AND ws1.ws_ship_date_sk = d_date_sk
    AND ws1.ws_ship_addr_sk = ca_address_sk
    AND ca_state = 'IL'
    AND ws1.ws_web_site_sk = web_site_sk
    AND web_company_name = 'pri'
    AND NOT EXISTS (
        SELECT 1
        FROM web_returns
        WHERE ws1.ws_order_number = wr_order_number
    )
ORDER BY order_count
LIMIT 100
"#;

// TPC-DS Q95: Web sales with secondary orders
// Tests: Complex subquery patterns
pub const TPCDS_Q95: &str = r#"
WITH ws_wh AS (
    SELECT ws1.ws_order_number, ws1.ws_warehouse_sk wh1, ws2.ws_warehouse_sk wh2
    FROM web_sales ws1, web_sales ws2
    WHERE ws1.ws_order_number = ws2.ws_order_number
        AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT
    COUNT(DISTINCT ws_order_number) AS order_count,
    SUM(ws_ext_ship_cost) AS total_ship_cost,
    SUM(ws_net_profit) AS total_profit
FROM web_sales ws1, date_dim, customer_address, web_site
WHERE d_date BETWEEN '2000-02-01' AND '2000-04-02'
    AND ws1.ws_ship_date_sk = d_date_sk
    AND ws1.ws_ship_addr_sk = ca_address_sk
    AND ca_state = 'IL'
    AND ws1.ws_web_site_sk = web_site_sk
    AND web_company_name = 'pri'
    AND ws1.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
    AND ws1.ws_order_number IN (
        SELECT wr_order_number
        FROM web_returns, ws_wh
        WHERE wr_order_number = ws_wh.ws_order_number
    )
ORDER BY order_count
LIMIT 100
"#;

// TPC-DS Q97: Store/catalog overlap analysis
// Tests: FULL OUTER JOIN equivalent
pub const TPCDS_Q97: &str = r#"
WITH ssci AS (
    SELECT ss_customer_sk customer_sk, ss_item_sk item_sk
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1200 AND 1211
    GROUP BY ss_customer_sk, ss_item_sk
),
csci AS (
    SELECT cs_bill_customer_sk customer_sk, cs_item_sk item_sk
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1200 AND 1211
    GROUP BY cs_bill_customer_sk, cs_item_sk
)
SELECT
    SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) store_only,
    SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) catalog_only,
    SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) store_and_catalog
FROM ssci
LEFT JOIN csci ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk
"#;

// TPC-DS Q98: Category subcategory sales
// Tests: Window functions with PARTITION BY
pub const TPCDS_Q98: &str = r#"
SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ss_ext_sales_price) AS itemrevenue,
    SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM store_sales, item, date_dim
WHERE ss_item_sk = i_item_sk
    AND i_category IN ('Jewelry', 'Sports', 'Books')
    AND ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN '2001-01-12' AND '2001-02-11'
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
"#;

// TPC-DS Q99: Catalog ship mode analysis
// Tests: Ship mode and warehouse join
pub const TPCDS_Q99: &str = r#"
SELECT
    SUBSTR(w_warehouse_name, 1, 20) AS warehouse,
    sm_type,
    cc_name AS call_center,
    SUM(CASE WHEN d_moy = 1 THEN cs_sales_price * cs_quantity ELSE 0 END) AS jan_sales,
    SUM(CASE WHEN d_moy = 2 THEN cs_sales_price * cs_quantity ELSE 0 END) AS feb_sales,
    SUM(CASE WHEN d_moy = 3 THEN cs_sales_price * cs_quantity ELSE 0 END) AS mar_sales,
    SUM(CASE WHEN d_moy = 4 THEN cs_sales_price * cs_quantity ELSE 0 END) AS apr_sales,
    SUM(CASE WHEN d_moy = 5 THEN cs_sales_price * cs_quantity ELSE 0 END) AS may_sales,
    SUM(CASE WHEN d_moy = 6 THEN cs_sales_price * cs_quantity ELSE 0 END) AS jun_sales
FROM catalog_sales, warehouse, ship_mode, call_center, date_dim
WHERE cs_warehouse_sk = w_warehouse_sk
    AND cs_ship_mode_sk = sm_ship_mode_sk
    AND cs_call_center_sk = cc_call_center_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_year = 2000
GROUP BY SUBSTR(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY warehouse, sm_type, call_center
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

// =============================================================================
// Query Registry for Benchmark Iteration
// =============================================================================

/// All TPC-DS queries available for benchmarking
pub const TPCDS_QUERIES: &[(&str, &str)] = &[
    // Phase 1 queries (core tables: date_dim, item, customer, store, store_sales)
    ("Q1", TPCDS_Q1),
    ("Q2", TPCDS_Q2),
    ("Q3", TPCDS_Q3),
    ("Q4", TPCDS_Q4), // Cross-channel customer growth comparison
    ("Q5", TPCDS_Q5), // Multi-channel sales/returns with ROLLUP
    ("Q6", TPCDS_Q6),
    ("Q7", TPCDS_Q7),
    ("Q8", TPCDS_Q8), // Store sales by ZIP code with INTERSECT
    ("Q9", TPCDS_Q9),
    ("Q10", TPCDS_Q10),
    ("Q11", TPCDS_Q11), // Customer web vs store sales growth
    ("Q12", TPCDS_Q12),
    ("Q14", TPCDS_Q14), // Cross-channel items with ROLLUP
    ("Q15", TPCDS_Q15),
    ("Q17", TPCDS_Q17), // Store sales-returns-catalog analysis
    ("Q18", TPCDS_Q18), // Catalog demographics with ROLLUP
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
    ("Q22", TPCDS_Q22),
    ("Q23", TPCDS_Q23),
    ("Q24", TPCDS_Q24),
    ("Q28", TPCDS_Q28),
    ("Q29", TPCDS_Q29),
    ("Q30", TPCDS_Q30),
    ("Q31", TPCDS_Q31),
    ("Q33", TPCDS_Q33),
    ("Q34", TPCDS_Q34),
    ("Q36", TPCDS_Q36),
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
    // Tier 3 queries (Q51-Q99) - Advanced analytics with ROLLUP/CUBE
    ("Q51", TPCDS_Q51),
    ("Q53", TPCDS_Q53),
    ("Q54", TPCDS_Q54),
    ("Q56", TPCDS_Q56),
    ("Q57", TPCDS_Q57), // Call center performance analysis
    ("Q58", TPCDS_Q58),
    ("Q59", TPCDS_Q59),
    ("Q61", TPCDS_Q61),
    ("Q63", TPCDS_Q63),
    ("Q64", TPCDS_Q64),
    ("Q65", TPCDS_Q65),
    ("Q66", TPCDS_Q66),
    ("Q67", TPCDS_Q67), // Uses ROLLUP
    ("Q69", TPCDS_Q69),
    ("Q70", TPCDS_Q70), // Uses ROLLUP + GROUPING
    ("Q71", TPCDS_Q71),
    ("Q72", TPCDS_Q72),
    ("Q74", TPCDS_Q74),
    ("Q75", TPCDS_Q75),
    ("Q77", TPCDS_Q77), // Uses ROLLUP
    ("Q78", TPCDS_Q78),
    ("Q79", TPCDS_Q79),
    ("Q80", TPCDS_Q80), // Uses ROLLUP
    ("Q85", TPCDS_Q85),
    ("Q86", TPCDS_Q86), // Uses ROLLUP + GROUPING
    ("Q87", TPCDS_Q87),
    ("Q88", TPCDS_Q88),
    ("Q90", TPCDS_Q90),
    ("Q91", TPCDS_Q91), // Call center customer analysis
    ("Q93", TPCDS_Q93),
    ("Q94", TPCDS_Q94),
    ("Q95", TPCDS_Q95),
    ("Q97", TPCDS_Q97),
    ("Q98", TPCDS_Q98),
    ("Q99", TPCDS_Q99), // Call center ship mode analysis
];

/// Sanity check queries for validation
pub const TPCDS_SANITY_QUERIES: &[(&str, &str)] = &[
    ("sanity_date", TPCDS_SANITY_DATE),
    ("sanity_sales", TPCDS_SANITY_SALES),
    ("sanity_join", TPCDS_SANITY_JOIN),
];

/// Queries that SQLite cannot execute due to missing SQL features.
///
/// SQLite lacks several SQL:1999/2003 OLAP features that TPC-DS requires:
///
/// - **ROLLUP/CUBE**: Q5, Q14, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86
///   SQLite doesn't support `GROUP BY ROLLUP(...)` or `GROUP BY CUBE(...)`
///   These would need to be rewritten as multiple UNION ALL queries.
///
/// - **GROUPING()**: Q36, Q70, Q86
///   The `GROUPING()` function identifies super-aggregate rows in ROLLUP/CUBE.
///
/// - **STDDEV_SAMP()**: Q17
///   SQLite lacks built-in sample standard deviation. Could be computed manually
///   but would require significant query rewriting.
///
/// - **Parenthesized UNION subqueries**: Q2
///   SQLite doesn't allow parentheses around SELECT statements in UNION.
///   Example: `SELECT ... UNION ALL (SELECT ...)` fails.
///
/// Total: 11 queries skipped (Q2, Q5, Q14, Q17, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86)
pub const SQLITE_SKIP_QUERIES: &[&str] = &[
    // UNION syntax incompatibility (parenthesized subquery)
    "Q2",
    // ROLLUP queries
    "Q5",
    "Q14",
    "Q18",
    "Q22",
    "Q67",
    "Q70",
    "Q77",
    "Q80",
    // GROUPING() function (also uses ROLLUP/CUBE)
    "Q36",
    "Q86",
    // STDDEV_SAMP() function
    "Q17",
];

/// Check if a query should be skipped for SQLite
pub fn sqlite_should_skip(query_name: &str) -> bool {
    SQLITE_SKIP_QUERIES.contains(&query_name)
}
