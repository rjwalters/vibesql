-- TPC-E Schema for DuckDB
-- 33 tables organized into Fixed, Customer, Market, and Trade categories

-- =========================================================================
-- Fixed Tables (9)
-- =========================================================================

CREATE TABLE charge (
    ch_tt_id VARCHAR NOT NULL,
    ch_c_tier INTEGER NOT NULL,
    ch_chrg DOUBLE NOT NULL,
    PRIMARY KEY (ch_tt_id, ch_c_tier)
);

CREATE TABLE commission_rate (
    cr_c_tier INTEGER NOT NULL,
    cr_tt_id VARCHAR NOT NULL,
    cr_ex_id VARCHAR NOT NULL,
    cr_from_qty INTEGER NOT NULL,
    cr_to_qty INTEGER NOT NULL,
    cr_rate DOUBLE NOT NULL,
    PRIMARY KEY (cr_c_tier, cr_tt_id, cr_ex_id, cr_from_qty)
);

CREATE TABLE exchange (
    ex_id VARCHAR PRIMARY KEY,
    ex_name VARCHAR NOT NULL,
    ex_num_symb INTEGER NOT NULL,
    ex_open INTEGER NOT NULL,
    ex_close INTEGER NOT NULL,
    ex_desc VARCHAR,
    ex_ad_id BIGINT NOT NULL
);

CREATE TABLE industry (
    in_id VARCHAR PRIMARY KEY,
    in_name VARCHAR NOT NULL,
    in_sc_id VARCHAR NOT NULL
);

CREATE TABLE sector (
    sc_id VARCHAR PRIMARY KEY,
    sc_name VARCHAR NOT NULL
);

CREATE TABLE status_type (
    st_id VARCHAR PRIMARY KEY,
    st_name VARCHAR NOT NULL
);

CREATE TABLE taxrate (
    tx_id VARCHAR PRIMARY KEY,
    tx_name VARCHAR NOT NULL,
    tx_rate DOUBLE NOT NULL
);

CREATE TABLE trade_type (
    tt_id VARCHAR PRIMARY KEY,
    tt_name VARCHAR NOT NULL,
    tt_is_sell BOOLEAN NOT NULL,
    tt_is_mrkt BOOLEAN NOT NULL
);

CREATE TABLE zip_code (
    zc_code VARCHAR PRIMARY KEY,
    zc_town VARCHAR NOT NULL,
    zc_div VARCHAR NOT NULL
);

-- =========================================================================
-- Customer Tables (8)
-- =========================================================================

CREATE TABLE address (
    ad_id BIGINT PRIMARY KEY,
    ad_line1 VARCHAR NOT NULL,
    ad_line2 VARCHAR,
    ad_zc_code VARCHAR NOT NULL,
    ad_ctry VARCHAR NOT NULL
);

CREATE TABLE customer (
    c_id BIGINT PRIMARY KEY,
    c_tax_id VARCHAR NOT NULL,
    c_st_id VARCHAR NOT NULL,
    c_l_name VARCHAR NOT NULL,
    c_f_name VARCHAR NOT NULL,
    c_m_name VARCHAR,
    c_gndr VARCHAR,
    c_tier INTEGER NOT NULL,
    c_dob VARCHAR NOT NULL,
    c_ad_id BIGINT NOT NULL,
    c_ctry_1 VARCHAR,
    c_area_1 VARCHAR,
    c_local_1 VARCHAR,
    c_ext_1 VARCHAR,
    c_ctry_2 VARCHAR,
    c_area_2 VARCHAR,
    c_local_2 VARCHAR,
    c_ext_2 VARCHAR,
    c_ctry_3 VARCHAR,
    c_area_3 VARCHAR,
    c_local_3 VARCHAR,
    c_ext_3 VARCHAR,
    c_email_1 VARCHAR,
    c_email_2 VARCHAR
);

CREATE TABLE customer_account (
    ca_id BIGINT PRIMARY KEY,
    ca_b_id BIGINT NOT NULL,
    ca_c_id BIGINT NOT NULL,
    ca_name VARCHAR,
    ca_tax_st INTEGER NOT NULL,
    ca_bal DOUBLE NOT NULL
);

CREATE TABLE customer_taxrate (
    cx_tx_id VARCHAR NOT NULL,
    cx_c_id BIGINT NOT NULL,
    PRIMARY KEY (cx_tx_id, cx_c_id)
);

CREATE TABLE account_permission (
    ap_ca_id BIGINT NOT NULL,
    ap_acl VARCHAR NOT NULL,
    ap_tax_id VARCHAR NOT NULL,
    ap_l_name VARCHAR NOT NULL,
    ap_f_name VARCHAR NOT NULL,
    PRIMARY KEY (ap_ca_id, ap_tax_id)
);

CREATE TABLE watch_list (
    wl_id BIGINT PRIMARY KEY,
    wl_c_id BIGINT NOT NULL
);

CREATE TABLE watch_item (
    wi_wl_id BIGINT NOT NULL,
    wi_s_symb VARCHAR NOT NULL,
    PRIMARY KEY (wi_wl_id, wi_s_symb)
);

CREATE TABLE broker (
    b_id BIGINT PRIMARY KEY,
    b_st_id VARCHAR NOT NULL,
    b_name VARCHAR NOT NULL,
    b_num_trades INTEGER NOT NULL,
    b_comm_total DOUBLE NOT NULL
);

-- =========================================================================
-- Market Tables (8)
-- =========================================================================

CREATE TABLE company (
    co_id BIGINT PRIMARY KEY,
    co_st_id VARCHAR NOT NULL,
    co_name VARCHAR NOT NULL,
    co_in_id VARCHAR NOT NULL,
    co_sp_rate VARCHAR,
    co_ceo VARCHAR,
    co_ad_id BIGINT NOT NULL,
    co_desc VARCHAR,
    co_open_date VARCHAR NOT NULL
);

CREATE TABLE company_competitor (
    cp_co_id BIGINT NOT NULL,
    cp_comp_co_id BIGINT NOT NULL,
    cp_in_id VARCHAR NOT NULL,
    PRIMARY KEY (cp_co_id, cp_comp_co_id, cp_in_id)
);

CREATE TABLE security (
    s_symb VARCHAR PRIMARY KEY,
    s_issue VARCHAR NOT NULL,
    s_st_id VARCHAR NOT NULL,
    s_name VARCHAR NOT NULL,
    s_ex_id VARCHAR NOT NULL,
    s_co_id BIGINT NOT NULL,
    s_num_out BIGINT NOT NULL,
    s_start_date VARCHAR NOT NULL,
    s_exch_date VARCHAR NOT NULL,
    s_pe DOUBLE,
    s_52wk_high DOUBLE NOT NULL,
    s_52wk_high_date VARCHAR NOT NULL,
    s_52wk_low DOUBLE NOT NULL,
    s_52wk_low_date VARCHAR NOT NULL,
    s_dividend DOUBLE NOT NULL,
    s_yield DOUBLE NOT NULL
);

CREATE TABLE daily_market (
    dm_date VARCHAR NOT NULL,
    dm_s_symb VARCHAR NOT NULL,
    dm_close DOUBLE NOT NULL,
    dm_high DOUBLE NOT NULL,
    dm_low DOUBLE NOT NULL,
    dm_vol BIGINT NOT NULL,
    PRIMARY KEY (dm_date, dm_s_symb)
);

CREATE TABLE last_trade (
    lt_s_symb VARCHAR PRIMARY KEY,
    lt_dts VARCHAR NOT NULL,
    lt_price DOUBLE NOT NULL,
    lt_open_price DOUBLE NOT NULL,
    lt_vol BIGINT NOT NULL
);

CREATE TABLE financial (
    fi_co_id BIGINT NOT NULL,
    fi_year INTEGER NOT NULL,
    fi_qtr INTEGER NOT NULL,
    fi_qtr_start_date VARCHAR NOT NULL,
    fi_revenue DOUBLE NOT NULL,
    fi_net_earn DOUBLE NOT NULL,
    fi_basic_eps DOUBLE NOT NULL,
    fi_dilut_eps DOUBLE NOT NULL,
    fi_margin DOUBLE NOT NULL,
    fi_inventory DOUBLE NOT NULL,
    fi_assets DOUBLE NOT NULL,
    fi_liability DOUBLE NOT NULL,
    fi_out_basic BIGINT NOT NULL,
    fi_out_dilut BIGINT NOT NULL,
    PRIMARY KEY (fi_co_id, fi_year, fi_qtr)
);

CREATE TABLE news_item (
    ni_id BIGINT PRIMARY KEY,
    ni_headline VARCHAR NOT NULL,
    ni_summary VARCHAR NOT NULL,
    ni_item VARCHAR NOT NULL,
    ni_dts VARCHAR NOT NULL,
    ni_source VARCHAR NOT NULL,
    ni_author VARCHAR
);

CREATE TABLE news_xref (
    nx_ni_id BIGINT NOT NULL,
    nx_co_id BIGINT NOT NULL,
    PRIMARY KEY (nx_ni_id, nx_co_id)
);

-- =========================================================================
-- Trade Tables (8)
-- =========================================================================

CREATE TABLE trade (
    t_id BIGINT PRIMARY KEY,
    t_dts VARCHAR NOT NULL,
    t_st_id VARCHAR NOT NULL,
    t_tt_id VARCHAR NOT NULL,
    t_is_cash BOOLEAN NOT NULL,
    t_s_symb VARCHAR NOT NULL,
    t_qty INTEGER NOT NULL,
    t_bid_price DOUBLE NOT NULL,
    t_ca_id BIGINT NOT NULL,
    t_exec_name VARCHAR NOT NULL,
    t_trade_price DOUBLE,
    t_chrg DOUBLE NOT NULL,
    t_comm DOUBLE NOT NULL,
    t_tax DOUBLE NOT NULL,
    t_lifo BOOLEAN NOT NULL
);

CREATE TABLE trade_history (
    th_t_id BIGINT NOT NULL,
    th_dts VARCHAR NOT NULL,
    th_st_id VARCHAR NOT NULL,
    PRIMARY KEY (th_t_id, th_st_id)
);

CREATE TABLE trade_request (
    tr_t_id BIGINT PRIMARY KEY,
    tr_tt_id VARCHAR NOT NULL,
    tr_s_symb VARCHAR NOT NULL,
    tr_qty INTEGER NOT NULL,
    tr_bid_price DOUBLE NOT NULL,
    tr_b_id BIGINT NOT NULL
);

CREATE TABLE settlement (
    se_t_id BIGINT PRIMARY KEY,
    se_cash_type VARCHAR NOT NULL,
    se_cash_due_date VARCHAR NOT NULL,
    se_amt DOUBLE NOT NULL
);

CREATE TABLE cash_transaction (
    ct_t_id BIGINT PRIMARY KEY,
    ct_dts VARCHAR NOT NULL,
    ct_amt DOUBLE NOT NULL,
    ct_name VARCHAR NOT NULL
);

CREATE TABLE holding (
    h_t_id BIGINT PRIMARY KEY,
    h_ca_id BIGINT NOT NULL,
    h_s_symb VARCHAR NOT NULL,
    h_dts VARCHAR NOT NULL,
    h_price DOUBLE NOT NULL,
    h_qty INTEGER NOT NULL
);

CREATE TABLE holding_history (
    hh_h_t_id BIGINT NOT NULL,
    hh_t_id BIGINT NOT NULL,
    hh_before_qty INTEGER NOT NULL,
    hh_after_qty INTEGER NOT NULL,
    PRIMARY KEY (hh_h_t_id, hh_t_id)
);

CREATE TABLE holding_summary (
    hs_ca_id BIGINT NOT NULL,
    hs_s_symb VARCHAR NOT NULL,
    hs_qty INTEGER NOT NULL,
    PRIMARY KEY (hs_ca_id, hs_s_symb)
);
