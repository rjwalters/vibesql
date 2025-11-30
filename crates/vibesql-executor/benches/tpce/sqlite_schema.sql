-- TPC-E Schema for SQLite
-- 33 tables organized into Fixed, Customer, Market, and Trade categories

-- =========================================================================
-- Fixed Tables (9)
-- =========================================================================

CREATE TABLE charge (
    ch_tt_id TEXT NOT NULL,
    ch_c_tier INTEGER NOT NULL,
    ch_chrg REAL NOT NULL,
    PRIMARY KEY (ch_tt_id, ch_c_tier)
);

CREATE TABLE commission_rate (
    cr_c_tier INTEGER NOT NULL,
    cr_tt_id TEXT NOT NULL,
    cr_ex_id TEXT NOT NULL,
    cr_from_qty INTEGER NOT NULL,
    cr_to_qty INTEGER NOT NULL,
    cr_rate REAL NOT NULL,
    PRIMARY KEY (cr_c_tier, cr_tt_id, cr_ex_id, cr_from_qty)
);

CREATE TABLE exchange (
    ex_id TEXT PRIMARY KEY,
    ex_name TEXT NOT NULL,
    ex_num_symb INTEGER NOT NULL,
    ex_open INTEGER NOT NULL,
    ex_close INTEGER NOT NULL,
    ex_desc TEXT,
    ex_ad_id INTEGER NOT NULL
);

CREATE TABLE industry (
    in_id TEXT PRIMARY KEY,
    in_name TEXT NOT NULL,
    in_sc_id TEXT NOT NULL
);

CREATE TABLE sector (
    sc_id TEXT PRIMARY KEY,
    sc_name TEXT NOT NULL
);

CREATE TABLE status_type (
    st_id TEXT PRIMARY KEY,
    st_name TEXT NOT NULL
);

CREATE TABLE taxrate (
    tx_id TEXT PRIMARY KEY,
    tx_name TEXT NOT NULL,
    tx_rate REAL NOT NULL
);

CREATE TABLE trade_type (
    tt_id TEXT PRIMARY KEY,
    tt_name TEXT NOT NULL,
    tt_is_sell INTEGER NOT NULL,
    tt_is_mrkt INTEGER NOT NULL
);

CREATE TABLE zip_code (
    zc_code TEXT PRIMARY KEY,
    zc_town TEXT NOT NULL,
    zc_div TEXT NOT NULL
);

-- =========================================================================
-- Customer Tables (8)
-- =========================================================================

CREATE TABLE address (
    ad_id INTEGER PRIMARY KEY,
    ad_line1 TEXT NOT NULL,
    ad_line2 TEXT,
    ad_zc_code TEXT NOT NULL,
    ad_ctry TEXT NOT NULL
);

CREATE TABLE customer (
    c_id INTEGER PRIMARY KEY,
    c_tax_id TEXT NOT NULL,
    c_st_id TEXT NOT NULL,
    c_l_name TEXT NOT NULL,
    c_f_name TEXT NOT NULL,
    c_m_name TEXT,
    c_gndr TEXT,
    c_tier INTEGER NOT NULL,
    c_dob TEXT NOT NULL,
    c_ad_id INTEGER NOT NULL,
    c_ctry_1 TEXT,
    c_area_1 TEXT,
    c_local_1 TEXT,
    c_ext_1 TEXT,
    c_ctry_2 TEXT,
    c_area_2 TEXT,
    c_local_2 TEXT,
    c_ext_2 TEXT,
    c_ctry_3 TEXT,
    c_area_3 TEXT,
    c_local_3 TEXT,
    c_ext_3 TEXT,
    c_email_1 TEXT,
    c_email_2 TEXT
);

CREATE TABLE customer_account (
    ca_id INTEGER PRIMARY KEY,
    ca_b_id INTEGER NOT NULL,
    ca_c_id INTEGER NOT NULL,
    ca_name TEXT,
    ca_tax_st INTEGER NOT NULL,
    ca_bal REAL NOT NULL
);

CREATE TABLE customer_taxrate (
    cx_tx_id TEXT NOT NULL,
    cx_c_id INTEGER NOT NULL,
    PRIMARY KEY (cx_tx_id, cx_c_id)
);

CREATE TABLE account_permission (
    ap_ca_id INTEGER NOT NULL,
    ap_acl TEXT NOT NULL,
    ap_tax_id TEXT NOT NULL,
    ap_l_name TEXT NOT NULL,
    ap_f_name TEXT NOT NULL,
    PRIMARY KEY (ap_ca_id, ap_tax_id)
);

CREATE TABLE watch_list (
    wl_id INTEGER PRIMARY KEY,
    wl_c_id INTEGER NOT NULL
);

CREATE TABLE watch_item (
    wi_wl_id INTEGER NOT NULL,
    wi_s_symb TEXT NOT NULL,
    PRIMARY KEY (wi_wl_id, wi_s_symb)
);

CREATE TABLE broker (
    b_id INTEGER PRIMARY KEY,
    b_st_id TEXT NOT NULL,
    b_name TEXT NOT NULL,
    b_num_trades INTEGER NOT NULL,
    b_comm_total REAL NOT NULL
);

-- =========================================================================
-- Market Tables (8)
-- =========================================================================

CREATE TABLE company (
    co_id INTEGER PRIMARY KEY,
    co_st_id TEXT NOT NULL,
    co_name TEXT NOT NULL,
    co_in_id TEXT NOT NULL,
    co_sp_rate TEXT,
    co_ceo TEXT,
    co_ad_id INTEGER NOT NULL,
    co_desc TEXT,
    co_open_date TEXT NOT NULL
);

CREATE TABLE company_competitor (
    cp_co_id INTEGER NOT NULL,
    cp_comp_co_id INTEGER NOT NULL,
    cp_in_id TEXT NOT NULL,
    PRIMARY KEY (cp_co_id, cp_comp_co_id, cp_in_id)
);

CREATE TABLE security (
    s_symb TEXT PRIMARY KEY,
    s_issue TEXT NOT NULL,
    s_st_id TEXT NOT NULL,
    s_name TEXT NOT NULL,
    s_ex_id TEXT NOT NULL,
    s_co_id INTEGER NOT NULL,
    s_num_out INTEGER NOT NULL,
    s_start_date TEXT NOT NULL,
    s_exch_date TEXT NOT NULL,
    s_pe REAL,
    s_52wk_high REAL NOT NULL,
    s_52wk_high_date TEXT NOT NULL,
    s_52wk_low REAL NOT NULL,
    s_52wk_low_date TEXT NOT NULL,
    s_dividend REAL NOT NULL,
    s_yield REAL NOT NULL
);

CREATE TABLE daily_market (
    dm_date TEXT NOT NULL,
    dm_s_symb TEXT NOT NULL,
    dm_close REAL NOT NULL,
    dm_high REAL NOT NULL,
    dm_low REAL NOT NULL,
    dm_vol INTEGER NOT NULL,
    PRIMARY KEY (dm_date, dm_s_symb)
);

CREATE TABLE last_trade (
    lt_s_symb TEXT PRIMARY KEY,
    lt_dts TEXT NOT NULL,
    lt_price REAL NOT NULL,
    lt_open_price REAL NOT NULL,
    lt_vol INTEGER NOT NULL
);

CREATE TABLE financial (
    fi_co_id INTEGER NOT NULL,
    fi_year INTEGER NOT NULL,
    fi_qtr INTEGER NOT NULL,
    fi_qtr_start_date TEXT NOT NULL,
    fi_revenue REAL NOT NULL,
    fi_net_earn REAL NOT NULL,
    fi_basic_eps REAL NOT NULL,
    fi_dilut_eps REAL NOT NULL,
    fi_margin REAL NOT NULL,
    fi_inventory REAL NOT NULL,
    fi_assets REAL NOT NULL,
    fi_liability REAL NOT NULL,
    fi_out_basic INTEGER NOT NULL,
    fi_out_dilut INTEGER NOT NULL,
    PRIMARY KEY (fi_co_id, fi_year, fi_qtr)
);

CREATE TABLE news_item (
    ni_id INTEGER PRIMARY KEY,
    ni_headline TEXT NOT NULL,
    ni_summary TEXT NOT NULL,
    ni_item TEXT NOT NULL,
    ni_dts TEXT NOT NULL,
    ni_source TEXT NOT NULL,
    ni_author TEXT
);

CREATE TABLE news_xref (
    nx_ni_id INTEGER NOT NULL,
    nx_co_id INTEGER NOT NULL,
    PRIMARY KEY (nx_ni_id, nx_co_id)
);

-- =========================================================================
-- Trade Tables (8)
-- =========================================================================

CREATE TABLE trade (
    t_id INTEGER PRIMARY KEY,
    t_dts TEXT NOT NULL,
    t_st_id TEXT NOT NULL,
    t_tt_id TEXT NOT NULL,
    t_is_cash INTEGER NOT NULL,
    t_s_symb TEXT NOT NULL,
    t_qty INTEGER NOT NULL,
    t_bid_price REAL NOT NULL,
    t_ca_id INTEGER NOT NULL,
    t_exec_name TEXT NOT NULL,
    t_trade_price REAL,
    t_chrg REAL NOT NULL,
    t_comm REAL NOT NULL,
    t_tax REAL NOT NULL,
    t_lifo INTEGER NOT NULL
);

CREATE TABLE trade_history (
    th_t_id INTEGER NOT NULL,
    th_dts TEXT NOT NULL,
    th_st_id TEXT NOT NULL,
    PRIMARY KEY (th_t_id, th_st_id)
);

CREATE TABLE trade_request (
    tr_t_id INTEGER PRIMARY KEY,
    tr_tt_id TEXT NOT NULL,
    tr_s_symb TEXT NOT NULL,
    tr_qty INTEGER NOT NULL,
    tr_bid_price REAL NOT NULL,
    tr_b_id INTEGER NOT NULL
);

CREATE TABLE settlement (
    se_t_id INTEGER PRIMARY KEY,
    se_cash_type TEXT NOT NULL,
    se_cash_due_date TEXT NOT NULL,
    se_amt REAL NOT NULL
);

CREATE TABLE cash_transaction (
    ct_t_id INTEGER PRIMARY KEY,
    ct_dts TEXT NOT NULL,
    ct_amt REAL NOT NULL,
    ct_name TEXT NOT NULL
);

CREATE TABLE holding (
    h_t_id INTEGER PRIMARY KEY,
    h_ca_id INTEGER NOT NULL,
    h_s_symb TEXT NOT NULL,
    h_dts TEXT NOT NULL,
    h_price REAL NOT NULL,
    h_qty INTEGER NOT NULL
);

CREATE TABLE holding_history (
    hh_h_t_id INTEGER NOT NULL,
    hh_t_id INTEGER NOT NULL,
    hh_before_qty INTEGER NOT NULL,
    hh_after_qty INTEGER NOT NULL,
    PRIMARY KEY (hh_h_t_id, hh_t_id)
);

CREATE TABLE holding_summary (
    hs_ca_id INTEGER NOT NULL,
    hs_s_symb TEXT NOT NULL,
    hs_qty INTEGER NOT NULL,
    PRIMARY KEY (hs_ca_id, hs_s_symb)
);
