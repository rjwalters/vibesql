//! TPC-E Data Generation
//!
//! Generates realistic TPC-E data following the specification.
//! Scale factor = number of customers / 1000 (SF=1 means 1000 customers).
//!
//! Data cardinalities (SF=1 = 1000 customers):
//! - CUSTOMER: 1,000 rows
//! - CUSTOMER_ACCOUNT: ~5,000 rows (avg 5 per customer)
//! - HOLDING: ~50,000 rows (avg 10 per account)
//! - TRADE: variable (grows during benchmark)
//! - SECURITY: 6,850 rows (fixed)
//! - COMPANY: 5,000 rows (fixed)
//!
//! Fixed tables have constant cardinalities regardless of scale:
//! - CHARGE: 15 rows
//! - COMMISSION_RATE: 240 rows
//! - EXCHANGE: 4 rows
//! - INDUSTRY: 102 rows
//! - SECTOR: 12 rows
//! - STATUS_TYPE: 5 rows
//! - TAXRATE: 320 rows
//! - TRADE_TYPE: 5 rows
//! - ZIP_CODE: 14,741 rows

/// Random number generator state for reproducible data
pub struct TPCERng {
    state: u64,
}

impl TPCERng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Linear congruential generator
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    /// Random integer in range [min, max]
    pub fn random_int(&mut self, min: i64, max: i64) -> i64 {
        let range = (max - min + 1) as u64;
        min + (self.next() % range) as i64
    }

    /// Random f64 in range [min, max]
    pub fn random_float(&mut self, min: f64, max: f64) -> f64 {
        let range = max - min;
        min + (self.next() as f64 / u64::MAX as f64) * range
    }

    /// Random decimal with specified scale
    pub fn random_decimal(&mut self, min: f64, max: f64, scale: u32) -> f64 {
        let multiplier = 10_f64.powi(scale as i32);
        let min_scaled = (min * multiplier) as i64;
        let max_scaled = (max * multiplier) as i64;
        let val = self.random_int(min_scaled, max_scaled);
        val as f64 / multiplier
    }

    /// Random boolean with given probability of true
    pub fn random_bool(&mut self, prob: f64) -> bool {
        self.random_float(0.0, 1.0) < prob
    }

    /// Generate random alphanumeric string
    pub fn random_astring(&mut self, min_len: usize, max_len: usize) -> String {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let len = self.random_int(min_len as i64, max_len as i64) as usize;
        (0..len)
            .map(|_| CHARS[self.random_int(0, CHARS.len() as i64 - 1) as usize] as char)
            .collect()
    }

    /// Generate random numeric string
    pub fn random_nstring(&mut self, min_len: usize, max_len: usize) -> String {
        const DIGITS: &[u8] = b"0123456789";
        let len = self.random_int(min_len as i64, max_len as i64) as usize;
        (0..len).map(|_| DIGITS[self.random_int(0, 9) as usize] as char).collect()
    }

    /// Generate random email address
    pub fn random_email(&mut self) -> String {
        let user = self.random_astring(5, 10).to_lowercase();
        let domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "broker.net"];
        let domain = domains[self.random_int(0, domains.len() as i64 - 1) as usize];
        format!("{}@{}", user, domain)
    }

    /// Generate random phone number
    pub fn random_phone(&mut self) -> String {
        format!(
            "({}) {}-{}",
            self.random_nstring(3, 3),
            self.random_nstring(3, 3),
            self.random_nstring(4, 4)
        )
    }

    /// Generate random date in YYYY-MM-DD format
    pub fn random_date(&mut self, start_year: i32, end_year: i32) -> String {
        let year = self.random_int(start_year as i64, end_year as i64) as i32;
        let month = self.random_int(1, 12) as i32;
        let day = self.random_int(1, 28) as i32; // Safe for all months
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    /// Generate random timestamp
    pub fn random_timestamp(&mut self) -> String {
        let date = self.random_date(2020, 2024);
        let hour = self.random_int(0, 23);
        let min = self.random_int(0, 59);
        let sec = self.random_int(0, 59);
        format!("{} {:02}:{:02}:{:02}", date, hour, min, sec)
    }

    /// Current timestamp string
    pub fn current_timestamp() -> String {
        "2024-01-01 09:30:00".to_string()
    }

    /// Pick random element from slice
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.random_int(0, items.len() as i64 - 1) as usize]
    }
}

// =============================================================================
// Fixed Table Records
// =============================================================================

/// CHARGE table: Trade charge rates
#[derive(Debug, Clone)]
pub struct Charge {
    pub ch_tt_id: String, // Trade type ID
    pub ch_c_tier: i32,   // Customer tier (1-3)
    pub ch_chrg: f64,     // Charge amount
}

/// COMMISSION_RATE table: Broker commission rates
#[derive(Debug, Clone)]
pub struct CommissionRate {
    pub cr_c_tier: i32,   // Customer tier
    pub cr_tt_id: String, // Trade type ID
    pub cr_ex_id: String, // Exchange ID
    pub cr_from_qty: i32, // From quantity
    pub cr_to_qty: i32,   // To quantity
    pub cr_rate: f64,     // Commission rate
}

/// EXCHANGE table: Stock exchanges
#[derive(Debug, Clone)]
pub struct Exchange {
    pub ex_id: String,    // Exchange ID (e.g., "NYSE")
    pub ex_name: String,  // Exchange name
    pub ex_num_symb: i32, // Number of symbols
    pub ex_open: i32,     // Opening time (HHMM)
    pub ex_close: i32,    // Closing time (HHMM)
    pub ex_desc: String,  // Description
    pub ex_ad_id: i64,    // Address ID
}

/// INDUSTRY table: Industry classifications
#[derive(Debug, Clone)]
pub struct Industry {
    pub in_id: String,    // Industry ID
    pub in_name: String,  // Industry name
    pub in_sc_id: String, // Sector ID
}

/// SECTOR table: Economic sectors
#[derive(Debug, Clone)]
pub struct Sector {
    pub sc_id: String,   // Sector ID
    pub sc_name: String, // Sector name
}

/// STATUS_TYPE table: Status codes
#[derive(Debug, Clone)]
pub struct StatusType {
    pub st_id: String,   // Status ID
    pub st_name: String, // Status name
}

/// TAXRATE table: Tax rates
#[derive(Debug, Clone)]
pub struct Taxrate {
    pub tx_id: String,   // Tax rate ID
    pub tx_name: String, // Tax name
    pub tx_rate: f64,    // Tax rate
}

/// TRADE_TYPE table: Types of trades
#[derive(Debug, Clone)]
pub struct TradeType {
    pub tt_id: String,    // Trade type ID
    pub tt_name: String,  // Trade type name
    pub tt_is_sell: bool, // Is sell order
    pub tt_is_mrkt: bool, // Is market order
}

/// ZIP_CODE table: ZIP codes
#[derive(Debug, Clone)]
pub struct ZipCode {
    pub zc_code: String, // ZIP code
    pub zc_town: String, // Town name
    pub zc_div: String,  // Division (state)
}

// =============================================================================
// Customer Table Records
// =============================================================================

/// CUSTOMER table: Customer information
#[derive(Debug, Clone)]
pub struct Customer {
    pub c_id: i64,         // Customer ID
    pub c_tax_id: String,  // Tax ID (SSN)
    pub c_st_id: String,   // Status ID
    pub c_l_name: String,  // Last name
    pub c_f_name: String,  // First name
    pub c_m_name: String,  // Middle name
    pub c_gndr: String,    // Gender (M/F)
    pub c_tier: i32,       // Customer tier (1-3)
    pub c_dob: String,     // Date of birth
    pub c_ad_id: i64,      // Address ID
    pub c_ctry_1: String,  // Country code 1
    pub c_area_1: String,  // Area code 1
    pub c_local_1: String, // Local number 1
    pub c_ext_1: String,   // Extension 1
    pub c_ctry_2: String,  // Country code 2
    pub c_area_2: String,  // Area code 2
    pub c_local_2: String, // Local number 2
    pub c_ext_2: String,   // Extension 2
    pub c_ctry_3: String,  // Country code 3
    pub c_area_3: String,  // Area code 3
    pub c_local_3: String, // Local number 3
    pub c_ext_3: String,   // Extension 3
    pub c_email_1: String, // Email 1
    pub c_email_2: String, // Email 2
}

/// CUSTOMER_ACCOUNT table: Brokerage accounts
#[derive(Debug, Clone)]
pub struct CustomerAccount {
    pub ca_id: i64,      // Account ID
    pub ca_b_id: i64,    // Broker ID
    pub ca_c_id: i64,    // Customer ID
    pub ca_name: String, // Account name
    pub ca_tax_st: i32,  // Tax status (0-2)
    pub ca_bal: f64,     // Account balance
}

/// CUSTOMER_TAXRATE table: Customer tax rates
#[derive(Debug, Clone)]
pub struct CustomerTaxrate {
    pub cx_tx_id: String, // Tax rate ID
    pub cx_c_id: i64,     // Customer ID
}

/// ACCOUNT_PERMISSION table: Account access permissions
#[derive(Debug, Clone)]
pub struct AccountPermission {
    pub ap_ca_id: i64,     // Account ID
    pub ap_acl: String,    // Access control list
    pub ap_tax_id: String, // Tax ID of authorized person
    pub ap_l_name: String, // Last name
    pub ap_f_name: String, // First name
}

/// WATCH_LIST table: Customer watch lists
#[derive(Debug, Clone)]
pub struct WatchList {
    pub wl_id: i64,   // Watch list ID
    pub wl_c_id: i64, // Customer ID
}

/// WATCH_ITEM table: Securities on watch lists
#[derive(Debug, Clone)]
pub struct WatchItem {
    pub wi_wl_id: i64,     // Watch list ID
    pub wi_s_symb: String, // Security symbol
}

/// ADDRESS table: Addresses
#[derive(Debug, Clone)]
pub struct Address {
    pub ad_id: i64,         // Address ID
    pub ad_line1: String,   // Address line 1
    pub ad_line2: String,   // Address line 2
    pub ad_zc_code: String, // ZIP code
    pub ad_ctry: String,    // Country
}

/// BROKER table: Broker information
#[derive(Debug, Clone)]
pub struct Broker {
    pub b_id: i64,         // Broker ID
    pub b_st_id: String,   // Status ID
    pub b_name: String,    // Broker name
    pub b_num_trades: i32, // Number of trades
    pub b_comm_total: f64, // Total commission
}

// =============================================================================
// Market Table Records
// =============================================================================

/// COMPANY table: Publicly traded companies
#[derive(Debug, Clone)]
pub struct Company {
    pub co_id: i64,           // Company ID
    pub co_st_id: String,     // Status ID
    pub co_name: String,      // Company name
    pub co_in_id: String,     // Industry ID
    pub co_sp_rate: String,   // S&P rating
    pub co_ceo: String,       // CEO name
    pub co_ad_id: i64,        // Address ID
    pub co_desc: String,      // Description
    pub co_open_date: String, // IPO date
}

/// COMPANY_COMPETITOR table: Company competitors
#[derive(Debug, Clone)]
pub struct CompanyCompetitor {
    pub cp_co_id: i64,      // Company ID
    pub cp_comp_co_id: i64, // Competitor company ID
    pub cp_in_id: String,   // Industry ID
}

/// SECURITY table: Tradeable securities
#[derive(Debug, Clone)]
pub struct Security {
    pub s_symb: String,           // Security symbol
    pub s_issue: String,          // Issue type
    pub s_st_id: String,          // Status ID
    pub s_name: String,           // Security name
    pub s_ex_id: String,          // Exchange ID
    pub s_co_id: i64,             // Company ID
    pub s_num_out: i64,           // Shares outstanding
    pub s_start_date: String,     // Start date
    pub s_exch_date: String,      // Exchange date
    pub s_pe: f64,                // P/E ratio
    pub s_52wk_high: f64,         // 52-week high
    pub s_52wk_high_date: String, // 52-week high date
    pub s_52wk_low: f64,          // 52-week low
    pub s_52wk_low_date: String,  // 52-week low date
    pub s_dividend: f64,          // Dividend
    pub s_yield: f64,             // Yield
}

/// DAILY_MARKET table: Daily market data
#[derive(Debug, Clone)]
pub struct DailyMarket {
    pub dm_date: String,   // Date
    pub dm_s_symb: String, // Security symbol
    pub dm_close: f64,     // Closing price
    pub dm_high: f64,      // High price
    pub dm_low: f64,       // Low price
    pub dm_vol: i64,       // Volume
}

/// LAST_TRADE table: Most recent trade info
#[derive(Debug, Clone)]
pub struct LastTrade {
    pub lt_s_symb: String,  // Security symbol
    pub lt_dts: String,     // Timestamp
    pub lt_price: f64,      // Last price
    pub lt_open_price: f64, // Opening price
    pub lt_vol: i64,        // Volume
}

/// FINANCIAL table: Company financial data
#[derive(Debug, Clone)]
pub struct Financial {
    pub fi_co_id: i64,             // Company ID
    pub fi_year: i32,              // Fiscal year
    pub fi_qtr: i32,               // Fiscal quarter
    pub fi_qtr_start_date: String, // Quarter start date
    pub fi_revenue: f64,           // Revenue
    pub fi_net_earn: f64,          // Net earnings
    pub fi_basic_eps: f64,         // Basic EPS
    pub fi_dilut_eps: f64,         // Diluted EPS
    pub fi_margin: f64,            // Margin
    pub fi_inventory: f64,         // Inventory
    pub fi_assets: f64,            // Assets
    pub fi_liability: f64,         // Liabilities
    pub fi_out_basic: i64,         // Basic shares outstanding
    pub fi_out_dilut: i64,         // Diluted shares outstanding
}

/// NEWS_ITEM table: News articles
#[derive(Debug, Clone)]
pub struct NewsItem {
    pub ni_id: i64,          // News item ID
    pub ni_headline: String, // Headline
    pub ni_summary: String,  // Summary
    pub ni_item: String,     // Full article
    pub ni_dts: String,      // Timestamp
    pub ni_source: String,   // Source
    pub ni_author: String,   // Author
}

/// NEWS_XREF table: News-company associations
#[derive(Debug, Clone)]
pub struct NewsXref {
    pub nx_ni_id: i64, // News item ID
    pub nx_co_id: i64, // Company ID
}

// =============================================================================
// Trade Table Records
// =============================================================================

/// TRADE table: All trades
#[derive(Debug, Clone)]
pub struct Trade {
    pub t_id: i64,           // Trade ID
    pub t_dts: String,       // Timestamp
    pub t_st_id: String,     // Status ID
    pub t_tt_id: String,     // Trade type ID
    pub t_is_cash: bool,     // Is cash trade
    pub t_s_symb: String,    // Security symbol
    pub t_qty: i32,          // Quantity
    pub t_bid_price: f64,    // Bid price
    pub t_ca_id: i64,        // Customer account ID
    pub t_exec_name: String, // Executor name
    pub t_trade_price: f64,  // Trade price (null if not executed)
    pub t_chrg: f64,         // Charge
    pub t_comm: f64,         // Commission
    pub t_tax: f64,          // Tax
    pub t_lifo: bool,        // LIFO flag
}

/// TRADE_HISTORY table: Trade status history
#[derive(Debug, Clone)]
pub struct TradeHistory {
    pub th_t_id: i64,     // Trade ID
    pub th_dts: String,   // Timestamp
    pub th_st_id: String, // Status ID
}

/// TRADE_REQUEST table: Pending limit orders
#[derive(Debug, Clone)]
pub struct TradeRequest {
    pub tr_t_id: i64,      // Trade ID
    pub tr_tt_id: String,  // Trade type ID
    pub tr_s_symb: String, // Security symbol
    pub tr_qty: i32,       // Quantity
    pub tr_bid_price: f64, // Bid price
    pub tr_b_id: i64,      // Broker ID
}

/// SETTLEMENT table: Trade settlements
#[derive(Debug, Clone)]
pub struct Settlement {
    pub se_t_id: i64,             // Trade ID
    pub se_cash_type: String,     // Cash type
    pub se_cash_due_date: String, // Due date
    pub se_amt: f64,              // Amount
}

/// CASH_TRANSACTION table: Cash movements
#[derive(Debug, Clone)]
pub struct CashTransaction {
    pub ct_t_id: i64,    // Trade ID
    pub ct_dts: String,  // Timestamp
    pub ct_amt: f64,     // Amount
    pub ct_name: String, // Transaction name
}

/// HOLDING table: Current holdings
#[derive(Debug, Clone)]
pub struct Holding {
    pub h_t_id: i64,      // Trade ID (original buy)
    pub h_ca_id: i64,     // Customer account ID
    pub h_s_symb: String, // Security symbol
    pub h_dts: String,    // Timestamp
    pub h_price: f64,     // Purchase price
    pub h_qty: i32,       // Quantity
}

/// HOLDING_HISTORY table: Holding changes
#[derive(Debug, Clone)]
pub struct HoldingHistory {
    pub hh_h_t_id: i64,     // Holding trade ID
    pub hh_t_id: i64,       // Trade ID that caused change
    pub hh_before_qty: i32, // Quantity before
    pub hh_after_qty: i32,  // Quantity after
}

/// HOLDING_SUMMARY table: Aggregated holdings
#[derive(Debug, Clone)]
pub struct HoldingSummary {
    pub hs_ca_id: i64,     // Customer account ID
    pub hs_s_symb: String, // Security symbol
    pub hs_qty: i32,       // Total quantity
}

// =============================================================================
// TPC-E Data Generator
// =============================================================================

/// TPC-E data generator
pub struct TPCEData {
    pub scale_factor: i32, // Number of customers / 1000
    raw_scale: f64,        // Raw scale factor
    pub rng: TPCERng,
    next_trade_id: i64,
    next_address_id: i64,
}

impl TPCEData {
    // Fixed table sizes (constant regardless of scale)
    pub const NUM_EXCHANGES: i32 = 4;
    pub const NUM_SECTORS: i32 = 12;
    pub const NUM_INDUSTRIES: i32 = 102;
    pub const NUM_STATUS_TYPES: i32 = 5;
    pub const NUM_TRADE_TYPES: i32 = 5;
    pub const NUM_TAXRATES: i32 = 320;
    pub const NUM_CHARGE_TIERS: i32 = 3;
    pub const NUM_ZIP_CODES: i32 = 1000; // Simplified from 14741
    pub const NUM_COMPANIES: i32 = 5000;
    pub const NUM_SECURITIES: i32 = 6850;

    // Customer-related scaling
    pub const CUSTOMERS_PER_SF: i32 = 1000;
    pub const ACCOUNTS_PER_CUSTOMER: i32 = 5;
    pub const HOLDINGS_PER_ACCOUNT: i32 = 10;
    pub const TRADES_PER_ACCOUNT: i32 = 50;
    pub const WATCH_LISTS_PER_CUSTOMER: i32 = 1;
    pub const ITEMS_PER_WATCH_LIST: i32 = 20;

    pub fn new(scale_factor: f64) -> Self {
        Self {
            scale_factor: scale_factor.max(0.001) as i32 + 1,
            raw_scale: scale_factor.max(0.001),
            rng: TPCERng::new(42),
            next_trade_id: 1,
            next_address_id: 1,
        }
    }

    pub fn is_micro_mode(&self) -> bool {
        self.raw_scale < 1.0
    }

    /// Number of customers
    pub fn num_customers(&self) -> i32 {
        ((Self::CUSTOMERS_PER_SF as f64 * self.raw_scale) as i32).max(10)
    }

    /// Number of brokers (~1 per 100 customers)
    pub fn num_brokers(&self) -> i32 {
        (self.num_customers() / 100).max(1)
    }

    /// Number of customer accounts
    pub fn num_accounts(&self) -> i32 {
        self.num_customers() * Self::ACCOUNTS_PER_CUSTOMER
    }

    /// Number of companies (fixed, but can be scaled for micro mode)
    pub fn num_companies(&self) -> i32 {
        if self.raw_scale < 1.0 {
            ((Self::NUM_COMPANIES as f64 * self.raw_scale) as i32).max(100)
        } else {
            Self::NUM_COMPANIES
        }
    }

    /// Number of securities (fixed, but can be scaled for micro mode)
    pub fn num_securities(&self) -> i32 {
        if self.raw_scale < 1.0 {
            ((Self::NUM_SECURITIES as f64 * self.raw_scale) as i32).max(100)
        } else {
            Self::NUM_SECURITIES
        }
    }

    /// Get next address ID
    pub fn next_address_id(&mut self) -> i64 {
        let id = self.next_address_id;
        self.next_address_id += 1;
        id
    }

    /// Get next trade ID
    pub fn next_trade_id(&mut self) -> i64 {
        let id = self.next_trade_id;
        self.next_trade_id += 1;
        id
    }
}

// =============================================================================
// Fixed Table Generators
// =============================================================================

impl TPCEData {
    /// Generate exchange records
    pub fn gen_exchanges(&mut self) -> Vec<Exchange> {
        vec![
            Exchange {
                ex_id: "NYSE".to_string(),
                ex_name: "New York Stock Exchange".to_string(),
                ex_num_symb: 3000,
                ex_open: 930,
                ex_close: 1600,
                ex_desc: "The largest stock exchange in the world".to_string(),
                ex_ad_id: self.next_address_id(),
            },
            Exchange {
                ex_id: "NASDAQ".to_string(),
                ex_name: "NASDAQ Stock Market".to_string(),
                ex_num_symb: 3500,
                ex_open: 930,
                ex_close: 1600,
                ex_desc: "Electronic stock market".to_string(),
                ex_ad_id: self.next_address_id(),
            },
            Exchange {
                ex_id: "AMEX".to_string(),
                ex_name: "American Stock Exchange".to_string(),
                ex_num_symb: 350,
                ex_open: 930,
                ex_close: 1600,
                ex_desc: "American Stock Exchange".to_string(),
                ex_ad_id: self.next_address_id(),
            },
            Exchange {
                ex_id: "PCX".to_string(),
                ex_name: "Pacific Exchange".to_string(),
                ex_num_symb: 0,
                ex_open: 930,
                ex_close: 1600,
                ex_desc: "Pacific Exchange".to_string(),
                ex_ad_id: self.next_address_id(),
            },
        ]
    }

    /// Generate sector records
    pub fn gen_sectors(&mut self) -> Vec<Sector> {
        let sectors = [
            ("SC01", "Energy"),
            ("SC02", "Materials"),
            ("SC03", "Industrials"),
            ("SC04", "Consumer Discretionary"),
            ("SC05", "Consumer Staples"),
            ("SC06", "Health Care"),
            ("SC07", "Financials"),
            ("SC08", "Information Technology"),
            ("SC09", "Telecommunication Services"),
            ("SC10", "Utilities"),
            ("SC11", "Real Estate"),
            ("SC12", "Other"),
        ];
        sectors
            .iter()
            .map(|(id, name)| Sector { sc_id: id.to_string(), sc_name: name.to_string() })
            .collect()
    }

    /// Generate industry records
    pub fn gen_industries(&mut self) -> Vec<Industry> {
        let industries = [
            ("IN0101", "Oil & Gas Drilling", "SC01"),
            ("IN0102", "Oil & Gas Equipment", "SC01"),
            ("IN0103", "Integrated Oil & Gas", "SC01"),
            ("IN0201", "Aluminum", "SC02"),
            ("IN0202", "Chemicals", "SC02"),
            ("IN0203", "Gold", "SC02"),
            ("IN0301", "Aerospace & Defense", "SC03"),
            ("IN0302", "Building Products", "SC03"),
            ("IN0303", "Construction", "SC03"),
            ("IN0401", "Auto Components", "SC04"),
            ("IN0402", "Automobiles", "SC04"),
            ("IN0403", "Hotels & Restaurants", "SC04"),
            ("IN0501", "Beverages", "SC05"),
            ("IN0502", "Food Products", "SC05"),
            ("IN0503", "Tobacco", "SC05"),
            ("IN0601", "Biotechnology", "SC06"),
            ("IN0602", "Health Care Equipment", "SC06"),
            ("IN0603", "Pharmaceuticals", "SC06"),
            ("IN0701", "Banks", "SC07"),
            ("IN0702", "Insurance", "SC07"),
            ("IN0703", "Diversified Financials", "SC07"),
            ("IN0801", "Internet Software", "SC08"),
            ("IN0802", "IT Services", "SC08"),
            ("IN0803", "Semiconductors", "SC08"),
            ("IN0901", "Diversified Telecom", "SC09"),
            ("IN0902", "Wireless Telecom", "SC09"),
            ("IN1001", "Electric Utilities", "SC10"),
            ("IN1002", "Gas Utilities", "SC10"),
            ("IN1101", "Real Estate", "SC11"),
            ("IN1201", "Other", "SC12"),
        ];
        industries
            .iter()
            .map(|(id, name, sc_id)| Industry {
                in_id: id.to_string(),
                in_name: name.to_string(),
                in_sc_id: sc_id.to_string(),
            })
            .collect()
    }

    /// Generate status type records
    pub fn gen_status_types(&mut self) -> Vec<StatusType> {
        vec![
            StatusType { st_id: "ACTV".to_string(), st_name: "Active".to_string() },
            StatusType { st_id: "CMPT".to_string(), st_name: "Completed".to_string() },
            StatusType { st_id: "CNCL".to_string(), st_name: "Canceled".to_string() },
            StatusType { st_id: "PNDG".to_string(), st_name: "Pending".to_string() },
            StatusType { st_id: "SBMT".to_string(), st_name: "Submitted".to_string() },
        ]
    }

    /// Generate trade type records
    pub fn gen_trade_types(&mut self) -> Vec<TradeType> {
        vec![
            TradeType {
                tt_id: "TMB".to_string(),
                tt_name: "Market-Buy".to_string(),
                tt_is_sell: false,
                tt_is_mrkt: true,
            },
            TradeType {
                tt_id: "TMS".to_string(),
                tt_name: "Market-Sell".to_string(),
                tt_is_sell: true,
                tt_is_mrkt: true,
            },
            TradeType {
                tt_id: "TSL".to_string(),
                tt_name: "Stop-Loss".to_string(),
                tt_is_sell: true,
                tt_is_mrkt: false,
            },
            TradeType {
                tt_id: "TLB".to_string(),
                tt_name: "Limit-Buy".to_string(),
                tt_is_sell: false,
                tt_is_mrkt: false,
            },
            TradeType {
                tt_id: "TLS".to_string(),
                tt_name: "Limit-Sell".to_string(),
                tt_is_sell: true,
                tt_is_mrkt: false,
            },
        ]
    }

    /// Generate charge records
    pub fn gen_charges(&mut self) -> Vec<Charge> {
        let mut charges = Vec::new();
        let trade_types = ["TMB", "TMS", "TSL", "TLB", "TLS"];
        for tt_id in &trade_types {
            for tier in 1..=3 {
                let base = match tier {
                    1 => 15.0,
                    2 => 12.0,
                    _ => 10.0,
                };
                charges.push(Charge {
                    ch_tt_id: tt_id.to_string(),
                    ch_c_tier: tier,
                    ch_chrg: base,
                });
            }
        }
        charges
    }

    /// Generate taxrate records
    pub fn gen_taxrates(&mut self) -> Vec<Taxrate> {
        let mut rates = Vec::new();
        // US states
        let states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"];
        for state in &states {
            rates.push(Taxrate {
                tx_id: format!("US_{}", state),
                tx_name: format!("US {} Tax", state),
                tx_rate: self.rng.random_decimal(0.01, 0.10, 4),
            });
        }
        // Countries
        let countries = [("UK", 0.20), ("DE", 0.25), ("FR", 0.30), ("JP", 0.20), ("CA", 0.15)];
        for (country, rate) in &countries {
            rates.push(Taxrate {
                tx_id: format!("{}_FED", country),
                tx_name: format!("{} Federal Tax", country),
                tx_rate: *rate,
            });
        }
        rates
    }

    /// Generate ZIP code records (simplified)
    pub fn gen_zip_codes(&mut self) -> Vec<ZipCode> {
        let mut codes = Vec::new();
        let states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"];
        for i in 0..Self::NUM_ZIP_CODES {
            let state = states[(i % states.len() as i32) as usize];
            codes.push(ZipCode {
                zc_code: format!("{:05}", 10000 + i),
                zc_town: format!("City{}", i),
                zc_div: state.to_string(),
            });
        }
        codes
    }
}

// =============================================================================
// Customer Table Generators
// =============================================================================

impl TPCEData {
    /// Generate address record
    pub fn gen_address(&mut self) -> Address {
        let ad_id = self.next_address_id();
        Address {
            ad_id,
            ad_line1: format!(
                "{} {} St",
                self.rng.random_int(1, 9999),
                self.rng.random_astring(5, 10)
            ),
            ad_line2: if self.rng.random_bool(0.3) {
                format!("Suite {}", self.rng.random_int(100, 999))
            } else {
                String::new()
            },
            ad_zc_code: format!("{:05}", self.rng.random_int(10000, 99999)),
            ad_ctry: "USA".to_string(),
        }
    }

    /// Generate customer record
    pub fn gen_customer(&mut self, c_id: i64) -> Customer {
        let first_names = [
            "James",
            "John",
            "Robert",
            "Michael",
            "William",
            "Mary",
            "Patricia",
            "Jennifer",
            "Linda",
            "Elizabeth",
        ];
        let last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
        ];

        Customer {
            c_id,
            c_tax_id: format!(
                "{}-{}-{}",
                self.rng.random_nstring(3, 3),
                self.rng.random_nstring(2, 2),
                self.rng.random_nstring(4, 4)
            ),
            c_st_id: "ACTV".to_string(),
            c_l_name: self.rng.pick(&last_names).to_string(),
            c_f_name: self.rng.pick(&first_names).to_string(),
            c_m_name: self.rng.random_astring(1, 1).to_uppercase(),
            c_gndr: if self.rng.random_bool(0.5) { "M" } else { "F" }.to_string(),
            c_tier: self.rng.random_int(1, 3) as i32,
            c_dob: self.rng.random_date(1940, 2000),
            c_ad_id: self.next_address_id(),
            c_ctry_1: "1".to_string(),
            c_area_1: self.rng.random_nstring(3, 3),
            c_local_1: self.rng.random_nstring(7, 7),
            c_ext_1: String::new(),
            c_ctry_2: "1".to_string(),
            c_area_2: self.rng.random_nstring(3, 3),
            c_local_2: self.rng.random_nstring(7, 7),
            c_ext_2: String::new(),
            c_ctry_3: String::new(),
            c_area_3: String::new(),
            c_local_3: String::new(),
            c_ext_3: String::new(),
            c_email_1: self.rng.random_email(),
            c_email_2: self.rng.random_email(),
        }
    }

    /// Generate customer account record
    pub fn gen_customer_account(&mut self, ca_id: i64, c_id: i64, b_id: i64) -> CustomerAccount {
        CustomerAccount {
            ca_id,
            ca_b_id: b_id,
            ca_c_id: c_id,
            ca_name: format!("Account-{}-{}", c_id, ca_id % 10),
            ca_tax_st: self.rng.random_int(0, 2) as i32,
            ca_bal: self.rng.random_decimal(1000.0, 100000.0, 2),
        }
    }

    /// Generate broker record
    pub fn gen_broker(&mut self, b_id: i64) -> Broker {
        let first_names = ["James", "John", "Robert", "Michael", "William"];
        let last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones"];
        Broker {
            b_id,
            b_st_id: "ACTV".to_string(),
            b_name: format!("{} {}", self.rng.pick(&first_names), self.rng.pick(&last_names)),
            b_num_trades: self.rng.random_int(0, 10000) as i32,
            b_comm_total: self.rng.random_decimal(0.0, 100000.0, 2),
        }
    }

    /// Generate watch list record
    pub fn gen_watch_list(&mut self, wl_id: i64, c_id: i64) -> WatchList {
        WatchList { wl_id, wl_c_id: c_id }
    }
}

// =============================================================================
// Market Table Generators
// =============================================================================

impl TPCEData {
    /// Generate company record
    pub fn gen_company(&mut self, co_id: i64) -> Company {
        let ceo_names =
            ["John Smith", "Jane Doe", "Bob Johnson", "Alice Williams", "Charlie Brown"];
        let sp_ratings = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"];
        let industries: Vec<String> = (1..=30).map(|i| format!("IN{:04}", i)).collect();

        Company {
            co_id,
            co_st_id: "ACTV".to_string(),
            co_name: format!("Company{} Inc", co_id),
            co_in_id: industries[self.rng.random_int(0, industries.len() as i64 - 1) as usize]
                .clone(),
            co_sp_rate: self.rng.pick(&sp_ratings).to_string(),
            co_ceo: self.rng.pick(&ceo_names).to_string(),
            co_ad_id: self.next_address_id(),
            co_desc: format!(
                "A leading company in its industry, Company{} has been serving customers since {}.",
                co_id,
                self.rng.random_int(1950, 2020)
            ),
            co_open_date: self.rng.random_date(1950, 2020),
        }
    }

    /// Generate security record
    pub fn gen_security(&mut self, s_symb: &str, co_id: i64) -> Security {
        let exchanges = ["NYSE", "NASDAQ", "AMEX", "PCX"];
        let price = self.rng.random_decimal(5.0, 500.0, 2);
        let high = price * self.rng.random_float(1.1, 1.5);
        let low = price * self.rng.random_float(0.5, 0.9);

        Security {
            s_symb: s_symb.to_string(),
            s_issue: "COMMON".to_string(),
            s_st_id: "ACTV".to_string(),
            s_name: format!("{} Common Stock", s_symb),
            s_ex_id: self.rng.pick(&exchanges).to_string(),
            s_co_id: co_id,
            s_num_out: self.rng.random_int(1_000_000, 1_000_000_000),
            s_start_date: self.rng.random_date(1980, 2020),
            s_exch_date: self.rng.random_date(1980, 2020),
            s_pe: self.rng.random_decimal(5.0, 50.0, 2),
            s_52wk_high: high,
            s_52wk_high_date: self.rng.random_date(2023, 2024),
            s_52wk_low: low,
            s_52wk_low_date: self.rng.random_date(2023, 2024),
            s_dividend: self.rng.random_decimal(0.0, 5.0, 2),
            s_yield: self.rng.random_decimal(0.0, 0.1, 4),
        }
    }

    /// Generate last trade record
    pub fn gen_last_trade(&mut self, s_symb: &str) -> LastTrade {
        let price = self.rng.random_decimal(5.0, 500.0, 2);
        LastTrade {
            lt_s_symb: s_symb.to_string(),
            lt_dts: TPCERng::current_timestamp(),
            lt_price: price,
            lt_open_price: price * self.rng.random_float(0.95, 1.05),
            lt_vol: self.rng.random_int(1000, 10_000_000),
        }
    }

    /// Generate financial record
    pub fn gen_financial(&mut self, co_id: i64, year: i32, qtr: i32) -> Financial {
        let revenue = self.rng.random_decimal(1_000_000.0, 1_000_000_000.0, 2);
        let margin = self.rng.random_decimal(0.05, 0.30, 4);
        let net_earn = revenue * margin;
        let shares = self.rng.random_int(10_000_000, 1_000_000_000);

        Financial {
            fi_co_id: co_id,
            fi_year: year,
            fi_qtr: qtr,
            fi_qtr_start_date: format!("{}-{:02}-01", year, (qtr - 1) * 3 + 1),
            fi_revenue: revenue,
            fi_net_earn: net_earn,
            fi_basic_eps: net_earn / shares as f64,
            fi_dilut_eps: net_earn / (shares as f64 * 1.1),
            fi_margin: margin,
            fi_inventory: self.rng.random_decimal(100_000.0, 10_000_000.0, 2),
            fi_assets: self.rng.random_decimal(10_000_000.0, 100_000_000_000.0, 2),
            fi_liability: self.rng.random_decimal(1_000_000.0, 50_000_000_000.0, 2),
            fi_out_basic: shares,
            fi_out_dilut: (shares as f64 * 1.1) as i64,
        }
    }
}

// =============================================================================
// Trade Table Generators
// =============================================================================

impl TPCEData {
    /// Generate trade record
    pub fn gen_trade(&mut self, ca_id: i64, s_symb: &str) -> Trade {
        let trade_types = ["TMB", "TMS", "TSL", "TLB", "TLS"];
        let tt_id = self.rng.pick(&trade_types).to_string();
        let _is_sell = tt_id == "TMS" || tt_id == "TSL" || tt_id == "TLS";
        let is_mrkt = tt_id == "TMB" || tt_id == "TMS";

        let qty = self.rng.random_int(100, 10000) as i32;
        let price = self.rng.random_decimal(5.0, 500.0, 2);
        let trade_price = if is_mrkt { price } else { 0.0 };

        Trade {
            t_id: self.next_trade_id(),
            t_dts: self.rng.random_timestamp(),
            t_st_id: if is_mrkt { "CMPT" } else { "PNDG" }.to_string(),
            t_tt_id: tt_id,
            t_is_cash: self.rng.random_bool(0.8),
            t_s_symb: s_symb.to_string(),
            t_qty: qty,
            t_bid_price: price,
            t_ca_id: ca_id,
            t_exec_name: format!("Exec-{}", self.rng.random_int(1, 1000)),
            t_trade_price: trade_price,
            t_chrg: self.rng.random_decimal(5.0, 20.0, 2),
            t_comm: price * qty as f64 * self.rng.random_decimal(0.001, 0.01, 4),
            t_tax: price * qty as f64 * self.rng.random_decimal(0.0, 0.05, 4),
            t_lifo: self.rng.random_bool(0.5),
        }
    }

    /// Generate holding record
    pub fn gen_holding(&mut self, ca_id: i64, s_symb: &str) -> Holding {
        Holding {
            h_t_id: self.next_trade_id(),
            h_ca_id: ca_id,
            h_s_symb: s_symb.to_string(),
            h_dts: self.rng.random_timestamp(),
            h_price: self.rng.random_decimal(5.0, 500.0, 2),
            h_qty: self.rng.random_int(100, 10000) as i32,
        }
    }

    /// Generate holding summary record
    pub fn gen_holding_summary(&mut self, ca_id: i64, s_symb: &str, qty: i32) -> HoldingSummary {
        HoldingSummary { hs_ca_id: ca_id, hs_s_symb: s_symb.to_string(), hs_qty: qty }
    }

    /// Generate settlement record for a trade
    pub fn gen_settlement(&mut self, t_id: i64, amount: f64) -> Settlement {
        Settlement {
            se_t_id: t_id,
            se_cash_type: if self.rng.random_bool(0.5) { "Cash" } else { "Margin" }.to_string(),
            se_cash_due_date: self.rng.random_date(2024, 2024),
            se_amt: amount,
        }
    }

    /// Generate cash transaction record
    pub fn gen_cash_transaction(&mut self, t_id: i64, amount: f64) -> CashTransaction {
        CashTransaction {
            ct_t_id: t_id,
            ct_dts: TPCERng::current_timestamp(),
            ct_amt: amount,
            ct_name: format!("Trade {} settlement", t_id),
        }
    }

    /// Generate trade history record
    pub fn gen_trade_history(&mut self, t_id: i64, st_id: &str) -> TradeHistory {
        TradeHistory {
            th_t_id: t_id,
            th_dts: self.rng.random_timestamp(),
            th_st_id: st_id.to_string(),
        }
    }
}

// =============================================================================
// Security Symbol Generator
// =============================================================================

impl TPCEData {
    /// Generate a unique security symbol
    pub fn gen_symbol(&mut self, index: i32) -> String {
        // Generate symbols like AAPL, MSFT, GOOGL, etc.
        // For larger indices, use longer symbols
        let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars().collect();
        if index < 26 {
            chars[index as usize].to_string()
        } else if index < 26 * 26 {
            let a = (index / 26) as usize;
            let b = (index % 26) as usize;
            format!("{}{}", chars[a], chars[b])
        } else if index < 26 * 26 * 26 {
            let a = (index / (26 * 26)) as usize;
            let b = ((index / 26) % 26) as usize;
            let c = (index % 26) as usize;
            format!("{}{}{}", chars[a], chars[b], chars[c])
        } else {
            let a = (index / (26 * 26 * 26)) as usize % 26;
            let b = (index / (26 * 26)) as usize % 26;
            let c = (index / 26) as usize % 26;
            let d = (index % 26) as usize;
            format!("{}{}{}{}", chars[a], chars[b], chars[c], chars[d])
        }
    }
}
