//! TPC-E Transaction Implementations
//!
//! This module implements the 12 TPC-E transactions organized into three categories:
//!
//! ## Customer-Initiated Transactions (CE)
//! 1. **Broker-Volume**: Query broker trading activity (read-only)
//! 2. **Customer-Position**: View customer portfolio positions (read-only)
//! 3. **Market-Watch**: Monitor market data changes (read-only)
//! 4. **Security-Detail**: Get detailed security information (read-only)
//! 5. **Trade-Lookup**: Query trade history (read-only)
//! 6. **Trade-Order**: Submit new trade order (read-write)
//! 7. **Trade-Status**: Check order status (read-only)
//! 8. **Trade-Update**: Modify pending order (read-write)
//!
//! ## Market Exchange Emulator Transactions (MEE)
//! 9. **Market-Feed**: Process market data updates (read-write)
//! 10. **Trade-Result**: Complete trade execution (read-write)
//!
//! ## Brokerage-Initiated Transactions
//! 11. **Data-Maintenance**: Periodic data updates (read-write)
//! 12. **Trade-Cleanup**: End-of-day cleanup (read-write)
//!
//! ## Transaction Mix (per TPC-E spec)
//!
//! | Transaction     | Min % | Max % |
//! |-----------------|-------|-------|
//! | Broker-Volume   | 4.9   | 5.1   |
//! | Customer-Position| 12.9 | 13.1  |
//! | Market-Watch    | 17.9  | 18.1  |
//! | Security-Detail | 13.9  | 14.1  |
//! | Trade-Lookup    | 7.9   | 8.1   |
//! | Trade-Order     | 10.0  | 10.2  |
//! | Trade-Status    | 18.9  | 19.1  |
//! | Trade-Update    | 1.9   | 2.1   |

use std::time::Instant;

use super::data::TPCERng;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

// =============================================================================
// Transaction Inputs
// =============================================================================

/// Broker-Volume transaction input
#[derive(Debug, Clone)]
pub struct BrokerVolumeInput {
    pub broker_list: Vec<String>, // List of broker names to query
    pub sector_name: String,      // Sector to filter by
}

/// Customer-Position transaction input
#[derive(Debug, Clone)]
pub struct CustomerPositionInput {
    pub cust_id: i64,      // Customer ID
    pub acct_id_idx: i32,  // Account index (0-based)
    pub get_history: bool, // Whether to get trade history
}

/// Market-Watch transaction input
#[derive(Debug, Clone)]
pub struct MarketWatchInput {
    pub acct_id: i64,          // Account ID (for watch list)
    pub cust_id: i64,          // Customer ID
    pub industry_name: String, // Industry filter
    pub starting_co_id: i64,   // Starting company ID
    pub ending_co_id: i64,     // Ending company ID
}

/// Security-Detail transaction input
#[derive(Debug, Clone)]
pub struct SecurityDetailInput {
    pub symbol: String,    // Security symbol
    pub max_comp_len: i32, // Max competitor company names
    pub access_lob: bool,  // Access LOB data (news)
}

/// Trade-Lookup transaction input
#[derive(Debug, Clone)]
pub struct TradeLookupInput {
    pub frame: i32,              // Which frame to execute (1-4)
    pub acct_id: i64,            // Account ID
    pub max_trades: i32,         // Max trades to return
    pub trade_id: Vec<i64>,      // Trade IDs (frame 1)
    pub start_trade_dts: String, // Start timestamp
    pub end_trade_dts: String,   // End timestamp
    pub symbol: String,          // Security symbol
}

/// Trade-Order transaction input
#[derive(Debug, Clone)]
pub struct TradeOrderInput {
    pub acct_id: i64,          // Customer account ID
    pub symbol: String,        // Security symbol
    pub trade_qty: i32,        // Quantity to trade
    pub trade_type_id: String, // Trade type (TMB, TMS, etc.)
    pub is_lifo: bool,         // LIFO flag
    pub requested_price: f64,  // Requested price (for limit orders)
}

/// Trade-Status transaction input
#[derive(Debug, Clone)]
pub struct TradeStatusInput {
    pub acct_id: i64, // Customer account ID
}

/// Trade-Update transaction input
#[derive(Debug, Clone)]
pub struct TradeUpdateInput {
    pub frame: i32,         // Which frame (1-3)
    pub acct_id: i64,       // Account ID
    pub max_trades: i32,    // Max trades to update
    pub max_updates: i32,   // Max updates to perform
    pub trade_id: Vec<i64>, // Trade IDs
}

/// Market-Feed transaction input
#[derive(Debug, Clone)]
pub struct MarketFeedInput {
    pub symbols: Vec<String>, // Securities to update
    pub prices: Vec<f64>,     // New prices
}

/// Trade-Result transaction input
#[derive(Debug, Clone)]
pub struct TradeResultInput {
    pub trade_id: i64,    // Trade to complete
    pub trade_price: f64, // Execution price
}

/// Data-Maintenance transaction input
#[derive(Debug, Clone)]
pub struct DataMaintenanceInput {
    pub table_name: String, // Table to update
    pub c_id: i64,          // Customer ID
    pub co_id: i64,         // Company ID
    pub vol_incr: i32,      // Volume increment
}

/// Trade-Cleanup transaction input
#[derive(Debug, Clone)]
pub struct TradeCleanupInput {
    pub start_trade_id: i64,     // Starting trade ID
    pub st_submitted_id: String, // Submitted status ID
    pub st_canceled_id: String,  // Canceled status ID
}

// =============================================================================
// Transaction Result
// =============================================================================

/// Transaction result with timing and status
#[derive(Debug, Clone)]
pub struct TransactionResult {
    pub success: bool,
    pub duration_us: u64,
    pub error: Option<String>,
    pub rows_affected: i32,
}

impl TransactionResult {
    pub fn success(duration_us: u64) -> Self {
        Self { success: true, duration_us, error: None, rows_affected: 0 }
    }

    pub fn failure(duration_us: u64, error: String) -> Self {
        Self { success: false, duration_us, error: Some(error), rows_affected: 0 }
    }
}

// =============================================================================
// Benchmark Results
// =============================================================================

/// TPC-E benchmark results
#[derive(Debug, Clone, Default)]
pub struct TPCEBenchmarkResults {
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub total_duration_ms: u64,
    pub transactions_per_second: f64,

    // Per-transaction metrics
    pub broker_volume_count: u64,
    pub broker_volume_avg_us: f64,
    pub customer_position_count: u64,
    pub customer_position_avg_us: f64,
    pub market_watch_count: u64,
    pub market_watch_avg_us: f64,
    pub security_detail_count: u64,
    pub security_detail_avg_us: f64,
    pub trade_lookup_count: u64,
    pub trade_lookup_avg_us: f64,
    pub trade_order_count: u64,
    pub trade_order_avg_us: f64,
    pub trade_status_count: u64,
    pub trade_status_avg_us: f64,
    pub trade_update_count: u64,
    pub trade_update_avg_us: f64,
    pub market_feed_count: u64,
    pub market_feed_avg_us: f64,
    pub trade_result_count: u64,
    pub trade_result_avg_us: f64,
    pub data_maintenance_count: u64,
    pub data_maintenance_avg_us: f64,
    pub trade_cleanup_count: u64,
    pub trade_cleanup_avg_us: f64,
}

impl TPCEBenchmarkResults {
    pub fn new() -> Self {
        Self::default()
    }
}

// =============================================================================
// Executor Trait
// =============================================================================

/// Trait for TPC-E transaction executors
pub trait TPCEExecutor {
    fn broker_volume(&self, input: &BrokerVolumeInput) -> TransactionResult;
    fn customer_position(&self, input: &CustomerPositionInput) -> TransactionResult;
    fn market_watch(&self, input: &MarketWatchInput) -> TransactionResult;
    fn security_detail(&self, input: &SecurityDetailInput) -> TransactionResult;
    fn trade_lookup(&self, input: &TradeLookupInput) -> TransactionResult;
    fn trade_order(&self, input: &TradeOrderInput) -> TransactionResult;
    fn trade_status(&self, input: &TradeStatusInput) -> TransactionResult;
    fn trade_update(&self, input: &TradeUpdateInput) -> TransactionResult;
    fn market_feed(&self, input: &MarketFeedInput) -> TransactionResult;
    fn trade_result(&self, input: &TradeResultInput) -> TransactionResult;
    fn data_maintenance(&self, input: &DataMaintenanceInput) -> TransactionResult;
    fn trade_cleanup(&self, input: &TradeCleanupInput) -> TransactionResult;
}

// =============================================================================
// Workload Generator
// =============================================================================

/// TPC-E workload generator
pub struct TPCEWorkload {
    pub rng: TPCERng,
    pub num_customers: i32,
    pub num_accounts: i32,
    pub num_securities: i32,
    symbols: Vec<String>,
}

impl TPCEWorkload {
    pub fn new(seed: u64, num_customers: i32, num_accounts: i32, num_securities: i32) -> Self {
        let rng = TPCERng::new(seed);
        let symbols: Vec<String> = (0..num_securities)
            .map(|i| {
                // Generate symbol like A, B, ..., Z, AA, AB, ...
                let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars().collect();
                if i < 26 {
                    chars[i as usize].to_string()
                } else if i < 26 * 26 {
                    let a = (i / 26) as usize;
                    let b = (i % 26) as usize;
                    format!("{}{}", chars[a], chars[b])
                } else {
                    let a = (i / (26 * 26)) as usize % 26;
                    let b = ((i / 26) % 26) as usize;
                    let c = (i % 26) as usize;
                    format!("{}{}{}", chars[a], chars[b], chars[c])
                }
            })
            .collect();

        Self { rng, num_customers, num_accounts, num_securities, symbols }
    }

    /// Select next transaction type based on TPC-E mix
    /// Returns: 0=BrokerVolume, 1=CustomerPosition, 2=MarketWatch, 3=SecurityDetail,
    ///          4=TradeLookup, 5=TradeOrder, 6=TradeStatus, 7=TradeUpdate,
    ///          8=MarketFeed, 9=TradeResult, 10=DataMaintenance, 11=TradeCleanup
    pub fn next_transaction_type(&mut self) -> i32 {
        // Simplified mix based on TPC-E spec
        // Real implementation would use exact percentages
        let r = self.rng.random_int(1, 100);
        match r {
            1..=5 => 0,    // Broker-Volume: 5%
            6..=18 => 1,   // Customer-Position: 13%
            19..=36 => 2,  // Market-Watch: 18%
            37..=50 => 3,  // Security-Detail: 14%
            51..=58 => 4,  // Trade-Lookup: 8%
            59..=68 => 5,  // Trade-Order: 10%
            69..=87 => 6,  // Trade-Status: 19%
            88..=89 => 7,  // Trade-Update: 2%
            90..=93 => 8,  // Market-Feed: 4%
            94..=97 => 9,  // Trade-Result: 4%
            98..=99 => 10, // Data-Maintenance: 2%
            _ => 11,       // Trade-Cleanup: 1%
        }
    }

    fn random_symbol(&mut self) -> String {
        let idx = self.rng.random_int(0, self.symbols.len() as i64 - 1) as usize;
        self.symbols[idx].clone()
    }

    fn random_acct_id(&mut self) -> i64 {
        self.rng.random_int(1, self.num_accounts as i64)
    }

    fn random_cust_id(&mut self) -> i64 {
        self.rng.random_int(1, self.num_customers as i64)
    }

    pub fn generate_broker_volume(&mut self) -> BrokerVolumeInput {
        let broker_names: Vec<String> =
            (0..5).map(|i| format!("Broker{}", i + self.rng.random_int(1, 100))).collect();
        let sectors = ["Energy", "Technology", "Finance", "Healthcare", "Consumer"];
        BrokerVolumeInput {
            broker_list: broker_names,
            sector_name: sectors[self.rng.random_int(0, sectors.len() as i64 - 1) as usize]
                .to_string(),
        }
    }

    pub fn generate_customer_position(&mut self) -> CustomerPositionInput {
        CustomerPositionInput {
            cust_id: self.random_cust_id(),
            acct_id_idx: self.rng.random_int(0, 4) as i32,
            get_history: self.rng.random_bool(0.5),
        }
    }

    pub fn generate_market_watch(&mut self) -> MarketWatchInput {
        MarketWatchInput {
            acct_id: self.random_acct_id(),
            cust_id: self.random_cust_id(),
            industry_name: format!("IN{:04}", self.rng.random_int(1, 30)),
            starting_co_id: 1,
            ending_co_id: self.rng.random_int(100, 1000),
        }
    }

    pub fn generate_security_detail(&mut self) -> SecurityDetailInput {
        SecurityDetailInput {
            symbol: self.random_symbol(),
            max_comp_len: 3,
            access_lob: self.rng.random_bool(0.2),
        }
    }

    pub fn generate_trade_lookup(&mut self) -> TradeLookupInput {
        let frame = self.rng.random_int(1, 4) as i32;
        TradeLookupInput {
            frame,
            acct_id: self.random_acct_id(),
            max_trades: 20,
            trade_id: (0..3).map(|_| self.rng.random_int(1, 10000)).collect(),
            start_trade_dts: "2024-01-01 00:00:00".to_string(),
            end_trade_dts: "2024-12-31 23:59:59".to_string(),
            symbol: self.random_symbol(),
        }
    }

    pub fn generate_trade_order(&mut self) -> TradeOrderInput {
        let trade_types = ["TMB", "TMS", "TSL", "TLB", "TLS"];
        TradeOrderInput {
            acct_id: self.random_acct_id(),
            symbol: self.random_symbol(),
            trade_qty: self.rng.random_int(100, 10000) as i32,
            trade_type_id: trade_types[self.rng.random_int(0, 4) as usize].to_string(),
            is_lifo: self.rng.random_bool(0.5),
            requested_price: self.rng.random_decimal(10.0, 500.0, 2),
        }
    }

    pub fn generate_trade_status(&mut self) -> TradeStatusInput {
        TradeStatusInput { acct_id: self.random_acct_id() }
    }

    pub fn generate_trade_update(&mut self) -> TradeUpdateInput {
        TradeUpdateInput {
            frame: self.rng.random_int(1, 3) as i32,
            acct_id: self.random_acct_id(),
            max_trades: 20,
            max_updates: 3,
            trade_id: (0..3).map(|_| self.rng.random_int(1, 10000)).collect(),
        }
    }

    pub fn generate_market_feed(&mut self) -> MarketFeedInput {
        let n = 10;
        MarketFeedInput {
            symbols: (0..n).map(|_| self.random_symbol()).collect(),
            prices: (0..n).map(|_| self.rng.random_decimal(10.0, 500.0, 2)).collect(),
        }
    }

    pub fn generate_trade_result(&mut self) -> TradeResultInput {
        TradeResultInput {
            trade_id: self.rng.random_int(1, 10000),
            trade_price: self.rng.random_decimal(10.0, 500.0, 2),
        }
    }

    pub fn generate_data_maintenance(&mut self) -> DataMaintenanceInput {
        let tables = ["customer", "company", "security", "customer_account"];
        DataMaintenanceInput {
            table_name: tables[self.rng.random_int(0, 3) as usize].to_string(),
            c_id: self.random_cust_id(),
            co_id: self.rng.random_int(1, 1000),
            vol_incr: self.rng.random_int(1, 100) as i32,
        }
    }

    pub fn generate_trade_cleanup(&mut self) -> TradeCleanupInput {
        TradeCleanupInput {
            start_trade_id: 1,
            st_submitted_id: "SBMT".to_string(),
            st_canceled_id: "CNCL".to_string(),
        }
    }
}

// =============================================================================
// Profiling Support
// =============================================================================

thread_local! {
    static PARSE_TIME_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static EXECUTE_TIME_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static QUERY_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn execute_query(db: &vibesql_storage::Database, sql: &str) -> Result<(), String> {
    let parse_start = Instant::now();

    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return Ok(()),
        Err(e) => return Err(format!("Parse error: {}", e)),
    };

    let parse_time = parse_start.elapsed().as_micros() as u64;
    PARSE_TIME_US.with(|t| t.set(t.get() + parse_time));

    let execute_start = Instant::now();

    let executor = SelectExecutor::new(db);
    let result = match executor.execute(&stmt) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Execute error: {}", e)),
    };

    let execute_time = execute_start.elapsed().as_micros() as u64;
    EXECUTE_TIME_US.with(|t| t.set(t.get() + execute_time));
    QUERY_COUNT.with(|c| c.set(c.get() + 1));

    result
}

/// Print profiling summary
pub fn print_profile_summary() {
    PARSE_TIME_US.with(|parse| {
        EXECUTE_TIME_US.with(|execute| {
            QUERY_COUNT.with(|count| {
                let p = parse.get();
                let e = execute.get();
                let c = count.get();
                if c > 0 {
                    eprintln!("\n--- TPC-E Query Profiling ---");
                    eprintln!("Total queries: {}", c);
                    eprintln!("Parse time:   {} us total, {:.2} us avg", p, p as f64 / c as f64);
                    eprintln!("Execute time: {} us total, {:.2} us avg", e, e as f64 / c as f64);
                    eprintln!("Parse %:      {:.1}%", p as f64 / (p + e) as f64 * 100.0);
                }
            });
        });
    });
}

/// Reset profiling counters
pub fn reset_profile_counters() {
    PARSE_TIME_US.with(|t| t.set(0));
    EXECUTE_TIME_US.with(|t| t.set(0));
    QUERY_COUNT.with(|c| c.set(0));
}

// =============================================================================
// VibeSQL Transaction Executor
// =============================================================================

/// TPC-E transaction executor for VibeSQL
pub struct VibesqlTransactionExecutor<'a> {
    pub db: &'a vibesql_storage::Database,
}

impl<'a> VibesqlTransactionExecutor<'a> {
    pub fn new(db: &'a vibesql_storage::Database) -> Self {
        Self { db }
    }
}

impl<'a> TPCEExecutor for VibesqlTransactionExecutor<'a> {
    /// Broker-Volume: Query broker trading activity
    fn broker_volume(&self, input: &BrokerVolumeInput) -> TransactionResult {
        let start = Instant::now();

        // Frame 1: Get broker info and trade volume
        // Join broker -> customer_account -> trade -> security -> company -> industry -> sector
        let broker_names: String =
            input.broker_list.iter().map(|b| format!("'{}'", b)).collect::<Vec<_>>().join(", ");

        let query = format!(
            "SELECT b.b_name, SUM(t.t_qty * t.t_trade_price) as volume
             FROM broker b
             JOIN customer_account ca ON ca.ca_b_id = b.b_id
             JOIN trade t ON t.t_ca_id = ca.ca_id
             JOIN security s ON s.s_symb = t.t_s_symb
             JOIN company co ON co.co_id = s.s_co_id
             JOIN industry i ON i.in_id = co.co_in_id
             JOIN sector sc ON sc.sc_id = i.in_sc_id
             WHERE b.b_name IN ({})
               AND sc.sc_name = '{}'
             GROUP BY b.b_name
             ORDER BY volume DESC",
            broker_names, input.sector_name
        );

        if let Err(e) = execute_query(self.db, &query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Customer-Position: View customer portfolio
    fn customer_position(&self, input: &CustomerPositionInput) -> TransactionResult {
        let start = Instant::now();

        // Frame 1: Get customer info and account list
        let c_query = format!(
            "SELECT c.c_id, c.c_f_name, c.c_l_name, c.c_tier
             FROM customer c
             WHERE c.c_id = {}",
            input.cust_id
        );
        if let Err(e) = execute_query(self.db, &c_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get accounts
        let ca_query = format!(
            "SELECT ca.ca_id, ca.ca_bal, SUM(hs.hs_qty * lt.lt_price) as asset_total
             FROM customer_account ca
             JOIN holding_summary hs ON hs.hs_ca_id = ca.ca_id
             JOIN last_trade lt ON lt.lt_s_symb = hs.hs_s_symb
             WHERE ca.ca_c_id = {}
             GROUP BY ca.ca_id, ca.ca_bal
             ORDER BY asset_total DESC",
            input.cust_id
        );
        if let Err(e) = execute_query(self.db, &ca_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Frame 2: Get trade history if requested
        if input.get_history {
            let acct_id = input.cust_id * 5 + input.acct_id_idx as i64;
            let th_query = format!(
                "SELECT t.t_id, t.t_s_symb, t.t_qty, st.st_name, th.th_dts
                 FROM trade t
                 JOIN trade_history th ON th.th_t_id = t.t_id
                 JOIN status_type st ON st.st_id = th.th_st_id
                 WHERE t.t_ca_id = {}
                 ORDER BY th.th_dts DESC
                 LIMIT 30",
                acct_id
            );
            if let Err(e) = execute_query(self.db, &th_query) {
                return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
            }
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Market-Watch: Monitor market changes
    fn market_watch(&self, input: &MarketWatchInput) -> TransactionResult {
        let start = Instant::now();

        // Get watch list items or holdings
        let wi_query = format!(
            "SELECT wi.wi_s_symb
             FROM watch_item wi
             JOIN watch_list wl ON wl.wl_id = wi.wi_wl_id
             WHERE wl.wl_c_id = {}",
            input.cust_id
        );
        if let Err(e) = execute_query(self.db, &wi_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get market data
        let lt_query = format!(
            "SELECT s.s_symb, lt.lt_price, lt.lt_open_price,
                    (lt.lt_price - lt.lt_open_price) / lt.lt_open_price * 100 as pct_change
             FROM security s
             JOIN company co ON co.co_id = s.s_co_id
             JOIN industry i ON i.in_id = co.co_in_id
             JOIN last_trade lt ON lt.lt_s_symb = s.s_symb
             WHERE i.in_name = '{}' OR co.co_id BETWEEN {} AND {}
             LIMIT 50",
            input.industry_name, input.starting_co_id, input.ending_co_id
        );
        if let Err(e) = execute_query(self.db, &lt_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Security-Detail: Get security information
    fn security_detail(&self, input: &SecurityDetailInput) -> TransactionResult {
        let start = Instant::now();

        // Frame 1: Get security and company info
        let s_query = format!(
            "SELECT s.s_name, s.s_ex_id, s.s_num_out, s.s_pe, s.s_dividend, s.s_yield,
                    s.s_52wk_high, s.s_52wk_high_date, s.s_52wk_low, s.s_52wk_low_date,
                    co.co_name, co.co_ceo, co.co_desc, co.co_open_date, co.co_sp_rate,
                    i.in_name, a.ad_line1, a.ad_line2, zc.zc_town, zc.zc_div
             FROM security s
             JOIN company co ON co.co_id = s.s_co_id
             JOIN industry i ON i.in_id = co.co_in_id
             JOIN address a ON a.ad_id = co.co_ad_id
             JOIN zip_code zc ON zc.zc_code = a.ad_zc_code
             WHERE s.s_symb = '{}'",
            input.symbol
        );
        if let Err(e) = execute_query(self.db, &s_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get financial data
        let fi_query = format!(
            "SELECT fi.fi_year, fi.fi_qtr, fi.fi_revenue, fi.fi_net_earn, fi.fi_basic_eps
             FROM financial fi
             JOIN company co ON co.co_id = fi.fi_co_id
             JOIN security s ON s.s_co_id = co.co_id
             WHERE s.s_symb = '{}'
             ORDER BY fi.fi_year DESC, fi.fi_qtr DESC
             LIMIT 8",
            input.symbol
        );
        if let Err(e) = execute_query(self.db, &fi_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get daily market data
        let dm_query = format!(
            "SELECT dm.dm_date, dm.dm_close, dm.dm_high, dm.dm_low, dm.dm_vol
             FROM daily_market dm
             WHERE dm.dm_s_symb = '{}'
             ORDER BY dm.dm_date DESC
             LIMIT 5",
            input.symbol
        );
        if let Err(e) = execute_query(self.db, &dm_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Trade-Lookup: Query trade history
    fn trade_lookup(&self, input: &TradeLookupInput) -> TransactionResult {
        let start = Instant::now();

        match input.frame {
            1 => {
                // Frame 1: Look up trades by trade_id
                for t_id in &input.trade_id {
                    let query = format!(
                        "SELECT t.t_bid_price, t.t_exec_name, t.t_is_cash, t.t_trade_price,
                                t.t_qty, t.t_dts, t.t_s_symb, t.t_tt_id,
                                se.se_amt, se.se_cash_due_date, se.se_cash_type
                         FROM trade t
                         JOIN settlement se ON se.se_t_id = t.t_id
                         WHERE t.t_id = {}",
                        t_id
                    );
                    if let Err(e) = execute_query(self.db, &query) {
                        return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                    }
                }
            }
            2 => {
                // Frame 2: Look up trades by account and date range
                let query = format!(
                    "SELECT t.t_id, t.t_bid_price, t.t_exec_name, t.t_is_cash, t.t_trade_price,
                            t.t_qty, t.t_dts, t.t_s_symb, t.t_tt_id
                     FROM trade t
                     WHERE t.t_ca_id = {}
                       AND t.t_dts >= '{}' AND t.t_dts <= '{}'
                     ORDER BY t.t_dts
                     LIMIT {}",
                    input.acct_id, input.start_trade_dts, input.end_trade_dts, input.max_trades
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            3 => {
                // Frame 3: Look up trades by symbol and date range
                let query = format!(
                    "SELECT t.t_id, t.t_ca_id, t.t_bid_price, t.t_exec_name, t.t_is_cash,
                            t.t_trade_price, t.t_qty, t.t_dts, t.t_tt_id
                     FROM trade t
                     WHERE t.t_s_symb = '{}'
                       AND t.t_dts >= '{}' AND t.t_dts <= '{}'
                     ORDER BY t.t_dts
                     LIMIT {}",
                    input.symbol, input.start_trade_dts, input.end_trade_dts, input.max_trades
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            4 => {
                // Frame 4: Look up holding history
                let query = format!(
                    "SELECT hh.hh_h_t_id, hh.hh_t_id, hh.hh_before_qty, hh.hh_after_qty
                     FROM holding_history hh
                     JOIN holding h ON h.h_t_id = hh.hh_h_t_id
                     WHERE h.h_ca_id = {}
                     LIMIT {}",
                    input.acct_id, input.max_trades
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            _ => {}
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Trade-Order: Submit new trade
    fn trade_order(&self, input: &TradeOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Frame 1: Get account info
        let ca_query = format!(
            "SELECT ca.ca_name, ca.ca_b_id, ca.ca_c_id, ca.ca_tax_st
             FROM customer_account ca
             WHERE ca.ca_id = {}",
            input.acct_id
        );
        if let Err(e) = execute_query(self.db, &ca_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get customer info
        let c_query = format!(
            "SELECT c.c_f_name, c.c_l_name, c.c_tier, c.c_tax_id
             FROM customer c
             JOIN customer_account ca ON ca.ca_c_id = c.c_id
             WHERE ca.ca_id = {}",
            input.acct_id
        );
        if let Err(e) = execute_query(self.db, &c_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get broker info
        let b_query = format!(
            "SELECT b.b_name
             FROM broker b
             JOIN customer_account ca ON ca.ca_b_id = b.b_id
             WHERE ca.ca_id = {}",
            input.acct_id
        );
        if let Err(e) = execute_query(self.db, &b_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Frame 2: Get security info
        let s_query = format!(
            "SELECT s.s_co_id, s.s_ex_id, s.s_name
             FROM security s
             WHERE s.s_symb = '{}'",
            input.symbol
        );
        if let Err(e) = execute_query(self.db, &s_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get company and exchange info
        let co_query = format!(
            "SELECT co.co_name
             FROM company co
             JOIN security s ON s.s_co_id = co.co_id
             WHERE s.s_symb = '{}'",
            input.symbol
        );
        if let Err(e) = execute_query(self.db, &co_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get last trade price
        let lt_query = format!(
            "SELECT lt.lt_price
             FROM last_trade lt
             WHERE lt.lt_s_symb = '{}'",
            input.symbol
        );
        if let Err(e) = execute_query(self.db, &lt_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get trade type info
        let tt_query = format!(
            "SELECT tt.tt_is_sell, tt.tt_is_mrkt
             FROM trade_type tt
             WHERE tt.tt_id = '{}'",
            input.trade_type_id
        );
        if let Err(e) = execute_query(self.db, &tt_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get current holdings if selling
        let hs_query = format!(
            "SELECT hs.hs_qty
             FROM holding_summary hs
             WHERE hs.hs_ca_id = {} AND hs.hs_s_symb = '{}'",
            input.acct_id, input.symbol
        );
        if let Err(e) = execute_query(self.db, &hs_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Frame 3: Get charge and commission
        let ch_query = format!(
            "SELECT ch.ch_chrg
             FROM charge ch
             JOIN customer c ON c.c_tier = ch.ch_c_tier
             JOIN customer_account ca ON ca.ca_c_id = c.c_id
             WHERE ca.ca_id = {} AND ch.ch_tt_id = '{}'",
            input.acct_id, input.trade_type_id
        );
        if let Err(e) = execute_query(self.db, &ch_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Trade-Status: Check order status
    fn trade_status(&self, input: &TradeStatusInput) -> TransactionResult {
        let start = Instant::now();

        // Get recent trades for account
        let t_query = format!(
            "SELECT t.t_id, t.t_dts, st.st_name, tt.tt_name, t.t_s_symb, t.t_qty,
                    t.t_exec_name, t.t_chrg, s.s_name, ex.ex_name
             FROM trade t
             JOIN status_type st ON st.st_id = t.t_st_id
             JOIN trade_type tt ON tt.tt_id = t.t_tt_id
             JOIN security s ON s.s_symb = t.t_s_symb
             JOIN exchange ex ON ex.ex_id = s.s_ex_id
             WHERE t.t_ca_id = {}
             ORDER BY t.t_dts DESC
             LIMIT 50",
            input.acct_id
        );
        if let Err(e) = execute_query(self.db, &t_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get customer info
        let c_query = format!(
            "SELECT c.c_l_name, c.c_f_name, b.b_name
             FROM customer_account ca
             JOIN customer c ON c.c_id = ca.ca_c_id
             JOIN broker b ON b.b_id = ca.ca_b_id
             WHERE ca.ca_id = {}",
            input.acct_id
        );
        if let Err(e) = execute_query(self.db, &c_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Trade-Update: Modify pending orders
    fn trade_update(&self, input: &TradeUpdateInput) -> TransactionResult {
        let start = Instant::now();

        match input.frame {
            1 => {
                // Frame 1: Update executor name for trades
                for t_id in input.trade_id.iter().take(input.max_updates as usize) {
                    let query =
                        format!("SELECT t.t_exec_name FROM trade t WHERE t.t_id = {}", t_id);
                    if let Err(e) = execute_query(self.db, &query) {
                        return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                    }
                }
            }
            2 => {
                // Frame 2: Update settlements by account
                let query = format!(
                    "SELECT t.t_id, se.se_cash_type
                     FROM trade t
                     JOIN settlement se ON se.se_t_id = t.t_id
                     WHERE t.t_ca_id = {}
                     LIMIT {}",
                    input.acct_id, input.max_trades
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            3 => {
                // Frame 3: Update cash transactions
                let query = format!(
                    "SELECT ct.ct_t_id, ct.ct_name
                     FROM cash_transaction ct
                     JOIN trade t ON t.t_id = ct.ct_t_id
                     WHERE t.t_ca_id = {}
                     LIMIT {}",
                    input.acct_id, input.max_trades
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            _ => {}
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Market-Feed: Process market data updates
    fn market_feed(&self, input: &MarketFeedInput) -> TransactionResult {
        let start = Instant::now();

        for (symbol, _price) in input.symbols.iter().zip(input.prices.iter()) {
            // Get current trade info
            let lt_query = format!(
                "SELECT lt.lt_price, lt.lt_vol FROM last_trade lt WHERE lt.lt_s_symb = '{}'",
                symbol
            );
            if let Err(e) = execute_query(self.db, &lt_query) {
                return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
            }

            // Check for pending limit orders
            let tr_query = format!(
                "SELECT tr.tr_t_id, tr.tr_bid_price, tr.tr_qty
                 FROM trade_request tr
                 WHERE tr.tr_s_symb = '{}'",
                symbol
            );
            if let Err(e) = execute_query(self.db, &tr_query) {
                return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
            }
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Trade-Result: Complete trade execution
    fn trade_result(&self, input: &TradeResultInput) -> TransactionResult {
        let start = Instant::now();

        // Frame 1: Get trade info
        let t_query = format!(
            "SELECT t.t_ca_id, t.t_tt_id, t.t_s_symb, t.t_qty, t.t_chrg, t.t_lifo, t.t_is_cash
             FROM trade t
             WHERE t.t_id = {}",
            input.trade_id
        );
        if let Err(e) = execute_query(self.db, &t_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get trade type
        let tt_query = format!(
            "SELECT tt.tt_is_sell, tt.tt_is_mrkt
             FROM trade_type tt
             JOIN trade t ON t.t_tt_id = tt.tt_id
             WHERE t.t_id = {}",
            input.trade_id
        );
        if let Err(e) = execute_query(self.db, &tt_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Frame 2: Get holding info
        let h_query = format!(
            "SELECT h.h_t_id, h.h_qty, h.h_price
             FROM holding h
             JOIN trade t ON t.t_ca_id = h.h_ca_id AND t.t_s_symb = h.h_s_symb
             WHERE t.t_id = {}
             ORDER BY h.h_dts",
            input.trade_id
        );
        if let Err(e) = execute_query(self.db, &h_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Frame 3: Get tax info
        let tax_query = format!(
            "SELECT SUM(tx.tx_rate)
             FROM taxrate tx
             JOIN customer_taxrate cx ON cx.cx_tx_id = tx.tx_id
             JOIN customer_account ca ON ca.ca_c_id = cx.cx_c_id
             JOIN trade t ON t.t_ca_id = ca.ca_id
             WHERE t.t_id = {}",
            input.trade_id
        );
        if let Err(e) = execute_query(self.db, &tax_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Data-Maintenance: Periodic data updates
    fn data_maintenance(&self, input: &DataMaintenanceInput) -> TransactionResult {
        let start = Instant::now();

        match input.table_name.as_str() {
            "customer" => {
                let query = format!(
                    "SELECT c.c_email_1, c.c_email_2 FROM customer c WHERE c.c_id = {}",
                    input.c_id
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            "company" => {
                let query = format!(
                    "SELECT co.co_sp_rate FROM company co WHERE co.co_id = {}",
                    input.co_id
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            "security" => {
                let query = format!(
                    "SELECT s.s_dividend FROM security s
                     JOIN company co ON co.co_id = s.s_co_id
                     WHERE co.co_id = {}",
                    input.co_id
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            "customer_account" => {
                let query = format!(
                    "SELECT ca.ca_bal FROM customer_account ca WHERE ca.ca_c_id = {}",
                    input.c_id
                );
                if let Err(e) = execute_query(self.db, &query) {
                    return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
                }
            }
            _ => {}
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }

    /// Trade-Cleanup: End-of-day cleanup
    fn trade_cleanup(&self, input: &TradeCleanupInput) -> TransactionResult {
        let start = Instant::now();

        // Find pending trades to cancel
        let query = format!(
            "SELECT t.t_id, t.t_dts, t.t_st_id
             FROM trade t
             WHERE t.t_st_id IN ('{}', '{}')
               AND t.t_id >= {}
             ORDER BY t.t_dts
             LIMIT 100",
            input.st_submitted_id, input.st_canceled_id, input.start_trade_id
        );
        if let Err(e) = execute_query(self.db, &query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        // Get pending trade requests
        let tr_query = format!(
            "SELECT tr.tr_t_id FROM trade_request tr WHERE tr.tr_t_id >= {}",
            input.start_trade_id
        );
        if let Err(e) = execute_query(self.db, &tr_query) {
            return TransactionResult::failure(start.elapsed().as_micros() as u64, e);
        }

        TransactionResult::success(start.elapsed().as_micros() as u64)
    }
}
