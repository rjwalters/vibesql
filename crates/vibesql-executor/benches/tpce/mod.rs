//! TPC-E Benchmark Module
//!
//! This module provides TPC-E (Transaction Processing Performance Council - E)
//! benchmark utilities for testing OLTP workload performance. TPC-E simulates
//! a brokerage firm workload with customer accounts, trades, and market data.
//!
//! **This is the first open-source Rust implementation of TPC-E.**
//!
//! ## Schema Overview
//!
//! TPC-E uses 33 tables organized into categories:
//!
//! ### Fixed Tables (9 tables - generated once, read-only)
//! - CHARGE: Trade charges by tier and type
//! - COMMISSION_RATE: Broker commission rates
//! - EXCHANGE: Stock exchanges (NYSE, NASDAQ, etc.)
//! - INDUSTRY: Industry classifications
//! - SECTOR: Economic sectors
//! - STATUS_TYPE: Trade/order status codes
//! - TAXRATE: Tax rates by jurisdiction
//! - TRADE_TYPE: Types of trades (buy, sell, etc.)
//! - ZIP_CODE: ZIP codes with city/state
//!
//! ### Customer Tables (8 tables - scale with customer count)
//! - CUSTOMER: Customer demographics
//! - CUSTOMER_ACCOUNT: Brokerage accounts
//! - CUSTOMER_TAXRATE: Customer tax rates
//! - ACCOUNT_PERMISSION: Account access permissions
//! - WATCH_LIST: Customer watch lists
//! - WATCH_ITEM: Securities on watch lists
//! - ADDRESS: Customer/company addresses
//! - BROKER: Broker information
//!
//! ### Market Tables (8 tables - securities and market data)
//! - COMPANY: Publicly traded companies
//! - COMPANY_COMPETITOR: Company competitors
//! - SECURITY: Tradeable securities
//! - DAILY_MARKET: Daily market data
//! - LAST_TRADE: Most recent trade info
//! - FINANCIAL: Company financial data
//! - NEWS_ITEM: News articles
//! - NEWS_XREF: News-company associations
//!
//! ### Trade Tables (8 tables - grow during benchmark)
//! - TRADE: All trades
//! - TRADE_HISTORY: Trade status history
//! - TRADE_REQUEST: Pending limit orders
//! - SETTLEMENT: Trade settlements
//! - CASH_TRANSACTION: Cash movements
//! - HOLDING: Current holdings
//! - HOLDING_HISTORY: Holding changes
//! - HOLDING_SUMMARY: Aggregated holdings
//!
//! ## Transactions (12 total)
//!
//! ### Customer-Initiated (CE - Customer Emulator)
//! 1. Broker-Volume: Query broker activity (read-only)
//! 2. Customer-Position: View portfolio positions (read-only)
//! 3. Market-Watch: Monitor market data (read-only)
//! 4. Security-Detail: Get security information (read-only)
//! 5. Trade-Lookup: Query trade history (read-only)
//! 6. Trade-Order: Submit new trade (read-write)
//! 7. Trade-Status: Check order status (read-only)
//! 8. Trade-Update: Modify pending order (read-write)
//!
//! ### Market-Triggered (MEE - Market Exchange Emulator)
//! 9. Market-Feed: Process market data updates (read-write)
//! 10. Trade-Result: Complete trade execution (read-write)
//!
//! ### Brokerage-Initiated
//! 11. Data-Maintenance: Periodic data updates (read-write)
//! 12. Trade-Cleanup: End-of-day cleanup (read-write)
//!
//! ## Scaling
//!
//! TPC-E uses customer count as the scaling factor:
//! - Scale Factor 1 = 1,000 customers
//! - Each customer has ~10 accounts, each account has ~10 holdings
//! - Securities: ~6,850 (fixed based on S&P 500 simulation)
//!
//! ## References
//!
//! - [TPC-E Specification](https://www.tpc.org/tpce/)
//! - [DBT-5 Reference Implementation (C++)](https://github.com/osdldbt/dbt5)

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod data;
pub mod schema;
pub mod transactions;

// Re-export commonly used items for convenience
pub use data::TPCEData;
pub use schema::load_vibesql;
pub use transactions::*;

#[cfg(feature = "sqlite-comparison")]
pub use schema::load_sqlite;
#[cfg(feature = "duckdb-comparison")]
pub use schema::load_duckdb;
