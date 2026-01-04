//! TPC-C Benchmark Module
//!
//! This module provides TPC-C (Transaction Processing Performance Council - C)
//! benchmark utilities for testing OLTP workload performance including:
//! - Data generation (`data` module)
//! - Transaction input types and generators
//!
//! TPC-C simulates a complete computing environment where users execute
//! transactions against a database. It measures throughput in transactions
//! per minute (tpmC).
//!
//! ## Schema
//!
//! TPC-C uses 9 tables:
//! - WAREHOUSE: Scale factor base table
//! - DISTRICT: 10 districts per warehouse
//! - CUSTOMER: 3000 customers per district
//! - HISTORY: Payment history
//! - NEW_ORDER: Pending orders
//! - ORDERS: Completed orders
//! - ORDER_LINE: Order line items
//! - ITEM: 100,000 items (constant)
//! - STOCK: Inventory per warehouse
//!
//! ## Transactions
//!
//! 1. New-Order (45%): Create new order with multiple line items
//! 2. Payment (43%): Process customer payment
//! 3. Order-Status (4%): Query customer's last order
//! 4. Delivery (4%): Batch process pending orders
//! 5. Stock-Level (4%): Check low stock items
//!
//! ## Usage
//!
//! ```
//! use vibesql_bench_common::tpcc::{TPCCData, TPCCWorkload};
//!
//! // Generate TPC-C data for scale factor 0.01 (micro mode)
//! let mut data = TPCCData::new(0.01);
//! let warehouse = data.gen_warehouse(1);
//!
//! // Generate transaction inputs
//! let mut workload = TPCCWorkload::new(42, 1);
//! let new_order = workload.generate_new_order();
//! ```

mod data;
pub mod schema;
mod transactions;

pub use data::*;
pub use transactions::*;
