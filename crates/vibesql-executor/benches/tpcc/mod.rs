//! TPC-C Benchmark Module
//!
//! This module provides TPC-C (Transaction Processing Performance Council - C)
//! benchmark utilities for testing OLTP workload performance including:
//! - Data generation (`data` module)
//! - Transaction definitions (`transactions` module)
//! - Schema creation and data loading (`schema` module)
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

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod data;
pub mod schema;
pub mod transactions;

// Re-export commonly used items for convenience
pub use data::TPCCData;
pub use schema::load_vibesql;
pub use transactions::*;

#[cfg(feature = "sqlite-comparison")]
pub use schema::load_sqlite;
#[cfg(feature = "duckdb-comparison")]
pub use schema::load_duckdb;
#[cfg(feature = "mysql-comparison")]
pub use schema::load_mysql;
