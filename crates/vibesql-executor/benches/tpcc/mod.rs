//! TPC-C Benchmark Module
//!
//! This module provides TPC-C (Transaction Processing Performance Council - C)
//! benchmark utilities for testing OLTP workload performance including:
//! - Data generation (from `vibesql-bench-common`)
//! - Transaction definitions (from `vibesql-bench-common`)
//! - Schema creation and data loading (`schema` module - engine-specific)
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

// Engine-specific schema loading code (stays here due to vibesql dependencies)
pub mod schema;

// Engine-specific transaction execution code (uses vibesql internals)
pub mod transactions;

// Re-export data generators from shared crate
pub use vibesql_bench_common::tpcc::{
    // Input generators
    generate_delivery_input,
    generate_new_order_input,
    generate_order_status_input,
    generate_payment_input,
    generate_stock_level_input,
    // Entity types
    Customer,
    // Transaction input types
    DeliveryInput,
    District,
    History,
    Item,
    NewOrder,
    NewOrderInput,
    NewOrderItemInput,
    Order,
    OrderLine,
    OrderStatusInput,
    PaymentInput,
    Stock,
    StockLevelInput,
    TPCCBenchmarkResults,
    // Data generation
    TPCCData,
    TPCCRng,
    // Workload generator
    TPCCWorkload,
    TransactionResult,
    Warehouse,
};

// Re-export schema loaders
#[cfg(feature = "duckdb")]
pub use schema::load_duckdb;
#[cfg(feature = "sqlite")]
pub use schema::load_sqlite;
pub use schema::load_vibesql;
#[cfg(feature = "mysql")]
pub use schema::{get_mysql_pool, load_mysql};

// Re-export transaction executors (engine-specific implementations)
#[cfg(feature = "duckdb")]
pub use transactions::DuckdbTransactionExecutor;
#[cfg(feature = "mysql")]
pub use transactions::MysqlTransactionExecutor;
#[cfg(feature = "sqlite")]
pub use transactions::SqliteTransactionExecutor;
pub use transactions::{
    print_profile_summary, reset_profile_counters, TPCCExecutor, VibesqlTransactionExecutor,
};
