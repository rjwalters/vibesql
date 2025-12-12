//! TPC-C Transaction Input Types and Generators
//!
//! This module provides transaction input types and random input generators
//! for TPC-C benchmarks. The actual transaction execution code remains in
//! each engine-specific benchmark crate.

use super::data::TPCCRng;

/// Transaction input for New-Order
#[derive(Debug, Clone)]
pub struct NewOrderInput {
    pub w_id: i32,
    pub d_id: i32,
    pub c_id: i32,
    pub ol_cnt: i32,
    pub items: Vec<NewOrderItemInput>,
}

#[derive(Debug, Clone)]
pub struct NewOrderItemInput {
    pub ol_i_id: i32,
    pub ol_supply_w_id: i32,
    pub ol_quantity: i32,
}

/// Transaction input for Payment
#[derive(Debug, Clone)]
pub struct PaymentInput {
    pub w_id: i32,
    pub d_id: i32,
    pub c_w_id: i32,
    pub c_d_id: i32,
    pub c_id: Option<i32>,
    pub c_last: Option<String>,
    pub h_amount: f64,
}

/// Transaction input for Order-Status
#[derive(Debug, Clone)]
pub struct OrderStatusInput {
    pub w_id: i32,
    pub d_id: i32,
    pub c_id: Option<i32>,
    pub c_last: Option<String>,
}

/// Transaction input for Delivery
#[derive(Debug, Clone)]
pub struct DeliveryInput {
    pub w_id: i32,
    pub o_carrier_id: i32,
}

/// Transaction input for Stock-Level
#[derive(Debug, Clone)]
pub struct StockLevelInput {
    pub w_id: i32,
    pub d_id: i32,
    pub threshold: i32,
}

/// Transaction result with timing information
#[derive(Debug, Clone)]
pub struct TransactionResult {
    pub success: bool,
    pub duration_us: u64,
    pub error: Option<String>,
}

/// Generate random New-Order transaction input
pub fn generate_new_order_input(rng: &mut TPCCRng, num_warehouses: i32) -> NewOrderInput {
    let w_id = rng.random_int(1, num_warehouses as i64) as i32;
    let d_id = rng.random_int(1, 10) as i32;
    let c_id = rng.nurand(1023, 1, 3000) as i32;
    let ol_cnt = rng.random_int(5, 15) as i32;

    let mut items = Vec::with_capacity(ol_cnt as usize);
    for _ in 0..ol_cnt {
        // 1% of items are from remote warehouse
        let ol_supply_w_id = if num_warehouses > 1 && rng.random_int(1, 100) == 1 {
            let mut remote = rng.random_int(1, num_warehouses as i64) as i32;
            while remote == w_id && num_warehouses > 1 {
                remote = rng.random_int(1, num_warehouses as i64) as i32;
            }
            remote
        } else {
            w_id
        };

        items.push(NewOrderItemInput {
            ol_i_id: rng.nurand(8191, 1, 100000) as i32,
            ol_supply_w_id,
            ol_quantity: rng.random_int(1, 10) as i32,
        });
    }

    NewOrderInput { w_id, d_id, c_id, ol_cnt, items }
}

/// Generate random Payment transaction input
pub fn generate_payment_input(rng: &mut TPCCRng, num_warehouses: i32) -> PaymentInput {
    let w_id = rng.random_int(1, num_warehouses as i64) as i32;
    let d_id = rng.random_int(1, 10) as i32;

    // 85% local, 15% remote
    let (c_w_id, c_d_id) = if num_warehouses > 1 && rng.random_int(1, 100) <= 15 {
        let mut remote_w = rng.random_int(1, num_warehouses as i64) as i32;
        while remote_w == w_id && num_warehouses > 1 {
            remote_w = rng.random_int(1, num_warehouses as i64) as i32;
        }
        (remote_w, rng.random_int(1, 10) as i32)
    } else {
        (w_id, d_id)
    };

    // 60% by customer ID, 40% by last name
    let (c_id, c_last) = if rng.random_int(1, 100) <= 60 {
        (Some(rng.nurand(1023, 1, 3000) as i32), None)
    } else {
        (None, Some(TPCCRng::last_name(rng.nurand(255, 0, 999))))
    };

    PaymentInput {
        w_id,
        d_id,
        c_w_id,
        c_d_id,
        c_id,
        c_last,
        h_amount: rng.random_int(100, 500000) as f64 / 100.0,
    }
}

/// Generate random Order-Status transaction input
pub fn generate_order_status_input(rng: &mut TPCCRng, num_warehouses: i32) -> OrderStatusInput {
    let w_id = rng.random_int(1, num_warehouses as i64) as i32;
    let d_id = rng.random_int(1, 10) as i32;

    // 60% by customer ID, 40% by last name
    let (c_id, c_last) = if rng.random_int(1, 100) <= 60 {
        (Some(rng.nurand(1023, 1, 3000) as i32), None)
    } else {
        (None, Some(TPCCRng::last_name(rng.nurand(255, 0, 999))))
    };

    OrderStatusInput { w_id, d_id, c_id, c_last }
}

/// Generate random Delivery transaction input
pub fn generate_delivery_input(rng: &mut TPCCRng, num_warehouses: i32) -> DeliveryInput {
    DeliveryInput {
        w_id: rng.random_int(1, num_warehouses as i64) as i32,
        o_carrier_id: rng.random_int(1, 10) as i32,
    }
}

/// Generate random Stock-Level transaction input
pub fn generate_stock_level_input(rng: &mut TPCCRng, num_warehouses: i32) -> StockLevelInput {
    StockLevelInput {
        w_id: rng.random_int(1, num_warehouses as i64) as i32,
        d_id: rng.random_int(1, 10) as i32,
        threshold: rng.random_int(10, 20) as i32,
    }
}

/// TPC-C workload generator following standard transaction mix
pub struct TPCCWorkload {
    pub rng: TPCCRng,
    pub num_warehouses: i32,
}

impl TPCCWorkload {
    pub fn new(seed: u64, num_warehouses: i32) -> Self {
        Self { rng: TPCCRng::new(seed), num_warehouses }
    }

    /// Generate next transaction according to TPC-C mix
    /// Returns: transaction_type (0=NewOrder, 1=Payment, 2=OrderStatus, 3=Delivery, 4=StockLevel)
    pub fn next_transaction_type(&mut self) -> i32 {
        let roll = self.rng.random_int(1, 100);
        if roll <= 45 {
            0 // New-Order (45%)
        } else if roll <= 88 {
            1 // Payment (43%)
        } else if roll <= 92 {
            2 // Order-Status (4%)
        } else if roll <= 96 {
            3 // Delivery (4%)
        } else {
            4 // Stock-Level (4%)
        }
    }

    pub fn generate_new_order(&mut self) -> NewOrderInput {
        generate_new_order_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_payment(&mut self) -> PaymentInput {
        generate_payment_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_order_status(&mut self) -> OrderStatusInput {
        generate_order_status_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_delivery(&mut self) -> DeliveryInput {
        generate_delivery_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_stock_level(&mut self) -> StockLevelInput {
        generate_stock_level_input(&mut self.rng, self.num_warehouses)
    }
}

/// Benchmark results summary
#[derive(Debug, Clone, Default)]
pub struct TPCCBenchmarkResults {
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub total_duration_ms: u64,
    pub transactions_per_second: f64,
    pub new_order_count: u64,
    pub new_order_avg_us: f64,
    pub payment_count: u64,
    pub payment_avg_us: f64,
    pub order_status_count: u64,
    pub order_status_avg_us: f64,
    pub delivery_count: u64,
    pub delivery_avg_us: f64,
    pub stock_level_count: u64,
    pub stock_level_avg_us: f64,
}

impl TPCCBenchmarkResults {
    pub fn new() -> Self {
        Self::default()
    }
}
