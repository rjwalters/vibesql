//! TPC-C Data Generation
//!
//! Generates realistic TPC-C data following the specification.
//! Scale factor = number of warehouses (SF=1 means 1 warehouse).
//!
//! Data sizes per warehouse (SF=1):
//! - WAREHOUSE: 1 row
//! - DISTRICT: 10 rows
//! - CUSTOMER: 30,000 rows (3,000 per district)
//! - HISTORY: 30,000 rows (1 per customer initially)
//! - ORDERS: 30,000 rows (3,000 per district)
//! - NEW_ORDER: 9,000 rows (900 per district, 30% of orders)
//! - ORDER_LINE: ~300,000 rows (avg 10 per order)
//! - ITEM: 100,000 rows (constant, shared across warehouses)
//! - STOCK: 100,000 rows (1 per item per warehouse)
//!
//! Micro scale factors (SF < 1) reduce all row counts proportionally:
//! - SF=0.01: ~2,500 total rows (100x smaller)
//! - SF=0.1: ~60,000 total rows (10x smaller)

/// Random number generator state for reproducible data
pub struct TPCCRng {
    state: u64,
}

impl TPCCRng {
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

    /// Random decimal with 2 decimal places
    pub fn random_decimal(&mut self, min: f64, max: f64) -> f64 {
        let range = ((max - min) * 100.0) as i64;
        let val = self.random_int(0, range);
        min + (val as f64) / 100.0
    }

    /// NURand function per TPC-C spec (Non-Uniform Random)
    pub fn nurand(&mut self, a: i64, x: i64, y: i64) -> i64 {
        let c = match a {
            255 => 123,  // For C_LAST
            1023 => 456, // For C_ID
            8191 => 789, // For OL_I_ID
            _ => 0,
        };
        (((self.random_int(0, a) | self.random_int(x, y)) + c) % (y - x + 1)) + x
    }

    /// Generate random a-string (alphanumeric)
    pub fn random_astring(&mut self, min_len: usize, max_len: usize) -> String {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let len = self.random_int(min_len as i64, max_len as i64) as usize;
        (0..len)
            .map(|_| CHARS[self.random_int(0, CHARS.len() as i64 - 1) as usize] as char)
            .collect()
    }

    /// Generate random n-string (numeric)
    pub fn random_nstring(&mut self, min_len: usize, max_len: usize) -> String {
        const DIGITS: &[u8] = b"0123456789";
        let len = self.random_int(min_len as i64, max_len as i64) as usize;
        (0..len).map(|_| DIGITS[self.random_int(0, 9) as usize] as char).collect()
    }

    /// Generate customer last name from number (0-999)
    pub fn last_name(num: i64) -> String {
        const SYLLABLES: [&str; 10] =
            ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"];
        let s1 = SYLLABLES[(num / 100) as usize];
        let s2 = SYLLABLES[((num / 10) % 10) as usize];
        let s3 = SYLLABLES[(num % 10) as usize];
        format!("{}{}{}", s1, s2, s3)
    }

    /// Generate random zip code
    pub fn random_zip(&mut self) -> String {
        format!("{}11111", self.random_nstring(4, 4))
    }

    /// Generate random phone number
    pub fn random_phone(&mut self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.random_nstring(3, 3),
            self.random_nstring(3, 3),
            self.random_nstring(3, 3),
            self.random_nstring(4, 4)
        )
    }

    /// Generate current timestamp string
    pub fn current_timestamp() -> String {
        // Use a fixed timestamp for reproducibility
        "2024-01-01 00:00:00".to_string()
    }

    /// Generate data field with possible "ORIGINAL" marker
    pub fn random_data(&mut self, min_len: usize, max_len: usize, original_pct: u8) -> String {
        let mut data = self.random_astring(min_len, max_len);
        if self.random_int(1, 100) <= original_pct as i64 {
            // Insert "ORIGINAL" at random position
            let pos = self.random_int(0, (data.len().saturating_sub(8)) as i64) as usize;
            data.replace_range(pos..pos + 8.min(data.len() - pos), "ORIGINAL");
        }
        data
    }
}

/// TPC-C data generator
pub struct TPCCData {
    pub scale_factor: i32, // Number of warehouses (always >= 1)
    raw_scale: f64,        // Raw scale factor for micro-scaling
    pub rng: TPCCRng,
}

impl TPCCData {
    pub fn new(scale_factor: f64) -> Self {
        Self {
            scale_factor: scale_factor.max(1.0) as i32,
            raw_scale: scale_factor,
            rng: TPCCRng::new(42), // Fixed seed for reproducibility
        }
    }

    /// Number of warehouses
    pub fn num_warehouses(&self) -> i32 {
        self.scale_factor
    }

    /// Base constants for SF=1
    pub const DISTRICTS_PER_WAREHOUSE_BASE: i32 = 10;
    pub const CUSTOMERS_PER_DISTRICT_BASE: i32 = 3000;
    pub const ORDERS_PER_DISTRICT_BASE: i32 = 3000;
    pub const NEW_ORDERS_PER_DISTRICT_BASE: i32 = 900;
    pub const NUM_ITEMS_BASE: i32 = 100000;

    /// Districts per warehouse (always 10)
    pub const DISTRICTS_PER_WAREHOUSE: i32 = 10;

    /// Customers per district (always 3000)
    pub const CUSTOMERS_PER_DISTRICT: i32 = 3000;

    /// Orders per district (always 3000 initially)
    pub const ORDERS_PER_DISTRICT: i32 = 3000;

    /// New orders per district (30% of orders = 900)
    pub const NEW_ORDERS_PER_DISTRICT: i32 = 900;

    /// Items (always 100,000)
    pub const NUM_ITEMS: i32 = 100000;

    /// Average order lines per order
    pub const AVG_ORDER_LINES: i32 = 10;

    /// Check if micro mode is enabled (SF < 1)
    pub fn is_micro_mode(&self) -> bool {
        self.raw_scale < 1.0
    }

    /// Get scaled number of items (for micro mode)
    pub fn num_items(&self) -> i32 {
        if self.raw_scale < 1.0 {
            ((Self::NUM_ITEMS_BASE as f64 * self.raw_scale) as i32).max(100)
        } else {
            Self::NUM_ITEMS_BASE
        }
    }

    /// Get scaled districts per warehouse (for micro mode)
    pub fn districts_per_warehouse(&self) -> i32 {
        if self.raw_scale < 1.0 {
            ((Self::DISTRICTS_PER_WAREHOUSE_BASE as f64 * self.raw_scale.sqrt()) as i32).max(1)
        } else {
            Self::DISTRICTS_PER_WAREHOUSE_BASE
        }
    }

    /// Get scaled customers per district (for micro mode)
    pub fn customers_per_district(&self) -> i32 {
        if self.raw_scale < 1.0 {
            ((Self::CUSTOMERS_PER_DISTRICT_BASE as f64 * self.raw_scale) as i32).max(10)
        } else {
            Self::CUSTOMERS_PER_DISTRICT_BASE
        }
    }

    /// Get scaled orders per district (for micro mode)
    pub fn orders_per_district(&self) -> i32 {
        if self.raw_scale < 1.0 {
            ((Self::ORDERS_PER_DISTRICT_BASE as f64 * self.raw_scale) as i32).max(10)
        } else {
            Self::ORDERS_PER_DISTRICT_BASE
        }
    }

    /// Get scaled new orders per district (for micro mode, 30% of orders)
    pub fn new_orders_per_district(&self) -> i32 {
        (self.orders_per_district() * 30 / 100).max(3)
    }
}

/// Warehouse record
#[derive(Debug, Clone)]
pub struct Warehouse {
    pub w_id: i32,
    pub w_name: String,
    pub w_street_1: String,
    pub w_street_2: String,
    pub w_city: String,
    pub w_state: String,
    pub w_zip: String,
    pub w_tax: f64,
    pub w_ytd: f64,
}

/// District record
#[derive(Debug, Clone)]
pub struct District {
    pub d_id: i32,
    pub d_w_id: i32,
    pub d_name: String,
    pub d_street_1: String,
    pub d_street_2: String,
    pub d_city: String,
    pub d_state: String,
    pub d_zip: String,
    pub d_tax: f64,
    pub d_ytd: f64,
    pub d_next_o_id: i32,
}

/// Customer record
#[derive(Debug, Clone)]
pub struct Customer {
    pub c_id: i32,
    pub c_d_id: i32,
    pub c_w_id: i32,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street_1: String,
    pub c_street_2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: String,
    pub c_credit: String,
    pub c_credit_lim: f64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: i32,
    pub c_delivery_cnt: i32,
    pub c_data: String,
}

/// History record
#[derive(Debug, Clone)]
pub struct History {
    pub h_c_id: i32,
    pub h_c_d_id: i32,
    pub h_c_w_id: i32,
    pub h_d_id: i32,
    pub h_w_id: i32,
    pub h_date: String,
    pub h_amount: f64,
    pub h_data: String,
}

/// Order record
#[derive(Debug, Clone)]
pub struct Order {
    pub o_id: i32,
    pub o_d_id: i32,
    pub o_w_id: i32,
    pub o_c_id: i32,
    pub o_entry_d: String,
    pub o_carrier_id: Option<i32>,
    pub o_ol_cnt: i32,
    pub o_all_local: i32,
}

/// New-Order record
#[derive(Debug, Clone)]
pub struct NewOrder {
    pub no_o_id: i32,
    pub no_d_id: i32,
    pub no_w_id: i32,
}

/// Order-Line record
#[derive(Debug, Clone)]
pub struct OrderLine {
    pub ol_o_id: i32,
    pub ol_d_id: i32,
    pub ol_w_id: i32,
    pub ol_number: i32,
    pub ol_i_id: i32,
    pub ol_supply_w_id: i32,
    pub ol_delivery_d: Option<String>,
    pub ol_quantity: i32,
    pub ol_amount: f64,
    pub ol_dist_info: String,
}

/// Item record
#[derive(Debug, Clone)]
pub struct Item {
    pub i_id: i32,
    pub i_im_id: i32,
    pub i_name: String,
    pub i_price: f64,
    pub i_data: String,
}

/// Stock record
#[derive(Debug, Clone)]
pub struct Stock {
    pub s_i_id: i32,
    pub s_w_id: i32,
    pub s_quantity: i32,
    pub s_dist_01: String,
    pub s_dist_02: String,
    pub s_dist_03: String,
    pub s_dist_04: String,
    pub s_dist_05: String,
    pub s_dist_06: String,
    pub s_dist_07: String,
    pub s_dist_08: String,
    pub s_dist_09: String,
    pub s_dist_10: String,
    pub s_ytd: i32,
    pub s_order_cnt: i32,
    pub s_remote_cnt: i32,
    pub s_data: String,
}

impl TPCCData {
    /// Generate warehouse record
    pub fn gen_warehouse(&mut self, w_id: i32) -> Warehouse {
        Warehouse {
            w_id,
            w_name: self.rng.random_astring(6, 10),
            w_street_1: self.rng.random_astring(10, 20),
            w_street_2: self.rng.random_astring(10, 20),
            w_city: self.rng.random_astring(10, 20),
            w_state: self.rng.random_astring(2, 2),
            w_zip: self.rng.random_zip(),
            w_tax: self.rng.random_decimal(0.0, 0.2),
            w_ytd: 300000.0,
        }
    }

    /// Generate district record
    pub fn gen_district(&mut self, d_id: i32, w_id: i32) -> District {
        District {
            d_id,
            d_w_id: w_id,
            d_name: self.rng.random_astring(6, 10),
            d_street_1: self.rng.random_astring(10, 20),
            d_street_2: self.rng.random_astring(10, 20),
            d_city: self.rng.random_astring(10, 20),
            d_state: self.rng.random_astring(2, 2),
            d_zip: self.rng.random_zip(),
            d_tax: self.rng.random_decimal(0.0, 0.2),
            d_ytd: 30000.0,
            d_next_o_id: self.orders_per_district() + 1,
        }
    }

    /// Generate customer record
    pub fn gen_customer(&mut self, c_id: i32, d_id: i32, w_id: i32) -> Customer {
        let last_name = if c_id <= 1000 {
            TPCCRng::last_name((c_id - 1) as i64)
        } else {
            TPCCRng::last_name(self.rng.nurand(255, 0, 999))
        };

        let credit = if self.rng.random_int(1, 100) <= 10 {
            "BC" // Bad credit (10%)
        } else {
            "GC" // Good credit (90%)
        };

        Customer {
            c_id,
            c_d_id: d_id,
            c_w_id: w_id,
            c_first: self.rng.random_astring(8, 16),
            c_middle: "OE".to_string(),
            c_last: last_name,
            c_street_1: self.rng.random_astring(10, 20),
            c_street_2: self.rng.random_astring(10, 20),
            c_city: self.rng.random_astring(10, 20),
            c_state: self.rng.random_astring(2, 2),
            c_zip: self.rng.random_zip(),
            c_phone: self.rng.random_phone(),
            c_since: TPCCRng::current_timestamp(),
            c_credit: credit.to_string(),
            c_credit_lim: 50000.0,
            c_discount: self.rng.random_decimal(0.0, 0.5),
            c_balance: -10.0,
            c_ytd_payment: 10.0,
            c_payment_cnt: 1,
            c_delivery_cnt: 0,
            c_data: self.rng.random_astring(300, 500),
        }
    }

    /// Generate item record
    pub fn gen_item(&mut self, i_id: i32) -> Item {
        Item {
            i_id,
            i_im_id: self.rng.random_int(1, 10000) as i32,
            i_name: self.rng.random_astring(14, 24),
            i_price: self.rng.random_decimal(1.0, 100.0),
            i_data: self.rng.random_data(26, 50, 10),
        }
    }

    /// Generate stock record
    pub fn gen_stock(&mut self, i_id: i32, w_id: i32) -> Stock {
        Stock {
            s_i_id: i_id,
            s_w_id: w_id,
            s_quantity: self.rng.random_int(10, 100) as i32,
            s_dist_01: self.rng.random_astring(24, 24),
            s_dist_02: self.rng.random_astring(24, 24),
            s_dist_03: self.rng.random_astring(24, 24),
            s_dist_04: self.rng.random_astring(24, 24),
            s_dist_05: self.rng.random_astring(24, 24),
            s_dist_06: self.rng.random_astring(24, 24),
            s_dist_07: self.rng.random_astring(24, 24),
            s_dist_08: self.rng.random_astring(24, 24),
            s_dist_09: self.rng.random_astring(24, 24),
            s_dist_10: self.rng.random_astring(24, 24),
            s_ytd: 0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: self.rng.random_data(26, 50, 10),
        }
    }

    /// Generate history record
    pub fn gen_history(&mut self, c_id: i32, d_id: i32, w_id: i32) -> History {
        History {
            h_c_id: c_id,
            h_c_d_id: d_id,
            h_c_w_id: w_id,
            h_d_id: d_id,
            h_w_id: w_id,
            h_date: TPCCRng::current_timestamp(),
            h_amount: 10.0,
            h_data: self.rng.random_astring(12, 24),
        }
    }

    /// Generate order record
    pub fn gen_order(&mut self, o_id: i32, d_id: i32, w_id: i32, c_id: i32) -> Order {
        // First 70% of orders have carrier_id (delivered), last 30% do not (new orders)
        let delivered_threshold = (self.orders_per_district() as f64 * 0.7) as i32;
        let carrier_id = if o_id <= delivered_threshold {
            Some(self.rng.random_int(1, 10) as i32)
        } else {
            None
        };

        Order {
            o_id,
            o_d_id: d_id,
            o_w_id: w_id,
            o_c_id: c_id,
            o_entry_d: TPCCRng::current_timestamp(),
            o_carrier_id: carrier_id,
            o_ol_cnt: self.rng.random_int(5, 15) as i32,
            o_all_local: 1,
        }
    }

    /// Generate order line record
    pub fn gen_order_line(
        &mut self,
        o_id: i32,
        d_id: i32,
        w_id: i32,
        ol_number: i32,
        delivered: bool,
    ) -> OrderLine {
        let num_items = self.num_items();
        OrderLine {
            ol_o_id: o_id,
            ol_d_id: d_id,
            ol_w_id: w_id,
            ol_number,
            ol_i_id: self.rng.random_int(1, num_items as i64) as i32,
            ol_supply_w_id: w_id,
            ol_delivery_d: if delivered { Some(TPCCRng::current_timestamp()) } else { None },
            ol_quantity: 5,
            ol_amount: if delivered { 0.0 } else { self.rng.random_decimal(0.01, 9999.99) },
            ol_dist_info: self.rng.random_astring(24, 24),
        }
    }

    /// Generate new order record (for orders 2101-3000)
    pub fn gen_new_order(&mut self, o_id: i32, d_id: i32, w_id: i32) -> NewOrder {
        NewOrder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id }
    }
}
