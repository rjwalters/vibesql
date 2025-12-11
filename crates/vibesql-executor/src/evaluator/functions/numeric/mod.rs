//! Numeric and mathematical functions
//!
//! This module implements SQL numeric functions including:
//! - Basic arithmetic: ABS, SIGN, MOD, PI
//! - Rounding: ROUND, TRUNCATE, FLOOR, CEIL/CEILING
//! - Exponential and logarithmic: POWER/POW, SQRT, EXP, LN/LOG, LOG10
//! - Trigonometric: SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2
//! - Angle conversion: RADIANS, DEGREES
//! - Comparison: GREATEST, LEAST
//! - Formatting: FORMAT
//!
//! SQL:1999 Section 6.27: Numeric value functions
//!
//! # Module Organization
//!
//! The numeric functions are organized into logical modules:
//! - `basic` - Basic operations (ABS, SIGN, MOD, PI)
//! - `rounding` - Rounding functions (ROUND, TRUNCATE, FLOOR, CEIL)
//! - `exponential` - Powers and logarithms (POWER, SQRT, EXP, LN, LOG)
//! - `trigonometric` - Trig functions (SIN, COS, TAN, etc.)
//! - `comparison` - Min/max comparison (GREATEST, LEAST)
//! - `decimal` - Number formatting (FORMAT)

mod basic;
mod comparison;
mod decimal;
mod exponential;
mod rounding;
mod trigonometric;

// Re-export all public functions
pub(super) use basic::{abs, mod_fn as mod_func, pi, sign};
pub(super) use comparison::{greatest, least, scalar_max, scalar_min};
pub(super) use decimal::format;
pub(super) use exponential::{exp, ln, log10, power, sqrt};
pub(super) use rounding::{ceil, floor, round, truncate};
pub(super) use trigonometric::{acos, asin, atan, atan2, cos, degrees, radians, sin, tan};
