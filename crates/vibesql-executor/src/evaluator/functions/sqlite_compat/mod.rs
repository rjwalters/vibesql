//! SQLite compatibility functions
//!
//! This module implements SQLite-specific scalar functions needed for TCL test compatibility.
//! These functions follow SQLite's exact semantics as documented at:
//! https://www.sqlite.org/lang_corefunc.html
//!
//! ## Organization
//!
//! Functions are organized into submodules by category:
//!
//! - [`type_funcs`]: Type inspection and conversion (TYPEOF, TOREAL, TOINTEGER, INTREAL)
//! - [`blob_funcs`]: Blob and encoding operations (HEX, UNHEX, ZEROBLOB, RANDOMBLOB, QUOTE)
//! - [`string_funcs`]: String manipulation (CHAR, UNICODE, CONCAT_WS, PRINTF)
//! - [`conditional_funcs`]: Conditional logic (IIF, IFNULL)
//! - [`math_funcs`]: Math and query hints (RANDOM, LIKELY, UNLIKELY, LIKELIHOOD)
//! - [`pattern_funcs`]: Pattern matching (LIKE, GLOB, REGEXP/REGEXPI)
//! - [`json_funcs`]: JSON functions (json, json_array, json_object, etc.)

mod blob_funcs;
mod conditional_funcs;
pub(crate) mod json_funcs;
pub(crate) mod jsonb;
mod math_funcs;
mod pattern_funcs;
mod string_funcs;
mod type_funcs;

// Re-export type functions
// Re-export blob/encoding functions
pub(super) use blob_funcs::{hex, quote, randomblob, unhex, zeroblob};
pub(super) use conditional_funcs::ifnull;
// Re-export conditional functions
pub(super) use conditional_funcs::iif;
// Re-export JSON functions
pub(super) use json_funcs::{
    json, json_array, json_array_length, json_error_position, json_extract, json_insert,
    json_object, json_patch, json_quote, json_remove, json_replace, json_set, json_type,
    json_valid, jsonb, jsonb_extract, jsonb_patch, jsonb_remove,
};
// Re-export math/hint functions
pub(super) use math_funcs::likelihood;
pub(super) use math_funcs::{likely, random, unlikely};
// Re-export pattern functions
pub(super) use pattern_funcs::glob;
pub(super) use pattern_funcs::{like, match_default, regexp, regexpi};
// Re-export string functions
pub(super) use string_funcs::char_func;
pub(super) use string_funcs::{concat_ws, printf, unicode, unistr};
pub(super) use type_funcs::{intreal, tointeger, toreal, typeof_func};
