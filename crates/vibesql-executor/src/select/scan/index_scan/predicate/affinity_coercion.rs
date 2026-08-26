//! WHERE-clause literal affinity coercion for index probes (issue #6555)
//!
//! Index probes look up raw `SqlValue` keys in the storage-layer index (see
//! `vibesql_storage::database::indexes::value_normalization`), which no
//! longer guesses at cross-storage-class coercion (a numeric-looking TEXT
//! literal like `'123'` is never silently turned into a number there,
//! because that would incorrectly collapse `1` (INTEGER) and `'1'` (TEXT)
//! into the same key on a column with TEXT/BLOB affinity).
//!
//! Row *values* reaching the index (via INSERT/UPDATE) are already coerced
//! to the column's declared affinity before they get there (see
//! `vibesql_executor::insert::validation::coerce_value`), so the index keys
//! for NUMERIC/INTEGER/REAL-affinity columns are already stored as numbers.
//! But a WHERE-clause *literal* extracted from a parsed query (e.g. `'123'`
//! in `WHERE int_col = '123'`) has never been through that coercion — it is
//! exactly the token the parser produced. Without this module, an
//! INTEGER-affinity column's probe would look up `Varchar("123")` against an
//! index storing `Integer(123)` and silently miss every row.
//!
//! This module coerces WHERE-clause literal probe values to the *declared*
//! affinity of the specific index column they're compared against — using
//! the same affinity rules as `coerce_value_to_column_type` (already used by
//! the PRIMARY KEY hash-map fast path in
//! `select::executor::fast_path::pk_lookup`) — before those literals reach
//! `IndexData::get`/`contains_key`/etc. A column with TEXT/BLOB affinity is
//! left untouched by `coerce_value_to_column_type`, so this coercion is a
//! no-op for exactly the columns where #6555 was filed, and only affects
//! NUMERIC/INTEGER/REAL-affinity columns, matching SQLite's affinity rules.

use vibesql_catalog::TableSchema;
use vibesql_types::SqlValue;

use super::{
    CompositePredicateType, IndexPredicate, PrefixPredicateResult, PrefixWithRangeResult,
    RangePredicate,
};
use crate::evaluator::coercion::coerce_value_to_column_type;

/// Look up a column's declared `DataType` by name (case-insensitive), if it
/// exists in `schema`.
fn column_data_type<'a>(
    schema: &'a TableSchema,
    col_name: &str,
) -> Option<&'a vibesql_types::DataType> {
    schema.get_column_index(col_name).map(|idx| &schema.columns[idx].data_type)
}

/// Coerce a single probe literal to `col_name`'s declared affinity. A no-op
/// when the column isn't found (e.g. a synthetic name) or has TEXT/BLOB/NONE
/// affinity.
fn coerce_value(schema: &TableSchema, col_name: &str, value: SqlValue) -> SqlValue {
    match column_data_type(schema, col_name) {
        Some(data_type) => coerce_value_to_column_type(value, data_type),
        None => value,
    }
}

/// Coerce the literal bound(s) of a single-column index predicate
/// (`IndexPredicate::Range`/`In`) to `column_name`'s declared affinity.
///
/// `column_name` should come from `IndexedColumn::column_name()`; callers
/// must skip this for expression-index columns (no single declared type).
pub(crate) fn coerce_index_predicate_for_affinity(
    predicate: Option<IndexPredicate>,
    column_name: &str,
    schema: &TableSchema,
) -> Option<IndexPredicate> {
    predicate.map(|pred| match pred {
        IndexPredicate::Range(range) => IndexPredicate::Range(RangePredicate {
            start: range.start.map(|v| coerce_value(schema, column_name, v)),
            end: range.end.map(|v| coerce_value(schema, column_name, v)),
            inclusive_start: range.inclusive_start,
            inclusive_end: range.inclusive_end,
            exclude_nulls: range.exclude_nulls,
        }),
        IndexPredicate::In(values) => IndexPredicate::In(
            values.into_iter().map(|v| coerce_value(schema, column_name, v)).collect(),
        ),
    })
}

/// Coerce every value in a composite (multi-column) predicate list to each
/// covered column's declared affinity. `predicates` and `column_names` are
/// positionally aligned, as guaranteed by `extract_composite_predicates_with_in`.
pub(crate) fn coerce_composite_predicates_for_affinity(
    predicates: Vec<CompositePredicateType>,
    column_names: &[&str],
    schema: &TableSchema,
) -> Vec<CompositePredicateType> {
    predicates
        .into_iter()
        .zip(column_names.iter())
        .map(|(pred, col_name)| match pred {
            CompositePredicateType::Equality(v) => {
                CompositePredicateType::Equality(coerce_value(schema, col_name, v))
            }
            CompositePredicateType::In(values) => CompositePredicateType::In(
                values.into_iter().map(|v| coerce_value(schema, col_name, v)).collect(),
            ),
        })
        .collect()
}

/// Coerce a flat list of equality-lookup key values, already positionally
/// aligned with `column_names`, to each column's declared affinity in place.
///
/// Used by the `select::executor::fast_path` secondary-index lookup helpers
/// (`try_secondary_index_lookup_fast`, `try_secondary_index_prefix_with_limit_fast`),
/// which build lookup keys directly from the WHERE clause rather than through
/// `IndexPredicate`/`CompositePredicateType` — a second, independent call
/// site for the same issue #6555 problem (a raw WHERE-clause literal probing
/// an index that no longer guesses string->number coercion on its own).
pub(crate) fn coerce_lookup_keys_for_affinity(
    key_values: &mut [SqlValue],
    column_names: &[&str],
    schema: &TableSchema,
) {
    for (value, col_name) in key_values.iter_mut().zip(column_names.iter()) {
        let owned = std::mem::replace(value, SqlValue::Null);
        *value = coerce_value(schema, col_name, owned);
    }
}

/// Coerce a prefix-equality lookup's key values to each covered column's
/// declared affinity. `result.prefix_key[i]` corresponds to `column_names[i]`.
pub(crate) fn coerce_prefix_result_for_affinity(
    result: PrefixPredicateResult,
    column_names: &[&str],
    schema: &TableSchema,
) -> PrefixPredicateResult {
    let prefix_key = result
        .prefix_key
        .into_iter()
        .zip(column_names.iter())
        .map(|(v, col_name)| coerce_value(schema, col_name, v))
        .collect();
    PrefixPredicateResult { prefix_key, covered_columns: result.covered_columns }
}

/// Coerce a prefix + trailing-range lookup's key/bound values to each
/// covered column's declared affinity. `result.prefix_key[i]` corresponds to
/// `column_names[i]`; the trailing range bounds correspond to
/// `column_names[result.prefix_key.len()]` (the next index column after the
/// prefix), matching how `extract_prefix_with_trailing_range` builds `result`.
pub(crate) fn coerce_prefix_with_range_result_for_affinity(
    result: PrefixWithRangeResult,
    column_names: &[&str],
    schema: &TableSchema,
) -> PrefixWithRangeResult {
    let prefix_len = result.prefix_key.len();
    let prefix_key = result
        .prefix_key
        .into_iter()
        .zip(column_names.iter())
        .map(|(v, col_name)| coerce_value(schema, col_name, v))
        .collect();
    let range_col_name = column_names.get(prefix_len).copied();
    let coerce_bound = |bound: Option<SqlValue>| match (bound, range_col_name) {
        (Some(v), Some(col_name)) => Some(coerce_value(schema, col_name, v)),
        (v, _) => v,
    };
    PrefixWithRangeResult {
        prefix_key,
        lower_bound: coerce_bound(result.lower_bound),
        inclusive_lower: result.inclusive_lower,
        upper_bound: coerce_bound(result.upper_bound),
        inclusive_upper: result.inclusive_upper,
        covered_columns: result.covered_columns,
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn schema_with(columns: Vec<(&str, DataType)>) -> TableSchema {
        let columns = columns
            .into_iter()
            .map(|(name, data_type)| ColumnSchema::new(name.to_string(), data_type, true))
            .collect();
        TableSchema::new("t".to_string(), columns)
    }

    #[test]
    fn integer_affinity_column_coerces_string_literal() {
        let schema = schema_with(vec![("a", DataType::Integer)]);
        let predicate = Some(IndexPredicate::Range(RangePredicate {
            start: Some(SqlValue::Varchar(arcstr::ArcStr::from("123"))),
            end: Some(SqlValue::Varchar(arcstr::ArcStr::from("123"))),
            inclusive_start: true,
            inclusive_end: true,
            exclude_nulls: false,
        }));
        let coerced = coerce_index_predicate_for_affinity(predicate, "a", &schema).unwrap();
        match coerced {
            IndexPredicate::Range(range) => {
                assert_eq!(range.start, Some(SqlValue::Integer(123)));
                assert_eq!(range.end, Some(SqlValue::Integer(123)));
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn text_affinity_column_leaves_string_literal_unchanged() {
        let schema = schema_with(vec![("a", DataType::Varchar { max_length: None })]);
        let predicate = Some(IndexPredicate::In(vec![SqlValue::Integer(1)]));
        let coerced = coerce_index_predicate_for_affinity(predicate, "a", &schema).unwrap();
        match coerced {
            // A TEXT-affinity column with a numeric literal is converted to
            // its string representation (SQLite affinity rule), never left
            // as the original numeric SqlValue variant.
            IndexPredicate::In(values) => {
                assert_eq!(values, vec![SqlValue::Varchar(arcstr::ArcStr::from("1"))]);
            }
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn blob_affinity_column_leaves_text_literal_unchanged() {
        // No declared type => BLOB/NONE affinity; the #6555 repro column.
        let schema = schema_with(vec![("p", DataType::Null)]);
        let predicate = Some(IndexPredicate::Range(RangePredicate {
            start: Some(SqlValue::Varchar(arcstr::ArcStr::from("1"))),
            end: Some(SqlValue::Varchar(arcstr::ArcStr::from("1"))),
            inclusive_start: true,
            inclusive_end: true,
            exclude_nulls: false,
        }));
        let coerced = coerce_index_predicate_for_affinity(predicate, "p", &schema).unwrap();
        match coerced {
            IndexPredicate::Range(range) => {
                assert_eq!(range.start, Some(SqlValue::Varchar(arcstr::ArcStr::from("1"))));
                assert_eq!(range.end, Some(SqlValue::Varchar(arcstr::ArcStr::from("1"))));
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn composite_predicates_coerce_per_column_affinity() {
        // Mixed composite index: first column INTEGER affinity, second BLOB affinity.
        let schema = schema_with(vec![("i", DataType::Integer), ("p", DataType::Null)]);
        let predicates = vec![
            CompositePredicateType::Equality(SqlValue::Varchar(arcstr::ArcStr::from("5"))),
            CompositePredicateType::Equality(SqlValue::Varchar(arcstr::ArcStr::from("1"))),
        ];
        let coerced = coerce_composite_predicates_for_affinity(predicates, &["i", "p"], &schema);
        match (&coerced[0], &coerced[1]) {
            (CompositePredicateType::Equality(i), CompositePredicateType::Equality(p)) => {
                assert_eq!(*i, SqlValue::Integer(5));
                assert_eq!(*p, SqlValue::Varchar(arcstr::ArcStr::from("1")));
            }
            _ => panic!("expected Equality/Equality"),
        }
    }

    #[test]
    fn prefix_with_range_result_coerces_trailing_bound_by_its_own_column() {
        // Index (w_id INTEGER, bal INTEGER); WHERE w_id = '1' AND bal < '10'
        let schema = schema_with(vec![("w_id", DataType::Integer), ("bal", DataType::Integer)]);
        let result = PrefixWithRangeResult {
            prefix_key: vec![SqlValue::Varchar(arcstr::ArcStr::from("1"))],
            lower_bound: None,
            inclusive_lower: false,
            upper_bound: Some(SqlValue::Varchar(arcstr::ArcStr::from("10"))),
            inclusive_upper: false,
            covered_columns: Default::default(),
        };
        let coerced =
            coerce_prefix_with_range_result_for_affinity(result, &["w_id", "bal"], &schema);
        assert_eq!(coerced.prefix_key, vec![SqlValue::Integer(1)]);
        assert_eq!(coerced.upper_bound, Some(SqlValue::Integer(10)));
    }
}
