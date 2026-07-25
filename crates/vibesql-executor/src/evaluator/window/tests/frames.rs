use vibesql_ast::{
    Expression, FrameBound, FrameUnit, NullsOrder, OrderByItem, OrderDirection, WindowFrame,
};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

/// Simple eval_fn for tests
fn test_eval_fn(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
    evaluate_expression(expr, row)
}

#[test]
fn test_calculate_frame_default() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    // Default frame WITHOUT ORDER BY: entire partition
    let frame = calculate_frame(&partition, 2, &None, &None, &test_eval_fn);

    assert_eq!(frame, 0..5); // Entire partition (no ORDER BY)
}

#[test]
fn test_calculate_frame_unbounded_preceding() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::UnboundedPreceding,
        end: Some(FrameBound::CurrentRow),
        exclude: None,
    };

    let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec), &test_eval_fn);

    assert_eq!(frame, 0..3); // Rows 0, 1, 2
}

#[test]
fn test_calculate_frame_preceding() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Integer(2)))),
        end: Some(FrameBound::CurrentRow),
        exclude: None,
    };

    let frame = calculate_frame(&partition, 3, &None, &Some(frame_spec), &test_eval_fn);

    // 2 PRECEDING from row 3 is row 1, so rows 1, 2, 3
    assert_eq!(frame, 1..4);
}

#[test]
fn test_calculate_frame_following() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::CurrentRow,
        end: Some(FrameBound::Following(Box::new(Expression::Literal(SqlValue::Integer(2))))),
        exclude: None,
    };

    let frame = calculate_frame(&partition, 1, &None, &Some(frame_spec), &test_eval_fn);

    // Current row 1 to 2 FOLLOWING (row 3), so rows 1, 2, 3
    assert_eq!(frame, 1..4);
}

#[test]
fn test_calculate_frame_unbounded_following() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::CurrentRow,
        end: Some(FrameBound::UnboundedFollowing),
        exclude: None,
    };

    let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec), &test_eval_fn);

    // Current row 2 to end: rows 2, 3, 4
    assert_eq!(frame, 2..5);
}

// ===== RANGE frames with NULLS FIRST/LAST and non-numeric keys (#5191) =====

fn make_value_rows(values: Vec<SqlValue>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![v])).collect()
}

fn order_by_first_col(
    direction: OrderDirection,
    nulls_order: Option<NullsOrder>,
) -> Option<Vec<OrderByItem>> {
    Some(vec![OrderByItem {
        expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("0", false)),
        direction,
        nulls_order,
    }])
}

#[test]
fn test_range_frame_asc_nulls_last() {
    // window8.test 4.3.2: ORDER BY a NULLS LAST
    // Partition sorted ASC NULLS LAST: 10, 10, NULL, NULL, NULL
    let partition = Partition::new(make_value_rows(vec![
        SqlValue::Integer(10),
        SqlValue::Integer(10),
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
    ]));
    let order_by = order_by_first_col(OrderDirection::Asc, Some(NullsOrder::Last));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Range,
        start: FrameBound::UnboundedPreceding,
        end: Some(FrameBound::Following(Box::new(Expression::Literal(SqlValue::Integer(10))))),
        exclude: None,
    };

    // Non-NULL rows: 10 FOLLOWING reaches value 20; the NULL suffix sorts
    // after all values and is out of range of the offset
    let frame = calculate_frame(&partition, 0, &order_by, &Some(frame_spec.clone()), &test_eval_fn);
    assert_eq!(frame, 0..2);

    // NULL rows: offset bounds collapse to the NULL peer group, which runs
    // to the end of the partition
    let frame = calculate_frame(&partition, 2, &order_by, &Some(frame_spec), &test_eval_fn);
    assert_eq!(frame, 0..5);
}

#[test]
fn test_range_frame_desc_nulls_first() {
    // window8.test 4.5.2: ORDER BY a DESC NULLS FIRST
    // Partition sorted DESC NULLS FIRST: NULL, NULL, NULL, 10, 10
    let partition = Partition::new(make_value_rows(vec![
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Integer(10),
        SqlValue::Integer(10),
    ]));
    let order_by = order_by_first_col(OrderDirection::Desc, Some(NullsOrder::First));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Range,
        start: FrameBound::UnboundedPreceding,
        end: Some(FrameBound::Following(Box::new(Expression::Literal(SqlValue::Integer(10))))),
        exclude: None,
    };

    // NULL rows: offset bounds collapse to the NULL peer group (prefix)
    let frame = calculate_frame(&partition, 0, &order_by, &Some(frame_spec.clone()), &test_eval_fn);
    assert_eq!(frame, 0..3);

    // Non-NULL rows (DESC): 10 FOLLOWING reaches value 0, which covers both
    // 10s; the NULL prefix is included positionally via UNBOUNDED PRECEDING
    let frame = calculate_frame(&partition, 3, &order_by, &Some(frame_spec), &test_eval_fn);
    assert_eq!(frame, 0..5);
}

#[test]
fn test_range_frame_text_keys_collate_nocase() {
    // windowB.test 8.1: ORDER BY a COLLATE nocase RANGE BETWEEN
    // 10.0 PRECEDING AND 5.0 PRECEDING over text keys.
    // Numeric offset arithmetic is a no-op on text, so the frame collapses
    // to the current row's peer group. Boundary comparisons must honor the
    // explicit collation (binary comparison would order 'BB' before 'aa').
    let partition = Partition::new(make_value_rows(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("aa")),
        SqlValue::Varchar(arcstr::ArcStr::from("BB")),
        SqlValue::Varchar(arcstr::ArcStr::from("CC")),
        SqlValue::Varchar(arcstr::ArcStr::from("dd")),
    ]));
    let order_by = Some(vec![OrderByItem {
        expr: Expression::Collate {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "0", false,
            ))),
            collation: "nocase".to_string(),
        },
        direction: OrderDirection::Asc,
        nulls_order: None,
    }]);

    // Test eval that peels COLLATE before resolving the column
    fn collate_eval_fn(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
        match expr {
            Expression::Collate { expr, .. } => collate_eval_fn(expr, row),
            _ => test_eval_fn(expr, row),
        }
    }

    let frame_spec = WindowFrame {
        unit: FrameUnit::Range,
        start: FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Real(10.0)))),
        end: Some(FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Real(5.0))))),
        exclude: None,
    };

    for i in 0..4 {
        let frame =
            calculate_frame(&partition, i, &order_by, &Some(frame_spec.clone()), &collate_eval_fn);
        assert_eq!(frame, i..i + 1, "frame for row {} should be its own peer group", i);
    }
}

/// Build an `N PRECEDING` bound from an integer literal.
fn preceding(n: i64) -> FrameBound {
    FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Integer(n))))
}

/// Build an `N FOLLOWING` bound from an integer literal.
fn following(n: i64) -> FrameBound {
    FrameBound::Following(Box::new(Expression::Literal(SqlValue::Integer(n))))
}

fn frame(unit: FrameUnit, start: FrameBound, end: Option<FrameBound>) -> Option<WindowFrame> {
    Some(WindowFrame { unit, start, end, exclude: None })
}

// window6.test 9.7.1: `ROWS BETWEEN CURRENT ROW AND 4 PRECEDING` — a CURRENT
// ROW start paired with an <expr> PRECEDING end is rejected by SQLite as an
// "unsupported frame specification".
#[test]
fn test_validate_frame_current_to_preceding_rejected() {
    let spec = frame(FrameUnit::Rows, FrameBound::CurrentRow, Some(preceding(4)));
    assert_eq!(validate_frame(&spec), Err("unsupported frame specification".to_string()));
}

// window6.test 9.7.2: a single `ROWS 4 FOLLOWING` bound (end defaults to CURRENT
// ROW) starts at <expr> FOLLOWING, which SQLite rejects.
#[test]
fn test_validate_frame_single_following_rejected() {
    let spec = frame(FrameUnit::Rows, following(4), None);
    assert_eq!(validate_frame(&spec), Err("unsupported frame specification".to_string()));
}

// window6.test 9.7.3: `ROWS BETWEEN 4 FOLLOWING AND CURRENT ROW`.
#[test]
fn test_validate_frame_following_to_current_rejected() {
    let spec = frame(FrameUnit::Rows, following(4), Some(FrameBound::CurrentRow));
    assert_eq!(validate_frame(&spec), Err("unsupported frame specification".to_string()));
}

// window6.test 9.7.4: `ROWS BETWEEN 4 FOLLOWING AND 2 PRECEDING`.
#[test]
fn test_validate_frame_following_to_preceding_rejected() {
    let spec = frame(FrameUnit::Rows, following(4), Some(preceding(2)));
    assert_eq!(validate_frame(&spec), Err("unsupported frame specification".to_string()));
}

// Valid frames whose bounds are correctly ordered must NOT be rejected by the
// new ordering check. These mirror frames exercised by window1/window5/window6.
#[test]
fn test_validate_frame_valid_specs_accepted() {
    // BETWEEN 1 PRECEDING AND 1 FOLLOWING
    assert!(validate_frame(&frame(FrameUnit::Rows, preceding(1), Some(following(1)))).is_ok());
    // BETWEEN 2 FOLLOWING AND 3 FOLLOWING (window6-5.5)
    assert!(validate_frame(&frame(FrameUnit::Rows, following(2), Some(following(3)))).is_ok());
    // BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    assert!(validate_frame(&frame(
        FrameUnit::Range,
        FrameBound::UnboundedPreceding,
        Some(FrameBound::UnboundedFollowing),
    ))
    .is_ok());
    // BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING (window1-10.5)
    assert!(validate_frame(&frame(
        FrameUnit::Rows,
        FrameBound::CurrentRow,
        Some(FrameBound::UnboundedFollowing),
    ))
    .is_ok());
    // Single CURRENT ROW bound
    assert!(validate_frame(&frame(FrameUnit::Rows, FrameBound::CurrentRow, None)).is_ok());
}

// SQLite coerces boolean literals to their numeric value (`true` -> 1, `false`
// -> 0) when they appear as a frame offset. Previously VibeSQL rejected a
// boolean offset as "frame ending offset must be a non-negative integer"
// (window1 30.0: `RANGE BETWEEN 5.2 PRECEDING AND true PRECEDING`).
#[test]
fn test_validate_frame_boolean_offset_accepted() {
    let bool_bound =
        |b: bool| FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Boolean(b))));
    // BETWEEN 5 PRECEDING AND true PRECEDING (true -> 1, non-negative, accepted)
    assert!(validate_frame(&frame(FrameUnit::Rows, preceding(5), Some(bool_bound(true)))).is_ok());
    // BETWEEN 5 PRECEDING AND false PRECEDING (false -> 0, likewise valid)
    assert!(validate_frame(&frame(FrameUnit::Rows, preceding(5), Some(bool_bound(false)))).is_ok());
}
