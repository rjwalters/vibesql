use vibesql_ast::{Expression, FrameBound, FrameUnit, WindowFrame};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

#[test]
fn test_calculate_frame_default() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    // Default frame WITHOUT ORDER BY: entire partition
    let frame = calculate_frame(&partition, 2, &None, &None);

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

    let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec));

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

    let frame = calculate_frame(&partition, 3, &None, &Some(frame_spec));

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

    let frame = calculate_frame(&partition, 1, &None, &Some(frame_spec));

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

    let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec));

    // Current row 2 to end: rows 2, 3, 4
    assert_eq!(frame, 2..5);
}
