// ============================================================================
// Type Serialization
// ============================================================================
//
// Handles serialization of various SQL types (CharacterUnit, TrimPosition, etc.).

use vibesql_ast::{CharacterUnit, FulltextMode, IntervalUnit, PseudoTable, TrimPosition};

impl_simple_enum_serialization!(
    CharacterUnit,
    write_character_unit,
    read_character_unit,
    "character unit",
    {
        CharacterUnit::Characters => 0,
        CharacterUnit::Octets => 1,
    }
);

impl_simple_enum_serialization!(
    TrimPosition,
    write_trim_position,
    read_trim_position,
    "trim position",
    {
        TrimPosition::Both => 0,
        TrimPosition::Leading => 1,
        TrimPosition::Trailing => 2,
    }
);

impl_simple_enum_serialization!(
    IntervalUnit,
    write_interval_unit,
    read_interval_unit,
    "interval unit",
    {
        IntervalUnit::Microsecond => 0,
        IntervalUnit::Second => 1,
        IntervalUnit::Minute => 2,
        IntervalUnit::Hour => 3,
        IntervalUnit::Day => 4,
        IntervalUnit::Week => 5,
        IntervalUnit::Month => 6,
        IntervalUnit::Quarter => 7,
        IntervalUnit::Year => 8,
        IntervalUnit::SecondMicrosecond => 9,
        IntervalUnit::MinuteMicrosecond => 10,
        IntervalUnit::MinuteSecond => 11,
        IntervalUnit::HourMicrosecond => 12,
        IntervalUnit::HourSecond => 13,
        IntervalUnit::HourMinute => 14,
        IntervalUnit::DayMicrosecond => 15,
        IntervalUnit::DaySecond => 16,
        IntervalUnit::DayMinute => 17,
        IntervalUnit::DayHour => 18,
        IntervalUnit::YearMonth => 19,
    }
);

impl_simple_enum_serialization!(
    FulltextMode,
    write_fulltext_mode,
    read_fulltext_mode,
    "fulltext mode",
    {
        FulltextMode::NaturalLanguage => 0,
        FulltextMode::Boolean => 1,
        FulltextMode::QueryExpansion => 2,
    }
);

impl_simple_enum_serialization!(
    PseudoTable,
    write_pseudo_table,
    read_pseudo_table,
    "pseudo table",
    {
        PseudoTable::Old => 0,
        PseudoTable::New => 1,
    }
);
