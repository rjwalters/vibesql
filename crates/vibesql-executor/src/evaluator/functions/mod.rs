//! SQL Function Implementations
//!
//! This module contains all scalar SQL function implementations, organized by category:
//!
//! - `null_handling`: NULL operations (COALESCE, NULLIF)
//! - `string`: String manipulation (UPPER, SUBSTR, CONCAT, etc.)
//! - `numeric`: Mathematical operations (ABS, ROUND, POWER, SIN, etc.)
//! - `datetime`: Date/time operations (CURRENT_DATE, YEAR, DATE_ADD, etc.)
//! - `control`: Control flow (IF)
//! - `vector`: Vector operations (COSINE_DISTANCE, L2_DISTANCE, INNER_PRODUCT, etc.)
//! - `spatial`: Spatial/geometric functions (ST_GeomFromText, ST_Contains, ST_Intersects, etc.)
//!
//! ## Usage
//!
//! The main entry point is `eval_scalar_function()`, which dispatches to the
//! appropriate module based on the function name.

use crate::errors::ExecutorError;

// Module declarations
mod control;
mod conversion;
pub(crate) mod datetime;
mod null_handling;
mod numeric;
#[cfg(feature = "spatial")]
pub(crate) mod spatial;
mod storage;
pub(crate) mod string;
mod system;
mod vector;

/// Evaluate a scalar function on given argument values
///
/// This handles SQL scalar functions that don't depend on table schemas
/// (unlike aggregates like COUNT, SUM which are handled elsewhere).
pub(super) fn eval_scalar_function(
    name: &str,
    args: &[vibesql_types::SqlValue],
    character_unit: &Option<vibesql_ast::CharacterUnit>,
    sql_mode: &vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match name.to_uppercase().as_str() {
        // NULL handling functions
        "COALESCE" => null_handling::coalesce(args),
        "NULLIF" => null_handling::nullif(args),

        // String functions
        "UPPER" => string::upper(args),
        "LOWER" => string::lower(args),
        "SUBSTRING" => string::substring(args),
        "SUBSTR" => string::substring(args), // Alias for SUBSTRING
        // Note: TRIM is handled as a special expression in the parser (like POSITION)
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => string::char_length(args, name, character_unit),
        "OCTET_LENGTH" => string::octet_length(args),
        "CONCAT" => string::concat(args),
        "LENGTH" => string::length(args),
        "POSITION" => string::position(args),
        "REPLACE" => string::replace(args),
        "REVERSE" => string::reverse(args),
        "LEFT" => string::left(args),
        "RIGHT" => string::right(args),
        "INSTR" => string::instr(args),
        "LOCATE" => string::locate(args),

        // Numeric functions
        "ABS" => numeric::abs(args),
        "ROUND" => numeric::round(args),
        "TRUNCATE" => numeric::truncate(args),
        "FLOOR" => numeric::floor(args),
        "CEIL" | "CEILING" => numeric::ceil(args),
        "MOD" => numeric::mod_func(args),
        "POWER" | "POW" => numeric::power(args),
        "SQRT" => numeric::sqrt(args),
        "EXP" => numeric::exp(args),
        "LN" | "LOG" => numeric::ln(args),
        "LOG10" => numeric::log10(args),
        "SIGN" => numeric::sign(args),
        "PI" => numeric::pi(args),
        "SIN" => numeric::sin(args),
        "COS" => numeric::cos(args),
        "TAN" => numeric::tan(args),
        "ASIN" => numeric::asin(args),
        "ACOS" => numeric::acos(args),
        "ATAN" => numeric::atan(args),
        "ATAN2" => numeric::atan2(args),
        "RADIANS" => numeric::radians(args),
        "DEGREES" => numeric::degrees(args),
        "GREATEST" => numeric::greatest(args),
        "LEAST" => numeric::least(args),
        "FORMAT" => numeric::format(args),

        // Vector functions
        "COSINE_DISTANCE" => vector::cosine_distance(args),
        "L2_DISTANCE" => vector::l2_distance(args),
        "INNER_PRODUCT" => vector::inner_product(args),
        "VECTOR_NORM" => vector::vector_norm(args),
        "VECTOR_DIMS" => vector::vector_dims(args),

        // Date/time functions
        "CURRENT_DATE" | "CURDATE" => datetime::current_date(args),
        "CURRENT_TIME" | "CURTIME" => datetime::current_time(args),
        "CURRENT_TIMESTAMP" | "NOW" => datetime::current_timestamp(args),
        "DATETIME" => datetime::datetime(args),
        "YEAR" => datetime::year(args),
        "MONTH" => datetime::month(args),
        "DAY" => datetime::day(args),
        "HOUR" => datetime::hour(args),
        "MINUTE" => datetime::minute(args),
        "SECOND" => datetime::second(args),
        "DATEDIFF" => datetime::datediff(args),
        "DATE_ADD" | "ADDDATE" => datetime::date_add(args),
        "DATE_SUB" | "SUBDATE" => datetime::date_sub(args),
        "EXTRACT" => datetime::extract(args),
        "AGE" => datetime::age(args),

        // Control flow functions
        "IF" => control::if_func(args),

        // Type conversion functions
        "TO_NUMBER" => conversion::to_number(args),
        "TO_DATE" => conversion::to_date(args),
        "TO_TIMESTAMP" => conversion::to_timestamp(args),
        "TO_CHAR" => conversion::to_char(args),
        "CAST" => conversion::cast(args, sql_mode),

        // System information functions
        "VERSION" => system::version(args),
        "DATABASE" | "SCHEMA" => system::database(args, name),
        "USER" | "CURRENT_USER" => system::user(args, name),

        // Storage/Blob functions
        "STORAGE_URL" => storage::eval_storage_url(args),
        "STORAGE_SIZE" => storage::eval_storage_size(args),

        // Spatial/Geometric functions
        #[cfg(feature = "spatial")]
        // Constructor functions - WKT (Phase 1)
        "ST_GEOMFROMTEXT" | "ST_GEOM_FROM_TEXT" => spatial::constructors::st_geom_from_text(args),
        #[cfg(feature = "spatial")]
        "ST_POINTFROMTEXT" | "ST_POINT_FROM_TEXT" => {
            spatial::constructors::st_point_from_text(args)
        }
        #[cfg(feature = "spatial")]
        "ST_LINEFROMTEXT" | "ST_LINE_FROM_TEXT" => spatial::constructors::st_line_from_text(args),
        #[cfg(feature = "spatial")]
        "ST_POLYGONFROMTEXT" | "ST_POLYGON_FROM_TEXT" => {
            spatial::constructors::st_polygon_from_text(args)
        }

        #[cfg(feature = "spatial")]
        // Constructor functions - WKB (Phase 2)
        "ST_GEOMFROMWKB" | "ST_GEOM_FROM_WKB" => spatial::constructors::st_geom_from_wkb(args),
        #[cfg(feature = "spatial")]
        "ST_POINTFROMWKB" | "ST_POINT_FROM_WKB" => spatial::constructors::st_point_from_wkb(args),
        #[cfg(feature = "spatial")]
        "ST_LINEFROMWKB" | "ST_LINE_FROM_WKB" => spatial::constructors::st_line_from_wkb(args),
        #[cfg(feature = "spatial")]
        "ST_POLYGONFROMWKB" | "ST_POLYGON_FROM_WKB" => {
            spatial::constructors::st_polygon_from_wkb(args)
        }

        #[cfg(feature = "spatial")]
        // Accessor functions
        "ST_X" => spatial::accessors::st_x(args),
        #[cfg(feature = "spatial")]
        "ST_Y" => spatial::accessors::st_y(args),
        #[cfg(feature = "spatial")]
        "ST_GEOMETRYTYPE" | "ST_GEOMETRY_TYPE" => spatial::accessors::st_geometry_type(args),
        #[cfg(feature = "spatial")]
        "ST_DIMENSION" => spatial::accessors::st_dimension(args),
        #[cfg(feature = "spatial")]
        "ST_ASTEXT" | "ST_AS_TEXT" => spatial::accessors::st_as_text(args),
        #[cfg(feature = "spatial")]
        "ST_ASBINARY" | "ST_AS_BINARY" => spatial::accessors::st_as_binary(args),
        #[cfg(feature = "spatial")]
        "ST_ASGEOJSON" | "ST_AS_GEOJSON" => spatial::accessors::st_as_geojson(args),

        #[cfg(feature = "spatial")]
        // SRID functions (Phase 2)
        "ST_SETSRID" | "ST_SET_SRID" => spatial::srid::st_set_srid(args),
        #[cfg(feature = "spatial")]
        "ST_SRID" => spatial::srid::st_srid(args),

        #[cfg(feature = "spatial")]
        // Spatial predicates
        "ST_CONTAINS" => spatial::predicates::st_contains(args),
        #[cfg(feature = "spatial")]
        "ST_WITHIN" => spatial::predicates::st_within(args),
        #[cfg(feature = "spatial")]
        "ST_INTERSECTS" => spatial::predicates::st_intersects(args),
        #[cfg(feature = "spatial")]
        "ST_DISJOINT" => spatial::predicates::st_disjoint(args),
        #[cfg(feature = "spatial")]
        "ST_EQUALS" => spatial::predicates::st_equals(args),
        #[cfg(feature = "spatial")]
        "ST_TOUCHES" => spatial::predicates::st_touches(args),
        #[cfg(feature = "spatial")]
        "ST_CROSSES" => spatial::predicates::st_crosses(args),
        #[cfg(feature = "spatial")]
        "ST_OVERLAPS" => spatial::predicates::st_overlaps(args),
        #[cfg(feature = "spatial")]
        "ST_COVERS" => spatial::predicates::st_covers(args),
        #[cfg(feature = "spatial")]
        "ST_COVEREDBY" | "ST_COVERED_BY" => spatial::predicates::st_coveredby(args),
        #[cfg(feature = "spatial")]
        "ST_DWITHIN" | "ST_D_WITHIN" => spatial::predicates::st_dwithin(args),
        #[cfg(feature = "spatial")]
        "ST_RELATE" => spatial::predicates::st_relate(args),

        #[cfg(feature = "spatial")]
        // Spatial measurement functions
        "ST_DISTANCE" => spatial::measurements::st_distance(args),
        #[cfg(feature = "spatial")]
        "ST_LENGTH" => spatial::measurements::st_length(args),
        #[cfg(feature = "spatial")]
        "ST_PERIMETER" => spatial::measurements::st_perimeter(args),
        #[cfg(feature = "spatial")]
        "ST_AREA" => spatial::measurements::st_area(args),
        #[cfg(feature = "spatial")]
        "ST_CENTROID" => spatial::measurements::st_centroid(args),
        #[cfg(feature = "spatial")]
        "ST_ENVELOPE" => spatial::measurements::st_envelope(args),
        #[cfg(feature = "spatial")]
        "ST_CONVEXHULL" | "ST_CONVEX_HULL" => spatial::measurements::st_convex_hull(args),
        #[cfg(feature = "spatial")]
        "ST_POINTONSURFACE" | "ST_POINT_ON_SURFACE" => {
            spatial::measurements::st_point_on_surface(args)
        }
        #[cfg(feature = "spatial")]
        "ST_BOUNDARY" => spatial::measurements::st_boundary(args),
        #[cfg(feature = "spatial")]
        "ST_HAUSDORFFDISTANCE" | "ST_HAUSDORFF_DISTANCE" => {
            spatial::measurements::st_hausdorff_distance(args)
        }

        #[cfg(feature = "spatial")]
        // Spatial operation functions
        "ST_SIMPLIFY" => spatial::operations::st_simplify(args),
        #[cfg(feature = "spatial")]
        "ST_UNION" => spatial::operations::st_union(args),
        #[cfg(feature = "spatial")]
        "ST_INTERSECTION" => spatial::operations::st_intersection(args),
        #[cfg(feature = "spatial")]
        "ST_DIFFERENCE" => spatial::operations::st_difference(args),
        #[cfg(feature = "spatial")]
        "ST_SYMDIFFERENCE" | "ST_SYM_DIFFERENCE" => spatial::operations::st_sym_difference(args),

        // Unknown function
        _ => Err(ExecutorError::UnsupportedFeature(format!("Unknown function: {}", name))),
    }
}
