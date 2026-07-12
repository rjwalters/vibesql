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
pub(crate) mod coercion;
mod control;
mod conversion;
pub(crate) mod datetime;
mod null_handling;
mod numeric;
#[cfg(feature = "spatial")]
pub(crate) mod spatial;
pub(crate) mod sqlite_compat;
mod storage;
pub(crate) mod string;
mod system;
mod vector;

/// Evaluate a scalar function on given argument values
///
/// This handles SQL scalar functions that don't depend on table schemas
/// (unlike aggregates like COUNT, SUM which are handled elsewhere).
///
/// `schema_context` identifies whether the expression being evaluated is a
/// schema-attached expression (CHECK constraint, generated column, or index
/// expression). The SQLite date/time functions consult it to reject
/// non-deterministic uses ('now', zero-argument defaults, 'localtime'/'utc')
/// at evaluation time.
pub(super) fn eval_scalar_function(
    name: &str,
    args: &[vibesql_types::SqlValue],
    character_unit: &Option<vibesql_ast::CharacterUnit>,
    sql_mode: &vibesql_types::SqlMode,
    schema_context: super::SchemaExprContext,
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
        "TRIM" => string::trim(args),
        "LTRIM" => string::ltrim(args),
        "RTRIM" => string::rtrim(args),
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => string::char_length(args, name, character_unit),
        "OCTET_LENGTH" => string::octet_length(args),
        "CONCAT" => string::concat(args, sql_mode),
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
        "TRUNCATE" | "TRUNC" => numeric::truncate(args),
        "FLOOR" => numeric::floor(args),
        "CEIL" | "CEILING" => numeric::ceil(args),
        "MOD" => numeric::mod_func(args),
        "POWER" | "POW" => numeric::power(args),
        "SQRT" => numeric::sqrt(args),
        "EXP" => numeric::exp(args),
        "LN" => numeric::ln(args),
        "LOG" => numeric::log(args),
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
        // SQLite-compatible scalar MIN/MAX (multi-argument form)
        // These return NULL if any argument is NULL, unlike LEAST/GREATEST
        "MIN" => numeric::scalar_min(args),
        "MAX" => numeric::scalar_max(args),
        // FORMAT is overloaded: SQLite's format() is an alias for printf()
        // (format-string first argument), while MySQL's FORMAT(number,
        // decimals) formats a number with thousand separators. Route by the
        // type of the first argument: strings (and NULL, which printf
        // propagates) go to printf, numerics keep the MySQL behavior.
        "FORMAT" => match args.first() {
            Some(
                vibesql_types::SqlValue::Varchar(_)
                | vibesql_types::SqlValue::Character(_)
                | vibesql_types::SqlValue::Null,
            ) => sqlite_compat::printf(args),
            _ => numeric::format(args),
        },

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
        "DATETIME" => datetime::datetime(args, schema_context),
        "DATE" => datetime::date(args, schema_context),
        "TIME" => datetime::time(args, schema_context),
        "STRFTIME" => datetime::strftime(args, schema_context),
        "JULIANDAY" => datetime::julianday(args, schema_context),
        "UNIXEPOCH" => datetime::unixepoch(args, schema_context),
        "TIMEDIFF" => datetime::timediff(args, schema_context),
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
        "IIF" => sqlite_compat::iif(args),

        // SQLite compatibility functions
        "TYPEOF" => sqlite_compat::typeof_func(args),
        "LIKELY" => sqlite_compat::likely(args),
        "UNLIKELY" => sqlite_compat::unlikely(args),
        "LIKELIHOOD" => sqlite_compat::likelihood(args),
        "IFNULL" => sqlite_compat::ifnull(args),
        "RANDOM" => sqlite_compat::random(args),
        "RANDOMBLOB" => sqlite_compat::randomblob(args),
        "HEX" => sqlite_compat::hex(args),
        // MD5SUM is now an aggregate function, not a scalar function
        "UNHEX" => sqlite_compat::unhex(args),
        "ZEROBLOB" => sqlite_compat::zeroblob(args),
        "UNICODE" => sqlite_compat::unicode(args),
        "CHAR" => sqlite_compat::char_func(args),
        "PRINTF" => sqlite_compat::printf(args),
        "UNISTR" => sqlite_compat::unistr(args),
        "TOREAL" => sqlite_compat::toreal(args),
        "TOINTEGER" => sqlite_compat::tointeger(args),
        "CONCAT_WS" => sqlite_compat::concat_ws(args),
        "QUOTE" => sqlite_compat::quote(args),
        "INTREAL" => sqlite_compat::intreal(args),
        "GLOB" => sqlite_compat::glob(args),
        "LIKE" => sqlite_compat::like(args),

        // JSON functions (SQLite JSON1 extension)
        //
        // The `json_*` names produce JSON *text*; the `jsonb`/`jsonb_*` names
        // produce SQLite's binary JSONB representation as a `SqlValue::Blob`
        // (Stage 1 of #6008). The text-mode consumers accept a JSONB blob
        // argument (decoding it back to a JSON node) so nested `jsonb(...)`
        // arguments keep working.
        "JSON" => sqlite_compat::json(args),
        "JSONB" => sqlite_compat::jsonb(args),
        "JSON_VALID" => sqlite_compat::json_valid(args),
        "JSON_EXTRACT" => sqlite_compat::json_extract(args),
        "JSONB_EXTRACT" => sqlite_compat::jsonb_extract(args),
        "JSON_TYPE" => sqlite_compat::json_type(args),
        "JSON_QUOTE" => sqlite_compat::json_quote(args),
        "JSON_ARRAY_LENGTH" => sqlite_compat::json_array_length(args),
        "JSON_REMOVE" => sqlite_compat::json_remove(args),
        "JSONB_REMOVE" => sqlite_compat::jsonb_remove(args),
        "JSON_PATCH" => sqlite_compat::json_patch(args),
        "JSONB_PATCH" => sqlite_compat::jsonb_patch(args),
        "JSON_ERROR_POSITION" => sqlite_compat::json_error_position(args),
        // The subtype-aware construction/mutation functions (JSON_ARRAY,
        // JSON_OBJECT, JSON_INSERT, JSON_REPLACE, JSON_SET and their JSONB
        // variants) are dispatched earlier in special.rs / arena.rs, where
        // per-argument JSON-subtype flags are available from the AST. Reaching
        // them here (no subtype info) still produces correct output for the
        // common no-embedding case; the JSONB variants emit a Blob.
        "JSON_ARRAY" => sqlite_compat::json_array(args, &[]),
        "JSONB_ARRAY" => sqlite_compat::json_funcs::jsonb_array(args, &[]),
        "JSON_OBJECT" => sqlite_compat::json_object(args, &[]),
        "JSONB_OBJECT" => sqlite_compat::json_funcs::jsonb_object(args, &[]),
        "JSON_INSERT" => sqlite_compat::json_insert(args, &[]),
        "JSONB_INSERT" => sqlite_compat::json_funcs::jsonb_insert(args, &[]),
        "JSON_REPLACE" => sqlite_compat::json_replace(args, &[]),
        "JSONB_REPLACE" => sqlite_compat::json_funcs::jsonb_replace(args, &[]),
        "JSON_SET" => sqlite_compat::json_set(args, &[]),
        "JSONB_SET" => sqlite_compat::json_funcs::jsonb_set(args, &[]),

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

        // Unknown function - use SQLite-compatible error format
        _ => Err(ExecutorError::NoSuchFunction { function_name: name.to_string() }),
    }
}

/// Evaluate a scalar function for a DEFAULT expression at INSERT time.
///
/// DEFAULT expressions are materialized without a row/schema/database context,
/// so only deterministic scalar functions whose arguments reduce to constants
/// (e.g. `abs(1)`) are supported. Aggregate functions never reach this path:
/// they parse as `Expression::AggregateFunction` and are rejected by the
/// DEFAULT evaluator with an `unknown function` error (table-16.2 ..
/// table-16.7), matching SQLite.
///
/// `schema_context` is `None` here because DEFAULT materialization is not a
/// schema-attached context (CHECK/generated/index); the non-deterministic
/// date/time guards do not apply.
pub(crate) fn eval_scalar_function_for_default(
    name: &str,
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    eval_scalar_function(
        name,
        args,
        &None,
        &vibesql_types::SqlMode::default(),
        super::SchemaExprContext::None,
    )
}

/// Pins `eval_scalar_function`'s dispatch table against the central
/// volatility classification in [`vibesql_ast::volatility`].
///
/// **If you add a function (or an alias) to the dispatch above whose
/// implementation reads the wall clock, the RNG, or session/server
/// state, you MUST classify its name in `vibesql_ast::volatility` as
/// well** — the Raft replication freeze pass (`vibesql-consensus`) and
/// the optimizer's predicate-pushdown guard both rely on that list, and
/// an unclassified alias silently diverges replicas (see PR #5382:
/// `curdate()`/`curtime()`/`age()` slipped through exactly this gap).
/// This test parses the dispatch source and fails when a name routed to
/// a volatile handler is missing from the classification.
#[cfg(test)]
mod dispatch_volatility {
    use vibesql_ast::volatility;

    /// The dispatch source, scanned for `"NAME" | "ALIAS" => handler`
    /// arms. Patterns and `=>` share a line in this file; a handler may
    /// continue on following lines (until the next arm's `=>`).
    const DISPATCH_SRC: &str = include_str!("mod.rs");

    /// Handler call tokens whose implementations are non-deterministic,
    /// paired with the volatility category their dispatch names must be
    /// classified under. Keep in sync with the implementations:
    /// - clock: read the wall clock unconditionally
    /// - datetime-family: read the clock for some argument shapes ('now', implicit now, 1-arg
    ///   age())
    /// - random: RNG
    /// - session: session/server state (identity, server info)
    const VOLATILE_HANDLERS: &[(&str, Category)] = &[
        ("datetime::current_date(", Category::Clock),
        ("datetime::current_time(", Category::Clock),
        ("datetime::current_timestamp(", Category::Clock),
        ("datetime::datetime(", Category::DatetimeFamily),
        ("datetime::date(", Category::DatetimeFamily),
        ("datetime::time(", Category::DatetimeFamily),
        ("datetime::strftime(", Category::DatetimeFamily),
        ("datetime::julianday(", Category::DatetimeFamily),
        ("datetime::unixepoch(", Category::DatetimeFamily),
        ("datetime::timediff(", Category::DatetimeFamily),
        ("datetime::age(", Category::DatetimeFamily),
        ("sqlite_compat::random(", Category::Random),
        ("sqlite_compat::randomblob(", Category::Random),
        ("system::version(", Category::Session),
        ("system::database(", Category::Session),
        ("system::user(", Category::Session),
    ];

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Category {
        Clock,
        DatetimeFamily,
        Random,
        Session,
    }

    impl Category {
        fn classifies(self, canonical_name: &str) -> bool {
            match self {
                Category::Clock => volatility::is_clock_function(canonical_name),
                Category::DatetimeFamily => volatility::is_datetime_family_function(canonical_name),
                Category::Random => volatility::is_random_function(canonical_name),
                Category::Session => volatility::is_session_state_function(canonical_name),
            }
        }
    }

    /// Extract `(pattern names, handler text)` pairs from the dispatch
    /// source: a line containing `=>` starts an arm whose pattern names
    /// are the quoted strings before the `=>`; its handler text runs to
    /// the next such line. The scan stops at this test module's own
    /// `#[cfg(test)]` attribute so the marker table above is not read
    /// as dispatch arms.
    fn dispatch_arms() -> Vec<(Vec<String>, String)> {
        let dispatch_only =
            DISPATCH_SRC.split("#[cfg(test)]").next().expect("split always yields a first part");
        let mut arms: Vec<(Vec<String>, String)> = Vec::new();
        for line in dispatch_only.lines() {
            if let Some(arrow) = line.find("=>") {
                let names = quoted_strings(&line[..arrow]);
                let handler = line[arrow + 2..].to_string();
                arms.push((names, handler));
            } else if let Some((_, handler)) = arms.last_mut() {
                handler.push('\n');
                handler.push_str(line);
            }
        }
        arms
    }

    fn quoted_strings(text: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut rest = text;
        while let Some(start) = rest.find('"') {
            let Some(len) = rest[start + 1..].find('"') else { break };
            names.push(rest[start + 1..start + 1 + len].to_string());
            rest = &rest[start + 1 + len + 1..];
        }
        names
    }

    #[test]
    fn every_volatile_dispatch_name_is_classified_in_vibesql_ast_volatility() {
        let mut seen: Vec<(Category, String)> = Vec::new();
        for (names, handler) in dispatch_arms() {
            for &(marker, category) in VOLATILE_HANDLERS {
                if !handler.contains(marker) {
                    continue;
                }
                assert!(
                    !names.is_empty(),
                    "volatile handler {marker} dispatched from an arm without name patterns"
                );
                for name in &names {
                    let canonical = name.to_lowercase();
                    assert!(
                        category.classifies(&canonical),
                        "executor dispatches '{name}' to volatile handler {marker}, but \
                         vibesql_ast::volatility does not classify '{canonical}' as \
                         {category:?} — add it to the classification (replication freeze \
                         and optimizer pushdown depend on it, #5377/#5382)"
                    );
                    assert!(
                        volatility::is_volatile_function(&canonical),
                        "'{canonical}' must be in the coarse volatile union"
                    );
                    seen.push((category, canonical));
                }
            }
        }

        // Sanity check that the source scan actually found the dispatch
        // arms (guards against refactors that would silently turn this
        // test into a no-op). These are the alias sets confirmed
        // divergence-prone in PR #5382's review.
        for (category, name) in [
            (Category::Clock, "current_date"),
            (Category::Clock, "curdate"),
            (Category::Clock, "current_time"),
            (Category::Clock, "curtime"),
            (Category::Clock, "current_timestamp"),
            (Category::Clock, "now"),
            (Category::DatetimeFamily, "age"),
            (Category::Random, "random"),
            (Category::Random, "randomblob"),
            (Category::Session, "user"),
            (Category::Session, "current_user"),
            (Category::Session, "database"),
            (Category::Session, "schema"),
            (Category::Session, "version"),
        ] {
            assert!(
                seen.contains(&(category, name.to_string())),
                "expected to find '{name}' dispatched to a {category:?} handler — did the \
                 dispatch-source scan break?"
            );
        }
    }
}
