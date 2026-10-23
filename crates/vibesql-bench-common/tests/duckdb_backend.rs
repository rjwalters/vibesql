#![cfg(feature = "duckdb")]

#[test]
fn bundled_duckdb_loads_and_queries_tpch_data() {
    let connection = vibesql_bench_common::tpch::schema::load_duckdb(0.001);
    let nations: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM nation JOIN region ON n_regionkey = r_regionkey",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(nations, 25);

    let (rows, revenue): (i64, f64) = connection
        .query_row(
            "SELECT COUNT(*), SUM(l_extendedprice * (1 - l_discount)) FROM lineitem",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(rows > 0);
    assert!(revenue.is_finite() && revenue > 0.0);
}
