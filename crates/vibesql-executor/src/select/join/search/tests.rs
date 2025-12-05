//! Tests for join order optimization

use std::collections::BTreeSet;

use super::optimizer::JoinOrderSearch;
use super::state::JoinCost;
use crate::select::join::reorder::{JoinEdge, JoinOrderAnalyzer};

#[test]
fn test_single_table_order() {
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string()]);

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    assert_eq!(order.len(), 1);
    assert_eq!(order[0], "t1");
}

#[test]
fn test_two_table_order() {
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string()]);

    // Add join edge t1 - t2
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    assert_eq!(order.len(), 2);
    // Both orderings are valid, just verify both tables are present
    assert!(order.contains(&"t1".to_string()));
    assert!(order.contains(&"t2".to_string()));
}

#[test]
fn test_three_table_chain() {
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

    // Create chain: t1 - t2 - t3
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    assert_eq!(order.len(), 3);
    assert!(order.contains(&"t1".to_string()));
    assert!(order.contains(&"t2".to_string()));
    assert!(order.contains(&"t3".to_string()));
}

#[test]
fn test_cost_comparison() {
    // Verify that cost model prefers chains with join edges
    let cost_with_edge = JoinCost::new(100, 1000);
    let cost_without_edge = JoinCost::new(500, 1000);

    assert!(cost_with_edge.total() < cost_without_edge.total());
}

#[test]
fn test_search_prunes_bad_paths() {
    // Create scenario where different orderings have different costs
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

    // t1 - t2 - t3 chain
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    // Verify we get a valid ordering (all tables present)
    assert_eq!(order.len(), 3);
}

#[test]
fn test_disconnected_tables() {
    // Tables with no join edges
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

    // No edges - will use cross product
    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    // Still should return all tables in some order
    assert_eq!(order.len(), 3);
}

#[test]
fn test_star_join_pattern() {
    // Test star join: t1 is central hub, t2/t3/t4 all join to t1
    // This pattern exposed the bug where we only looked for conditions
    // between consecutive tables in the reordered sequence
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "t1".to_string(),
        "t2".to_string(),
        "t3".to_string(),
        "t4".to_string(),
    ]);

    // Star pattern: all join to t1 (but not to each other)
    //     t2
    //      |
    //  t3--t1--t4
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    // Should return all 4 tables
    assert_eq!(order.len(), 4);
    assert!(order.contains(&"t1".to_string()));
    assert!(order.contains(&"t2".to_string()));
    assert!(order.contains(&"t3".to_string()));
    assert!(order.contains(&"t4".to_string()));

    // t1 should be early in the order since it's the hub that connects everything
    // (though exact position depends on cost estimates)
    let t1_pos = order.iter().position(|t| t == "t1").unwrap();

    // t1 should be in the first 2 positions (either first or second)
    // because it's the only table that can join to all others
    assert!(
        t1_pos <= 1,
        "Hub table t1 should be early in join order, found at position {}",
        t1_pos
    );
}

#[test]
fn test_parallel_bfs_selection() {
    // Test that should_use_parallel_search selects correctly
    let mut analyzer = JoinOrderAnalyzer::new();

    // Test 1: Small query (2 tables) should use DFS
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string()]);
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    assert!(!search.should_use_parallel_search(), "Small queries should use DFS");

    // Test 2: Highly connected 5-table query should use parallel BFS
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "t1".to_string(),
        "t2".to_string(),
        "t3".to_string(),
        "t4".to_string(),
        "t5".to_string(),
    ]);

    // Create a highly connected graph (star + additional edges)
    // 8 edges / 5 tables = 1.6 edge density > 1.5 threshold
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t5".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t3".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t4".to_string(),
        left_column: "id".to_string(),
        right_table: "t5".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t5".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    assert!(
        search.should_use_parallel_search(),
        "Highly connected 5-table query should use parallel BFS"
    );

    // Verify it produces valid ordering
    let order = search.find_optimal_order();
    assert_eq!(order.len(), 5);
}

#[test]
fn test_parallel_bfs_produces_valid_ordering() {
    // Test that parallel BFS produces a valid ordering
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "t1".to_string(),
        "t2".to_string(),
        "t3".to_string(),
        "t4".to_string(),
    ]);

    // Star pattern with 3 edges (3/4 = 0.75, below threshold)
    // Force parallel BFS by adding more edges
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t3".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);

    // Verify parallel BFS is selected
    assert!(search.should_use_parallel_search());

    // Test both DFS and parallel BFS produce valid orderings
    let order_parallel = search.context.find_optimal_order_parallel();
    let order_dfs = search.context.find_optimal_order_dfs();

    // Both should contain all tables
    assert_eq!(order_parallel.len(), 4);
    assert_eq!(order_dfs.len(), 4);

    // Both should be valid orderings
    for table in &["t1", "t2", "t3", "t4"] {
        assert!(order_parallel.contains(&table.to_string()));
        assert!(order_dfs.contains(&table.to_string()));
    }
}

#[test]
fn test_tpch_q3_star_schema_no_cross_join() {
    // Regression test for issue #2286: Join reordering optimizer chooses invalid
    // orders for star-schema queries, causing CROSS JOIN memory limits
    //
    // TPC-H Q3 has a star join pattern:
    //   customer ←→ orders ←→ lineitem
    //
    // Orders is the hub table. Customer and lineitem have NO direct join.
    // The optimizer should never choose an order like [customer, lineitem, orders]
    // which would require a CROSS JOIN between customer and lineitem.
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "customer".to_string(),
        "orders".to_string(),
        "lineitem".to_string(),
    ]);

    // Star pattern: orders is the hub
    analyzer.add_edge(JoinEdge {
        left_table: "customer".to_string(),
        left_column: "c_custkey".to_string(),
        right_table: "orders".to_string(),
        right_column: "o_custkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "lineitem".to_string(),
        left_column: "l_orderkey".to_string(),
        right_table: "orders".to_string(),
        right_column: "o_orderkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    // Verify we got all 3 tables
    assert_eq!(order.len(), 3);
    assert!(order.contains(&"customer".to_string()));
    assert!(order.contains(&"orders".to_string()));
    assert!(order.contains(&"lineitem".to_string()));

    // Validate: each table after the first must have a join edge to at least
    // one previously-joined table (no CROSS JOINs)
    for i in 1..order.len() {
        let current_table = &order[i];
        let previous_tables: BTreeSet<String> = order[0..i].iter().cloned().collect();

        let has_connection = search.context.has_join_edge(&previous_tables, current_table);

        assert!(
            has_connection,
            "Table {} at position {} has no join condition with previous tables {:?}. \
             This would cause a CROSS JOIN and memory limit exceeded. \
             Full order: {:?}",
            current_table, i, previous_tables, order
        );
    }

    // Note: customer and lineitem CAN be adjacent in orders like [orders, lineitem, customer]
    // because both connect to orders. The key is that each table after the first has
    // a connection to at least one previously-joined table, which we verified above.
}

#[test]
fn test_join_order_determinism() {
    // Test that join order search produces deterministic results across multiple runs
    // This is important for reproducibility and testing
    //
    // After switching from HashSet to BTreeSet, iteration order should be deterministic
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "customer".to_string(),
        "orders".to_string(),
        "lineitem".to_string(),
        "supplier".to_string(),
        "nation".to_string(),
    ]);

    // Create a connected join graph (TPC-H Q5-style)
    analyzer.add_edge(JoinEdge {
        left_table: "customer".to_string(),
        left_column: "c_custkey".to_string(),
        right_table: "orders".to_string(),
        right_column: "o_custkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "orders".to_string(),
        left_column: "o_orderkey".to_string(),
        right_table: "lineitem".to_string(),
        right_column: "l_orderkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "lineitem".to_string(),
        left_column: "l_suppkey".to_string(),
        right_table: "supplier".to_string(),
        right_column: "s_suppkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "supplier".to_string(),
        left_column: "s_nationkey".to_string(),
        right_table: "nation".to_string(),
        right_column: "n_nationkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "customer".to_string(),
        left_column: "c_nationkey".to_string(),
        right_table: "nation".to_string(),
        right_column: "n_nationkey".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);

    // Run search 10 times
    let mut orders = Vec::new();
    for _ in 0..10 {
        let order = search.find_optimal_order();
        orders.push(order);
    }

    // All orders should be identical
    let first_order = &orders[0];
    for (i, order) in orders.iter().enumerate().skip(1) {
        assert_eq!(
            order, first_order,
            "Join order iteration {} differed from first: {:?} != {:?}",
            i, order, first_order
        );
    }

    // Verify we got all 5 tables
    assert_eq!(first_order.len(), 5);
}

#[test]
fn test_time_bounded_search_returns_valid_ordering() {
    // Test that time-bounded search always returns a valid ordering
    // even with a very short time budget
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "t1".to_string(),
        "t2".to_string(),
        "t3".to_string(),
        "t4".to_string(),
        "t5".to_string(),
    ]);

    // Create a connected graph
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t3".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t4".to_string(),
        left_column: "id".to_string(),
        right_table: "t5".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let mut search = JoinOrderSearch::from_analyzer(&analyzer, &db);

    // Set a very short time budget to test early termination
    search.context.config.time_budget_ms = 1; // 1ms - very short
    search.context.config.use_time_budget = true;

    let order = search.find_optimal_order();

    // Should still return all tables
    assert_eq!(order.len(), 5);

    // All tables should be present
    assert!(order.contains(&"t1".to_string()));
    assert!(order.contains(&"t2".to_string()));
    assert!(order.contains(&"t3".to_string()));
    assert!(order.contains(&"t4".to_string()));
    assert!(order.contains(&"t5".to_string()));
}

#[test]
fn test_time_bounded_search_completes_fast_for_small_queries() {
    // Test that small queries complete within time budget
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);

    let start = std::time::Instant::now();
    let order = search.find_optimal_order();
    let elapsed = start.elapsed();

    // Should complete very quickly for 3 tables
    assert!(elapsed.as_millis() < 100, "Small query took too long: {:?}", elapsed);
    assert_eq!(order.len(), 3);
}

#[test]
fn test_time_bounded_search_with_generous_budget() {
    // Test that search completes exhaustively with generous time budget
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec![
        "t1".to_string(),
        "t2".to_string(),
        "t3".to_string(),
        "t4".to_string(),
    ]);

    // Star pattern
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t4".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let mut search = JoinOrderSearch::from_analyzer(&analyzer, &db);

    // Set generous time budget
    search.context.config.time_budget_ms = 10000; // 10 seconds
    search.context.config.use_time_budget = true;

    let order = search.find_optimal_order();

    // Should return all tables
    assert_eq!(order.len(), 4);

    // t1 should be early in the order (hub table)
    let t1_pos = order.iter().position(|t| t == "t1").unwrap();
    assert!(t1_pos <= 1, "Hub table should be early in order");
}

#[test]
fn test_legacy_behavior_with_time_budget_disabled() {
    // Test that disabling time budget uses legacy table-count based logic
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

    analyzer.add_edge(JoinEdge {
        left_table: "t1".to_string(),
        left_column: "id".to_string(),
        right_table: "t2".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });
    analyzer.add_edge(JoinEdge {
        left_table: "t2".to_string(),
        left_column: "id".to_string(),
        right_table: "t3".to_string(),
        right_column: "id".to_string(),
        join_type: vibesql_ast::JoinType::Inner,
    });

    let db = vibesql_storage::Database::new();
    let mut search = JoinOrderSearch::from_analyzer(&analyzer, &db);

    // Disable time-bounded search
    search.context.config.use_time_budget = false;

    let order = search.find_optimal_order();

    // Should still return valid ordering
    assert_eq!(order.len(), 3);
    assert!(order.contains(&"t1".to_string()));
    assert!(order.contains(&"t2".to_string()));
    assert!(order.contains(&"t3".to_string()));
}
