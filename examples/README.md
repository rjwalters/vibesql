# Examples

Runnable examples for the root `vibesql` crate. The `.rs` files are
auto-discovered by Cargo as example targets: run them with
`cargo run --example <name>`.

| Example | Run with | What it demonstrates |
|---------|----------|----------------------|
| `query_runner.rs` | `cargo run --example query_runner` | Executes SQL queries and formats output as expected results for web demo examples |
| `batch_query_runner.rs` | `cargo run --example batch_query_runner` | Runs all 27 advanced web-demo example queries, generating expected results or SKIP comments |
| `batch_results_generator.rs` | `cargo run --example batch_results_generator [--filter category]` | Processes all examples in the web demo's `examples.ts` and generates output to paste back in |
| `test_spatial_types.rs` | `cargo run --example test_spatial_types` | Quick integration check of MySQL spatial data types (issue #818) |
| `scheduled-functions.sql` | Paste into the `vibesql` CLI | SQL examples of scheduled execution (`SCHEDULE AFTER` / `SCHEDULE AT` / recurring jobs) |
