# Transactions

VibeSQL provides full transaction support with configurable durability guarantees.

## Basic Transaction Syntax

```sql
-- Begin a transaction
BEGIN;
-- or
BEGIN TRANSACTION;
-- or
START TRANSACTION;

-- Perform operations
INSERT INTO accounts (id, balance) VALUES (1, 1000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

-- Commit the transaction
COMMIT;

-- Or rollback to discard changes
ROLLBACK;
```

## Savepoints

Savepoints allow partial rollback within a transaction:

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (1, 500);

SAVEPOINT order_items;
INSERT INTO order_items (order_id, product_id) VALUES (1, 100);
INSERT INTO order_items (order_id, product_id) VALUES (1, 200);

-- Oops, wrong items - rollback to savepoint
ROLLBACK TO SAVEPOINT order_items;

-- Re-insert correct items
INSERT INTO order_items (order_id, product_id) VALUES (1, 150);

RELEASE SAVEPOINT order_items;
COMMIT;
```

## Transaction Durability Hints

VibeSQL supports per-transaction durability hints that control how changes are persisted to disk. This allows you to trade off between performance and data safety on a per-transaction basis.

### Syntax

```sql
BEGIN [TRANSACTION] WITH DURABILITY [=] <mode>
START TRANSACTION WITH DURABILITY [=] <mode>
```

The `=` sign is optional.

### Durability Modes

| Mode | WAL Write | Sync on Commit | Use Case |
|------|-----------|----------------|----------|
| `DEFAULT` | Database setting | Database setting | Use the database's default durability |
| `DURABLE` | Immediate | Yes (fsync) | Critical data that must survive crashes |
| `LAZY` | Batched | Periodic | Bulk imports, better performance |
| `VOLATILE` | Never | Never | Temporary computations, testing |

### Mode Details

#### DEFAULT

Uses the database's configured durability mode. This is the behavior when no durability hint is specified.

```sql
BEGIN WITH DURABILITY DEFAULT;
-- Same as: BEGIN;
```

#### DURABLE

Forces synchronous WAL writes with fsync on commit. Committed transactions are guaranteed to survive system crashes. Use this for critical operations like financial transactions.

```sql
-- Force durable commit for critical data
BEGIN WITH DURABILITY = DURABLE;
INSERT INTO accounts (id, balance) VALUES (1, 1000);
UPDATE ledger SET posted = TRUE WHERE account_id = 1;
COMMIT;  -- fsync ensures data is on disk
```

**Characteristics:**
- WAL written immediately on commit
- fsync called after each commit
- Committed transactions are crash-safe
- Slower than LAZY mode (~100μs per commit vs ~1μs)

#### LAZY

Allows batched WAL writes with periodic sync. Provides better performance at the cost of potentially losing up to ~100ms of recent transactions on crash.

```sql
-- Lazy transaction for bulk imports
START TRANSACTION WITH DURABILITY LAZY;
INSERT INTO logs SELECT * FROM staging_logs;
INSERT INTO metrics SELECT * FROM staging_metrics;
COMMIT;  -- May not immediately sync to disk
```

**Characteristics:**
- WAL entries batched for efficiency
- Sync every 50-100ms or after N entries
- Up to ~100ms of data could be lost on crash
- Good balance of speed and safety for most workloads

#### VOLATILE

Skips WAL writes entirely for this transaction. Data is only kept in memory and will be lost on shutdown or crash. Use for temporary computations that don't need persistence.

```sql
-- Volatile transaction for temporary computations
BEGIN TRANSACTION WITH DURABILITY = VOLATILE;
CREATE TEMP TABLE results AS
    SELECT user_id, SUM(amount) as total
    FROM orders
    GROUP BY user_id;
-- Use results for further queries...
COMMIT;  -- No WAL written
```

**Characteristics:**
- Maximum performance
- No WAL writes
- All data lost on shutdown/crash
- Ideal for temp tables and ephemeral computations

### Performance Comparison

| Mode | Single INSERT | Bulk 1M rows | Data Loss Window |
|------|---------------|--------------|------------------|
| VOLATILE | ~1μs | ~500ms | All uncommitted |
| LAZY | ~1μs | ~600ms | ~50-100ms |
| DURABLE | ~100μs | ~1s | None (committed) |

### Examples

#### Critical Financial Transaction

```sql
-- Ensure this transaction survives any crash
BEGIN WITH DURABILITY = DURABLE;
UPDATE accounts SET balance = balance - 500.00 WHERE id = 123;
UPDATE accounts SET balance = balance + 500.00 WHERE id = 456;
INSERT INTO transfers (from_id, to_id, amount, timestamp)
    VALUES (123, 456, 500.00, CURRENT_TIMESTAMP);
COMMIT;
```

#### Bulk Data Import

```sql
-- Prioritize speed for bulk import
START TRANSACTION WITH DURABILITY LAZY;
INSERT INTO events SELECT * FROM staging_events;
INSERT INTO metrics SELECT * FROM staging_metrics;
DELETE FROM staging_events;
DELETE FROM staging_metrics;
COMMIT;
```

#### Temporary Analytics

```sql
-- No persistence needed for temporary calculations
BEGIN WITH DURABILITY = VOLATILE;
CREATE TEMP TABLE user_stats AS
    SELECT
        user_id,
        COUNT(*) as order_count,
        SUM(total) as lifetime_value
    FROM orders
    GROUP BY user_id;

SELECT * FROM user_stats WHERE lifetime_value > 10000;
COMMIT;
```

#### Mixed Workload

```sql
-- Most operations use default durability
BEGIN;
INSERT INTO logs (message) VALUES ('Processing started');
COMMIT;

-- Critical operation forces durable commit
BEGIN WITH DURABILITY = DURABLE;
UPDATE user_preferences SET verified = TRUE WHERE user_id = 123;
COMMIT;

-- Bulk operation uses lazy mode
START TRANSACTION WITH DURABILITY = LAZY;
INSERT INTO analytics SELECT * FROM raw_events WHERE processed = FALSE;
UPDATE raw_events SET processed = TRUE WHERE processed = FALSE;
COMMIT;
```

## Transaction Isolation

VibeSQL uses SERIALIZABLE isolation level, providing the strongest isolation guarantees.

```sql
-- Set transaction characteristics (SQL:1999 E152)
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET TRANSACTION READ ONLY;
SET TRANSACTION READ WRITE;
SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

## See Also

- [CLI Guide](CLI_GUIDE.md) - Transaction commands in the CLI
- [HTTP API](http-api.md) - Transaction support in REST API
