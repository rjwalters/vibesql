# VibeSQL Server Metrics

VibeSQL exposes Prometheus-compatible metrics via OpenTelemetry for monitoring server health, query performance, and subscription efficiency.

## Metrics Reference

### Connection Metrics

| Metric | Type | Unit | Labels | Description |
|--------|------|------|--------|-------------|
| `vibesql_server_connections_total` | Counter | connection | - | Total connections accepted |
| `vibesql_server_connection_errors_total` | Counter | error | `error_type` | Connection failures by error type |
| `vibesql_server_connection_duration_seconds` | Histogram | seconds | - | Connection lifetime distribution |

### Query Metrics

| Metric | Type | Unit | Labels | Description |
|--------|------|------|--------|-------------|
| `vibesql_server_queries_total` | Counter | query | `statement_type`, `success` | Queries executed by statement type |
| `vibesql_server_query_duration_seconds` | Histogram | seconds | `statement_type`, `success` | Query execution latency |
| `vibesql_server_query_errors_total` | Counter | error | `error_type`, `statement_type` | Query errors by error type |
| `vibesql_server_query_rows_affected` | Histogram | row | `statement_type`, `success` | Rows affected distribution |

### Protocol Metrics

| Metric | Type | Unit | Labels | Description |
|--------|------|------|--------|-------------|
| `vibesql_server_messages_received_total` | Counter | message | `message_type` | PostgreSQL protocol messages received |
| `vibesql_server_messages_sent_total` | Counter | message | `message_type` | PostgreSQL protocol messages sent |
| `vibesql_server_bytes_received_total` | Counter | bytes | - | Total bytes received |
| `vibesql_server_bytes_sent_total` | Counter | bytes | - | Total bytes sent |

### Subscription Metrics

| Metric | Type | Unit | Labels | Description |
|--------|------|------|--------|-------------|
| `vibesql_subscription_updates_total` | Counter | update | `type`, `row_count` | Subscription updates sent by type |
| `vibesql_selective_update_columns_sent` | Histogram | column | - | Number of columns sent in selective updates |
| `vibesql_selective_update_changed_ratio` | Histogram | ratio (0-1) | - | Ratio of changed columns to total columns |
| `vibesql_subscriptions_selective_eligible` | Gauge | subscription | - | Active subscriptions eligible for selective updates |

**Subscription Update Types** (`type` label values):
- `full` - Complete result set refresh
- `delta_insert` - New rows added
- `delta_update` - Existing rows modified
- `delta_delete` - Rows removed
- `selective` - Partial column update (only changed columns sent)

### Partial Update Efficiency Metrics

These metrics track the effectiveness of partial/selective updates, which reduce bandwidth by only sending changed columns.

| Metric | Type | Unit | Labels | Description |
|--------|------|------|--------|-------------|
| `vibesql_partial_update_fallbacks_total` | Counter | fallback | `reason` | Times partial update was skipped |
| `vibesql_partial_update_bytes_saved` | Histogram | bytes | - | Estimated bytes saved per partial update |
| `vibesql_partial_update_efficiency` | Gauge | ratio (0-1) | - | Rolling average of column efficiency |

**Fallback Reasons** (`reason` label values):
- `disabled` - Selective updates disabled in configuration
- `threshold_exceeded` - Too many columns changed (exceeded threshold)
- `row_count_mismatch` - Old and new row counts differ
- `pk_mismatch` - Cannot find matching old row by primary key
- `no_changes` - No column changes detected

### Selective Update Eligibility Breakdown Metrics

These metrics provide visibility into selective update eligibility, helping operators understand what percentage of subscriptions can benefit from 0xF7 optimization.

| Metric | Type | Unit | Labels | Description |
|--------|------|------|--------|-------------|
| `vibesql_subscription_pk_detection_total` | Counter | detection | `result`, `reason` | Primary key detection outcomes during subscription registration |
| `vibesql_subscription_selective_update_decisions_total` | Counter | decision | `decision`, `reason` | Selective update decisions (sent_partial, sent_full, skipped) |
| `vibesql_subscription_selective_update_column_ratio` | Histogram | ratio (0-1) | - | Ratio of changed columns to total columns when evaluating selective updates |

**PK Detection Results** (`result` label values):
- `confident` - Successfully detected PK columns from single-table query
- `not_confident` - Could not confidently detect PK columns

**PK Detection Reasons** (`reason` label values for `not_confident`):
- `no_table` - No table found in query (e.g., VALUES clause)
- `join_query` - Multi-table join query (PK detection is complex)
- `pk_not_in_result` - PK columns not included in SELECT list
- `unknown` - Other detection issues

**Selective Update Decisions** (`decision` label values):
- `sent_partial` - Successfully sent partial update (0xF7)
- `sent_full` - Fell back to full row update
- `skipped` - Update was skipped (e.g., no changes)

**Decision Reasons** (`reason` label values):
- `disabled` - Selective updates disabled in configuration
- `threshold_exceeded` - Too many columns changed (exceeded max_changed_columns_ratio)
- `row_count_mismatch` - Old and new row counts differ (inserts/deletes mixed with updates)
- `pk_mismatch` - Cannot find matching old row by primary key
- `no_changes` - No column changes detected

### Debug Logging for PK Detection

When running with `RUST_LOG=debug` or `RUST_LOG=vibesql_server=debug`, additional debug messages are logged to help diagnose why subscriptions fall back to full updates instead of selective updates.

**PK Detection Logs** (at subscription creation):
```
PK detection confident for subscription: pk_columns=[0], tables={"users"}
PK detection not confident for subscription: reason=<reason>, pk_columns=[0], tables={"users"}, query=SELECT ...
```

**PK Detection Failure Reasons**:
| Reason | Description |
|--------|-------------|
| `query parse error` | SQL query could not be parsed |
| `query contains set operation (UNION/INTERSECT/EXCEPT)` | Query uses UNION, INTERSECT, or EXCEPT |
| `query has no FROM clause` | Query does not reference any table |
| `query involves multiple tables (join)` | Query joins multiple tables |
| `table not found in database` | Referenced table does not exist |
| `table has no primary key defined` | Table lacks a PRIMARY KEY constraint |
| `PK columns not present in SELECT list` | Primary key columns not included in SELECT |
| `FROM clause contains subquery` | Query uses a subquery in the FROM clause |

**Selective Update Fallback Logs** (at update time):
```
Selective update skipped for subscription <id>: disabled in config
Selective update skipped for subscription <id>: row count mismatch (old=X, new=Y)
Selective update skipped for subscription <id>: cannot match row by PK (pk_columns=[...])
Selective update: N rows exceeded change threshold for subscription <id>
Selective update skipped for subscription <id>: no column changes detected
```

These logs help operators understand why subscriptions are sending full data updates (0xF2) instead of selective column updates (0xF7).

## Understanding Partial Update Efficiency

The partial update system optimizes bandwidth for subscriptions by only sending columns that have changed, rather than full rows.

**Efficiency Calculation:**
```
efficiency = 1 - (columns_sent / total_columns)
```

- **1.0 (100%)** - No columns sent (all unchanged)
- **0.7 (70%)** - Only 30% of columns sent
- **0.0 (0%)** - All columns sent (full row update)

Higher efficiency means more bandwidth saved.

## PromQL Examples

### Query Performance

```promql
# Average query latency by statement type (last 5 minutes)
rate(vibesql_server_query_duration_seconds_sum[5m])
  / rate(vibesql_server_query_duration_seconds_count[5m])

# Query error rate
sum(rate(vibesql_server_query_errors_total[5m]))
  / sum(rate(vibesql_server_queries_total[5m])) * 100

# 95th percentile query latency
histogram_quantile(0.95,
  sum(rate(vibesql_server_query_duration_seconds_bucket[5m])) by (le))
```

### Connection Health

```promql
# Active connections (approximation based on connection rate and duration)
rate(vibesql_server_connections_total[5m])
  * avg(vibesql_server_connection_duration_seconds)

# Connection error rate
rate(vibesql_server_connection_errors_total[5m])
  / rate(vibesql_server_connections_total[5m]) * 100
```

### Subscription Efficiency

```promql
# Partial update efficiency (should be high, closer to 1.0 is better)
vibesql_partial_update_efficiency

# Fallback rate by reason (lower is better)
sum by (reason) (rate(vibesql_partial_update_fallbacks_total[5m]))

# Total fallback rate as percentage of selective-eligible subscriptions
sum(rate(vibesql_partial_update_fallbacks_total[5m]))
  / vibesql_subscriptions_selective_eligible * 100

# Average bytes saved per partial update
rate(vibesql_partial_update_bytes_saved_sum[5m])
  / rate(vibesql_partial_update_bytes_saved_count[5m])

# Subscription update breakdown by type
sum by (type) (rate(vibesql_subscription_updates_total[5m]))

# Selective update ratio (selective updates / total updates)
sum(rate(vibesql_subscription_updates_total{type="selective"}[5m]))
  / sum(rate(vibesql_subscription_updates_total[5m])) * 100
```

### Selective Update Eligibility Analysis

```promql
# Percentage of subscriptions eligible for selective updates
sum(vibesql_subscriptions_selective_eligible) / sum(vibesql_subscriptions_active)

# PK detection success rate
sum(rate(vibesql_subscription_pk_detection_total{result="confident"}[5m]))
  / sum(rate(vibesql_subscription_pk_detection_total[5m])) * 100

# PK detection failure breakdown by reason
sum by (reason) (rate(vibesql_subscription_pk_detection_total{result="not_confident"}[5m]))

# Selective update decision breakdown
sum by (decision, reason) (rate(vibesql_subscription_selective_update_decisions_total[5m]))

# Partial update success rate (sent_partial / total decisions)
sum(rate(vibesql_subscription_selective_update_decisions_total{decision="sent_partial"}[5m]))
  / sum(rate(vibesql_subscription_selective_update_decisions_total[5m])) * 100

# Average column change ratio (helps tune max_changed_columns_ratio threshold)
histogram_quantile(0.50,
  sum(rate(vibesql_subscription_selective_update_column_ratio_bucket[5m])) by (le))

# 95th percentile column change ratio
histogram_quantile(0.95,
  sum(rate(vibesql_subscription_selective_update_column_ratio_bucket[5m])) by (le))
```

### Bandwidth Monitoring

```promql
# Network throughput
rate(vibesql_server_bytes_sent_total[5m])
rate(vibesql_server_bytes_received_total[5m])

# Estimated bandwidth savings from partial updates (bytes/sec)
rate(vibesql_partial_update_bytes_saved_sum[5m])
```

## Alerting Guidelines

### Recommended Alerts

#### High Query Error Rate
```yaml
alert: VibeSQL_HighQueryErrorRate
expr: |
  sum(rate(vibesql_server_query_errors_total[5m]))
  / sum(rate(vibesql_server_queries_total[5m])) > 0.05
for: 5m
labels:
  severity: warning
annotations:
  summary: "High query error rate (>5%)"
```

#### High Query Latency
```yaml
alert: VibeSQL_HighQueryLatency
expr: |
  histogram_quantile(0.95,
    sum(rate(vibesql_server_query_duration_seconds_bucket[5m])) by (le)) > 1.0
for: 5m
labels:
  severity: warning
annotations:
  summary: "95th percentile query latency exceeds 1 second"
```

#### Low Partial Update Efficiency
```yaml
alert: VibeSQL_LowPartialUpdateEfficiency
expr: vibesql_partial_update_efficiency < 0.3
for: 10m
labels:
  severity: info
annotations:
  summary: "Partial update efficiency below 30%"
  description: "Most updates are sending majority of columns. Consider reviewing query patterns."
```

#### High Partial Update Fallback Rate
```yaml
alert: VibeSQL_HighPartialUpdateFallbacks
expr: |
  sum(rate(vibesql_partial_update_fallbacks_total[5m]))
  / (vibesql_subscriptions_selective_eligible + 1) > 0.5
for: 5m
labels:
  severity: warning
annotations:
  summary: "High partial update fallback rate"
  description: "More than 50% of updates falling back to full updates. Check reason label for cause."
```

#### No Selective-Eligible Subscriptions
```yaml
alert: VibeSQL_NoSelectiveEligibleSubscriptions
expr: vibesql_subscriptions_selective_eligible == 0
for: 15m
labels:
  severity: info
annotations:
  summary: "No subscriptions eligible for selective updates"
  description: "Ensure tables have primary keys defined for selective update eligibility."
```

#### Low PK Detection Success Rate
```yaml
alert: VibeSQL_LowPKDetectionRate
expr: |
  sum(rate(vibesql_subscription_pk_detection_total{result="confident"}[5m]))
  / sum(rate(vibesql_subscription_pk_detection_total[5m])) < 0.5
for: 10m
labels:
  severity: info
annotations:
  summary: "Low primary key detection success rate"
  description: "Less than 50% of subscriptions have confident PK detection. Check reason label breakdown for optimization opportunities."
```

### Suggested Thresholds

| Metric | Warning | Critical | Notes |
|--------|---------|----------|-------|
| Query error rate | >5% | >20% | Investigate error types |
| P95 query latency | >1s | >5s | Check slow queries |
| Connection error rate | >1% | >10% | Check server resources |
| Partial update efficiency | <0.3 | <0.1 | Review update patterns |
| Fallback rate (per eligible) | >0.5/s | >2.0/s | Check fallback reasons |
| PK detection success rate | <50% | <20% | Review query patterns and table schemas |

## Grafana Dashboard

Example dashboard panels for VibeSQL monitoring:

1. **Overview Row**
   - Total queries/sec (counter)
   - Active connections (gauge)
   - Error rate (percentage)

2. **Query Performance Row**
   - Query latency heatmap
   - Queries by statement type
   - Error breakdown by type

3. **Subscription Efficiency Row**
   - Partial update efficiency gauge
   - Update type distribution pie chart
   - Fallback reasons over time
   - Bytes saved rate

4. **Selective Update Eligibility Row**
   - PK detection success rate (pie chart: confident vs not_confident)
   - PK detection failure reasons over time (stacked area)
   - Selective update decision breakdown (pie chart: sent_partial, sent_full, skipped)
   - Column change ratio distribution (histogram)

5. **Network Row**
   - Bytes sent/received
   - Messages by type
   - Bandwidth savings from partial updates
