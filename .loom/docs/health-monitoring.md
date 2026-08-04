# Health Monitoring

Proactive health monitoring for extended unattended autonomous operation — health
score, CLI, alert types, files, configuration, MCP tools, and the UI dashboard.

## Health Monitoring

Loom provides proactive health monitoring for extended unattended autonomous operation. The health system tracks throughput, latency, error rates, and resource usage to detect degradation patterns before they become critical.

### Health Score

The health score (0-100) is computed from multiple factors:
- **Throughput trend** - Declining throughput reduces score
- **Queue depth trend** - Growing queues reduce score
- **Error rate** - Increasing errors reduce score
- **Resource availability** - Near capacity limits reduce score
- **Stuck agents** - Agents without heartbeats reduce score

Score ranges:
| Range | Status | Description |
|-------|--------|-------------|
| 90-100 | Excellent | System operating optimally |
| 70-89 | Good | Normal operation, minor issues |
| 50-69 | Fair | Some degradation detected |
| 30-49 | Warning | Significant issues, attention needed |
| 0-29 | Critical | Immediate intervention required |

### Health Monitoring CLI

```bash
# View current health status
./.loom/scripts/health-check.sh

# JSON output for automation
./.loom/scripts/health-check.sh --json

# Collect and store metrics (called by daemon)
./.loom/scripts/health-check.sh --collect

# View alerts
./.loom/scripts/health-check.sh --alerts

# Acknowledge an alert
./.loom/scripts/health-check.sh --acknowledge <alert-id>

# View metric history (last 4 hours)
./.loom/scripts/health-check.sh --history 4
```

### Alert Types

| Alert Type | Description | Severity |
|------------|-------------|----------|
| `stuck_agents` | Agents without recent heartbeats | warning/critical |
| `high_error_rate` | Consecutive iteration failures | warning/critical |
| `resource_exhaustion` | Session budget near limits | warning/critical |
| `queue_growth` | Ready queue growing without progress | warning |
| `throughput_decline` | Significant throughput drop | warning |

### Health Files

| File | Purpose |
|------|---------|
| `.loom/health-metrics.json` | Historical health metrics (24-hour retention) |
| `.loom/alerts.json` | Active and acknowledged alerts |

### Configuration

Health monitoring thresholds can be configured via environment variables:

```bash
LOOM_HEALTH_RETENTION_HOURS=24       # Metric retention period
LOOM_THROUGHPUT_DECLINE_THRESHOLD=50 # % decline to trigger alert
LOOM_QUEUE_GROWTH_THRESHOLD=5        # Queue growth count threshold
LOOM_STUCK_AGENT_THRESHOLD=10        # Minutes without heartbeat
LOOM_ERROR_RATE_THRESHOLD=20         # % error rate threshold
```

Or via `.loom/config.json`:

```json
{
  "health_monitoring": {
    "enabled": true,
    "collect_interval_minutes": 5,
    "retention_hours": 24,
    "thresholds": {
      "throughput_decline_percent": 50,
      "queue_growth_count": 5,
      "stuck_agent_minutes": 10,
      "error_rate_percent": 20
    }
  }
}
```

### MCP Health Tools

The following MCP tools are available for health monitoring:

| Tool | Description |
|------|-------------|
| `get_health_metrics` | Get current health score and latest metrics |
| `get_health_history` | Get historical metrics for trend analysis |
| `get_active_alerts` | Get unacknowledged alerts |
| `acknowledge_alert` | Acknowledge an alert by ID |

### UI Health Dashboard

Access the Health Dashboard via:
- Menu: View > Health Dashboard
- Keyboard: Cmd+H (macOS) / Ctrl+H (Windows/Linux)

The dashboard displays:
- Health score gauge with status indicator
- Current metrics grid (throughput, queues, errors, resources)
- Active alerts with acknowledge button
- Historical trends with sparkline visualizations
