# AIStore Grafana Integration

This document explains how to integrate AIStore with Grafana for visualization and monitoring of AIStore metrics. Grafana provides powerful visualization capabilities for Prometheus metrics collected from AIStore nodes.

## Architecture

The integration architecture follows a standard monitoring pattern:

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│             │      │             │      │             │
│  AIStore    │scrape│  Prometheus │query │   Grafana   │
│   Nodes     │─────▶│   Server    │─────▶│ Dashboards  │
│             │      │             │      │             │
└─────────────┘      └─────────────┘      └─────────────┘
```

1. AIStore nodes expose metrics endpoints
2. Prometheus scrapes metrics at regular intervals
3. Grafana queries Prometheus and visualizes the data

## Prerequisites

- AIStore cluster with Prometheus metrics enabled
- Prometheus server configured to scrape AIStore metrics
- Grafana server (v9.0+ recommended)

## Grafana Installation

If you don't have Grafana installed yet:

### Docker Installation

```bash
docker run -d -p 3000:3000 --name grafana grafana/grafana-oss
```

### Standard Installation

```bash
# Debian/Ubuntu
sudo apt-get install -y apt-transport-https software-properties-common
sudo add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
sudo apt-get update
sudo apt-get install grafana

# Start Grafana
sudo systemctl start grafana-server
```

For other platforms, see the [Grafana installation documentation](https://grafana.com/docs/grafana/latest/installation/).

## Configuring Grafana

### Adding Prometheus Data Source

1. Log in to Grafana (default: http://localhost:3000, admin/admin)
2. Go to Configuration > Data Sources > Add data source
3. Select Prometheus
4. Set the URL to your Prometheus server (e.g., http://localhost:9090)
5. Click "Save & Test" to verify the connection

### Importing AIStore Dashboards

AIStore provides pre-built Grafana dashboards for monitoring:

#### Method 1: Import from JSON

1. Download the AIStore dashboard JSON file:
   - [AIStore Cluster Dashboard](https://github.com/NVIDIA/aistore/blob/main/deploy/grafana/dashboards/ais_dashboard.json)
   - [AIStore Kubernetes Dashboard](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/kube-prom/dashboard-configmap/ais_dashboard.json)

2. In Grafana, go to Dashboards > Import
3. Upload the JSON file or paste its contents
4. Select your Prometheus data source
5. Click Import

#### Method 2: Import via Grafana.com

1. In Grafana, go to Dashboards > Import
2. Enter the dashboard ID: `10770` (AIStore Cluster Dashboard)
3. Select your Prometheus data source
4. Click Import

## Available Dashboards

### AIStore Cluster Dashboard

The main AIStore dashboard provides:

- Cluster health overview
- Storage capacity and usage
- Throughput and latency metrics
- Operation rates by type
- Error rates
- Resource usage (CPU, memory)

![AIStore Grafana Dashboard](https://github.com/NVIDIA/aistore/blob/main/docs/images/grafana_dashboard.png)

### AIStore Kubernetes Dashboard

For Kubernetes deployments, a specialized dashboard includes:

- Pod status and health
- Node resource utilization
- Storage performance by pod
- Network metrics
- Rebalance monitoring

## Creating Custom Dashboards

### Key Metrics to Visualize

When creating custom dashboards, consider including:

1. **Cluster Health**
   - Number of online nodes
   - Storage capacity and usage
   - Error rates

2. **Performance**
   - Throughput (GET/PUT/DELETE)
   - Operation latency
   - Request rates

3. **Resources**
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network traffic

### Example Dashboard Panels

#### Throughput Panel

```
# Query A (GET throughput)
sum(rate(ais_throughput_bytes{op="get"}[5m]))

# Query B (PUT throughput)
sum(rate(ais_throughput_bytes{op="put"}[5m]))

# Query C (DELETE throughput)
sum(rate(ais_throughput_bytes{op="delete"}[5m]))
```

#### Latency Panel

```
# Query A (GET latency)
sum(rate(ais_latency_ns{op="get"}[5m])) / sum(rate(ais_operations_count{op="get"}[5m])) / 1000000

# Query B (PUT latency)
sum(rate(ais_latency_ns{op="put"}[5m])) / sum(rate(ais_operations_count{op="put"}[5m])) / 1000000
```

#### Storage Usage Panel

```
# Query
100 * sum(ais_disk_used_bytes) / sum(ais_disk_capacity_bytes)
```

## Alert Configuration

Grafana can be configured to send alerts based on metric thresholds:

1. Edit a panel in your dashboard
2. Go to the Alert tab
3. Configure alert conditions, for example:
   - Condition: avg() of query(A,5m,now) is above 90
   - (For disk usage > 90%)
4. Set the notification channel (email, Slack, etc.)
5. Save the alert

### Common Alert Thresholds

| Metric | Warning | Critical | Description |
|--------|---------|----------|-------------|
| Disk Usage | 85% | 95% | Storage capacity utilization |
| Error Rate | 1% | 5% | Operation error percentage |
| Node Count | n-1 | n-2 | Number of online nodes vs. expected |
| CPU Usage | 70% | 90% | CPU utilization |
| Memory Usage | 80% | 95% | Memory utilization |
| Latency | 2x baseline | 5x baseline | Operation latency increase |

## Advanced Visualization

### Variable Templates

Create dashboard variables to make your dashboard more interactive:

1. Dashboard Settings > Variables > New
2. Create a variable for node selection:
   - Name: `node`
   - Type: Query
   - Data source: Prometheus
   - Query: `label_values(ais_daemon_info, daemon_id)`

Use the variable in your queries: `{daemon_id="$node"}`

### Heatmaps for Latency Distribution

For latency visualization, consider using Grafana heatmaps:

1. Add a new panel
2. Select Heatmap visualization
3. Use a query like:
   ```
   rate(ais_latency_ns_bucket{op="get"}[5m])
   ```
4. Format as Heatmap

### Multi-Cluster Comparison

For comparing multiple AIStore clusters:

1. Create a variable for cluster selection:
   - Name: `cluster`
   - Type: Query
   - Query: `label_values(ais_cluster_version, cluster_name)`

2. Use in your queries: `{cluster_name="$cluster"}`

## Production Best Practices

1. **Retention and Sampling**
   - Configure appropriate retention periods in Prometheus
   - Use recording rules for complex queries
   - Consider downsampling for long-term storage

2. **Dashboard Organization**
   - Group related metrics on the same dashboard
   - Use row dividers to organize panels
   - Add documentation links and descriptions

3. **Performance Considerations**
   - Limit the number of panels per dashboard
   - Use appropriate time ranges
   - Avoid overly complex queries

4. **User Management**
   - Configure proper authentication
   - Set up appropriate user roles
   - Share dashboards with read-only permissions

## Troubleshooting

### No Data in Grafana

1. Verify Prometheus data source connection
2. Check if Prometheus is successfully scraping AIStore metrics
3. Validate that the query returns data in Prometheus UI
4. Check time range settings in Grafana

### Incomplete Metrics

1. Ensure all AIStore nodes are being scraped
2. Verify that AIStore has Prometheus metrics enabled
3. Check for any errors in Prometheus logs

### Dashboard Performance Issues

1. Simplify complex queries
2. Reduce the number of panels
3. Increase the minimum refresh interval
4. Use recording rules for frequently used queries

## Further Resources

- [AIStore Metrics Reference](31-metrics-reference.md)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus and Grafana Best Practices](https://prometheus.io/docs/practices/instrumentation/)
- [Advanced PromQL Queries](https://prometheus.io/docs/prometheus/latest/querying/examples/)
