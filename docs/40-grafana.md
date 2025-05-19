# AIStore Grafana Integration

This document explains how to integrate AIStore with Grafana for visualization and monitoring of AIStore metrics. Grafana provides powerful visualization capabilities for Prometheus metrics collected from AIStore nodes.

## Table of Contents
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Deployment Options](#deployment-options)
  - [Kubernetes Deployment](#kubernetes-deployment)
  - [Standalone Deployment](#standalone-deployment)
- [Grafana Setup](#grafana-setup)
  - [Adding Prometheus Data Source](#adding-prometheus-data-source)
  - [Importing AIStore Dashboards](#importing-aistore-dashboards)
- [Available Dashboards](#available-dashboards)
  - [AIStore Cluster Dashboard](#aistore-cluster-dashboard)
  - [AIStore Kubernetes Dashboard](#aistore-kubernetes-dashboard)
- [Creating Custom Dashboards](#creating-custom-dashboards)
  - [Key Metrics to Visualize](#key-metrics-to-visualize)
  - [Example PromQL Queries](#example-promql-queries)
  - [Variable Templates](#variable-templates)
- [Alert Configuration](#alert-configuration)
  - [Node State Alerts](#node-state-alerts)
  - [Performance Alerts](#performance-alerts)
  - [Resource Alerts](#resource-alerts)
- [Production Best Practices](#production-best-practices)
- [Troubleshooting](#troubleshooting)
- [Further Resources](#further-resources)

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

1. AIStore nodes expose metrics endpoints (via the `/metrics` HTTP endpoint)
2. Prometheus scrapes metrics at regular intervals
3. Grafana queries Prometheus and visualizes the data through customizable dashboards

## Prerequisites

- AIStore cluster with Prometheus metrics enabled (default in current versions)
- Prometheus server configured to scrape AIStore metrics
- Grafana server (v9.0+ recommended)

## Deployment Options

### Kubernetes Deployment

For production deployments, AIStore is typically deployed on Kubernetes using the [ais-k8s operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator).

The [github.com/NVIDIA/ais-k8s](https://github.com/NVIDIA/ais-k8s) repository includes monitoring components that set up Prometheus and Grafana with preconfigured dashboards in the [monitoring](https://github.com/NVIDIA/ais-k8s/tree/main/monitoring) directory.

To deploy AIStore with monitoring on Kubernetes:

1. Clone the ais-k8s repository:
   ```bash
   git clone https://github.com/NVIDIA/ais-k8s.git
   cd ais-k8s
   ```

2. Deploy AIStore with the operator, which includes monitoring stack:
   ```bash
   # Follow the operator deployment instructions in ais-k8s documentation
   ```

3. The monitoring stack includes:
   - Prometheus for metrics collection
   - Grafana for visualization
   - AlertManager for alert routing
   - Preconfigured dashboards for AIStore

### Standalone Deployment

For non-Kubernetes deployments or development environments, you can set up Grafana separately.

#### Grafana Installation

##### Docker Installation

```bash
docker run -d -p 3000:3000 --name grafana grafana/grafana-oss
```

##### Standard Installation

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

## Grafana Setup

### Adding Prometheus Data Source

1. Log in to Grafana (default: http://localhost:3000, admin/admin)
2. Go to Configuration > Data Sources > Add data source
3. Select Prometheus
4. Set the URL to your Prometheus server (e.g., http://prometheus-server:9090)
5. Click "Save & Test" to verify the connection

### Importing AIStore Dashboards

AIStore provides pre-built Grafana dashboards for monitoring:

#### Method 1: Import from JSON

1. Download the AIStore dashboard JSON file:
   - [AIStore Kubernetes Dashboard](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/kube-prom/dashboard-configmap/ais_dashboard.json)

2. In Grafana, go to Dashboards > Import
3. Upload the JSON file or paste its contents
4. Select your Prometheus data source
5. Click Import

#### Method 2: Manual Dashboard Creation

If you prefer to build dashboards from scratch:

1. In Grafana, create a new dashboard (+ > Create > Dashboard)
2. Add panels with AIStore-specific metrics using the PromQL queries provided in the [Example PromQL Queries](#example-promql-queries) section
3. Organize panels into logical sections (cluster overview, node details, operations, etc.)
4. Save the dashboard

## Available Dashboards

### AIStore Cluster Dashboard

The main AIStore dashboard provides:

- Cluster health overview
- Storage capacity and usage
- Throughput and latency metrics
- Operation rates by type
- Error rates
- Resource usage (CPU, memory)

![AIStore Grafana Dashboard](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/images/grafana.png)

Key panels include:

- **Cluster Health**: Node status, rebalance status
- **Operations**: GET/PUT/DELETE throughput, latency statistics
- **Storage**: Capacity, utilization, growth trends
- **Resources**: CPU, memory, network usage by node
- **Errors**: Error rates, error distribution by type

### AIStore Kubernetes Dashboard

For Kubernetes deployments, the specialized dashboard includes:

- Pod status and health
- Node resource utilization by pod
- Storage performance by pod
- Network metrics
- Rebalance monitoring
- Kubernetes-specific resource allocation

## Creating Custom Dashboards

### Key Metrics to Visualize

When creating custom dashboards, consider including:

1. **Cluster Health**
   - Number of online nodes
   - Storage capacity and usage
   - Error rates
   - Node states and alerts

2. **Performance**
   - Throughput (GET/PUT/DELETE)
   - Operation latency
   - Request rates
   - Cache hit ratios

3. **Resources**
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network traffic

### Example PromQL Queries

#### Throughput Panels

```
# GET throughput (bytes/sec)
sum(rate(ais_target_get_bytes[5m]))

# PUT throughput (bytes/sec)
sum(rate(ais_target_put_bytes[5m]))

# DELETE operations/sec
sum(rate(ais_target_delete_count[5m]))
```

#### Latency Panels

```
# GET latency (milliseconds) using PromQL
sum(rate(ais_target_get_ns_total[5m])) / sum(rate(ais_target_get_count[5m])) / 1000000

# PUT latency (milliseconds) using PromQL
sum(rate(ais_target_put_ns_total[5m])) / sum(rate(ais_target_put_count[5m])) / 1000000
```

#### Storage Usage Panel

```
# Cluster storage utilization percentage
100 * sum(ais_target_capacity_used) / sum(ais_target_capacity_total)

# Storage used by node
ais_target_capacity_used{node_id="$node"}
```

#### Node State Panels

```
# Node state flags (alert conditions)
ais_target_state_flags{node_id="$node"}

# Nodes with red alerts (OOS - out of space)
ais_target_state_flags > 0 and ais_target_state_flags & 8192 > 0
```

### Variable Templates

Create dashboard variables to make your dashboard more interactive:

1. Dashboard Settings > Variables > New
2. Create a variable for node selection:
   - Name: `node`
   - Type: Query
   - Data source: Prometheus
   - Query: `label_values(ais_target_uptime, node_id)`

Use the variable in your queries: `{node_id="$node"}`

## Alert Configuration

Grafana can be configured to send alerts based on metric thresholds:

### Node State Alerts

Set up alerts based on the AIStore node state flags (refer to [Node Alerts in AIStore Prometheus docs](/docs/30-prometheus.md#node-alerts) for all available states):

1. Create an alert for red alert conditions:
   - Condition: `ais_target_state_flags{node_id=~"$node"} > 0 and (ais_target_state_flags{node_id=~"$node"} & 8192 > 0 or ais_target_state_flags{node_id=~"$node"} & 16384 > 0)`
   - Description: "Critical node state alert detected"
   - Severity: Critical

2. Create an alert for warning conditions:
   - Condition: `ais_target_state_flags{node_id=~"$node"} > 0 and (ais_target_state_flags{node_id=~"$node"} & 128 > 0 or ais_target_state_flags{node_id=~"$node"} & 4096 > 0)`
   - Description: "Warning node state alert detected"
   - Severity: Warning

### Performance Alerts

1. High latency alert:
   - Condition: `sum(rate(ais_target_get_ns_total[5m])) / sum(rate(ais_target_get_count[5m])) / 1000000 > 200`
   - Description: "GET operation latency exceeds 200ms"

2. Error rate alert:
   - Condition: `sum(rate(ais_target_err_get_count[5m])) / sum(rate(ais_target_get_count[5m])) * 100 > 1`
   - Description: "GET error rate exceeds 1%"

### Resource Alerts

Common alert thresholds:

| Metric | Warning | Critical | Description |
|--------|---------|----------|-------------|
| Disk Usage | 85% | 95% | Storage capacity utilization |
| Error Rate | 1% | 5% | Operation error percentage |
| Node Count | n-1 | n-2 | Number of online nodes vs. expected |
| CPU Usage | 70% | 90% | CPU utilization |
| Memory Usage | 80% | 95% | Memory utilization |
| Latency | 2x baseline | 5x baseline | Operation latency increase |

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
   - Avoid overly complex queries that could impact Prometheus

4. **High Availability**
   - Deploy Prometheus with high availability for production
   - Consider using Grafana Enterprise for critical environments
   - Set up redundant alerting paths

5. **Security**
   - Configure proper authentication
   - Set up appropriate user roles
   - Share dashboards with read-only permissions
   - Use TLS for all communications

## Troubleshooting

### No Data in Grafana

1. Verify Prometheus data source connection:
   - Check Prometheus server health
   - Ensure connectivity between Grafana and Prometheus
   - Test connection in Data Sources section

2. Check metrics collection:
   - Verify AIStore nodes are exposing metrics
   - Test direct access to metrics endpoint: `curl http://<aistore-node>:<port>/metrics`
   - Check Prometheus targets status

3. Validate PromQL queries:
   - Test queries directly in Prometheus UI
   - Check for typos in metric names
   - Verify label selectors match your deployment

### Incomplete Metrics

1. Ensure all AIStore nodes are being scraped:
   - Check Prometheus targets
   - Verify scrape configurations
   - Check for connectivity issues

2. Check for missing or incomplete metrics:
   - Review AIStore logs for metrics-related messages
   - Verify AIStore version compatibility with dashboards
   - Check for misconfiguration in Prometheus scrape settings

### Dashboard Performance Issues

1. Optimize queries:
   - Simplify complex queries
   - Add appropriate time ranges
   - Use recording rules for frequently used queries

2. Reduce load:
   - Decrease dashboard refresh rate
   - Limit the number of panels per dashboard
   - Consider splitting complex dashboards

## Further Resources

- [AIStore Metrics Reference](/docs/31-metrics-reference.md)
- [AIStore Prometheus Integration](/docs/30-prometheus.md)
- [AIStore K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator)
- [AIStore K8s Monitoring](https://github.com/NVIDIA/ais-k8s/tree/main/monitoring)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus and Grafana Best Practices](https://prometheus.io/docs/practices/instrumentation/)
- [Advanced PromQL Queries](https://prometheus.io/docs/prometheus/latest/querying/examples/)
