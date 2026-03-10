#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

# ── SEO title overrides ──────────────────────────────────────────────
# Keys: basename (without .md). When a file is not listed here,
# the title is auto-generated from the filename (title-cased).
declare -A title_overrides=(
  # Root docs
  ["overview"]="Architecture Overview"
  ["getting_started"]="Getting Started"
  ["bucket"]="Buckets"
  ["cli"]="CLI Reference"
  ["etl"]="ETL: Extract, Transform, Load"
  ["s3compat"]="S3 API Compatibility"
  ["http_api"]="HTTP RESTful API Reference"
  ["performance"]="Performance Tuning"
  ["batch"]="Batch Operations and Xactions"
  ["configuration"]="Configuration Guide"
  ["providers"]="Backend Providers"
  ["ha"]="High Availability"
  ["authn"]="Authentication (AuthN)"
  ["auth_validation"]="Authentication Validation"
  ["networking"]="Networking Guide"
  ["get_batch"]="Batch Object Retrieval (get-batch)"
  ["aisloader"]="AISLoader Benchmark Tool"
  ["blob_downloader"]="Blob Downloader"
  ["dsort"]="Distributed Sort (dSort)"
  ["downloader"]="Internet Downloader"
  ["storage_svcs"]="Storage Services"
  ["out_of_band"]="Out-of-Band Updates"
  ["on_disk_layout"]="On-Disk Layout"
  ["environment-vars"]="Environment Variables"
  ["feature_flags"]="Feature Flags"
  ["build_tags"]="Build Tags"
  ["fshc"]="Filesystem Health Checker (FSHC)"
  ["ic"]="Information Center (IC)"
  ["msgp"]="MessagePack Protocol"
  ["sysfiles"]="System Files"
  ["distributed-tracing"]="Distributed Tracing"
  ["docker_main"]="Docker Deployment"
  ["easy_url"]="Easy URL Access"
  ["howto_benchmark"]="How to Benchmark"
  ["howto_virt_dirs"]="Virtual Directories"
  ["https"]="HTTPS Configuration"
  ["switch_https"]="Switching to HTTPS"
  ["idle_connections"]="Idle Connections"
  ["join_cluster"]="Joining a Cluster"
  ["leave_cluster"]="Leaving a Cluster"
  ["lifecycle"]="Bucket Lifecycle"
  ["lifecycle_node"]="Node Lifecycle"
  ["long_names"]="Long Object Names"
  ["rate_limit"]="Rate Limiting"
  ["traffic_patterns"]="Traffic Patterns"
  ["helpful_links"]="Helpful Links"
  ["command_line"]="Command-Line Tools"
  ["monitoring-overview"]="Monitoring Overview"
  ["monitoring-cli"]="Monitoring with CLI"
  ["monitoring-grafana"]="Monitoring with Grafana"
  ["monitoring-kubernetes"]="Monitoring on Kubernetes"
  ["monitoring-logs"]="Log Management"
  ["monitoring-metrics"]="Metrics Reference"
  ["monitoring-prometheus"]="Monitoring with Prometheus"
  ["monitoring-get-batch"]="Monitoring Get-Batch"
  ["tensorflow"]="TensorFlow Integration"
  # CLI subcommands (use cli/ prefix to avoid conflicts with root docs)
  ["cli/advanced"]="CLI: Advanced Commands"
  ["cli/alias"]="CLI: Aliases"
  ["cli/archive"]="CLI: Archive and Shard Commands"
  ["cli/auth"]="CLI: Auth Commands"
  ["cli/aws_profile_endpoint"]="CLI: AWS Profile and Endpoint Config"
  ["cli/blob-downloader"]="CLI: Blob Downloader Commands"
  ["cli/bucket"]="CLI: Bucket Commands"
  ["cli/cluster"]="CLI: Cluster Management"
  ["cli/config"]="CLI: Configuration Commands"
  ["cli/download"]="CLI: Download Commands"
  ["cli/dsort"]="CLI: Distributed Sort (dSort)"
  ["cli/etl"]="CLI: ETL Commands"
  ["cli/evicting_buckets_andor_data"]="CLI: Evict Commands"
  ["cli/gcp_creds"]="CLI: GCP Credentials"
  ["cli/help"]="CLI: Help and Quick Tips"
  ["cli/job"]="CLI: Job Commands (Xactions)"
  ["cli/log"]="CLI: Log Commands"
  ["cli/ml"]="CLI: Machine Learning Commands"
  ["cli/object"]="CLI: Object Commands"
  ["cli/performance"]="CLI: Performance Monitoring"
  ["cli/search"]="CLI: Search Commands"
  ["cli/show"]="CLI: Show Commands"
  ["cli/storage"]="CLI: Storage Commands"
  ["cli/x509"]="CLI: X.509 Certificate Commands"
  # Release notes (use relnotes/ prefix)
  ["relnotes/3.30"]="Release Notes v3.30"
  ["relnotes/4.0"]="Release Notes v4.0"
  ["relnotes/4.1"]="Release Notes v4.1"
  ["relnotes/4.2"]="Release Notes v4.2"
  # Tutorials (use tutorials/etl/ prefix)
  ["tutorials/etl/compute_md5"]="Tutorial: Computing MD5 with ETL"
  ["tutorials/etl/etl_imagenet_pytorch"]="Tutorial: ETL with ImageNet and PyTorch"
  ["tutorials/etl/etl_webdataset"]="Tutorial: ETL with WebDataset"
)

# ── SEO descriptions ─────────────────────────────────────────────────
# Keys: basename (without .md). Pages not listed here get a default.
declare -A descriptions=(
  # Root docs
  ["overview"]="AIStore architecture with proxy and target nodes, distributed metadata, and petascale storage for AI workloads."
  ["getting_started"]="Deploy AIStore from a single Linux machine to bare-metal clusters with this quick start guide."
  ["bucket"]="Create, list, copy, rename, and manage AIStore buckets across native and cloud storage providers."
  ["cli"]="Complete guide to AIStore CLI commands for buckets, objects, clusters, jobs, and configuration."
  ["etl"]="Run user-defined ETL transforms on AIStore storage nodes to reduce data movement for AI/ML pipelines."
  ["s3compat"]="Use AIStore with Amazon S3 API including backend storage, frontend compatibility, and presigned URLs."
  ["http_api"]="Complete HTTP RESTful API reference for AIStore cluster, bucket, object, and job operations."
  ["performance"]="Optimize AIStore throughput with CPU, network, filesystem, and kernel tuning recommendations."
  ["batch"]="Run 30+ asynchronous batch operations in AIStore: rebalance, prefetch, copy, transform, and more."
  ["configuration"]="Configure AIStore cluster-wide and per-node settings for backends, logging, and runtime tuning."
  ["providers"]="Access AWS S3, Google Cloud, Azure, OCI, and HDFS through AIStore unified storage API."
  ["ha"]="AIStore high availability with redundant gateways, mirroring, erasure coding, and self-healing."
  ["authn"]="Manage users, tokens, and access control with AIStore built-in authentication server."
  ["auth_validation"]="Validate authentication tokens and configure auth enforcement in AIStore clusters."
  ["networking"]="Configure multi-homing, network separation, and IPv6 for data, control, and intra-cluster traffic."
  ["get_batch"]="Retrieve thousands of objects efficiently with parallel, pipelined bulk downloads for AI training."
  ["aisloader"]="Benchmark AIStore performance with aisloader, a built-in load generator for stress testing."
  ["blob_downloader"]="Download large objects from remote cloud storage with chunked, parallel blob downloads."
  ["dsort"]="Distributed sort and shuffle for resharding massive datasets across AIStore cluster nodes."
  ["downloader"]="Download objects from HTTP/HTTPS/cloud sources into AIStore buckets at scale."
  ["storage_svcs"]="AIStore storage services including mirroring, erasure coding, and data protection."
  ["out_of_band"]="Handle out-of-band updates when remote cloud objects change outside of AIStore."
  ["on_disk_layout"]="AIStore on-disk data layout, mountpath structure, and content-type directories."
  ["environment-vars"]="Environment variables reference for configuring AIStore daemons and tools."
  ["feature_flags"]="Feature flags for enabling and disabling AIStore capabilities at runtime."
  ["build_tags"]="Go build tags for compiling AIStore with specific cloud backends and features."
  ["fshc"]="Filesystem Health Checker automatically detects and handles failing storage drives."
  ["ic"]="Information Center protocol for distributed cluster state and notification management."
  ["msgp"]="MessagePack-based protocol for efficient binary serialization in AIStore metadata."
  ["checksum"]="Checksum configuration and data integrity verification in AIStore."
  ["sysfiles"]="AIStore system files, marker files, and internal metadata management."
  ["distributed-tracing"]="Configure OpenTelemetry distributed tracing for AIStore request flows."
  ["docker_main"]="Deploy AIStore using Docker containers for development and testing."
  ["easy_url"]="Simplified URL patterns for accessing AIStore objects without API calls."
  ["howto_benchmark"]="Step-by-step guide to benchmarking AIStore throughput and latency."
  ["howto_virt_dirs"]="Create and manage virtual directories for hierarchical object organization."
  ["https"]="Configure HTTPS/TLS for secure AIStore cluster communication."
  ["switch_https"]="Switch a running AIStore cluster between HTTP and HTTPS modes."
  ["idle_connections"]="Tune HTTP idle connection settings for optimal AIStore network performance."
  ["join_cluster"]="Add new proxy or target nodes to a running AIStore cluster."
  ["leave_cluster"]="Gracefully remove nodes from an AIStore cluster with automatic rebalancing."
  ["lifecycle"]="AIStore bucket lifecycle from creation through deletion and cleanup."
  ["lifecycle_node"]="AIStore node lifecycle: startup, registration, maintenance, and decommission."
  ["rebalance"]="Automatic data rebalancing when AIStore cluster membership changes."
  ["resilver"]="Resilver (repair) data on AIStore target nodes after drive replacement."
  ["long_names"]="Support for long object names exceeding filesystem path limits in AIStore."
  ["unicode"]="Unicode support for object names and bucket names in AIStore."
  ["rate_limit"]="Configure rate limiting for AIStore backend requests to cloud providers."
  ["traffic_patterns"]="AIStore network traffic patterns for reads, writes, rebalance, and replication."
  ["helpful_links"]="Curated links to AIStore resources, articles, and related projects."
  ["command_line"]="AIStore command-line tools overview: CLI, aisloader, authn, and more."
  ["archive"]="AIStore archive and shard operations for tar, zip, and compressed objects."
  ["development"]="AIStore development guide for contributors: building, testing, and debugging."
  ["tools"]="AIStore development and operational tools reference."
  ["troubleshooting"]="Troubleshoot common AIStore issues with diagnostics and solutions."
  ["tensorflow"]="TensorFlow integration for accessing AIStore datasets in training pipelines."
  ["monitoring-overview"]="Monitor AIStore clusters with Prometheus, Grafana, CLI, and logging."
  ["monitoring-cli"]="Monitor AIStore cluster health, performance, and jobs using the CLI."
  ["monitoring-grafana"]="Set up Grafana dashboards for visualizing AIStore cluster metrics."
  ["monitoring-kubernetes"]="Monitor AIStore deployments on Kubernetes with Prometheus and Grafana."
  ["monitoring-logs"]="Configure and manage AIStore log levels, rotation, and output."
  ["monitoring-metrics"]="Complete reference of AIStore Prometheus metrics for monitoring."
  ["monitoring-prometheus"]="Set up Prometheus monitoring for AIStore cluster metrics collection."
  ["monitoring-get-batch"]="Monitor get-batch operations with progress tracking and metrics."
  # CLI subcommands (use cli/ prefix to avoid conflicts with root docs)
  ["cli/advanced"]="AIStore CLI advanced commands for power users and scripting."
  ["cli/alias"]="Create and manage AIStore CLI command aliases for common operations."
  ["cli/archive"]="AIStore CLI commands for working with tar, zip, and compressed archives."
  ["cli/auth"]="AIStore CLI commands for user authentication, token management, and roles."
  ["cli/aws_profile_endpoint"]="Configure AWS profiles and custom S3 endpoints in AIStore CLI."
  ["cli/blob-downloader"]="AIStore CLI commands for downloading large objects with chunked blob downloads."
  ["cli/bucket"]="AIStore CLI commands for creating, listing, copying, renaming, and managing buckets."
  ["cli/cluster"]="AIStore CLI commands for cluster status, node management, and membership."
  ["cli/config"]="AIStore CLI commands for viewing and modifying cluster and node configuration."
  ["cli/download"]="AIStore CLI commands for downloading objects from HTTP/cloud sources."
  ["cli/dsort"]="AIStore CLI commands for distributed sort and resharding operations."
  ["cli/etl"]="AIStore CLI commands for initializing, running, and managing ETL transforms."
  ["cli/evicting_buckets_andor_data"]="AIStore CLI commands for evicting cached remote data and buckets."
  ["cli/gcp_creds"]="Configure Google Cloud credentials for AIStore CLI access."
  ["cli/help"]="AIStore CLI help system, quick tips, and command discovery."
  ["cli/job"]="AIStore CLI commands for starting, monitoring, and managing async jobs."
  ["cli/log"]="AIStore CLI commands for viewing and managing cluster node logs."
  ["cli/ml"]="AIStore CLI commands for machine learning data operations."
  ["cli/object"]="AIStore CLI commands for getting, putting, listing, and managing objects."
  ["cli/performance"]="AIStore CLI commands for throughput monitoring, disk stats, and performance tracking."
  ["cli/search"]="AIStore CLI commands for searching objects and cluster resources."
  ["cli/show"]="AIStore CLI show commands for cluster status, performance, and config."
  ["cli/storage"]="AIStore CLI commands for managing storage mountpaths and disk capacity."
  ["cli/x509"]="AIStore CLI commands for managing X.509 certificates and TLS."
  # Tutorials (use full path prefix)
  ["tutorials/etl/compute_md5"]="Tutorial: compute MD5 checksums using AIStore ETL inline transforms."
  ["tutorials/etl/etl_imagenet_pytorch"]="Tutorial: transform ImageNet data with AIStore ETL and PyTorch dataloaders."
  ["tutorials/etl/etl_webdataset"]="Tutorial: process WebDataset shards with AIStore ETL pipelines."
  # Release notes (use relnotes/ prefix)
  ["relnotes/3.30"]="What's new in AIStore v3.30: features, improvements, and bug fixes."
  ["relnotes/4.0"]="What's new in AIStore v4.0: major release with new features and improvements."
  ["relnotes/4.1"]="What's new in AIStore v4.1: features, improvements, and bug fixes."
  ["relnotes/4.2"]="What's new in AIStore v4.2: features, improvements, and bug fixes."
)

# ── Title helper: filename → Title Case ───────────────────────────────
to_title_case() {
  echo "$1" | sed 's/_/ /g; s/-/ /g' \
    | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2))}1'
}

# ── Main loop ─────────────────────────────────────────────────────────
find docs -type f -name "*.md" | while read -r file; do
  # Skip directories that already have front-matter-controlled docs
  if [[ "$file" == */_posts/* || "$file" == */_drafts/* || \
        "$file" == */_pages/* || "$file" == */changelog/* || \
        "$file" == */vendor/* ]]; then
    echo "Skip $file (special dir)"
    continue
  fi

  bname=$(basename "$file" .md)
  # Skip README and index.md
  if [[ "$bname" == "README" || "$bname" == "index" ]]; then
    echo "Skip $file (README/index)"
    continue
  fi

  # Skip if front-matter exists
  if [[ "$(head -c 3 "$file")" == "---" ]]; then
    echo "Skip $file (already has front-matter)"
    continue
  fi

  rel_path=${file#docs/}
  dir_path=$(dirname "$rel_path")

  if [[ "$dir_path" == "." ]]; then
    permalink="/docs/$bname"
    redirect1="/${bname}.md/"
    redirect2="/docs/${bname}.md/"
  elif [[ "$dir_path" == "Models" ]]; then
    # Models need .html extension for proper MIME type
    permalink="/docs/$dir_path/$bname.html"
    redirect1="/${dir_path}/${bname}.md/"
    redirect2="/docs/${dir_path}/${bname}.md/"
  else
    permalink="/docs/$dir_path/$bname"
    redirect1="/${dir_path}/${bname}.md/"
    redirect2="/docs/${dir_path}/${bname}.md/"
  fi

  # ── Lookup key: try dir/basename first, then just basename ────────
  if [[ "$dir_path" != "." ]]; then
    lookup_key="$dir_path/$bname"
  else
    lookup_key="$bname"
  fi

  # ── SEO title ────────────────────────────────────────────────────
  if [[ -n "${title_overrides[$lookup_key]+x}" ]]; then
    seo_title="${title_overrides[$lookup_key]}"
  elif [[ -n "${title_overrides[$bname]+x}" ]]; then
    seo_title="${title_overrides[$bname]}"
  else
    raw_title=$(to_title_case "$bname")
    if [[ "$dir_path" == cli ]]; then
      seo_title="CLI: $raw_title"
    elif [[ "$dir_path" == relnotes ]]; then
      seo_title="Release Notes $raw_title"
    elif [[ "$dir_path" == tutorials/* ]]; then
      seo_title="Tutorial: $raw_title"
    else
      seo_title="$raw_title"
    fi
  fi

  # ── SEO description ──────────────────────────────────────────────
  if [[ -n "${descriptions[$lookup_key]+x}" ]]; then
    desc="${descriptions[$lookup_key]}"
  elif [[ -n "${descriptions[$bname]+x}" ]]; then
    desc="${descriptions[$bname]}"
  else
    desc="AIStore documentation for $(to_title_case "$bname")."
  fi

  frontmatter=$(cat <<EOF
---
layout: post
title: "$seo_title"
description: "$desc"
permalink: $permalink
redirect_from:
 - $redirect1
 - $redirect2
---
EOF
)

  tmp=$(mktemp)
  printf '%s\n\n' "$frontmatter" > "$tmp"

  if [[ "$file" == *"http-api.md"* ]]; then
    # Fix escaped underscores in parameter names (e.g., parameter\_name -> parameter_name)
    sed 's/\([a-zA-Z0-9]\)\\\_\([a-zA-Z0-9]\)/\1_\2/g' "$file" >> "$tmp"
  else
    cat "$file" >> "$tmp"
  fi

  mv "$tmp" "$file"
  echo "Added front-matter to $file"
done
