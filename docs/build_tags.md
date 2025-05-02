---
layout: post
title: CONDITIONAL LINKAGE
permalink: /docs/build_tags
redirect_from:
 - /build_tags.md/
 - /docs/build_tags.md/
---

Conditional linkage generally refers to the inclusion or exclusion of certain pieces of code or libraries during the build process.

In Go, we use **build tags** to achieve the same.

Here's the current list, grouped by category.

## Supported backends

| build tag | comment |
| --- | --- |
| `aws`| Include support for AWS S3 |
| `azure`| Include support for Azure Blob Storage |
| `gcp`| Include support for Google Cloud Platform |
| `ht`| Include support for a custom `ht://` backend |
| `oci`| Include support for Oracle Cloud Infrastructure (OCI) |

## Debug & development

| build tag | comment |
| --- | --- |
| `debug`| Include debug-related code and assertions |
| `deadbeef`| `DEADBEEF` freed [memsys](https://github.com/NVIDIA/aistore/tree/main/memsys) buffers |

## StatsD

| build tag | comment |
| --- | --- |
| `statsd`| Build with StatsD support instead of Prometheus (default) |

## Open telemetry

| build tag | comment |
| --- | --- |
| `oteltracing`| Include support for [OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/) tracing |

## Intra-cluster transport

| build tag | comment |
| --- | --- |
| `nethttp`| Use [net/http](https://pkg.go.dev/net/http) for intra-cluster transport (the default is [fasthttp](github.com/valyala/fasthttp)) |

## fs.Walk from the standard library

| build tag | comment |
| --- | --- |
| `stdlibwalk`| Instead of [godirwalk](github.com/karrick/godirwalk) use `filepath.WalkDir` from the standard library |

