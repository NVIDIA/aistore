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

> StatsD was deprecated in v3.28 (Spring 2025) and completely removed in v4.0 (September 2025).

## Open telemetry

| build tag | comment |
| --- | --- |
| `oteltracing`| Include support for [OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/) tracing |

## Intra-cluster transport

There are two alternative ways to build `transport` package for intra-cluster networking:

1. using Go net/http, or
2. with a 3rd party github.com/valyala/fasthttp aka "fasthttp"

The second option is the current default.

| build tag | comment |
| --- | --- |
| `nethttp`| Use [net/http](https://pkg.go.dev/net/http) for intra-cluster transport (the default is [fasthttp](https://github.com/valyala/fasthttp)) |

## fs.Walk from the standard library

| build tag | comment |
| --- | --- |
| `stdlibwalk`| Instead of [godirwalk](https://github.com/karrick/godirwalk) use `filepath.WalkDir` from the standard library |

## Dsort (distributed shuffle)

| build tag | comment |
| --- | --- |
| `sharding`| Build `aisnode` with dsort enabled and the respective APIs not stubbed; for details, see docs/dsort.md, api/dsort.go (Go), and python/aistore/sdk/dsort (Python) |
