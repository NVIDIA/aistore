AIStore supports distributed tracing with OpenTelemetry (OTEL), alongside its [metrics and logging features](/docs/monitoring-overview.md).
Tracing follows client requests across AIStore proxy and target daemons to show request flow and help identify performance bottlenecks.

For more details:
- [Understanding Distributed Tracing](https://opentelemetry.io/docs/concepts/observability-primer/#understanding-distributed-tracing)
- [What is OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/)

> WARNING: Enabling distributed tracing introduces slight overhead in AIStore's critical data path. Enable this feature only after carefully considering its performance impact and ensuring that the benefits of enhanced observability justify the potential trade-offs.


## Table of Contents

- [Getting Started](#getting-started)
  - [Example operations](#example-operations)
- [Configuration](#configuration)
  - [Build AIStore with tracing](#build-aistore-with-tracing)

## Getting Started

Use a fresh, disposable AIStore [local playground](/docs/getting_started.md#local-playground) configuration with no existing instance or data to retain at its paths. Before deploying AIS, configure a compatible **TLS-enabled OTLP/gRPC collector** reachable from every AIS node. Set up collector-side TLS and its trace-query interface using the [current Jaeger documentation](https://www.jaegertracing.io/docs/latest/) or your collector's documentation. The collector address, certificates, access controls, and trace UI are environment-dependent. A default Jaeger all-in-one container is not evidence of a TLS-enabled collector; this guide does not provide or validate a collector deployment.

From the repository root, build the CLI and deploy 1 target and 1 proxy with the exporter endpoint set to your TLS-enabled collector. Replace `localhost:4317` if the collector is not reachable at that address from **each** node:

```sh
TAGS=oteltracing AIS_TRACING_ENDPOINT="localhost:4317" \
  make cli deploy <<< $'1\n1'
```

The local deployment script builds `aisnode` with tracing enabled and generates `tracing.skip_verify: true` when `AIS_TRACING_ENDPOINT` is set. That setting supplies TLS credentials with server-certificate verification **disabled**; it is only for isolated testing with a TLS-enabled collector. It does not select plaintext gRPC. For a real deployment, require verified TLS and establish trust in the collector certificate using deployment-appropriate configuration; do not reuse this local skip-verification setting. The collector must accept the configured OTLP/gRPC connection.

### Example operations

With `AIS_ENDPOINT` pointed at the proxy and the CLI available, run these example operations from the repository root using a new test bucket name in place of `nnn`:

```sh
ais create ais://nnn
ais put README.md ais://nnn
ais get ais://nnn/README.md /dev/null
```

Then check your collector's trace-query interface for AIStore proxy and target spans from those requests. If no spans appear, check collector reachability, TLS compatibility, exporter logs, and sampling before retrying. The query interface and trace visibility depend on the collector configuration and running environment.

## Configuration

The `tracing` configuration is cluster-wide. For other AIStore options, refer to [configuration.md](/docs/configuration.md). Tracing configuration changes require a daemon restart to take effect.

| Option name | Default value | Description |
|---|---|---|
| `tracing.enabled` | `false` | If true, enables distributed tracing |
| `tracing.exporter_endpoint` | `''` | OTEL exporter gRPC endpoint |
| `tracing.service_name_prefix` | `aistore` | Prefix added to OTEL service name reported by exporter |
| `tracing.attributes` | `{}` | Extra attributes to add to traces |
| `tracing.sampler_probability` | `1.0` (sample all traces) | Sampling ratio in [0,1], stored as a JSON string |
| `tracing.skip_verify` | `false` | When true, provide TLS credentials with server-certificate verification disabled; isolated tests only, not plaintext transport |
| `tracing.exporter_auth.token_header` | `''` | Request header used for exporter auth token |
| `tracing.exporter_auth.token_file` | `''` | Filepath to obtain exporter auth token |


Example isolated-test `tracing` configuration section for a TLS-enabled collector (not a complete `ais.json`). The local deployment helper generates `skip_verify: true`; do not reuse that setting as a trust configuration for a real deployment:

```json
{
    "tracing": {
        "enabled": true,
        "exporter_endpoint": "localhost:4317",
        "skip_verify": true,
        "service_name_prefix": "aistore",
        "sampler_probability": "1.0"
    }
}
```

### Build AIStore with tracing

Distributed tracing is a build-time option controlled by the `oteltracing` build tag.

When the `aisnode` binary is built without this tag, tracing configuration is ignored and tracing operations are no-ops.

```console
# build with tracing support
TAGS=oteltracing make node

# build without tracing support
make node
```
