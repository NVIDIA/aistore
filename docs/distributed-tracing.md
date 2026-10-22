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

Start with a new [Local playground](/docs/getting_started.md#local-playground) and a **TLS-enabled OTLP/gRPC collector** reachable from every AIS node. Configure the collector and trace UI using the [Jaeger documentation](https://www.jaegertracing.io/docs/latest/) or your collector's guide. A plaintext collector does not work with the local deployment helper's TLS settings.

From the repository root, build the CLI and deploy 1 target and 1 proxy with the exporter endpoint set to your TLS-enabled collector. Replace `localhost:4317` if the collector is not reachable at that address from **each** node:

```sh
export AIS_ENDPOINT=http://localhost:8080
TAGS=oteltracing AIS_TRACING_ENDPOINT="localhost:4317" \
  make cli deploy <<< $'1\n1'
```

Setting `AIS_TRACING_ENDPOINT` makes the local deployment helper enable tracing and generate `tracing.skip_verify: true`. The exporter uses TLS with certificate verification disabled. Use this setting only for isolated tests; see [Configuration](#configuration) for verified TLS.

### Example operations

With `AIS_ENDPOINT` pointed at the proxy and the CLI available, run these example operations from the repository root using a new test bucket name in place of `nnn`:

```sh
ais create ais://nnn
ais put README.md ais://nnn
ais get ais://nnn/README.md /dev/null
```

Check the collector's trace UI for AIS proxy and target spans. If none appear, check collector reachability, TLS settings, exporter logs, and sampling.

## Configuration

The `tracing` configuration is cluster-wide. For other AIStore options, refer to [configuration.md](/docs/configuration.md). Tracing configuration changes require a daemon restart to take effect.

For verified collector TLS, set `tracing.skip_verify=false` and leave `OTEL_EXPORTER_OTLP_INSECURE` and `OTEL_EXPORTER_OTLP_TRACES_INSECURE` unset or false. The exporter uses system-trusted CAs by default. For a private CA, set `OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE=/absolute/path/collector-ca.crt` in every AIS daemon's environment before starting or restarting it. This is an [OpenTelemetry exporter setting](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc), separate from AIS HTTP client TLS settings. `tracing.skip_verify=true` overrides the exporter's CA configuration.

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


Example `tracing` section for a TLS-enabled collector, with certificate verification enabled:

```json
{
    "tracing": {
        "enabled": true,
        "exporter_endpoint": "localhost:4317",
        "skip_verify": false,
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
