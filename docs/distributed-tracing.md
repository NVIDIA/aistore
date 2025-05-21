AIStore supports distributed tracing via OpenTelemetry (OTEL), enhancing its observability capabilities alongside existing extensive [metrics and logging features](/docs/monitoring-overview.md).
Distributed tracing enables tracking client requests across AIStore's proxy and target daemons, providing better visibility into the request flow and offering valuable performance insights

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

In this section, we use AIStore [Local Playground](/docs/getting_started.md#local-playground) and local [Jaeger](https://www.jaegertracing.io/). This is done for purely (easy-to-use-and-repropduce) demonsration purposes.


> #### Pre-Requisite
> - Docker

1. Local Jaeger setup
    ```sh
    docker run -d --name jaeger \
    -e COLLECTOR_OTLP_ENABLED=true \
    -p 16686:16686 \
    -p 4317:4317 \
    -p 4318:4318 \
    jaegertracing/all-in-one:latest
    ```

2. Optionally, shutdown and cleanup [Local Playground](/docs/getting_started.md#local-playground):

    ```sh
    make kill clean
    ```

3. Deploy the cluster with AuthN enabled:
   ```sh
   AIS_TRACING_ENDPOINT="localhost:4317" make deploy
   ```

   This will start up an AIStore cluster with distributed-tracing enabled.

### Example operations

```sh
ais bucket create ais://nnn
ais put README.md ais://nnn
ais get ais://nnn/README.md /dev/null
```

View traces at: [http://localhost:16686](http://localhost:16686/)

## Configuration

Cluster-wide `tracing` configuration. For list of AIStore config options refer to [configuration.md](/docs/configuration.md).

| Option name | Default value | Description |
|---|---|---|
| `tracing.enabled` | `false` | If true, enables distributed tracing |
| `tracing.exporter_endpoint` | `''` | OTEL exporter gRPC endpoint |
| `tracing.service_name_prefix` | `aistore` | Prefix added to OTEL service name reported by exporter |
| `tracing.attributes` | `{}` | Extra attributes to be added the traces |
| `tracing.sampler_probablity` | `1` (export all traces) | Percentage of traces to sample [0,1] |
| `tracing.skip_verify` | `false` | Allow insecure (TLS) exporter gRPC connection |
| `tracing.exporter_auth.token_header` | `''` | Request header used for exporter auth token |
| `tracing.exporter_auth.token_file` | `''` | Filepath to obtain exporter auth token |


Sample aistore cluster configuration:

```json
{
    ...
    "tracing": {
        "enabled": true,
        "exporter_endpoint": "localhost:4317",
        "skip_verify": true,
        "service_name_prefix": "aistore",
        "sampler_probability": "1.0"
    },
    ...
}
```

### Build AIStore with tracing

Distributed tracing is a build-time option controlled using *oteltracing* build tag.

When `aisnode` binary is built without this build tag, tracing configuration is ignored and the entire tracing functionality becomes a no-op.

```console
# build with tracing support
TAGS=oteltracing make node

# build without tracing support
make node
```
