# `aisinit`

`aisinit` is a Kubernetes init container that runs before `aisnode` in the same
pod. It generates the local and initial cluster configuration files consumed by
`aisnode` at startup; it is neither a general-purpose configuration utility nor
a long-running service.

**The `aisinit` image must match the `aisnode` release it initializes.**

The container performs two related jobs:

1. **Local configuration:** it reads a template, fills in pod-specific
   networking information from the Kubernetes environment, and writes the
   node's local configuration.
2. **Initial cluster configuration:** it starts with the production bootstrap
   values in [`config.go`](config.go), hydrates default-omittable
   sections, applies the deployment-provided `ConfigToSet` override, validates
   and rehydrates the merged default-omittable sections, removes private
   authentication material, and prunes wholly default sections before writing
   the resulting sparse configuration.

Hydration before the merge is essential: a partial override must inherit the
remaining default values of its section. Pruning afterward avoids copying
those defaults into deployment configuration and lets the running
AIStore version supply its current defaults.

The generated cluster configuration is bootstrap input, not the cluster's
ongoing source of truth. It is used when a node has no persisted cluster
configuration. Once cluster configuration has been persisted, that persisted
state is authoritative; editing or regenerating the initial file does not
update a running cluster.

## Local Network Configuration

`aisinit` configures the network settings for each AIS instance based on the provided environment variables.

### Intra-cluster Hostnames

By default, `aisinit` sets the intra-cluster (data and control) hostnames to the pod DNS name.
For target nodes with env var `HOST_NETWORK` set to true and IP [DNS mode](#dns-mode), `aisinit` clears the intra-data value to discover the host IP at runtime.

### DNS Mode

The DNS mode (`AIS_PUBLIC_DNS_MODE`) controls how the public hostname is determined for external client access.
This affects TLS certificate validation since clients connect using this hostname.
This value is set by [publicNetDNSMode](https://github.com/NVIDIA/ais-k8s/blob/main/docs/tls.md#using-an-operator-managed-certificate) in the AIS operator.

Modes:

- **IP (default)**
  - Use host IP address (`status.hostIP`).
  - Simple but requires IP SANs in TLS certs.
  - For clusters which use auto-scaling, consider using Node or Pod modes instead since the host IP will change.
- **Node**
  - Use Kubernetes node name (`spec.nodeName`).
  - Useful when node names are resolvable DNS names (e.g., AWS EC2 private DNS like ip-10-0-1-5.ec2.internal).
- **Pod**
  - Use pod DNS name (e.g. `pod.service.namespace.svc.cluster.local`).
  - Note that with hostNetwork, pod DNS resolves to the host IP.

This mode is used by the operator to set the value for env var `AIS_PUBLIC_HOSTNAME`.
`aisinit` uses this value for the AIS node's public hostname if not superseded by other options.
When `hostPort` is configured, the operator sets `AIS_PUBLIC_HOSTNAME` to the following values for `aisinit` to use:

| Mode           | `AIS_PUBLIC_HOSTNAME`                   |
|----------------|-----------------------------------------|
| `IP` (default) | `status.hostIP`                         |
| `Node`         | `spec.nodeName`                         |
| `Pod`          | `status.hostIP` (not used by `aisinit`) |

> Note: When a public domain is configured, AIS Operator v4.2.0 and later set `AIS_PUBLIC_HOSTNAME` on targets to `<pod>.<domain>`.
> To prevent `aisinit` from replacing this value with the LoadBalancer IP, the operator also sets `ENABLE_EXTERNAL_ACCESS` to `false`, even when external access is enabled in the AIS custom resource spec.
> A future release will change this behavior to set `AIS_PUBLIC_HOSTNAME_OVERRIDE` instead.

### Target Public Hostname

`aisinit` uses the first rule that applies:

1. If `AIS_PUBLIC_HOSTNAME_OVERRIDE` is set, `aisinit` uses its value.
2. If `ENABLE_EXTERNAL_ACCESS` is true, `aisinit` uses the LoadBalancer IP of the target.
3. If `HOST_NETWORK` is true and the DNS mode is `IP`, `aisinit` leaves the public hostname empty. `aisnode` discovers the host IP at startup.
4. If the DNS mode is `Pod`, `aisinit` uses the pod DNS name.
5. If none of the above apply, `aisinit` uses `AIS_PUBLIC_HOSTNAME` as set by the operator in the [section above](#dns-mode).

### Proxy Public Hostname

`aisinit` uses the first rule that applies:

1. If `AIS_PUBLIC_HOSTNAME_OVERRIDE` is set, `aisinit` uses its value.
2. If the DNS mode is `Pod`, `aisinit` uses the pod DNS name.
3. Otherwise, `aisinit` uses `AIS_PUBLIC_HOSTNAME` as set by the operator in the [section above](#dns-mode).

A proxy ignores `ENABLE_EXTERNAL_ACCESS` and `HOST_NETWORK`.

### Hostname Map

When `--hostname_map_file` is set, `aisinit` looks up the public hostname in the map.
This applies to targets and proxies.
If the map has no entry for the hostname or the entry is empty, the hostname does not change.

## Maintaining the Hard-coded Defaults

The values in `config.go` are production bootstrap defaults, not a duplicate of
every AIStore default in `cmn/config.go`. Maintain them by reconciling three
sources:

1. **Current AIStore defaults:** values reconstructed by the corresponding
   `cmn` section's `Validate` method.
2. **Production bootstrap defaults:** environment-sensitive values that
   `aisinit` must put into a newly generated configuration.
3. **Deployment overrides:** explicit Operator, Helm, or production-manifest
   settings, including site policy such as authentication, TLS, backends, and
   feature flags.

Do not add a section here merely to spell out AIStore defaults. When a
`ClusterConfig` section is pointer-backed, implements `defaultOmittable`, and
its zero value validates to the complete intended default, leave it absent:
`aisnode` will hydrate it. This keeps a single owner for the default and avoids
drift between `cmn`, this container, and deployment manifests.

Conversely, do not remove a value section or setting merely because a similar
value exists elsewhere. Keep it here when the intended bootstrap value depends
on the deployment environment or cannot be reconstructed unambiguously from a
Go zero value. `memsys` is the clearest example: production nodes and developer
machines require different memory tuning.

When changing configuration defaults:

- compare `config.go` with current production configuration and deployment
  overrides;
- determine whether the value is an AIStore default or environment-specific;
- verify that a generated configuration loads and validates in `aisnode`;
- review upgrade and downgrade implications when a section becomes sparse.

See related:

* [AIStore configuration](https://github.com/NVIDIA/aistore/blob/main/docs/configuration.md)
  - in particular, section [Configuration: startup ordering and deployment generators](https://github.com/NVIDIA/aistore/blob/main/docs/configuration.md#startup-ordering-and-deployment-generators)
* [Top-level README](https://github.com/NVIDIA/aistore/blob/main/cmd/README.md)

## Build and Deployment

The image definition lives in
[`deploy/prod/k8s/aisinit_container`](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/k8s/aisinit_container).
The binary is built with:

```console
make aisinit
```

In Kubernetes manifests, `aisinit` runs as an init container and writes the
generated files into a volume shared with the subsequent `aisnode` container.
