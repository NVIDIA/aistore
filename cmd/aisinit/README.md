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
   values in [`config.go`](config.go), hydrates canonical default-omittable
   sections, applies the deployment-provided `ConfigToSet` override, validates
   and re-canonicalizes the merged default-omittable sections, removes private
   authentication material, and prunes wholly default sections before writing
   the resulting sparse configuration.

Hydration before the merge is essential: a partial override must inherit the
remaining canonical values of its section. Pruning afterward avoids copying
those canonical defaults into deployment configuration and lets the running
AIStore version supply its current defaults.

The generated cluster configuration is bootstrap input, not the cluster's
ongoing source of truth. It is used when a node has no persisted cluster
configuration. Once cluster configuration has been persisted, that persisted
state is authoritative; editing or regenerating the initial file does not
update a running cluster.

## Maintaining the hardcoded defaults

The values in `config.go` are production bootstrap defaults, not a duplicate of
every canonical default in `cmn/config.go`. Maintain them by reconciling three
sources:

1. **Canonical in-memory defaults:** values reconstructed by the corresponding
   `cmn` section's `Validate` method.
2. **Production bootstrap defaults:** environment-sensitive values that
   `aisinit` must put into a newly generated configuration.
3. **Deployment overrides:** explicit Operator, Helm, or production-manifest
   settings, including site policy such as authentication, TLS, backends, and
   feature flags.

Do not add a section here merely to spell out canonical defaults. When a
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
- determine whether the value is canonical or environment-specific;
- verify that a generated configuration loads and validates in `aisnode`;
- review upgrade and downgrade implications when a section becomes sparse.

See related:

* [AIStore configuration](https://github.com/NVIDIA/aistore/blob/main/docs/configuration.md)
  - in particular, section [Configuration: startup ordering and deployment generators](https://github.com/NVIDIA/aistore/blob/main/docs/configuration.md#startup-ordering-and-deployment-generators)
* [Top-level README](https://github.com/NVIDIA/aistore/blob/main/cmd/README.md)

## Build and deployment

The image definition lives in
[`deploy/prod/k8s/aisinit_container`](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/k8s/aisinit_container).
The binary is built with:

```console
make aisinit
```

In Kubernetes manifests, `aisinit` runs as an init container and writes the
generated files into a volume shared with the subsequent `aisnode` container.
