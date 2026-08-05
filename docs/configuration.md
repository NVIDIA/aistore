# AIStore Configuration

AIStore configuration is both an operator interface and a versioned part of cluster metadata. Operators inspect and change settings through the CLI. Developers additionally need to know how defaults are filled in, how partial updates are merged, and why what AIStore writes to disk is smaller than what it shows you.

- [For users and operators](#for-users-and-operators) - how to see and change settings, what to expect in production, and what to do when a change doesn't behave.
- [For developers](#for-developers) - the Go data model, validation and defaulting rules, sparse persistence, scope enforcement, and the checklist for adding configuration.

For the complete CLI syntax, see [CLI: configuration](/docs/cli/config.md). This page explains the model behind those commands.

## Contents

- [For users and operators](#for-users-and-operators)
  - [Cluster and node configuration](#cluster-and-node-configuration)
  - [Viewing configuration](#viewing-configuration)
  - [Changing configuration](#changing-configuration)
  - [Node overrides](#node-overrides)
  - [Transient updates](#transient-updates)
  - [Resetting configuration](#resetting-configuration)
  - [Worked example: change a setting end to end](#worked-example-change-a-setting-end-to-end)
  - [What AIStore stores on disk](#what-aistore-stores-on-disk)
  - [Troubleshooting](#troubleshooting)
  - [Deploying: initial and local configuration](#deploying-initial-and-local-configuration)
  - [Startup overrides](#startup-overrides)
  - [Managing mountpaths](#managing-mountpaths)
  - [Reducing extended-attribute usage](#reducing-extended-attribute-usage)
  - [Backup and upgrade](#backup-and-upgrade)
  - [Production checklist](#production-checklist)
- [For developers](#for-developers)
  - [Data model and update paths](#data-model-and-update-paths)
  - [Adding a field or section](#adding-a-field-or-section)
  - [Defaults and validation](#defaults-and-validation)
  - [Making a section default-omittable](#making-a-section-default-omittable)
  - [Pointerizing an existing section](#pointerizing-an-existing-section)
  - [Default-enabled sections](#default-enabled-sections)
  - [Scope, transient updates, and cross-section checks](#scope-transient-updates-and-cross-section-checks)
  - [Startup ordering and deployment generators](#startup-ordering-and-deployment-generators)
  - [Compatibility and test checklist](#compatibility-and-test-checklist)
- [Related documentation](#related-documentation)

---

## For users and operators

### Cluster and node configuration

Every node in an AIStore cluster has two kinds of configuration:

- **Cluster configuration** - shared by the whole cluster: checksums, timeouts, logging, capacity watermarks, transport, rebalance, erasure coding, and other named sections. The primary proxy owns it. Changing it changes every node.
- **Local configuration** - belongs to one node and necessarily differs between nodes: its directories, its network listeners, and (on targets) its disks. Read from a plain-text file at startup; not changeable through the configuration CLI.

Two smaller mechanisms sit on top:

- **Node overrides** - one node running a different value for an otherwise cluster-wide setting. Useful for diagnostics; deliberately makes that node different.
- **Transient updates** - a change applied in memory only, meant to be short-lived. Read [Transient updates](#transient-updates) before using one; the name promises slightly less than it sounds like.

What a node actually runs:

```text
effective settings = cluster configuration + this node's overrides + this node's local configuration
```

Some sections are **cluster-only** and cannot be overridden per node: `auth`, `backend`, `proxy`, `checksum`, `tcb`, `tco`, `arch`, `lso`, `rebalance`, `ec`, `get_batch`, `net`, `transport`, `chunks`, `mirror`, `periodic`, `features`, `versioning`. Attempting a node override of one of these fails with a clear error.

### Viewing configuration

Whole cluster configuration, flat or JSON:

```console
$ ais config cluster
$ ais config cluster --json
```

One section:

```console
$ ais config cluster log
$ ais config cluster get_batch --json
```

What one node is actually running, including its overrides:

```console
$ ais show config t[ABC123] inherited
$ ais show config t[ABC123] inherited log
```

In the flat inherited output, the `DEFAULT` column shows the cluster value whenever a node override differs from it; `-` means the node is simply inheriting.

That node's own local configuration - directories, listeners, disks:

```console
$ ais show config t[ABC123] local
$ ais show config t[ABC123] local host_net --json
```

Local configuration can be viewed but not set through the CLI. To change it, edit the node's plain-text local file and restart that node.

> Everything these commands print is the complete effective configuration. See [What AIStore stores on disk](#what-aistore-stores-on-disk) for why the files are smaller.

### Changing configuration

Cluster-wide, persistent (the default):

```console
$ ais config cluster periodic.stats_time=1m
$ ais config cluster periodic.stats_time=1m disk.iostat_time_long=4s
```

Multiple settings in one command are applied together as a single new configuration - either all of them take effect or none do. The primary validates the whole proposed configuration first, so a change can be rejected because of how it combines with another setting, not only because the value itself is out of range.

Most changes take effect immediately. Five do not, and are applied only after a restart:

```text
memsys, net, tracing, timeout.cplane_operation, timeout.max_keepalive
```

The CLI warns you when your change touches one of them. Depending on scope, restart the affected node or the cluster.

The CLI also accepts a whole section as JSON. Quote it so the shell leaves it alone:

```console
$ ais config cluster checksum.type=md5
$ ais config cluster checksum='{"type":"md5","validate_warm_get":true}'
```

Use the plain `name=value` form unless you are setting several fields of one section at once.

### Node overrides

To make one node differ:

```console
$ ais config node t[ABC123] log.level=4
$ ais config node t[ABC123] disk.disk_util_low_wm=40 disk.disk_util_high_wm=90
```

The override is stored on that node and reapplied every time the node receives a new cluster configuration - so if the same setting later changes cluster-wide, this node keeps its own value until you reset it.

Overriding a cluster-only section fails:

```console
$ ais config node t[ABC123] periodic.stats_time=1m
Error: periodic configuration can only be globally updated
```

Node overrides are a diagnostic tool. A node running different watermarks or timeouts than its peers will behave differently under load, and that difference is invisible unless you go looking for it. Prefer a cluster-wide change.

### Transient updates

Both cluster and node commands accept `--transient`:

```console
$ ais config cluster log.level=4 --transient
$ ais config node t[ABC123] log.level=4 --transient
```

A transient cluster update reaches every node that is currently running, but does not create a new cluster configuration version. A node that restarts or joins later will not have it.

Some settings cannot be changed transiently at all, and the request is rejected: `auth`, `net`, `tracing`, `memsys`, `keepalivetracker`, `timeout.max_keepalive`, `timeout.cplane_operation`, and any cluster-only section.

#### Transient values can become permanent

`--transient` means the *current* command doesn't write to disk. It does not mean the value is quarantined. Transient values are merged into the same in-memory override set that a later persistent operation writes out.

```console
# 1. a quick experiment
$ ais config node t[ABC123] log.level=5 --transient

# 2. later, an unrelated persistent change to the same node
$ ais config node t[ABC123] disk.iostat_time_long=4s

# 3. the experiment is now permanent - it was written along with the disk change
$ ais show config t[ABC123] inherited log
PROPERTY         VALUE   DEFAULT
log.level        5       3
```

The same thing happens if the node persists a runtime mountpath change while a transient value is live.

So: use `--transient` for short experiments, and before making any persistent change to that node, either check its inherited configuration and clear what you don't want, or `ais config reset` the node first.

### Resetting configuration

Drop one node's overrides and return it to the cluster configuration:

```console
$ ais config reset t[ABC123]
```

Drop overrides on every currently active node:

```console
$ ais config reset cluster
```

Because the override file is per-node local metadata, the primary cannot clear it on a node that is unreachable at the time. A node that was down comes back with its overrides intact - re-run the reset, or reset that node individually, once it rejoins.

Neither command resets the *cluster* configuration to AIStore's built-in defaults - they remove node-level divergence only.

> **Mountpath note:** runtime `fspaths` changes are stored in the same per-node file as overrides, so a reset removes that record too. Active mountpaths are not detached immediately, but after a restart the target falls back to the `fspaths` in its plain-text local file. If runtime mountpath changes need to survive both, keep the deployment-managed local file in sync.

### Worked example: change a setting end to end

```console
# 1. what is it now?
$ ais config cluster space --json
{
    "cleanupwm": 65,
    "lowwm": 75,
    "highwm": 90,
    "out_of_space": 95,
    "batch_size": 32768,
    "dont_cleanup_time": "30m"
}

# 2. change it
$ ais config cluster space.highwm=88
Config has been updated successfully.

# 3. confirm - cluster-wide
$ ais config cluster space.highwm
PROPERTY        VALUE
space.highwm    88

# 4. confirm - what one target is actually running
$ ais show config t[ABC123] inherited space
PROPERTY                  VALUE     DEFAULT
space.cleanupwm           65        -
space.lowwm               75        -
space.highwm              88        -
space.out_of_space        95        -
space.dont_cleanup_time   30m       -

# 5. what got written to disk on that node
#    (.ais.conf is protected metadata, not raw JSON - extract it first)
$ xmeta -x -in=/etc/ais/.ais.conf -out=/tmp/ais-conf.json
$ jq .space /tmp/ais-conf.json
{
  "cleanupwm": 65,
  "lowwm": 75,
  "highwm": 88,
  "out_of_space": 95,
  "batch_size": 32768,
  "dont_cleanup_time": "30m"
}
```

The `space` section is stored in full, because it no longer matches the built-in defaults. Look at the same file for a section you haven't touched - `periodic`, say - and it isn't there at all. That's the part that surprises people, and it's explained next.

### What AIStore stores on disk

**What you see in `ais config cluster` is the complete, effective configuration. What AIStore writes to disk is smaller: fully default-valued, default-omittable sections are not written out, and AIStore fills them back in when it loads.**

That is the whole idea, and it exists for three reasons: persisted and metasynced configuration stays small, clusters pick up *current* defaults instead of carrying values that merely happened to be the default years ago, and there's exactly one place in AIStore that owns each default value.

Pruning is all-or-nothing per section. A section is dropped only when *every* field in it matches the canonical default; change one field and the whole section is written out.

Practical consequences:

- **A section missing from `.ais.conf` is not off, and its values are not zero.** It's at defaults.
- **Any section whose effective value differs from its canonical default is kept** - including an explicit disable of a default-enabled subsystem.
- **Never read `.ais.conf` to find out what a setting is.** Use the CLI or the API. It is protected metadata rather than plain JSON, and it is an internal representation.
- **Defaults can change between AIStore releases.** Either way, check the release notes and set the desired value again if it differs from the new default. A value equal to the current canonical default cannot be pinned by explicitly setting it; it may be pruned again.

The sections AIStore can reconstruct this way, as of v5.0 (22 in total; the same set is listed again under [Default-enabled sections](#default-enabled-sections), where `expectedOmittable` in `cmn/prune_defaults_internal_test.go` is the authoritative source):

```text
arch          chunks        client        checksum      disk
downloader    ec            fshc          get_batch     keepalivetracker
log           lru           lso           mirror        periodic
rate_limit    rebalance     space         tcb           tco
transport     write_policy
```

Sections not on that list are not removed by default-pruning. In particular, `memsys` and the network and bootstrap settings remain explicit because their correct values depend on the machine and deployment. A 32 GiB development box and a target with terabytes of RAM should not share one `memsys` default.

Independently, optional sections such as `tracing`, `distributed_sort`, and `ext` may be absent through their normal serialization rules. Their absence is unrelated to default pruning.

The configuration APIs and the CLI always return the complete configuration, with sensitive values redacted.

### Troubleshooting

**"My node update returned success but nothing changed."**
Check whether the section is cluster-only - those fail with `... can only be globally updated`. If the setting came from a persisted override file rather than the command line (for instance after an upgrade in which the section became cluster-only), it is skipped with a warning in the node log rather than an error. Grep the node log for `ignoring node override for cluster-scoped config`.

**"My change didn't survive a restart."**
It was transient. Transient changes are never written to disk by the command that makes them. Reapply it without `--transient`.

**"A value I set transiently is still there, and I never made it permanent."**
A later persistent operation on that node wrote the whole merged override set, including your transient value. See [Transient values can become permanent](#transient-values-can-become-permanent). `ais config reset NODE_ID` clears it.

**"After upgrading, a setting reverted."**
Two possibilities. The default for that setting changed in the new release and your cluster was sitting at the old default (which is not stored, so there was nothing to preserve). Or you had explicitly set the value that used to be the default, and it was pruned as such. Either way: check the release notes and re-set the value explicitly.

**"`.ais.conf` doesn't contain the section I configured."**
Expected - see [What AIStore stores on disk](#what-aistore-stores-on-disk). Verify with `ais config cluster SECTION --json`.

**"One target behaves differently from the others."**
Compare `ais show config t[ID] inherited` against `ais config cluster`. In the flat output, any row with a value in the `DEFAULT` column is an override on that node.

### Deploying: initial and local configuration

A node starts with two files:

```console
$ aisnode -config=/etc/ais/ais.json \
    -local_config=/etc/ais/ais_local.json \
    -role=target
```

- `-config` is the **initial** cluster configuration: bootstrap input, used only when this node has no persisted cluster configuration yet. It may be shared as a deployment template.
- `-local_config` describes this node: its directories, its listeners, and on targets its `fspaths`.

**The initial file is not the source of truth.** Once a node has a persisted cluster configuration, editing the initial file changes nothing - not the running cluster, and not later restarts of that node. New and restarting nodes get the current configuration from the cluster when they join. Use the CLI or API to change a deployed cluster; do not hand-edit `.ais.conf` or `.ais.override_config`.

Starting with v5.0, the public listener must differ from both intra-cluster listeners; intra-control and intra-data may share one. See [Networking](/docs/networking.md).

#### Sections you can leave out of the initial file

Any section on the list in [What AIStore stores on disk](#what-aistore-stores-on-disk) can be omitted entirely, and AIStore will supply current defaults. Keep explicit: bootstrap-dependent settings, sections AIStore does not reconstruct, and every intentional non-default choice.

#### Sections that are on by default need care

`rebalance` and `fshc` are enabled by default. Omitting the section, or writing an empty one, gives you all defaults including `enabled: true`:

```json
{
    "rebalance": {},
    "fshc": {}
}
```

But if you customize one field and don't mention `enabled`, JSON decoding supplies `false` - and AIStore cannot distinguish that from your deliberately turning the subsystem off. **This silently disables rebalance:**

```json
{
    "rebalance": {"dest_retry_time": "3m"}
}
```

Always spell out `enabled` when you customize either section:

```json
{
    "rebalance": {"enabled": true, "dest_retry_time": "3m"},
    "fshc": {"enabled": true, "error_limit": 3}
}
```

This applies to hand-written initial configuration only. CLI and API updates track separately whether you supplied `enabled`, so `ais config cluster rebalance.dest_retry_time=3m` is safe.

### Startup overrides

`-config_custom` applies node overrides at startup:

```console
$ aisnode -config=/etc/ais/ais.json \
    -local_config=/etc/ais/ais_local.json \
    -role=target \
    -config_custom="client.client_timeout=13s,log.level=4"
```

These are persistent. Add `-transient=true` to keep them in memory only - subject to the same caveat as any transient value ([above](#transient-values-can-become-permanent)).

Avoid passing secrets on the command line, where local process inspection can read them. Use deployment-managed files or the environment's secret mechanism.

Environment variables and other startup flags override specific behaviors; they are not a general substitute for cluster configuration. See [Environment variables](/docs/environment-vars.md) and [`aisnode` command-line arguments](/docs/command_line.md).

### Managing mountpaths

A target [mountpath](/docs/terminology.md#mountpath) is a formatted disk or RAID volume plus a directory AIStore owns for user data and system metadata. In production each mountpath should be a distinct local filesystem on a non-shared device. Mountpath directories cannot be nested.

`fspaths` in the local file defines the startup list. `test_fspaths` partitions one shared filesystem and is for development only.

Use the storage commands rather than editing configuration:

```console
$ ais storage mountpath show
$ ais storage mountpath attach t[ABC123]=/data/nvme1
$ ais storage mountpath disable t[ABC123]=/data/nvme1
$ ais storage mountpath enable t[ABC123]=/data/nvme1
$ ais storage mountpath detach t[ABC123]=/data/nvme1
```

Runtime changes are persisted on the node; the plain-text local file is not rewritten for you. Keep it in sync.

See [CLI: storage and mountpaths](/docs/cli/storage.md#mountpath-and-disk-management) and [Filesystem Health Checker](/docs/fshc.md).

### Reducing extended-attribute usage

For fast, **temporary** storage, AIStore can be configured to skip persisting per-object metadata to extended attributes:

```console
$ ais config cluster checksum.type=none versioning.enabled=true write_policy.md=never
```

Or per bucket:

```console
$ ais bucket props set ais://mybucket write_policy.md=never
```

This does **not** make AIStore independent of extended attributes. Targets still probe for xattr support at startup and still use xattrs at mountpath roots - for the target ID, among other metadata. A filesystem without working xattr support is not a supported deployment.

What it does change: in-memory (a.k.a. *dirty*) metadata does not survive a node reboot.

### Backup and upgrade

The files you pass with `-config` and `-local_config` are not a backup. The initial file doesn't track later changes, and either path may live outside the AIStore configuration directory.

Back up all of the following before an upgrade or disruptive maintenance:

- deployment-managed initial and local configuration files;
- each node's complete AIStore configuration directory; and
- the `.ais.*` metadata files at each target mountpath root - **not** whole mountpaths, which hold user data.

Depending on node role this includes cluster maps, bucket metadata, rebalance state, and node identity. See [System files](/docs/sysfiles.md). Keep backups off-node and restore only with a compatible AIStore version.

**Downgrade is not supported.** Configuration written by a newer release is not valid input to an older binary: starting with v5.0, an older version may refuse to start on a section it doesn't find, or reconstruct a different value for it. This has never been a supported operation in AIStore, but v5.0 makes the failure sharper. Restore a matching backup instead. See [v5.0 release notes](/docs/relnotes/5.0.md).

### Production checklist

| Area | Recommendation |
| --- | --- |
| **Networking** | Configure distinct public and intra-cluster listeners (required as of v5.0); use separate physical networks or VLANs where the workload justifies it. See [Networking](/docs/networking.md). |
| **Storage** | Use one local filesystem per mountpath and verify extended-attribute support. See [Getting started: prerequisites](/docs/getting_started.md#prerequisites). |
| **Filesystem health** | Keep FSHC enabled unless you have a specific reason not to; tune its thresholds for the storage environment. See [FSHC](/docs/fshc.md). |
| **TLS and authentication** | Configure public and intra-cluster TLS deliberately; configure token validation when authentication is enabled. See [HTTPS](/docs/https.md) and [Token validation](/docs/auth_validation.md). |
| **Backends** | Enable only the providers you need; manage credentials through the environment's secret mechanism. See [Backend providers](/docs/providers.md). |
| **Memory** | Size `memsys` for the actual node. Never copy its settings between a development box and a production target. |
| **Performance** | Size file-descriptor limits, networking, and filesystems for the intended workload. See [Performance](/docs/performance.md). |
| **Templates** | Omit only reconstructible sections; keep bootstrap values and intentional choices explicit. |
| **After every upgrade** | Review effective values with `ais config cluster --json` and compare them against the release notes. |
| **Backups** | Back up deployment inputs, configuration directories, and mountpath metadata before upgrades and disruptive maintenance. |
| **Kubernetes** | Use the [ais-k8s](https://github.com/NVIDIA/ais-k8s) operator. |

---

## For developers

### Data model and update paths

Core types live in [`cmn/config.go`](https://github.com/NVIDIA/aistore/blob/main/cmn/config.go):

| Type | Role |
| --- | --- |
| `Config` | One daemon's effective configuration: `LocalConfig` plus `ClusterConfig` |
| `ClusterConfig` | Hydrated runtime cluster settings plus cluster metadata |
| `LocalConfig` | Node directories, listeners, `fspaths`, development storage settings |
| `ConfigToSet` | Presence-aware partial update; leaf fields are pointers, so omitted and explicit-zero stay distinct |
| `<Section>Conf` | Runtime representation of one section |
| `<Section>ConfToSet` | Partial-update representation of the same section |

Property names come from JSON tags joined with a dot - `client.client_timeout`. [`IterFields`](https://github.com/NVIDIA/aistore/blob/main/cmn/iter_fields.go) is the tag-driven recursive traversal behind update, listing, and CLI paths.

Principal paths:

- **Startup:** `cmn.LoadConfig` loads local and persisted-or-initial cluster configuration, hydrates omittable sections, applies `.ais.override_config`, validates.
- **Persistent cluster update:** `setCluCfgPersistent` runs primary-only pre-flight checks; `configOwner.modify` merges into a private copy, validates, versions, prunes, persists, metasyncs.
- **Metasync receive:** decode, hydrate, then reapply node overrides via `GCO.Update`.
- **Node update:** `setConfig` clones the effective config, merges a `ConfigToSet`, validates, updates the override set.
- **Transient cluster update:** `setCluCfgTransient` - `_checkTransient` first, then a daemon-scope update broadcast to active nodes.
- **GET:** always hydrated, never the sparse form.

The invariant:

```text
persisted / metasynced ClusterConfig: sparse
live / API ClusterConfig:             fully hydrated
```

Three functions maintain it, and they are **not** interchangeable:

| Function | Does | Used by |
| --- | --- | --- |
| `allocOmittables()` | Allocates nil sections as zero values. Does **not** default them. | `Config.Validate`, ahead of the recursive validation traversal |
| `HydrateOmittables()` | Allocates **and** validates *every* omittable section, returns error | Decode side, `LoadConfig`, `UpdateClusterConfig` |
| `PruneOmittables()` | Drops sections equal to canonical defaults. In-place. | `globalConfig._encode` only |

`HydrateOmittables` validates every omittable section, not only the absent ones - validators are idempotent, so repeated calls are safe; the additional validation also repairs partially populated sections as a side effect.

`PruneOmittables` must run on a private copy at the persist/metasync boundary. **Never prune the live `GCO` configuration.**

### Adding a field or section

Decide first:

1. Cluster-wide, node-overridable, or truly local?
2. Does zero mean "use the default", or is zero itself a meaningful value?
3. Can the default be derived identically on every node and in every deployment?
4. Safe to change at runtime, or restart-required?
5. Does validation depend on the section alone, the whole `Config`, or live cluster state?

New field:

1. Add it to `<Section>Conf` with the right JSON tag.
2. Add the matching **pointer** field to `<Section>ConfToSet` - required to tell an omitted update from an explicit zero, empty string, or `false`.
3. Put its defaulting and range checks in `Validate`; keep the method idempotent.
4. Update runtime consumers, `String()` helpers, and any read-mostly (`Rom`) accessors.
5. Add validation and update tests.

New section, additionally:

1. Add `<Section>Conf` to `ClusterConfig` and `<Section>ConfToSet` to `ConfigToSet`.
2. Add `allow:"cluster"` if per-node overrides must be rejected.
3. Add an interface guard for its validator.
4. Decide whether it qualifies for default omission - pointerizing alone does not make it omittable. `Tracing` and `Dsort` are the standing examples: both are `*Conf` with `omitempty`, neither implements `defaultOmittable`.
5. Audit clone, startup, persistence, metasync, CLI, deployment generators, and compatibility.

Tags:

| Tag | Effect |
| --- | --- |
| `json:"name"` | Public property name, for JSON and `IterFields` |
| `json:",inline"` | Explicitly promotes an embedded struct during traversal |
| `allow:"cluster"` | Rejects per-node overrides |
| `list:"readonly"` | Visible, but rejects updates |
| `list:"omit"` | Excluded from listing and update traversal |

Never infer presence from a `ToSet` field's Go zero value. The pointer carries presence; the pointee carries the value.

Deprecating a knob: mark it `// Deprecated:` on both the `Conf` and `ToSet` fields, and keep accepting, validating, and hydrating it. `keepalivetracker.retry_factor` is the current example - still functional, deprecated because of its indirect cross-section effect on failure detection, not because it is inert.

### Defaults and validation

`Validate` methods are mutating normalizers as well as validators: they fill in fields whose zero means "default", then range-check.

Every validator must be **idempotent**:

```go
if err := section.Validate(); err != nil {
    return err
}
if err := section.Validate(); err != nil { // must remain valid, and unchanged
    return err
}
```

Required, because an omittable section is validated during hydration and again in the `Config.Validate` traversal.

Treat each zero deliberately. Zero may mean use-the-default, disabled, unlimited, empty, or an ordinary explicit value. Do not mechanically convert. Document sentinels such as `-1`, and make sure partial updates can express every supported value.

**Hydration must precede the merge.** Otherwise, changing one field of an absent section leaves its siblings at Go zero and silently redefines the rest of the section. `Config.UpdateClusterConfig` calls `HydrateOmittables` before `CopyProps` for exactly this reason. This is not theoretical: a node-scoped `disk.disk_util_high_wm=85` replayed onto a still-sparse configuration produced `{0, 85, 0}` and killed the target at restart.

Section-local checks go in `Validate`. Checks needing the whole configuration go in a `contextValidator` or `Config.Validate`. Checks needing live cluster state go in a primary pre-flight - for example the keepalive interval versus `timeout.max_keepalive` comparison in `_checkKalive`.

### Making a section default-omittable

All of the following must hold:

1. `ClusterConfig` stores it as `*SectionConf` with `json:"section,omitempty"`.
2. Validating a zero-valued section fully reconstructs one canonical default.
3. `Validate` is idempotent and correct on already-materialized values.
4. That default is independent of node role, hardware size, topology, environment, and startup order.
5. The section holds only value-typed fields - no maps, slices, pointers, interfaces, functions, or channels.
6. Every runtime path sees a non-nil section after hydration.
7. It implements the private `defaultOmittable` marker and appears in `expectedOmittable` in `cmn/prune_defaults_internal_test.go`.
8. Rolling-upgrade and downgrade behavior is reviewed and documented in the release notes.

The value-type restriction exists because `PruneOmittables` validates a shallow scratch copy: reference fields could alias live state, and nil-versus-empty would defeat stable default comparison.

The deciding question for (2) is **not** whether the section is mechanically simple. It is whether zero means "unset" or is itself a valid operator choice. A setting that is on by default fails this test unless it can be rescued - see [Default-enabled sections](#default-enabled-sections). `versioning` and `resilver` are ineligible under their current plain-bool representation: every field is a bool, so a wholly zero section is indistinguishable from a deliberately disabled one and the sentinel has nothing to key on.

`memsys` is the canonical environmental counterexample: a developer laptop and a target with terabytes of RAM must not share bootstrap tuning, and no single default serves both.

Do not add a section to deployment templates just to spell out canonical defaults. Conversely, do not remove it from `cmd/aisinit`, the local playground, Helm/Operator inputs, or production YAML until the validator reconstructs the exact intended value *for that environment*.

### Pointerizing an existing section

Converting `SectionConf` to `*SectionConf` breaks two things that compile cleanly.

**1. Comparisons silently become pointer comparisons.** Any existing `oldConfig.X != newConfig.X` now compares addresses and is essentially always true. Grep for the section name in comparisons and add the deref:

```go
// ais/tgtcp.go, receiveConfig
if *oldConfig.Space != *newConfig.Space {
    fs.ExpireCapCache()
}
```

Without the `*`, this fires on every configuration metasync.

**2. Any `Config` built or decoded outside the standard paths is now a nil-deref.** A bare `&cmn.Config{}` no longer has usable sections. The decode side is symmetric by design - `_decode`, `_loadMeta`, and `_loadPlain` in `ais/gconfig.go` each end in `HydrateOmittables()`, so every `globalConfig` handed to a caller is complete. **Any other code that constructs or decodes a `Config` must hydrate it itself.** Both bugs of this shape found so far - `remaisClients(cfg.Client)` off a metasync-decoded config, and aisloader's bare `&cmn.Config{}` - were nil panics at runtime, not compile errors.

Then search every startup consumer for the earliest dereference; see [Startup ordering](#startup-ordering-and-deployment-generators).

### Default-enabled sections

A plain Go `bool` cannot distinguish an omitted JSON field from explicit `false`. For a wholly zero section, a validator can establish the default:

```go
if *c == (RebalanceConf{}) {
    c.Enabled = true
}
```

This preserves the three meanings:

- absent section or `{}` → all defaults, including `enabled: true`;
- hydrated default section → prunable;
- explicit `enabled: false` → differs from default, stays persisted.

`ConfigToSet` has no such ambiguity - `Enabled *bool` separates absent, true, and false. Do not replace that pointer with a plain `bool`.

A partially specified initial section is supported **only when `enabled` is explicit**. A customized `rebalance` or `fshc` section that omits `enabled` is unsupported, because omission is indistinguishable from explicit `false` - `{"rebalance":{"dest_retry_time":"3m"}}` is not wholly zero, so the sentinel above cannot rescue it. Documented on the `RebalanceConf` and `FSHCConf` struct definitions and in [the operator section above](#sections-that-are-on-by-default-need-care).

Both sections clear the bar and are omittable. The full set as of v5.0 - 22 sections, with `expectedOmittable` in `cmn/prune_defaults_internal_test.go` as the authoritative list:

```text
arch          chunks        client        checksum      disk
downloader    ec            fshc          get_batch     keepalivetracker
log           lru           lso           mirror        periodic
rate_limit    rebalance     space         tcb           tco
transport     write_policy
```

### Scope, transient updates, and cross-section checks

Scope is enforced from `ClusterConfig` tags inside `_copyProps`, with three distinct outcomes:

| Situation | Outcome |
| --- | --- |
| Daemon-scope update of an `allow:"cluster"` section | Error: `X configuration can only be globally updated` |
| Transient update of an `allow:"cluster"` section | Error: `X (cluster-scoped) configuration cannot be changed transiently` |
| Same, but with `IgnoreScope: true` | Warning to the log, field skipped |

`IgnoreScope: true` is passed only on the two override-*replay* paths - `handleOverrideConfig` at startup and `GCO.Update` on metasync receive - where a persisted override file may legitimately contain a section that has since become cluster-only. It is never set on a live user update. Consequence worth knowing: a section that gains `allow:"cluster"` in a release will have its existing per-node overrides silently dropped after upgrade, with only `ignoring node override for cluster-scoped config` in the log.

When adding a restart-sensitive setting, update `cmn.ConfigRestartRequired`. That list drives a CLI warning; it does not itself reinitialize anything.

Persistent cluster updates run primary-only pre-flight checks in `ais/prxclu.go`. Transient updates do not take that path. When you add a pre-flight check, decide explicitly what the transient path does - either perform the equivalent check on the merged values, or reject the update. `_checkTransient` currently refuses everything in `cmn.ConfigRestartRequired` plus `auth` and `keepalivetracker`, returning `cmn.NewErrUnsupp` (501). **Do not leave the transient path less validated than the persistent one.**

Cross-section checks must compare **post-merge** values. `_checkKalive` is the model: it validates its private copy first - resolving zero-means-default and range-checking - and only then compares both intervals against the proposed `timeout.max_keepalive`. Comparing raw `ConfigToSet` zeroes instead would reject a legitimate `interval=0s` and accept an illegitimate `max_keepalive=0s`.

Never mutate live `GCO` configuration during a pre-flight check.

### Startup ordering and deployment generators

Pointerizing creates an initialization-order obligation: the section must be hydrated before its first dereference.

Most sections are consumed after `handleOverrideConfig` and full validation. `log` is the deliberate exception - startup calls `nlog.SetPost(config.Log.ToStderr, config.Log.MaxSize)` before override handling, which is why `LoadConfig` hydrates omittable sections first. When pointerizing or adding a section, search every startup consumer and verify the earliest safe hydration point.

Once a section is fully default-hydrated:

1. Remove duplicate canonical defaults from `cmd/aisinit`.
2. Remove local-playground defaults only where local behavior is *meant* to match the canonical default.
3. Preserve genuinely environment-specific settings.
4. Update production manifests separately and deliberately - repository code cannot rewrite deployed YAML.

The goal is one owner per default, not a minimal initial file at the cost of deployment intent.

### Compatibility and test checklist

Sparse persistence changes what older binaries receive. Before adding an omittable section, determine how every release in the supported upgrade window treats its absence. An older validator may hydrate the same values, hydrate different values, accept zeros and run with unintended behavior, or refuse to start.

That last one is real and has been reproduced: deploying a pointerized branch and then rolling back to a binary without the corresponding `Validate` produced `FATAL ERROR: invalid ec.data_slices: 0` on every node, because the persisted sparse configuration outlives the binary. Document the outcome in the release notes; do not claim downgrade support.

Coverage for any configuration change should include:

- zero-value hydration into the expected defaults;
- repeated-validation idempotence;
- all-default pruning;
- pruning of an unvalidated zero section;
- survival of every non-default value, including zero-valued siblings;
- sparse encode / decode / hydrate round trip;
- partial override onto an absent or empty section;
- preservation of explicit disable for default-enabled sections;
- deep-clone alias safety for pointerized sections;
- metasync receive-boundary hydration;
- scope and read-only enforcement;
- transient restrictions and pre-flight parity;
- startup from both plain-text initial and persisted sparse configuration;
- release-note updates whenever the omittable set changes.

[`cmn/prune_defaults_internal_test.go`](https://github.com/NVIDIA/aistore/blob/main/cmn/prune_defaults_internal_test.go), the configuration tests under `cmn/tests`, and the metasync tests under `ais` encode these invariants. Extend `expectedOmittable` only as part of the same reviewed change.

## Related documentation

- [CLI: configuration](/docs/cli/config.md)
- [AIStore HTTP API](/docs/http_api.md)
- [AIStore Networking Model](/docs/networking.md)
- [HTTPS and TLS](/docs/https.md)
- [Authentication and token validation](/docs/auth_validation.md)
- [Mountpaths and storage CLI](/docs/cli/storage.md#mountpath-and-disk-management)
- [Filesystem Health Checker](/docs/fshc.md)
- [Backend providers](/docs/providers.md)
- [Performance recommendations](/docs/performance.md)
- [Environment variables](/docs/environment-vars.md)
- [`aisnode` command-line arguments](/docs/command_line.md)
- [System files](/docs/sysfiles.md)
- [v5.0 release notes](/docs/relnotes/5.0.md)

> This page deliberately carries no per-knob reference table. Defaults, ranges, and scope live in the struct tags and `Validate` implementations in `cmn/config.go`; for effective values on a running cluster, use `ais config cluster --json`.
