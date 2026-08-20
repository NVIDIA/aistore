# Node-Join Authentication

Normally, any AIStore (AIS) intra-cluster request signing assumes established cluster
membership. A sender's verifying key is obtained from the cluster map (Smap),
and the Smap itself is trusted cluster metadata.

Node join necessarily happens before those assumptions hold. A prospective
node is not yet present in the Smap, so the primary cannot use the ordinary
per-node sign/verify mechanism to establish that the node is authorized to
join. Conversely, the joining node has not yet received authenticated cluster
metadata and must not trust an arbitrary endpoint merely because it claims to
be the primary.

Node joining therefore presents a bootstrap trust problem with two independent
requirements:

1. The primary must authenticate the joining node before admitting it to the Smap.
2. The joining node must authenticate the primary before accepting cluster
   metadata and completing the join.

`auth.intra_cluster.node_join_secret_path` provides the out-of-band trust used
for this mutual authentication. The configured file contains an opaque shared
secret, which is never transmitted: each side proves possession of it over the
exchanged message rather than sending it.

The credential is used only to establish cluster membership. Once a node is
admitted and its verifying key is distributed through the Smap, ordinary
per-node signing protects subsequent intra-cluster requests.

> **Version note:** [v5.0 bridge](/docs/relnotes/5.0.md) accepts and persists `node_join_secret_path`, but does not enforce node-join authentication.
> Enforcement, along with the rest `auth.intra_cluster` capabilities, is enabled in post-5.0 versions.

**Table of Contents**

- [Implementation Status](#implementation-status)
- [Mutual Bootstrap Authentication](#mutual-bootstrap-authentication)
  - [The primary authenticates the joining node](#the-primary-authenticates-the-joining-node)
  - [The joining node authenticates the primary](#the-joining-node-authenticates-the-primary)
- [Protected Boundary](#protected-boundary)
- [Configuration](#configuration)
- [Credential File](#credential-file)
- [Creating a New Cluster](#creating-a-new-cluster)
- [Enabling Protection on an Existing Cluster](#enabling-protection-on-an-existing-cluster)
- [Adding, Restarting, and Administratively Joining Nodes](#adding-restarting-and-administratively-joining-nodes)
- [Kubernetes Provisioning](#kubernetes-provisioning)
- [Force-Joining Distinct Clusters](#force-joining-distinct-clusters)
- [Failure and Recovery](#failure-and-recovery)
- [Future Development](#future-development)
- [References](#references)

## Implementation Status

This document describes both the design, the capabilities, and the current implementation status.

| Capability | Status |
|---|---|
| Startup self-join: node proves possession to the primary | Supported |
| Startup self-join: primary proves possession to the node | Supported |
| Slow keepalive that would admit an absent or restarted node | Supported |
| Credential permission and non-empty checks; fail-closed at startup | Supported |
| Timestamp binding, bounded by `auth.intra_cluster.nonce_window` | Supported |
| Administrative join | Not implemented yet - see [Future Development](#future-development) |
| Force-join (single node or whole cluster) | Not implemented yet - registration is signed, destination metadata is not authenticated |
| Cluster UUID bound into the proof | Not implemented yet |
| Nonce / replay cache | Not implemented yet - see [Future Development](#future-development) |
| Multiple secrets per file; online rotation | Not implemented yet - the file's first line is the only secret |
| Reloading the credential without a restart | Not implemented yet |

## Mutual Bootstrap Authentication

### The primary authenticates the joining node

Possession of a provisioned secret means that a node has been authorized
out of band and is authorized to join that cluster. Before adding an unknown
node to the Smap, the primary requires proof of possession bound to:

- the joining node's identity and the metadata it reports (both are carried in
  the signed registration body);
- the join direction; and
- the proof format and version.

The destination cluster UUID is *not* currently bound into the proof. Until it
is, a secret shared between two clusters authorizes joins in both directions -
see [Future Development](#future-development). Generate a distinct secret per
cluster; with no UUID binding this is a requirement, not defense in depth.

The Ed25519 verifying key supplied by a joining node cannot authenticate its
own admission. An untrusted node can generate a key pair as easily as a
legitimate node. The primary accepts and distributes that key only after the
bootstrap proof succeeds.

### The joining node authenticates the primary

The reverse check is equally important. A joining node does not accept an Smap,
cluster configuration, or other cluster metadata solely because an endpoint
identifies itself as the primary. The primary must prove possession of the
secret provisioned on the joining node, over the exact response body it sends.

Only after that proof succeeds does the node accept the destination metadata
and complete the join. This check is implemented for startup self-join. It is
not yet implemented for administrative join and force-join; see
[Implementation Status](#implementation-status).

The exact HTTP headers, canonical encoding, and retry sequence are
implementation details. They are intentionally outside the configuration and
operational contract described here.

## Protected Boundary

Node-join authentication does not require proxy mediation for client traffic
and is deliberately not part of `AuthConf.RequiresProxyMediation()`.

## Configuration

The setting belongs to the cluster-wide `auth.intra_cluster` section:

```json
{
    "auth": {
        "intra_cluster": {
            "node_join_secret_path": "/var/run/secrets/ais/node-join"
        }
    }
}
```

It can be updated with the CLI:

```console
$ ais config cluster auth.intra_cluster.node_join_secret_path /var/run/secrets/ais/node-join
```

The semantics are:

- An empty path means node-join authentication is not configured.
- A nonempty path identifies a credential file that must be locally accessible
  to the AIS node.
- The pathname is replicated as cluster configuration; the file contents remain
  local, are never persisted, sent, or replicated through AIS metadata.
- Every proxy and target resolves the configured pathname locally. The same
  pathname may refer to separately mounted copies of the same credential.
- All proxies require the credential because any eligible proxy may become the
  primary. Targets and non-primary proxies require it to authenticate a
  primary when they join or rejoin.

The pathname itself is not secret. The file and its contents must be protected
as deployment credentials.

Because the pathname is cluster-wide but the file is local, the credential can
only be validated by each node against its own filesystem. A primary accepting
`ais config cluster ...` cannot verify that the path resolves anywhere else, so
a successful configuration update is not evidence that provisioning is complete.
Verify each node, as described below.

## Credential File

The credential file contains an opaque shared secret. It does not contain the
cluster UUID, which allows Vault, a Kubernetes Secret, or another deployment
system to provision the credential before the AIS cluster exists.

For example:

```text
3b8ce35ef769159a904fe47a7d9b87ddc3e260d65c09a4a59c301f0672bbf5a2
```

Use a cryptographically random secret. A generated hexadecimal or base64url
value representing at least 256 random bits is recommended. Do not use a
password, cluster name, Kubernetes namespace, or other predictable value.

Only the first line is currently read. Its trailing newline is removed without
other trimming; any subsequent lines are ignored.

The proof is a keyed MAC over the message being authenticated:

```text
sig = base64url( HMAC-SHA256(secret, domain | 0x00 | unixTimestamp | 0x00 | body) )
```

carried in two headers:

```text
Ais-Join-Time: <unix seconds>
Ais-Join-Sig:  <base64url, unpadded>
```

with the direction fixed by the domain string:

```text
request  (node => primary)  "ais-self-join-v1"
response (primary => node)  "ais-self-join-response-v1"
```

Distinct domains prevent either directional proof from being reflected as the
other. Fields are NUL-separated in fixed order so that no two distinct inputs
produce the same MAC input; the vertical bars above are notation, the `0x00` is
literal. `body` is the exact registration or response payload, byte for byte -
the proof therefore covers the joining node's identity and everything else it
reports.

The timestamp is bound into the MAC and verified against
`auth.intra_cluster.nonce_window` (default 1m, max 10m), tolerating skew in
either direction. There is no nonce and no replay cache: an observed proof can
be replayed within the window, so protect the join exchange with TLS.

Generate a different secret for every cluster. Because the cluster UUID is not
currently bound into the proof, a secret shared between two clusters authorizes
each to produce proofs the other will accept.

Never write a secret to logs, error responses, cluster metadata, or command
output.

The file must be accessible by the AIS process and by nobody else: typically,
K8s default mode `0400` owned by the account the node runs as. A credential that is
group- or world-accessible is rejected rather than used - a permissive mode is
far more often an accident of provisioning than a deliberate choice.

## Creating a New Cluster

Do not configure `node_join_secret_path` in the initial configuration of a
brand-new cluster.

Initial formation must take place on a trusted or isolated network.

Historically the reason was that AIS generates the cluster UUID during primary
startup, after collecting initial node registrations, leaving no UUID to bind
into a proof. Since the UUID is not currently bound into the proof at all, that
particular obstacle does not apply - but initial formation remains the least
exercised path for this feature, and the admission decisions taken during
early-start registration are not covered by the node-join tests. Treat
enforcement during formation as unvalidated.

The secret file itself may be generated and mounted before cluster formation.
Leave `node_join_secret_path` empty until the cluster has formed.

After the cluster has formed:

1. Wait for initial registration to complete and verify that the cluster is
   stable.
2. Obtain the established UUID - not used by the proof today, but worth
   recording so that per-cluster secrets stay distinguishable:

```console
$ ais show cluster smap --json | grep uuid
```

or, same:

```console
$ ais show cluster | grep "Cluster Map"
```

3. Generate a unique random shared secret, unless it was provisioned earlier.
4. Provision the file at the intended pathname on every proxy and target.
5. Verify that every AIS node can access its local copy.
6. Set `auth.intra_cluster.node_join_secret_path` only after provisioning has
   completed everywhere.

This protects subsequent admissions. It does not retroactively authenticate
the nodes admitted during initial cluster formation.

Note that this section is about forming a *brand-new cluster*. A new *node*
joining an already-formed cluster is the opposite case: it must be provisioned
with the credential before it starts - see
[Adding, Restarting, and Administratively Joining Nodes](#adding-restarting-and-administratively-joining-nodes).

## Enabling Protection on an Existing Cluster

Use the following order for an existing cluster:

1. Identify the cluster and record its UUID for your own bookkeeping.
2. Generate a cryptographically random shared secret, distinct from every other
   cluster's.
3. Create a credential file containing the secret.
4. Provision the credential file on every proxy and target, including standby
   or temporarily offline nodes that are expected to return.
5. Verify the local pathname and file contents on every node.
6. Update `node_join_secret_path` through the cluster configuration API or CLI.
7. Confirm the effective configuration on all nodes.
8. Do not rely on enforcement until every node is running a version that
   supports it.

Provision first and configure second. Reversing that order can prevent a node
from rejoining after a restart and can leave a newly elected primary unable to
authenticate admissions.

## Adding, Restarting, and Administratively Joining Nodes

Before starting a new node, provision both:

- bootstrap configuration containing the same `node_join_secret_path`; and
- a credential file containing the destination cluster's shared secret.

The joining node cannot learn the pathname only from post-join cluster
configuration: it needs the credential to authenticate the primary before it
can safely accept that configuration.

A restarted member follows the same rule. Its previous Smap membership does
not replace possession of the admission credential when the primary must admit
it again or renew its ephemeral verifying key.

Administrative authorization and node authentication protect different
actors. An administrative token authorizes the operator to request an
admin-join; the shared secret authenticates the candidate node and destination
primary to each other. Administrative authorization must not substitute for
node proof.

## Kubernetes Provisioning

Kubernetes Secrets are a natural way to provision the credential without
placing it in a ConfigMap or AIS cluster metadata. A typical rollout is:

1. Form the new AIS cluster without `node_join_secret_path`, or obtain the UUID
   of an existing cluster.
2. Create a Kubernetes Secret containing the opaque credential file.
3. Project the Secret read-only into every proxy and target pod at a consistent
   pathname.
4. Wait until the Secret is mounted and readable everywhere.
5. Update the AIS cluster configuration with the mounted pathname.
6. Add, restart, or roll nodes only after the preceding steps complete.

Two Kubernetes specifics are easy to miss and both are load-bearing:

- **`defaultMode`.** A Secret volume mounts `0644` unless told otherwise. Set
  `defaultMode: 0400` explicitly; a default mount produces a credential that
  fails the permission check.
- **Do not use `subPath`.** A `subPath`-mounted file is copied once at pod
  start and never refreshed.

Kubernetes projected Secret files commonly use symlinks internally. File
validation must follow those symlinks rather than rejecting the projected file
because of its representation.

The [AIS Kubernetes operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator) should preserve the same ordering: create or update
the Secret, update pod mounts, confirm availability, and only then update the
AIS cluster configuration.

## Force-Joining Distinct Clusters

> **Not yet enforced.** Force-join registrations are signed like any other
> registration, but the destination's cluster metadata is not authenticated by
> the source nodes, and the prepare phase does not preflight source credentials.
> The requirements below are the intended contract; do not rely on them as a
> control today.

When an entire source cluster joins a destination cluster with a different
UUID, every source node must be provisioned with the destination credential
before force-join begins. A file containing both clusters' secrets would allow
the source cluster to remain recoverable while its nodes prepare to authenticate
to the destination - this depends on multi-secret files, which are not yet
implemented.

The destination cluster's requirements are authoritative:

- the source primary and every source member must authenticate using the
  destination UUID and secret;
- source nodes must authenticate the destination primary before accepting its
  metadata;
- the prepare phase must detect missing or mismatched destination credentials
  before changing local Smap or cluster configuration; and
- `--force` does not bypass destination authentication.

If the destination uses a different `node_join_secret_path`, provision the
destination pathname on every source node before starting the operation.

The same requirement applies to forcing a *single* node to join a different
cluster. That path pushes cluster metadata directly at the node, so it is the
node-authenticates-the-primary requirement in its most exposed form: without a
proof, an endpoint that merely claims to be a primary can hand a node a new
Smap, BMD and cluster configuration.

## Failure and Recovery

When node-join authentication is configured, admission fails closed if the
credential file is missing, unreadable, malformed, or holds a secret that does
not match the presented proof. Error messages may identify the
pathname, UUID, or failure category, but must never include secret material.

A node with misconfigured path (inaccessible, too permissive, or empty content) - will fail to start.

An already running member does not use this credential for ordinary client or
intra-cluster traffic. Losing the file therefore does not by itself evict that
member, but it prevents future authenticated joins and may prevent the node
from returning after a restart. Every proxy must retain the credential so that
primary failover does not disable admission.

If all usable copies of the credential are lost, recovery requires an explicit
administrative decision. An authorized administrator may clear
`node_join_secret_path`, temporarily returning admission to the unprotected
mode, then provision a new credential and re-enable it. Perform this recovery
on an isolated network and audit it as a security-sensitive configuration
change. Join and force-join requests themselves never provide a hidden bypass.

## Future Development

Ordered roughly by how much each one closes a gap between this document and the
implementation. See also [Implementation Status](#implementation-status).

**Credential reload API.** The secret is read once, at node startup, and held
for the process lifetime. Changing `node_join_secret_path` through
`ais config cluster ...` persists the new pathname but has no effect until every
node restarts.

**Ordered multi-secret credential files.** First secret signs, all secrets
verify. Required for any rotation that does not stop admission, and for staging
a force-join between clusters with different secrets.

**Cluster UUID binding.** Include the destination UUID in the MAC input so that
a proof is valid only against the cluster it was made for. This restores the
property that most of this document was originally written around, and it
removes secret reuse across clusters as a cross-authorization hazard.

**Nonce and replay cache.** The timestamp bounds replay to
`auth.intra_cluster.nonce_window`; it does not prevent it. Add a verifier-chosen
nonce where the round trip allows it, plus a short-lived seen-nonce cache.

**Administrative join.** Authenticate the candidate node and the destination
primary to each other, in addition to the administrative authorization that
already gates the operation.

**Force-join.** Authenticate destination metadata before a source node applies
it, and preflight every source node's credential during the prepare phase, so
that a missing credential is detected before local cluster map or configuration
changes.

## References

For related security boundaries, see [Authentication and Authorization](/docs/auth_validation.md).
For node lifecycle operations, see [Node Lifecycle](/docs/lifecycle_node.md).
