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
for this mutual authentication. The configured file contains one or more
opaque shared secrets. A secret is not annotated with the cluster UUID in the
file; instead, each proof binds it to the UUID as part of the authenticated
input. The secret itself is never transmitted.

The credential is used only to establish cluster membership. Once a node is
admitted and its verifying key is distributed through the Smap, ordinary
per-node signing protects subsequent intra-cluster requests.

> **Version note:** AIS v5.0 accepts and persists
> `node_join_secret_path`, but does not read the file or enforce node-join
> authentication. Enforcement is intended for v5.1 and requires every node in
> the cluster to support it. Do not rely on this setting during a mixed-version
> rollout.

**Table of Contents**

- [Mutual Bootstrap Authentication](#mutual-bootstrap-authentication)
  - [The primary authenticates the joining node](#the-primary-authenticates-the-joining-node)
  - [The joining node authenticates the primary](#the-joining-node-authenticates-the-primary)
- [Protected Boundary](#protected-boundary)
- [Configuration](#configuration)
- [Credential File](#credential-file)
- [Version Behavior](#version-behavior)
- [Creating a New Cluster](#creating-a-new-cluster)
- [Enabling Protection on an Existing Cluster](#enabling-protection-on-an-existing-cluster)
- [Adding, Restarting, and Administratively Joining Nodes](#adding-restarting-and-administratively-joining-nodes)
- [Kubernetes Provisioning](#kubernetes-provisioning)
- [Secret Rotation](#secret-rotation)
- [Force-Joining Distinct Clusters](#force-joining-distinct-clusters)
- [Failure and Recovery](#failure-and-recovery)
- [References](#references)

## Mutual Bootstrap Authentication

### The primary authenticates the joining node

Possession of a provisioned secret means that a node has been authorized
out of band and is authorized to join that cluster. Before adding an unknown
node to the Smap, the primary requires proof of possession bound to:

- the destination cluster UUID;
- the joining node's identity;
- the join direction; and
- the proof format and version.

The Ed25519 verifying key supplied by a joining node cannot authenticate its
own admission. An untrusted node can generate a key pair as easily as a
legitimate node. The primary accepts and distributes that key only after the
bootstrap proof succeeds.

### The joining node authenticates the primary

The reverse check is equally important. A joining node does not accept an Smap,
cluster configuration, or other cluster metadata solely because an endpoint
identifies itself as the primary. The primary must prove possession of one of
the secrets provisioned on the joining node, with the advertised cluster UUID
included in the proof.

Only after that proof succeeds does the node accept the destination metadata
and complete the join. This requirement applies to startup self-join,
administrative join, and force-join workflows.

The exact HTTP headers, canonical encoding, and retry sequence are
implementation details. They are intentionally outside the configuration and
operational contract described here.

## Protected Boundary

The protected event is an unknown node ID becoming a member of the Smap. It
is broader than the `/v1/cluster/autoreg` endpoint or any one join opcode.

| Operation | Node-join authentication |
|---|---|
| Startup self-join | Required before admission |
| Administrative join | Required in addition to administrative authorization |
| Slow keepalive from a restarted or unknown node | Required - such a keepalive MAY (in a certain scenario) be promoted to self-join |
| Keepalive from an established, already-admitted member | Not a new admission |
| Split-branch or whole-cluster force-join | Required by the destination cluster |
| Ordinary intra-cluster request | Controlled by `auth.intra_cluster.request_auth` |
| Client request and ACL check | Controlled by `auth.client_auth_required` |

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
- The pathname is replicated as cluster configuration; the file contents are
  never replicated through AIS metadata.
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

A credential file may contain more than one secret, one per line. The first is
the active secret used to produce new proofs; all listed secrets are accepted
when verifying proofs. This overlap supports secret rotation and preparation
for a force-join. An optional final newline is allowed, but empty records are
not. Whitespace is not trimmed.

The UUID binding is part of the proof rather than the file format:

```text
invite = HMAC(secret, "ais-join-invite-v1" | clusterUUID | nodeID)
accept = HMAC(secret, "ais-join-accept-v1" | clusterUUID | nodeID)
```

The primary creates the invite with its active secret. The joining node finds
the matching locally provisioned secret and uses it to create the accept proof.
Distinct domain strings prevent either directional proof from being reflected
as the other. The fields are encoded unambiguously; the vertical bars above are
notation, not literal separators.

The v1 proofs deliberately contain neither a nonce nor a timestamp. A deployment
that must prevent replay of an observed proof must protect the join exchange
with TLS.

<!--
TODO(implementation): the two usual anti-replay items are deliberately absent
from the v1 proof above (and the very first deliverable).

That's:
- timestamp: bind into both HMACs; reject outside auth.intra_cluster.nonce_window
  - the knob already exists and already means exactly this;
- nonce: verifier-chosen where the round trip allows it, plus a short-lived
  seen-nonce cache on the verifier.
-->

Generate a different secret for every cluster. Including `clusterUUID` in the
HMAC input binds a proof to the advertised UUID, but reusing the underlying
secret across clusters would authorize each cluster to produce proofs for the
other.

Never write a secret to logs, error responses, cluster metadata, or command
output.

The file must be accessible by the AIS process and by nobody else: typically,
K8s default mode `0400` owned by the account the node runs as. A credential that is
group- or world-accessible is rejected rather than used - a permissive mode is
far more often an accident of provisioning than a deliberate choice.

## Version Behavior

| Cluster state | Behavior |
|---|---|
| Path empty | Admission authentication is not configured |
| Path nonempty on v5.0 | Setting is accepted and persisted, but the file is not used |
| Mixed v5.0/v5.1 cluster | Enforcement has no effect until all nodes are running v5.1 |
| All nodes v5.1 or later | When configured, node admission is mutually authenticated |

The v5.0 staging window allows operators to provision and persist the setting
before upgrading. The CLI warns when the setting is updated while one or more
nodes do not support v5.1 behavior.

## Creating a New Cluster

Do not configure `node_join_secret_path` in the initial configuration of a
brand-new cluster.

AIS generates the cluster UUID during primary startup, after collecting
initial node registrations. There is therefore no established UUID to include
in a proof during the initial registration phase. Initial formation must take
place on a trusted or isolated network.

The secret file itself may be generated and mounted before cluster formation:
its contents do not depend on the UUID. Leave `node_join_secret_path` empty
until the cluster has formed.

After the cluster has formed:

1. Wait for initial registration to complete and verify that the cluster is
   stable.
2. Obtain the established UUID, for example with:

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
the nodes admitted during initial cluster formation. Protection from the very
first registration would require a separate pre-UUID bootstrap mechanism and
is not provided by this setting.

Note that this section is about forming a *brand-new cluster*. A new *node*
joining an already-formed cluster is the opposite case: it must be provisioned
with the credential before it starts - see
[Adding, Restarting, and Administratively Joining Nodes](#adding-restarting-and-administratively-joining-nodes).

On v5.0, the final configuration update is persisted but remains a runtime
no-op. It can be performed in advance of the all-node v5.1 upgrade.

## Enabling Protection on an Existing Cluster

Use the following order for an existing cluster:

1. Obtain and verify the cluster UUID.
2. Generate a cryptographically random shared secret.
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
  start and never refreshed, which makes [secret rotation](#secret-rotation)
  silently impossible. Mount the directory instead.

Kubernetes projected Secret files commonly use symlinks internally. File
validation must follow those symlinks rather than rejecting the projected file
because of its representation.

The [AIS Kubernetes operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator) should preserve the same ordering: create or update
the Secret, update pod mounts, confirm availability, and only then update the
AIS cluster configuration.

## Secret Rotation

Rotation must preserve an overlap during which primaries can verify proofs made
with either secret. With the ordered secrets in the credential file, use three
rollout stages:

1. **Introduce:** keep the old secret first and append the new secret. Complete
   this update on every node.
2. **Switch:** place the new secret first and retain the old secret second.
   Complete this update on every node.
3. **Retire:** remove the old secret after every node is known to use and accept
   the new secret.

Do not begin a stage until the preceding stage has completed on every node.
The reload mechanism and observability for a specific release must be checked
before rotation. If live credential reload is not supported, perform each stage
with a controlled node rollout.

`auth.intra_cluster.rotation_grace` belongs to per-node request-signing key
rotation. It does not provide an overlap window for the node-join shared secret.

## Force-Joining Distinct Clusters

When an entire source cluster joins a destination cluster with a different
UUID, every source node must be provisioned with the destination credential
before force-join begins. A file containing both clusters' secrets allows the
source cluster to remain recoverable while its nodes prepare to authenticate to
the destination. The destination primary's invite identifies which locally
provisioned secret must be used for the accept proof.

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
credential file is missing, unreadable, malformed, or contains no secret that
matches the proof for the advertised UUID. Error messages may identify the
pathname, UUID, or failure category, but must never include secret material.

A node whose configured path is unusable at startup fails to start, loudly,
rather than starting and quietly never becoming a member. The two outcomes are
operationally very different: a node that will not start is immediately visible
in any orchestrator, while a node that is up but unadmitted looks healthy and is
not. This is also why the check belongs to node startup rather than to
cluster-configuration validation, which runs wherever the update is applied.

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

## References

For related security boundaries, see [Authentication and Authorization](/docs/auth_validation.md).
For node lifecycle operations, see [Node Lifecycle](/docs/lifecycle_node.md).
