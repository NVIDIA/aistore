---
layout: post
title: "One Cluster, Two Perimeters, Three Networks"
date: Jul 28, 2026
author: Alex Aizman
categories: security networking storage distributed-systems performance aistore
---

Ask an AIStore (AIS) cluster for any data and the gateway that checked your credentials does not send it back to you.
It signs a short envelope with its own cluster-recognized key, hands that envelope to you, and redirects you to a storage node that would otherwise reject this unmediated request.
Your HTTP client then carries the gateway's signature back to the cluster.

That one request involves both of AIStore's security perimeters. Network path and security authority do not align hop for hop: a client may cross the public network carrying a gateway-signed handoff that is verified at the inner perimeter.

An AIS cluster has three networks and two security perimeters. The numbers do not match, and the mismatch is not an accident of naming.
The networks classify where traffic arrives and keep traffic with very different performance characteristics out of each other's way.
The perimeters establish whose authority a request carries: that of an external user, or that of a current cluster member. Those are different questions, and the second one is why a client can end up holding a cluster-member signature.

Most requests fit the pattern you would guess. Clients use the public network; cluster nodes use the intra-cluster networks. The interesting cases are the ones that do not. A client following a redirect arrives directly at a storage node carrying a gateway-signed handoff. A single API call may begin on the public network, fan out over intra-control, and move its payload over intra-data. A node that crash-restarts keeps its stable node ID and acquires an entirely new cryptographic identity.

Those cases are why AIS 5.x implements network separation, request authentication, and cluster membership as a single security model rather than three independent mechanisms.

> **Planned staged rollout**
>
> Unless noted otherwise, this text describes AIStore **5.1** and later.
> Version 5.0 is covered only as the [compatibility bridge](#why-50-is-the-bridge): it establishes the network separation and key propagation that enforcement depends on, with signing itself disabled.
> Version 5.1 follows shortly.

## Contents

- **Architecture**
  - [One cluster, two node types](#one-cluster-two-node-types)
  - [Three networks](#three-networks)
  - [Two perimeters](#two-perimeters)
- **Request paths**
  - [Get-batch: one request across all three networks](#get-batch-one-request-across-all-three-networks)
  - [A normal object request](#a-normal-object-request)
  - [What the target decides, and in what order](#what-the-target-decides-and-in-what-order)
  - [Control messages and data streams](#control-messages-and-data-streams)
  - [Transport streams use a separate signed envelope](#transport-streams-use-a-separate-signed-envelope)
  - [S3 compatibility: another API, another set of semantics](#s3-compatibility-another-api-another-set-of-semantics)
- **Per-node identity**
  - [From a cluster secret to per-node identity](#from-a-cluster-secret-to-per-node-identity)
  - [Restart, rejoin, and key propagation](#restart-rejoin-and-key-propagation)
  - [Why 5.0 is the bridge](#why-50-is-the-bridge)
- [Future work](#future-work)
- [Recap](#recap)

## One cluster, two node types

An AIS cluster consists of gateways and storage targets.

Gateways are diskless cluster coordinators. They accept public API requests, authenticate and authorize users when configured to do so, maintain and distribute cluster-level metadata, coordinate cluster-wide operations, and select storage targets. On the normal native object path a gateway redirects the client to a target rather than relaying object data itself.

Targets store objects on locally attached filesystems. They manage [mountpaths](https://github.com/NVIDIA/aistore/blob/main/docs/terminology.md#mountpath), perform local I/O,
and move data for replication (`ais cp`), erasure coding, [global rebalance](https://github.com/NVIDIA/aistore/blob/main/docs/rebalance.md), [ETL](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md), [get-batch](https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md), and other distributed operations.

Every gateway and target holds the cluster map, or Smap. The Smap identifies the current members and their network endpoints. It also provides the shared input for highest-random-weight placement: given the same fully qualified (bucket and object) name and the same Smap, every node independently selects the same target. There is no per-object metadata server and no object-location lookup.

That shared membership view turns out to be the foundation of the inner security perimeter, though it was not built for that.

## Three networks

AIS has distinguished three traffic domains since its original architecture, and the original reason had nothing to do with security. It was **bandwidth and latency**. Bulk data movement between targets will starve the small messages that hold a cluster together - keepalives, membership updates, coordination - if they share a pipe, and a cluster that cannot hear its own heartbeat under load is a cluster that falls apart exactly when it is busiest. Separating the traffic classes was a performance decision circa 2018.

| Network | Primary traffic | Typical examples |
| --- | --- | --- |
| Public | User and application requests | CLI, SDKs, notebooks, training jobs, S3 clients |
| Intra-control | Cluster coordination and distributed API work | API fanout (gateway => nodes), keep-alive, metasync, elections, membership changes, xaction (batch job) coordination, rebalance control |
| Intra-data | Bulk data movement between targets | Global rebalance, [get-batch](#get-batch-one-request-across-all-three-networks), ETL, replication, erasure coding (transport streams) |

A production deployment commonly assigns separate interfaces and addresses to all three. They may use different subnets, VLANs, physical fabrics, or firewall domains while retaining the same port number on each interface. Smaller deployments can keep them on one host and one physical interface while still using distinct endpoint addresses. Intra-control and intra-data may also share a single HTTP endpoint; they remain distinct traffic roles even when they use the same listener.

What changed in 5.0 is that public and intra-cluster traffic can no longer collapse onto one listener. At least one intra-cluster endpoint must be configured, and it must be distinct from the public endpoint.

The reason is structural. A server can trust the listener that accepted a connection, because the server created that listener and assigned its role. It cannot obtain the same assurance from a header or a URL, both of which the caller supplies. Remote addresses are no better: proxies, NAT, containers, and shared hosts all transform or obscure them. If presenting an internal-only header is what makes a request trusted, then a public caller who presents the same header is trusted too - and that is not an exotic attack, it is the direct consequence of asking caller-controlled data to identify which side of a boundary the caller is on.

AIS therefore records the arrival network when a connection is accepted and carries that fact with every request handled on the connection. This does not identify the caller. It establishes the traffic domain within which the caller must then be authenticated.

Separate interfaces, VLANs, and firewalls can further restrict who is able to reach each network.
When enabled, TLS protects confidentiality and integrity in transit and authenticates the server endpoint.
Neither replaces application identity: user tokens establish user identity, node signatures establish current-member authority.

## Two perimeters

The outer and inner perimeters answer different questions.

The **outer perimeter** handles original user requests arriving at a gateway over the public network. It validates the user's token and applies the required cluster or bucket permission. AIStore AuthN supports JWT validation, role-based cluster and bucket ACLs, token revocation, mandatory expiration, RSA signing, and OIDC issuer discovery.

The **inner perimeter** handles authority derived from current cluster membership. A node signs with its own Ed25519 private key; the receiver verifies against that node's public key in its current Smap. Some operations additionally require a particular cluster role - for example, that the signer be the primary gateway.

The inner perimeter appears in two forms:

1. A gateway or target sends a request directly to another cluster member.
2. A client presents a request envelope previously signed by a gateway, as happens after an object redirect.

The second form is the one worth pausing on. The client originates the HTTP request to the target, over the public network, with no membership credentials of its own. The authority that permits the request belongs to the gateway that signed the envelope the client is carrying. Arrival network and request authority are therefore not the same property, and a design that conflates them cannot express this case at all.

The perimeters are not concentric filters applied to every HTTP request. Each message has one provenance and follows the corresponding security path. A larger client operation can cross both, through a sequence of messages.

## Get-batch: one request across all three networks

A plain read (GET) request follows a short path: the gateway authorizes the request, selects the target, and redirects the client.
AIS [get-batch](https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md) is maybe a more revealing example:
one API call uses all three networks and crosses both security perimeters without collapsing them into a single boundary.

This is a single API call to retrieve multiple objects, object ranges, and archived files - in one shot, as one assembled output stream.
The requested data may belong to different buckets, may be sharded (archived), may (and most likely will) be distributed across nodes.
One target is selected as the **designated target**, or DT.
The remaining `N - 1` targets act as _senders_: they read their local content and stream the requested payloads to the DT, which assembles the result and returns it to the client.

The operation has three phases:

![Get-batch: securing a three-phase distributed flow](/assets/one-two-three-get-batch.webp)

> Only the designated target (DT) returns data to the client; the target senders never communicate with the client directly.
> The dashed boundaries are conceptual. The inner perimeter is drawn inside the outer one for layout only - a request is not filtered by both in sequence.
> Each message crosses the perimeter corresponding to the authority it relies on.

### Phase 1: initialize the designated target

The client sends the get-batch request to a gateway over the public network. The request body may contain entries such as:

```text
{ bucket: A, objname: b/c/d.flac }
{ bucket: E, objname: f.tgz, archpath: samples/g.wav }
{ bucket: H, objname: i.tar, start: 1234, length: 5678 }
```

The gateway parses the request, resolves and authorizes every referenced bucket, and selects the designated target. Depending on the requested colocation level, that selection may be derived from the input objects or made independently using the current Smap.

The gateway then sends a signed intra-control `POST` to the DT. This is not yet the client data path. It is cluster coordination: the DT renews or creates the get-batch xaction, opens the long-lived peer-to-peer connections used by the transport subsystem, prepares its receive-side state, and returns the real xaction ID.

The gateway is therefore acting under two different authorities during this phase. It first handles the original request under the user's authorization at the outer perimeter. It then addresses the DT as a current cluster member at the inner perimeter, signing the intra-control request with its own Ed25519 key.

No user data is returned in phase 1.

### Phase 2: redirect the client to the designated target

Once the DT has initialized its receive-side state, the gateway redirects the client's original GET to that target, normally over the public network.

> A configured [local-CIDR](https://github.com/NVIDIA/aistore/blob/main/docs/environment-vars.md) optimization may instead use the target's intra-control endpoint.

The redirect carries the xaction ID, a per-request work ID, the expected target count, and the gateway's signed handoff. The client does not become a cluster member and does not receive the gateway's private key. It merely carries the signed redirect URL from one public endpoint to another.

At the DT, the request arrives over the public network from a client, not a cluster member. The DT nevertheless accepts the handoff because it carries a gateway signature.
The latter identifies the signing gateway in the current Smap and verifies the signature using that gateway's recorded verifying key before beginning assembly.

The DT then starts producing the response stream. Some requested objects (or archived files) may already be local. Others will arrive from target senders during phase 3.

### Phase 3: start the target senders

After issuing the redirect, the gateway asynchronously broadcasts a signed intra-control `POST` to every active target except the DT.

Each target sender renews the same get-batch xaction by ID, opens its peer-to-peer transport connections, reads the local data assigned to it, and streams those payloads directly to the DT over the intra-data network.

This phase illustrates why intra-control and intra-data remain separate roles even though both are inside the inner perimeter.

The gateway's broadcast is control-plane work: small HTTP requests that identify the xaction, the DT, and the work to start. Those requests use the same node-level HTTP signing and verification mechanism as other intra-control operations.

The resulting data streams are data-plane work: large, long-lived, and carried by the transport subsystem rather than ordinary request-response HTTP.
They are authenticated as well, but by a different envelope signed when the connection opens - the subject of a [later section](#transport-streams-use-a-separate-signed-envelope).
Both mechanisms establish current-member authority; they authenticate different protocols and different facts.

### One operation, two perimeters

Get-batch is one client operation, but there is no single end-to-end trust decision that covers all of its traffic.

The gateway authorizes the original request at the outer perimeter. The DT trusts the redirected phase-2 request because it carries a valid gateway-signed handoff. The targets trust phase-1 and phase-3 coordination because the HTTP requests are signed by a current cluster member. The DT trusts incoming phase-3 streams because their transport envelopes are independently signed by the sending targets.

The three networks classify where each part of the operation belongs:

| Phase | Path                     | Network       | What establishes trust                        |
| ----- | ------------------------ | ------------- | --------------------------------------------- |
| 1     | Client → gateway         | Public        | User authentication and bucket ACLs           |
| 1     | Gateway → DT             | Intra-control | Gateway node signature                        |
| 2     | Client → DT              | Public        | Gateway-signed redirect carried by the client |
| 3     | Gateway → target senders | Intra-control | Gateway node signature                        |
| 3     | Target senders → DT      | Intra-data    | Target transport-stream signatures            |

This is the architectural pattern in its fullest form: one cluster, two security perimeters, and three network roles participating in one logical request.

## A normal object request

A plain-vanilla GET(object) is the same model in its simplest form, step by step:

1. The client sends the request to a gateway over the public network.
2. At the outer perimeter, the gateway authenticates the user and checks read permission for the bucket.
3. The gateway uses the current Smap and the object name to select the responsible target.
4. The gateway signs an envelope covering the request and returns a redirect.
5. The client follows the redirect to the target.
6. At the inner perimeter, the target verifies that a current gateway signed the envelope. It does not repeat the user-token and bucket-ACL checks.
7. The target reads and returns the object directly.

In normal production deployments the redirect names the target's public endpoint, because that is the target address a client can reach (see "local-CIDR" comment above).

A PUT follows the same pattern with a method-preserving redirect. The gateway checks write permission, selects a target, and signs. The target verifies the handoff before accepting and finalizing the object. If the bucket requires replication, erasure coding, or other derived work, the target then acts under its own node identity when it talks to its peers - the user is no longer speaking to storage at that point; a cluster member is.

The mapping is therefore not one network to one perimeter:

| Receiver and arrival network | Request | Security path |
| --- | --- | --- |
| Gateway, public | Original client API request | Outer: user token and ACL |
| Target, normally public | Client carrying a signed gateway redirect | Inner: current gateway's signature |
| Gateway or target, intra-control | Peer control request | Inner: current member's signature and required role |
| Target, intra-data | Peer data request or stream establishment | Inner: sender signature on the request or the stream opening |
| Target, public | Unmarked direct request | Rejected when gateway mediation is required |

The last row deserves a note. A target's public endpoint must be reachable, because redirected clients have to reach it. Being reachable is not the same as being open: when gateway mediation is required, a request that names the target directly and carries no gateway envelope does not get to skip the outer perimeter by choosing a different address.

## What the target decides, and in what order

The natural assumption is that the arrival network decides how a request is checked. It does not - at least, not first. A target receiving an object request asks three questions in a fixed order, and the order is the whole three-into-two problem in miniature:

```text
   object request arrives at a target
                │
                ▼
     ┌──────────────────────┐
     │ signing enforced?    │──── no ────▶ compatibility path (5.0 bridge)
     └──────────┬───────────┘
                │ yes
                ▼
     ┌──────────────────────┐
     │ gateway envelope?    │──── yes ───▶ verify the gateway's signature
     └──────────┬───────────┘              (arrival network not decisive)
                │ no
                ▼
     ┌──────────────────────┐
     │ arrived intra-net?   │──── no ────▶ reject direct target access
     └──────────┬───────────┘              (when gateway mediation is required)
                │ yes
                ▼
       verify the peer's signature against its
       verifying key in the current Smap
```

A redirected client request is authorized by the gateway's signature and deliberately not by where it landed. This matters because where it lands is genuinely variable: normally the target's public endpoint, sometimes the intra-data one, and when a deployment collapses its two intra-cluster endpoints into one, traffic that logically belongs to intra-data legitimately arrives on the control listener. Any rule of the form "requests on this network are peer requests" would be wrong in all three configurations.

The arrival network still decides something, but only after the envelope question has been answered. A request with no gateway envelope, arriving on the public listener, is a client trying to talk to storage directly - and that is where the network check does its work.

## Control messages and data streams

Many public API calls trigger several internal messages.

For a distributed list-objects request, a gateway first applies the outer perimeter to the user's request. It then fans the operation out to the targets over intra-control. Each target verifies the gateway as a current cluster member before contributing its page of results, and the gateway merges those responses for the client.

Keepalive, metasync, primary elections, [membership changes](https://github.com/NVIDIA/aistore/blob/main/docs/lifecycle_node.md), and xaction coordination do not represent a user speaking to storage at all. They belong entirely to the inner perimeter: there is no user token, no bucket ACL, and no delegated user authority involved. Their handlers can rely on both the intra-cluster arrival domain and the verified identity of the sending node. In this case, network role and security authority coincide - unlike a redirected user GET or PUT, which normally arrives over the public network carrying a gateway-signed handoff.

Bulk intra-cluster data movement uses the same member identity in a different traffic role. Examples include [offline ETL](https://github.com/NVIDIA/aistore/blob/main/docs/etl.md), [global rebalance](https://github.com/NVIDIA/aistore/blob/main/docs/rebalance.md), and the previously described [get-batch](#get-batch-one-request-across-all-three-networks).

These operations exchange setup and coordination messages over intra-control, then move object payloads over intra-data. Long-lived transport connections are authenticated when they are opened and whenever they are re-established.
The signed opening binds the transport name, sender ID, session ID, Smap (cluster map) version, and nonce. Once established, the connection carries object payloads for its lifetime.

## Transport streams use a separate signed envelope

HTTP requests and intra-cluster transport streams use the same per-node Ed25519 identity, but they authenticate different things. The HTTP envelope covers one request: its method, path, sender, Smap version, content length, and nonce. It may authenticate either a direct peer request or a gateway-signed redirect carried by a client.

A transport stream is different. It is a long-lived connection between targets, accepted only on an intra-cluster listener, and may carry many objects over its lifetime. Authentication therefore happens when the connection is opened and whenever it is re-established.
The signed stream opening binds the transport name, sender ID, session ID, Smap version, and nonce. It also uses a separate signature domain, preventing an HTTP signature from being replayed as proof for a transport connection.

> The stream payload uses an explicit protocol domain (`ais-transport-v1`) to prevent cross-protocol signature reuse.

The two mechanisms thus share node identity and verification policy, but sign different facts: HTTP signing authenticates an individual request, while transport signing authenticates a member opening a long-lived intra-cluster channel.

## S3 compatibility: another API, another set of semantics

Putting another API on the front of a storage system adds more than another set of URLs. S3 has its own request semantics: operations encoded in query parameters, lists of object names carried inside XML bodies, multipart state distributed across storage nodes, and clients with different redirect behavior. The gateway must translate those requests into native AIStore redirects, fanouts, and bulk operations.

Whenever the underlying security model changes, that translation layer deserves a separate review. An invariant that is obvious in the native API may no longer be obvious - or even visible - after an S3 request has been transformed.

[`ListMultipartUploads`](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#Client.ListMultipartUploads) created one such case. Although the operation returns metadata, the target-side S3 endpoint is served over the public and intra-data networks, not intra-control. With multiple targets, the gateway queries every target over intra-data and aggregates their locally known uploads.

The common request helper, however, decided whether to stamp and sign an intra-cluster request by checking whether its destination matched the target's intra-control URL. An explicit intra-data destination did not match, so the multipart-listing fanout went out unsigned. Every target correctly rejected it. The aggregation path treated those rejections as empty results, and the client received a successful response reporting zero uploads when three existed.

The fix was to recognize both of a member's intra-cluster endpoints. The inner perimeter covers intra-control and intra-data alike; the network determines the traffic role, not whether member authentication is required.

Multi-object delete exposed a different translation boundary. An ordinary S3 object request carries its object name in the URL, where the target-side S3 handler can validate it before constructing local object metadata. A multi-object-delete request instead carries many keys inside an XML document. The gateway parses that document, extracts the names, converts them into an internal bulk-delete message, and broadcasts the operation.

By that point, the names are no longer part of an S3 object URL, so the normal target-side S3 path validation cannot be assumed to have covered them. Every key is now validated explicitly before it is admitted into the internal bulk-delete request.

Neither issue required novel cryptography. Both came from a semantic change at the API boundary: an intra-data call was mistaken for a non-intra call, and object names moved from a validated URL path into a decoded request body. The S3 compatibility API is therefore its own security surface, not merely another route to the same handlers.

## From a cluster secret to per-node identity

AIS 4.x authenticated intra-cluster messages with one persistent symmetric key. Every node held the same key and used it to generate and verify HMACs.

That design could distinguish a cluster insider from an outsider, but it could not distinguish one member from another. A shared secret gives every node the same signet ring: a valid seal proves that someone holding the cluster secret stamped the request, and nothing more. Any node could produce a message attributed to any other node. Compromise of one copy exposed the cluster's only secret, and rotation meant redistributing a replacement to every member.

AIS 5.0 replaces the shared secret with one Ed25519 keypair per node. A node alone holds its signing key. Its verifying key is published with its identity in the Smap. A receiver can therefore verify not merely that a request arrived with cluster credentials, but which current member authorized it.

Private keys are ephemeral: each node generates a new keypair upon startup and keeps the private half in memory only - never writes it down. This removes an entire category of secret management: no private-key files to protect, no durable copies to reconcile across mountpaths, no filesystem metadata holding key material, nothing left on disk after a node stops.

The Smap is the natural distribution vehicle for the public half. It already distributes node IDs, roles, endpoints, flags, and membership changes; adding the verifying key keeps membership and member identity in the same versioned object instead of introducing a separate key-distribution service that would need its own trust bootstrap. The consequence - and it is a large one - is that a key change is now a membership event.

## Restart, rejoin, and key propagation

A node ID is stable across restart. Its (public, private) keypair is not.

When a node rejoins, the primary gateway must compare both its network information and its verifying key against the current Smap entry. A changed key requires a renewed entry even when the node ID, endpoints, role, and flags are all unchanged. The updated Smap then propagates, and once a receiver holds the new entry it verifies against the new key and stops accepting the old one.

The ordering matters during a quick crash and restart:

1. The restarted node has already generated and begun using its new signing key.
2. A peer may still hold an Smap containing the old verifying key.
3. Until the updated Smap reaches that peer, a correctly formed signature from the restarted node is cryptographically invalid from the peer's point of view.

This is different from a missing signature during a narrowly defined bootstrap or compatibility path. An absent signature can be handled by the rules for that transitional state. An invalid signature cannot be treated as absence, because doing so would convert failed authentication into an authentication bypass - which is the one thing this entire model exists to prevent.

The same comparison is required during primary startup and cluster reconstruction. Any registration path that decides a node is unchanged based only on its ID and endpoints can leave the Smap holding a stale verifying key.

In the 5.x model, current node identity is the stable node ID, plus current membership, plus the verifying key recorded for that member in the current Smap. Key rotation has become membership propagation.

## Why 5.0 is the bridge

The rollout is deliberately staged.

Version 5.0 establishes the prerequisites: separate public and intra-cluster arrival domains, and a cluster map format that carries per-node verifying keys. Signature enforcement remains disabled while the cluster moves across that shared-metadata boundary.

Version 5.1 then allows to enable signing and verification (once every member understands the new identity model).

A direct 4.x-to-5.1 upgrade is therefore unsupported (and not permitted).

## Future work

The authenticated nonce already carried by signed requests is a foundation for replay handling, but replay rejection requires receiver-side state and a policy that accounts for concurrent requests, retries, out-of-order arrival, multiple destinations, memory limits, and node key epochs. A naive "reject anything not strictly newer" check would look reassuring while rejecting a great deal of valid traffic.

The signature envelope currently covers the method, path, sender, Smap version, content length, and nonce. Future extensions can add canonical query coverage, and body digests where appropriate.

> The default model will likely remain: authenticate the gateway handoff, bind routing-critical fields, and let the target perform ordinary semantic validation. Consistent with AIS design and implementation, the TBD feature flag would strengthen that to cryptographically bind additional request semantics - at an added datapath cost.

A third area that, in theory, may require further investigation is a quick node restart, when the primary did not have enough time to notice the loss. Generally, a node should delay signing its control-plane messages until its newly generated verifying key has been distributed with the current Smap. The receive side already distinguishes missing startup credentials from invalid signatures; a send-side readiness barrier would make the transition explicit from both ends.

## Recap

In AIS, the [three networks](https://github.com/NVIDIA/aistore/blob/main/docs/networking.md) were always separated to keep user requests, cluster coordination, and bulk data movement out of each other's way. Eight years later, that same separation turned out to be the structure needed to apply the right security policy to each connection - this is the honest fact.

The outer perimeter authenticates users and enforces their permissions. The inner perimeter authenticates current cluster members and the handoffs they authorize. A single operation may cross from one to the other, but the two checks remain distinct, and no hop is asked to answer both questions at once.

The cluster map joins the model together. It gives every node the same view of membership and placement, distributes the verifying keys that belong to that membership, and turns a cluster member's identity into something every peer can check for itself.
