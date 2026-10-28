# AIStore Networking Model

AIStore (AIS) separates traffic across three logical network roles:

* **Public network**: user-facing API, CLI/SDK traffic, metrics, and external tooling.
* **Intra-cluster control network**: health, metasync, membership, xaction control, and internal control-plane broadcast.
* **Intra-cluster data network**: object movement between AIS nodes, including target-to-target GET/PUT, global rebalance, resilver, get-batch streams, and other data-mover paths.

These are **logical roles**. They may map to separate physical networks, or they may share physical infrastructure, depending on deployment size and performance requirements. Starting with v5.0, however, AIS requires the public HTTP endpoint to be distinct from intra-cluster endpoints.

## Table of Contents

- [v5.0 Network-Separation Rule](#v50-network-separation-rule)
  - [Valid minimal two-listener deployment](#valid-minimal-two-listener-deployment)
  - [Valid development-style deployment](#valid-development-style-deployment)
  - [Invalid post-v5.0 deployment](#invalid-post-v50-deployment)
- [Logical vs Physical Networks](#logical-vs-physical-networks)
- [Network Layout](#network-layout)
  - [Intra-Cluster Data Network Breakdown](#intra-cluster-data-network-breakdown)
  - [HTTP intra-data](#http-intra-data)
  - [Transport streams](#transport-streams)
- [Public Network](#public-network)
- [Intra-Cluster Control Network](#intra-cluster-control-network)
- [Intra-Cluster Data Network](#intra-cluster-data-network)
- [Multi-Homing](#multi-homing)
- [Node-to-Node TLS](#node-to-node-tls)
  - [Two certificates, one rule](#two-certificates-one-rule)
  - [Why `alt_tls_dns_name` exists](#why-alt_tls_dns_name-exists)
  - [Why not everywhere](#why-not-everywhere)
  - [Who accepts what](#who-accepts-what)
  - [Deployment cases](#deployment-cases)
  - [Troubleshooting](#troubleshooting)
- [IPv6](#ipv6)
  - [Enabling IPv6](#enabling-ipv6)
  - [What "prefer IPv6" means](#what-prefer-ipv6-means)
  - [Switching an existing cluster: IPv4 ↔ IPv6](#switching-an-existing-cluster-ipv4--ipv6)
  - [URL format](#url-format)
  - [Socket-level tuning](#socket-level-tuning)
  - [Kubernetes](#kubernetes)
- [Effective Networking and IP Family Selection](#effective-networking-and-ip-family-selection)
- [Initialization Phases](#initialization-phases)
  - [Phase 1: Network Resolution and Server Construction](#phase-1-network-resolution-and-server-construction)
  - [Phase 2: Client and Runtime Initialization](#phase-2-client-and-runtime-initialization)
- [Duplicate Runs and Duplicate Addresses](#duplicate-runs-and-duplicate-addresses)
  - [Local listener conflicts and overlapping runs](#local-listener-conflicts-and-overlapping-runs)
  - [Duplicate addresses (misconfiguration)](#duplicate-addresses-misconfiguration)
  - [Troubleshooting Duplicate Endpoints](#troubleshooting-duplicate-endpoints)
    - [Local listener conflicts](#local-listener-conflicts)
    - [Conflicting cluster registrations](#conflicting-cluster-registrations)
- [Summary](#summary)

## v5.0 Network-Separation Rule

Starting with v5.0, AIS requires the public network to be separated from intra-cluster networking at the HTTP listener level.

In practical terms:

* the public endpoint must not be the same listener as intra-control or intra-data;
* intra-control and intra-data may share the same listener;
* public multi-homing is allowed, but all public addresses remain public-only;
* physical network isolation is strongly recommended for production, but is not required by this rule.

The mandatory rule is about **HTTP endpoint separation**, not necessarily physical NIC separation. A deployment may use fewer physical networks than logical roles, as long as the public listener is distinct from the intra-cluster listener.

### Valid minimal two-listener deployment

```json
"host_net": {
    "hostname": "10.50.56.200",
    "hostname_intra_control": "10.50.56.205",
    "hostname_intra_data": "10.50.56.205",
    "port": "51081",
    "port_intra_control": "51081",
    "port_intra_data": "51081"
}
```

In this configuration:

* public traffic uses `10.50.56.200:51081`;
* intra-control and intra-data share `10.50.56.205:51081`;
* AIS runs two HTTP listeners and two HTTP servers: one public, one intra-cluster.

### Valid development-style deployment

This is also valid on a single host or shared physical fabric, because the public and intra-cluster endpoints are still distinct:

```json
"host_net": {
    "hostname": "127.0.0.1",
    "hostname_intra_control": "127.0.0.1",
    "hostname_intra_data": "127.0.0.1",
    "port": "8080",
    "port_intra_control": "9080",
    "port_intra_data": "9080"
}
```

Here, public, intra-control, and intra-data traffic use the same loopback interface and the two [local playground](/docs/getting_started.md#local-playground) ports.

### Invalid post-v5.0 deployment

The following is no longer valid:

```json
"host_net": {
    "hostname": "127.0.0.1",
    "hostname_intra_control": "127.0.0.1",
    "hostname_intra_data": "127.0.0.1",
    "port": "8080",
    "port_intra_control": "8080",
    "port_intra_data": "8080"
}
```

This configuration collapses public and intra-cluster traffic onto the same HTTP listener.

## Logical vs Physical Networks

The three AIS network roles may map to:

1. three physically isolated fabrics with separate, non-shared bandwidth;
2. two physical or logical endpoints, with control and data collapsed into one intra-cluster endpoint;
3. a single shared physical fabric, provided that the public HTTP endpoint is still separate from the intra-cluster endpoint.

For production clusters, option 1 is **strongly recommended**. The main reason is not only aggregate bandwidth, but isolation: bulk data movement can fill NIC, TCP, kernel, and application queues, causing head-of-line (HoL) blocking for small latency-sensitive control messages.

Large object transfers, GFN requests, rebalance/resilver traffic, and get-batch streams must not be allowed to delay keepalives, health checks, metasync, or membership updates.

The control plane itself is low-bandwidth. A modest but **dedicated** control path is often sufficient; what matters most is predictable latency and freedom from bulk-data queueing. By contrast, the data path benefits directly from high throughput and should be provisioned accordingly.

| Topology                                               | Valid in v5.0? | Notes                                                               |
| ------------------------------------------------------ | -------------- | ------------------------------------------------------------------- |
| `public == control == data` endpoint                   | No             | One HTTP listener cannot serve both public and intra-cluster roles. |
| `public` separate, `control == data`                   | Yes            | Minimal supported topology.                                         |
| `public` separate, `control` separate, `data` separate | Yes            | Preferred production topology.                                      |
| Same physical fabric, different public/intra endpoints | Yes            | Useful for development and small deployments.                       |
| Public multi-homed, intra-cluster separate             | Yes            | Multi-homing applies only to PubNet.                                |

## Network Layout

```text
                     ┌───────────────────────────────────────┐
                     │               Clients                 │
                     │   ais CLI / SDK / notebooks / tools   │
                     └───────────────────┬───────────────────┘
                                         │ PubNet (user-facing)
                                         ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                             AIStore Cluster                               │
│                                                                           │
│   ┌───────────────────────────────────────────────────────────────────┐   │
│   │                           Proxies (P)                             │   │
│   │                                                                   │   │
│   │   PubNet:     user-facing API, metrics, external clients          │   │
│   │   CtlNet:     intra-cluster control-plane HTTP                    │   │
│   │   DataNet:    intra-cluster data movement                         │   │
│   └───────────────┬───────────────────────────┬───────────────────────┘   │
│                   │                           │                           │
│                   │ Intra-control             │ Intra-data                │
│                   ▼                           ▼                           │
│   ┌──────────────────────────────┐   ┌────────────────────────────────┐   │
│   │          Targets (T)         │   │           Targets (T)          │   │
│   │                              │   │                                │   │
│   │  CtlNet endpoint(s):         │   │  DataNet endpoint(s):          │   │
│   │    - health                  │   │    - HTTP T2T GET/PUT          │   │
│   │    - metasync                │   │    - GFN                       │   │
│   │    - membership              │   │    - transport/bundle streams  │   │
│   │    - control fanout          │   │    - rebalance/resilver/etc.   │   │
│   └──────────────────────────────┘   └────────────────────────────────┘   │
│                                                                           │
│ Notes:                                                                    │
│ * PubNet/CtlNet/DataNet are logical roles.                                │
│ * PubNet must not share an HTTP listener with intra-cluster networking.   │
│ * CtlNet and DataNet may collapse into one intra-cluster endpoint.        │
│ * Multi-homing applies only to PubNet.                                    │
└───────────────────────────────────────────────────────────────────────────┘
```

### Intra-Cluster Data Network Breakdown

The intra-cluster data network carries more than one data-movement mechanism. Some paths use regular `net/http`; others use AIS transport streams, which use `fasthttp` by default. Both belong to the intra-cluster data path from an operator's perspective.

| Attribute                    | **HTTP Intra-Data**                                                                    | **Transport Subsystem**                                                                             |
| ---------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| **Examples**                 | GFN, target-to-target HTTP GET/PUT, HTTP object relocation paths                       | Rebalance, resilver, get-batch streams, shared-DM/SDM, other data movers                            |
| **Endpoint configuration**   | Uses `host_net.hostname_intra_data` / `port_intra_data` via the intra-data HTTP client | Uses the same intra-data network by default; current AIS transport data movers rely on this default |
| **Library / protocol layer** | Standard Go `net/http`                                                                 | `fasthttp` by default, with AIS transport framing, PDUs, and demuxing                               |
| **Mechanism**                | Request/response HTTP                                                                  | Long-lived streaming connections                                                                    |
| **Traffic profile**          | Transactional, object/path-scoped, often latency-sensitive                             | High-volume, continuous or bursty bulk data movement                                                |
| **Primary concern**          | Request latency, timeout behavior, and retry behavior                                  | Throughput, backpressure, queueing, and HoL impact                                                  |
| **Implementation layer**     | HTTP server muxers and intra-data HTTP clients                                         | Transport subsystem, including bundle/DM/shared-DM mechanisms                                       |

### HTTP intra-data

HTTP intra-data paths are regular request/response operations over the intra-data endpoint.

```text
        Target A                         Target B
     ┌───────────┐                    ┌───────────┐
     │ net/http  │  HTTP DataNet      │ net/http  │
     │ handlers  │◄──────────────────►│ handlers  │
     └───────────┘                    └───────────┘
```

A representative example is GFN, where one target fetches an object or archive member from another target using the intra-data URL and the intra-data HTTP client.

### Transport streams

Transport streams are long-lived data-mover connections used by high-volume operations.

```text
        Target A                          Target B
     ┌───────────┐   transport streams  ┌───────────┐
     │ transport │◄════════════════════►│ transport │
     │ bundle/DM │   PDU / streaming    │ bundle/DM │
     └───────────┘                      └───────────┘
```

Transport streams do not use the regular Go `net/http` mux, handler, or `http.Client` path. They use the AIS transport subsystem, with [`fasthttp`](https://github.com/valyala/fasthttp) as the default library, and long-lived streaming connections with AIS-specific framing and demuxing.

From an operator's perspective, however, these streams still belong to the intra-cluster data network. In current AIS data movers, the transport network defaults to `cmn.NetIntraData`.

> See also: [transport package README](https://github.com/NVIDIA/aistore/blob/main/transport/README.md).

## Public Network

The **public network** is user-facing and client-oriented.

Examples:

* CLI (`ais`)
* SDKs
* notebooks
* browsers
* Prometheus metrics
* external tools
* AuthN-enabled client requests

Characteristics:

* lowest trust boundary;
* may be multi-homed;
* may be fronted by load balancers or ingress;
* may use public-facing TLS identity and trust roots;
* may have tracing enabled.

Because the public listener is distinct from intra-cluster listeners, deployments may use different TLS identities and trust roots for public and intra-cluster traffic. For example, the public listener may use a certificate issued by an organization-wide or public CA, while intra-cluster listeners may use certificates issued by an internal cluster CA.

## Intra-Cluster Control Network

The **control network** carries cluster coordination traffic.

Examples:

* health checks
* keepalives
* metasync
* membership changes
* xaction control paths
* internal API fanout

Characteristics:

* low bandwidth;
* latency-sensitive;
* stable, predictable request volume;
* should be isolated from bulk data traffic.

The control plane does not require high throughput. Its critical requirement is predictable low latency. Even a relatively small dedicated path is often sufficient, provided it is not blocked behind data-plane queues.

## Intra-Cluster Data Network

The **data network** carries object movement between AIS nodes.

Examples:

* target-to-target GET;
* target-to-target PUT;
* GFN;
* HTTP-based object relocation paths;
* transport/bundle data movers, including rebalance, resilver, and get-batch streams.

Characteristics:

* high throughput;
* bursty or continuous transfer patterns;
* sensitive to backpressure and queue buildup;
* should be provisioned for available storage and network bandwidth.

Important distinction:

> The intra-cluster data network is the operator-visible data path. Within that path, AIS uses both regular HTTP request/response operations and the transport subsystem. The transport subsystem is separate from `net/http` muxers and `http.Client` paths, but it is not a fourth logical network role.

## Multi-Homing

Public networking supports **multi-homing** to utilize multiple NICs without LACP:

```json
"host_net": {
    "hostname": "10.50.56.205,10.50.56.206",
    "port": "51081"
}
```

Effect:

* AIS listens on all listed public addresses;
* clients may connect via any of them;
* no additional configuration is required.

Multi-homing currently applies to the **public network** only. All public multi-home addresses are equivalent public endpoints; they do not create additional intra-cluster listeners.

## Node-to-Node TLS

With HTTPS, AIS nodes are also TLS clients of each other. This section explains which certificate each node-to-node request sees, and which names it accepts. The short version: **give intra-cluster listeners a certificate that covers the nodes' advertised intra-cluster names, and use `alt_tls_dns_name` only for a public certificate that does not cover the nodes' public hostnames.**

### Two certificates, one rule

Every node serves two groups of listeners:

* **Intra-cluster listeners** (control and data, or a single collapsed intra-cluster endpoint) serve the default certificate, `net.http.server_crt`.
* **Public listeners** (including multi-homed ones) serve `net.http.pub.server_crt` when configured; otherwise the same default certificate.

Every node-to-node TLS connection verifies the peer certificate the usual way: against the hostname or IP address the client dialed. That is the destination node's *advertised* address on the network being used, as recorded in the cluster map.

Almost all node-to-node traffic dials intra-cluster addresses: keepalive, metasync, membership changes, xaction control, target-to-target GET/PUT, rebalance, and other data movers. Hence the one rule:

> The default certificate must be valid for every node's advertised intra-cluster hostname or IP address.

### Why `alt_tls_dns_name` exists

A few node-to-node requests go to a node's **public** URL and, therefore, land on its public listener:

* a non-primary proxy forwards admin and other control requests to the primary at the primary's public URL;
* a node joining (or rejoining) the cluster may contact the primary at its public URL;
* when a node does not respond on the intra-control network, some requests are retried once via its public URL.

Public certificates often cover only an external name, such as a load balancer, an ingress, or a wildcard domain, and not each node's individual public hostname. Verification of those requests then fails, even though the certificate is perfectly valid for the cluster.

Setting `net.http.alt_tls_dns_name` to a DNS name the public certificate covers fixes that: requests that may target a node's public URL accept a peer certificate valid for *either* the dialed hostname or IP address, *or* `alt_tls_dns_name`.

### Why not everywhere

`alt_tls_dns_name` is deliberately accepted only where it is needed:

* **Direct intra-data requests and transport streams use intra-cluster URLs.** They go to the intra-data endpoint, or to the intra-control endpoint when the two are collapsed, and retain ordinary hostname/IP verification. Same-cluster relays use a separate transport, as described below.
* **The intra-data HTTP client also serves external destinations**: cloud backends (via presigned requests) and ETL pods. A public name configured for *this* cluster has no business authenticating any of those. (Requests relayed to remote AIS clusters and clouds use a separate client that does not accept it either.)

The alternative name is confined to the control transport and same-cluster relays. The shared data client and external relay clients retain ordinary endpoint-name verification.

### Who accepts what

| Request | Destination | Certificate presented | Names accepted |
|---|---|---|---|
| Control-plane requests using the control transport | node's intra-control URL | default | advertised name or IP; also `alt_tls_dns_name` |
| Proxy forwarding control requests to the primary | primary's public URL | public (else default) | advertised name or IP, or `alt_tls_dns_name` |
| Other same-cluster proxy relays, including S3 | node's intra-control or intra-data URL | default | advertised name or IP, or `alt_tls_dns_name` |
| Join and rejoin | primary's intra-control or public URL | default or public | advertised name or IP, or `alt_tls_dns_name` |
| Fallback retry via public URL | node's public URL | public (else default) | advertised name or IP, or `alt_tls_dns_name` |
| HTTP intra-data: GFN, target-to-target GET/PUT, EC metadata | node's intra-data URL (intra-control when collapsed) | default | advertised name or IP only |
| Transport streams: rebalance, get-batch, EC, and other data movers | node's intra-data URL (intra-control when collapsed) | default | advertised name or IP only |
| External: cloud backends, ETL pods, remote AIS clusters | external endpoint | remote endpoint's certificate | the endpoint's own name only |

The control transport accepts `alt_tls_dns_name` for all its requests, including those to intra-control URLs. Longer calls to private URLs may instead use the data client, which accepts only the advertised hostname or IP. Long calls to a known node's public URL use `controlLong`, sharing the control transport. The default certificate must therefore cover the advertised intra-cluster names regardless of `alt_tls_dns_name`.

### Deployment cases

| Certificates | `alt_tls_dns_name` | Result |
|---|---|---|
| One certificate covering intra-cluster and public hostnames | not needed | works |
| Default certificate covers intra-cluster names; `net.http.pub` certificate covers only an external name (e.g., `ais.example.com`) | set to that external name | works |
| Default certificate covers intra-cluster names and the external name, but not the nodes' public hostnames | set to the external name | works |
| One certificate covering **only** an external name, served on all listeners | any | **not supported**: intra-data traffic and private control requests using strict clients fail hostname verification |

For the last case, move the external-only certificate under `net.http.pub` and give the intra-cluster listeners a certificate for the nodes' intra-cluster names (or add those names to the single certificate).

Example (second row): nodes advertise `ais-target-0.ais-target.ais.svc.cluster.local` and similar names on the intra-cluster networks, and `ais-target-0.example.com` and similar on the public network, all on port 51080; the public certificate covers only `ais.example.com`:

```json
"net": {
  "http": {
    "use_https": true,
    "server_crt": "/etc/ais/tls/intra.crt",
    "server_key": "/etc/ais/tls/intra.key",
    "client_ca_tls": "/etc/ais/tls/intra-ca.crt",
    "alt_tls_dns_name": "ais.example.com",
    "pub": {
      "server_crt": "/etc/ais/tls/pub.crt",
      "server_key": "/etc/ais/tls/pub.key"
    }
  }
}
```

The public certificate must chain to the system roots or to `net.http.client_ca_tls`. Changing `alt_tls_dns_name` requires restart; see [HTTPS: alternative DNS name](/docs/https.md#alternative-dns-name) for upgrade notes.

### Troubleshooting

A hostname mismatch in node logs, such as `x509: certificate is valid for ais.example.com, not ais-target-0.ais-target.ais.svc.cluster.local`, means a node dialed an address its peer's certificate does not cover:

* the address is an **intra-cluster** name: ensure the default certificate covers that name; direct intra-data clients and streams do not use `alt_tls_dns_name`;
* the address is a **public** hostname: set `alt_tls_dns_name` to a name the public certificate covers, or add the public hostname to the certificate.

## IPv6

AIStore supports IPv6 for all three logical network roles and for the transport streaming layer.

### Enabling IPv6

IPv6 is a cluster-wide setting:

```console
$ ais config cluster net.use_ipv6 true
```

To take effect, the change requires a cluster restart. On restart, each node discovers local IPv6 addresses and binds all listeners using IPv6 addresses when usable IPv6 addresses are available.

> **Operational note:**
> As with any cluster-wide configuration change that rebuilds cluster metadata (such as the cluster map (Smap)) - back up BMD, Smap, and cluster configuration
> as described in [Upgrades and metadata backup](/docs/configuration.md#upgrades-and-metadata-backup).
> This is not specific to IPv6.

### What "prefer IPv6" means

The `use_ipv6` flag expresses **preference**, not a hard requirement. At startup, each node:

1. attempts to find usable IPv6 unicast addresses, excluding link-local `fe80::/10`;
2. if none are found, falls back to IPv4 with a warning;
3. records the effective IP family for the lifetime of the process.

This means `use_ipv6: true` on a host with no routable IPv6 will not prevent the node from starting. The node will run on IPv4.

### Switching an existing cluster: IPv4 ↔ IPv6

An existing cluster can switch address families without changing cluster identity or affecting stored data:

```console
# 1. set the flag; the update propagates to all nodes via metasync
$ ais config cluster net.use_ipv6 true

# 2. restart the cluster
```

On restart, the primary builds a fresh Smap. Each node discovers its new addresses, for example `[::1]` instead of `127.0.0.1`, and joins with current network information. Node IDs are persistent, so the cluster retains its identity: same UUID, same BMD, same data.

The reverse switch, from `true` to `false`, works the same way.

**Precondition:** the `primary_url` in each node's local config must be a hostname, such as `localhost` or an FQDN, not a literal IP address. This allows DNS resolution to return the appropriate address for the current IP family. With a hardcoded IP such as `127.0.0.1`, nodes cannot find the primary after switching to IPv6.

### URL format

IPv6 addresses in URLs use bracket notation:

```text
http://[::1]:8080
http://[fd00::1]:51081
```

This applies to AIS-internal URLs, Smap entries, intra-cluster communication, and the CLI endpoint:

```console
$ AIS_ENDPOINT=http://[::1]:8080 ais show cluster
```

### Socket-level tuning

AIS sets low-latency traffic class on control-plane sockets: `IP_TOS` for IPv4 and `IPV6_TCLASS` for IPv6. The correct socket option is selected automatically based on the actual socket family, not the configuration flag.

This handles edge cases where a dual-stack dialer creates an IPv4 socket despite an IPv6 preference.

No operator action is required.

### Kubernetes

In Kubernetes deployments, the address family is determined by the CNI and cluster networking configuration, not by AIStore's `use_ipv6` flag. AIStore nodes accept whatever addresses the infrastructure assigns.

For Kubernetes-specific network configuration, including ports, CNI requirements, and bandwidth tuning, see [ais-k8s network configuration](https://github.com/NVIDIA/ais-k8s/blob/main/docs/network_configuration.md).

## Effective Networking and IP Family Selection

The configuration option:

```json
"net": {
    "use_ipv6": true
}
```

expresses **intent**, not a guarantee.

At startup, each node performs runtime network resolution:

1. attempts to resolve usable IPv6 addresses;
2. if unsuccessful, falls back to IPv4;
3. records the effective IP family for the node.

This decision is made once, early in initialization, and applies uniformly to:

* all HTTP listeners: public, intra-control, and intra-data;
* all intra-cluster HTTP clients;
* control-plane auxiliary clients, such as AuthN and JWKS fetch;
* socket options, including `IP_TOS` vs. `IPV6_TCLASS`;
* transport streams.

The original configuration is not mutated. Instead, the resolved choice becomes part of the node's effective runtime networking state.

> Once a node determines that IPv6 is not usable, it must not attempt IPv6 connections elsewhere, even if the configuration requested it.

This avoids mixed-family failures where a node listens on IPv4 but dials IPv6.

## Initialization Phases

AIS initialization separates networking concerns into two phases.

### Phase 1: Network Resolution and Server Construction

During phase 1, AIS:

* resolves local IPs;
* decides the effective IP family;
* constructs HTTP servers and muxers:
  - public;
  - intra-control;
  - intra-data;
* builds `Snode` network descriptors.

No sockets are opened yet.

### Phase 2: Client and Runtime Initialization

During phase 2, AIS:

* initializes TLS;
* creates intra-cluster HTTP clients using the effective IP family;
* initializes transport data movers;
* initializes owners, memory managers, and housekeeping;
* registers handlers;
* starts listeners.

This split guarantees that:

* handlers always attach to existing muxers;
* clients never contradict server address families;
* transport and HTTP paths use the same effective networking decision.

## Duplicate Runs and Duplicate Addresses

Two different problems may look alike in the logs ("in use", "duplicate"), but they have different causes and are detected in different places:

* a local listener conflict, including overlapping runs of the same node - detected by the starting target;
* two different node IDs advertising the same endpoint - checked cluster-wide, by the primary.

### Local listener conflicts and overlapping runs

A second `aisnode` process with the same node identity may start while the first one is still running or shutting down - for instance, a manual start racing with a service manager, or a restart that overlaps the previous instance.

Such a run can act on the node-restarted state (`node_restarted.prev` and the subsequent GFN sequence) before eventually failing to listen ("bind: address already in use"). A different AIS node or an unrelated application using the same local endpoint can also cause a listener conflict.

To catch overlaps before restart recovery proceeds, a target restarting after an unclean shutdown (that is, when the `node_restarted` marker is present):

* binds and immediately closes each effective local listener: intra-control, intra-data (when separate), and public;
* probes the public wildcard bind (`:PORT`) when the public port differs from the intra-cluster ports; otherwise, probes the configured public address and any additional multi-homed public addresses;
* when an endpoint is in use (`EADDRINUSE`), retries over the `timeout.max_keepalive` grace period to let a previous instance shut down;
* if the endpoint is still in use, exits with:

```console
t[gKVtRfza]: "10.50.56.205:51082" is in use (duplicate or overlapping run?)
```

Notes:

* The check is local: the node does not dial its own advertised addresses. In Kubernetes, a public address may belong to a load balancer or a Gateway that accepts connections whether or not any target is behind it.
* Bind-and-close is a best-effort check: it does not reserve ports, which leaves a window between the check and the actual listeners.
* Containers in the same pod share its network namespace and ports. Separate pods normally have distinct network namespaces, but pods using [`hostNetwork: true`](https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/) share the host's network namespace and can conflict with each other or host processes.
* Only targets perform this check.

### Duplicate addresses (misconfiguration)

A different problem: two nodes with different node IDs advertise the same address and TCP port - typically, due to a copy-pasted local configuration or a wrong `host_net.hostname`, `host_net.hostname_intra_control`, or `host_net.hostname_intra_data`. Sharing an IP address with distinct ports is valid, as in the local playground.

Binding local sockets checks for conflicts in the node's own network namespace. It cannot establish whether another node elsewhere advertises the same endpoint.

Instead, the primary compares network descriptors of all nodes in the cluster map. Two nodes conflict when any of their public, intra-control, or intra-data endpoints share the same `host:port` (in any combination). The primary checks for conflicts:

* at cluster startup, when merging cluster maps;
* on slow keepalive paths that add or update a registration;
* during self-join and admin-join, with a warning only.

Fast keepalives and slow keepalives that leave the registration unchanged bypass the endpoint comparison. A restarted target's keepalive can be promoted to self-join and follow the warning-only path.

The comparison uses advertised IP/DNS names and ports. It does not resolve DNS aliases or inspect additional public multi-home addresses.

Example:

```console
duplicate IPs: t[gKVtRfza] and t[aXBtgKqP] share the same "10.50.56.205:51082", Smap v412[ZfyqS4nvKb3, p[pZltOvK0], t=16, p=3]
```

Separately, when a node self-joins under an existing ID but with different network info (e.g., a new pod IP after restart), the primary queries its previously registered intra-control URL. If the responding node's ID or public/control/data endpoints differ from the joining node's report, the registration is rejected:

```console
duplicate node ID "gKVtRfza" (t[gKVtRfza], t[gKVtRfza])
```

Self-join can proceed after a retriable connection failure to the old control URL. Targets self-join before opening their listeners, so an unreachable endpoint during startup is inconclusive. Slow keepalives that change network information also use identity confirmation; failed confirmation can reject that keepalive.

In Kubernetes, the public hostname is not validated against local addresses (it may be a load balancer IP or a service DNS name) - make sure each node's external address is unique.

### Troubleshooting Duplicate Endpoints

#### Local listener conflicts

Use the address and port from the startup diagnostic to inspect listening sockets on the same host, or inside the same pod's network namespace:

```console
ss -ltnp
lsof -nP -iTCP:51082 -sTCP:LISTEN
```

Replace `51082` with the reported port. Check public, intra-control, and intra-data listeners, including wildcard binds: `:PORT` can conflict with a listener on a specific local address. Identify the owning process and its configuration before stopping it.

#### Conflicting cluster registrations

During self-join and admin-join, the primary logs a warning if a joining node's advertised endpoint conflicts with another node ID in the current Smap. The warning includes both IDs, the shared endpoint, Smap version, and the conflicting node's flags. It says `proceeding with admission`: this check does not reject the join or remove the existing registration.

To investigate:

1. Run `ais show cluster smap --json` against a reachable cluster endpoint. Compare the conflicting nodes' `public_net`, `intra_control_net`, and `intra_data_net` addresses and ports with their local `host_net` configurations.
2. In Kubernetes, check pod addresses, DNS resolution, and the selectors and backends of each per-target Service or LB. Confirm that each endpoint routes to the intended target.
3. Account for address reuse after a restart. A node retaining its ID can re-register with changed network information; another node's old registration may still contain the reused address.
4. Determine whether the conflicting node is still running. Maintenance or decommission flags alone do not establish that its registration is stale. Resolve configuration errors or use the normal [cluster membership procedures](/docs/cli/cluster.md) to handle an obsolete node.

Do not delete restart markers or Smap files merely to suppress a duplicate-endpoint diagnostic.

## Summary

* AIS uses three logical network roles: public, intra-cluster control, and intra-cluster data.
* Starting with v5.0, public traffic must not share an HTTP listener with intra-cluster traffic.
* Intra-control and intra-data may collapse into one intra-cluster endpoint.
* The three logical roles may map to fewer physical networks, provided the public endpoint remains separate.
* Production deployments should provision separate, dedicated bandwidth for public, control, and data roles.
* The control plane is low-bandwidth but latency-sensitive; it must be protected from bulk-data HoL blocking.
* The data path includes both regular net/http operations (e.g., intra-cluster GFN) and transport/bundle streams (which use fasthttp by default and bypass standard muxers).
* Public multi-homing is supported; intra-cluster multi-homing is not.
* With HTTPS, the default certificate must cover the nodes' advertised intra-cluster names; `alt_tls_dns_name` supports public certificates that omit the nodes' public hostnames.
* Targets probe local listeners after an unclean shutdown; the primary checks advertised endpoints across node IDs, with warnings only during self-join and admin-join.
* IPv6 is supported cluster-wide via `net.use_ipv6`; switching between IPv4 and IPv6 requires a config change and restart.
* IP family selection is resolved at runtime and applied consistently.
* DNS hostnames and Kubernetes FQDNs are preferred over hardcoded IP literals, especially for IPv4/IPv6 transitions.
* Operators should back up BMD, Smap, and configuration as described in [Upgrades and metadata backup](/docs/configuration.md#upgrades-and-metadata-backup).
