# AIStore Networking Model

AIStore (AIS) separates traffic across three logical network roles:

* **Public network**: user-facing API, CLI/SDK traffic, metrics, and external tooling.
* **Intra-cluster control network**: health, metasync, membership, xaction control, and internal control-plane broadcast.
* **Intra-cluster data network**: object movement between AIS nodes, including target-to-target GET/PUT, global rebalance, resilver, get-batch streams, and other data-mover paths.

These are **logical roles**. They may map to separate physical networks, or they may share physical infrastructure, depending on deployment size and performance requirements. Starting with v5.0, however, AIS requires the public HTTP endpoint to be distinct from intra-cluster endpoints.

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

Here, public and intra-cluster traffic use the same loopback interface, but different TCP endpoints.

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

## IPv6

AIStore supports IPv6 for all three logical network roles and for the transport streaming layer.

### Enabling IPv6

IPv6 is a cluster-wide setting:

```console
$ ais config cluster net.use_ipv6 true
```

To take effect, the change requires a cluster restart. On restart, each node discovers local IPv6 addresses and binds all listeners using IPv6 addresses when usable IPv6 addresses are available.

> **Operational note:** As with any cluster-wide configuration change that rebuilds cluster metadata, such as the cluster map (Smap), it is good practice to periodically back up [cluster-level metadata](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md), including BMD, Smap, and configuration. This is not specific to IPv6.

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

## Summary

* AIS uses three logical network roles: public, intra-cluster control, and intra-cluster data.
* Starting with v5.0, public traffic must not share an HTTP listener with intra-cluster traffic.
* Intra-control and intra-data may collapse into one intra-cluster endpoint.
* The three logical roles may map to fewer physical networks, provided the public endpoint remains separate.
* Production deployments should provision separate, dedicated bandwidth for public, control, and data roles.
* The control plane is low-bandwidth but latency-sensitive; it must be protected from bulk-data HoL blocking.
* The data path includes both regular net/http operations (e.g., intra-cluster GFN) and transport/bundle streams (which use fasthttp by default and bypass standard muxers).
* Public multi-homing is supported; intra-cluster multi-homing is not.
* IPv6 is supported cluster-wide via `net.use_ipv6`; switching between IPv4 and IPv6 requires a config change and restart.
* IP family selection is resolved at runtime and applied consistently.
* DNS hostnames and Kubernetes FQDNs are preferred over hardcoded IP literals, especially for IPv4/IPv6 transitions.
* Operators should periodically back up [cluster-level metadata](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md), including BMD, Smap, and configuration.
