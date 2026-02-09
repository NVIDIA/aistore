## AIStore Networking Model

AIStore (AIS) supports **three logical HTTP networks**, each with a distinct role, lifecycle, and performance profile:

1. **Public** (user-facing) network
2. **Intra-cluster control** network
3. **Intra-cluster data** network

These are *logical* networks. They may map to:

* three physically isolated fabrics (ideal production),
* a subset of those, or
* a single shared network (development / small deployments).

> NOTE: It is strongly recommended to (a) configure three logical networks, and (b) provision each one with **separate, dedicated bandwidth** (for example, to avoid head-of-line blocking that can impact latency-sensitive control-plane traffic).

Separately, AIS runs an additional distinct intra-cluster data plane:

### Two distinct data planes

(A) HTTP intra-data network (DataNet)
    - request/response style
    - used by target-to-target GET/PUT paths (related term: "GFN")
    - configured via host_net.{hostname_intra_data,port_intra_data}

        Target A                         Target B
     ┌───────────┐                    ┌───────────┐
     │ net/http  │  HTTP DataNet      │ net/http  │
     │ handlers  │◄──────────────────►│ handlers  │
     └───────────┘                    └───────────┘

(B) Transport/bundle streams (data movers)
    - high-volume streaming (rebalance, resilver, get-batch streams, etc.)
    - uses AIStore transport subsystem (bundle/DM/shared-DM, fasthttp, PDUs, etc.)
    - separate from net/http muxers and http.Clients

        Target A                          Target B
     ┌───────────┐   transport streams  ┌───────────┐
     │ transport │◄════════════════════►│ transport │
     │ bundle/DM │   (PDU/streaming)    │ bundle/DM │
     └───────────┘                      └───────────┘

See also: [transport package: README](https://github.com/NVIDIA/aistore/blob/main/transport/README.md)

Rest of this document is structured as follows:

**Table of Contents**

* [Two distinct data planes](#two-distinct-data-planes)
* [Logical vs Physical Networks](#logical-vs-physical-networks)
* [Public Network](#public-network)
* [Intra-Cluster Control Network](#intra-cluster-control-network)
* [Intra-Cluster Data Network](#intra-cluster-data-network)
* [Multi-Homing](#multi-homing)
* [IPv6](#ipv6)
  - [Enabling IPv6](#enabling-ipv6)
  - [What "prefer IPv6" means](#what-prefer-ipv6-means)
  - [Switching an existing cluster (IPv4-↔-IPv6)](#switching-an-existing-cluster-ipv4--ipv6)
  - [URL format](#url-format)
  - [Socket-level tuning](#socket-level-tuning)
  - [Kubernetes](#kubernetes)
* [Effective Networking and IP Family Selection](#effective-networking-and-ip-family-selection)
* [Initialization Phases (Networking-Relevant)](#initialization-phases-networking-relevant)
  - [Phase 1: Network Resolution and Server Construction](#phase-1-network-resolution-and-server-construction)
  - [Phase 2: Client and Runtime Initialization](#phase-2-client-and-runtime-initialization)
* [Summary](#summary)

### Logical vs Physical Networks

The presence of three logical networks **does not require** three physical networks:
                     ┌───────────────────────────────────────┐
                     │               Clients                 │
                     │   ais CLI / SDK / notebooks / tools   │
                     └───────────────────┬───────────────────┘
                                         │ PubNet (user-facing)
                                         ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                             AIStore Cluster                               │
│   ┌───────────────────────────────────────────────────────────────────┐   │
│   │                           Proxies (P)                             │   │
│   │                                                                   │   │
│   │   PubNet:     user-facing API, metrics, etc.                      │   │
│   │   CtlNet:     intra-cluster control-plane HTTP                    │   │
│   │   DataNet:    intra-cluster HTTP object paths (T2T GET/PUT)       │   │
│   └───────────────┬───────────────────────────┬───────────────────────┘   │
│                   │                           │                           │
│                   │ Intra-control HTTP        │ Intra-data HTTP           │
│                   ▼                           ▼                           │
│   ┌──────────────────────────────┐   ┌────────────────────────────────┐   │
│   │          Targets (T)         │   │           Targets (T)          │   │
│   │                              │   │                                │   │
│   │  CtlNet endpoint(s):         │   │  DataNet endpoint(s):          │   │
│   │    - metasync, health        │   │    - HTTP T2T GET/PUT          │   │
│   │    - membership, control     │   │    - HTTP object move paths    │   │
│   └──────────────────────────────┘   └────────────────────────────────┘   │
│ Notes:                                                                    │
│ * "PubNet/CtlNet/DataNet" are logical networks - may alias to one fabric. │
│ * Multi-homing applies to PubNet (multiple listen addresses).             │
└───────────────────────────────────────────────────────────────────────────┘

If multiple logical networks resolve to the same address and port, AIS **aliases** them internally and operates correctly over a single interface.

Example: single-network deployment

```json
"host_net": {
    "hostname": "10.50.56.205",
    "hostname_intra_control": "10.50.56.205",
    "hostname_intra_data": "10.50.56.205",
    "port": "51081",
    "port_intra_control": "51081",
    "port_intra_data": "51081"
}
```

In this case:

* public == control == data
* one listener, one HTTP server, one set of muxers

Compare with (preferred) configuration with 3 logical networks:

```console
$ ais config node t[mSwroVjn] local host_net --json
{
    "host_net": {
        "hostname": "10.50.56.208",
        "hostname_intra_control": "ais-target-8.ais-target.ais.svc.cluster.local",
        "hostname_intra_data": "ais-target-8.ais-target.ais.svc.cluster.local",
        "port": "51081",
        "port_intra_control": "51082",
        "port_intra_data": "51083"
    }
}
```

> NOTE: It is strongly recommended to (a) configure three logical networks, and (b) provision each one with **separate, dedicated bandwidth**.

### Public Network

The **public network** is user-facing and client-oriented:

* CLI (`ais`), SDKs, notebooks, browsers
* AuthN-enabled client requests
* Prometheus metrics
* External tooling

Characteristics:

* Lowest trust boundary
* May be multi-homed
* Tracing may be enabled
* Often fronted by load balancers or ingress

### Intra-Cluster Control Network

The **control network** carries cluster coordination traffic:

* metasync
* health checks
* membership changes
* xaction control paths
* internal API fanout

Characteristics:

* Low bandwidth
* Latency-sensitive
* Stable, predictable request volume
* Prefer isolation from data traffic

### Intra-Cluster Data Network

The **data network** carries HTTP-based object movement between [targets](/docs/overview.md#target):

* target <=> target GET
* target <=> target PUT
* HTTP-based object relocation paths

Important distinction:

> High-volume streaming operations such as rebalance, resilver, and get-batch
> **do not use net/http** and are **not part of the intra-cluster data HTTP network**.
> They use AIStore's transport/bundle subsystem (and fasthttp), which is a
> separate data-mover layer.

Thus, "data network" here means **HTTP object paths**, not transport streams.

### Multi-Homing

Public networking supports **multi-homing** to utilize multiple NICs without LACP:

```json
"host_net": {
    "hostname": "10.50.56.205,10.50.56.206",
    "port": "51081"
}
```

Effect:

* AIS listens on all listed addresses
* Clients may connect via any
* No additional configuration required

Multi-homing currently applies to the **public network** only.

### IPv6

AIStore supports IPv6 for all three logical networks and the transport streaming layer.

#### Enabling IPv6

IPv6 is a cluster-wide setting:

```console
$ ais config cluster net.use_ipv6 true
```

To take effect, the change requires a cluster restart. On restart, each node discovers local IPv6 addresses and binds all listeners (public, control, data) using IPv6 addresses.

> **Operational note**: As with any cluster-wide configuration change that rebuilds cluster metadata (e.g., cluster map (Smap)), it is good practice to periodically back up [cluster-level metadata](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md) (BMD, Smap, configuration). This is not specific to IPv6.

#### What "prefer IPv6" means

The `use_ipv6` flag expresses **preference**, not a hard requirement. At startup, each node:

1. Attempts to find usable IPv6 unicast addresses (excluding link-local `fe80::/10`)
2. If none are found, **falls back to IPv4** with a warning
3. Records the *effective* IP family for the lifetime of the process

This means `use_ipv6: true` on a host with no routable IPv6 will not prevent the node from starting - it will run on IPv4.

#### Switching an existing cluster (IPv4 <=> IPv6)

An existing cluster can switch address families without changing cluster identity or affecting stored data in any shape or form:

```console
# 1. set the flag (propagates to all nodes via metasync)
$ ais config cluster net.use_ipv6 true

# 2. restart the cluster
```

On restart, the primary builds a fresh Smap. Each node discovers its new addresses (e.g., `[::1]` instead of `127.0.0.1`) and joins with current net-info. Node IDs are persistent, so the cluster retains its identity - same UUID, same BMD, same data.

The reverse switch (`true` => `false`) works the same way.

**Precondition:** the `primary_url` in each node's local config must be a **hostname** (e.g., `localhost`, an FQDN), not a literal IP address. This allows DNS resolution to return the appropriate address for the current IP family. With a hardcoded IP like `127.0.0.1`, nodes cannot find the primary after switching to IPv6.

#### URL format

IPv6 addresses in URLs use bracket notation per RFC 2732:

```
http://[::1]:8080
http://[fd00::1]:51081
```

This applies to all AIS-internal URLs (Smap entries, intra-cluster communication) and to the CLI endpoint:

```console
$ AIS_ENDPOINT=http://[::1]:8080 ais show cluster
```

#### Socket-level tuning

AIS sets low-latency traffic class on control-plane sockets (`IP_TOS` for IPv4, `IPV6_TCLASS` for IPv6). The correct socket option is selected automatically based on the actual socket family - not the configuration flag. This handles edge cases where a dual-stack dialer creates an IPv4 socket despite an IPv6 preference.

No operator action is required.

#### Kubernetes

In Kubernetes deployments, the address family is determined by the CNI and cluster networking configuration - not by AIStore's `use_ipv6` flag. AIStore nodes accept whatever addresses the infrastructure assigns.

For K8s-specific network configuration (ports, CNI requirements, bandwidth tuning), see [ais-k8s network configuration](https://github.com/NVIDIA/ais-k8s/blob/main/docs/network_configuration.md).

### Effective Networking and IP Family Selection

The configuration option:

```json
"net": {
    "use_ipv6": true
}
```

expresses **intent**, not a guarantee.

At startup, each node performs **runtime network resolution**:

1. Attempt to resolve usable IPv6 addresses
2. If unsuccessful, **fall back to IPv4**
3. Record the *effective* IP family for the node

This decision is made **once**, early in initialization, and applies uniformly to:

* all HTTP listeners (public / control / data)
* all intra-cluster HTTP clients
* control-plane auxiliary clients (e.g., AuthN, JWKS fetch)
* socket options (`IP_TOS` vs `IPV6_TCLASS`)

The original configuration is **not mutated**.

Instead, the resolved choice becomes part of the node's effective runtime networking state.

> Once a node determines that IPv6 is not usable, it must not attempt IPv6
> connections elsewhere, even if the configuration requested it.

This avoids mixed-family failures where a node listens on IPv4 but dials IPv6.

### Initialization Phases (Networking-Relevant)

AIS initialization separates networking concerns into two phases:

#### Phase 1: Network Resolution and Server Construction

* resolve local IPs
* decide effective IP family
* construct HTTP servers and muxers:

  * public
  * intra-control
  * intra-data
* build `Snode` network descriptors

No sockets are opened yet.

#### Phase 2: Client and Runtime Initialization

* initialize TLS
* create intra-cluster HTTP clients using the **effective** IP family
* initialize owners, memory managers, housekeeping
* register handlers
* start listeners

This split guarantees:

* handlers always attach to existing muxers
* clients never contradict server address families

### Summary

* AIStore models **three logical HTTP networks** with additional intra-cluster data plane utilized by most [xactions](/docs/overview.md#xaction).
* They may map to fewer physical networks
* IPv6 is supported cluster-wide via `net.use_ipv6`; switching between IPv4 and IPv6 requires only a config change and restart
* IP family selection is resolved at runtime and applied consistently

Further, it is generally recommended:

* to provision each of the 3 logical networks with its separate, dedicated bandwidth;
* when considering IPv6, making sure the addresses are routable
* using fewer (or not using at all) hardcoded IP literals (DNS hostnames and K8s FQDNs are preferred);
* periodically back up [cluster-level metadata](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md) (BMD, Smap, configuration).
