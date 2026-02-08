## AIStore Networking Model

AIStore (AIS) supports **three logical HTTP networks**, each with a distinct role, lifecycle, and performance profile:

1. **Public** (user-facing) network
2. **Intra-cluster control** network
3. **Intra-cluster data** network

These are *logical* networks. They may map to:

* three physically isolated fabrics (ideal production),
* a subset of those, or
* a single shared network (development / small deployments).

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

The **data network** carries HTTP-based object movement between targets:

* target ↔ target GET
* target ↔ target PUT
* HTTP-based object relocation paths

Important distinction:

> High-volume streaming operations such as rebalance, resilver, and get-batch
> **do not use net/http** and are **not part of the intra-cluster data HTTP network**.
> They use AIStore’s transport/bundle subsystem (and fasthttp), which is a
> separate data-mover layer.

Thus, “data network” here means **HTTP object paths**, not transport streams.

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

The original configuration is **not mutated**.

Instead, the resolved choice becomes part of the node’s effective runtime networking state.

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

* AIStore models **three logical HTTP networks**
* They may map to fewer physical networks
* High-volume data movement uses a **separate transport subsystem**
* IP family selection is resolved at runtime and applied consistently
* Configuration expresses intent; runtime state expresses reality
