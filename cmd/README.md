## Table of Contents
- [Executables](#executables)
- [Installing from GitHub](#installing-from-github)
- [Installing from source](#installing-from-source)
  - [CLI](#cli)
  - [aisloader](#aisloader)
  - [xmeta](#xmeta)
  - [ishard](#ishard)
- [aisinit](#aisinit)

## Executables

| Directory | Binary name | Description  | README |
|---|---|---|---|
| `cmd/cli` | `ais` | AIS command line management utility | [CLI](/docs/cli.md) |
| `cmd/aisloader` | `aisloader` | AIS integrated load generator | [aisloader](/docs/aisloader.md) |
| `bench/tools/aisloader-composer` | `aisloader-composer` | Scripts and ansible playbooks to benchmark an AIS cluster using multiple hosts running [aisloader](https://github.com/NVIDIA/aistore/tree/main/bench/tools/aisloader), controlled by [ansible](https://github.com/ansible/ansible) | [aisloader-composer](https://github.com/NVIDIA/aistore/tree/main/bench/tools/aisloader-composer) |
| `cmd/aisnode` | `aisnode` | AIS node (gateway or target) binary | |
| `cmd/aisnodeprofile` | `aisnode` | ... with profiling enabled | |
| `cmd/aisinit` | `aisinit` | Kubernetes init container: generates `aisnode` config from pod environment (DNS, hostnames, networking mode, external LB) | |
| `cmd/authn` | `authn` | Standalone server providing token-based secure access to AIS clusters | [AuthN](/docs/authn.md) |
| `cmd/xmeta` | `xmeta` | Utility to inspect, extract, format, and (in limited cases) edit internal AIS metadata structures (cluster map, bucket metadata, etc.) | [xmeta](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md) |
| `cmd/ishard` | `ishard` | Utility to create well-formed shards from an original dataset | [ishard](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md) |

**NOTE**: installed CLI executable is named `ais`.

## Installing from GitHub

Generally, AIStore (cluster) requires at least some sort of [deployment](https://github.com/NVIDIA/aistore/tree/main/deploy#contents) process or sequence.

Standalone binaries, on the other hand, can be [built](https://github.com/NVIDIA/aistore/blob/main/Makefile) from source or installed directly from the latest or previous GitHub [releases](https://github.com/NVIDIA/aistore/releases).

Each [release](https://github.com/NVIDIA/aistore/releases) includes `ais` (CLI) and `aisloader` binaries for `linux-amd64` and `darwin-arm64`:

```console
$ ./scripts/install_from_binaries.sh --help
NAME:
  install_from_binaries.sh - install 'ais' (CLI) and 'aisloader' from release binaries

USAGE:
  ./install_from_binaries.sh [options...]

OPTIONS:
  --tmpdir <dir>        work directory, e.g. /root/tmp
  --dstdir <dir>        installation destination
  --release             release tag, e.g. v1.4.1, v1.4.3, latest (default: latest)
  --completions         install and enable _only_ CLI autocompletions (ie., skip installing binaries)
  -h, --help            show this help
```

**NOTE:** For CLI, the script will also enable auto-completions. CLI can be used without (bash, zsh) auto-completions but, generally, using auto-completions is strongly recommended.

### Example: download 'ais' and 'aisloader' binaries from the latest release

```console
$ ./scripts/install_from_binaries.sh --dstdir /tmp/qqq
```

> **Note**: AIS container images (`aisnode`, `aisinit`, `authn`, etc.) are not distributed via the GitHub release page or this install script. They are published separately and are also available with native **arm64** support. See [Kubernetes deployment](https://github.com/NVIDIA/ais-k8s) for details.

## Installing from source

### CLI

The preferable way is to use [Makefile](https://github.com/NVIDIA/aistore/blob/main/Makefile):

```console
$ make cli
```

builds AIS CLI from the local aistore repository and installs it in your $GOPATH/bin.

**NOTE**: installed CLI binary is named `ais`.

Alternatively, you could also use `go install`:

```console
$ go install github.com/NVIDIA/aistore/cmd/cli@latest && mv $GOPATH/bin/cli $GOPATH/bin/ais
```

To install CLI auto-completions, you could also, and separately, use `cmd/cli/install_autocompletions.sh`

### aisloader

[Makefile](https://github.com/NVIDIA/aistore/blob/main/Makefile) way:

```console
$ make aisloader
```

But again, you could also use `go install`:

```console
$ go install github.com/NVIDIA/aistore/cmd/aisloader@latest
```

### xmeta

`xmeta` is a utility to inspect, extract, format, and (in limited cases) edit internal AIS metadata structures - see [usage](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md).

For command line options and usage examples, simply run `xmeta` with no arguments:

```console
$ xmeta
Usage of xmeta:
  -f string ...
...
Examples:
        # Smap:
        xmeta -x -in=~/.ais0/.ais.smap      - extract Smap to STDOUT
...
```

To install, run:

```console
$ make xmeta
```

OR, same:

```console
$ cd cmd/xmeta
$ go install
```

### ishard

`ishard` is a utility to create well-formed shards from an original dataset - see [usage](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md).

To install, run:

```console
$ make ishard
```

Or use `go install`:

```console
$ go install github.com/NVIDIA/aistore/cmd/ishard@latest
```

## aisinit

`aisinit` is a Kubernetes init container that runs before `aisnode` in the same pod. It takes a template local configuration and produces the final node config based on the pod's runtime environment.

Specifically, `aisinit`:

- resolves pod DNS names for intra-cluster (data and control) networks
- sets the public hostname based on the configured DNS mode (`IP`, `Node`, or `Pod`)
- fetches external LoadBalancer IPs for targets when external access is enabled
- applies optional hostname mappings and cluster config overrides

The DNS mode (`AIS_PUBLIC_DNS_MODE`) controls how the public-facing hostname is determined:

| Mode | Hostname source | Typical use case |
|---|---|---|
| `IP` (default) | Host IP (discovered at runtime) | Simple deployments; requires IP SANs in TLS certs |
| `Node` | Kubernetes node name (`spec.nodeName`) | Environments where node names are resolvable (e.g., AWS EC2 private DNS) |
| `Pod` | Pod DNS (`pod.service.namespace.svc.cluster.local`) | Host networking + TLS with wildcard certs |

`aisinit` is not installed as a standalone binary. It is built into the `aisinit` container image and deployed via the [AIS Kubernetes operator](https://github.com/NVIDIA/ais-k8s).
