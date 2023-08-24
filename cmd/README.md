## Table of Contents
- [Executables](#executables)
- [Installing from GitHub](#installing-from-github)
- [Installing from source](#installing-from-source)

## Executables

| Directory | Binary name | Description  | README |
|---|---|---|---|
| `cmd/cli` | `ais` | AIS command line management utility | [CLI](/docs/cli.md) |
| `cmd/aisloader` | `aisloader` | AIS integrated load generator | [aisloader](/docs/aisloader.md) |
| `cmd/aisnode` | `aisnode` | AIS node (gateway or target) binary | |
| `cmd/aisnodeprofile` | `aisnode` | ... with profiling enabled | |
| `cmd/authn` | `authn` | Standalone server providing token-based secure access to AIS clusters | [AuthN](/docs/authn.md) |
| `cmd/xmeta` | `xmeta` | Low-level tool to format (or extract in plain text) assorted AIS metadata and control structures | [xmeta](/cmd/xmeta/README.md) |

**NOTE**: installed CLI executable is named `ais`.

## Installing from GitHub

Generally, AIStore (cluster) requires at least some sort of [deployment](/deploy#contents) process or sequence.

Standalone binaries, on the other hand, can be [built](Makefile) from source or installed directly from the latest or previous GitHub releases.

**NOTE:** binary installation is supported only for the `linux-amd64` platform.

In particular:

```console
$ ./deploy/scripts/install_from_binaries.sh --help

NAME:
  install_from_binaries.sh - install 'ais' (CLI) and 'aisloader' from release binaries

USAGE:
  ./install_from_binaries.sh [options...]

OPTIONS:
  --tmpdir <dir>        work directory, e.g. /root/tmp
  --dstdir <dir>        installation destination
  --release             e.g., v1.3.15, v1.3.16, latest (default: latest)
  --completions         install and enable _only_ CLI autocompletions (ie., skip installing binaries)
```

**NOTE:** For CLI, the script will also enable auto-completions. CLI can be used without (bash, zsh) auto-completions but, generally, using using auto-completions is strongly recommended.

### Example: download 'ais' and 'aisloader' binaries from the latest release

```console
$ ./install_from_binaries.sh --dstdir /tmp/qqq
```

Upon execution, the two specific `linux-amd64` binaries, ready for usage, will be placed in `/tmp/qqq` destination.

## Installing from source

### CLI

The preferable way is to use [Makefile](/Makefile):

```console
$ make cli
```

builds AIS CLI from the local aistore repository and installs it in your $GOPATH/bin.

**NOTE**: installed CLI binary is named `ais`.

Alternatively, you could also use `go install`:

```console
$ go install github.com/NVIDIA/aistore/cmd/cli@latest` && mv $GOPATH/bin/cli $GOPATH/bin/ais
```

To install CLI auto-completions, you could also, and separately, use `cmd/cli/install_autocompletions.sh`

### aisloader

[Makefile](/Makefile) way:

```console
$ make aisloader
```

But again, you could also use `go install`:

```console
$ go install github.com/NVIDIA/aistore/cmd/aisloader@latest
```

## xmeta

`xmeta` is a low-level utility to format (or extract and show) assorted AIS control structures - see [usage](/cmd/xmeta/README.md).

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
