---
layout: post
title: TOOLS
permalink: /docs/tools
redirect_from:
 - /tools.md/
 - /docs/tools.md/
---

## ais

AIS CLI (`ais`) is an easy-to-use utility to perform data and cluster management operations (such as `create bucket`, `list objects`, `put object`, and many more).

For background, usage, tips, auto-completions, and numerous examples, please see [this document](/docs/cli.md).

For downloading and installing the latest binary release, run:

```console
$ go install github.com/NVIDIA/aistore/cmd/cli@latest` && mv $GOPATH/bin/cli $GOPATH/bin/ais
```

## aisfs

AIS FS (`aisfs`) is FUSE-based tool that enables regular file (aka POSIX) access to the AIStore.

For information on usage, see [this readme](/docs/aisfs.md).

For downloading and installing the latest binary release, run `go install github.com/NVIDIA/aistore/cmd/aisfs@latest`.

## aisloader

AIS Loader (`aisloader`) is a load-generating tool for run a vast variety of stress tests on the AIS cluster. In particular, `aisloader` generates synthetic workloads that emulate large-scale training and inference under stress.

For downloading and installing the latest binary release, run `go install github.com/NVIDIA/aistore/cmd/aisloader@latest`.

## xmeta

Low-level utility to format (or extract into plain text) assorted AIS control structures - see [usage](/cmd/xmeta/README.md).
