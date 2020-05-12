---
layout: post
title: MPATH
permalink: cmd/cli/resources/mpath
redirect_from:
 - cmd/cli/resources/mpath.md/
---

## Attach mountpath

`ais attach mountpath DAEMON_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Attach a mountpath on a specified target to AIS storage.

### Examples

```console
$ ais attach mountpath 12367t8080=/data/dir
```

## Detach mountpath

`ais detach mountpath DAEMON_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Detach a mountpath on a specified target from AIS storage.

### Examples

```console
$ ais detach mountpath 12367t8080=/data/dir
```
