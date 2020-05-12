---
layout: post
title: REMOTE
permalink: cmd/cli/resources/remote
redirect_from:
 - cmd/cli/resources/remote.md/
---

CLI allows a user to attach any remove AIS cluster and works transparently with its buckets and objects. See more about remote clusters and examples [here](/aistore/docs/providers.md).

## Attach remote cluster

`ais attach remote UUID=URL [UUID=URL...]`

or

`ais attach remote ALIAS=URL [ALIAS=URL...]`

Attach a remote AIS cluster to this one by the remote cluster public URL. Alias(a user-defined name) can be used instead of cluster UUID for convenience.

### Examples

First cluster is attached by its UUID, the second one gets user-friendly alias.

```console
$ ais attach remote a345e890=http://one.remote:51080 two=http://two.remote:51080`
```

## Detach cluster

`ais detach remote UUID|ALIAS`

Detach a remote cluster from AIS storage by its alias or UUID.

### Examples

```console
$ ais detach remote two
```
