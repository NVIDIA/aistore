# Attach, Detach, and monitor remote clusters

For details and background on *remote clustering*, please refer to this [document](/docs/providers.md).

## Attach remote cluster

`ais cluster attach UUID=URL [UUID=URL...]`

or

`ais cluster attach ALIAS=URL [ALIAS=URL...]`

Attach a remote AIS cluster to this one by the remote cluster public URL. Alias(a user-defined name) can be used instead of cluster UUID for convenience.

### Examples

First cluster is attached by its UUID, the second one gets user-friendly alias.

```console
$ ais cluster attach a345e890=http://one.remote:51080 two=http://two.remote:51080`
```

## Detach remote cluster

`ais cluster detach UUID|ALIAS`

Detach a remote cluster from AIS storage by its alias or UUID.

### Examples

```console
$ ais cluster detach two
```

## Show remote clusters

The following two commands attach and then show remote cluster at the address`my.remote.ais:51080`:

```console
$ ais cluster attach alias111=http://my.remote.ais:51080
Remote cluster (alias111=http://my.remote.ais:51080) successfully attached
$ ais show remote-cluster
UUID      URL                     Alias     Primary         Smap  Targets  Online
eKyvPyHr  my.remote.ais:51080     alias111  p[80381p11080]  v27   10       yes
```

Notice that:

* user can assign an arbitrary name (aka alias) to a given remote cluster
* the remote cluster does *not* have to be online at attachment time; offline or currently not reachable clusters are shown as follows:

```console
$ ais show remote-cluster
UUID        URL                       Alias     Primary         Smap  Targets  Online
eKyvPyHr    my.remote.ais:51080       alias111  p[primary1]     v27   10       no
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```

Notice the difference between the first and the second lines in the printout above: while both clusters appear to be currently offline (see the rightmost column), the first one was accessible at some earlier time and therefore we do show that it has (in this example) 10 storage nodes and other details.

To `detach` any of the previously configured association, simply run:

```console
$ ais cluster detach alias111
$ ais show remote-cluster
UUID        URL                       Alias     Primary         Smap  Targets  Online
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```
