---
layout: post
title: PROVIDERS
permalink: docs/providers
redirect_from:
 - docs/providers.md/
---

## Terminology

| Term                     | Definition                                                   |
| ------------------------ | ------------------------------------------------------------ |
| 3rd party cloud          | HTTP(S) accessible object storage with primary examples including Amazon S3, Google Cloud, and Microsoft Azure. |
| remote AIS cluster       | Let's say, there are two deployed AIS clusters: A and B. And let's also say that you can access cluster directly A via any/all of its gateways' URLs. If B gets *attached* to A (as shown below) then B becomes transparently accessibly via A and we call B *remote*. |
| unified global namespace | By *attaching* multiple AIS clusters to each other, we effectively create a super-cluster providing unified global namespace whereby all buckets and all objects of all included clusters are uniformly accessible via any and all individual access points (of those clusters). This *attaching* and *detaching* can be done ad-hoc and at any time without disrupting or otherwise affecting operations of any individual cluster. |

## Introduction

AIStore natively integrates with 3 (three) 3rd party Cloud storages:

* [Amazon S3](https://aws.amazon.com/s3)
* [Google Cloud Storage](https://cloud.google.com)
* [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs)

In each case, we use the vendor's own SDK/API to provide transparent access to Cloud storage with the additional capability of *persistently caching* all read data in the AIStore's [cloud buckets](bucket.md).

> The term "persistent caching" is used to indicate much more than what's conventionally understood as "caching": irrespectively of its origin and source, all data inside an AIStore cluster is end-to-end checksummed and protected by the [storage services](storage_svcs.md) configured both globally and on a per bucket basis. For instance, both cloud buckets and ais buckets can be erasure coded, etc.

> Notwithstanding, *cloud buckets* will often serve as a fast cache or a fast tier in front of a given 3rd party Cloud storage.

> Note as well that AIS provides [5 (five) easy ways to populate its *cloud buckets*](overview.md) - including, but not limited to conventional on-demand caching (aka *cold GET*).

But there's more.

In addition to the listed above 3rd party Cloud storages, AIS integrates with itself via its own RESTful API. One AIS cluster can be *attached* to another.

Between two AIS clusters A and B (see [Terminology](#Terminology) the same exact rules apply: as soon as B gets attached to A, any read access to (remote) objects and datasets from B will have the side effect of cluster A persistently caching those objects and datasets on its own clustered servers (aka storage targets), subject to the rules and policies configured on the corresponding A's buckets.

By *attaching* AIS clusters we are, effectively and ad-hoc, forming a unified global namespace of all individually hosted datasets.

---------------------

To reiterate, a storage bucket that is visible/accessible/modifiable via AIS may originate in a given AIS cluster, or:

* in a 3rd party Cloud, or
* in another AIS cluster, which we then respectively call *remote*

## Supported Cloud Providers

To reiterate, AIStore can be deployed as a fast tier in front of several storage backends. Supported *cloud providers* include: AIS (`ais`) itself, as well as AWS (`aws`), GCP (`gcp`), and Azure (`azure`), and all the respective S3, Google Cloud, and Azure compliant storages.

In the AIS [CLI](/aistore/cmd/cli/README.md), we use protocol prefixes to designate any specific Cloud Provider:

* `ais://` - for AIS
* `aws://` or `s3://` interchangeably - for S3
* `gcp://` or `gs://` - for Google Cloud Storage
* `azure://` - for Microsoft Azure.

Further:

* For additional information on working with buckets, please refer to [bucket readme](./bucket.md)
* For API reference, see [the RESTful API reference and examples](./http_api.md)
* For AIS command-line management, see [CLI](/aistore/cmd/cli/README.md)

### Unified Global Namespace

Examples first. The following two commands attach and then show remote cluster at the address`my.remote.ais:51080`:

```console
$ ais attach remote alias111=http://my.remote.ais:51080
Remote cluster (alias111=http://my.remote.ais:51080) successfully attached
$ ais show remote
UUID      URL                     Alias     Primary         Smap  Targets  Online
eKyvPyHr  my.remote.ais:51080     alias111  p[80381p11080]  v27   10       yes
```

Notice two aspects of this:

* user-defined aliasing whereby a user can assign an arbitrary name (aka alias) to a given remote cluster
* the remote cluster does *not* have to be online at attachment time; offline or currently not reachable clusters are shown as follows: 

```console
$ ais show remote
UUID        URL                       Alias     Primary         Smap  Targets  Online
eKyvPyHr    my.remote.ais:51080       alias111  p[primary1]     v27   10       no
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```

Notice the difference between the first and the second lines in the printout above: while both clusters appear to be currently offline (see the rightmost column), the first one was accessible at some earlier time and therefore we do show that it has (in this example) 10 storage nodes and other details.

To `detach` any of the previously configured association, simply run:

```console
$ ais detach remote alias111
$ ais show remote
UUID        URL                       Alias     Primary         Smap  Targets  Online
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```

----------

Configuration-wise, the following two examples specify a single-URL and multi-URL attachments that can be also be [configured](configuration.md) prior to runtime (*or* can be added at runtime via the `ais remote attach` CLI as shown above):

* Example: single URL

    ```json
    "cloud": {
        "ais": {
            "remote-cluster-alias": ["http://10.233.84.233:51080"]
        }
    }
    ```

* Example: multiple URL

    ```json
    "cloud": {
         "ais": {
              "remote-cluster-alias": [
                    "http://10.233.84.217",
                    "https://nvidia.ais-cluster.org",
              ]
          }
    }
    ```

> Multiple remote URLs can be provided for the same typical reasons that include fault tolerance.
> However, once connected we will rely on the remote cluster map to retry upon connection errors and load balance.

For more usage examples, please see [working with remote AIS bucket](bucket.md#cli-example-working-with-remote-ais-bucket).

And one final comment:

You can run `ais remote attach` and/or `ais show remote` CLI to *refresh* remote configuration: check availability and reload cluster maps.
In other words, repeating the same `ais attach remote` command will have the side effect of refreshing all the currently configured attachments.
Or, use `ais show remote` CLI for the same exact purpose.
