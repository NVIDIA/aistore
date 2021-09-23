---
layout: post
title: PROVIDERS
permalink: /docs/providers
redirect_from:
 - /providers.md/
 - /docs/providers.md/
---

## Introduction

AIStore natively integrates with multiple backend providers:

| Backend | Schema(s) | Description |
| ------- | ----------- | ----------- |
| `ais` | `ais://`, `ais://@remote_uuid` | AIStore bucket provider, can also refer to [remote AIS cluster](#remote-ais-cluster) |
| `aws` | `aws://`, `s3://` | [Amazon Cloud Storage](#cloud-object-storage) |
| `azure` | `azure://`, `az://` | [Azure Cloud Storage](#cloud-object-storage)|
| `gcp` | `gcp://`, `gs://` | [Google Cloud Storage](#cloud-object-storage) |
| `hdfs` | `hdfs://` | [Hadoop Distributed File System](#hdfs-provider) |
| `ht` | `ht://` | [HTTP(S) based dataset](#https-based-dataset) |

The full taxonomy of the supported backends is shown below (and note that AIS supports itself on the back as well):

![Supported Backends](images/supported-backends.png)

Further:

* For additional information on working with buckets, please refer to [bucket readme](bucket.md).
* For API reference, see [the RESTful API reference and examples](http_api.md).
* For AIS command-line management, see [CLI](/docs/cli.md).

## Remote AIS cluster

In addition to the listed above 3rd party Cloud storages and non-Cloud HTTP(S) based datasets, AIS *integrates with itself* via its own RESTful API.
In other words, one AIS cluster can be *attached* to another (to transparently access and replicate each other's distributed storage).

Between two AIS clusters A and B the same exact rules apply: as soon as B gets attached to A, any read access to (remote) objects and datasets from B will have the side effect of cluster A persistently caching those objects and datasets on its own clustered servers (aka storage targets), subject to the rules and policies configured on the corresponding A's buckets.
By *attaching* AIS clusters we are, effectively and ad-hoc, forming a unified global namespace of all individually hosted datasets.

> Example working with remote AIS cluster (as well as easy-to-use scripts) can be found in the [README for developers](development.md).

### Unified Global Namespace

Examples first. The following two commands attach and then show remote cluster at the address`my.remote.ais:51080`:

```console
$ ais cluster attach alias111=http://my.remote.ais:51080
Remote cluster (alias111=http://my.remote.ais:51080) successfully attached
$ ais show remote-cluster
UUID      URL                     Alias     Primary         Smap  Targets  Online
eKyvPyHr  my.remote.ais:51080     alias111  p[80381p11080]  v27   10       yes
```

Notice two aspects of this:

* user-defined aliasing whereby a user can assign an arbitrary name (aka alias) to a given remote cluster
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

----------

Configuration-wise, the following two examples specify a single-URL and multi-URL attachments that can be also be [configured](configuration.md) prior to runtime (*or* can be added at runtime via the `ais remote attach` CLI as shown above):

* Example: single URL

    ```json
    "backend": {
      "ais": {
        "remote-cluster-alias": ["http://10.233.84.233:51080"]
      }
    }
    ```

* Example: multiple URL

    ```json
    "backend": {
      "ais": {
        "remote-cluster-alias": [
          "http://10.233.84.217",
          "https://cluster.aistore.org"
        ]
      }
    }
    ```

> Multiple remote URLs can be provided for the same typical reasons that include fault tolerance.
> However, once connected we will rely on the remote cluster map to retry upon connection errors and load balance.

For more usage examples, please see [working with remote AIS bucket](bucket.md#cli-example-working-with-remote-ais-bucket).

And one final comment:

You can run `ais remote attach` and/or `ais show remote-cluster` CLI to *refresh* remote configuration: check availability and reload cluster maps.
In other words, repeating the same `ais cluster attach` command will have the side effect of refreshing all the currently configured attachments.
Or, use `ais show remote-cluster` CLI for the same exact purpose.

## Cloud object storage

Cloud-based object storage include:
* `aws` - [Amazon S3](https://aws.amazon.com/s3)
* `azure` - [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs)
* `gcp` - [Google Cloud Storage](https://cloud.google.com)

In each case, we use the vendor's own SDK/API to provide transparent access to Cloud storage with the additional capability of *persistently caching* all read data in the AIStore's [remote buckets](bucket.md).

> The term "persistent caching" is used to indicate much more than what's conventionally understood as "caching": irrespectively of its origin and source, all data inside an AIStore cluster is end-to-end checksummed and protected by the [storage services](storage_svcs.md) configured both globally and on a per bucket basis.
> For instance, both remote buckets and ais buckets can be erasure coded, etc.

> Notwithstanding, *remote buckets* will often serve as a fast cache or a fast tier in front of a given 3rd party Cloud storage.

> Note as well that AIS provides [5 (five) easy ways to populate its *remote buckets*](overview.md) - including, but not limited to conventional on-demand caching (aka *cold GET*).

## HDFS Provider

Hadoop and HDFS is well known and widely used software for distributed processing of large datasets using MapReduce model.
For years, it has been considered as a standard for big data.

HDFS backend provider is a way to access files contained inside the HDFS cluster from AIStore.
Here we will talk about standard configuration and usages (see also [full tutorial on HDFS provider](/docs/tutorials/various/hdfs_backend.md)).

### Configuration

Before we jump to functionalities, let's first focus on configuration.
AIStore needs to know the address of NameNode server and the username for the requests.
Important note here is that the NameNode and DataNode addresses must be accessible from the AIStore, otherwise the connection will fail.

Example of HDFS provider configuration:
```json
"backend": {
  "hdfs": {
    "user": "root",
    "addresses": ["localhost:8020"],
    "use_datanode_hostname": false
  }
}
```

* `user` specifies which HDFS user the client will act as.
* `addresses` specifies the namenode(s) to connect to.
* `use_datanode_hostname` specifies whether the client should connect to the datanodes via hostname (which is useful in multi-homed setups) or IP address, which may be required if DNS isn't available.

### Usage

After the HDFS is set up, and the binary is built with HDFS provider, we can see everything in action.
```console
$ ais bucket create hdfs://yt8m --bucket-props="extra.hdfs.ref_directory=/part1/video"
"hdfs://yt8m" bucket created
$ ais bucket ls hdfs://
HDFS Buckets (1)
  hdfs://yt8m
$ ais object put 1.mp4 hdfs://yt8m/1.mp4
PUT "1.mp4" into bucket "hdfs://yt8m"
$ ais bucket ls hdfs://yt8m
NAME	 SIZE
1.mp4	 76.31KiB
$ ais object get hdfs://yt8m/1.mp4 video.mp4
GET "1.mp4" from bucket "hdfs://yt8m" as "video.mp4" [76.31KiB]
```

The first thing to notice is `--bucket-props="extra.hdfs.ref_directory=/part1/video"`.
Here we specify the **required** path the `hdfs://yt8m` bucket will refer to (the directory must exist on bucket creation).
It means that when accessing object `hdfs://yt8m/1.mp4` the path will be resolved to `/part1/video/1.mp4` (`/part1/video` + `1.mp4`).

## HTTP(S) based dataset

AIS bucket may be implicitly defined by HTTP(S) based dataset, where files such as, for instance:

* https://a/b/c/imagenet/train-000000.tar
* https://a/b/c/imagenet/train-123456.tar
* ...
* https://a/b/c/imagenet/train-999999.tar

would all be stored in a single AIS bucket that would have a protocol prefix `ht://` and a bucket name derived from the *directory* part of the URL Path ("a/b/c/imagenet", in this case).

WARNING: Currently HTTP(S) based datasets can only be used with clients which support an option of overriding the proxy for certain hosts (for e.g. `curl ... --noproxy=$(curl -s G/v1/cluster?what=target_ips)`).
If used otherwise, we get stuck in a redirect loop, as the request to target gets redirected via proxy.
