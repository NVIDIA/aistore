---
layout: post
title: HDFS_BACKEND
permalink: ./tutorials/various/hdfs_backend
redirect_from:
 - /tutorials/various/hdfs_backend.md/
 - /docs/tutorials/various/hdfs_backend.md/
---

# HDFS Backend Provider

HDFS is an open source framework that is used to efficiently store and process large datasets ranging in sizes from gigabytes to petabytes.
Instead of using one large computer to store and process the data, Hadoop allows clustering multiple computers to analyze massive datasets in parallel more quickly.

For years, it has been a standard in big data community and is used to this day for big data processing.

To make sure you can easily access the data from HDFS using AIStore we have added HDFS backend provider.
In this tutorial we will see how to configure and use AIStore with HDFS backend.

## Prerequisites

In this tutorial we will assume that you already have connection to the HDFS cluster.
If you don't, here is short description on how you can set this up.

```console
$ # Run docker with HDFS up and running.
$ docker run -p 8020:8020 -p 9000:9000 -p 50010:50010 -p 50070:50070 -d harisekhon/hadoop
7e565201ce175dca4305731734dfa7a55d42d91f25fd9327bb844999963d444e
$ docker ps
CONTAINER ID   IMAGE               COMMAND                   CREATED         STATUS         PORTS                                                                                                                                                                           NAMES
7e565201ce17   harisekhon/hadoop   "/bin/sh -c \"/entryp…"   3 seconds ago   Up 2 seconds   8042/tcp, 0.0.0.0:8020->8020/tcp, 8088/tcp, 10020/tcp, 0.0.0.0:9000->9000/tcp, 19888/tcp, 0.0.0.0:50010->50010/tcp, 50020/tcp, 50075/tcp, 0.0.0.0:50070->50070/tcp, 50090/tcp   jovial_haslett
$
$ # Now edit the `/etc/hosts` so the `7e565201ce17` hostname can be resolved.
$ cat /etc/hosts
127.0.0.1   7e565201ce17
```

There are a couple other solutions which you can consider:
* https://github.com/Gradiant/charts
* https://artifacthub.io/packages/helm/gradiant/hdfs
* https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/quick-start-guide.md

## Configuration

We have our HDFS cluster running, so now we need to make sure that the AIStore will be able to connect to it.
To do it, we must edit config file.
Open `deploy/dev/local/aisnode_config.sh` and edit HDFS configuration.
Example:
```json
{
  "user": "root",
  "addresses": ["localhost:8020"],
  "use_datanode_hostname": true
}
```

* `user` specifies which HDFS user the client will act as.
* `addresses` specifies the namenode(s) to connect to.
* `use_datanode_hostname` specifies whether the client should connect to the datanodes via hostname (which is useful in multi-homed setups) or IP address, which may be required if DNS isn't available.

## Deploy AIStore

Once, we have our HDFS connection configured we can deploy the AIStore with HDFS support.

```console
$ make deploy
Enter number of storage targets:
5
Enter number of proxies (gateways):
5
Number of local cache directories (enter 0 for preconfigured filesystems):
3
Select backend providers:
Amazon S3: (y/n) ?
n
Google Cloud Storage: (y/n) ?
n
Azure: (y/n) ?
n
HDFS: (y/n) ?
y
Would you like to create loopback mount points: (y/n) ?
n
```

Notice `y` answered for HDFS provider question.

Now we are also ready to prepare CLI for the next steps.
We can simply do it with:
```console
$ make cli
Building ais (CLI)...
*** To enable autocompletions in your current shell, run:
*** source /Users/virrages/go/src/github.com/NVIDIA/aistore/cmd/cli/autocomplete/bash or
*** source /Users/virrages/go/src/github.com/NVIDIA/aistore/cmd/cli/autocomplete/zsh
```

And we are ready to play with HDFS backend.

## Accessing HDFS data

Before we can access the HDFS data, we must create a bucket on the AIStore.
The problem is that there isn't notion of a bucket in HDFS world.
Therefore, we must point the bucket to the specific directory in HDFS that will act as an entry point for all subsequent requests.

On my local HDFS cluster setup I have following directory structure:
```console
$ hdfs dfs -ls -R /
drwxr-x---   - root supergroup          0 2021-01-22 10:42 /yt8m
-rw-r--r--   1 root supergroup      78141 2021-01-22 10:42 /yt8m/1.mp4
-rw-r--r--   1 root supergroup      78141 2021-01-22 10:42 /yt8m/2.mp4
-rw-r--r--   1 root supergroup      78141 2021-01-22 10:42 /yt8m/3.mp4
```

Therefore, I can point my new bucket to `/yt8m` directory, and I should be able to access all the files contained in it.
Let's try it out.

```console
$ ais bucket create hdfs://yt8m --bucket-props="extra.hdfs.ref_directory=/yt8m"
"hdfs://yt8m" bucket created
$ ais bucket ls hdfs://yt8m
NAME	 SIZE
1.mp4	 76.31KiB
2.mp4	 76.31KiB
3.mp4	 76.31KiB
```

Perfect! As you can see the names inside the bucket don't have `/yt8m/` prefix because we treat them relative to the `extra.hdfs.ref_directory` directory.
Remember that `extra.hdfs.ref_directory` directory must exist before you try creating a bucket, otherwise the error will show up.

Okay, we are ready to list all the files, but we also need to make sure that we can get them.
Otherwise, this whole stuff wouldn't be so interesting.

```console
$ ais object get hdfs://yt8m/1.mp4 video.mp4
GET "1.mp4" from bucket "hdfs://yt8m" as "video.mp4" [76.31KiB]
$ ls -l | grep "video.mp4"
.rw-r--r--  78k user 22 Jan 11:54 video.mp4
```

Everything as expected :)
That's all in this tutorial.
We encourage playing with HDFS backend provider a little more to get the grasp of all possibilities.
