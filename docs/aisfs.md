---
layout: post
title: AISFS
permalink: /docs/aisfs
redirect_from:
 - /aisfs.md/
 - /docs/aisfs.md/
---

## Table of Contents

- [High-level architecture](#high-level-architecture)
- [Background](#background)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
  - [Installation](#installation)
  - [Quick Local Setup](#quick-local-setup)
  - [Configuration](#configuration)
    - [Updating configuration at runtime](#updating-configuration-at-runtime)
  - [Mounting](#mounting)
  - [Unmounting](#unmounting)

## High-level architecture

AISFS enables a non-privileged user to mount a single AIStore bucket
(one bucket per mount) in an empty directory on a local filesystem and access
objects as if they were regular files. System calls (such as file open, read, write
etc.) from applications are routed through the VFS layer to the Linux FUSE driver,
which in turn invokes the corresponding AISFS instance responsible for POSIX to
REST API mapping of the request. A communication diagram is shown in the figure
below.

![aisfs-high-level-architecture](images/aisfs-high-level-architecture.png)

> Clearly, there is a lot of data transfer involved between kernel space and user
space. This "up-and-down" communication between the kernel and user processes
is causing significant overhead, a performance weak spot for FUSE filesystems
in general. One of the main goals of FUSE client for AIStore is to give as good
performance as possible by reducing the effect of mentioned overhead.

## Background

FUSE (Filesystem in Userspace) is an interface in Linux and other Unix-like
operating systems that enables user space programs to export a filesystem
to the kernel. FUSE consists of a kernel module `fuse.ko`, a user space library
and a mount utility `fusermount`. There are currently over 100 FUSE filesystems
available on the Web - the fact that can likely be attributed to popularity
of the file (aka POSIX) access to storage on the one hand, and ease of
development and maintenance of the filesystem in userspace (as opposed to
kernel), on the other.

Generally, storage-backend-specific FUSE client provides a convenient
(albeit not necessarily fast) way to access and interact with a storage service.
Notable examples include
[s3fs](https://github.com/s3fs-fuse/s3fs-fuse) for Amazon S3 and
[gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse) for Google Cloud
Storage. The original motivation behind FUSE client for AIStore was the same:
provide a familiar filesystem interface for interactive work with AIStore buckets
and objects as well as native integration with Linux tools, libraries
and frameworks.

#### Namespace caching

To provide for faster access, `aisfs` periodically queries cluster via list-objects API and then updates its local cache. By totally eliminating or greatly reducing POSIX lookups, `aisfs` cache may significantly improve I/O throughput, especially when the workload "concentrates" inside few selected POSIX directories. Caching, however, does not affect read/write performance on the level of individual objects (ie., files). To state the same differently, user data is currently _not_ being cached on the file-client side.

Performance of the cache depends in part on its configuration described in the [configuration section](#configuration) below.

Note that the current implementation may not be as efficient for buckets containing large numbers of objects (files) with no subdirectories.

When the space required to cache the entire directory hierarchy and file names is larger than the configured memory limit the current implementations "falls" back to the regular mechanism that involves additional HTTP requests to AIS cluster.

## Prerequisites

* Linux
* `fuse.ko` FUSE kernel module
* `fusermount` mount CLI utility
* [Go 1.17 or later](https://golang.org/dl/)
* Running [AIStore](/README.md) cluster

Depending on your Linux distribution, you may or may not already have `fuse.ko`
and `fusermount` installed on your system. If you are using Ubuntu 18.04 LTS,
you can verify this by running the following commands (expect similar output to
the one shown below):

```console
$ grep -i fuse /lib/modules/$(uname -r)/modules.builtin
kernel/fs/fuse/fuse.ko
$ fusermount -V
fusermount version: 2.9.7
```

## Getting Started

### Installation

To install `aisfs` on your system, execute the following commands:

```console
$ cd $GOPATH/src/github.com/NVIDIA/aistore
$ make aisfs
```

Verify that `aisfs` has been successfully installed:

```console
$ aisfs -v
aisfs version 0.2 (build d9882586)
```

View help and usage information by running:

```console
$ aisfs --help
```

### Quick Local Setup

To quickly set up and try `aisfs` on your local machine, first
deploy an AIStore cluster (for more info see AIStore [README](/README.md)):

```console
$ cd $GOPATH/src/github.com/NVIDIA/aistore
$ make deploy
```

Using [AIS CLI](/docs/cli.md) create an ais bucket, download several objects
and place them into the bucket:

```console
$ ais bucket create mybucket
$ ais job start download "gs://lpr-vision/imagenet/imagenet_train-{000000..000010}.tgz" ais://mybucket/
Ys78ND09g
$ ais show job download Ys78ND09g --progress # wait for download to finish
```

Create an empty directory in your local filesystem:

```console
$ cd $HOME
$ mkdir localdir
```

Finally, mount a filesystem and run a filesystem server as a daemon using
the following command:

```console
$ aisfs mybucket localdir/
```

List objects in the bucket:

```console
$ ls localdir/
```

Unmount the file system and remove previously created local directory:

```console
$ fusermount -u localdir/
$ rmdir localdir
```

### Configuration

Some parameters of AISFS can be tuned through a JSON configuration file
that is loaded at startup. Only one JSON configuration will be loaded,
but multiple JSON files named `<bucket>_mount.json` can exist, allowing
separate configuration of each bucket mount. If the corresponding
JSON file is not found during startup, one will be generated with
default parameter values. By default, configuration files will be
placed in `$HOME/.config/aisfs`, but if `XDG_CONFIG_HOME` environment
variable is set, the location of these files will instead be
`$XDG_CONFIG_HOME/aisfs`.

An example of one configuration file:

```json
{
  "cluster": {
    "url": "http://127.0.0.1:8080"
  },
  "timeout": {
    "tcp_timeout": "60s",
    "http_timeout": "0s"
  },
  "periodic": {
    "sync_interval": "20m"
  },
  "log": {
    "error_file": "/var/log/aisfs.log",
    "debug_file": ""
  },
  "io": {
    "write_buf_size": 1048576
  },
  "memory_limit": "1GB"
}
```

| Config key | Description | Additional information  |
| ---------- | -------- | ------- |
| `cluster.url` | HTTP URL to AIS cluster. | |
| `timeout.tcp_timeout` | Determines how long AISFS will wait for TCP to establish connection to the cluster. | |
| `timouet.http_timeout` | Determines how long AISFS will wait for AIS to respond with initial data. | |
| `periodic.sync_interval` | Determines how often locally cached metadata is synced with AIS cluster. | Setting this value to `0` disables syncing with AIS. Disabling sync or setting it to high value can result in problems with consistency since AISFS can perceive the status of the objects differently. |
| `log.error_file` | Location where errors are written to. Must be an absolute path. | Empty value/string will result in writing errors to STDERR. |
| `log.debug_file` | Location where debug logs are written to. Must be an absolute path. | Empty value/string disables writing debug logs. |
| `io.write_buf_size` | Size of the buffer used to cache data during PUT/write operation. | High value can result in higher memory usage but also in better performance when writing large files. |
| `memory_limit` | Determines how much memory AISFS can use to cache metadata locally (like structure and filenames). Can be in format of raw numbers (`1024`) or with suffix `10MB`. | High value can result in much better performance for the most frequent operations. We recommend allowing as much memory to AISFS as it is possible. |


### Updating configuration at runtime

Even though loading [configuration](#configuration) takes an effect at [mount](#mounting) time it is still possible to reconfigure AISFS at runtime - without remounting.
This can be achieved by sending `SIGHUP` signal to `aisfs` daemon, e.g.:

```console
$ kill -HUP $(pidof aisfs)
```

`SIGHUP` causes running `aisfs` daemon to reload its configuration and apply updates to the following config variables:

* `periodic.sync_interval`
* `io.write_buf_size`
* `memory_limit`

In other words, if you'd want to, for instance, update AISFS memory limit, you can simply write a new value into AISFS configuration and apply it via `SIGHUP`.
Success or failure of the operation is reflected in the debug logs (if enabled).

### Mounting

It is not necessary to use a separate utility to mount a filesystem,
`aisfs` is both a filesystem server and a mount utility.

Assuming `mybucket` is a bucket name, `localdir` is an empty directory
in your local filesystem and an instance of AIStore cluster is running
on the local machine, the following command will mount `mybucket`
in `localdir`, making `localdir` the root directory of `aisfs`:

```console
$ aisfs mybucket localdir/
```

Filesystem is, by default, serviced by the user space background process (daemon), that will continue to run until the filesystem is unmounted.
To run the filesystem server in the foreground, pass the `--wait ` option to `aisfs`:

```console
$ aisfs --wait mybucket localdir/
```

A complete list of command-line options is given in the table below:

| Option        | Description |
| -----------   | ----------- |
| `--wait`      | Run a filesystem server in the foreground |
| `--uid`       | Mount owner's UID |
| `--gid`       | Mount owner's GID |
| `-o`          | Additional mount options to be passed to `mount` (see `man 8 mount`) |
| `--help,-h`   | Print help and exit |
| `--version,-v`| Print version and exit |

> Note: Mount owner is the user who does the mounting, not necessarily
the user who will perform filesystem operations.

#### FUSE control filesystem

A control filesystem for FUSE should be mounted under
`/sys/fs/fuse/connections`. Under this filesystem each FUSE connection
has a directory named by a unique number, containing the following files:

| File | Usage |
| ---- | ----- |
| `abort` | Writing anything into this file will abort the filesystem connection. Note that the filesystem will not respond to requests but it will remain mounted. |
| `waiting` | Reading from this file shows the number of requests which are waiting to be transferred to userspace or being processed by the filesystem server. |
| `max_background` | Reading from this file shows the maximum number of asynchronous requests residing in the pending queue. |
| `congestion_threshold` | Reading from this file shows the number of asynchronous requests in the pending queue and processed by the filesystem server that, if reached, will cause FUSE to inform the Linux VFS that it is congested. |

> Note: The control filesystem is managed by the kernel, meaning that file
reading/writing, maintaining directory tree etc, is done by the kernel.

For more information about implementation details of FUSE, visit this
[page](https://www.kernel.org/doc/Documentation/filesystems/fuse.txt), or
read the paper ["To FUSE or Not to FUSE: Performance of User-Space File Systems"](https://www.usenix.org/system/files/conference/fast17/fast17-vangoor.pdf).

### Unmounting

There are three methods for unmounting the AISFS filesystem and stopping
the AISFS user space daemon:

1. Use the `fusermount` utility. The `-u` option requests unmounting:

    ```console
    $ fusermount -u localdir/
    ```

2. Use the `umount` command:

    ```console
    $ umount localdir/
    ```

    You may need root privileges to execute this command.

3. If `aisfs` is invoked in the foreground by passing the `--wait` option, terminate the process by pressing Ctrl-C. This will also unmount the filesystem.
