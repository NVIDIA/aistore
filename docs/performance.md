AIStore is all about performance. It's all about performance and reliability, to be precise. An assortment of tips and recommendations that you find in this text will go a long way to ensure that AIS _does_ deliver.

But first, one important consideration:

Currently(**) AIS utilizes local filesystems - local formatted drives of any kind. You could use HDDs and/or NVMes, and any Linux filesystem: xfs, zfs, ext4... you name it. In all cases, the resulting throughput of an AIS cluster will be the sum of throughputs of individual drives.

Usage of local filesystems has its pros and cons, its inevitable trade-offs. In particular, one immediate implication is called [`ulimit`](#maximum-number-of-open-files) - the limit on the maximum number of open file descriptors. We do recommend to fix that specific tunable right away - as described [below](#maximum-number-of-open-files).

The second related option is [`noatime`](#noatime) - and the same argument applies. Please make sure to review and tune-up them both.

> (**) The interface between AIS and a local filesystem is extremely limited and boils down to writing/reading key/value pairs (where values are, effectively, files) and checking existence of keys (filenames). Moving AIS off of POSIX to some sort of (TBD) key/value storage engine should be a straightforward exercise. Related expectations/requirements include reliability and performance under stressful workloads that read and write "values" of 1MB and greater in size.

- [Operating System](#operating-system)
- [CPU](#cpu)
- [Network](#network)
- [Smoke test](#smoke-test)
- [Maximum number of open files](#maximum-number-of-open-files)
- [Storage](#storage)
  - [Disk priority](#disk-priority)
  - [Benchmarking disk](#benchmarking-disk)
  - [Local filesystem](#local-filesystem)
  - [`noatime`](#noatime)
- [Virtualization](#virtualization)
- [Metadata write policy](#metadata-write-policy)
- [PUT latency](#put-latency)
- [GET throughput](#get-throughput)
- [`aisloader`](#aisloader)

## Operating System

There are two types of nodes in AIS cluster:

* targets (ie., storage nodes)
* proxies (aka gateways)

> AIS target must have a data disk, or disks. AIS proxies do not "see" a single byte of user data - they implement most of the control plane and provide access points to the cluster.

The question, then, is how to get the maximum out of the underlying hardware? How to improve datapath performance. This and similar questions have one simple answer: **tune-up** target's operating system.

Specifically, `sysctl` selected system variables, such as `net.core.wmem_max`, `net.core.rmem_max`, `vm.swappiness`, and more - here's the approximate list:

* [sysctl](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/host-config/vars/host_config_sysctl.yml)

The document is part of a separate [repository](https://github.com/NVIDIA/ais-k8s) that serves the (specific) purposes of deploying AIS on **bare-metal Kubernetes**. The repo includes a number of [playbooks](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/README.md) to assist in a full deployment of AIStore.

In particular, there is a section of pre-deployment playbooks to [prepare AIS nodes for deployment on bare-metal Kubernetes](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/host-config/README.md)

General references:

* [Ubuntu Performance Tuning (archived)](https://web.archive.org/web/20190811180754/https://wiki.mikejung.biz/Ubuntu_Performance_Tuning) <- good guide about general optimizations (some of them are described below)
* [RHEL 7 Performance Tuning Guide](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/pdf/performance_tuning_guide/Red_Hat_Enterprise_Linux-7-Performance_Tuning_Guide-en-US.pdf) <- detailed view on how to tune the RHEL lot of the tips and tricks apply for other Linux distributions

## CPU

Setting CPU governor (P-States) to `performance` may make a big difference and, in particular, result in much better network throughput:

* [How to tune your 100G host](https://fasterdata.es.net/assets/Papers-and-Publications/100G-Tuning-TechEx2016.tierney.pdf)

On `Linux`:

```console
$ echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

or using `cpupower` package (on `Debian` and `Ubuntu`):

```console
$ apt-get install -y linux-tools-$(uname -r) # install `cpupower`
$ cpupower frequency-info # check current settings
$ cpupower frequency-set -r -g performance # set `performance` setting to all CPU's
$ cpupower frequency-info # check settings after the change
```

Once the packages are installed (the step that will depend on your Linux distribution), you can then follow the *tuning instructions* from the referenced PDF (above).

## Network

AIStore supports 3 (three) logical networks:

* public (default port `51081`)
* intra-cluster control (`51082`), and
* intra-cluster data (`51083`)

Ideally, all 3 are provisioned (physically) separately - to reduce contention, avoid HoL, and ultimately optimize intra-cluster performance.

Separately, and in addition:

* MTU should be set to `9000` (Jumbo frames) - this is one of the most important configurations
* Optimize TCP send buffer sizes on the target side (`net.core.rmem_max`, `net.ipv4.tcp_rmem`)
* Optimize TCP receive buffer on the client (reading) side (`net.core.wmem_max`, `net.ipv4.tcp_wmem`)
* Set `net.ipv4.tcp_mtu_probing = 2`

> The last setting is especially important when client's MTU is greater than 1500.

> The list of tunables (above) cannot be considered _exhaustive_. Optimal (high-performance) choices always depend on the hardware, Linux kernel, and a variety of factors outside the scope.

## Smoke test

To ensure client ⇔ proxy, client ⇔ target, proxy ⇔ target, and target ⇔ target connectivity you can use `iperf` (make sure to use Jumbo frames and disable fragmentation).
Here is example use of `iperf`:

```console
$ iperf -P 20 -l 128K -i 1 -t 30 -w 512K -c <IP-address>
```

**NOTE**: `iperf` must show 95% of the bandwidth of a given phys interface. If it does not, try to find out why. It might have no sense to run any benchmark prior to finding out.

## Maximum number of open files

This must be done before running benchmarks, let alone deploying AIS in production - the maximum number of open file descriptors must be increased.

The corresponding system configuration file is `/etc/security/limits.conf`.

> In Linux, the default per-process maximum is 1024. It is **strongly recommended** to raise it to at least `100,000` for AIStore processes.

Here's a full replica of [/etc/security/limits.conf](https://github.com/NVIDIA/aistore/blob/main/deploy/conf/limits.conf) that we use for development _and_ production.

To check your current settings, run `ulimit -n` or `tail /etc/security/limits.conf`.

To increase the limits, copy the following lines into `/etc/security/limits.conf`:

```console
$ tail /etc/security/limits.conf
#ftp             hard    nproc           0
#ftp             -       chroot          /ftp
#@student        -       maxlogins       4

root             hard    nofile          250000
root             soft    nofile          250000
*                hard    nofile          65536
*                soft    nofile          65536

# End of file
```

**Important:** The 250,000 limit applies only to the root user. This is typically sufficient for AIStore deployments where aisnode processes (proxy + target) run with elevated privileges and may consume tens of thousands of file descriptors under load.

Regular users get a 65,536 limit, which is adequate for AIStore utilities and tooling while preventing runaway processes from consuming excessive system resources.

Once done, re-login and double-check that both *soft* and *hard* limits have indeed changed.

**As root, you should expect to see:**

```console
$ ulimit -n
250000
$ ulimit -Hn
250000
```

**As a regular user, you should see:**

```console
$ ulimit -n
65536
$ ulimit -Hn
65536
```

**For containerized deployments:** You may also need to configure container-level limits. For further references, consult:

* `docker run --ulimit`
* `DefaultLimitNOFILE` (for systemd-managed services)
* Kubernetes resource limits and security contexts
* [api/apc/const.go](https://github.com/NVIDIA/aistore/blob/main/api/apc/const.go)

## Storage

Storage-wise, each local `ais_local.json` config must be looking as follows:

```json
{
    "net": {
        "hostname": "127.0.1.0",
        "port": "8080",
    },
    "fspaths": {
        "/tmp/ais/1": {},
        "/tmp/ais/2": {},
        "/tmp/ais/3": {}
    }
}
```

* Each local path from the `fspaths` section above must be (or contain as a prefix) a mountpath of a local filesystem.
* Each local filesystem (above) must utilize one or more data drives, whereby none of the data drives is shared between two or more local filesystems.
* Each filesystem must be fine-tuned for reading large files/blocks/xfersizes.

### Disk priority

AIStore can be sensitive to spikes in I/O latencies, especially when running bursty traffic that carries large numbers of small-size I/O requests (resulting from writing and/or reading small-size objects).
To ensure that latencies do not spike because of some other processes running on the system it is recommended to give `aisnode` process the highest disk priority.
This can be done using [`ionice` tool](https://linux.die.net/man/1/ionice).

```console
$ # Best effort, highest priority
$ sudo ionice -c2 -n0 -p $(pgrep aisnode)
```

### Benchmarking disk

**TIP:** double check that rootfs/tmpfs of the AIStore target are _not_ used when reading and writing data.

When running a benchmark, make sure to run and collect the following in each and every target:

```console
$ iostat -cdxtm 10
```

Local hard drive read performance, the fastest block-level reading smoke test is:

```console
$ hdparm -Tt /dev/<drive-name>
```

Reading the block from certain offset (in gigabytes), `--direct` argument ensure that we bypass the drive’s buffer cache and read directly from the disk:

```console
$ hdparm -t --direct --offset 100 /dev/<drive-name>
```

More: [Tune hard disk with `hdparm`](http://www.linux-magazine.com/Online/Features/Tune-Your-Hard-Disk-with-hdparm).

### Local filesystem

Another way to increase storage performance is to benchmark different filesystems: `ext`, `xfs`, `openzfs`.
Tuning the corresponding IO scheduler can prove to be important:

* [ais_enable_multiqueue](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/host-config/docs/ais_enable_multiqueue.md)

Other related references:

* [How to improve disk IO performance in Linux](https://www.golinuxcloud.com/how-to-improve-disk-io-performance-in-linux/)
* [Performance Tuning on Linux — Disk I/O](https://cromwell-intl.com/open-source/performance-tuning/disks.html)

### `noatime`

One of the most important performance improvements can be achieved by simply turning off `atime` (access time) updates on the filesystem.

Example: mount xfs with the following performance-optimized parameters (that also include `noatime`):

```console
noatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier
```

> As aside, it is maybe important to note and disclose that AIStore itself lazily updates access times. The complete story has two pieces:
1. On the one hand, AIS can be deployed as a fast cache tier in front of any of the multiple supported [backends](providers.md). To support this specific usage scenario, AIS tracks access times and the remaining capacity. The latter has 3 (three) configurable thresholds: low-watermark, high-watermark, and OOS (out of space) - by default, 75%, 90%, and 95%, respectively. Running low on free space automatically triggers LRU job that starts visiting evictable buckets (if any), sorting their content by access times, and yes - evicting. Bucket "evictability" is also configurable and, in turn, has two different defaults: true for buckets that have remote backend, and false otherwise.
2. On the other hand, we certainly don't want to have any writes upon reads. That's why object metadata is cached and atime gets updated in memory to a) avoid extra reads and b) absorb multiple accesses and multiple atime updates. Further, if and only if we update atime in memory, we also mark the metadata as `dirty`. So that later, if for whatever reason we need to un-cache it, we flush it to disk (along with the most recently updates access time).
3. The actual mechanism of when and whether to flush the corresponding metadata is driven by two conditions: the time since last access and the amount of free memory. If there's plenty of memory, we flush `dirty` metadata after 1 hour of inactivity.
4. Finally, in AIS all configuration-related defaults (e.g., the default watermarks mentioned above) are also configurable - but that's a different story and a different scope...

External links:

* [The atime and noatime attribute](http://en.tldp.org/LDP/solrhe/Securing-Optimizing-Linux-RH-Edition-v1.3/chap6sec73.html)
* [Mount with noatime](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/global_file_system_2/s2-manage-mountnoatime)
* [Gain 30% Linux Disk Performance with noatime](https://lonesysadmin.net/2013/12/08/gain-30-linux-disk-performance-noatime-nodiratime-relatime)

## Virtualization

There must be no sharing of host resources between two or more VMs that are AIS nodes.

Even if there is a single virtual machine, the host may decide to swap it out when idle, or give it a single hyperthreaded vCPU instead of a full-blown physical core - this condition must be prevented.

Virtualized AIS node needs to have a physical resource - memory, CPU, network, storage - in its entirety. Hypervisor must resort to the remaining absolutely required minimum.

Make sure to use PCI passthrough to assign a device (NIC, HDD) directly to the AIS VM.

AIStore's primary goal is to scale with clustered drives. Therefore, the choice of a drive type and its capabilities remains very important.

Finally, when initializing virtualized disks it'd be advisable to set an optimal [block size](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disk-performance.html).

## Metadata write policy

There will be times when it'll make sense to keep storage system's metadata strictly in memory or, maybe, cache and flush it on a best-effort basis. The corresponding use cases include temporary datasets - transient data of any kind that can (and will) be discarded. There's also the case when AIS is used as a high-performance caching layer where original data is already sufficiently replicated and protected.

Metadata write policy - json tag `write_policy.md` - was introduced specifically to support those and related scenarios. Configurable both globally and for each individual bucket (or dataset), the current set of supported policies includes:

| Policy | Description |
| --- | ---|
| `immediate` | write immediately upon updates (global default) |
| `delayed`   | cache and flush when not accessed for a while (see also: [noatime](#noatime)) |
| `never`     | never write but always keep metadata in memory (aka "transient") |

> For the most recently updated enumeration, please see the [source](/cmn/api_const.go).

## PUT latency

AIS provides checksumming and self-healing - the capabilities that ensure that user data is end-to-end protected and that data corruption, if it ever happens, will be properly and timely detected and - in presence of any type of data redundancy - resolved by the system.

There's a price, though, and the scenarios where you could make an educated choice to trade checksumming for performance.

In particular, let's say that we are massively writing a new content into a bucket.

> Type of the bucket doesn't matter - it may be an `ais://` bucket, or `s3://`, or any other supported [backend](/docs/overview.md#backend-provider).

What matters is that we do *know* that we'll be overwriting few objects, percentage-wise. Then it would stand to reason that AIS, on its end, should probably refrain from trying to load the destination object's metadata. Skip loading existing object's metadata in order to compare checksums (and thus maybe avoid writing altogether if the checksums match) and/or update the object's version, etc.

* [API: PUT(object)](/api/object.go) - and look for `SkipVC` option
* [CLI: PUT(object)](/docs/cli/object.md#put-object) - `--skip-vc` option

## GET throughput

AIS is elastic cluster that can grow and shrink at runtime when you attach (or detach) disks and add (or remove) nodes. Drive are prone to sudden failures while nodes can experience unexpected crashes. At all times, though, and in presence of any and all events, AIS tries to keep [fair and balanced](/docs/rebalance.md) distribution of user data across all clustered nodes and drives.

The idea to keep all drives **equally utilized** is the absolute cornerstone of the design.

There's one simple trick, though, to improve drive utilization even further: [n-way mirroring](/docs/storage_svcs.md#n-way-mirror). In fact, 

  * if you observe <span style="color:red">90% and higher</span> utilizations,
  * and if adding more drives (or more nodes) is not an option,
  * and if you still have some spare capacity to create additional copies of data

**if** all of the above is true, it'd be a very good idea to *mirror* the corresponding bucket, e.g.:

```console
# reconfigure existing bucket (`ais://abc` in the example) for 3 copies

$ ais bucket props set ais://abc mirror.enabled=true mirror.copies=3
Bucket props successfully updated
"mirror.copies" set to: "3" (was: "2")
"mirror.enabled" set to: "true" (was: "false")
```

In other words, under extreme loads n-way mirroring is especially, and strongly, recommended. (Data protection would then be an additional bonus, needless to say.) The way it works is simple:

* AIS target constantly monitors drive utilizations
* Given multiple copies, AIS target always selects a copy (and a drive that stores it) based on the drive's utilization.

Ultimately, a drive that has fewer outstanding I/O requests and is less utilized - will always win.

## `aisloader`

AIStore includes `aisloader` - a powerful benchmarking tool that can be used to generate a wide variety of workloads closely resembling those produced by AI apps.

For numerous command-line options and usage examples, please see:

* [Load Generator](/docs/aisloader.md)
* [How To Benchmark AIStore](howto_benchmark.md)

Or, simply run `aisloader` with no arguments and see its online help, including command-line options and numerous usage examples:

```console
$ make aisloader; aisloader

AIS loader (aisloader v1.3, build ...) is a tool to measure storage performance.
It's a load generator that has been developed to benchmark and stress-test AIStore
but can be easily extended to benchmark any S3-compatible backend.
For usage, run: `aisloader`, or `aisloader usage`, or `aisloader --help`.
Further details at https://github.com/NVIDIA/aistore/blob/main/docs/howto_benchmark.md

Command-line options
====================
...
...

```

Note as well that `aisloader` is fully StatsD-enabled - collected metrics can be forwarded to any StatsD-compliant backend for visualization and further analysis.
