---
layout: post
title: PERFORMANCE
permalink: /docs/performance
redirect_from:
 - /performance.md/
 - /docs/performance.md/
---

AIStore is all about the performance.
Below you will find some tips and tricks to ensure that AIStore does deliver.

- [Performance tuning](#performance-tuning)
  - [CPU](#cpu)
  - [Network](#network)
    - [Smoke test](#smoke-test)
    - [Maximum open files](#maximum-open-files)
  - [Storage](#storage)
    - [Block settings](#block-settings)
    - [Benchmarking disk](#benchmarking-disk)
    - [Underlying filesystem](#underlying-filesystem)
      - [noatime](#noatime)
  - [Virtualization](#virtualization)
  - [Metadata write policy](#metadata-write-policy)
- [AIS Loader](#ais-loader)

# Performance tuning

Articles you may find useful:
* [Ubuntu Performance Tuning (archived)](https://web.archive.org/web/20190811180754/https://wiki.mikejung.biz/Ubuntu_Performance_Tuning) <- good guide about general optimizations (some of them are described below)
* [RHEL 7 Performance Tuning Guide](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/pdf/performance_tuning_guide/Red_Hat_Enterprise_Linux-7-Performance_Tuning_Guide-en-US.pdf) <- detailed view on how to tune the RHEL lot of the tips and tricks apply for other Linux distributions

## CPU

Setting CPU governor (P-States) to `performance` may make a big difference and, in particular, result in much better network throughput:

* [Recent Linux TCP Updates, and how to tune your 100G host](https://fasterdata.es.net/assets/Papers-and-Publications/100G-Tuning-TechEx2016.tierney.pdf) (slide 13)

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

* MTU should be set to `9000` (Jumbo frames) - this is one of the most important configurations
* Optimize TCP send buffer sizes on the target side (`net.core.rmem_max`, `net.ipv4.tcp_rmem`)
* Optimize TCP receive buffer on the client (reading) side (`net.core.wmem_max`, `net.ipv4.tcp_wmem`)
* `net.ipv4.tcp_mtu_probing = 2` # especially important in communication between client <-> proxy or client <-> target and if client has `mtu` set > 1500
* Wait.. there is more: [all ip-sysctl configurations](https://wiki.linuxfoundation.org/networking/ip-sysctl)

### Smoke test

To ensure client ⇔ proxy, client ⇔ target, proxy ⇔ target, and target ⇔ target connectivity you can use `iperf` (make sure to use Jumbo frames and disable fragmentation).
Here is example use of `iperf`:

```console
$ iperf -P 20 -l 128K -i 1 -t 30 -w 512K -c <IP-address>
```

**NOTE**: `iperf` must show 95% of the bandwidth of a given phys interface. If it does not, try to find out why. It might have no sense to run any benchmark prior to finding out.

### Maximum open files

To ensure that AIStore works properly you probably need to increase the default number of open files. To check the current settings, you can use `ulimit -n`.

> It is strongly recommended to raise ulimit to at least `100,000`.

Here're the (example) settings that we use for development:

```
$ tail /etc/security/limits.conf
#ftp             hard    nproc           0
#ftp             -       chroot          /ftp
#@student        -       maxlogins       4

root             hard    nofile          999999
root             soft    nofile          999999
ubuntu           hard    nofile          999999
ubuntu           soft    nofile          999999

# End of file
```

> If the change in the ` /etc/security/limits.conf` (above) does not cause `ulimit -n` to show expected numbers, try then modifying `/etc/systemd/user.conf` and `/etc/systemd/system.conf`. In both configurations files, look for the line `#DefaultLimitNOFILE=` and uncomment it as `DefaultLimitNOFILE=999999`. A brief discussion of the topic can be found [here](https://superuser.com/questions/1200539/cannot-increase-open-file-limit-past-4096-ubuntu).

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

### Block settings

When initializing the disk it is necessary to set proper block size: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disk-performance.html

```console
$ dd if=/dev/zero of=/dev/<disk_name> bs=<block_size>
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

### Underlying filesystem

Another way to increase storage performance is to benchmark different filesystems: `ext`, `xfs`, `openzfs`.
Tuning the IO scheduler can be a very important part of this process:
 * http://blackbird.si/tips-for-optimizing-disk-performance-on-linux/
 * https://cromwell-intl.com/open-source/performance-tuning/disks.html

It seems like generally `deadline` scheduler is a good choice for AIStore, instead of default `cfq`.
When you consider using `xfs` keep in mind that:

> According to xfs.org, the CFQ scheduler defeats much of the parallelization in XFS.

#### noatime

One of the most important performance improvements can be achieved by turning off `atime` (access time) updates on the filesystem.
This can be achieved by specifying `noatime` option when mounting the storage disk.

`atime` updates generate additional write traffic during file access (retrieving the object) which can significantly impact the overall throughput of the system.
Therefore, we **strongly** advise to use the `noatime` option when mounting a disk.

Important to note is that AIStore will still maintain access time updates but with using more optimized techniques as well as ensuring that it is consistent during object migration.

External links:
 * http://en.tldp.org/LDP/solrhe/Securing-Optimizing-Linux-RH-Edition-v1.3/chap6sec73.html
 * https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/global_file_system_2/s2-manage-mountnoatime
 * https://lonesysadmin.net/2013/12/08/gain-30-linux-disk-performance-noatime-nodiratime-relatime/

## Virtualization

AIStore node (proxy and target) must be deployed as a single VM on a given bare-metal host.
There must be no sharing of host resources between two or more VMs.
Even if there is a single VM, the host may decide to swap it out when idle, or give it a single hyperthreaded vCPU instead of a full-blown physical core - this condition must be prevented.
AIStore node needs to have a physical resource in its entirety: RAM, CPU, network, and storage. Hypervisor must resort to the remaining absolutely required minimum.
Make sure to use PCI passthrough to assign a device (NIC, HDD) directly to the AIStore node VM.

AIStore's primary goal is to scale with clustered drives. Therefore, the choice of a drive (type and capabilities) is very important.

## Metadata write policy

Sometimes it will make sense to keep storage system's metadata strictly in memory or, maybe, cache and flush it on a best-effort basis. The corresponding use cases include temporary datasets - transient data of any kind that can (and will) be discarded. There's also the case when AIS is used as a high-performance caching layer with original data already sufficiently replicated and protected.

Metadata write policy - json tag `md_write` - was introduced specifically to support those and related scenarios. Configurable both globally and for each individual bucket (or dataset), the current set of supported policies includes:

| Policy | Description |
| --- | ---|
| `immediate` | write immediately upon updates (global default) |
| `delayed`   | cache and flush when not accessed for a while (see also: [noatime](#noatime)) |
| `never`     | never write but always keep metadata in memory (aka "transient") |

> For the most recently updated enumeration, please see the [source](/cmn/api_const.go).

# AIS Loader

AIStore includes `aisloader` - a powerful tool that we use to simulate a variety of AI workloads. For numerous command-line options and usage examples, please see [Load Generator](/docs/aisloader.md) and [How To Benchmark AIStore](howto_benchmark.md). Or, just run the tool with empty command line and see its online help and examples.

> Note as well that `aisloader` is fully StatsD-enabled - the metrics can be formwarded to any StatsD-compliant backend for visualization and further analysis.
