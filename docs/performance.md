# Performance

AIStore is all about the performance.
As AIStore strives to be the most efficient storage solution for AI it is not possible without underlying components optimalizations.
Below you will find some tips and tricks which you can use to ensure that AIStore delivers the best performance.

- [Performance tuning](#performance-tuning)
    - [General](#general)
    - [CPU](#cpu)
    - [Network](#network)
        - [Smoke test](#smoke-test)
        - [Maximum open files](#maximum-open-files)
    - [Storage](#storage)
        - [Block settings](#block-settings)
        - [Benchmarking disk](#benchmarking-disk)
        - [Underlying filesystem](#underlying-filesystem)
    - [Virtualization](#virtualization)
- [Performance testing](#performance-testing)

## Performance tuning

### General

Here we gathered couple articles/papers which we think you may find useful:

* https://wiki.mikejung.biz/Ubuntu_Performance_Tuning <- good guide about general optimizations (some of them are described below)
* https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/pdf/performance_tuning_guide/Red_Hat_Enterprise_Linux-7-Performance_Tuning_Guide-en-US.pdf <- detailed view on how to tune the RHEL lot of the tips and tricks apply for other Linux distributions

### CPU

Setting CPU governor to `performance` setting can result in much better network throughput and overall performance: [Tuning Net](https://fasterdata.es.net/assets/Papers-and-Publications/100G-Tuning-TechEx2016.tierney.pdf) (slide 13)

Depending on distribution you may need to install differrent tooling to be able to change it. On `Debian` and `Ubuntu` you can do:

```bash
apt-get install -y linux-tools-$(uname -r) # install `cpupower`
cpupower frequency-info # check current settings
cpupower frequency-set -r -g performance # set `performance` setting to all CPU's
cpupower frequency-info # check settings after the change
```

### Network

* MTU should be set to `9000` (Jumbo frames) - this is one of the most important configurations
* Optimize TCP send buffer sizes on the target side (`net.core.rmem_max`, `net.ipv4.tcp_rmem`)
* Optimize TCP receive buffer on the client (reading) side (`net.core.wmem_max`, `net.ipv4.tcp_wmem`)
* `net.ipv4.tcp_mtu_probing = 2` # especially important in communication between client <-> proxy or client <-> target and if client has `mtu` set > 1500
* Wait.. there is more: [all ip-sysctl configurations](https://www.cyberciti.biz/files/linux-kernel/Documentation/networking/ip-sysctl.txt)

#### Smoke test

To ensure client ⇔ proxy, client ⇔ target, proxy ⇔ target, and target ⇔ target connectivity you can use `iperf` (make sure to use Jumbo frames and disable fragmentation).
Here is example use of `iperf`:
```bash
iperf -P 20 -l 128K -i 1 -t 30 -w 512K -c <IP-address>
```

**NOTE**: `iperf` must show 95% of the bandwidth of a given phys interface. If it does not, try to find out why. It might have no sense to run any benchmark prior to finding out.

#### Maximum open files

To ensure that AIStore works properly you probably need to increase the default number of open files.
To check current setting you can use `ulimit -n`.
We advise to set it to at least `100'000`.

### Storage

Storage-wise, each local `ais.json` config must be looking as follows:

((missing -- image))

* Each local path from the `fspaths` section above must be (or contain as a prefix) a mountpoint of a local filesystem.
* Each local filesystem (above) must utilize one or more data drives, whereby none of the data drives is shared between two or more local filesystems.
* Each filesystem must be fine-tuned for reading large files/blocks/xfersizes.

#### Block settings

When initializing the disk it is necessary to set proper block size: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disk-performance.html
```bash
dd if=/dev/zero of=/dev/<disk_name> bs=<block_size>
```

#### Benchmarking disk

**TIP:** double check that rootfs/tmpfs of the AIStore target are _not_ used when reading and writing data.

When running a benchmark, make sure to run and collect the following in each and every target:

```bash
iostat -cdxtm 10
```

Local hard drive read performance, the fastest block-level reading smoke test is:
```bash
hdparm -Tt /dev/<drive-name>
```

Reading the block from certain offset (in gigabytes), `--direct` argument ensure that we bypass the drive’s buffer cache and read directly from the disk:
```bash
hdparm -t --direct --offset 100 /dev/<drive-name>
```

More: [Tune hard disk with `hdparm`](http://www.linux-magazine.com/Online/Features/Tune-Your-Hard-Disk-with-hdparm).

#### Underlying filesystem

Another way to increase storage performance is to benchmark different filesystems: `ext`, `xfs`, `openzfs`.
Tuning the IO scheduler can be very important part in this process:
 * http://blackbird.si/tips-for-optimizing-disk-performance-on-linux/
 * https://cromwell-intl.com/open-source/performance-tuning/disks.html

It seems like generally `deadline` scheduler is a good choice for AIStore, instead of default `cfq`.
When you consider using `xfs` keep in mind that:

> According to xfs.org, the CFQ scheduler defeats much of the parallelization in XFS.

### Virtualization

AIStore node (proxy and target) must be deployed as a single VM on a given bare-metal host.
There must be no sharing of host resources between two or more VMs.
Even if there is a single VM, the host may decide to swap it out when idle, or give it a single hyperthreaded vCPU instead of a full blown physical core - this condition must be prevented.
AIStore node needs to have a physical resource in its entirety: RAM, CPU, network and storage. Hypervisor must resort to the remaining absolutely required minimum.
Make sure to use PCI passthrough to assign a device (NIC, HDD) directly to the AIStore node VM.

AIStore's primary goal is to scale with clustered drives. Therefore, the choice of a drive (type and capabilities) is very important.

## Performance testing

[AIStore load generator](docs/howto_benchmark.md) is a built-in tool to test performance. However, it won't show which local subsystem - disk or network - could be a bottleneck. AIStore provides a way to switch off disk and/or network IO to test their impact on performance. It can be done by passing command line arguments or by setting environment variables. The environment variables have higher priority: if both a command line argument and an environment variable are defined then AIStore uses the environment variable.

If any kind of IO is disabled then AIStore sends a warning to stderr and turns off some internal features including object checksumming, versioning, atime and extended attributes management.

>> As of version 2.0, disabling and (re)enabling IO on the fly (at runtime) is not supported.

| CLI argument | Environment variable | Default value | Description |
|---|---|---|---|
| nodiskio | AIS_NODISKIO | false | true - disables disk IO. For GET requests a storage target does not read anything from disks - no file stat, file open etc - and returns an in-memory object with predefined size (see AIS_DRYOBJSIZE variable). For PUT requests it reads the request's body to /dev/null.<br>Valid values are true or 1, and falseor 0 |
| nonetio | AIS_NONETIO | false | true - disables HTTP read and write. For GET requests a storage target reads the data from disks but does not send bytes to a caller. It results in that the caller always gets an empty object. For PUT requests, after opening a connection, AIStore reads the data from in-memory object and saves the data to disks.<br>Valid values are true or 1, and false or 0 |
| dryobjsize | AIS_DRYOBJSIZE | 8m | A size of an object when a source is a 'fake' one: disk IO disabled for GET requests, and network IO disabled for PUT requests. The size is in bytes but suffixes can be used. The following suffixes are supported: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. Default value is '8m' - the size of an object is 8 megabytes |

Example of deploying a cluster with disk IO disabled and object size 256KB:

```
/opt/aistore/ais$ AIS_NODISKIO=true AIS_DRYOBJSIZE=256k make deploy
```

>> The command-line load generator shows 0 bytes throughput for GET operations when network IO is disabled because a caller opens a connection but a storage target does not write anything to it. In this case the throughput can be calculated only indirectly by comparing total number of GETs or latency of the current test and those of previous test that had network IO enabled.
