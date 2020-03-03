# Performance

AIStore is all about the performance. Below you will find some tips and tricks to ensure that AIStore does deliver.

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
            - [noatime](#noatime)
    - [Virtualization](#virtualization)
- [Performance testing](#performance-testing)

## Performance tuning

### General

A couple of articles that we think you may find useful or helpful:

* https://wiki.mikejung.biz/Ubuntu_Performance_Tuning <- good guide about general optimizations (some of them are described below)
* https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/pdf/performance_tuning_guide/Red_Hat_Enterprise_Linux-7-Performance_Tuning_Guide-en-US.pdf <- detailed view on how to tune the RHEL lot of the tips and tricks apply for other Linux distributions

### CPU

Setting CPU governor (P-States) to `performance` may make a big difference and, in particular, result in much better network throughput:

* [Recent Linux TCP Updates, and how to tune your 100G host](https://fasterdata.es.net/assets/Papers-and-Publications/100G-Tuning-TechEx2016.tierney.pdf) (slide 13)

On `Debian` and `Ubuntu`:

```bash
apt-get install -y linux-tools-$(uname -r) # install `cpupower`
cpupower frequency-info # check current settings
cpupower frequency-set -r -g performance # set `performance` setting to all CPU's
cpupower frequency-info # check settings after the change
```

Once the packages are installed (the step that will depend on your Linux distribution), you can then follow the *tuning instructions* from the referenced PDF (above).

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

To ensure that AIStore works properly you probably need to increase the default number of open files. To check the current settings, you can use `ulimit -n`.

> It is strongly recommended to raise ulimit to at least `100,000`.

Here're the (example) settings that we use for development:

```shell
# tail /etc/security/limits.conf
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
Tuning the IO scheduler can be a very important part of this process:
 * http://blackbird.si/tips-for-optimizing-disk-performance-on-linux/
 * https://cromwell-intl.com/open-source/performance-tuning/disks.html

It seems like generally `deadline` scheduler is a good choice for AIStore, instead of default `cfq`.
When you consider using `xfs` keep in mind that:

> According to xfs.org, the CFQ scheduler defeats much of the parallelization in XFS.

##### noatime

One of the most important performance improvements can be achieved by turning off `atime` (access time) updates on the filesystem.
This can be achieved by specifying `noatime` option when mounting the storage disk.

`atime` updates generate additional write traffic during file access (retrieving the object) which can significantly impact the overall throughput of the system.
Therefore, we **strongly** advise to use the `noatime` option when mounting a disk.

Important to note is that AIStore will still maintain access time updates but with using more optimized techniques as well as ensuring that it is consistent during object migration.

External links:
 * http://en.tldp.org/LDP/solrhe/Securing-Optimizing-Linux-RH-Edition-v1.3/chap6sec73.html
 * https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/global_file_system_2/s2-manage-mountnoatime
 * https://lonesysadmin.net/2013/12/08/gain-30-linux-disk-performance-noatime-nodiratime-relatime/

### Virtualization

AIStore node (proxy and target) must be deployed as a single VM on a given bare-metal host.
There must be no sharing of host resources between two or more VMs.
Even if there is a single VM, the host may decide to swap it out when idle, or give it a single hyperthreaded vCPU instead of a full-blown physical core - this condition must be prevented.
AIStore node needs to have a physical resource in its entirety: RAM, CPU, network, and storage. Hypervisor must resort to the remaining absolutely required minimum.
Make sure to use PCI passthrough to assign a device (NIC, HDD) directly to the AIStore node VM.

AIStore's primary goal is to scale with clustered drives. Therefore, the choice of a drive (type and capabilities) is very important.

## Performance testing

[AIStore load generator](/docs/howto_benchmark.md) is a built-in tool to test performance. One of the most common questions that arise when analyzing performance results is whether the bottleneck is imposed by the hardware - namely, HDDs.

To that end, AIStore supports switching off disk IO to, effectively, perform dry-run type benchmarking. This can be done by passing command-line arguments or by setting environment variables (see below).

> The environment variables have higher priority: if both environment and command-line are specified the former takes precedence.

> Disabling disk IO must be done at startup; disabling/enabling disk IO at runtime is not supported.

| CLI argument | Environment variable | Default value | Description |
|---|---|---|---|
| nodiskio | AIS_NODISKIO | false | true - disables disk IO. For GET requests a storage target does not read anything from disks - no file stat, file open etc - and returns an in-memory object with predefined size (see AIS_DRYOBJSIZE variable). For PUT requests it reads the request's body to /dev/null.<br>Valid values are true or 1, and false or 0 |
| dryobjsize | AIS_DRYOBJSIZE | 8m | A size of an object when a source is a 'fake' one: disk IO disabled for GET requests, and network IO disabled for PUT requests. The size is in bytes but suffixes can be used. The following suffixes are supported: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. The default value is '8m' - the size of an object is 8 megabytes |

Example of deploying a cluster with disk IO disabled and object size 256KB:

```
/opt/aistore/ais$ AIS_NODISKIO=true AIS_DRYOBJSIZE=256k make deploy
```

>> The command-line load generator shows 0 bytes throughput for GET operations when network IO is disabled because a caller opens a connection but a storage target does not write anything to it. In this case, the throughput can be calculated only indirectly by comparing total number of GETs or latency of the current test and those of the previous test that had network IO enabled.
