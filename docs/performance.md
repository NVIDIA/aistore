## Table of Contents
- [Performance tuning](#performance-tuning)
- [Performance testing](#performance-testing)

## Performance tuning

AIStore utilizes local filesystems, which means that under pressure a AIStore target will have a significant number of open files. This is often the case when running stress tests that perform highly-intensive, concurrent object PUT operations. In the event that errors stating `too many open files` are encountered, system settings must be changed. To overcome the system's default `ulimit`, have the following 3 lines in each target's `/etc/security/limits.conf`:

```
root             hard    nofile          10240
ubuntu           hard    nofile          1048576
ubuntu           soft    nofile          1048576
```
After restarting, confirm that the limits have been increased accordingly:
```shell
ulimit -n
```

If you find that the result is still lower than expected, take the additional steps of modifying
both `/etc/systemd/system.conf` and `/etc/systemd/user.conf` to change the value of `DefaultLimitNOFILE` to the desired limit. If that line does not exist, append it under the `Manager` section of those two files as such:
```
DefaultLimitNOFILE=$desiredLimit
```

Additionally, add the following line to the end of `/etc/sysctl.conf`:
```
fs.file-max=$desiredLimit
```

After a restart, verify using the same command, `ulimit -n`, that the limit for the number of open files has been increased accordingly.

For more information, refer to this [link](https://ro-che.info/articles/2017-03-26-increase-open-files-limit).

Generally, configuring a AIStore cluster to perform under load is a vast topic that would be outside the scope of this README. The usual checklist includes (but is not limited to):

1. Setting MTU = 9000 (aka Jumbo frames)

2. Following instruction guidelines for the Linux distribution that you deploy, e.g.:
    - [Ubuntu Performance Tuning](https://wiki.mikejung.biz/Ubuntu_Performance_Tuning)
    - [Red Hat Enterprise Linux 7 Performance Tuning](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/pdf/performance_tuning_guide/Red_Hat_Enterprise_Linux-7-Performance_Tuning_Guide-en-US.pdf)

3. Tuning TCP stack - in part, increasing the TCP send and receive buffer sizes:

```shell
$ sysctl -a | grep -i wmem
$ sysctl -a | grep -i ipv4
```

And more.

Virtualization overhead may require a separate investigation. It is strongly recommended that a (virtualized) AIStore storage node (whether it's a gateway or a target) would have a direct and non-shared access to the (CPU, disk, memory and network) resources of its bare-metal host. Ensure that AIStore VMs do not get swapped out when idle.

AIStore storage node, in particular, needs to have a physical resource in its entirety: RAM, CPU, network and storage. The underlying hypervisor must "resort" to the remaining minimum that is absolutely required.

And, of course, make sure to use PCI passthrough for all local hard drives given to AIStore.

Finally, to ease troubleshooting, consider the usual and familiar load generators such as `fio` and `iperf`, and observability tools: `iostat`, `mpstat`, `sar`, `top`, and more. For instance, `fio` and `iperf` may appear to be almost indispensable in terms of validating and then tuning performances of local storages and clustered networks, respectively. Goes without saying that it does make sense to do this type of basic checking-and-validating prior to running AIStore under stressful workloads.

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

