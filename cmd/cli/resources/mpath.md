# Mountpath (disk) management

A *mountpath* is a single disk **or** a volume (a RAID) formatted with a local filesystem of choice, **and** a local directory that AIS utilizes to store user data and AIS metadata. A mountpath can be disabled and (re)enabled, automatically or administratively, at any point during runtime. In a given cluster, a total number of mountpaths would normally compute as a direct product of (number of storage targets) x (number of disks in each target).

## Show mountpaths

`ais show mountpath [DAEMON_ID]`

Show mountpaths for a given target or all targets.

### Examples

```console
$ ais show mountpath 12367t8085
247389t8085
        Available:
			/tmp/ais/5/3
			/tmp/ais/5/1
        Disabled:
			/tmp/ais/5/2

$ ais show mountpath
247389t8085
        Available:
			/tmp/ais/5/3
			/tmp/ais/5/1
        Disabled:
			/tmp/ais/5/2
147665t8084
        Available:
			/tmp/ais/4/3
			/tmp/ais/4/1
			/tmp/ais/4/2
426988t8086
		No mountpaths
```

## Attach mountpath

`ais attach mountpath DAEMON_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Attach a mountpath on a specified target to AIS storage.

### Examples

```console
$ ais attach mountpath 12367t8080=/data/dir
```

## Detach mountpath

`ais detach mountpath DAEMON_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Detach a mountpath on a specified target from AIS storage.

### Examples

```console
$ ais detach mountpath 12367t8080=/data/dir
```
