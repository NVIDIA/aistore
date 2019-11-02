# aisfs: FUSE file system for AIStore

## Prerequisites

* Linux system

* [Go 1.13 or later](https://golang.org/dl/)
* Loaded FUSE kernel module (`fuse.ko`)
* `fusermount` mounting utility

Depending on your Linux distribution, you may or may not already have
`fuse.ko` and `fusermount` installed on your system.
If you are using Ubuntu 18.04 LTS, you can verify this by
running the following commands (expect similar output to the one shown below):

```shell
$ grep -i fuse /lib/modules/$(uname -r)/modules.builtin
kernel/fs/fuse/fuse.ko
$ fusermount -V
fusermount version: 2.9.7
```

## Getting Started

### Installation

In order to install `aisfs` on your system, execute the following commands:

```shell
$ cd $GOPATH/src/github.com/NVIDIA/aistore/aisfs
$ chmod +x ./install.sh
$ ./install.sh
```

Verify that `aisfs` has been successfully installed.

```shell
$ aisfs -v
aisfs version 0.1 (build 6204591d)
```

### Mounting

Assuming `mybucket` is a name of a bucket, `localdir` is an empty directory
in your local file system and an instance of AIStore cluster is running
on the local machine, the following command will mount `mybucket`
in `localdir`, making `localdir` the root directory of `aisfs`:

```shell
$ aisfs mybucket localdir/
```

File system is served by the user-space background process (daemon)
that will continue to run until the file system is unmounted.
In order to run the file system server in the foreground,
pass the `--wait ` option to `aisfs`:

```shell
$ aisfs --wait mybucket localdir/
```

Although `aisfs` will attempt to discover an AIStore cluster by itself,
it is possible to explicitly provide cluster's URL as a command-line flag:

```shell
$ aisfs --url 'http://127.0.0.1:8080' mybucket localdir/
```

In order to view usage information, including a full list of options, run:

```shell
$ aisfs --help
```

### Unmounting

There are three methods for stopping and unmounting the file system:

1. Use the `fusermount` utility:

   ```shell
   $ fusermount -u localdir/
   ```

2. Use the `umount` command:

   ```shell
   $ umount localdir/
   ```

   >  Note: You may need root privileges to execute this command.

3. If `aisfs` is invoked in the foreground by passing the `--wait` option,
   simply interrupting the process by pressing Ctrl-C.

