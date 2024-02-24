---
layout: post
title: HELP
permalink: /docs/cli/help
redirect_from:
 - /cli/help.md/
 - /docs/cli/help.md/
---

This readme is a loose assortment of quick tips.

## Installing CLI directly from the latest GitHub release

The default destination is /usr/local/bin but here we install into /tmp/www

```console
$ deploy/scripts/install_from_binaries.sh --dstdir /tmp/www
Installing aisloader => /tmp/www/aisloader
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
100 9847k  100 9847k    0     0  3553k      0  0:00:02  0:00:02 --:--:-- 4301k
aisloader
Installing CLI => /tmp/www/ais
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
100  9.9M  100  9.9M    0     0  4436k      0  0:00:02  0:00:02 --:--:-- 5901k
ais
Downloading CLI autocompletions (bash & zsh)...
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  2350  100  2350    0     0   7000      0 --:--:-- --:--:-- --:--:--  6994
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   578  100   578    0     0   1673      0 --:--:-- --:--:-- --:--:--  1675
NOTE:
   *** CLI autocompletions are now copied to /etc/bash_completion.d/ais ***
   *** To enable, simply run: source /etc/bash_completion.d/ais         ***

Done.

$ ls /tmp/www
ais  aisloader
```

To see the version:
```console
$ ais version
version 1.1.950525a50 (build 2023-03-30T18:51:58-0400)
```

# Getting help

Note that `ais help <command>` is identical to `ais <command> --help`. In fact, the `--help` option is absolutely universal and will work across the entire CLI terms of providing context-relevant information. 

But first, let's see all CLI top-level commands with brief descriptions.

> The text below can serve as a 30-seconds brief introduction into CLI usage and its capabilities.

```console
NAME:
   ais - AIS CLI: command-line management utility for AIStore

USAGE:
   ais [global options] command [command options] [arguments...]

VERSION:
   1.1.950525a50

DESCRIPTION:
   If <TAB-TAB> completion doesn't work:
   * download https://github.com/NVIDIA/aistore/tree/main/cmd/cli/autocomplete
   * run 'cmd/cli/autocomplete/install.sh'
   To install CLI directly from GitHub: https://github.com/NVIDIA/aistore/blob/main/deploy/scripts/install_from_binaries.sh

COMMANDS:
   bucket          create/destroy buckets, list bucket's contents, show existing buckets and their properties
   object          put, get, list, rename, remove, and other operations on objects
   cluster         monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.
   config          configure AIS cluster and individual nodes (in the cluster); configure CLI (tool)
   etl             execute custom transformations on objects
   job             monitor, query, start/stop and manage jobs and eXtended actions (xactions)
   auth            add/remove/show users, manage user roles, manage access to AIS clusters
   show            show configuration, buckets, jobs, etc. - all managed entities in the cluster, and the cluster itself
   help            show a list of commands; show help for a given command
   advanced        special commands intended for development and advanced usage
   storage         monitor and manage clustered storage
   archive         Create multi-object archive, append files to an existing archive
   log             show log
   performance     show performance counters, throughput, latency, and more (press <TAB-TAB> to select specific view)
   remote-cluster  show attached AIS clusters
   alias           manage top-level aliases
   put             (alias for "object put") PUT or APPEND one file or one directory, or multiple files and/or directories.
                   - use optional shell filename pattern (wildcard) to match/select sources;
                   - request '--compute-checksum' to facilitate end-to-end protection;
                   - progress bar via '--progress' to show runtime execution (uploaded files count and size);
                   - when writing directly from standard input use Ctrl-D to terminate;
                   - use '--archpath' to APPEND to an existing tar-formatted object.
   start           (alias for "job start") run batch job
   stop            (alias for "job stop") terminate a single batch job or multiple jobs (press <TAB-TAB> to select, '--help' for options)
   wait            (alias for "job wait") wait for a specific batch job to complete (press <TAB-TAB> to select, '--help' for options)
   cp              (alias for "bucket cp") copy entire bucket or selected objects (to select, use '--list' or '--template')
   create          (alias for "bucket create") create ais buckets
   get             (alias for "object get") get an object, an archived file, or a range of bytes from the above, and in addition:
                   - write the content locally with destination options including: filename, directory, STDOUT ('-');
                   - use '--prefix' to get multiple objects in one shot (empty prefix for the entire bucket).
   ls              (alias for "bucket ls") list buckets, objects in buckets, and files in objects formatted as archives
   search          search ais commands

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```


