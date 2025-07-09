This readme is a loose assortment of quick tips.

## Installing CLI directly from the latest GitHub release

The default destination is /usr/local/bin but here we install into /tmp/www

```console
$ scripts/install_from_binaries.sh --dstdir /tmp/www
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
   ais [global options] [arguments...]  command [command options]  

VERSION:
   1.18.605c658ff

TAB completions (Bash and Zsh):
   If <TAB-TAB> completion doesn't work:
   * download https://github.com/NVIDIA/aistore/tree/main/cmd/cli/autocomplete
   * run 'cmd/cli/autocomplete/install.sh'
   To install CLI directly from GitHub: https://github.com/NVIDIA/aistore/blob/main/scripts/install_from_binaries.sh

COMMANDS:
   bucket          Create and destroy buckets, list bucket's content, show existing buckets and their properties
   object          PUT, GET, list, rename, remove, and other operations on objects
   cluster         Monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.
   config          Configure AIS cluster and individual nodes (in the cluster); configure CLI (tool)
   etl             Manage and execute custom ETL (Extract, Transform, Load) jobs
   job             Monitor, query, start/stop and manage jobs and eXtended actions (xactions)
   auth            Add/remove/show users, manage user roles, manage access to AIS clusters
   show            Show configuration, buckets, jobs, etc. - all managed entities in the cluster, and the cluster itself
   help            Show a list of commands; show help for a given command
   advanced        Special commands intended for development and advanced usage
   storage         Monitor and manage clustered storage
   archive         Archive multiple objects from a given bucket; archive local files and directories; list archived content
   log             View ais node's log in real time; download the current log; download all logs (history)
   tls             Load or reload (an updated) TLS certificate; display information about currently deployed certificates
   performance     Show performance counters, throughput, latency, disks, used/available capacities (press <TAB-TAB> to select specific view)
   remote-cluster  Show attached AIS clusters
   ml              Machine learning operations: batch dataset operations, and ML-specific workflows
   alias           Manage top-level aliases
   kubectl         Show Kubernetes pods and services

GLOBAL OPTIONS:
   --help, -h     Show help
   --version, -v  print the version

ALIASES:
   cp => 'bucket cp'; create => 'bucket create'; evict => 'bucket evict'; 
   ls => 'bucket ls'; rmb => 'bucket rm'; start => 'job start'; 
   blob-download => 'job start blob-download'; download => 'job start download'; 
   dsort => 'job start dsort'; stop => 'job stop'; wait => 'job wait'; 
   get => 'object get'; prefetch => 'object prefetch'; put => 'object put'; 
   rmo => 'object rm'; space-cleanup => 'storage cleanup'; scrub => 'storage validate'
```


