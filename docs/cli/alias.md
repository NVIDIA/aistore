---
layout: post
title: ALIAS
permalink: /docs/cli/alias
redirect_from:
 - /cli/alias.md/
 - /docs/cli/alias.md/
---

# CLI Reference for Aliases

AIS CLI supports user-defined aliases, similar to the Unix `alias` command.

## Table of Contents

## Create an Alias

`ais alias set ALIAS AIS_COMMAND`

Create an alias "`ALIAS`" for `AIS_COMMAND`.

Note: ALIAS is always a single word, or possibly multiple words connected with - (hyphen) or _ (underscore). The rest of arguments is the full command.
The command can be put inside quotes for readability.

For example, `ais alias set show-clu show cluster` and `ais alias set show-clu "show cluster"` creates the same alias.

### Examples

```console
$ ais alias sc "show cluster"
Aliased "show cluster"="sc"

$ ais sc
PROXY            MEM USED %      MEM AVAIL       UPTIME
IWOup8082        0.25%           15.43GiB        24h
Kflkp8083        0.25%           15.43GiB        24h
xqfwp8081        0.25%           15.43GiB        24h
hJzRp8084        0.26%           15.43GiB        24h
WSLop8080[P]     0.29%           15.43GiB        24h

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME
ejpCt8086        0.38%           15.43GiB        14.00%          1.951TiB        0.12%           -               24h
CASGt8088        0.35%           15.43GiB        14.00%          1.951TiB        0.11%           -               24h
xZntt8087        0.36%           15.43GiB        14.00%          1.951TiB        0.11%           -               24h
Hwbmt8085        0.31%           15.43GiB        14.00%          1.951TiB        0.12%           -               24h
DMwvt8089        0.37%           15.43GiB        14.00%          1.951TiB        0.11%           -               24h

Summary:
 Proxies:       5 (0 unelectable)
 Targets:       5
 Primary Proxy: WSLop8080
 Smap Version:  43
 Deployment:    dev

$ ais sc CASGt8088
TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME  DEPLOYMENT        STATUS
CASGt8088        0.35%           15.43GiB        14.00%          1.951TiB        0.11%           -               24h     dev      online
```

## Remove Alias

`ais alias rm ALIAS`

Removes existing alias "`ALIAS`".

### Examples

```console
$ ais alias
ALIAS   COMMAND
get     object get
ls      bucket ls
put     object put
sc      show cluster

$ ais alias rm sc

$ ais alias
ALIAS   COMMAND
get     object get
ls      bucket ls
put     object put
```

## List Aliases

`ais alias show` or `ais alias`

List all created aliases.
Similar to how the `alias` command works, `ais alias` lists all aliases when no arguments are provided.

Note that aliases are also shown in the app-level help message (`ais -h`).

### Examples

#### List aliases

```console
$ ais alias
ALIAS   COMMAND
get     object get
ls      bucket ls
put     object put

$ ais alias set sc "show cluster"
Aliased "sc"="show cluster"

$ ais alias
ALIAS   COMMAND
get     object get
ls      bucket ls
put     object put
sc      show cluster
```

#### View aliases from app-level help

```console
$ ais -h
NAME:
   ais - AIS CLI: command-line management utility for AIStore(tm)

USAGE:
   ais [global options] command [command options] [arguments...]

VERSION:
   0.5 (build aff1f037d)

COMMANDS:
   bucket     create/destroy buckets, list bucket's content, show existing buckets and their properties
   object     PUT (write), GET (read), list, move (rename) and other operations on objects in a given bucket
   cluster    monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.
   config     set local/global AIS cluster configurations
   mountpath  manage mountpaths (disks) in a given storage target
   etl        execute custom transformations on objects
   job        query and manage jobs (aka extended actions or xactions)
   auth       add/remove/show users, manage user roles, manage access to remote clusters
   show       show information about buckets, jobs, all other managed entities in the cluster and the cluster itself
   help       show a list of commands; show help for a given command
   advanced   special commands intended for development and advanced usage
   alias      create top-level alias to a CLI command
   search     search ais commands

   ALIASES:
     sc   (alias for "show cluster") show cluster details
     get  (alias for "object get") get the object from the specified bucket
     ls   (alias for "bucket ls") list buckets and their objects
     put  (alias for "object put") put the objects into the specified bucket

GLOBAL OPTIONS:
   --help, -h     show help
   --no-color     disable colored output
   --version, -V  print only the version
```

## Reset Aliases

`ais alias reset`

Clear all created aliases, and only keep the defaults.

### Example

```console
$ ais alias
ALIAS   COMMAND
get     object get
ls      bucket ls
put     object put
sc      show cluster

$ ais alias reset
Aliases reset to default

$ ais alias
ALIAS   COMMAND
get     object get
ls      bucket ls
put     object put
```

## Alias Configuration File

As with other CLI configurations, aliases are stored in the [CLI config file](/docs/cli.md#config).

All aliases are stored under `"aliases"` as a map of strings (`ALIAS` to `AIS_COMMAND`).
If an incorrect alias is manually added to the config file, it will be silently ignored.

```json
// cat ~/.config/ais/config.json
{
  "cluster": {
    "url": "http://127.0.0.1:8080",
    "default_ais_host": "http://127.0.0.1:8080",
    "default_docker_host": "http://172.50.0.2:8080",
    "skip_verify_crt": false
  },
  "timeout": {
    "tcp_timeout": "60s",
    "http_timeout": "0s"
  },
  "auth": {
    "url": "http://127.0.0.1:52001"
  },
  "aliases": {
    "sc": "show cluster",
    "create": "create buckets"
  }
}
```
