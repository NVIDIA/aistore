# CLI Reference for Aliases

AIS CLI supports user-defined aliases, similar to the Unix `alias` command. Defining your own alias for an existing command can make the AIS CLI more intuitive and efficient to use.

> [Auto-complete](/docs/cli.md) options also hold for the alias you create!

The current set of default aliases (which you may also redefine or remove at any time) includes:

```console
$ ais alias
ALIAS           COMMAND
=====           =======
cp              bucket cp
create          bucket create
evict           bucket evict
ls              bucket ls
rmb             bucket rm
start           job start
blob-download   job start blob-download
download        job start download
dsort           job start dsort
stop            job stop
wait            job wait
get             object get
prefetch        object prefetch
put             object put
rmo             object rm
space-cleanup   storage cleanup
scrub           storage validate
```

## Table of Contents

## Create an Alias

`ais alias set ALIAS AIS_COMMAND`

Create an alias "`ALIAS`" for existing `AIS_COMMAND`.

Note: ALIAS must be a single word, or multiple words connected with - (hyphen) or _ (underscore). The arguments following ALIAS constitute the full `ais` command.
The `ais` command can be put inside quotes for readability.

For example, `ais alias set show-clu show cluster` and `ais alias set show-clu "show cluster"` create the same alias for `show cluster`.

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
`ais alias` with no arguments lists all previously added aliases -- the same behavior you expect from Unix shell `alias`. 

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
   0.8 (build 30bf97129)

DESCRIPTION:
   If [Tab] completion doesn't work:
   * download https://github.com/NVIDIA/aistore/tree/main/cmd/cli/autocomplete
   * run 'cmd/cli/autocomplete/install.sh'
   For more information, please refer to https://github.com/NVIDIA/aistore/blob/main/cmd/cli/README.md

COMMANDS:
   bucket          create/destroy buckets, list bucket's content, show existing buckets and their properties
   object          put, get, rename, remove, and other operations on objects
   cluster         monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.
   config          set local/global AIS cluster configurations
   etl             execute custom transformations on objects
   job             query and manage jobs (aka extended actions or xactions)
   auth            add/remove/show users, manage user roles, manage access to remote clusters
   show            show information about buckets, jobs, all other managed entities in the cluster and the cluster itself
   help            show a list of commands; show help for a given command
   advanced        special commands intended for development and advanced usage
   storage         monitor and manage clustered storage
   archive         Create archive and append files to archive
   log             show log
   rebalance       show rebalance details
   remote-cluster  show attached AIS clusters
   alias           manage top-level aliases
   kubectl         show kubernetes pods and services
   search          search ais commands

   ALIASES:
     get  (alias for "object get") get object from the specified bucket
     put  (alias for "object put") put object(s) into the specified bucket
     ls   (alias for "bucket ls") list buckets and their objects

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

```json
// cat ~/.config/ais/cli/cli.json
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
Users can manually add aliases to the config file, but all commands must follow the rules for [creating an alias](#create-an-alias).
E.g., aliases:

```json
   "show clu": "show cluster",
   "show-clu": "show kluster",
```
are ignored because the name of the first one is not a single (or hyphenated ) word, while the AIS command of the second one does not exist.
