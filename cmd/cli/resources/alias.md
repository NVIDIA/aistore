# CLI Reference for Aliases

AIS CLI supports user defined aliases, similar to the unix `alias` command.

## Table of Contents

## Create an Alias

`ais alias AIS_COMMAND --as ALIAS`

Create an alias "`ALIAS`" for `AIS_COMMAND`.

Note: `ALIAS` must be one word. For example, `--as show-clu` is valid, but `--as show clu` is not.

### Examples

```console
$ ais alias show cluster --as sc
aliasing "show cluster" = "sc"

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
 Proxies:       5 (0 - unelectable)
 Targets:       5
 Primary Proxy: WSLop8080
 Smap Version:  43
 Deployment:    dev

$ ais sc CASGt8088 
TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME  DEPLOYMENT        STATUS
CASGt8088        0.35%           15.43GiB        14.00%          1.951TiB        0.11%           -               24h     dev      online
```

## Alias Config

As with other CLI configurations, aliases are stored in the [CLI config file](/cmd/cli/README.md#config).

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
