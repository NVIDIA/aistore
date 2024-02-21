---
layout: post
title: CLI
permalink: /docs/cli
redirect_from:
 - /cli.md/
 - /docs/cli.md/
---

## Table of contents

- [Getting Started](#getting-started)
- [CLI Reference](#cli-reference)
- [CLI Config](#cli-config)
- [Environment variables](#environment-variables)
- [First steps](#first-steps)
- [Global options](#global-options)
- [Backend Provider](#backend-provider)
- [Verbose errors](#verbose-errors)


AIS CLI (command-line interface) is intended to easily control and monitor every aspect of the AIS cluster life-cycle.
In addition, CLI provides dataset management commands, commands to read and write data, and more.

## Getting Started

To build CLI from source, run the following two steps:

```console
$ make cli			# 1. build CLI binary and install it into your `$GOPATH/bin` directory
$ make cli-autocompletions	# 2. install CLI autocompletions (Bash and/or Zsh)
```

To build with debug, run:

```console
$ MODE=debug make cli
```

Alternatively, install directly from GitHub:

* [Install CLI from release binaries](https://github.com/NVIDIA/aistore/blob/main/deploy/scripts/install_from_binaries.sh)

For example, the following command extracts CLI binary to the specified destination and, secondly, installs `bash` autocompletions:

```console
$ ./deploy/scripts/install_from_binaries.sh --dstdir /tmp/www --completions
```

For more usage options, run: `./deploy/scripts/install_from_binaries.sh --help`

You can also install `bash` and/or `zsh` autocompletions separately at any (later) time:

* [Install CLI autocompletions](https://github.com/NVIDIA/aistore/blob/main/cmd/cli/install_autocompletions.sh)

To uninstall autocompletions, follow the `install_autocompletions.sh` generated prompts, or simply run `bash autocomplete/uninstall.sh`.

**Please note**: using CLI with autocompletions enabled is strongly recommended.

Once installed, you should be able to start by running ais `<TAB-TAB>`, selecting one of the available (completion) options, and repeating until the command is ready to be entered.

**TL;DR**: see section [CLI reference](#cli-reference) below to quickly locate useful commands. There's also a (structured as a reference) list of CLI resources with numerous examples and usage guides that we constantly keep updating.

**TIP**: when starting with AIS, [`ais search`](/docs/cli/search.md) command may be especially handy. It will list all possible variations of a command you are maybe looking for - by exact match, synonym, or regex.

See also:

* [cmd/cli/README.md](https://github.com/NVIDIA/aistore/blob/main/cmd/cli/README.md)

> The rest of the README assumes that user's `PATH` environment variable contains `$GOPATH/bin` directory.
> Run `export PATH=$PATH:$GOPATH/bin` if this is not the case.
> You can find more about $GOPATH environment [here](https://golang.org/doc/code.html#GOPATH).

## CLI Reference

The recommended and, actually, fastest way to get started with CLI is to type `ais` and press `<TAB-TAB>`:

```console
$ ais <TAB-TAB>

bucket        etl         help           log              create        dsort        stop         blob-download
object        job         advanced       performance      download      evict        cp           rmo
cluster       auth        storage        remote-cluster   prefetch      get          rmb          wait
config        show        archive        alias            put           ls           start        search
```

These are the top-level commands as of early 2024. Each command has its own extended help (`--help`) and sub-commands
(which, in turn, have their respective helps and subcommands).

The list of top-level commands must give maybe the first idea of the supported functionality and functional grouping.

Following is a brief summary (that's non-exhaustive and slightly outdated):

| Command | Use Case |
|---------|----------|
| [`ais help`](/docs/cli/help.md) | All top-level commands and brief descriptions; version and build; general usage guidelines. |
| [`ais advanced`](/docs/cli/advanced.md) | Special commands for developers and advanced usage. |
| [`ais alias`](/docs/cli/alias.md) | User-defined command aliases. |
| [`ais archive`](/docs/cli/archive.md) | Read, write, and list archives (i.e., objects formatted as TAR, TGZ, ZIP, etc.) |
| [`ais auth`](/docs/cli/auth.md) | Add/remove/show users, manage user roles, manage access to remote clusters. |
| [`ais bucket`](/docs/cli/bucket.md) | Create/destroy buckets, list bucket's content, show existing buckets and their properties. |
| [`ais cluster`](/docs/cli/cluster.md) | Monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc. |
| [`ais config`](/docs/cli/config.md) | Set local/global AIS cluster configurations. |
| [`ais etl`](/docs/cli/etl.md) | Execute custom transformations on objects. |
| [`ais job`](/docs/cli/job.md) | Query and manage jobs (aka eXtended actions or `xactions`). |
| [`ais object`](/docs/cli/object.md) | PUT and GET (write and read), APPEND, archive, concat, list (buckets, objects), move, evict, promote, ... |
| [`ais search`](/docs/cli/search.md) | Search `ais` commands. |
| [`ais show`](/docs/cli/show.md) | Monitor anything and everything: performance (all aspects), buckets, jobs, remote clusters, and more. |
| [`ais log`](/docs/cli/log.md) | Download ais nodes' logs or view the logs in real time. |
| [`ais storage`](/docs/cli/storage.md) | Show capacity usage on a per bucket basis (num objects and sizes), attach/detach mountpaths (disks). |
{: .nobreak}

Other CLI documentation:
- [Attach, Detach, and monitor remote clusters](/docs/cli/remote.md)
- [Start, Stop, and monitor downloads](/docs/cli/download.md)
- [Distributed Sort](/docs/cli/dsort.md)

> Note: In CLI docs, the terms "xaction" and "job" are used interchangeably.

## CLI Config

Notice:

* CLI configuration directory: `$HOME/.config/ais/cli`
* CLI configuration filename: `cli.json`

> For the most updated system filenames and configuration directories, please see [`fname/fname.go`](https://github.com/NVIDIA/aistore/blob/main/cmn/fname/fname.go) source.


When used the very first time, *or* if the `$HOME/.config/ais/cli/cli.json` does not exist, the latter will be created with default parameters:

```json
$ ais config cli --json

{
    "cluster": {
        "url": "http://127.0.0.1:8080",
        "default_ais_host": "http://127.0.0.1:8080",
        "default_docker_host": "http://172.50.0.2:8080",
        "client_crt": "",
        "client_crt_key": "",
        "client_ca_tls": "",
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
        "wait": "job wait",
        "cp": "bucket cp",
        "create": "bucket create",
        "download": "job start download",
        "evict": "bucket evict",
        "get": "object get",
        "put": "object put",
        "dsort": "job start dsort",
        "ls": "bucket ls",
        "rmb": "bucket rm",
        "start": "job start",
        "stop": "job stop"
    },
    "default_provider": "ais",
    "no_color": false,
    "verbose": false
}
```

CLI config can be updated using `ais config cli set` command or even simply by changing the config file.

The next time you run it CLI will use the updated values.

To get back to system defaults, run `ais config cli reset`.

## Environment variables

First and foremost, there's `AIS_ENDPOINT`. If defined, it'll take precedence over "cluster.url" (section [CLI Config](#cli-config) above).

Example:

```console
$ export AIS_ENDPOINT=https://10.07.56.68:51080
```

In addition, environment can be used to **override** client-side TLS (aka, HTTPS) configuration - the knobs "client_crt", etc. also listed in the table below:

| var name | description | the corresponding [CLI Config](#cli-config) |
| -- | -- | -- |
| `AIS_CRT`             | X509 certificate | "cluster.client_crt" |
| `AIS_CRT_KEY`         | X509 certificate's private key | "cluster.client_crt_key"|
| `AIS_CLIENT_CA`       | Certificate authority that authorized (signed) the certificate | "cluster.client_ca_tls" |
| `AIS_SKIP_VERIFY_CRT` | true: skip X509 cert verification (usually enabled to circumvent limitations of self-signed certs) | "cluster.skip_verify_crt" |

* See also: [TLS: testing with self-signed certificates](/docs/getting_started.md#tls-testing-with-self-signed-certificates)

## First steps

To get the list of supported commands, run:

```console
$ ais help
```

> Alternatively, you could start making use of auto-completions by typing `ais ` and pressing TAB key two times in a row.

To check if the CLI can correctly contact the cluster and to get cluster status, run following command:

```console
$ ais show cluster
```

## Global options

Besides a set of options specific for each command, AIS CLI provides global options:

- `--no-color` - by default AIS CLI displays messages with colors (e.g, errors are printed in red color).
  Colors are automatically disabled if CLI output is redirected or environment variable `TERM=dumb` is set.
  To disable colors in other cases, pass `--no-color` to the application.

Please note that the place of a global options in the command line is fixed.
Global options must follow the application name directly.
At the same time, the location of a command-specific option is arbitrary: you can put them anywhere.
Examples:

```console
$ # Correct usage of global and command-specific options.
$ ais --no-color ls ais://bck --props all
$ ais --no-color ls --props all ais://bck
$
$ # Incorrect usage of a global option.
$ ais ls ais://bck --props all --no-color
```

## Backend Provider

The syntax `provider://BUCKET_NAME` (referred to as `BUCKET` in help messages) works across all commands.
For more details, please refer to each specific command's documentation.
`provider://` can be omitted if the `default_provider` config value is set (in such case the config value will be used implicitly).

Supported backend providers currently include:
* `ais://` - AIStore provider
* `aws://` or `s3://` - Amazon Web Services
* `azure://` or `az://` - Azure Blob Storage
* `gcp://` or `gs://` - Google Cloud Storage
* `hdfs://` - HDFS Storage
* `ht://` - HTTP(S) datasets

See also:

> [Backend Providers](/docs/providers.md)
> [Buckets: definition, operations, properties](/docs/bucket.md)

## Verbose errors

CLI uses [AIS API](https://github.com/NVIDIA/aistore/tree/main/api) to execute operations on a cluster.

Of course, a remote API call - any API call, for that matter - may return errors. For developers, it may be sometimes useful to see a complete and unredacted error information.

Here's an example where we are trying to rename a non-existing bucket:

```console
$ ais bucket mv ais://ddd ais://mmm
Error: bucket "ais://ddd" does not exist
```

But here's how it'll look once we put CLI in verbose mode:

```console
$ ais config cli set verbose true
"verbose" set to: "true" (was: "false")

$ ais bucket mv ais://ddd ais://mmm
Error: {"tcode":"ErrBckNotFound","message":"bucket \"ais://ddd\" does not exist","method":"HEAD","url_path":"/v1/buckets/ddd","remote_addr":"127.0.0.1:57026","caller":"","node":"p[JFkp8080]","status":404}: HEAD /v1/buckets/ddd (stack: [utils.go:445 <- bucket.go:104 <- bucket_hdlr.go:343])
```
