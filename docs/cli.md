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
- [AIS CLI Shell Autocomplete](#ais-cli-shell-autocomplete)
  - [Installing](#installing)
  - [Uninstalling](#uninstalling)
- [CLI Reference](#cli-reference)
- [CLI Config](#cli-config)
- [First steps](#first-steps)
- [Global options](#global-options)
- [Backend Provider](#backend-provider)


AIS CLI (command-line interface) is intended to easily control and monitor every aspect of the AIS cluster life-cycle. 
In addition, CLI provides dataset management commands, commands to read and write data, and more.

You should be able to start by running ais <TAB-TAB>, selecting one of the available (completion) options, and repeating until the command is ready to be entered.

**TL;DR**: see section [CLI reference](#cli-reference) below to quickly locate useful commands. There's also a (structured as a reference) list of CLI resources with numerous examples and usage guides that we constantly keep updating.

**TIP**: when starting with AIS, [`ais search`](/docs/cli/search.md) command may be especially handy. It will list all possible variations of a command you are maybe looking for - by exact match, synonym, or regex.

{% include youtubePlayer.html id="VPIhQm2sMD8" %}

## Getting Started

Run `make cli` in AIStore repo root directory to install the AIS CLI binary in your `$GOPATH/bin` directory.
You can also install [shell autocompletions](#ais-cli-shell-autocomplete) for AIS CLI with `make cli-autocompletions`.

> The rest of the README assumes that user's `PATH` environment variable contains `$GOPATH/bin` directory.
> Run `export PATH=$PATH:$GOPATH/bin` if this is not the case for you.
> You can read more about GOPATH environment variable [here](https://golang.org/doc/code.html#GOPATH).

## AIS CLI Shell Autocomplete

The CLI tool supports `bash` and `zsh` auto-complete functionality.

### Installing

When running `install.sh` you will be asked if you want to install autocompletions.
To install them manually, run `bash autocomplete/install.sh`.

### Uninstalling

To uninstall autocompletions, run `bash autocomplete/uninstall.sh`.

## CLI Reference

| Command | Use Case |
|---------|----------|
| [`ais advanced`](/docs/cli/advanced.md) | Special commands for developers and advanced usage. |
| [`ais alias`](/docs/cli/alias.md) | User-defined command aliases. |
| [`ais archive`](/docs/cli/archive.md) | Read, write, and list archives (i.e., objects formatted as TAR, TGZ, ZIP, etc.) |
| [`ais auth`](/docs/cli/auth.md) | Add/remove/show users, manage user roles, manage access to remote clusters. |
| [`ais bucket`](/docs/cli/bucket.md) | Create/destroy buckets, list bucket's content, show existing buckets and their properties. |
| [`ais cluster`](/docs/cli/cluster.md) | Monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc. |
| [`ais config`](/docs/cli/config.md) | Set local/global AIS cluster configurations. |
| [`ais etl`](/docs/cli/etl.md) | Execute custom transformations on objects. |
| [`ais job`](/docs/cli/job.md) | Query and manage jobs (aka extended actions or xactions). |
| [`ais object`](/docs/cli/object.md) | PUT (write), GET (read), list, move (rename) and other operations on objects in a given bucket. |
| [`ais search`](/docs/cli/search.md) | Search `ais` commands. |
| [`ais show`](/docs/cli/show.md) | Show information about buckets, jobs, all other managed entities in the cluster and the cluster itself. |
| [`ais storage`](/docs/cli/storage.md) | Show capacity usage on a per bucket basis, attach/detach mountpaths (disks), run certain bucket validation logic, and more. |
{: .nobreak}

Other CLI documentation:
- [Attach, Detach, and monitor remote clusters](/docs/cli/remote.md)
- [Start, Stop, and monitor downloads](/docs/cli/download.md)
- [Distributed Sort](/docs/cli/dsort.md)

> Note: In CLI docs, the terms "xaction" and "job" are used interchangeably.

## CLI Config

Notice:

* CLI configuration directory: `$HOME/.config/ais`
* CLI configuration filename: `cli.json`

When used the very first time, *or* if the `$HOME/.config/ais/cli.json` does not exist, the latter will be created with default parameters:

```json
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
    "get": "object get",
    "ls": "bucket ls",
    "put": "object put"
  },
  "default_provider": "ais"
}
```

If you update config via `ais config cli set` command (or even simply change the config file) the next time CLI will use updated values.

## First steps

To get the list of supported commands, run:

```console
$ ais --help
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
$ ais bucket ls ais://bck --props all --no-color
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
