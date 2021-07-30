---
layout: post
title: CLI
permalink: cmd/cli
redirect_from:
 - cmd/cli/README.md/
---

# Table of content

- [Getting Started](#getting-started)
- [Using AIS CLI](#using-ais-cli)
	- [Config](#config)
	- [First steps](#first-steps)
	- [Global options](#global-options)
- [AIS CLI Shell Autocomplete](#ais-cli-shell-autocomplete)
	- [Installing](#installing)
	- [Uninstalling](#uninstalling)
- [CLI reference](#cli-reference)
- [Info For Developers](#info-for-developers)
	- [Adding New Commands](#adding-new-commands)
- [Default flag and argument values via environment variables](#default-flag-and-argument-values-via-environment-variables)


AIS CLI (command-line interface) is intended to easily control and monitor every aspect of the AIS cluster life-cycle.
In addition, CLI provides dataset management commands, commands to read and write data, and more.

**TL;DR**: see section [CLI reference](#cli-reference) below to quickly locate useful commands. There's also a (structured as a reference) list CLI resources with numerous examples and usage guides that we constantly keep updating.

[![AIStore CLI Demo](/docs/images/cli-demo-400.png)](https://youtu.be/VPIhQm2sMD8 "AIStore CLI Demo (Youtube video)")

## Getting Started

Run `make cli` in AIStore repo root directory to install AIS CLI binary in your `$GOPATH/bin` directory.
You can also install [shell autocompletions](#ais-cli-shell-autocomplete) for AIS CLI with `make cli-autocompletions`.

> The rest of the README assumes that user's `PATH` environment variable contains `$GOPATH/bin` directory.
> Run `export PATH=$PATH:$GOPATH/bin` if this is not the case for you.
> You can read more about GOPATH environment variable [here](https://golang.org/doc/code.html#GOPATH).

## Using AIS CLI

### Config

On first use, CLI will create `config.json` file in `$XDG_CONFIG_HOME/ais` (or if `XDG_CONFIG_HOME` is not set, in `~/.config/ais`) directory.
The content of the file presents as follows:

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

Simply change config file so next time CLI will use updated values.

### First steps

To get the list of commands, run following command:

```console
$ ais --help
```

To check if the CLI can correctly contact the cluster and to get cluster status, run following command:

```console
$ ais show cluster
```

### Global options

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

## AIS CLI Shell Autocomplete

The CLI tool supports `bash` and `zsh` auto-complete functionality.

### Installing

When running `install.sh` you will be asked if you want to install autocompletions.
To install them manually, run `bash autocomplete/install.sh`.

### Uninstalling

To uninstall autocompletions, run `bash autocomplete/uninstall.sh`.

## CLI reference

| Command | Use Case |
|---------|----------|
| [`ais bucket`](/cmd/cli/resources/bucket.md) | Create/destroy buckets, list bucket's content, show existing buckets and their properties |
| [`ais object`](/cmd/cli/resources/object.md) | PUT (write), GET (read), list, move (rename) and other operations on objects in a given bucket |
| [`ais cluster`](/cmd/cli/resources/cluster.md) | Monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc. |
| [`ais config`](/cmd/cli/resources/config.md) | Set local/global AIS cluster configurations |
| [`ais mountpath`](/cmd/cli/resources/mpath.md) | Manage mountpaths (disks) in a given storage target |
| [`ais etl`](/cmd/cli/resources/etl.md) | Execute custom transformations on objects |
| [`ais job`](/cmd/cli/resources/job.md) | Query and manage jobs (aka extended actions or xactions) |
| [`ais auth`](/cmd/cli/resources/auth.md) | Add/remove/show users, manage user roles, manage access to remote clusters |
| [`ais show`](/cmd/cli/resources/show.md) | Show information about buckets, jobs, all other managed entities in the cluster and the cluster itself |
| [`ais advanced`](/cmd/cli/resources/advanced.md) | Special commands intended for development and advanced usage |
| [`ais search`](/cmd/cli/resources/search.md) | Search ais commands |

Other CLI documentation:
- [Attach, Detach, and monitor remote clusters](/cmd/cli/resources/remote.md)
- [Start, Stop, and monitor downloads](/cmd/cli/resources/download.md)
- [Distributed Sort](/cmd/cli/resources/dsort.md)

> Note: In CLI docs, the terms "xaction" and "job" are used interchangeably.

## Info For Developers

AIS CLI utilizes [urfave/cli](https://github.com/urfave/cli/blob/master/docs/v1/manual.md) open-source framework.

### Adding New Commands

Currently, the CLI has the format of `ais <resource> <command>`.

To add a new command to an existing resource:

1. Create a subcommand entry for the command in the resource object
2. Create an entry in the command's flag map for the new command
3. Register flags in the subcommand object
4. Register the handler function (named `XXXHandler`) in the subcommand object

To add a new resource:

1. Create a new Go file (named `xxx_hdlr.go`) with the name of the new resource and follow the format of existing files
2. Once the new resource and its commands are implemented, make sure to register the new resource with the CLI (see `setupCommands()` in `app.go`)

## Default flag and argument values via environment variables

#### Backend Provider

The syntax `provider://BUCKET_NAME` (referred to as `BUCKET` in help messages) works across all commands.
For more details, please refer to each specific command's documentation.
`provider://` can be omitted if the `default_provider` config value is set (in such case the config value will be used implicitly).

Supported backend providers currently include:
* `ais://` - AIStore provider
* `aws://` or `s3://` - Amazon Web Services
* `azure://` or `az://` - Azure Blob Storage
* `gcp://` or `gs://` - Google Cloud Storage
* `hdfs://` - HDFS Storage
* `ht://` (\* see below) - HTTP(S) datasets

> See also: [Backend Providers](/docs/providers.md)
>
> See also: [Buckets](/docs/bucket.md)
