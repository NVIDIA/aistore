---
layout: post
title: CLI
permalink: cmd/cli
redirect_from:
 - cmd/cli/README.md/
---

AIS CLI (command-line interface) is intended to easily control and monitor every aspect of the AIS cluster life-cycle.
In addition, CLI provides dataset management commands, commands to read and write data, and more.

**TL;DR**: see section [CLI reference](#cli-reference) below to quickly locate useful commands. There's also a (structured as a reference) list CLI resources with numerous examples and usage guides that we constantly keep updating.

<img src="/aistore/docs/images/ais2.13.gif" alt="CLI-playback" width="900">

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
    "default_docker_host": "http://172.50.0.2:8080"
  },
  "timeout": {
    "tcp_timeout": "60s",
    "http_timeout": "0s"
  },
  "auth": {
    "url": "http://127.0.0.1:52001"
  }
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

## AIS CLI Shell Autocomplete

The CLI tool supports `bash` and `zsh` auto-complete functionality.

### Installing

When running `install.sh` you will be asked if you want to install autocompletions.
To install them manually, run `bash autocomplete/install.sh`.

### Uninstalling

To uninstall autocompletions, run `bash autocomplete/uninstall.sh`.

## CLI reference

- [Create, destroy, list, and other operations on buckets](resources/bucket.md)
- [GET, PUT, APPEND, PROMOTE, and other operations on objects](resources/object.md)
- [Cluster and Node management](resources/daeclu.md)
- [Mountpath (Disk) management](resources/mpath.md)
- [Attach, Detach, and monitor remote clusters](resources/remote.md)
- [Start, Stop, and monitor downloads](resources/download.md)
- [Distributed Sort](resources/dsort.md)
- [User account and access management](resources/users.md)
- [Xaction (Job) management](resources/xaction.md)
- [Search CLI Commands](resources/search.md)

## Info For Developers

The CLI uses [urfave/cli](https://github.com/urfave/cli) framework.

### Adding New Commands

Currently, the CLI has the format of `ais <command> <resource>`.

To add a new resource to an existing command,

1. Create a subcommand entry for the resource in the command object
2. Create an entry in the command's flag map for the new resource
3. Register flags in the subcommand object
4. Register the handler function (named `XXXHandler`) in the subcommand object

To add a new resource to a new command,

1. Create a new Go file (named `xxx_hdlr.go`) with the name of the new command and follow the format of existing files
2. Once the new command and subcommands are implemented, make sure to register the new command with the CLI (see `setupCommands()` in `app.go`)

## Default flag and argument values via environment variables

#### Bucket Provider

Provider syntax `[provider://]BUCKET_NAME` is valid CLI-wide, meaning that every command supporting `BUCKET_NAME` argument
also supports provider syntax. For more details refer to each command's documentation.

Allowed values: `''` (autodetect provider), `ais` (local cluster), `aws` (Amazon Web Services), `gcp` (Google Cloud Platform),
`azure` (Microsoft Azure), `cloud` (anonymous - cloud provider determined automatically).
Additionally `provider://` syntax supports aliases `s3` (for `aws`), `gs` (for `gcp`) and `az` (for `azure`).
{% include_relative videos.md %}
