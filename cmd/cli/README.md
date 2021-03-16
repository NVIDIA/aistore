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

<img src="/docs/images/ais2.13.gif" alt="CLI-playback" width="900">

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

- [Create, destroy, list, and other operations on buckets](resources/bucket.md)
- [GET, PUT, APPEND, PROMOTE, and other operations on objects](resources/object.md)
- [Cluster and Node management](resources/cluster.md)
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

#### Backend Provider

Provider's syntax `provider://BUCKET_NAME` (referred as `BUCKET`) works across all commands.
For more details, please refer to each specific command's documentation.

Supported backend providers currently include:
* `ais://` - AIStore provider
* `aws://` or `s3://` - Amazon Web Services
* `azure://` or `az://` - Azure Blob Storage
* `gcp://` or `gs://` - Google Cloud Storage
* `hdfs://` - HDFS Storage
* `ht://` (\* see below) - HTTP(S) datasets

> See also: [Backend Providers](/docs/providers.md)
> See also: [Buckets](/docs/bucket.md)
