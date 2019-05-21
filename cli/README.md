# AIS CLI

AIS CLI is a tool used to interact with resources of AIStore. Users are able to query information from specific daemons, create or delete resources or download files to buckets directly from the command line.

## Getting Started

Run the `install.sh` script to install AIS CLI binary in your `$GOPATH/bin` directory.  
The script also allows you to install [shell autocompletions](#ais-cli-shell-auto-complete) for AIS CLI.
> The rest of the README assumes that user's `PATH` environment variable contains `$GOPATH/bin` directory.
> Run `export PATH=$PATH:$GOPATH/bin` if this is not the case for you.  
> You can read more about GOPATH environment variable [here](https://golang.org/doc/code.html#GOPATH).

## Using AIS CLI

Before using the CLI, we need to export the `AIS_URL` environment variable (eg. `http://<YOUR_CLUSTER_IP>:<PORT>`) to configure the CLI tool to point to the AIStore cluster.
 ```sh
 $ export AIS_URL=http://localhost:8080
 $ ais --help
 ```
 This should return the list of commands for the CLI.

> By default, the CLI is configured to point to `http://172.50.0.2:8080` and `http:/127.0.0.1:8080` for local containerized and non-containerized deployments respectively.

## AIS CLI Shell Auto-Complete

The CLI tool supports bash and zsh auto-complete functionality.

##### Installing

When running `install.sh` you will be asked if you want to install autocompletions.  
To install them manually, run `bash autocomplete/install.sh`.

##### Uninstalling

To uninstall autocompletions run `bash autocomplete/uninstall.sh`.

## Supported Resources

List of available CLI resources

* [Bucket](./resources/bucket.md)

* [Daemon/Cluster](./resources/daeclu.md)

* [Downloader](./resources/downloader.md)

* [Object](./resources/object.md)

* [Xaction](./resources/xaction.md)

* [DSort](./resources/dsort.md)

## Info For Developers

The framework that the CLI uses is [urfave](https://github.com/urfave/cli). It is a simple framework that enables developers to create custom CLI commands quickly.

### Adding New Commands

Currently, the CLI has the format of '`ais <resource> <command>`'.

To add a new command to an existing resource,

1. Create an entry in the resource's flag map and add the entry to the commands object
2. Register the new command in the corresponding resource handler (it should be named something similar to `XXXHandler`)

To add a new command to a new resource,

1. Create a new `.go` file with the name of the new resource and follow the format of the existing files
2. Once the new resource and commands are implemented, make sure to add the new command set to the main function located in `ais.go`.

## Default flag values via environment variables

#### Bucket
If `AIS_BUCKET` environment variable is set, the `--bucket` flag is set to the value of this variable.
Setting `--bucket` flag overwrites the default value.

#### Bucket Provider
If `AIS_BUCKET_PROVIDER` environment variable is set, the `--provider` flag is set to the value of this variable.
Setting `--provider` flag overwrites the default value.

## Enums

| Enum | Values | Description |
| --- | --- | --- |
| Provider | `local`, `cloud`, `""` | Locality of the bucket. If empty, AIS automatically determines the locality. |
