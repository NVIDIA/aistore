# AIS CLI

AIS Command Line Interface is a tool used to interact with resources of AIStore. It gives users ability to query information from specific daemons,
create or delete resources or download files to buckets directly from the command line.

## Getting Started

Run the `install.sh` script to install AIS CLI binary in your `$GOPATH/bin` directory.
The script also allows you to install [shell autocompletions](#ais-cli-shell-auto-complete) for AIS CLI.
> The rest of the README assumes that user's `PATH` environment variable contains `$GOPATH/bin` directory.
> Run `export PATH=$PATH:$GOPATH/bin` if this is not the case for you.
> You can read more about GOPATH environment variable [here](https://golang.org/doc/code.html#GOPATH).

## Using AIS CLI

AIS CLI makes requests to AIStore cluster. It resolves cluster address in the following order:
1. `AIS_URL` environment variable (eg. `http://<YOUR_CLUSTER_IP>:<PORT>`); if not present:
2. Discovers IP address of proxy kubernetes pod; if kubernetes runs multiple clusters, set an environment variable `AIS_NAMESPACE` to select a proxy from the given namespace
3. Discovers IP address of proxy docker container; if multiple docker clusters running, picks the IP address of one of them and prints relevant message;
if not successful or local non-containerized deployment:
4. Default `http://172.50.0.2:8080` and `http://127.0.0.1:8080` for local containerized and non-containerized deployments respectively

This command returns the list of commands for the CLI.
 ```sh
 $ ais --help
 ```
This command returns status of the cluster; if successful, cluster address was resolved correctly.
 ```sh
 $ ais status
 ```

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

* [Object](./resources/object.md)

* [Daemon/Cluster](./resources/daeclu.md)

* [Xaction](./resources/xaction.md)

* [Downloader](./resources/download.md)

* [DSort](./resources/dsort.md)

* [Auth](./resources/users.md)

## Info For Developers

The CLI uses [urfave/cli](https://github.com/urfave/cli) framework.

### Adding New Commands

Currently, the CLI has the format of '`ais <command> <resource>`'.

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
If `AIS_BUCKET_PROVIDER` environment variable is set, the `--provider` flag is set to the value of this variable.
Setting `--provider` flag overwrites the default value.

## Enums

| Enum | Values | Description |
| --- | --- | --- |
| Provider | `ais`, `cloud`, `""` | Locality of the bucket. If empty, AIS automatically determines the locality. |
