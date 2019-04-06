# AIS CLI

AIS CLI is a tool used to interact with resources of AIStore. Users are able to query information from specific daemons, create or delete resources or download files to buckets directly from the command line.

## Getting Started

By default, the CLI tool is automatically installed as a binary in the system's `GOPATH` when AIStore is deployed.

## Using AIS CLI

Before using the CLI, we need to export the `AIS_URL` environment variable (eg. `http://<YOUR_CLUSTER_IP>:<PORT>`) to configure the CLI tool to point to the AIStore cluster.
 ```sh
 $ export AIS_URL=http://localhost:8080
 $ ais --help
 ```
 This should return the list of commands for the CLI.

## AIS CLI Shell Auto-Complete

The CLI tool supports bash and zsh auto-complete functionality. To enable shell auto-complete, source the auto-complete script or install it into your `/etc/bash_completion.d` directory
* Sourcing

 ```sh
 $ source aiscli_autocomplete
 ```

* Installing to `/etc/bash_autocomplete.d`

 ```sh
 $ sudo cp aiscli_autocomplete /etc/bash_completion.d/ais
 $ source /etc/bash_completion.d/ais
 ```
Doing `ais <tab><tab>` should return the list of available commands.

## Supported Resources

List of available CLI resources

* [Bucket](./resources/bucket.md)

* [Daemon/Cluster](./resources/daeclu.md)

* [Downloader](./resources/downloader.md)

* [Object](./resources/object.md)

## Enums

| Enum | Values | Description |
| --- | --- | --- |
| Provider | `local`, `cloud`, `''` | Locality of bucket. If empty, AIS automatically determines the locality. |