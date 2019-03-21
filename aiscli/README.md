# AISCLI
> Under development

AISCLI is a command-line interface that allows users to interact with the AIS cluster. It currently supports `list`, `smap`, `config`, `stats` and `status` commands.

## How To Use
1. To get started using the AISCLI tool, run `go install`. 

2. Export the `AIS_URL` environment variable (eg. `http://<YOUR_CLUSTER_IP>:<PORT>`) to configure the CLI tool to point to the AIS cluster.
 ```sh
 $ export AIS_URL=http://localhost:8080
 $ aiscli --help
 ```
 Should return the list of commands for the CLI
