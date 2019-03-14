# AISCLI
> Under development

AISCLI is a command-line interface that allows users to interact with the AIS cluster. It currently supports `list`, `smap`, `config`, `stats` and `status` commands.

## How To Use
1. To get started using the AISCLI tool, run `go install`. 

2. Set the environment variable, `AIS_URL`, to point to the AIS cluster.
 ```sh
 $ export AIS_URL=http://127.0.0.1:8080
 $ aiscli --help
 ```
    
3. To enable bash auto-completion
 ```sh
 $ PROG=aiscli source ../vendor/github.com/urfave/cli/autocomplete/bash_autocomplete
 ```
