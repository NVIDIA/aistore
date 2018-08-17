## Developer docker scripts

Use these scripts to deploy a DFC cluster on a single machine. The cluster can be deployed with one or two networks. The latter case is used to deploy a cluster with separated data and control planes.

### Utility scripts

Every new deployment builds a new docker image. The new image replaces the old one. The old one becomes obsolete and gets name `<none>`. Script `del_old_images.sh` deletes all obsolete and unused docker images.

## Requirements

Install docker and docker-composer prior to deploying a cluster. How to setup docker services please read in [Getting started: Docker](docker/README.md)

GOPATH environment variable must be defined before starting cluster deployment. Docker attaches $GOPATH/src directory to a container, so the container at start builds new binaries from the current sources.

DFC creates two networks: tests_public and tests_internal. The latter one is used only if the cluster is deployed with multiple network switch. It is expected that only DFC cluster is attached to these networks.

## How to deploy a cluster

Run `./deploy_docker.sh` without arguments if you want to deploy a cluster in interactive mode. It results in deploying a cluster with single network, and GCP as cloud provider. If AWS is required the path to AWS credentials file must be provided in command line:

```
./deploy_docker.sh -a <path_to_credentials_file>
```

Be sure that the credentials file is located outside of the script directory. The script copies credentials from provided location to its working directory.

Passing `-a` enables AWS instead GCP even if AWS credentials file is invalid or empty.

To deploy a cluster in 'silent' mode use the following options (if any of them is not set, then the script switches to interactive mode and asks for details):

- p [int] - the number of proxies
- t [int] - the number of targets
- s [/path1,/pathext2]- the comma separated list of filesystems to use as a storage inside targets
- l [int] - the number of directories to use as a storage inside targets in testing mode
- m - if it is set the cluster uses multiple networks(one for data and one for control plane) instead of only one for everything

Note: `-s` and `-l` are mutually exclusive. If `-s` is defined, the option `-l` is automatically disabled. Please see [main DFC README](/README.md#configuration) for more information about testing mode.

Example:
```
./deploy_docker.sh -p 3 -t 3 -l 2
```
The command deploys a cluster with 3 proxies, 3 targets and 2 local directories in testing mode.

### Stop and restart a cluster

Running the same command that you used to deploy a cluster stops the running cluster and deploy a new one from scratch. It results in a fresh cluster with all containers and networks rebuilt.

To stop the cluster, run the same script with only one argument **stop**:
```
./deploy_docker.sh stop
```

This command stops all running containers, removes stopped and dead ones, and removes docker networks used by a cluster.

## Extra configuration

It is possible that default settings do not work in specific cases, e.g, default networks cannot be used by DFC container. To fix this you can tune up variables in [deployment script](docker/tests/deploy_docker.sh).

Useful script variables:

| Variable | Default value | Description |
|---|---|---|
| PUB_NET | 172.50.0 | Public network (data plane for multiple networks case) |
| INT_NET | 172.51.0 | Internal network (control plane for multiple networks case) |
| PORT | 8080 | HTTP port for public API |
| PORT_INTRA | 9080 | HTTP port for internal API (for multiple networks case) |
| TESTFSPATHROOT | /tmp/dfc/ | the base directory for directories used in testing mode(option `-l` is set). All testing directories are located inside TESTFSPATHROOT and have short names 0, 1, 2 etc. |

## Running tests in docker environment

Tests are started in the same way as it is done for regular cluster:

```
BUCKET=vlocal go test -v ./tests -count 1 -p 1 -timeout 1h
```

Tests detect docker cluster and use primary URL "http://172.50.0.2:8080" (see PUB_NET variable). If PUB_NET or PORT variable is changed or original primary is stopped or deleted then tests require extra argument that points to an existing proxy, preferably the current primary one:

```
BUCKET=vlocal go test -v ./tests -count 1 -p 1 -timeout 1h --args -url=http://172.51.0.7:8080
```

## Limitations

If you are going to move deployment scripts to another directory, do not forget to change network names where they are used. Docker composer generate network name with the pattern `<directory_name>+"_"+<network_name_from_yml>`. That results in `tests_public` and `tests_internal` for DFC network names. Deployment scripts do not use these names directly but test scripts can do.

