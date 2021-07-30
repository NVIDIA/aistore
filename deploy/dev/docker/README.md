---
layout: post
title: DOCKER
permalink: deploy/dev/docker
redirect_from:
 - deploy/dev/docker/README.md/
---

There are two different ways, or modes, to deploy AIS in Docker containers.
We refer to those ways as [quick-start mode](#quick-start-ais-cluster) and [development mode](#developer-mode).

[Quick-start mode](#quick-start-ais-cluster) allows for a quick containerized deployment of AIS with minimal setup.
[Development mode](#developer-mode) allows users to test and develop AIS in a more robust containerized environment.

## Quick-start AIS Cluster

Create a containerized deployment of AIStore within seconds.
The minimum requirements to get this working is to have Docker installed.
If you don't have Docker and Docker-Compose installed, please see [Getting started with Docker.](/docs/docker_main.md)

> For some tests, AWS config and credential files are needed

1. To quick-start AIStore, run:

    ```console
    $ ./deploy_docker -qs
    ```

    > The first build may take some time, but subsequent builds will be much faster.

2. Once it finishes building, you should be inside the AIS container. Run:

    ```console
    $ make deploy
    ```

Type `1` for all options to create a very basic AIStore cluster.

### Quick-start Example

If everything went smoothly

```console
$ CGO_ENABLED=0 BUCKET=test go test ./tests -v -count=1 -run=smoke
```

runs some smoke tests.

```console
$ CGO_ENABLED=0 BUCKET=test go test ./tests -v -count=1 -run=bucketname
```

Will return you a list of all local and remote buckets.

> CGO_ENABLED is set to false to enable cross-compiling

### Quick-start Configurations

#### AWS

Quick-start AIS also works with AWS by enabling users access to their S3 Buckets. To do so, users can specify the location of their AWS credentials by passing in the path. For example,

```console
$ ./deploy_docker.sh -qs="~/.aws/"
```

## Developer Mode

Use the `./deploy_docker.sh` script to deploy AIStore cluster(s) on a single machine. Each cluster can be deployed with one or three networks. The latter case is used to deploy a cluster with separated data and control planes. When deploying multiple clusters, only multiple networks are allowed.
Use the `./stop_docker.sh` script to stop the AIStore cluster(s) that were deployed.

## Requirements

> Install Docker and Docker-Compose prior to deploying a cluster. For setting up Docker services please read [Getting started with Docker.](/docs/docker_main.md)

[`$GOPATH`](https://golang.org/doc/code.html#GOPATH) environment variable must be defined before starting cluster deployment. Docker uses the `$GOPATH/src` directory as a bind mount for the container. The container at start builds new binaries from the current sources.

For the *i*th cluster, AIStore creates three networks: ais${i}\_public, ais${i}\_internal\_control, and ais${i}\_internal\_data. The latter two are used only if the cluster is deployed with multiple networks (`-m` argument must be passed to the deploy script). It is expected that only AIStore cluster *i* is attached to each these networks. In a multi-cluster configuration, proxy containers of one cluster are connected to the Docker public networks of other clusters to allow for multi-tiering and replication.  In multi-cluster configuration, target containers of one cluster are connected to the Docker public and replication networks of other clusters to allow for multi-tiering and replication.

## Deploying a Development Cluster

Run `./deploy_docker.sh` without arguments if you want to deploy a cluster in interactive mode.
The script will ask you for a number of configuration parameters and deploy AIS accordingly:

```console
$ ./deploy_docker.sh
```

Be sure that the AWS credentials and configuration files are located outside of the script directory.
The script copies AWS credentials and configuration from the provided location to `/tmp/docker_ais/aws.env` and passes this file to each container.

To deploy a cluster in 'silent' mode use the following options (if any of them are not set, then the script switches to interactive mode and asks for the missing configuration parameters):

* `-c=NUM` or `--cluster=NUM`               : where NUM is the number of clusters
* `-d=NUM` or `--directories=NUM`           : where NUM is the number of local cache directories
* `-f=LIST` or `--filesystems=LIST`         : where LIST is a comma separated list of filesystems
* `-h` or `--help`                          : show usage
* `-l` or `--last`                          : redeploy using the arguments from the last AIS Docker deployment
* `-m` or `--multi`                         : use multiple networks
* `-p=NUM` or `--proxy=NUM`                 : where NUM is the number of proxies
* `-s` or `--single`                        : use a single network
* `-t=NUM` or `--target=NUM`                : where NUM is the number of targets
* `-qs=AWS_DIR` or `--quickstart=AWS_DIR`   : deploys a quickstart version of AIS
* `-nocloud`                                : to deploy AIS without any backend provider
* `-grafana`                                : starts Graphite and Grafana (can be accessed at [localhost:3000](http://localhost:3000)) for displaying metrics
* `-nodiskio=BOOL`                          : run Dry-Run mode with disk IO is disabled (default = false)
* `-dryobjsize=SIZE`                        : size of an object when a source is a 'fake' one. 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. Default value is '8m'


Note:
* If the `-f` or `--filesystems` flag is used, the `-d` or `--directories` flag is disabled and vice-versa
* If the `-s` or `--single` and `-m` or `--multi` flag are used, then multiple networks will take precedence
* Be sure that the aws credentials and configuration files are located outside of the script directory. The script copies AWS credentials and configuration from the provided location to `/tmp/docker_ais/aws.env` and passes this file to each container.

Please see [main AIStore README](/docs/configuration.md) for more information about testing mode.

Example Usage:

```console
$ ./deploy_docker.sh -p=3 -t=4 -d=2 -c=1
```

The command deploys a single cluster with 3 proxies, 4 targets and 2 local cache directories using AWS in normal mode.

### Deploying Multiple Clusters

When deploying multi-cluster configurations, each cluster will have the same number of proxies and targets. Each container name will be of this format: ais${i}\_${target,proxy}\_${j}. Where *i* denotes the cluster number and *j* denotes the *j*th daemon of type target or proxy.

Example:

```console
$ ./deploy_docker.sh -p=3 -t=3 -d=2 -c=3
```

The command deploys three clusters, each with 3 proxies, 3 targets and 2 local directories in normal mode.

## Restarting a Cluster

Running the same command that you used to deploy AIS using Docker stops the running cluster and deploys a new AIS configuration from scratch (including multi-cluster configurations). It results in a fresh cluster(s) with all containers and networks rebuilt. Either run the previous deployment command (supply the same command arguments) or use the following command to redeploy using the arguments from the last AIS Docker deployment:

```console
$ ./deploy_docker.sh -l
```

NOTE: The deploy script saves configuration parameters to `/tmp/docker_ais/deploy.env` each time before deploying. If this file doesn't exist or gets deleted, the command won't work.

## Stopping a Cluster

To stop the cluster, run one of the following scripts:

```console
# To stop the last deployed Docker configuration
$ ./stop_docker.sh -l
```

NOTE: This command uses the saved configuration parameters in `/tmp/docker_ais/deploy.env` to determine how to stop AIS. If this file doesn't exist or gets deleted, the command won't work.

```console
# If a single network AIS configuration is currently deployed
$ ./stop_docker.sh -s
```

```console
# If a multi network AIS configuration is currently deployed
$ ./stop_docker.sh -m
```

```console
# To stop a multiple cluster configuration of AIS that is currently deployed, where NUM_CLUSTERS >= 1
$ ./stop_docker.sh -c=NUM_CLUSTERS
```

These commands stop all containers (even stopped and dead ones), and remove Docker networks used by AIS. Refer to the stop_docker.sh script for more details about its usage.

## Viewing the Local Filesystem of a Container

The Docker-Compose file is currently set to mount the `/tmp/ais/${i}/${hostname}` directory, where *i* is the cluster number and *hostname* is hostname of container, to `/tmp/ais` inside the container.
Thus, to see the `/tmp/ais` folder of container `ais${i}\_${target,proxy}\_${j}` obtain the hostname of given container and navigate to `/tmp/ais/${i}/${hostname}` directory on your local machine.

## Extra configuration

It is possible that default settings do not work in specific cases, e.g, default networks cannot be used by AIStore container (default is `172.50.0.0/24` subnet). To fix this you can tune up variables in [deployment script](/deploy/dev/docker/deploy_docker.sh).

Useful script variables:

| Variable | Default value | Description |
|---|---|---|
| PUB_NET | 172.50.0 | Public network (data plane for multiple networks case) |
| INT_CONTROL_NET | 172.51.0 | Internal network (control plane for multiple networks case) |
| INT_DATA_NET | 172.52.0 | Internal network (data plane for multiple networks case) |
| PORT | 51080 | HTTP port for public API |
| PORT_INTRA_CONTROL | 9080 | HTTP port for internal control plane API (for multiple networks case) |
| PORT_INTRA_DATA | 10080 | HTTP port for internal data plane API (for multiple networks case) |
| TEST_FSPATH_ROOT | `/tmp/ais/` | the base directory for directories used in testing mode(option `-l` is set). All testing directories are located inside TESTFSPATHROOT and have short names 0, 1, 2 etc. |

## Running tests in Docker environment

Tests are started in the same way as it is done for regular cluster:

```console
$ BUCKET=vlocal go test -v ./tests -count 1 -p 1 -timeout 1h
```

> The above command assumes that you're in the `aistore/ais` directory

Tests detect the Docker cluster and use primary URL "http://172.50.0.2:51080" (see `PUB_NET` variable). If `PUB_NET` or `PORT` variable is changed or original primary is stopped or deleted then tests require extra argument that points to an existing proxy, preferably the current primary one:

```console
$ BUCKET=vlocal go test -v ./tests -count 1 -p 1 -timeout 1h -url=http://172.51.0.7:51080
```

**NOTE:** Some tests require a minimum number of targets or proxies. Also, due to Docker permissions, you might have to run tests with `sudo` too.

## Running benchmark tests in the Docker environment

AIStore Docker clusters can also be deployed in Dry-Run mode. These modes can be activated either through the interactive interface or by passing in `-nodiskio`. See more about benchmark tests, see [AIS Loader](/bench/aisloader/README.md)

## Utility Scripts

### `logs.sh`

To quickly view the logs of all running containers, use the following command:

```console
$ ./logs.sh -d
```

To view the logs of a specific container, use the following command:

```console
$ ./logs.sh -c=container_name -t=a
```

Refer to the `logs.sh` script for more details about its usage.

### `get_ip_addresses.sh`

If you'd want to quickly get IP addresses of all running containers, execute the following:

```console
$ ./get_ip_addresses.sh
```

> The default port numbers will be `51080`, `9080`, and `10080` respectively for the **public**, **intra-control** and **intra-data** networks.

### Prune docker

To prune old docker containers, images and networks you can use:

```console
$ docker kill $(docker ps -q) # in case you want to kill ALL containers
$ docker system prune -f
```

### Accessing These Scripts From Anywhere

Add the following to the end of your `~/.profile`:

```bash
if [ -d "$GOPATH/src/github.com/NVIDIA/aistore/deploy/dev/docker" ] ; then
  PATH="$PATH:$GOPATH/src/github.com/NVIDIA/aistore/deploy/dev/docker"
fi
```

After that, execute the following to update your $PATH variable:

```console
$ source ~/.profile
```

Then, you can just execute the script name from any working directory. Example:

```console
$ container_logs.sh CONTAINER_NAME
```

## Limitations

Certain tests require the ability to modify files/directories or set extended attributes(xattrs) directly onto the filesystem of a container. These tests are currently skipped when using Docker because of Docker's write protection of container volumes.
