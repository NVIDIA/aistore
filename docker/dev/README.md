## Developer Docker Scripts

Use the `./deploy_docker.sh` script to deploy DFC cluster(s) on a single machine. Each cluster can be deployed with one or three networks. The latter case is used to deploy a cluster with separated data and control planes. When deploying multiple clusters, only multiple networks are allowed.
Use the `./stop_docker.sh` script to stop the DFC cluster(s) that were deployed.

## Requirements

Install docker and docker-composer prior to deploying a cluster. How to setup docker services please read in [Getting started: Docker](docker/README.md)

GOPATH environment variable must be defined before starting cluster deployment. Docker attaches $GOPATH/src directory to a container, so the container at start builds new binaries from the current sources.

For the *i*th cluster, DFC creates three networks: dfc${i}\_public, dfc${i}\_internal\_control, and dfc${i}\_internal\_data. The latter two are used only if the cluster is deployed with multiple networks (-m argument must be passed to the deploy script). It is expected that only DFC cluster *i* is attached to each these networks. In a multi-cluster configuration, proxy containers of one cluster are connected to the docker public networks of other clusters to allow for multi-tiering and replication.  In multi-cluster configuration, target containers of one cluster are connected to the docker public and replication networks of other clusters to allow for multi-tiering and replication.

## Deploying a Cluster

Run `./deploy_docker.sh` without arguments if you want to deploy a cluster in interactive mode. The script will ask you for a number of configuration parameters and deploy dfc accordingly:

```
./deploy_docker.sh
```

Be sure that the aws credentials and configuration files are located outside of the script directory. The script copies AWS credentials and configuration from the provided location to `/tmp/docker_dfc/aws.env` and passes this file to each container.

To deploy a cluster in 'silent' mode use the following options (if any of them are not set, then the script switches to interactive mode and asks for the missing configuration parameters):

* -a=AWS_DIR or --aws=AWS_DIR   : to use AWS, where AWS_DIR is the location of AWS configuration and credential files
* -c=NUM or --cluster=NUM       : where NUM is the number of clusters
* -d=NUM or --directories=NUM   : where NUM is the number of local cache directories
* -f=LIST or --filesystems=LIST : where LIST is a comma seperated list of filesystems
* -g or --gcp                   : to use GCP
* -h or --help                  : show usage
* -l or --last                  : redeploy using the arguments from the last dfc docker deployment
* -m or --multi                 : use multiple networks
* -p=NUM or --proxy=NUM         : where NUM is the number of proxies
* -s or --single                : use a single network
* -t=NUM or --target=NUM        : where NUM is the number of targets

Note:
* If the -f or --filesystems flag is used, the -d or --directories flag is disabled and vice-versa
* If the -a or --aws flag is used, the -g or --gcp flag is disabled and vice-versa
* If the -s or --single and -m  or --multi flag are used, then multiple networks will take precedence
* Be sure that the aws credentials and configuration files are located outside of the script directory. The script copies AWS credentials and configuration from the provided location to `/tmp/docker_dfc/aws.env` and passes this file to each container.

Please see [main DFC README](/README.md#configuration) for more information about testing mode.

Example Usage:
```
./deploy_docker.sh -p=3 -t=4 -d=2 -c=1 -a=~/.aws/
```
* The command deploys a single cluster with 3 proxies, 4 targets and 2 local cache directories using aws in testing mode.

### Deploying Multiple Clusters

When deploying multi-cluster configurations, each cluster will have the same number of proxies and targets. Each container name will be of this format: dfc${i}\_${target,proxy}\_${j}. Where *i* denotes the cluster number and *j* denotes the *j*th daemon of type target or proxy.

Example:
```
./deploy_docker.sh -c=3 -p=3 -t=3 -d=2
```
* The command deploys three clusters, each with 3 proxies, 3 targets and 2 local directories in testing mode.

## Restarting a Cluster

Running the same command that you used to deploy dfc using docker stops the running cluster and deploys a new dfc configuration from scratch (including multi-cluster configurations). It results in a fresh cluster(s) with all containers and networks rebuilt. Either run the previous deployment command (supply the same command arguments) or use the following command to redeploy using the arguments from the last dfc docker deployment:
```
./deploy_docker.sh -l
```
* Note: The deploy script saves configuration parameters to `/tmp/docker_dfc/deploy.env` each time before deploying. If this file doesn't exist or gets deleted, the command won't work.

## Stopping a Cluster
To stop the cluster, run one of the following scripts:
```
# To stop the last deployed docker configuration
./stop_docker.sh -l
```
* Note: This command uses the saved configuration parameters in `/tmp/docker_dfc/deploy.env` to determine how to stop dfc. If this file doesn't exist or gets deleted, the command won't work.
```
# If a single network dfc configuration is currently deployed
./stop_docker.sh -s
```
```
# If a multi network dfc configuration is currently deployed
./stop_docker.sh -m
```
```
# To stop a multiple cluster configuration of dfc that is currently deployed, where NUM_CLUSTERS >= 1
./stop_docker.sh -c=NUM_CLUSTERS
```
These commands stop all containers (even stopped and dead ones), and remove docker networks used by dfc. Refer to the stop_docker.sh script for more details about its usage.

After the cluster has been stopped, delete the `/tmp/dfc/` directory on your local machine. The following command does this (note Docker protects the contents of this directory):
```
sudo rm -rf /tmp/dfc
```

## Viewing the Local Filesystem of a Container
The docker compose file is currently set to mount the /tmp/dfc/c${i}\_${target,proxy}\_${j} directory, where *i* is the cluster number and *j* is the daemon number, to /tmp/dfc inside the container.
Thus, to see the /tmp/dfc folder of container dfc${i}\_${target,proxy}\_${j}, navigate to /tmp/dfc/c${i}\_{target,proxy}\_${j} directory on your local machine.

## Extra configuration

It is possible that default settings do not work in specific cases, e.g, default networks cannot be used by DFC container. To fix this you can tune up variables in [deployment script](docker/dev/deploy_docker.sh).

Useful script variables:

| Variable | Default value | Description |
|---|---|---|
| PUB_NET | 172.50.0 | Public network (data plane for multiple networks case) |
| INT_CONTROL_NET | 172.51.0 | Internal network (control plane for multiple networks case) |
| INT_DATA_NET | 172.52.0 | Internal network (data plane for multiple networks case) |
| PORT | 8080 | HTTP port for public API |
| PORT_INTRA_CONTROL | 9080 | HTTP port for internal control plane API (for multiple networks case) |
| PORT_INTRA_DATA | 10080 | HTTP port for internal data plane API (for multiple networks case) |
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

## Utility Scripts
### `logs.sh`
To quickly view the logs of all running containers, use the following command:
```
./logs.sh -d
```
To view the logs of a specific container, use the following command:
```
./logs.sh -c=container_name -t=a
```
Refer to the `logs.sh` script for more details about its usage. 

### `del_old_images.sh`
Every new deployment builds a new docker image. The new image replaces the old one. The old one becomes obsolete and gets name `<none>`. Use the following script to delete all obsolete and unused docker images:
```
./del_old_images.sh
```

### `get_ip_addresses.sh`
If you want to quickly get the ip addresses of each container of all clusters, use the following script:
```
./get_ip_addresses.sh 
```
* Note: The port numbers for the ip addresses for each container will be 8080, 9080, 10080 by default for the public, intra control and intra data networks respectively.

### `container_shell.sh` 
To open an interactive shell for a daemon with container name CONTAINER_NAME, use the following script: 
```
./container_logs.sh CONTAINER_NAME
```
* Note: The command currently defaults to open the /tmp/dfc/log working directory. To view the list of running containers and obtain a container name, use the command: `docker ps`

### Accessing These Scripts From Anywhere
Add the following to the end of your `~/.profile`:
```
if [ -d "$GOPATH/src/github.com/NVIDIA/dfcpub/docker/dev" ] ; then
  PATH="$PATH:$GOPATH/src/github.com/NVIDIA/dfcpub/docker/dev"
fi
```
After that, execute the following to update your $PATH variable:
```
$source ~/.profile
```
Then, you can just execute the script name from any working directory. Example:
```
$container_logs.sh CONTAINER_NAME
```


## Limitations

Certain tests require the ability to modify files/directories or set Xattributes directly onto the filesystem of a container. These tests are currently skipped when using docker because of docker's write protection of container volumes.

