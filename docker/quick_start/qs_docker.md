## Getting started quickly with AIStore using Docker

This guide will help you set up a containerized, one-proxy, one-target deployment of AIStore within seconds. If you don't have Docker and Docker-Compose installed, please see [Getting started with Docker.](../README.md)

1. `docker pull aistorage/ais-quick-start`
2. `docker run -di aistorage/ais-quick-start`
3. `./quick_start_ais.sh`

The first command will pull the latest image from the repository. To pull an image with a specific tag, check out [the available tags](https://hub.docker.com/r/aistorage/ais-quick-start/tags/) and do `docker pull aistorage/ais-quick-start:<TAG>` instead.

[`quick_start_ais.sh`](quick_start_ais.sh) will deploy AIStore with one proxy, one target, one local cache directory, and will be configured to use AWS as the cloud provider.

### Configuration

#### AWS

To set up your AWS configuration, use the [`aws configure`](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) command inside the container. Alternatively, mount your credentials and config file to the container in the `docker run` command. 

For example, teardown the original cluster with `./teardown.sh`. This stop the cluster and removes the container. 

Perform the same actions from above but replace 

```shell
$ docker run -di aistorage/ais-quick-start
```

with 

```shell
$ docker run -div ~/.aws/credentials:/root/.aws/credentials \
               -v ~/.aws/config:/root/.aws/config aistorage/ais-quick-start
```

and run

```shell
./quick_start_ais.sh
```

> This assumes that your AWS credential and config files are located in your `$HOME` directory and copies it to the `root` directory in the cluster.

#### Port

By default, the proxy will be deployed on port `8080` and the target on port `8081`.

To change the ports AIStore will be deployed on, set the `PORT` environment variable. For example:

```shell
$ PORT=8082 ./quick_start_dfc.sh
```
 
The proxy will be then deployed on port `$PORT` and the target on port `$PORT+1`.


### Example
If everything went smoothly, running 

```shell
$ BUCKET=test go test ../../ais/tests -v -count=1 -run=bucketname
```

Will return you a list of all local and AWS cloud buckets, where `BUCKET` is an environment variable. 

> For this specific test, the value of `BUCKET` can be anything. 

#### Docker Storage Options

To persist and safely share AIStore data in this container with the host machine and other containers, use a volume. See [Start a container with a volume](https://docs.docker.com/storage/volumes/#start-a-container-with-a-volume) for examples and more details.

For Docker on Linux, use a tmpfs mount for a non-persistent storage option. See [Use a tmpfs mount in a container](https://docs.docker.com/storage/tmpfs/#use-a-tmpfs-mount-in-a-container) for examples and more details.
