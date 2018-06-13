## Getting started quickly with DFC using Docker

This guide will help you set up a containerized, one-proxy, one-target deployment of DFC within seconds.

1. `docker pull aistorage/dfc-quick-start`
2. `docker run -di aistorage/dfc-quick-start`
3. `./quick_start_dfc.sh`

The first command will pull the latest image from the repository. To pull an image with a specific tag, check out [the available tags](https://hub.docker.com/r/aistorage/dfc-quick-start/tags/) and do `docker pull aistorage/dfc-quick-start:<TAG>` instead.

[`quick_start_dfc.sh`](quick_start_dfc.sh) will deploy DFC with one proxy, one target, one local cache directory, and will be configured to use AWS as the cloud provider.

### Configuration

#### AWS

To set up your AWS configuration, use the [`aws configure`](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) command inside the container.

Alternatively, mount your credentials file to the container in the `docker run` command, for example:

`docker run -div ~/.aws/credentials:/root/.aws/credentials liangdrew/dfc`

#### Port

By default, the proxy will be deployed on port 8080 and the target on port 8081.

To change the ports DFC will be deployed on, set the `PORT` environment variable.
For example: `$ PORT=8082 ./quick_start_dfc.sh`
 
The proxy will be then deployed on port `$PORT` and the target on port `$PORT+1`.

#### Docker Storage Options

To persist and safely share DFC data in this container with the host machine and other containers, use a volume. See [Start a container with a volume](https://docs.docker.com/storage/volumes/#start-a-container-with-a-volume) for examples and more details.

For Docker on Linux, use a tmpfs mount for a non-persistent storage option. See [Use a tmpfs mount in a container](https://docs.docker.com/storage/tmpfs/#use-a-tmpfs-mount-in-a-container) for examples and more details.
