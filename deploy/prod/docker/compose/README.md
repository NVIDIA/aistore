# Minimal Compose Deployment

Two-container AIS cluster (one proxy, one target) via Docker Compose or Podman Compose.

## Prerequisites

- Docker or Podman, with Compose
- [AIS CLI](/docs/cli.md) (`make cli` from repo root)

## Quick Start

Pull the pre-built image from DockerHub and start:

```console
$ make up
$ export AIS_ENDPOINT="http://localhost:51080"
$ ais show cluster
```

## Building from Source

Build locally if you need to test source changes:

```console
$ make up-build
```

## Cloud Backends

### AWS

Using environment variables:

```console
$ AWS_ACCESS_KEY_ID=<key> \
  AWS_SECRET_ACCESS_KEY=<secret> \
  make up
```

Or using an AWS config directory (containing `config` and/or `credentials` files):

```console
$ AWS_CREDENTIALS_PATH=~/.aws \
  make up
```

### GCP

GCP requires mounting a service account JSON file:

```console
$ GCP_CREDENTIALS_PATH=/path/to/gcp-creds.json \
  make up
```

`GCP_CREDENTIALS_PATH` is the path on the host. It is mounted read-only into the container at `/credentials/gcp.json`.

### Multiple Backends

```console
$ AWS_CREDENTIALS_PATH=~/.aws \
  GCP_CREDENTIALS_PATH=/path/to/gcp-creds.json \
  make up
```

## Makefile Targets

| Target          | Description                               |
|-----------------|-------------------------------------------|
| `make up`       | Start the cluster (pulls image if needed) |
| `make up-build` | Build from source, then start             |
| `make build`    | Build the image from source only          |
| `make down`     | Stop the cluster                          |
| `make clean`    | Stop and remove data volumes              |
| `make logs`     | Tail container logs                       |
| `make status`   | Show container status                     |

The Makefile auto-detects `podman` or `docker`. Override with `COMPOSE="docker compose"`.

## Environment Variables

All environment variables set in the shell will pass through make targets to the [docker-compose.yml](./docker-compose.yml)

| Variable                | Default                       | Description                                                                                                                                                                                          |
|-------------------------|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AIS_BACKEND_PROVIDERS` | `"aws gcp"`                   | Space-separated list: `aws`, `gcp`, `azure`, `oci`                                                                                                                                                   |
| `AWS_ACCESS_KEY_ID`     |                               | AWS access key                                                                                                                                                                                       |
| `AWS_SECRET_ACCESS_KEY` |                               | AWS secret key                                                                                                                                                                                       |
| `AWS_REGION`            |                               | AWS region                                                                                                                                                                                           |
| `AWS_PROFILE`           |                               | Named AWS profile from config/credentials files                                                                                                                                                      |
| `S3_ENDPOINT`           |                               | Custom S3 endpoint for non-AWS providers                                                                                                                                                             |
| `AWS_CREDENTIALS_PATH`  |                               | Directory for S3 config/credential files on the host (mounted read-only to /root/.aws internally)                                                                                                    |
| `GCP_CREDENTIALS_PATH`  |                               | Host path to GCP service account JSON                                                                                                                                                                |
| `AIS_PORT`              | `51080`                       | Host port for the proxy                                                                                                                                                                              |
| `AIS_TARGET_PORT`       | `51081`                       | Host port for the target                                                                                                                                                                             |
| `AIS_FS_LABEL`          | `container-local`             | Filesystem label for mountpaths. Must be set to any value to indicate a volume without direct disk resolution. See [/docs/configuration.md](/docs/configuration.md#managing-mountpaths) for context. |
| `IMAGE_REPO`            | `aistorage/cluster-minimal`   | Image repository (Makefile only)                                                                                                                                                                     |
| `IMAGE_TAG`             | `latest`                      | Image tag (Makefile only)                                                                                                                                                                            |

## Shutting Down

```console
$ ais cluster shutdown
$ make down
```

To also remove the target's data volume:

```console
$ make clean
```

## Limitations

- **Single target only.** Scaling to multiple targets requires additional compose services with distinct ports, hostnames, and volumes.
- **Local access only.** The proxy and target advertise `127.0.0.1`. Remote clients cannot connect without modifying `HOSTNAME_LIST`.
- **Single disk per target.** Additional disks require editing the compose file to add volumes and updating `AIS_FS_PATHS`.
- **No HTTPS.** TLS is not configured.
- **No authentication.** AuthN is disabled.
