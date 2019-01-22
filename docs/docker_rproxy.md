    - [Quick trial start with Docker](#quick-trial-start-with-docker)
    - [Quick trial start with AIStore as an HTTP proxy](#quick-trial-start-with-aistore-as-an-http-proxy)

### Quick trial start with Docker

To get started quickly with a containerized, one-proxy, one-target deployment of AIStore, see [Getting started quickly with AIStore using Docker](docker/quick_start/README.md).

### Quick trial start with AIStore as an HTTP proxy

1. Set the field `rproxy` to `cloud` or `target` in  [the configuration](ais/setup/config.sh) prior to deployment.
2. Set the environment variable `http_proxy` (supported by most UNIX systems) to the primary proxy URL of your AIStore cluster.

```shell
$ export http_proxy=<PRIMARY-PROXY-URL>
```

When these two are set, AIStore will act as a reverse proxy for your outgoing HTTP requests.

>> Note that this should only be used for a quick trial of AIStore, and not for production.

