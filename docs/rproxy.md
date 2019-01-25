### AIStore as an HTTP proxy

1. Set the field `rproxy` to `cloud` or `target` in  [the configuration](ais/setup/config.sh) prior to deployment.
2. Set the environment variable `http_proxy` (supported by most UNIX systems) to the primary proxy URL of your AIStore cluster.

```shell
$ export http_proxy=<PRIMARY-PROXY-URL>
```

When these two are set, AIStore will act as a reverse proxy for your outgoing HTTP requests.
