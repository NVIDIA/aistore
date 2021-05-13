## Monitoring AIStore with Prometheus

AIStore tracks a fairly long and growing list of [performance metrics](metrics.md). To view those metrics, use logs, [CLI](/cmd/cli/README.md), any [StatsD](https://github.com/etsy/statsd) compliant backend (e.g., Graphite/Grafana), **and/or** [Prometheus](https://prometheus.io/) - via its own included [statsd_exporter](https://github.com/prometheus/statsd_exporter) extension that on-the-fly translates StatsD formatted metrics into Prometheus.

> For details on collected metrics, conventions, extended examples and, separately, metrics that AIS benchmarking tool (`aisloader`) generates, please refer to this [readme](metrics.md).

This text explain how to make use of the "Prometheus" option, and what needs to be done.

First, the picture:

<img src="images/statsd-exporter.png" alt="AIStore monitoring with Prometheus" width="640">

The diagram depicts AIS cluster that runs an arbitrary number of nodes with each node periodically sending its StatsD metrics to a configured UDP address of any compliant StatsD server. In fact, [statsd_exporter](https://github.com/prometheus/statsd_exporter) is one such compliant StatsD server that happens to be available out of the box.

To deploy [statsd_exporter](https://github.com/prometheus/statsd_exporter):

* you could either use [prebuilt container image](https://quay.io/repository/prometheus/statsd-exporter);
* or, `git clone` or `go get` the exporter's own repository at https://github.com/prometheus/statsd_exporter and then run it as shown above. Just take a note of the default StatsD port: **8125**.

To test a combination of AIStore and [statsd_exporter](https://github.com/prometheus/statsd_exporter) without Prometheus, run the exporter with debug:

```console
$ statsd_exporter --statsd.listen-udp localhost:8125 --log.level debug
```

The resulting (debug) output will look something like:

```console
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:321 msg="Starting StatsD -> Prometheus Exporter" version="(version=, branch=, revision=)"
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:322 msg="Build context" context="(go=go1.16.3, user=, date=)"
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:361 msg="Accepting StatsD Traffic" udp=localhost:8125 tcp=:9125 unixgram=
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:362 msg="Accepting Prometheus Requests" addr=:9102
level=debug ts=2021-05-13T15:30:27.811Z caller=listener.go:73 msg="Incoming line" proto=udp line=aistarget.pakftUgh.kalive.latency:1|ms
level=debug ts=2021-05-13T15:30:29.891Z caller=listener.go:73 msg="Incoming line" proto=udp line=aisproxy.qYyhpllR.pst.count:77|c
level=debug ts=2021-05-13T15:30:37.811Z caller=listener.go:73 msg="Incoming line" proto=udp line=aistarget.pakftUgh.kalive.latency:1|ms
level=debug ts=2021-05-13T15:30:39.892Z caller=listener.go:73 msg="Incoming line" proto=udp line=aisproxy.qYyhpllR.pst.count:78|c
level=debug ts=2021-05-13T15:30:47.811Z caller=listener.go:73 msg="Incoming line" proto=udp line=aistarget.pakftUgh.kalive.latency:1|ms
level=debug ts=2021-05-13T15:30:49.892Z caller=listener.go:73 msg="Incoming line" proto=udp line=aisproxy.qYyhpllR.pst.count:79|c
...
```

Finally, point any available Prometheus instance to poll the listening port - **9102** by default - of the exporter.

Note that the two listening ports mentioned - StatsD port **8125** and Prometheus port **9102** - are both configurable via the exporter's command line. To see all supported options, run:

```console
$ statsd_exporter --help
```
