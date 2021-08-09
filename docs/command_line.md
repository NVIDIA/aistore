---
layout: post
title: COMMAND LINE
permalink: /docs/command-line
redirect_from:
 - /command_line.md/
 - /docs/command_line.md/
---

### Command-Line arguments

AIS proxy and AIS target (executables) both support the following command-line arguments where those that are *mandatory* are marked with `***`:

```
  -alsologtostderr
        log to standard error as well as files
  -config string
        config filename: local file that stores the global cluster configuration
  -config_custom string
        "key1=value1,key2=value2" formatted string to override selected entries in config
  -daemon_id string
        unique ID to be assigned to the AIS daemon
  -h    show usage and exit
  -local_config string
        config filename: local file that stores daemon's local configuration
  -log_backtrace_at value
        when logging hits line file:N, emit a stack trace
  -logtostderr
        log to standard error instead of files
  -ntargets int
        number of storage targets to expect at startup (hint, proxy-only)
  -override_backends
        set remote backends at deployment time
  -role string
        role of this AIS daemon: proxy | target
  -skip_startup
        determines if primary proxy should skip waiting for target registrations when starting up
  -stderrthreshold value
        logs at or above this threshold go to stderr
  -transient
        false: apply command-line args to the configuration and save the latter to disk
        true: keep it transient (for this run only)
  -v value
        log level for V logs
  -vmodule value
        comma-separated list of pattern=N settings for file-filtered logging

   Usage:
        aisnode -role=<proxy|target> -config=</dir/config.json> -local_config=</dir/local-config.json> ...

```

> For the most recently updated set of command-line options, run `aisnode` with empty command-line:

```console
$ $GOPATH/bin/aisnode
```

Example:

```console
$ $GOPATH/bin/aisnode -config=/etc/ais.json -local_config=/etc/ais_local.json -role=target -daemon_id=aistarget1
```

The command starts a target daemon with ID `aistarget1` and the specified global and local configurations from `/etc/ais.json` and `/etc/ais_local.json`, respectively.
