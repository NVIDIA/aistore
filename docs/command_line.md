---
layout: post
title: COMMAND LINE
permalink: /docs/command-line
redirect_from:
 - /command_line.md/
 - /docs/command_line.md/
---

### Command-Line arguments

There is a single AIS node (`aisnode`) binary that functions either as AIS proxy (gateway) or AIS target, depending on the `-role` option - examples follow below:

```console
# Example deploying `aisnode` proxy
$ aisnode -config=/etc/ais/config.json -local_config=/etc/ais/local_config.json -role=proxy -ntargets=16

# Example deploying `aisnode` target (ie., storage server)
$ aisnode -config=/etc/ais/config.json -local_config=/etc/ais/local_config.json -role=target
```

The common executable, typically called `aisnode`, supports the following command-line arguments:

```console
  -alsologtostderr
        log to standard error as well as files
  -config string
        config filename: local file that stores the global cluster configuration
  -config_custom string
        "key1=value1,key2=value2" formatted string to override selected entries in config
  -daemon_id string
        user-specified node ID (advanced usage only!)
  -h    show usage and exit
  -local_config string
        config filename: local file that stores daemon's local configuration
  -log_backtrace_at value
        when logging hits line file:N, emit a stack trace
  -logtostderr
        log to standard error instead of files
  -ntargets int
        number of storage targets expected to be joining at startup (optional, primary-only)
  -override_backends
        configure remote backends at deployment time (potentially, override previously stored configuration)
  -role string
        _role_ of this aisnode: 'proxy' OR 'target'
  -skip_startup
        whether primary, when starting up, should skip waiting for target joins (used only in tests)
  -standby
        when starting up, do not try to join cluster - standby and wait for admin request (target-only)
  -stderrthreshold value
        logs at or above this threshold go to stderr
  -transient
        false: store customized (via config_custom) configuration
        true: runtime only (non-persistent)
  -v value
        log level for V logs
  -vmodule value
        comma-separated list of pattern=N settings for file-filtered logging

```

For usage and the most recently updated set of command-line options, run `aisnode` with empty command-line:

```console
$ $GOPATH/bin/aisnode
```
