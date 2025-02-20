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
  -config string
        config filename: local file that stores the global cluster configuration
  -config_custom string
        "key1=value1,key2=value2" formatted string to override selected entries in config
  -daemon_id string
        user-specified node ID (advanced usage only!)
  -h    show usage and exit
  -local_config string
        config filename: local file that stores daemon's local configuration
  -loopback
        use loopback devices (local playground, target-only)
  -ntargets int
        number of storage targets expected to be joining at startup (optional, primary-only)
  -role string
        _role_ of this aisnode: 'proxy' OR 'target'
  -skip_startup
        whether primary, when starting up, should skip waiting for target joins (used only in tests)
  -standby
        when starting up, do not try to auto-join cluster - stand by and wait for admin request (target-only)
  -start_with_lost_mountpath
        force starting up with a lost or missing mountpath (target-only)
  -transient
        false: store customized (via '-config_custom') configuration
        true: keep '-config_custom' settings in memory only (non-persistent)
```

For usage and the most recently updated set of command-line options, run `aisnode` with empty command-line:

```console
$ $GOPATH/bin/aisnode
```
