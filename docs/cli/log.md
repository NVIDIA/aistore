---
layout: post
title: LOG
permalink: /docs/cli/log
redirect_from:
 - /cli/log.md/
 - /docs/cli/log.md/
---

# Table of Contents
- [Download log or all logs (including history)](#ais-log-get-command)
- [View current log](#ais-log-show-command)

# `ais log get` command

```console
$ ais log get --help
NAME:
   ais log get - download log (or all logs including history) from selected node or all nodes in the cluster, e.g.:
               - 'ais log get NODE_ID /tmp' - download the specified node's current log; save the result to the specified directory;
               - 'ais log get NODE_ID /tmp/out --refresh 10' - download the current log as /tmp/out
                  keep updating (ie., appending) the latter every 10s;
               - 'ais log get cluster /tmp' - download TAR.GZ archived logs from _all_ nodes in the cluster
                  ('cluster' implies '--all') and save the result to the specified destination;
               - 'ais log get NODE_ID --all' - download the node's TAR.GZ log archive
               - 'ais log get NODE_ID --all --severity e' - TAR.GZ archive of (only) logged errors and warnings

USAGE:
   ais log get [command options] NODE_ID [OUT_FILE|OUT_DIR|-]

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports, e.g.:
                      '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --severity value  log severity is either 'info' (default) or 'error', whereby error logs contain both errors and warnings, e.g.:
                     - 'ais show log NODE_ID'
                     - 'ais log show NODE_ID --severity i' - same as above
                     - 'ais show log NODE_ID --severity error' - errors and warnings only
                     - 'ais show log NODE_ID --severity w' - same as above
   --yes, -y         assume 'yes' to all questions
   --all             download all logs
   --help, -h        show help
```

# `ais log show` command

```console
$ ais log show --help
NAME:
   ais log show - for a given node: show its current log (use '--refresh' to update, '--help' for details)

USAGE:
   ais log show [command options] NODE_ID

OPTIONS:
   --refresh value    interval for continuous monitoring;
                      valid time units: ns, us (or µs), ms, s (default), m, h
   --count value      used together with '--refresh' to limit the number of generated reports, e.g.:
                       '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --severity value   log severity is either 'info' (default) or 'error', whereby error logs contain both errors and warnings, e.g.:
                      - 'ais show log NODE_ID'
                      - 'ais log show NODE_ID --severity i' - same as above
                      - 'ais show log NODE_ID --severity error' - errors and warnings only
                      - 'ais show log NODE_ID --severity w' - same as above
   --log-flush value  can be used in combination with '--refresh' to override configured 'log.flush_time'
   --help, -h         show help
```
