## Introduction

This readme covers a set of topics that can often be found under alternative
subtitles: "graceful termination", "node restart", "joining and leaving cluster", and similar.

Speaking of termination, one of the supported ways would be using `shutdown` command:

```console
$ ais cluster add-remove-nodes shutdown --help
NAME:
   ais cluster add-remove-nodes shutdown - shutdown a node, gracefully or immediately;
              note: upon shutdown the node won't be decommissioned - it'll remain in the cluster map
              and can be manually restarted to rejoin the cluster at any later time;
              see also: 'ais advanced remove-from-smap'

USAGE:
   ais cluster add-remove-nodes shutdown [command options] NODE_ID

OPTIONS:
   --no-rebalance  do _not_ run global rebalance after putting node in maintenance (advanced usage only!)
   --rm-user-data  remove all user data when decommissioning node from the cluster
   --yes, -y       assume 'yes' for all questions
```

For instance, given a 3-node single-gateway cluster, we can shutdown one of the nodes:

```console
$ ais cluster add-remove-nodes shutdown <TAB-TAB>
p[MWIp8080]   t[ikht8083]   t[noXt8082]   t[VmQt8081]

$ ais cluster add-remove-nodes shutdown t[ikht8083] -y

Started rebalance "g47" (to monitor, run 'ais show rebalance').
t[ikht8083] is shutting down, please wait for cluster rebalancing to finish

Note: the node t[ikht8083] is _not_ decommissioned - it remains in the cluster map and can be manually
restarted at any later time (and subsequently activated via 'stop-maintenance' operation).
```

## Rebalance

CLI help texts (above) may provide initial intuition and usage insights but, in particular, notice the word (and the term) "rebalance".

As far as aistore is concerned, "rebalance" always means only one thing: user data migrating from some nodes in the cluster to some other nodes (in the same cluster), and vice versa. And all this migration (call it "rebalancing") is there to satisfy a single purpose and a single (location-governing) rule simply stating that user data must be _properly_ located.

> Whereby the _proper_ location is defined by the current cluster map and - locally on each target node - by the configured target's mountpaths.

> Conceptually, aistore rebalance (aka "global rebalance") is similar to what's called "RAID rebuild". Of course, the underlying mechanics is very different.


For instance, after executing `shutdown t[ikht8083]` above, we'll see the following:

```console
$ ais show cluster
...
...
t[ikht8083][x]   -  -   -   -  maintenance
```

At first, the word `maintenance` will show up in red indicating the simple fact that rebalance is currently running and user data is speedily migrating from the node that is about to shut down.

But eventually, when all set and done, `ais show cluster` will report that rebalance ("g47" in the example) has finished and the node `ikht8083` terminated. Simultaneously, `maintenance` in the `show cluster` output will become non-red.

> Notice the sequence: first, global rebalance runs its full way, and only after that the node in question terminates.

> Note again: after **successful and complete rebalance**. Never before.

> Red color, hopefully, provides a visual cue indicating something like: "please don't disconnect, do not power off".
