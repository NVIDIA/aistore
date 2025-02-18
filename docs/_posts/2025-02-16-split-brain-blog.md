---
layout: post
title:  "Split-brain is Inevitable"
date:   Feb 16, 2025
author: Alex Aizman
categories: high-availability split-brain aistore
---

Split-brain is inevitable. The way it approaches varies greatly but there are telltale signs that, in hindsight, you wish you'd taken more seriously.

> Next time, you certainly will.

But what? What exactly could've been done differently when keepalives started failing seemingly at random? When one node after another reports that the _primary_ is unreachable, initiating re-election.

In the moment, dealing with it is quite unsettling. Looking at a jumble of intertwined alerts that keep coming in waves, where seemingly sporadic attempts to elect a new _primary_ get voted out by fragmented majorities.

And that's going on for a while. Feels like a long while but actually it is not. Because split-brain is arriving. In fact, it's already here, and it _will_ reshape the cluster.

![split-brain is inevitable](/assets/split-brain.jpg)

When exactly will it happen? Well, the sequence entails apparently random node kills (triggered by failing Kubernetes probes), along with users departing and taking their respective workloads with them.

For a new-found stability to emerge, the users must step aside. Needless to say, users typically comply. Eventually, they all do.

And when the smoke clears we see a picture-frame containing two brand-new clusters.

Why only two, one may ask? Honestly, I don't know. There's no shame in admitting — I don't understand _why_ we never see the original cluster splinter into countless infinitesimally small pieces. After all the drama and confusion that unfolded before our eyes...

On the other hand, bifurcation is sort of good news if you look at it from the right perspective. Ultimately, what else could you do when your intra-cluster network starts failing left and right, randomly and unpredictably?

The best you can do is make the best of it — and sometimes that simply means that you go ahead and bifurcate. Plain and simple.

> As an aside, it takes time and dedication to develop one's inner ability to see good news and promise where there is, ostensibly, only despair and devastation. For example, and keeping with the topic at hand, consider the following written argument:

>> Yes, we have a split-brain situation. Yes, it is maybe-arguably-unfortunate. But (!) we remain available. After all, even when the nodes splinter, each island elected its own leader (its _primary_), gathered what remains of user content, and immediately resumed operation.

That's HA by definition. In a roundabout way, that's a saving grace.

## The Double-Edged Sword

HA systems are built to keep running despite failures — whether due to network partitions, hardware malfunctions, software bugs, DDoS attacks, human errors, or a mixed bag of all of the above.

The wisdom, therefore, in striving for high availability is widely accepted, incontrovertible, and ultimately self-evident.

Yet, some of the most entrenched skeptics among us may still attempt to claim that there's a dark downside, that those same mechanisms can make the system more fragile.

In fact, those same skeptics may further counter that the idea to keep systems operational may have consequences.
Such as split-brain, for instance.

After all, the road that leads to you-know-where is firmly paved with good intentions. As was aptly noted almost a millennium ago: [L'enfer est plein de bonnes volontés ou désirs.](#references)

## The Prescription

Conventional split-brain handling wisdom typically entails a variety of configurable heuristics along with associated prevention or isolation (impact-limiting) logic.

The approach relies on implementing failure detection logic that establishes boundary conditions for what constitutes a "normal" vs "abnormal" behavior.

> Beware: "failure detection" often rhymes with "false positives"

Now, for illustration purposes let's make it really simple:

- Losing two drives within an hour can be regarded unlikely but plausible
- Losing two nodes during the same interval raises (or, must raise) immediate red flags, even when each of those nodes was able to quickly come up and rejoin

Clearly, the corresponding bit can be written into the system, along with proper knobs and their respective defaults (that I won't enumerate due to severe space-and-scope limitations), as well as unit tests, integration tests, and user docs. Nine yards, as we always say in such circumstances. The whole nine yards.

> Why only nine, by the way?

Anyway, speaking of circumstances — some of those may be quite involved.

Take, for instance, network latency or spikes thereof. Could be intermittent.
Could also be misdiagnosed. Misattributed. Misinterpreted. Totally innocuous after all. And could also indicate an impending partition.

Anything's possible. And if it is possible it will surely happen.

But still, suppose we - eventually — heuristicize our ways to detect the unexpected.
Suppose, we nail it. Suppose, we already have. What do we do about it?

Well, again, the aforementioned conventional wisdom comes to the rescue, offering a menu of decisive militaristic actions:

1. Brickify — i.e., rapidly convert the entire cluster (and each AIS target in particular) into a read-only brick.
2. Fence off — the Linux-HA (circa 2001) idea, better known to the broader public as [shooting in the head](https://en.wikipedia.org/wiki/Fencing_(computing)).
3. Arbitrate — namely, deploy AIS proxies outside the hosting Kubernetes cluster, granting them additional weight to decide primary elections and whether to initiate a global rebalance.

Those are some of the recommended actions. The only problem with all of them? Complexity.

> Or, more precisely, complexity greatly amplified by limited time.

### Evolve!

The old adage goes, "The complexity of a large system tends to grow exponentially."

Take, for instance, AIStore. Over the past seven years [and counting], it has grown in every measurable dimension - adding modules, components, layers, extensions, APIs, backends; you name it. AIStore is a microcosm of how complexity emerges in a gradually evolving system.

And having emerged, it requires respect. Demands it, in fact, when _not_ making choices consistent with incremental and balanced evolution.

High Availability (HA) is at the core of it. HA isn’t a feature you can "slide in" using semi-automated code-writing mode that comes with long practice.

No, HA is never _incremental_.

And so we face a binary choice:

- **Option 1:** Support conventional, time-honored, heuristics-driven split-brain detection and prevention.
- **Option 2:** Accept the risk and focus on handling split-brain when it occurs — in the most efficient and user-friendly way possible.

The first option is highly appealing, even seductive. The promise of automated failover and five-nines (or N-nines, for your preferred integer N > 5) availability is compelling — it conforms to the well-established expectation. It even offers peace of mind, if only temporarily.

> But again (the inner skeptic wouldn’t shut up!) — each layer of distributed prevention logic creates new failure modes, and every heuristic carries its own risks of false positives. **And ultimately, all of it adds up**.
'What if?' — the eternal question. What if, by instrumenting yet another piece of elaborate logic, we inadvertently make the system fragile? What if??

## What Have We Done

And so, in [v3.26](https://github.com/NVIDIA/aistore/releases/tag/v1.3.26), we chose option 2: ex post facto cluster reunification.

When writing it, the first challenge was to convince nodes in one cluster to _accept_ the nodes from another.
The latter arrive with a different cluster UUID and different, sometimes greater, versions of cluster-level metadata that includes:

* cluster map
* global configuration, cluster-level defaults
* buckets, and their respective configurations and properties

In normal circumstances, any version and/or UUID mismatch would trigger an immediate rejection.

But here we are — recovering from split-brain, merging clusters. Far from any normal.

Long story short, the actual implementation comprises about 10 steps with 3 rollbacks, contained primarily in a single source file — making it easy to revisit and maintain if needed.

Further, to reliably unify splintered clusters (while maintaining data consistency), we needed a way to split them in the first place.

And that was the second task: developing fast and reliable cluster-splitting tooling. The way it's supposed to work (and does work) is easy to demonstrate.

Here's a look at a fresh dev cluster, ready for testing:

```console
$ ais show cluster

PROXY            MEM USED(%)     MEM AVAIL    LOAD AVERAGE    UPTIME  STATUS
p[CkipqfqdD]     0.16%           23.96GiB     [0.4 0.5 0.5]   -       online
p[JbJpCIVwB]     0.15%           23.96GiB     [0.4 0.5 0.5]   -       online
p[KzjpEKshn]     0.15%           23.96GiB     [0.4 0.5 0.5]   -       online
p[YHnpZGMEh][P]  0.18%           23.96GiB     [0.4 0.5 0.5]   -       online
p[gOCproqcw]     0.16%           23.96GiB     [0.4 0.5 0.5]   -       online
p[xjTpNwRln]     0.16%           23.96GiB     [0.4 0.5 0.5]   -       online

TARGET           MEM USED(%)     MEM AVAIL    CAP USED(%)     CAP AVAIL       LOAD AVERAGE    REBALANCE   UPTIME  STATUS
t[FRRtFguSh]     0.16%           23.96GiB     17%             361.151GiB      [0.4 0.5 0.5]   -           -       online
t[IzPtBtMQt]     0.17%           23.96GiB     17%             361.151GiB      [0.4 0.5 0.5]   -           -       online
t[JwbtJAsuH]     0.16%           23.96GiB     17%             361.151GiB      [0.4 0.5 0.5]   -           -       online
t[VhDtmHGqR]     0.17%           23.96GiB     17%             361.151GiB      [0.4 0.5 0.5]   -           -       online
t[lLItLeRio]     0.17%           23.96GiB     17%             361.151GiB      [0.4 0.5 0.5]   -           -       online
t[mAXtDAuhj]     0.17%           23.96GiB     17%             361.151GiB      [0.4 0.5 0.5]   -           -       online

Summary:
   Proxies:             6 (all electable)
   Targets:             6
   Cluster Map:         version 16, UUID DW0rviO0T, primary p[YHnp8080]
   Software:            3.27.11a6e3419 (build: 2025-02-15T11:24:12-0500)
   Deployment:          dev
   Status:              12 online
   Rebalance:           n/a
   Version:             3.27.11a6e3419
   Build:               2025-02-15T11:24:12-0500
```

And here's the same set of nodes cleanly divided into two separate islands (after cluster-splitting tool gets applied):

```console
PROXY            MEM USED(%)     MEM AVAIL    LOAD AVERAGE    UPTIME  STATUS
p[KzjpbMDzp]     0.15%           23.76GiB     [0.5 0.5 0.5]   1m40s   online
p[YHnpanVFj]     0.19%           23.76GiB     [0.5 0.5 0.5]   1m40s   online
p[gOCpOerlc][P]  0.16%           23.76GiB     [0.5 0.5 0.5]   1m40s   online

TARGET           MEM USED(%)     MEM AVAIL    CAP USED(%)     CAP AVAIL       LOAD AVERAGE    REBALANCE   UPTIME  STATUS
t[FRRtPOCIo]     0.17%           23.76GiB     17%             361.150GiB      [0.5 0.5 0.5]   -           1m40s   online
t[VhDtgtYKv]     0.17%           23.76GiB     17%             361.150GiB      [0.5 0.5 0.5]   -           1m40s   online
t[lLItiislP]     0.17%           23.76GiB     17%             361.150GiB      [0.5 0.5 0.5]   -           1m40s   online

Summary:
   Proxies:             3 (all electable)
   Targets:             3
...
```

and

```console
PROXY            MEM USED(%)     MEM AVAIL    LOAD AVERAGE    UPTIME  STATUS
p[CkipCKjBV]     0.16%           23.91GiB     [0.4 0.5 0.5]   4m10s   online
p[JbJpGtObG][P]  0.17%           23.91GiB     [0.4 0.5 0.5]   4m10s   online
p[xjTpiyTkM]     0.16%           23.91GiB     [0.4 0.5 0.5]   4m10s   online

TARGET           MEM USED(%)     MEM AVAIL    CAP USED(%)     CAP AVAIL       LOAD AVERAGE    REBALANCE   UPTIME  STATUS
t[IzPtcbLpi]     0.17%           23.91GiB     17%             361.149GiB      [0.4 0.5 0.5]   -           4m10s   online
t[JwbtULxfI]     0.17%           23.91GiB     17%             361.149GiB      [0.4 0.5 0.5]   -           4m10s   online
t[mAXtzxIcX]     0.17%           23.91GiB     17%             361.149GiB      [0.4 0.5 0.5]   -           4m10s   online

Summary:
   Proxies:             3 (all electable)
   Targets:             3
...
```

Finally, what about usage? Well, usage is easy:

1. Run the `ais cluster set-primary --force` command (but only after carefully reviewing the user manual and accepting all responsibilities).
2. Observe as the splintered clusters merge back into one.
3. Optionally, consider `ais start rebalance` and/or `ais prefetch --latest` - the latter assuming remote backend(s).

## Postscriptum

As for the root cause behind the avalanche of heartbeats and, eventually, split-brain? Well, yes, we experienced that for a while in one of our production setups. It was a simple `net.ipv4.tcp_mem` misconfiguration.

## References

[1] Bernard of Clairvaux, 12th century: "Hell is full of good wishes or desires."
