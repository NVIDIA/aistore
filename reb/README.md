# Global Rebalance - Execution Flow (sketch)

Global rebalance uses a single transport name (`trname = "reb"`) for cross-target object
streaming. Because the trname is fixed, at most one DM may be registered against
the transport at any moment. This document describes how that constraint is
maintained across rebalance generations.

> Cleanup mode is a separate rebalance generation mode; it reuses the `*Reb`
lifecycle but does not open data streams. See [Cleanup mode](#cleanup-mode).
Unless explicitly stated otherwise, the lifecycle and invariants below describe
regular, data-moving rebalance generations that use DM/transport streaming.

## Lifecycle

The `*Reb` service is constructed once at target startup and reused across all
rebalance generations.

The DM, however, is **per-streaming generation**: a new DM is
constructed at the start of each `Run()` that needs streams, and torn down before
`Run()` returns.

> When rebalance runs in cleanup mode it certainly does not open streams.


```
New()
    initialize Reb service state only; no DM

Run()
    preempt previous rebalance, if any
    compute haveStreams
    initRenew()

_renew()                            // under reb.mu -------------
    if haveStreams:
        NewDM
        RegRecv      on that DM
        Open         that same DM

endStreams()                        // under reb.mu (called from fini)
    Close        that same DM
    UnregRecv    that same DM
    reb.dm = nil

fini()
    endStreams (above) before xreb.Finish(),
    so EndTime is the safe signal for the next generation
```

## Invariants

1. **Single ownership.** `reb.dm != nil` if and only if this Run is past a
   successful `_renew` and has not yet reached `endStreams`. No other code path
   sets or clears `reb.dm`.

2. **Atomic generation start.** `NewDM + RegRecv + Open` runs under `reb.mu`
   inside `_renew`. No other generation can observe a half-constructed DM, and
   any failure in this sequence unregisters and zeros `reb.dm` before returning.

3. **Atomic generation end.** `Close + UnregRecv + (reb.dm = nil)` runs under
   `reb.mu` inside `endStreams`, the sole DM teardown site.

4. **Preempt waits `preemptRetries` seconds for full cleanup.** The next generation's preempt polls
   `oxreb.EndTime().IsZero()`, not `IsDone()`. `EndTime` becomes non-zero only
   after `xreb.Finish()`, which runs strictly after `endStreams` completes.
   Therefore, when preempt observes a non-zero `EndTime`, the previous
   generation's `UnregRecv` has already happened and the trname slot is free.


### Preempt timeout

The (currently hardcoded) `preemptRetries` polling budget in `_preempt()` is a compromise.

> `preemptRetries` is currently 16 seconds

During this time the previous - already aborted - generation must fully exit,
which entails:
* abort propagation through joggers and (optional) nwp workers
* in-flight transport, and
* fini() quiesce

Under degraded disks or heavy load, abort propagation alone can approach this bound.

In the end, the timeout value is a compromise: long enough to cover
typical cleanup, short enough that Smap flicker (when nodes keep leaving and (re)joining)
doesn't stack waiters.

## Cleanup mode

Cleanup mode is a rebalance generation that reuses the `*Reb` lifecycle but does
not open data streams and does not migrate object payloads.

The motivation is scalability. A regular data-moving rebalance may temporarily
leave extra local copies while the cluster converges. Tracking every migrated
object at runtime, only to remove the old copy later, would not scale for large
clusters and buckets with millions or billions of objects.

Cleanup mode is therefore out-of-band. It performs a separate local walk,
recomputes the expected HRW owner for each object, verifies the object at that
expected location, and removes the local misplaced copy only when it is safe to
do so.
