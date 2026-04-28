# Global Rebalance - Execution Flow (sketch)

Global rebalance uses a single transport name (`trname = "reb"`) for cross-target object
streaming. Because the trname is fixed, at most one DM may be registered against
the transport at any moment. This document describes how that constraint is
maintained across rebalance generations.

## Lifecycle

The `*Reb` service is constructed once at target startup and reused across all
rebalance generations. The DM, however, is **per-generation**: a new DM is
constructed at the start of each `Run()` that needs streams (single-node cluster
doesn't) - and torn down before `Run()` returns.

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

4. **Preempt waits 10s for full cleanup.** The next generation's preempt polls
   `oxreb.EndTime().IsZero()`, not `IsDone()`. `EndTime` becomes non-zero only
   after `xreb.Finish()`, which runs strictly after `endStreams` completes.
   Therefore, when preempt observes a non-zero `EndTime`, the previous
   generation's `UnregRecv` has already happened and the trname slot is free.


### 10s preempt timeout

The (currently hardcoded) 10s polling budget in `_preempt()` is a compromise.

During 10s the previous - and already aborted - generation must fully exit,
which entails:
* abort propagation through joggers and (optional) nwp workers
* in-flight transport, and
* fini() quiesce

Under degraded disks or heavy load, abort propagation alone can approach this bound.

In the end, the 10s value is a compromise: long enough to cover
typical cleanup, short enough that Smap flicker (when nodes keep leaving and (re)joining)
doesn't stack waiters.
