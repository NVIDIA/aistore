Package `xact` is the top-level home for AIStore eXtended Actions (a.k.a. batch jobs):
their static descriptor table, common interfaces, and shared lifecycle helpers.

An xaction is a long-running, asynchronous, batch job supported and tracked by AIS.
Examples include global rebalance, resilver, LRU eviction, bucket copy, bucket transform,
prefetch, download, EC encode, mirroring, list-objects, get-batch, and more.

## Xaction kinds

The canonical source for supported xaction kinds is
`xact.Table`; the user-facing names are also visible via `ais show job --help`.

```text
archive             blob-download  cleanup       copy-bucket    copy-objects      create-inventory
delete-objects      download       ec-bucket     ec-get         ec-put            ec-resp
elect-primary       etl-bucket     etl-inline    etl-objects    evict-objects     evict-remote-bucket
get-batch           list           lru-eviction  mirror         prefetch-objects  promote-files
put-copies          rebalance      rechunk       rename-bucket  resilver          shard-index
shard-index-demand  shard-summary  summary       warm-up-metadata
```

## Subdirectories

- `xreg` - local xaction registry and lifecycle coordination.
- `xs` - concrete named xactions and shared implementations.
- other files in this package define common xaction descriptors, base structs,
  snapshots, statistics, notification helpers, and utility code.

For the canonical list of xaction kinds and their static properties, see
`xact.Table` in `xact/api_table.go`.

Xaction kinds are generally the `apc.Act*` constants defined under `api/apc`.

## Static descriptor table

`xact.Table` is the static source of truth for xaction kind properties.

Each entry maps an xaction kind to a `Descriptor`. The descriptor captures
properties that would otherwise be scattered across the codebase:

- display name;
- access defaults;
- scope;
- whether the kind can be started through the generic start API;
- whether it changes cluster metadata;
- whether it refreshes capacity statistics;
- whether it is rebalance or resilver itself;
- whether it conflicts with rebalance or resilver;
- whether it must be aborted by rebalance;
- whether it can idle between requests;
- whether it exposes extended statistics;
- whether registry history should be kept briefly and quietly;
- whether it reports status to IC.

The table is intentionally static. Runtime state belongs to the local registry,
xaction instances, snapshots, IC notifications, or action-specific managers.

## Xaction scope

Each descriptor has a scope:

- `ScopeG` - cluster-wide xaction.
- `ScopeB` - bucket-scoped xaction.
- `ScopeGB` - either one bucket or all buckets.
- `ScopeT` - single-target xaction.

Scope describes the natural domain of the xaction kind. It does not by itself
define how status is queried, how the xaction is started, or whether the xaction
reports to IC.

For example, resilver is target-local and therefore `ScopeT`. Rebalance is
cluster-wide. Many storage-service and multi-object operations are bucket-scoped.

## Starting xactions

`Descriptor.Startable` means the xaction can be started via the generic
`StartXaction` path.

A non-startable xaction may still be a normal xaction. It can have registry
state, snapshots, statistics, abort semantics, and status reporting. It is simply
started through a different path.

Examples of non-generic start paths include:

- bucket copy, move, and transform APIs;
- PUT-triggered EC and mirroring lifecycles;
- list-objects request handling;
- GetBatch request handling;
- action-specific managers such as download;
- periodic or resource-triggered background work.

Therefore, do not interpret `Startable == false` as "not a real xaction" or
"not visible in status." It only excludes the generic start API.

## Local registry

Each target maintains a local xaction registry.

The registry is responsible for local lifecycle coordination:

- create or renew a running xaction;
- find running xactions;
- enforce limited coexistence rules;
- abort selected xactions;
- keep recently finished xactions briefly for status/history;
- expose local snapshots.

The registry is local to each daemon. Cluster-wide status is built either by
aggregating local snapshots from targets or by using IC status for xactions that
report to IC.

## Snapshots

A snapshot, or [`core.Snap`](../core/xaction.go), is a local description of an xaction instance.

Snapshots are the general-purpose status representation for xactions registered
locally. A snapshot can include common fields such as kind, ID, bucket, start
and end time, aborted/finished state, counters, and optional extended
xaction-specific statistics.

The snaps-based model is the broadest status model: targets expose their local
snapshots, and the caller or proxy aggregates them as needed.

This model is used by generic job display paths such as `show job`, and it is
the fallback for xaction kinds that do not participate in generic IC status
reporting.

## Status and wait models

AIS has more than one status/wait model because xactions have different
lifecycles and scaling requirements.

At a high level:

- snaps-based status queries local registry snapshots;
- IC status queries status reported to Information Center proxies;
- action-specific status uses custom machinery for xactions with specialized
  managers or protocols.

The descriptor table declares which generic model is valid for a given kind.

## Snaps-based status

In the snaps-based model, each target exposes local xaction snapshots. A caller,
usually via a proxy, aggregates per-target results.

This model is general and does not require targets to proactively report status
to IC. It works for xactions that maintain local registry state, including kinds
that are not IC-tracked.

Use snaps-based status/wait when:

- `Descriptor.ICMode == ICNone`;
- the xaction has local registry state but no generic IC status path;
- the caller needs target-by-target detail;
- a CLI or API path intentionally aggregates local target snapshots;
- the xaction has no meaningful IC terminal/progress reporting contract.

`ICNone` does not mean "no status." It means "no generic IC status path."

## IC status

IC stands for Information Center.

IC is a bounded set of AIS proxies that includes the current primary. For
xactions that report to IC, targets notify IC members about terminal state
and, for some kinds, periodic progress. Clients can then query or wait on one
IC proxy instead of polling every target.

This is primarily a scalability boundary.

IC bounds the client-facing query fanout to a single proxy regardless of cluster size.
Target-side push notifications still travel from every target to each IC proxy, but the
inbound rate is bounded by xaction lifecycle events, not by client poll frequency.

Generic IC status/wait is available only when the descriptor's `ICMode` is
non-zero.

## ICMode

`Descriptor.ICMode` declares whether and how the xaction kind reports status to
IC.

Modes:

- `ICNone` - the kind does not support the generic IC status/wait path.
  Callers must use snaps-based status/wait or an action-specific status path.
- `ICUponTerm` - targets notify IC when the xaction reaches terminal state.
- `ICUponProgress` - targets periodically notify IC with progress.

`ICUponProgress` is normally used together with `ICUponTerm`: progress updates
keep IC current while the xaction is running, and terminal notification closes
the lifecycle.

Most IC-tracked xactions use `ICUponTerm`.

`ActDownload` is the current progress-reporting case: targets push periodic
progress and also finalize on terminal state.

## ICNone

`ICNone` can mean several different things depending on the xaction kind.
The related reasons fall into three categories:
* intrinsic (e.g., ScopeT)
* action-specific machinery (e.g., list-objects)
* historical scope where IC reporting could be added later (e.g., multi-object kinds that may _idle_ between requests)

Again, `ICNone` does not mean "no state" or "no status." It only means that
the generic IC status/wait APIs must not be used for that kind.

## Waiting

Waiting is status plus a completion condition.

Generic wait APIs must use the correct status model:

- IC wait is valid for IC-tracked kinds. When the caller provides kind, the client
     validates `ICMode` before issuing the request.

     > ID-only status/wait requests are also valid; in that case the server resolves
     > the xaction ID and owns the final status/error semantics.

- Snaps-based wait is valid for all xactions. The completion condition is
  chosen by the caller: `args.Finished()` for normal lifecycles,
  `args.Idle()` for demand-driven or idle-cycle xactions where "finished"
  is delayed for a separately configured idle timeout
  after useful work is done (see `xact.IdlesBeforeFinishing`).

This distinction matters because a wait path that queries IC for a non-IC
xaction can wait forever or return misleading state. Conversely, forcing every
client to poll all targets for an IC-tracked xaction defeats the scalability
benefit of IC.

When adding a new xaction kind, define its status/wait model explicitly in
`xact.Table`.

## Idling xactions

Some xactions are demand-driven and may temporarily have no work while still
remaining alive. These kinds set `Descriptor.Idles`.

An idling xaction may transition through an idle state between requests. The
absence of current work does not necessarily mean the xaction is permanently
finished.

This affects status and IC reporting. For some idle-cycle xactions, a simple
terminal notification model is not a good fit. Those kinds should either remain
`ICNone` and use snaps-based or action-specific status, or explicitly implement
progress-based IC reporting.

Examples of idle or demand-driven categories include:

- on-demand EC/mirroring lifecycles;
- multi-object operations;
- list-objects;
- GetBatch;
- long-running background managers with intermittent work.

## Finishing and aborting

An accepted abort records cancellation immediately, but termination is asynchronous:
workers may still be exiting, streams may still be closing, and cleanup may
still be running. Neither an aborted status nor a return from `Abort` proves
that these activities have ended.

A cluster-wide job consists of participating targets' local xaction instances.
Each instance has its own lifecycle and resources. Reporting their results to
IC is separate from establishing that their peer-to-peer work has completed.

### Terms

- **Work item**: an independently admitted operation within an xaction. TCO
  transactions, archive requests, and GetBatch requests are examples. The
  objects, pages, or worker tasks of one batch are not necessarily independent
  work items.
- **Admission**: accepting more requests or work items into this instance.
  Closing admission does not mean that incoming peer data can be discarded.
- **Drain**: wait for queued and in-flight work to complete. For a data mover
  (DM), graceful `dm.Close(nil)` drains outbound streams and waits for peers'
  end-of-stream (FIN) acknowledgements. It does not unregister the receive endpoint.
- **Quiesce**: wait while a callback checks outstanding activity or peers.
  `Base.Quiesce` does not close the DM; its callback defines the completion
  condition. Silence alone is not an acknowledgement from a peer.
- **Teardown**: end this instance's transport participation. For a job-owned
  DM, this includes `dm.Close` and `dm.UnregRecv`. A shared data mover (SDM)
  remains open: its users unregister their own receive endpoints. Releasing
  work items, buffers, and other resources is referred to as cleanup below.
- **Owner path**: the path responsible for teardown and `Finish`, including
  helpers it starts and joins. A callback may initiate stopping and delegate
  cleanup to `Run`; the initiator need not execute the cleanup itself.

### Logical classes

The primary distinctions are whether an xaction is on-demand and whether one
instance serves independently admitted work items or a single logical job.

| Lifetime | Work served by one instance | Examples | Graceful completion condition |
|---|---|---|---|
| finite | one requested batch | TCB, prefetch, rechunk | finish the batch and its dependent activity |
| on-demand | one retained request context | list-objects, with successive pages | retire the context after idle expiration or explicit termination |
| on-demand | multiple independent work items | TCO, archive, GetBatch | finish items independently; retire the instance after admission closes and outstanding activity ends |

On-demand EC, mirroring, and indexing services also accept repeated operations;
an empty queue does not by itself retire the service. Completing a work item
and finishing its enclosing xaction are different events. A work-item failure
may be handled at item scope; `Abort` applies to the enclosing instance.

These distinctions explain when an instance may retire. They do not determine
who calls `Finish` or whether peers must agree on completion. Those mechanisms
are documented separately below.

### Run state and completion

| State or phase | Meaning | Current `RunState()` representation |
|---|---|---|
| running | executing work, or available for work | running |
| idle | temporarily without work; an on-demand instance may still accept requests | idle |
| stopping | admission is closed; draining, receiving, or teardown may remain | running or idle, unless aborted |
| aborted | cancellation and its cause have been recorded; execution may not yet have exited | aborted, including after `Finish` |
| finished | `Finish` has recorded the end time; notification and cleanup ordering depends on the implementation | finished, or aborted if the instance was aborted |

Stopping is an internal phase, not a separate wire run-state value. Idle is
not terminal, and aborted is not evidence that cleanup has completed.

`Base` tracks lifecycle and abort outcome separately. The lifecycle field
`eutime` is zero before stopping, `cos.MSB64` while stopping,
`finishSentinel` during the `Finish` commit, and an end time afterward.
Finishing is an internal commit step, not another public run state.

Consequences for callers:

- `IsDone()` includes stopping and finishing; it does not mean `Finish` has run.
- `EndTime()` remains zero through stopping and finishing, then becomes set.
- `Base.RunState()` gives the abort flag precedence over the end time.
  `DemandBase.RunState()` additionally reports idle when applicable, including
  during a non-aborted stopping phase. Neither reply proves admission is open.
- `ChanAbort()` closes on both abort and normal `Finish`; check `IsAborted()`
  or `AbortErr()` to distinguish them.
- `BackendCtx()` is canceled upon abort and finish. It cancels backend calls;
  it is not a lifecycle signal.
- A non-aborted instance can still finish with errors recorded by `AddErr`.
  Finished does not necessarily mean successful.

### Operations and ownership

| Operation | Entry condition | Effect |
|---|---|---|
| `SetStopping()` | lifecycle has not reached stopping | atomically enter stopping; return true only to the caller that makes this transition |
| `DemandBase.Stop()` | first call for this instance | mark stopping, unregister the idle housekeeper callback, close the idle channel if present; do not drain or finish |
| `Base.Abort(err)` | lifecycle has not reached stopping; no earlier accepted abort | record abort and its cause, mark stopping, cancel backend context, signal `ChanAbort`; do not call `Finish` |
| `Base.AbortByOwner(err)` | running or stopping; no earlier accepted abort | record the same abort state without invoking kind-specific teardown or `Finish` |
| `Finish()` | running or stopping | claim finishing, record end time, publish notifications, and refresh capacity where configured; repeated calls have no effect |

The first accepted abort retains its cause. `RecvAbortByOwner` preserves the
received-abort marker as well, so the received cause is not broadcast again.

Where callbacks race to perform cleanup, the successful `SetStopping` is the
ownership claim. The winner may perform cleanup or signal `Run` to do it.
`DemandBase.Stop` does not return that claim to its caller and does not join
workers. Its housekeeper unregistration also matters when a replacement
instance can reuse the same UUID and housekeeper name.

Outside callers use the xaction's `Abort` method, including its kind-specific
override. Once stopping is visible, `Base.Abort` rejects these calls. The owner
may still encounter a failed close, a peer timeout, or another completion
failure; it uses `AbortByOwner` and remains responsible for `Finish`.
Choose the method by the caller's responsibility, not by checking the state
and trying whichever abort is currently accepted.

The owner may call `AbortByOwner` through a quiesce callback or joined helpers.
For example, sentinel polls peer status concurrently, joins those polls before
returning, and lets the owner call `Finish` afterward. An unrelated T2T or IC
handler is not part of that owner path merely because it reports the same error.

The table describes `Base` methods. Overrides can perform additional work:
`DemandBase.Abort`, for example, calls `Finish` immediately after an accepted
base abort. Thus abort is asynchronous with respect to termination, but an
individual override may block while performing some cleanup inside the call.

### Current completion mechanisms

The following describes implementation differences within the logical classes;
it is not another classification of job lifetimes.

| Instances | Graceful path | Abort path |
|---|---|---|
| finite jobs without a peer completion protocol, e.g. cleanup and prefetch | `Run` completes and joins dependent work, then finishes | signal cancellation; `Run` exits and finishes; the kind determines cluster-wide propagation |
| demand loops using `DemandBase.Abort`, e.g. EC/mirroring services and download; list has its own abort override | idle signal retires the instance through its normal stopping path | the accepted abort calls `Finish` from the aborting goroutine; `Run` may continue cleaning up |
| TCB and TCO | `Run` drains outbound streams, announces done, waits for peers, unregisters receiving, and finishes | the owner records completion failures with `AbortByOwner`; TCO's inherited outside abort can finish earlier, before `Stop` |
| archive | `Run` stops admission, coordinates peers, closes the DM, and finishes | owner callbacks use `AbortByOwner`; inherited outside abort has the same early-finish behavior as TCO |
| GetBatch/x-moss | idle `fini` claims stopping, unregisters from SDM, cleans up work items, and finishes | the `Abort` override performs cleanup only after its base abort succeeds; inherited `DemandBase.Abort` publishes completion before that cleanup |
| demand shard-index | `onIdle` closes admission and signals workers; `Run` joins workers, releases queued work, stops the demand base, and finishes | the override records a base abort and signals workers; `Run` still owns cleanup and `Finish` |

Rebalance and resilver have their own lifecycle and stage coordination; see
[`reb`](../reb) and [`res`](../res).

A job without a peer completion protocol may still have workers, downloads,
or callbacks to drain. Conversely, a request-driven protocol can exchange
pages between targets without needing a whole-job 'done' exchange.

For TCB/TCO, successfully sending local data does not prove that receiving is
over. Each peer must confirm that it has finished sending. Target-to-target
(T2T) 'done' control provides that confirmation; status polling recovers a missed
notification or detects an aborted or unresponsive peer. The normal sender must
drain its streams before announcing done. Single-target jobs have no peers to
coordinate with.

Whole-job abort propagation is kind-specific: peer T2T broadcasts, IC aborts,
or a cluster event may inform the other participants. Preserve the original
cause across these paths and suppress rebroadcast of a received abort. IC
terminal notifications report an instance's result; they do not replace the
peer completion protocol.

### Known gaps and ordering differences

- The state check in `Abort` and the update of `abort.done` are separate.
  A concurrent `Finish` can publish success before the abort is recorded
  (compare the TODO in `reb/globrun.go`). The owner method's state check is
  not a lock either: its callers must finish their joined helpers before
  calling `Finish`.
- `DemandBase.Abort` can publish completion before `Run` tears down transport.
  x-moss's abort path also finishes before its cleanup. Archive calls `Finish`
  before its final work-item cleanup. An end time is therefore not a universal
  guarantee that all resources have been released.
- After `DemandBase.Stop`, outside aborts are rejected. Sentinel status polling
  can recover a peer abort, but does not recover a dropped user stop.
- Archive still sends 'done' in-band and ignores the result of `dm.Close`.
  Its current ordering differs from the TCB/TCO drain-before-done protocol.

## Rebalance and resilver coexistence

Some xactions require a stable target set. Others can safely run concurrently
with global rebalance or node-local resilver. The descriptor table captures this
with two primary flags:

- `ConflictRebRes`
- `AbortByReb`

`ConflictRebRes` means the kind must not start while rebalance or resilver is
already running. Conversely, rebalance or resilver must not start while this
kind is running.

`AbortByReb` means an in-flight instance of this kind is aborted when a new
global rebalance starts.

These flags often travel together: if a job needs a stable target set to start,
it usually needs the same stable set to finish. Exceptions are deliberate and
must be documented in `api_table.go`.

Current exception classes include:

- `AbortByReb` without `ConflictRebRes`:
  long-running or on-demand work that should not be refused merely because
  rebalance is currently running, but cannot correctly survive a topology
  change mid-flight.

- `ConflictRebRes` without `AbortByReb`:
  work where a partial result remains useful or recoverable, and aborting near
  completion would be wasteful.

## Rebalance and resilver descriptors

`Descriptor.Rebalance` and `Descriptor.Resilver` mark the xactions that are
themselves rebalance or resilver.

These flags are not generic conflict flags. They are used by registry and
coexistence logic to recognize self-conflicts and rebalance/resilver-specific
behavior.

`ActMoveBck` is rebalance-like and is marked accordingly because it moves data
between nodes as part of a bucket move.

## Stable target set

A stable target set is required when a job cannot correctly continue across a
cluster membership change.

A kind requires a stable target set if it sets either:

- `ConflictRebRes`;
- `AbortByReb`.

Xactions that open intra-cluster peer-to-peer streams generally pin those
streams to the Smap snapshot selected at construction. If the target set changes
while such a job is running, the original routing and stream assumptions may no
longer be valid.

## Metadata and capacity effects

`Descriptor.Metasync` means the xaction changes cluster metadata and must
participate in metadata synchronization semantics.

`Descriptor.RefreshCap` means capacity statistics should be refreshed on
completion.

These flags are independent of status reporting. An xaction can update metadata
or capacity while using IC status, snaps-based status, or an action-specific
status path.

## Extended statistics

`Descriptor.ExtendedStats` marks xactions that expose xaction-specific extended
statistics in addition to common snapshot fields.

Extended statistics belong to the snapshot/status representation. They do not
imply a particular status transport. They may be surfaced through snaps-based
queries, action-specific paths, or IC-backed status depending on the kind.

## Quiet logging and brief history

`Descriptor.Quiet` suppresses verbose per-state log records. `V(4, ModXs)`
overrides this suppression: verbosity level 4 or higher, or selecting the `xs`
logging module, enables these records.

`Descriptor.Brief` selects short finished-history retention for high-rate,
short-lived xactions. Entries become eligible after `hk.OldAgeXshort` (one
minute), or `hk.OldAgeXshortV` (ten minutes) at `V(4, ModXs)`, and are removed
by the approximately two-minute housekeeping pass. Remaining entries keep this cleanup armed even
without new completions; each pass uses the current verbosity.

List-objects uses both flags. GetBatch/x-moss uses `Quiet` only: it serves multiple
work items per instance and retains its finished history under the ordinary
`hk.OldAgeX` and `keepOldThreshold` policy. Neither flag limits live tracking.

## Action-specific status paths

Some xactions intentionally do not use the generic IC status path because they
have their own manager, protocol, or aggregation model.

Examples include:

- list-objects, where clients stream pages directly and generic job display uses
  snapshots;
- GetBatch/x-moss, which is proxy-coordinated and uses target-direct status;
- synchronous operations where a proxy aggregates per-target results and returns
  directly to the caller.

For these kinds, keep `ICMode == ICNone` and document the intended status path
near the descriptor entry.

## Adding a new xaction kind

When adding a new xaction kind, update `xact.Table` first.

Decide and document:

1. Scope:
   - cluster, bucket, bucket-or-global, or target-local.

2. Start path:
   - generic `StartXaction`, action-specific API, workload-triggered,
     periodic, or internal.

3. Registry behavior:
   - whether the xaction registers locally;
   - whether it can idle;
   - how long finished state should remain visible.

4. Status/wait model:
   - IC status/wait;
   - snaps-based status/wait;
   - action-specific status/wait.

5. IC reporting:
   - `ICNone`;
   - `ICUponTerm`;
   - `ICUponProgress`;
   - or a combination.

6. Rebalance/resilver coexistence:
   - whether a stable target set is required;
   - whether to refuse start during rebalance/resilver;
   - whether to abort on new rebalance.

7. Metadata and capacity effects:
   - `Metasync`;
   - `RefreshCap`.

8. Extended statistics:
   - whether common snapshot fields are enough;
   - whether `Snap.Ext` should carry action-specific data.

9. User-facing behavior:
   - CLI display;
   - wait semantics;
   - abort semantics;
   - documentation links.

If the kind has `ICMode == ICNone`, make sure generic IC status/wait callers
fail explicitly or route to the correct snaps-based or action-specific path.

## Common mistakes

Avoid treating `Startable == false` as "not visible" or "not a real xaction."

Avoid treating `ICNone` as "no status."

Avoid using generic IC wait for a kind that does not report to IC.

Avoid adding a new xaction kind without deciding how it behaves during global
rebalance and node-local resilver.

Avoid assuming that idle means finished, or that aborted means exited.

Avoid treating `IsDone()` as finished: it includes stopping.

Avoid using `Abort` for an owner-detected failure after `DemandBase.Stop`: the
base call is rejected. Use `AbortByOwner` and leave `Finish` to the owner.

Avoid calling `AbortByOwner` from outside the owner path.

Avoid teardown in an `Abort` override unless `Base.Abort` (or
`DemandBase.Abort`) returned true for that call. A stopping callback may
already own that cleanup.

Avoid reporting success for a peer-coordinated job when peers' receipt of
all data cannot be confirmed.

Avoid adding action-specific status machinery without documenting why generic
snaps or IC status is insufficient.

## User-facing documentation

This README is developer-facing. It describes internal xaction structure and
status models.

For user-facing commands, examples, and operational documentation, see:

- `docs/cli/job.md`
- `docs/cli/download.md`
- `docs/rebalance.md`
- `docs/batch.md`
- `docs/cli/object.md`
- `docs/storage_svcs.md`
