This document collects follow-up work and design directions for the extended actions
in this `xact/xs` package.

Each entry records implementation context, motivation, and a possible
approach to guide future development and review. The list spans targeted fixes,
scalability improvements, and longer-term consolidation, with priorities and
dependencies noted where applicable.

## Contents

- [Remote prefix iteration: single designated target](#remote-prefix-iteration-single-designated-target-highest-priority)
- [Cluster metadata: declared per-job policy](#cluster-metadata-declared-per-job-policy)
- [On-demand adaptive rate limiter for jobs](#on-demand-adaptive-rate-limiter-for-jobs)
- [Data mover: finish without the receive-side quiesce](#data-mover-finish-without-the-receive-side-quiesce)
- [Out-of-band control for all xactions](#out-of-band-control-for-all-xactions)
- [prune: transient HEAD errors delete destination objects](#prune-transient-head-errors-delete-destination-objects)
- [Miscellaneous](#miscellaneous)

## Remote prefix iteration: single designated target (highest priority)

`lrit._prefix` (lrit.go): for a remote bucket, every target calls `backend.ListObjects` for the entire
prefix (`lsoPage`), page by page, and keeps only the names it owns (HRW).
`XactNBI.Run` (create_nbi.go) also lists the remote bucket on every target before `filterKeepMine`;
the duplication is not confined to `lrit`.

- backend list traffic grows with cluster size: N targets, N times the `ListObjects` calls
- larger clusters increase throttling pressure (429 or provider-reported 503); independent `lsoPage`
  retries add requests, although each loop backs off and has a retry limit
- callers: prefetch, evict and delete (evict.go), archive, tcobjs - all list-range-prefix jobs on remote buckets

Overhaul to an R-flow, as in list-objects (ais/plstcx.go `lsObjsR`; xs/lso.go):

- one designated target, `HrwTargetTask(xid)`, calls `backend.ListObjects`; listing retries stay on it alone
- all participants must agree on the request/work-item ID and Smap used to select the DT and assign names;
  a reused demand xaction can have multiple requests, so distinguish their page streams
- DT shares pages, or per-target subsets of names, with the other targets (reuse the list-objects R-flow
  machinery where possible); other targets iterate received names as they do local pages today
- related to the above, evaluate whole-page broadcast versus partitioning by (location => target) at the DT:
  - (a) broadcast encodes the page once but repeats network transfer, decoding and ownership checks at receivers;
  - (b) partitioning reduces that aggregate work but needs per-peer batches, encoding and flow control at the DT.
- initialize receivers before the DT starts publishing pages (as `lsObjsR` does); bound buffered pages,
  propagate listing errors, and distinguish an empty page with a continuation token from end of listing
- abort on DT failure or relevant target/placement changes; do not silently reassign the DT mid-listing
- unchanged: AIS buckets (`nextPageA`), explicit lists and ranges (`_list`, `_range`)

## Cluster metadata: declared per-job policy

Jobs obtain cluster metadata ad hoc via `core.T.Sowner().Get()` / `core.T.Bowner()`, without a declared
policy for when it is captured, how long it remains valid, or which changes invalidate it:

- stored snapshots: sentinel in tcb/tcobjs/archive, dpromote and lso (`r.smap`), moss (`wi.smap`)
- capture at different boundaries: `create_nbi.Run` holds one Smap through listing and finalization;
  `newNpgCtx` captures one per context; prefetch/evict pass one to `lrit.run`; nsumm selects its remote
  listing DT during construction and captures BMD when expanding a bucket query
- fresh lookups also occur inside operations (e.g., destination selection in `coi.do`, ais/tgtobj.go), so retaining
  a snapshot in the parent does not ensure that every dependent operation uses it

Existing reactions are partial and separate:

- `AbortByReb` (xact/api_table.go), enforced only when a rebalance starts (reb/globrun.go: `xreg.AbortByNewReb`)
- in-job checks at arbitrary points: `CheckSameTargets` in sentinel (tcb, tcobjs), lso, moss
- BMD application: `_postBMD` aborts jobs on removed buckets; `_abortDisabled` aborts mirroring/EC
  when disabled (ais/tgtcp.go). These are not a general metadata-consistency policy

Unlikely events:

- relying on `AbortByReb` for metadata consistency leaves a gap when no rebalance starts (disabled,
  `--no-rebalance`, or a change that triggers none); in-job checks cover only selected paths
- no declared policy for bucket generation (BID), relevant props, or bucket placement overrides;
  a destroy/re-create can change BID even when the name is unchanged between observed BMD versions

Direction:

- declare defaults per kind in `xact.Table`: no dependency, snapshot with invalidation, or just-in-time
  metadata; specify whether the snapshot belongs to the xaction or an individual work item
- keep optional job snapshots in shared state (e.g., `xact.Base`), and work-item snapshots with the item;
  pass them to dependent operations rather than replacing every fresh lookup indiscriminately
- on Smap publication (`meta.SmapListeners` / `ListenSmapChanged`), check affected running jobs;
  a version bump alone is not grounds for abort. `CheckSameTargets` compares target membership/state
  and relative cluster placement weights, but ignores proxy-only changes, target restarts and endpoints;
  jobs depending on peer incarnation/endpoints need additional handling (`CompareTargets`)
- add corresponding checks at BMD publication: track all dependent buckets (including source and
  destination), BID and relevant props/placement overrides; unrelated bucket changes must not abort a job
- close the snapshot/registration race by rechecking after registration; schedule checks/aborts outside
  metadata locks and avoid listener registration/unregistration inside Smap callbacks
- use `ErrMembershipChange` for target/placement invalidation and a suitable bucket error for BMD changes;
  retain `AbortByReb` for rebalance-specific coexistence/resource conflicts

## On-demand adaptive rate limiter for jobs

Current behavior:

- bucket has `rate_limit.backend.enabled`: `rlbackend` (ais/rlbackend.go) lazily builds a per-(bucket, verb)
  `cos.AdaptRateLim` per target (`Bck.NewBackendRateLim`) and retries safely repeatable calls via
  `rlbackend.retry`, returning `ErrBackendRetry` when backend throttling exhausts retries
- without that limiter, `lrit.lsoPage` has its own bounded backoff (`remPageSleepIni`, `remPageRetriesMax`);
  prefetch records failed cold GETs rather than retrying them itself. ext/dload has separate HTTP retries
- moss's `adv.Sleep` and admission 429s concern local resource pressure; they are separate from backend throttling

Goal: one mechanism for backend throttling; retain distinct handling for other retryable failures and local load.

- a job (prefetch first) creates its `cos.AdaptRateLim` if and only if:
  1. the bucket's backend rate limiter is not enabled, and
  2. backend throttling actually happened (429 or a provider-classified throttling 503)
- construction on demand and atomic (concurrent workers hit 429 at the same time): e.g., `atomic.Pointer` + CAS
- start with validated `rate_limit.backend` defaults; factor construction out of `NewBackendRateLim`,
  which currently returns nil when disabled. Divide a distributed budget among participating callers;
  a single LIST DT should not receive only 1/N of the budget merely because N targets are active
- after activation, workers share that job's limiter for the affected bucket/verb: acquire before calls,
  including retries, and feed throttling errors back through `OnErr`
- one shared retry helper for both worlds (generalize `rlbackend.retry`): bounded retries/wait,
  cancellation-aware backoff, reader cleanup, and replayable bodies for PUT retries
- preserve configured limits and `ErrBackendRetry` (as `lsoPage` already checks); do not stack a fallback
  limiter/retry loop on top. Propagate fallback state to nested backend calls and blob-download children
- scope: per job, keyed by (bucket, verb), including HEAD/GET/LIST as applicable; independent jobs still
  have independent fallback budgets, so this does not enforce a bucket-wide aggregate limit

## Data mover: finish without the receive-side quiesce

DM receive teardown adds a quiet-period wait to successful jobs, even after sentinel coordination:

- `XactTCB.run` -> `DM.UnregRecv` -> `DM.quiesce(transport.quiescent)` (transport/bundle/dmover.go)
- `transport.quiescent` defaults to 10s (validated minimum 8s); receive activity can extend the wait
- a sentinel `QuiDone` means peers sent their completion markers, not that every data callback has finished
- observed example: copying 20 small objects takes about 13s; this fixed teardown cost also affects CLI e2e

This is not every job's total delay after its last object: tco/archive also have demand-idle and
per-transaction completion lifecycles, and single-target/no-DM paths do not incur DM receive teardown.

The receive-side quiesce cannot be skipped as is: with `bundle_multiplier` > 1, `Streams.Send` round-robins
across streams to the same destination, so a peer's `done` may arrive before its last objects on another stream.

Approach (coordinate with [out-of-band control](#out-of-band-control-for-all-xactions); counts also apply to in-stream markers):

- freeze each sender's per-destination data count after all local producers stop; send failures must
  prevent successful completion. Transport queue acceptance alone is not a delivery acknowledgment
- receiver counts completed application callbacks per sender, after consuming the payload and completing
  the write; `DM.wrapRecvData` calls `InObjsAdd` before the callback and cannot prove this
- a peer is complete only after its `done` arrives and completed counts match, including an explicit zero;
  scope counts to xaction/transaction IDs and make duplicate control delivery idempotent
- cover per-transaction completion in tco/archive as well as whole-xaction teardown; otherwise a work item
  may be released or an archive finalized before its last data callback finishes
- once all expected peers satisfy this rule, and local work/receives are drained: explicitly authorize
  `UnregRecv` to skip `DM.quiesce`. Track callback exit and any asynchronous finalization before freeing state
- preserve existing abort/error cleanup and the quiet-period fallback whenever successful drain is unproven;
  timeout or fail on missing peers, stream errors, or count mismatches rather than waiting forever
- validate with `bundle_multiplier` > 1, delayed data/control, duplicate markers and failed writes;
  include unknown-size ETL payloads (count completed objects, independent of their size)

Goal: remove the additional receive quiet period. Sentinel probing is typically about 1s, but other job
lifecycles and work still contribute to elapsed time. Consider a feature flag for the first release.

Not recommended: `done` on every stream (each stream is FIFO, and receive callbacks run inline:
`rxloop` -> `rxObj` -> `recv`). This requires covering every stream, including reconnects and failures,
and still draining any work deferred by callbacks; prefer a count-based protocol independent of stream layout.

## Out-of-band control for all xactions

Replace in-stream control opcodes (`transport.OpcDone`, `OpcAbort`, `OpcRequest`, `OpcResponse`) with
T2T control (xact/t2tctrl.go: `POST /v1/xactions/t2tctrl/{xkind}/{xid}/{wid}/{opcode}`; `SendCtrl`, `BcastCtrl`).

Goal: reliable control, independent of data streams and DM. HTTP alone does not supply delivery or completion guarantees.

- today, only x-moss uses T2T control (moss.go; ais/ml.go); `target.xactCtrl` (ais/tgtxact.go) explicitly
  rejects other kinds, so generalize dispatch as well as adding opcodes
- still in-stream: sentinel.go (tcb, tcobjs, archive), streaming.go, lso.go (abort)
- add opcodes as needed: done (with per-destination counts, see above), progress request/response
- delivery: `SendCtrl` waits for an HTTP response; `BcastCtrl` launches asynchronous sends and only logs
  failures. Add bounded retries/acknowledgments or a receiver-side status fallback for completion
- ordering: initialize receive state before producers start; handle control arriving before xaction/work-item
  registration or after completion. Distinguish xaction and work-item IDs, validate the sender, and make
  duplicate/late delivery safe (a lost HTTP response can lead to a retry after the handler already ran)
- retain data-drain proof from the [DM item](#data-mover-finish-without-the-receive-side-quiesce);
  receiving `done` over HTTP does not establish ordering with data streams
- define capability/version gating for mixed-version clusters; remove group-1 opcodes from
  transport/header.go only after legacy use is no longer supported, updating `IsControl` and tests too

## prune: transient HEAD errors delete destination objects

`prune.do` (prune.go): `if _, err := core.T.HeadObjT2T(src, tsi); err != nil` shadows the outer `err`
and sets `ecode = http.StatusNotFound` on any error when the AIS source is owned by another target.
A timeout, 503, or connection reset therefore reads as "source does not exist", and `--sync` can remove
the destination object. This is a data-safety fix, not just error-message cleanup.

Preserve the actual error/status and remove only on a confirmed not-found; retain the destination and
report other HEAD failures. Add an integration regression that injects transient peer HEAD failures
and verifies that the destination survives, while a genuine source 404 still permits pruning.

## Miscellaneous

- rechunk (`xactRechunk.do`): takes the write lock for every object, including those it then skips
  (copies, monolithic staying monolithic, matching layout) - GETs block for nothing; read lock first,
  upgrade only before rewriting, reload after a failed upgrade (see in-code TODO)
- streaming (`streamingF.genBEID`, using `_tag`): xid is derived, not assigned; concurrent requests (same from, to) via different
  proxies can make targets adopt different xactions and fail with ErrBusy; mint on a designated target and
  hand out via the proxy, as get-batch does (ais/ml.go) - protocol change
- tcb, tcobjs, archive: duplicated code (three in-code TODOs); consolidate, preferably together with
  the out-of-band control migration
- tcb, tcobjs `_recv`: resolve the zero-atime workaround (`lom.AtimeUnix() == 0`); `coi._dm` already copies
  source attributes, so trace the paths that omit atime before removing the receive-side fallback
- coi: direct PUT (`cmn.ErrSkip`) does not report actual output bytes; `coi.do` (ais/tgtobj.go) can return only Err/Ecode,
  leaving `CoiRes.Lsize` zero, which `copier.do` then adds to `OutObjsAdd`. Obtain the actual transformed size
- wi_lso (`LsMissing`): any `hlom.Load` error reports `LocIsCopyMissingObj`; distinguish missing from
  unreadable/corrupt metadata. This path does not delete data, but its missing-object status can be misleading
