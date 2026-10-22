// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// On-demand shard indexing provides best-effort indexing of plain TAR shards.
// A single target-wide xaction serves all buckets through a bounded worker queue.
//
//   TrySubmit -> workCh -> worker: buildIdx -> LOM.SaveShardIndex
//       |
//       +-- duplicate/full/stopping: skip
//
// Submission never waits for queue capacity. Each shard stays reserved while
// queued or processing, so duplicate submissions are skipped within this xaction.
// Reservations are keyed by the full object name.
//
// Workers verify existing indexes and rebuild missing, stale, or corrupt ones.
// They hold a source read lock while verifying and scanning, then release it
// before saving. Core revalidates the source before committing the index.
// Busy or missing sources are skipped, as are items that reach a worker under high node load.
// Each admitted item is attempted at most once.
//
// DemandBase handles idle expiration. The idle callback and abort path stop
// admission before signaling workers to exit. Run waits for workers to exit,
// drains queued work, and releases remaining reservations before finishing.
// Every admitted item is accounted for through a worker attempt, a skip after
// abort, or queue draining. Abort cannot interrupt a TAR scan already in progress.

// TODO: size the queue from XactConf.Burst and workers by mountpath count.
// TODO: coordinate admission and renewal with idle expiration in the shared demand base
const (
	demandShardIndexBurst      = cmn.XactBurstDflt
	demandShardIndexNumWorkers = 4
)

type (
	// create and renew one shard indexing xaction per target
	demandShardIndexFactory struct {
		xreg.RenewBase
		xctn *XactDemandShardIndex
	}
	XactDemandShardIndex struct {
		// shared idle and pending atomics, hk and renewal operate outside mu
		xact.DemandBase
		// admitted work awaiting a worker
		workCh chan core.LIF
		// stop signal for workers on idle shutdown or abort
		stopCh cos.StopCh
		// guard reservations and serialize admission, completion, stopping and abort
		mu sync.Mutex
		// shards reserved while queued or processing
		reserved cos.StrSet
		// workers that must exit before queue cleanup
		wg sync.WaitGroup
		// target local counters for indexing outcomes and admission accounting
		stats struct {
			admitted    atomic.Int64 // successful queue admissions, excluding renewal references
			duplicated  atomic.Int64 // duplicate submissions rejected while reserved
			processed   atomic.Int64 // completed worker attempts, including errors and expected skips
			skipAbort   atomic.Int64 // dequeued after abort without a worker attempt
			drained     atomic.Int64 // discarded during shutdown
			skipHasIdx  atomic.Int64 // shards with existing up-to-date index, skipped
			skipBusy    atomic.Int64 // source busy during read or commit
			skipLoad    atomic.Int64 // node load at or above High
			skipMissing atomic.Int64 // missing/unavailable sources or obsolete bucket identities
			indexed     atomic.Int64 // newly indexed shards
			stale       atomic.Int64 // stale indexes successfully rebuilt
			corrupt     atomic.Int64 // corrupt indexes successfully rebuilt
		}
	}
)

// interface guard
var (
	_ xact.Demand    = (*XactDemandShardIndex)(nil)
	_ xreg.Renewable = (*demandShardIndexFactory)(nil)
)

/////////////////////////////
// demandShardIndexFactory //
/////////////////////////////

// create factory instance, ignore bck arg since xaction is for all bcks
func (*demandShardIndexFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &demandShardIndexFactory{RenewBase: xreg.RenewBase{Args: args}}
}

// create the xact with fields and channels
// this function does NOT launch worker goroutines
func (p *demandShardIndexFactory) Start() error {
	if p.Args.UUID == "" {
		p.Args.UUID = cos.GenUUID()
	}
	r := &XactDemandShardIndex{
		workCh:   make(chan core.LIF, demandShardIndexBurst),
		reserved: make(cos.StrSet, demandShardIndexBurst),
	}
	// initialize worker signaling before registering hk
	r.stopCh.Init()
	r.DemandBase.Init(p.UUID(), p.Kind(), nil /*no specific bck*/, xact.IdleDefault, r.onIdle)
	p.xctn = r
	return nil
}

// used to distinguish xact type
func (*demandShardIndexFactory) Kind() string { return apc.ActDemandShardIndex }

// return xact created by Start()
func (p *demandShardIndexFactory) Get() core.Xact { return p.xctn }

// reuse existing xaction per target
func (*demandShardIndexFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprUse, nil
}

//////////////////////////
// XactDemandShardIndex //
//////////////////////////

// skip duplicates and never wait for queue capacity
// true reports admission, not completion
// callers submit LIFs for eligible TAR shards, workers recheck source eligibility
func (r *XactDemandShardIndex) TrySubmit(lif core.LIF) bool {
	// coordinate submission with stopping and abort
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.IsRunning() {
		return false
	}

	// bucket recreation may briefly skip work while the name is reserved
	if r.reserved.Contains(lif.Uname) {
		r.stats.duplicated.Inc()
		return false
	}
	r.reserved.Set(lif.Uname)

	// prevent idle so we can enqueue
	r.IncPending()

	// enqueue or release the reservation and pending reference
	select {
	case r.workCh <- lif:
		r.stats.admitted.Inc()
		return true
	default:
		r.reserved.Delete(lif.Uname)
		r.DecPending()
		return false
	}
}

// commit idle stopping and signal workers
func (r *XactDemandShardIndex) onIdle(int64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	// recheck pending under the admission lock
	if r.Pending() == 0 && r.SetStopping() {
		r.stopCh.Close()
	}
	return xact.IdleDefault
}

// return the worker stop signal for the demand interface
// callback mode leaves the base idle channel nil, the inherited accessor would panic
func (r *XactDemandShardIndex) IdleTimer() <-chan struct{} { return r.stopCh.Listen() }

// start workers and own final cleanup
func (r *XactDemandShardIndex) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name())
	var adv load.Advice
	adv.Init(load.FlMem|load.FlCla|load.FlDsk, &load.Extra{Cfg: cmn.GCO.Get().Disk, RW: true})
	// launch workers with private advice copies
	for range demandShardIndexNumWorkers {
		r.wg.Go(func() { r.work(adv) })
	}
	// acknowledge startup
	if wg != nil {
		wg.Done()
	}

	// wait for workers to finish in flight work
	r.wg.Wait()
	// release queued reservations after all workers exit
	r.mu.Lock()
	n := core.DrainLIF(r.workCh)
	debug.Assertf(len(r.reserved) == n, "%s: reserved %d != drained %d", r.Name(), len(r.reserved), n)
	clear(r.reserved)
	if n > 0 {
		r.SubPending(n)
		r.stats.drained.Add(int64(n))
	}
	r.mu.Unlock()
	// verify admission accounting
	admitted, processed := r.stats.admitted.Load(), r.stats.processed.Load()
	skipped, drained := r.stats.skipAbort.Load(), r.stats.drained.Load()
	debug.Assertf(admitted == processed+skipped+drained,
		"%s: admitted %d != processed %d + skipped %d + drained %d",
		r.Name(), admitted, processed, skipped, drained)
	// unregister hk and finish after cleanup
	r.DemandBase.Stop()
	r.Finish()
}

// record abort and signal workers, leave cleanup to run
func (r *XactDemandShardIndex) Abort(err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil && !r.IsIdle() {
		err = cmn.NewErrAborted(r.Name(), "aborting non-idle", nil)
	}
	ok := r.Base.Abort(err)
	if ok {
		r.stopCh.Close()
	}
	return ok
}

// observe stopping and process queued work
func (r *XactDemandShardIndex) work(adv load.Advice) {
	for {
		select {
		case <-r.stopCh.Listen():
			return
		case lif := <-r.workCh:
			// ensure not aborted before doing work
			aborted := r.IsAborted()
			if !aborted {
				// give way before index I/O or taking the source lock
				adv.Refresh()
			}
			switch {
			case aborted:
				r.stats.skipAbort.Inc()
			case adv.Load >= load.High:
				// skip at High to keep indexing best effort
				r.stats.skipLoad.Inc()
			default:
				r.do(lif)
			}
			if !aborted {
				r.stats.processed.Inc()
			}

			// release ownership after the worker attempt and cleanup
			r.mu.Lock()
			r.reserved.Delete(lif.Uname)
			r.DecPending()
			r.mu.Unlock()

			// back off without holding locks or the source reservation
			if !aborted && adv.Sleep > 0 {
				time.Sleep(adv.Sleep)
			}
		}
	}
}

// process one item in queue and create shard index
func (r *XactDemandShardIndex) do(lif core.LIF) {
	// predefine for later cleanup
	var (
		lom    *core.LOM
		idx    *archive.ShardIndex
		status core.ShardIndexStatus
		err    error
	)

	// reconstruct LOM using the LIFs saved bck identity
	lom, err = lif.LOM()
	if err != nil {
		goto done
	}
	defer core.FreeLOM(lom)

	// source read lock is released before error reporting or saving
	idx, status, err = r.buildIdx(lom)
	if idx != nil {
		defer idx.Free()
	}

	switch ctxErr := r.Context().Err(); {
	case err != nil:
		// build or verification failed
	case idx == nil:
		if status == core.ShardIndexExisting {
			r.stats.skipHasIdx.Inc()
		}
	case ctxErr != nil:
		// canceled before saving
		err = ctxErr
	default:
		// save the new index
		err = lom.SaveShardIndex(idx)
	}
	if err != nil || idx == nil {
		goto done
	}

	// increment counters based on status
	switch status {
	case core.ShardIndexNew:
		r.stats.indexed.Inc()
	case core.ShardIndexStale:
		r.stats.stale.Inc()
	case core.ShardIndexCorrupt:
		r.stats.corrupt.Inc()
	}
	r.ObjsAdd(1, idx.SrcSize())

done:
	// demand indexing counts expected skips without retaining them as xaction errors
	switch {
	case err == nil || r.IsAborted():
	case cmn.IsErrObjNought(err) || cmn.IsErrBucketNought(err):
		r.stats.skipMissing.Inc()
	case cmn.IsErrBusy(err):
		r.stats.skipBusy.Inc()
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Infoln(r.Name(), err)
		}
	default:
		r.AddErr(err, 4)
	}
}

// try the source read lock, then load and build (the caller owns the returned)
// index and reports errors after return, when the source lock has been released
func (r *XactDemandShardIndex) buildIdx(lom *core.LOM) (*archive.ShardIndex, core.ShardIndexStatus, error) {
	// if canceled skip
	if err := r.Context().Err(); err != nil {
		return nil, core.ShardIndexNone, err
	}

	// only handle regular TAR shards
	mime, err := archive.Mime("", lom.ObjName)
	if err != nil || mime != archive.ExtTar {
		return nil, core.ShardIndexNone, nil
	}

	// read lock busy, so we skip our attempt
	if !lom.TryLock(false) {
		return nil, core.ShardIndexNone, cmn.NewErrBusy("demand shard index", lom.Cname())
	}
	defer lom.Unlock(false)

	// again, if canceled skip
	if err := r.Context().Err(); err != nil {
		return nil, core.ShardIndexNone, err
	}

	// load lom since lif only gets obj handle
	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		return nil, core.ShardIndexNone, err
	}

	// skip mirror copies
	if lom.IsCopy() {
		return nil, core.ShardIndexNone, nil
	}

	// check the existing index first
	status := core.ShardIndexNew
	if lom.HasShardIdx() {
		existing, err := lom.LoadShardIndex()
		if existing != nil {
			existing.Free()
			return nil, core.ShardIndexExisting, nil
		}
		status, err = idxLoadStatus(err)
		if err != nil {
			return nil, status, err
		}
		// missing, stale, or corrupt indexes need (re)building
		// proceed to build below
	}

	// cancellation is checked again before saving
	// the TAR scan is not interruptible
	idx, err := lom.ScanShardIndex()
	return idx, status, err
}

// build status string used in API resp/cli output
func (r *XactDemandShardIndex) CtlMsg() string {
	var sb cos.SB
	sb.Init(128)
	idxAppend(&sb, "pending", strconv.FormatInt(r.Pending(), 10))
	if n := r.stats.duplicated.Load(); n > 0 {
		idxAppend(&sb, "skip-dup", strconv.FormatInt(n, 10))
	}
	if n := r.stats.skipHasIdx.Load(); n > 0 {
		idxAppend(&sb, "skip-indexed", strconv.FormatInt(n, 10))
	}
	if n := r.stats.skipMissing.Load(); n > 0 {
		idxAppend(&sb, "skip-missing", strconv.FormatInt(n, 10))
	}
	if n := r.stats.indexed.Load(); n > 0 {
		idxAppend(&sb, "index", strconv.FormatInt(n, 10))
	}
	if n := r.stats.stale.Load(); n > 0 {
		idxAppend(&sb, "re-stale", strconv.FormatInt(n, 10))
	}
	if n := r.stats.corrupt.Load(); n > 0 {
		idxAppend(&sb, "re-corrupt", strconv.FormatInt(n, 10))
	}
	if n := r.stats.skipBusy.Load(); n > 0 {
		idxAppend(&sb, "skip-busy", strconv.FormatInt(n, 10))
	}
	if n := r.stats.skipLoad.Load(); n > 0 {
		idxAppend(&sb, "skip-load", strconv.FormatInt(n, 10))
	}
	if n := r.ErrCnt(); n > 0 {
		idxAppend(&sb, "errs", strconv.Itoa(n))
	}
	return sb.String()
}

// return snapshot of current xaction state
func (r *XactDemandShardIndex) Snap() *core.Snap { return r.Base.NewSnap(r) }
