// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODOs:
// - inline shard index in PUT datapath (cold-GET, PUT) (no xaction)
// - bckjogrunner: support `NonRecurs` mode
// - add self-healing mechanism: detect the corrupted index on the fly and repair it

const idxLogInterval = 30 * time.Second

type (
	shardIndexFactory struct {
		xreg.RenewBase
		xctn *xactShardIndex
		kind string
	}
	xactShardIndex struct {
		msg *apc.IndexShardMsg
		xact.BckJogRunner
		stats struct { // per-target runtime counters (for CtlMsg observability)
			skipNonTar atomic.Int64 // non-TAR objects skipped (not indexable)
			skipHasIdx atomic.Int64 // shards with existing up-to-date index, skipped
			skipBusy   atomic.Int64 // source locked during metadata commit
			indexed    atomic.Int64 // newly indexed shards
			stale      atomic.Int64 // stale indexes successfully rebuilt
			corrupt    atomic.Int64 // corrupt indexes successfully rebuilt
			cached     atomic.Int64 // indexes loaded or reused in memory
		}
		lastLog atomic.Int64 // last log timestamp (sparse)
	}
)

// interface guard
var (
	_ core.Xact      = (*xactShardIndex)(nil)
	_ xreg.Renewable = (*shardIndexFactory)(nil)
)

///////////////////////
// shardIndexFactory //
///////////////////////

func (p *shardIndexFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &shardIndexFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind}
}

func (p *shardIndexFactory) Start() (err error) {
	p.xctn, err = newxactShardIndex(p)
	if err == nil && cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln("start index-shard", p.Bck.String(), "xid", p.UUID())
	}
	return err
}

func (p *shardIndexFactory) Kind() string   { return p.kind }
func (p *shardIndexFactory) Get() core.Xact { return p.xctn }

func (p *shardIndexFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (xreg.WPR, error) {
	prev := prevEntry.(*shardIndexFactory)
	if p.UUID() == prev.UUID() {
		return xreg.WprUse, nil
	}
	prevMsg := prev.Args.Custom.(*apc.IndexShardMsg)
	currMsg := p.Args.Custom.(*apc.IndexShardMsg)

	// empty prefix means "all objects" - always overlaps with everything
	if currMsg.Prefix != "" && prevMsg.Prefix != "" {
		// allow parallel jobs on strictly non-overlapping prefixes
		if !cmn.DirHasOrIsPrefix(currMsg.Prefix, prevMsg.Prefix) {
			return xreg.WprKeepAndStartNew, nil
		}
	}
	xprev := prevEntry.Get()
	return xreg.WprUse, cmn.NewErrFailedTo(core.T, "start index-shard", xprev.String(),
		fmt.Errorf("prefix %q overlaps with already running prefix %q; abort it first", currMsg.Prefix, prevMsg.Prefix))
}

/////////////////////
// xactShardIndex  //
/////////////////////

func newxactShardIndex(p *shardIndexFactory) (*xactShardIndex, error) {
	msg := p.Args.Custom.(*apc.IndexShardMsg)
	r := &xactShardIndex{msg: msg}
	err := r.BckJogRunner.Init(p.UUID(), p.Kind(), p.Bck, xact.BckJogRunnerOpts{
		CbObj:      r.do,
		Prefix:     msg.Prefix,
		RW:         true,
		NumWorkers: msg.NumWorkers,
	}, cmn.GCO.Get())
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *xactShardIndex) do(lom *core.LOM, _ []byte) error {
	// Only plain-TAR objects are indexable by BuildShardIndex.
	mime, err := archive.Mime("", lom.ObjName)
	if err != nil || mime != archive.ExtTar {
		r.stats.skipNonTar.Inc()
		return nil
	}

	idx, status, err := r.buildIdx(lom)
	if err != nil {
		r.addIndexErr(err, status)
		return nil
	}
	if idx == nil {
		if status == core.ShardIndexExisting {
			r.stats.skipHasIdx.Inc()
		}
		return nil
	}
	defer idx.Free()

	if err := r.Context().Err(); err != nil {
		r.addIndexErr(err, core.ShardIndexNone)
		return nil
	}
	if err := lom.SaveShardIndex(idx); err != nil {
		r.addIndexErr(err, core.ShardIndexNone)
		return nil
	}
	switch status {
	case core.ShardIndexNew:
		r.stats.indexed.Inc()
	case core.ShardIndexStale:
		r.stats.stale.Inc()
	case core.ShardIndexCorrupt:
		r.stats.corrupt.Inc()
	}
	r.ObjsAdd(1, idx.SrcSize()) // the amount of data indexed
	if r.msg.Cache {
		// cache the saved index via the archive-read path
		if err := r.cacheSavedIdx(lom); err != nil {
			r.addIndexErr(err, core.ShardIndexLoadFailed)
		}
	}

	if now := mono.NanoTime(); time.Duration(now-r.lastLog.Load()) > idxLogInterval {
		r.lastLog.Store(now)
		nlog.Infoln(r.Name(), "ctlmsg (", r.CtlMsg(), ")")
	}
	return nil
}

// Load and build under source(R); release before returning.
// Caller frees the index and reports errors.
// - no index: build it
// - existing index: verify unless SkipVerify, cache if requested (see checkIdx)
// - stale or corrupt: rebuild
func (r *xactShardIndex) buildIdx(lom *core.LOM) (*archive.ShardIndex, core.ShardIndexStatus, error) {
	if err := r.Context().Err(); err != nil {
		return nil, core.ShardIndexNone, err
	}

	lom.Lock(false)
	defer lom.Unlock(false)
	if err := r.Context().Err(); err != nil {
		return nil, core.ShardIndexNone, err
	}
	// jogger does not preload - load under our own lock
	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return nil, core.ShardIndexNone, nil
		}
		return nil, core.ShardIndexNone, err
	}
	if lom.IsCopy() {
		return nil, core.ShardIndexNone, nil
	}

	status := core.ShardIndexNew
	if lom.HasShardIdx() {
		var err error
		status, err = r.checkIdx(lom)
		switch status {
		case core.ShardIndexExisting, core.ShardIndexLoadFailed:
			return nil, status, err
		}
		// New (index object gone), Stale, Corrupt: (re)build
	}

	// TAR scan is not interruptible; check cancellation again before saving.
	idx, err := lom.ScanShardIndex()
	return idx, status, err
}

// existing index: verify unless SkipVerify; cache if requested
// - caching (i.e., loading) verifies it (decoding, staleness), and
// - invalid index is rebuilt regardless of SkipVerify
func (r *xactShardIndex) checkIdx(lom *core.LOM) (core.ShardIndexStatus, error) {
	if r.msg.Cache {
		idx, err := lom.LoadCachedShardIndex() // shared - do not Free
		if idx != nil {
			r.stats.cached.Inc()
			return core.ShardIndexExisting, nil
		}
		if !errors.Is(err, core.ErrShardIdxNotCached) {
			return idxLoadStatus(err)
		}
	}
	if r.msg.SkipVerify {
		return core.ShardIndexExisting, nil
	}
	idx, err := lom.LoadShardIndex()
	if idx != nil {
		idx.Free()
		return core.ShardIndexExisting, nil
	}
	return idxLoadStatus(err)
}

// failed or empty load => status
func idxLoadStatus(err error) (core.ShardIndexStatus, error) {
	switch {
	case err == nil:
		return core.ShardIndexNew, nil // index object gone
	case errors.Is(err, archive.ErrShardIdxStale):
		return core.ShardIndexStale, nil
	case errors.Is(err, archive.ErrShardIdxCorrupt):
		return core.ShardIndexCorrupt, nil
	default:
		return core.ShardIndexLoadFailed, err
	}
}

// best effort: load just-saved index in memory
func (r *xactShardIndex) cacheSavedIdx(lom *core.LOM) error {
	lom.Lock(false)
	defer lom.Unlock(false)
	if r.Context().Err() != nil {
		return nil
	}
	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return nil
		}
		return err
	}
	if lom.IsCopy() {
		return nil
	}
	idx, err := lom.LoadCachedShardIndex()
	switch {
	case idx != nil:
		debug.AssertNoErr(err)
		r.stats.cached.Inc()
		return nil
	case errors.Is(err, core.ErrShardIdxNotCached), errors.Is(err, archive.ErrShardIdxStale):
		return nil // gone, not admitted, changed
	default:
		return err // return nil or error
	}
}

func (r *xactShardIndex) addIndexErr(err error, status core.ShardIndexStatus) {
	switch {
	case status == core.ShardIndexLoadFailed:
		r.AddErr(err, 4)
	case cmn.IsErrBusy(err):
		r.stats.skipBusy.Inc()
		r.AddErr(err, 4)
	default:
		r.AddErr(err, 0)
	}
}

func (r *xactShardIndex) Run(wg *sync.WaitGroup) {
	wg.Done()
	r.BckJogRunner.Run()
	if errJog := r.BckJogRunner.Wait(); errJog != nil && !r.IsAborted() {
		nlog.Warningln(r.Name(), errJog)
	}
	nlog.Infoln("finish index-shard", r.Name(), r.CtlMsg())
	r.Finish()
}

func (r *xactShardIndex) Snap() *core.Snap {
	snap := r.Base.NewSnap(r)
	snap.Pack(r.BckJogRunner.NumJoggers(), r.BckJogRunner.NumWorkers(), r.BckJogRunner.WorkChanFull())
	return snap
}

func (r *xactShardIndex) CtlMsg() string {
	var sb cos.SB
	sb.Init(128)
	if r.msg.Prefix != "" {
		idxAppend(&sb, "prefix", r.msg.Prefix)
	}
	switch r.msg.NumWorkers {
	case xact.NwpDflt:
		// auto-tuned, nothing to show
	case xact.NwpNone:
		idxAppend(&sb, "num-workers", "none")
	default:
		idxAppend(&sb, "num-workers", strconv.Itoa(r.msg.NumWorkers))
	}
	if r.msg.SkipVerify {
		idxAppend(&sb, "skip-verify", "true")
	}
	if r.msg.Cache {
		idxAppend(&sb, "cache", "true")
	}
	if n := r.stats.skipNonTar.Load(); n > 0 {
		idxAppend(&sb, "skip-nontar", strconv.FormatInt(n, 10))
	}
	if n := r.stats.skipHasIdx.Load(); n > 0 {
		idxAppend(&sb, "skip-indexed", strconv.FormatInt(n, 10))
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
	if n := r.stats.cached.Load(); n > 0 {
		idxAppend(&sb, "cached", strconv.FormatInt(n, 10))
	}
	if n := r.stats.skipBusy.Load(); n > 0 {
		idxAppend(&sb, "skip-busy", strconv.FormatInt(n, 10))
	}
	if n := r.ErrCnt(); n > 0 {
		idxAppend(&sb, "errs", strconv.Itoa(n))
	}
	return sb.String()
}

func idxAppend(sb *cos.SB, key, val string) {
	if sb.Len() > 0 {
		sb.WriteString(", ")
	}
	sb.WriteString(key)
	sb.WriteUint8(':')
	sb.WriteString(val)
}
