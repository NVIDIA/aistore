// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODOs:
// - integrate shard index into GET datapath (GetBatch, GET with archpath
// - inline shard index in PUT datapath (cold-GET, PUT) (no xaction)
// - bckjogrunner:
//   - support `NoLoad` mode
//   - support `NonRecurs` mode
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
		// per-target runtime counters (for CtlMsg observability)
		cntSkipNonTar     atomic.Int64 // non-TAR objects skipped (not indexable)
		cntSkipHasIdx     atomic.Int64 // shards with existing up-to-date index, skipped
		cntIndexed        atomic.Int64 // shards newly indexed in this run
		cntReindexedStale atomic.Int64 // shards re-indexed due to stale index detection
		lastLog           atomic.Int64 // last log timestamp (sparse)
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

	// empty prefix means "all objects" — always overlaps with everything
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
		r.cntSkipNonTar.Inc()
		return nil
	}

	// buildIdx holds the read lock only while streaming the TAR; releases before returning.
	idx, stale := r.buildIdx(lom)
	if idx == nil {
		return nil
	}

	// SaveShardIndex acquires its own write lock on archlom for the metadata commit.
	if err := core.SaveShardIndex(lom, idx); err != nil {
		r.AddErr(err, 0)
		return nil
	}
	if stale {
		r.cntReindexedStale.Inc()
	} else {
		r.cntIndexed.Inc()
	}
	r.ObjsAdd(1, idx.SrcSize) // the amount of data indexed

	if now := mono.NanoTime(); time.Duration(now-r.lastLog.Load()) > idxLogInterval {
		r.lastLog.Store(now)
		nlog.Infoln(r.Name(), "ctlmsg (", r.CtlMsg(), ")")
	}
	return nil
}

// buildIdx acquires a read lock, loads the LOM, checks whether indexing is needed,
// scan through the TAR, and returns the resulting ShardIndex.
// Returns (nil, false) if the object is already indexed (up-to-date) or an error occurred.
// Returns (idx, true) when re-indexing a stale entry (shard re-uploaded; PUT preserves HasShardIdx,
// so staleness is detected via the embedded SrcCksum/SrcSize — see LoadShardIndex and IsStale).
// The read lock is released before returning.
func (r *xactShardIndex) buildIdx(lom *core.LOM) (*archive.ShardIndex, bool) {
	lom.Lock(false)
	defer lom.Unlock(false)

	// Always load under our own lock — the jogger does not pre-load (NoLoad: true).
	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		r.AddErr(err, 0)
		return nil, false
	}

	// SkipVerify=true  — trust HasShardIdx; skip without loading the index.
	// SkipVerify=true  — skip loading and verifying the index.
	// NOTE: stale indexes remain until next non-SkipVerify run or individual read (ErrShardIdxStale).
	stale := false
	if lom.HasShardIdx() {
		if r.msg.SkipVerify {
			r.cntSkipHasIdx.Inc()
			return nil, false
		}
		existing, err := core.LoadShardIndex(lom)
		switch {
		case err != nil:
			stale = true // stale or corrupt — re-index
		case existing != nil:
			// verified up-to-date
			r.cntSkipHasIdx.Inc()
			return nil, false
		}
		// existing == nil: index absent or unreadable; fall through to re-index
	}

	// start to index/re-index the shard
	srcCksum := lom.Checksum() // capture under read lock for IsStale on next load
	srcSize := lom.Lsize()

	fh, err := lom.Open()
	if err != nil {
		r.AddErr(err, 0)
		return nil, false
	}

	idx, err := archive.BuildShardIndex(fh, srcSize)
	cos.Close(fh)
	if err != nil {
		r.AddErr(err, 0)
		return nil, false
	}
	idx.SrcCksum = srcCksum
	idx.SrcSize = srcSize
	return idx, stale
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
	if n := r.cntSkipNonTar.Load(); n > 0 {
		idxAppend(&sb, "skip-nontar", strconv.FormatInt(n, 10))
	}
	if n := r.cntSkipHasIdx.Load(); n > 0 {
		idxAppend(&sb, "skip-indexed", strconv.FormatInt(n, 10))
	}
	if n := r.cntIndexed.Load(); n > 0 {
		idxAppend(&sb, "index", strconv.FormatInt(n, 10))
	}
	if n := r.cntReindexedStale.Load(); n > 0 {
		idxAppend(&sb, "re-stale", strconv.FormatInt(n, 10))
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
