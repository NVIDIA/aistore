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

type idxStatus uint8

const (
	idxNone idxStatus = iota
	idxNew
	idxStale
	idxCorrupt
)

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

	// buildIdx holds the read lock only while streaming the TAR; releases before returning.
	idx, status := r.buildIdx(lom)
	if idx == nil {
		return nil
	}

	if err := lom.SaveShardIndex(idx); err != nil {
		if cmn.IsErrBusy(err) {
			// benign and expected (concurrent reader/writer): count it, record the (named)
			// busy error but log it only sparsely
			r.stats.skipBusy.Inc()
			r.AddErr(err, 4)
			return nil
		}
		r.AddErr(err, 0)
		return nil
	}
	switch status {
	case idxNew:
		r.stats.indexed.Inc()
	case idxStale:
		r.stats.stale.Inc()
	case idxCorrupt:
		r.stats.corrupt.Inc()
	}
	r.ObjsAdd(1, idx.SrcSize) // the amount of data indexed

	if now := mono.NanoTime(); time.Duration(now-r.lastLog.Load()) > idxLogInterval {
		r.lastLog.Store(now)
		nlog.Infoln(r.Name(), "ctlmsg (", r.CtlMsg(), ")")
	}
	return nil
}

// Take rlock, load the LOM, check whether indexing is needed, scan the TAR, and
// return the resulting ShardIndex.
// Return (nil, idxNone) if the object is already indexed (up-to-date) or an error occurred.
// Otherwise, the status indicates whether the index is new or replaces a stale/corrupt one.
func (r *xactShardIndex) buildIdx(lom *core.LOM) (*archive.ShardIndex, idxStatus) {
	lom.Lock(false)
	defer lom.Unlock(false)

	// jogger does not pre-load - load under our own lock.
	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return nil, idxNone
		}
		r.AddErr(err, 0)
		return nil, idxNone
	}
	if lom.IsCopy() {
		return nil, idxNone
	}

	// SkipVerify=true:
	// - trust HasShardIdx; skip without loading or verifying the index
	// - stale indexes remain until next non-SkipVerify run or individual read (ErrShardIdxStale)
	status := idxNew
	if lom.HasShardIdx() {
		if r.msg.SkipVerify {
			r.stats.skipHasIdx.Inc()
			return nil, idxNone
		}
		existing, err := lom.LoadShardIndex()
		switch {
		case errors.Is(err, archive.ErrShardIdxStale):
			status = idxStale
		case errors.Is(err, archive.ErrShardIdxCorrupt):
			status = idxCorrupt
		case err != nil:
			r.AddErr(err, 4)
			return nil, idxNone
		case existing != nil:
			r.stats.skipHasIdx.Inc()
			return nil, idxNone
		}
		// existing == nil: index absent or unreadable; fall through to re-index
	}

	// start to index/re-index the shard
	srcCksum := lom.Checksum() // capture under the read lock, for IsStale on the next load
	srcSize := lom.Lsize()

	fh, err := lom.Open()
	if err != nil {
		r.AddErr(err, 0)
		return nil, idxNone
	}

	idx, err := archive.BuildShardIndex(fh, srcSize)
	cos.Close(fh)
	if err != nil {
		r.AddErr(err, 0)
		return nil, idxNone
	}
	idx.SrcCksum = srcCksum
	idx.SrcSize = srcSize
	return idx, status
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
