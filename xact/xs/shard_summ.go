// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	shardSummFactory struct {
		xctn *XactShardSumm
		msg  *apc.ShardSummMsg
		xreg.RenewBase
	}
	XactShardSumm struct {
		p *shardSummFactory
		// live counters; snapshot into apc.ShardSummResult in Result()
		nTarObjs      atomic.Uint64
		nTarSize      atomic.Uint64
		nShards       atomic.Uint64
		nShardSize    atomic.Uint64
		nArchivedObjs atomic.Uint64
		nStale        atomic.Uint64
		nInvalid      atomic.Uint64
		// TODO: include orphaned index count by sampling sysbucket indexes.
		xact.BckJogRunner
	}
)

var (
	_ core.Xact      = (*XactShardSumm)(nil)
	_ xreg.Renewable = (*shardSummFactory)(nil)
)

func (*shardSummFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &shardSummFactory{
		RenewBase: xreg.RenewBase{Args: args, Bck: bck},
		msg:       args.Custom.(*apc.ShardSummMsg),
	}
}

// construction only (the renewal caller does xact.GoRunW)
func (p *shardSummFactory) Start() error {
	r := &XactShardSumm{p: p}
	err := r.BckJogRunner.Init(p.UUID(), p.Kind(), p.Bck, xact.BckJogRunnerOpts{
		CbObj:      r.visit,
		Prefix:     p.msg.Prefix,
		NumWorkers: xact.NwpNone,
	}, cmn.GCO.Get())
	if err != nil {
		return err
	}
	p.xctn = r
	return nil
}

func (*shardSummFactory) Kind() string     { return apc.ActSummaryShard }
func (p *shardSummFactory) Get() core.Xact { return p.xctn }

func (*shardSummFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func (r *XactShardSumm) Run(wg *sync.WaitGroup) {
	wg.Done()
	r.BckJogRunner.Run()
	if err := r.BckJogRunner.Wait(); err != nil {
		r.AddErr(err)
	}
	r.Finish()
}

func (r *XactShardSumm) visit(lom *core.LOM, _ []byte) error {
	mime, err := archive.Mime("", lom.ObjName)
	if err != nil || mime != archive.ExtTar {
		return nil
	}

	lom.Lock(false)
	defer lom.Unlock(false)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return nil
		}
		return err
	}
	if lom.IsCopy() {
		return nil
	}

	size := lom.Lsize()
	r.nTarObjs.Inc()
	r.nTarSize.Add(uint64(size))
	r.ObjsAdd(1, size)
	if !lom.HasShardIdx() {
		return nil
	}

	idx, err := core.LoadShardIndex(lom)
	if err != nil {
		if errors.Is(err, archive.ErrShardIdxStale) {
			r.nStale.Inc()
		} else {
			r.nInvalid.Inc()
		}
		return nil
	}
	if idx == nil {
		return nil
	}
	r.nShards.Inc()
	r.nShardSize.Add(uint64(size))
	r.nArchivedObjs.Add(uint64(len(idx.Entries)))
	return nil
}

func (r *XactShardSumm) Result() (*apc.ShardSummResult, error) {
	return &apc.ShardSummResult{
		TarObjs:        r.nTarObjs.Load(),
		TarSize:        r.nTarSize.Load(),
		Shards:         r.nShards.Load(),
		ShardSize:      r.nShardSize.Load(),
		ArchivedObjs:   r.nArchivedObjs.Load(),
		StaleIndexes:   r.nStale.Load(),
		InvalidIndexes: r.nInvalid.Load(),
	}, r.Err()
}

func (r *XactShardSumm) Snap() *core.Snap { return r.Base.NewSnap(r) }

func (r *XactShardSumm) CtlMsg() string {
	if r.p == nil || r.p.msg == nil {
		return ""
	}
	var sb cos.SB
	sb.Init(160)
	if r.p.msg.Prefix != "" {
		idxAppend(&sb, "prefix", r.p.msg.Prefix)
	}
	tarObjs := r.nTarObjs.Load()
	shards := r.nShards.Load()
	idxAppend(&sb, "tar-objs", strconv.FormatUint(tarObjs, 10))
	idxAppend(&sb, "tar-size", strconv.FormatUint(r.nTarSize.Load(), 10))
	idxAppend(&sb, "shards", strconv.FormatUint(shards, 10))
	idxAppend(&sb, "shard-size", strconv.FormatUint(r.nShardSize.Load(), 10))
	idxAppend(&sb, "archived-objs", strconv.FormatUint(r.nArchivedObjs.Load(), 10))
	if n := r.nStale.Load(); n > 0 {
		idxAppend(&sb, "stale", strconv.FormatUint(n, 10))
	}
	if n := r.nInvalid.Load(); n > 0 {
		idxAppend(&sb, "invalid-index", strconv.FormatUint(n, 10))
	}
	if n := r.ErrCnt(); n > 0 {
		idxAppend(&sb, "errs", strconv.Itoa(n))
	}
	return sb.String()
}
