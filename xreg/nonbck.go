// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"context"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

type BckSummaryArgs struct {
	Ctx context.Context
	Msg *cmn.BucketSummaryMsg
}

func RegNonBckXact(entry Renewable) { defaultReg.regNonBckXact(entry) }

func (r *registry) regNonBckXact(entry Renewable) {
	debug.Assert(xaction.Table[entry.Kind()].Scope != xaction.ScopeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.nonbckXacts[entry.Kind()] = entry
}

func RenewRebalance(id int64) RenewRes {
	return defaultReg.renewRebalance(id)
}

func (r *registry) renewRebalance(id int64) RenewRes {
	e := r.nonbckXacts[cmn.ActRebalance].New(Args{UUID: xaction.RebID2S(id)}, nil)
	return r.renew(e, nil)
}

func RenewResilver(id string) cluster.Xact { return defaultReg.renewResilver(id) }

func (r *registry) renewResilver(id string) cluster.Xact {
	e := r.nonbckXacts[cmn.ActResilver].New(Args{UUID: id}, nil)
	rns := r.renew(e, nil)
	debug.Assert(!rns.IsRunning()) // NOTE: resilver is always preempted
	return rns.Entry.Get()
}

func RenewElection() RenewRes { return defaultReg.renewElection() }

func (r *registry) renewElection() RenewRes {
	e := r.nonbckXacts[cmn.ActElection].New(Args{}, nil)
	return r.renew(e, nil)
}

func RenewLRU(id string) RenewRes { return defaultReg.renewLRU(id) }

func (r *registry) renewLRU(id string) RenewRes {
	e := r.nonbckXacts[cmn.ActLRU].New(Args{UUID: id}, nil)
	return r.renew(e, nil)
}

func RenewStoreCleanup(id string) RenewRes { return defaultReg.renewStoreCleanup(id) }

func (r *registry) renewStoreCleanup(id string) RenewRes {
	e := r.nonbckXacts[cmn.ActStoreCleanup].New(Args{UUID: id}, nil)
	return r.renew(e, nil)
}

func RenewDownloader(t cluster.Target, statsT stats.Tracker) RenewRes {
	return defaultReg.renewDownloader(t, statsT)
}

func (r *registry) renewDownloader(t cluster.Target, statsT stats.Tracker) RenewRes {
	e := r.nonbckXacts[cmn.ActDownload].New(Args{
		T:      t,
		Custom: statsT,
	}, nil)
	return r.renew(e, nil)
}

func RenewETL(t cluster.Target, msg interface{}) RenewRes {
	return defaultReg.renewETL(t, msg)
}

func (r *registry) renewETL(t cluster.Target, msg interface{}) RenewRes {
	e := r.nonbckXacts[cmn.ActETLInline].New(Args{
		T:      t,
		Custom: msg,
	}, nil)
	return r.renew(e, nil)
}

func RenewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) RenewRes {
	return defaultReg.renewBckSummary(ctx, t, bck, msg)
}

func (r *registry) renewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) RenewRes {
	custom := &BckSummaryArgs{Ctx: ctx, Msg: msg}
	e := r.nonbckXacts[cmn.ActSummaryBck].New(Args{
		T:      t,
		UUID:   msg.UUID,
		Custom: custom,
	}, bck)
	return r.renew(e, bck)
}
