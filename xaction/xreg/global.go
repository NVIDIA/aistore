// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

type RebalanceArgs struct {
	ID          string
	StatTracker stats.Tracker
}

//////////////
// registry //
//////////////

func RegGlobXact(entry Factory) { defaultReg.regGlobFactory(entry) }

func (r *registry) regGlobFactory(entry Factory) {
	debug.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeGlobal)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.globalXacts[entry.Kind()] = entry
}

func RenewRebalance(id int64, statTracker stats.Tracker) cluster.Xact {
	return defaultReg.renewRebalance(id, statTracker)
}

func (r *registry) renewRebalance(id int64, statTracker stats.Tracker) cluster.Xact {
	e := r.globalXacts[cmn.ActRebalance].New(Args{Custom: &RebalanceArgs{
		ID:          xaction.RebID2S(id),
		StatTracker: statTracker,
	}})
	res := r.renew(e, nil)
	if res.UUID != "" { // previous global rebalance is still running
		return nil
	}
	return res.Entry.Get()
}

func RenewResilver(id string) cluster.Xact { return defaultReg.renewResilver(id) }

func (r *registry) renewResilver(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActResilver].New(Args{UUID: id})
	rns := r.renew(e, nil)
	debug.Assert(rns.UUID == "") // resilver must be always preempted
	return rns.Entry.Get()
}

func RenewElection() cluster.Xact { return defaultReg.renewElection() }

func (r *registry) renewElection() cluster.Xact {
	e := r.globalXacts[cmn.ActElection].New(Args{})
	rns := r.renew(e, nil)
	if rns.UUID != "" { // previous election is still running
		return nil
	}
	return rns.Entry.Get()
}

func RenewLRU(id string) cluster.Xact { return defaultReg.renewLRU(id) }

func (r *registry) renewLRU(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActLRU].New(Args{UUID: id})
	res := r.renew(e, nil)
	if res.UUID != "" { // Previous LRU is still running.
		res.Entry.Get().Renew()
		return nil
	}
	return res.Entry.Get()
}

func RenewDownloader(t cluster.Target, statsT stats.Tracker) RenewRes {
	return defaultReg.renewDownloader(t, statsT)
}

func (r *registry) renewDownloader(t cluster.Target, statsT stats.Tracker) RenewRes {
	e := r.globalXacts[cmn.ActDownload].New(Args{
		T:      t,
		Custom: statsT,
	})
	return r.renew(e, nil)
}
