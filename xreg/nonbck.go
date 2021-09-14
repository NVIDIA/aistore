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

func RegNonBckXact(entry Renewable) { defaultReg.regNonBckXact(entry) }

func (r *registry) regNonBckXact(entry Renewable) {
	debug.Assert(xaction.Table[entry.Kind()].Scope != xaction.ScopeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.nonbckXacts[entry.Kind()] = entry
}

func RenewRebalance(id int64, statTracker stats.Tracker) RenewRes {
	return defaultReg.renewRebalance(id, statTracker)
}

func (r *registry) renewRebalance(id int64, statTracker stats.Tracker) RenewRes {
	e := r.nonbckXacts[cmn.ActRebalance].New(Args{Custom: &RebalanceArgs{
		ID:          xaction.RebID2S(id),
		StatTracker: statTracker,
	}}, nil)
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
