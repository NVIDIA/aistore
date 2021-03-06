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

type (
	GlobalEntry interface {
		BaseEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		PreRenewHook(previousEntry GlobalEntry) (keep bool)
		// post-renew hook
		PostRenewHook(previousEntry GlobalEntry)
	}

	BaseGlobalEntry struct{}

	GlobalFactory interface {
		New(args Args) GlobalEntry
		Kind() string
	}

	RebalanceArgs struct {
		ID          string
		StatTracker stats.Tracker
	}
)

/////////////////////
// BaseGlobalEntry //
/////////////////////

func (*BaseGlobalEntry) PreRenewHook(previousEntry GlobalEntry) (keep bool) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (*BaseGlobalEntry) PostRenewHook(_ GlobalEntry) {}

//////////////
// registry //
//////////////

func RegGlobXact(entry GlobalFactory) { defaultReg.regGlobFactory(entry) }

func (r *registry) regGlobFactory(entry GlobalFactory) {
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
	res := r.renewGlobalXaction(e)
	if res.UUID != "" { // previous global rebalance is still running
		return nil
	}
	return res.Entry.Get()
}

func RenewResilver(id string) cluster.Xact { return defaultReg.renewResilver(id) }

func (r *registry) renewResilver(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActResilver].New(Args{UUID: id})
	res := r.renewGlobalXaction(e)
	debug.Assert(res.UUID == "") // resilver must be always preempted
	return res.Entry.Get()
}

func RenewElection() cluster.Xact { return defaultReg.renewElection() }

func (r *registry) renewElection() cluster.Xact {
	e := r.globalXacts[cmn.ActElection].New(Args{})
	res := r.renewGlobalXaction(e)
	if res.UUID != "" { // previous election is still running
		return nil
	}
	return res.Entry.Get()
}

func RenewLRU(id string) cluster.Xact { return defaultReg.renewLRU(id) }

func (r *registry) renewLRU(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActLRU].New(Args{UUID: id})
	res := r.renewGlobalXaction(e)
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
	return r.renewGlobalXaction(e)
}
