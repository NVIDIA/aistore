// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

type BaseGlobalEntry struct{}

func (b *BaseGlobalEntry) PreRenewHook(previousEntry GlobalEntry) (keep bool) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}
func (b *BaseGlobalEntry) PostRenewHook(_ GlobalEntry) {}

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

	GlobalEntryProvider interface {
		New(args XactArgs) GlobalEntry
		Kind() string
	}

	RebalanceArgs struct {
		ID          xaction.RebID
		StatTracker stats.Tracker
	}
)

func RegGlobXact(entry GlobalEntryProvider) { defaultReg.regGlobXact(entry) }

func (r *registry) regGlobXact(entry GlobalEntryProvider) {
	cos.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeGlobal)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.globalXacts[entry.Kind()] = entry
}

func RenewRebalance(id int64, statTracker stats.Tracker) cluster.Xact {
	return defaultReg.renewRebalance(id, statTracker)
}

func (r *registry) renewRebalance(id int64, statTracker stats.Tracker) cluster.Xact {
	e := r.globalXacts[cmn.ActRebalance].New(XactArgs{Custom: &RebalanceArgs{
		ID:          xaction.RebID(id),
		StatTracker: statTracker,
	}})
	res := r.renewGlobalXaction(e)
	if !res.isNew { // previous global rebalance is still running
		return nil
	}
	return res.entry.Get()
}

func RenewResilver(id string) cluster.Xact { return defaultReg.renewResilver(id) }

func (r *registry) renewResilver(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActResilver].New(XactArgs{UUID: id})
	res := r.renewGlobalXaction(e)
	cos.Assert(res.isNew) // resilver must be always preempted
	return res.entry.Get()
}

func RenewElection() cluster.Xact { return defaultReg.renewElection() }

func (r *registry) renewElection() cluster.Xact {
	e := r.globalXacts[cmn.ActElection].New(XactArgs{})
	res := r.renewGlobalXaction(e)
	if !res.isNew { // previous election is still running
		return nil
	}
	return res.entry.Get()
}

func RenewLRU(id string) cluster.Xact { return defaultReg.renewLRU(id) }

func (r *registry) renewLRU(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActLRU].New(XactArgs{UUID: id})
	res := r.renewGlobalXaction(e)
	if !res.isNew { // Previous LRU is still running.
		res.entry.Get().Renew()
		return nil
	}
	return res.entry.Get()
}

func RenewDownloader(t cluster.Target, statsT stats.Tracker) (cluster.Xact, error) {
	return defaultReg.renewDownloader(t, statsT)
}

func (r *registry) renewDownloader(t cluster.Target, statsT stats.Tracker) (cluster.Xact, error) {
	e := r.globalXacts[cmn.ActDownload].New(XactArgs{
		T:      t,
		Custom: statsT,
	})
	res := r.renewGlobalXaction(e)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get(), nil
}
