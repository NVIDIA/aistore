// Package registry provides core functionality for the AIStore extended actions registry.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package registry

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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

	GlobalEntryProvider interface {
		New(args XactArgs) GlobalEntry
		Kind() string
	}

	RebalanceArgs struct {
		ID          xaction.RebID
		StatsRunner *stats.Trunner
	}
)

func (r *registry) RegisterGlobalXact(entry GlobalEntryProvider) {
	cmn.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeGlobal)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.globalXacts[entry.Kind()] = entry
}

func (r *registry) RenewRebalance(id int64, statsRunner *stats.Trunner) cluster.Xact {
	e := r.globalXacts[cmn.ActRebalance].New(XactArgs{Custom: &RebalanceArgs{
		ID:          xaction.RebID(id),
		StatsRunner: statsRunner,
	}})
	res := r.renewGlobalXaction(e)
	if !res.isNew { // previous global rebalance is still running
		return nil
	}
	return res.entry.Get()
}

func (r *registry) RenewResilver(id string) cluster.Xact {
	e := r.globalXacts[cmn.ActResilver].New(XactArgs{UUID: id})
	res := r.renewGlobalXaction(e)
	cmn.Assert(res.isNew) // resilver must be always preempted
	return res.entry.Get()
}

func (r *registry) RenewElection() cluster.Xact {
	e := r.globalXacts[cmn.ActElection].New(XactArgs{})
	res := r.renewGlobalXaction(e)
	if !res.isNew { // previous election is still running
		return nil
	}
	return res.entry.Get()
}

//
// baseGlobalEntry
//

type (
	// nolint:unused // For now unused, soon it must change.
	baseGlobalEntry struct{}
)

func (b *baseGlobalEntry) PreRenewHook(previousEntry GlobalEntry) (keep bool) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (b *baseGlobalEntry) PostRenewHook(_ GlobalEntry) {}
