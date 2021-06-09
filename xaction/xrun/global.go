// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xrun

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	getMarked = func() xaction.XactMarked
	RebBase   struct {
		xaction.XactBase
		wg *sync.WaitGroup
	}

	rebalanceProvider struct {
		xact *Rebalance
		args *xreg.RebalanceArgs
	}
	Rebalance struct {
		RebBase
		statTracker  stats.Tracker // extended stats
		getRebMarked getMarked
	}

	resilverProvider struct {
		xact *Resilver
		id   string
	}
	Resilver struct {
		RebBase
	}

	electionProvider struct {
		xact *Election
	}
	Election struct {
		xaction.XactBase
	}
)

// interface guard
var (
	_ cluster.Xact             = (*Rebalance)(nil)
	_ xreg.GlobalEntryProvider = (*rebalanceProvider)(nil)
	_ cluster.Xact             = (*Resilver)(nil)
	_ xreg.GlobalEntryProvider = (*resilverProvider)(nil)
	_ cluster.Xact             = (*Election)(nil)
	_ xreg.GlobalEntryProvider = (*electionProvider)(nil)
)

func (xact *RebBase) MarkDone()      { xact.wg.Done() }
func (xact *RebBase) WaitForFinish() { xact.wg.Wait() }
func (xact *RebBase) Run()           { cos.Assert(false) }

func (xact *RebBase) String() string {
	s := xact.XactBase.String()
	if xact.Bck().Name != "" {
		s += ", bucket " + xact.Bck().String()
	}
	return s
}

//
// resilver|rebalance helper
//
func makeXactRebBase(id cluster.XactID, kind string) RebBase {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	args := xaction.Args{ID: id, Kind: kind}
	return RebBase{XactBase: *xaction.NewXactBase(args), wg: wg}
}

///////////////
// Rebalance //
///////////////

func (p *rebalanceProvider) New(args xreg.XactArgs) xreg.GlobalEntry {
	return &rebalanceProvider{args: args.Custom.(*xreg.RebalanceArgs)}
}

func (p *rebalanceProvider) Start(_ cmn.Bck) error {
	p.xact = NewRebalance(p.args.ID, p.Kind(), p.args.StatTracker, xreg.GetRebMarked)
	return nil
}
func (*rebalanceProvider) Kind() string        { return cmn.ActRebalance }
func (p *rebalanceProvider) Get() cluster.Xact { return p.xact }
func (p *rebalanceProvider) PreRenewHook(previousEntry xreg.GlobalEntry) (keep bool) {
	xreb := previousEntry.(*rebalanceProvider)
	if xreb.args.ID > p.args.ID {
		glog.Errorf("(reb: %s) g%d is greater than g%d", xreb.xact, xreb.args.ID, p.args.ID)
		keep = true
	} else if xreb.args.ID == p.args.ID {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xreb.xact)
		}
		keep = true
	}
	return
}

func (p *rebalanceProvider) PostRenewHook(previousEntry xreg.GlobalEntry) {
	xreb := previousEntry.(*rebalanceProvider).xact
	xreb.Abort()
	xreb.WaitForFinish()
}

func NewRebalance(id cluster.XactID, kind string, statTracker stats.Tracker, getMarked getMarked) *Rebalance {
	return &Rebalance{
		RebBase:      makeXactRebBase(id, kind),
		statTracker:  statTracker,
		getRebMarked: getMarked,
	}
}

func (xact *Rebalance) String() string {
	return fmt.Sprintf("%s, %s", xact.RebBase.String(), xact.ID())
}

// override/extend cmn.XactBase.Stats()
func (xact *Rebalance) Stats() cluster.XactStats {
	var (
		baseStats   = xact.XactBase.Stats().(*xaction.BaseXactStats)
		rebStats    = stats.RebalanceTargetStats{BaseXactStats: *baseStats}
		statsRunner = xact.statTracker
	)
	rebStats.Ext.RebTxCount = statsRunner.Get(stats.RebTxCount)
	rebStats.Ext.RebTxSize = statsRunner.Get(stats.RebTxSize)
	rebStats.Ext.RebRxCount = statsRunner.Get(stats.RebRxCount)
	rebStats.Ext.RebRxSize = statsRunner.Get(stats.RebRxSize)
	if marked := xact.getRebMarked(); marked.Xact != nil {
		rebStats.Ext.RebID = marked.Xact.ID().Int()
	} else {
		rebStats.Ext.RebID = 0
	}
	rebStats.ObjCountX = rebStats.Ext.RebTxCount + rebStats.Ext.RebRxCount
	rebStats.BytesCountX = rebStats.Ext.RebTxSize + rebStats.Ext.RebRxSize
	return &rebStats
}

//////////////
// Resilver //
//////////////

func (*resilverProvider) New(args xreg.XactArgs) xreg.GlobalEntry {
	return &resilverProvider{id: args.UUID}
}

func (p *resilverProvider) Start(_ cmn.Bck) error {
	p.xact = NewResilver(p.id, p.Kind())
	return nil
}
func (*resilverProvider) Kind() string                           { return cmn.ActResilver }
func (p *resilverProvider) Get() cluster.Xact                    { return p.xact }
func (p *resilverProvider) PreRenewHook(_ xreg.GlobalEntry) bool { return false }
func (p *resilverProvider) PostRenewHook(previousEntry xreg.GlobalEntry) {
	xresilver := previousEntry.(*resilverProvider).xact
	xresilver.Abort()
	xresilver.WaitForFinish()
}

func NewResilver(uuid, kind string) *Resilver {
	return &Resilver{
		RebBase: makeXactRebBase(xaction.BaseID(uuid), kind),
	}
}

func (xact *Resilver) String() string {
	return xact.RebBase.String()
}

//////////////
// Election //
//////////////

func (*electionProvider) New(_ xreg.XactArgs) xreg.GlobalEntry { return &electionProvider{} }

func (p *electionProvider) Start(_ cmn.Bck) error {
	args := xaction.Args{ID: xaction.BaseID(""), Kind: cmn.ActElection}
	p.xact = &Election{XactBase: *xaction.NewXactBase(args)}
	return nil
}

func (*electionProvider) Kind() string                           { return cmn.ActElection }
func (p *electionProvider) Get() cluster.Xact                    { return p.xact }
func (p *electionProvider) PreRenewHook(_ xreg.GlobalEntry) bool { return true }
func (p *electionProvider) PostRenewHook(_ xreg.GlobalEntry)     {}
func (e *Election) Run()                                         { cos.Assert(false) }
