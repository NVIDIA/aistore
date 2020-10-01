// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runners

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
)

func init() {
	registry.Registry.RegisterGlobalXact(&electionProvider{})
	registry.Registry.RegisterGlobalXact(&resilverProvider{})
	registry.Registry.RegisterGlobalXact(&rebalanceProvider{})
}

type (
	getMarked = func() xaction.XactMarked
	RebBase   struct {
		xaction.XactBase
		wg *sync.WaitGroup
	}

	rebalanceProvider struct {
		xact *Rebalance
		args *registry.RebalanceArgs
	}
	Rebalance struct {
		RebBase
		statsRunner  *stats.Trunner // extended stats
		getRebMarked getMarked
	}

	resilverProvider struct {
		id   string
		xact *Resilver
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

func (xact *RebBase) MarkDone()      { xact.wg.Done() }
func (xact *RebBase) WaitForFinish() { xact.wg.Wait() }

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
	return RebBase{
		XactBase: *xaction.NewXactBase(id, kind),
		wg:       wg,
	}
}

// Rebalance

func (e *rebalanceProvider) New(args registry.XactArgs) registry.GlobalEntry {
	return &rebalanceProvider{args: args.Custom.(*registry.RebalanceArgs)}
}
func (e *rebalanceProvider) Start(_ cmn.Bck) error {
	xreb := NewRebalance(e.args.ID, e.Kind(), e.args.StatsRunner, registry.GetRebMarked)
	e.xact = xreb
	return nil
}
func (e *rebalanceProvider) Kind() string      { return cmn.ActRebalance }
func (e *rebalanceProvider) Get() cluster.Xact { return e.xact }
func (e *rebalanceProvider) PreRenewHook(previousEntry registry.GlobalEntry) (keep bool) {
	xreb := previousEntry.(*rebalanceProvider)
	if xreb.args.ID > e.args.ID {
		glog.Errorf("(reb: %s) g%d is greater than g%d", xreb.xact, xreb.args.ID, e.args.ID)
		keep = true
	} else if xreb.args.ID == e.args.ID {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xreb.xact)
		}
		keep = true
	}
	return
}
func (e *rebalanceProvider) PostRenewHook(previousEntry registry.GlobalEntry) {
	xreb := previousEntry.(*rebalanceProvider).xact
	xreb.Abort()
	xreb.WaitForFinish()
}

func NewRebalance(id cluster.XactID, kind string, statsRunner *stats.Trunner, getMarked getMarked) *Rebalance {
	return &Rebalance{
		RebBase:      makeXactRebBase(id, kind),
		statsRunner:  statsRunner,
		getRebMarked: getMarked,
	}
}

func (xact *Rebalance) IsMountpathXact() bool { return false }

func (xact *Rebalance) String() string {
	return fmt.Sprintf("%s, %s", xact.RebBase.String(), xact.ID())
}

// override/extend cmn.XactBase.Stats()
func (xact *Rebalance) Stats() cluster.XactStats {
	var (
		baseStats   = xact.XactBase.Stats().(*xaction.BaseXactStats)
		rebStats    = stats.RebalanceTargetStats{BaseXactStats: *baseStats}
		statsRunner = xact.statsRunner
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

// Resilver

func (e *resilverProvider) New(args registry.XactArgs) registry.GlobalEntry {
	return &resilverProvider{id: args.UUID}
}
func (e *resilverProvider) Start(_ cmn.Bck) error {
	e.xact = NewResilver(e.id, e.Kind())
	return nil
}
func (e *resilverProvider) Kind() string                             { return cmn.ActResilver }
func (e *resilverProvider) Get() cluster.Xact                        { return e.xact }
func (e *resilverProvider) PreRenewHook(_ registry.GlobalEntry) bool { return false }
func (e *resilverProvider) PostRenewHook(previousEntry registry.GlobalEntry) {
	xresilver := previousEntry.(*resilverProvider).xact
	xresilver.Abort()
	xresilver.WaitForFinish()
}

func NewResilver(uuid, kind string) *Resilver {
	return &Resilver{
		RebBase: makeXactRebBase(xaction.XactBaseID(uuid), kind),
	}
}

func (xact *Resilver) IsMountpathXact() bool { return true }

func (xact *Resilver) String() string {
	return xact.RebBase.String()
}

// Election

func (e *electionProvider) New(_ registry.XactArgs) registry.GlobalEntry {
	return &electionProvider{}
}
func (e *electionProvider) Start(_ cmn.Bck) error {
	e.xact = &Election{
		XactBase: *xaction.NewXactBase(xaction.XactBaseID(""), cmn.ActElection),
	}
	return nil
}
func (e *electionProvider) Get() cluster.Xact                        { return e.xact }
func (e *electionProvider) Kind() string                             { return cmn.ActElection }
func (e *electionProvider) PreRenewHook(_ registry.GlobalEntry) bool { return true }
func (e *electionProvider) PostRenewHook(_ registry.GlobalEntry)     {}

func (e *Election) IsMountpathXact() bool { return false }
