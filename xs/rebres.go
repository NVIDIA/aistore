// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

// rebalance & resilver xactions

type (
	getMarked = func() xaction.XactMarked
	RebBase   struct {
		xaction.XactBase
		wg *sync.WaitGroup
	}
	rebFactory struct {
		xact *Rebalance
		args *xreg.RebalanceArgs
	}
	Rebalance struct {
		RebBase
		statTracker  stats.Tracker // extended stats
		getRebMarked getMarked
	}
	resilverFactory struct {
		xact *Resilver
		id   string
	}
	Resilver struct {
		RebBase
	}
)

// interface guard
var (
	_ cluster.Xact       = (*Rebalance)(nil)
	_ xreg.GlobalFactory = (*rebFactory)(nil)
	_ cluster.Xact       = (*Resilver)(nil)
	_ xreg.GlobalFactory = (*resilverFactory)(nil)
)

func (xact *RebBase) MarkDone()      { xact.wg.Done() }
func (xact *RebBase) WaitForFinish() { xact.wg.Wait() }
func (*RebBase) Run()                { debug.Assert(false) }

func (xact *RebBase) String() string {
	s := xact.XactBase.String()
	if xact.Bck().Name != "" {
		s += ", bucket " + xact.Bck().String()
	}
	return s
}

func (xact *RebBase) initRebBase(id, kind string) {
	xact.wg = &sync.WaitGroup{}
	xact.wg.Add(1)
	xact.InitBase(id, kind, nil)
}

///////////////
// Rebalance //
///////////////

func (*rebFactory) New(args xreg.Args) xreg.GlobalEntry {
	return &rebFactory{args: args.Custom.(*xreg.RebalanceArgs)}
}

func (p *rebFactory) Start(_ cmn.Bck) error {
	p.xact = NewRebalance(p.args.ID, p.Kind(), p.args.StatTracker, xreg.GetRebMarked)
	return nil
}

func (*rebFactory) Kind() string        { return cmn.ActRebalance }
func (p *rebFactory) Get() cluster.Xact { return p.xact }

func (p *rebFactory) PreRenewHook(previousEntry xreg.GlobalEntry) (keep bool) {
	xreb := previousEntry.(*rebFactory)
	if xreb.args.ID > p.args.ID {
		glog.Errorf("(reb: %s) %s is greater than %s", xreb.xact, xreb.args.ID, p.args.ID)
		keep = true
	} else if xreb.args.ID == p.args.ID {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xreb.xact)
		}
		keep = true
	}
	return
}

func (*rebFactory) PostRenewHook(previousEntry xreg.GlobalEntry) {
	xreb := previousEntry.(*rebFactory).xact
	xreb.Abort()
	xreb.WaitForFinish()
}

func NewRebalance(id, kind string, statTracker stats.Tracker, getMarked getMarked) (xact *Rebalance) {
	xact = &Rebalance{statTracker: statTracker, getRebMarked: getMarked}
	xact.initRebBase(id, kind)
	return
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
		var err error
		rebStats.Ext.RebID, err = xaction.S2RebID(marked.Xact.ID())
		debug.AssertNoErr(err)
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

func (*resilverFactory) New(args xreg.Args) xreg.GlobalEntry {
	return &resilverFactory{id: args.UUID}
}

func (p *resilverFactory) Start(_ cmn.Bck) error {
	p.xact = NewResilver(p.id, p.Kind())
	return nil
}

func (*resilverFactory) Kind() string                         { return cmn.ActResilver }
func (p *resilverFactory) Get() cluster.Xact                  { return p.xact }
func (*resilverFactory) PreRenewHook(_ xreg.GlobalEntry) bool { return false }

func (*resilverFactory) PostRenewHook(previousEntry xreg.GlobalEntry) {
	xresilver := previousEntry.(*resilverFactory).xact
	xresilver.Abort()
	xresilver.WaitForFinish()
}

func NewResilver(uuid, kind string) (xact *Resilver) {
	xact = &Resilver{}
	xact.initRebBase(uuid, kind)
	return
}

func (xact *Resilver) String() string {
	return xact.RebBase.String()
}
