// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

// rebalance & resilver xactions

type (
	rebFactory struct {
		xreg.RenewBase
		xact *Rebalance
	}
	resFactory struct {
		xreg.RenewBase
		xact *Resilver
	}

	Rebalance struct {
		xaction.XactBase
	}
	Resilver struct {
		xaction.XactBase
	}
)

// interface guard
var (
	_ cluster.Xact   = (*Rebalance)(nil)
	_ xreg.Renewable = (*rebFactory)(nil)
	_ cluster.Xact   = (*Resilver)(nil)
	_ xreg.Renewable = (*resFactory)(nil)
)

///////////////
// Rebalance //
///////////////

func (*rebFactory) New(args xreg.Args, _ *cluster.Bck) xreg.Renewable {
	return &rebFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *rebFactory) Start() error {
	p.xact = NewRebalance(p.Args.UUID, p.Kind())
	return nil
}

func (*rebFactory) Kind() string        { return cmn.ActRebalance }
func (p *rebFactory) Get() cluster.Xact { return p.xact }

func (p *rebFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	xreb := prevEntry.(*rebFactory)
	wpr = xreg.WprAbort
	if xreb.Args.UUID > p.Args.UUID {
		glog.Errorf("(reb: %s) %s is greater than %s", xreb.xact, xreb.Args.UUID, p.Args.UUID)
		wpr = xreg.WprUse
	} else if xreb.Args.UUID == p.Args.UUID {
		if verbose {
			glog.Infof("%s already running, nothing to do", xreb.xact)
		}
		wpr = xreg.WprUse
	}
	return
}

func NewRebalance(id, kind string) (xact *Rebalance) {
	xact = &Rebalance{}
	xact.InitBase(id, kind, nil)
	return
}

func (*Rebalance) Run(*sync.WaitGroup) { debug.Assert(false) }

func (xact *Rebalance) Snap() cluster.XactionSnap {
	rebSnap := &stats.RebalanceSnap{}
	xact.ToSnap(&rebSnap.Snap)
	if marked := xreg.GetRebMarked(); marked.Xact != nil {
		id, err := xaction.S2RebID(marked.Xact.ID())
		debug.AssertNoErr(err)
		rebSnap.RebID = id
	} else {
		rebSnap.RebID = 0
	}
	// NOTE: the number of rebalanced objects _is_ the number of transmitted objects
	//       (definition)
	rebSnap.Stats.Objs = rebSnap.Stats.OutObjs
	rebSnap.Stats.Bytes = rebSnap.Stats.OutBytes
	return rebSnap
}

//////////////
// Resilver //
//////////////

func (*resFactory) New(args xreg.Args, _ *cluster.Bck) xreg.Renewable {
	return &resFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *resFactory) Start() error {
	p.xact = NewResilver(p.UUID(), p.Kind())
	return nil
}

func (*resFactory) Kind() string                                       { return cmn.ActResilver }
func (p *resFactory) Get() cluster.Xact                                { return p.xact }
func (*resFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) { return xreg.WprAbort, nil }

func NewResilver(id, kind string) (xact *Resilver) {
	xact = &Resilver{}
	xact.InitBase(id, kind, nil)
	return
}

func (*Resilver) Run(*sync.WaitGroup) { debug.Assert(false) }

// TODO -- FIXME: check "resilver-marked" and unify with rebalance
func (xact *Resilver) Snap() cluster.XactionSnap {
	baseStats := xact.XactBase.Snap().(*xaction.Snap)
	return baseStats
}
