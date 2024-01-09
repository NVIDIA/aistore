// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// rebalance & resilver xactions

type (
	rebFactory struct {
		xreg.RenewBase
		xctn *Rebalance
	}
	resFactory struct {
		xreg.RenewBase
		xctn *Resilver
	}

	Rebalance struct {
		xact.Base
	}
	Resilver struct {
		xact.Base
	}
)

// interface guard
var (
	_ core.Xact      = (*Rebalance)(nil)
	_ xreg.Renewable = (*rebFactory)(nil)

	_ core.Xact      = (*Resilver)(nil)
	_ xreg.Renewable = (*resFactory)(nil)
)

///////////////
// Rebalance //
///////////////

func (*rebFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &rebFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *rebFactory) Start() error {
	p.xctn = NewRebalance(p.Args.UUID, p.Kind())
	return nil
}

func (*rebFactory) Kind() string     { return apc.ActRebalance }
func (p *rebFactory) Get() core.Xact { return p.xctn }

func (p *rebFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	xreb := prevEntry.(*rebFactory)
	wpr = xreg.WprAbort
	if xreb.Args.UUID > p.Args.UUID {
		nlog.Errorf("(reb: %s) %s is greater than %s", xreb.xctn, xreb.Args.UUID, p.Args.UUID)
		wpr = xreg.WprUse
	} else if xreb.Args.UUID == p.Args.UUID {
		if cmn.Rom.FastV(4, cos.SmoduleXs) {
			nlog.Infof("%s already running, nothing to do", xreb.xctn)
		}
		wpr = xreg.WprUse
	}
	return
}

func NewRebalance(id, kind string) (xreb *Rebalance) {
	xreb = &Rebalance{}
	xreb.InitBase(id, kind, nil)
	return
}

func (*Rebalance) Run(*sync.WaitGroup) { debug.Assert(false) }

func (xreb *Rebalance) RebID() int64 {
	id, err := xact.S2RebID(xreb.ID())
	debug.AssertNoErr(err)
	return id
}

func (xreb *Rebalance) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	xreb.ToSnap(snap)
	snap.RebID = xreb.RebID()

	snap.IdleX = xreb.IsIdle()

	// the number of rebalanced objects _is_ the number of transmitted objects (definition)
	// (TODO: revisit)
	snap.Stats.Objs = snap.Stats.OutObjs
	snap.Stats.Bytes = snap.Stats.OutBytes
	return
}

//////////////
// Resilver //
//////////////

func (*resFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &resFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *resFactory) Start() error {
	p.xctn = NewResilver(p.UUID(), p.Kind())
	return nil
}

func (*resFactory) Kind() string                                       { return apc.ActResilver }
func (p *resFactory) Get() core.Xact                                   { return p.xctn }
func (*resFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) { return xreg.WprAbort, nil }

func NewResilver(id, kind string) (xres *Resilver) {
	xres = &Resilver{}
	xres.InitBase(id, kind, nil)
	return
}

func (*Resilver) Run(*sync.WaitGroup) { debug.Assert(false) }

func (xres *Resilver) String() string {
	if xres == nil {
		return "<xres-nil>"
	}
	return xres.Base.String()
}

func (xres *Resilver) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	xres.ToSnap(snap)

	snap.IdleX = xres.IsIdle()
	return
}
