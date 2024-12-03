// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// rebalance & resilver xactions

const fmtpend = "%s: rebalance[%s] is "

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

var _rebID atomic.Int64

///////////////
// Rebalance //
///////////////

func (*rebFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &rebFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *rebFactory) Start() (err error) {
	p.xctn, err = newRebalance(p)
	return err
}

func (*rebFactory) Kind() string     { return apc.ActRebalance }
func (p *rebFactory) Get() core.Xact { return p.xctn }

func (p *rebFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*rebFactory)
	if prev.Args.UUID == p.Args.UUID {
		return xreg.WprUse, nil
	}

	//
	// NOTE: we always abort _previous_ (via `reb._preempt`) prior to starting a new one
	//
	nlog.Errorln(core.T.String(), "unexpected when-prev-running call:", prev.Args.UUID, p.Args.UUID)

	ic, ec := xact.S2RebID(p.Args.UUID)
	if ec != nil {
		nlog.Errorln("FATAL:", p.Args.UUID, ec)
		return xreg.WprAbort, ec // (unlikely)
	}
	ip, ep := xact.S2RebID(prev.Args.UUID)
	if ep != nil {
		nlog.Errorln("FATAL:", prev.Args.UUID, ep)
		return xreg.WprAbort, ep
	}
	debug.Assert(ip < ic)
	return xreg.WprAbort, nil
}

func newRebalance(p *rebFactory) (xreb *Rebalance, err error) {
	xreb = &Rebalance{}
	xreb.InitBase(p.Args.UUID, p.Kind(), nil)

	id, err := xact.S2RebID(p.Args.UUID)
	if err != nil {
		return nil, err
	}
	rebID := _rebID.Load()
	if rebID > id {
		return nil, fmt.Errorf(fmtpend+"old", core.T.String(), p.Args.UUID)
	}
	if rebID == id {
		return nil, fmt.Errorf(fmtpend+"current", core.T.String(), p.Args.UUID)
	}
	_rebID.Store(id)

	return xreb, nil
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
