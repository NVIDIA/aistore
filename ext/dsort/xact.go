//go:build sharding

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

/////////////
// factory //
/////////////

type (
	factory struct {
		xreg.RenewBase
		xctn *xaction
	}
	xaction struct {
		xact.Base
		args *xreg.DsortArgs
	}
)

func (*factory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &factory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *factory) Start() error {
	custom := p.Args.Custom
	args, ok := custom.(*xreg.DsortArgs)
	debug.Assert(ok)
	p.xctn = &xaction{args: args}
	p.xctn.InitBase(p.UUID(), apc.ActDsort, args.BckTo /*compare w/ tcb and tco*/)

	g.once.Do(func() {
		hk.Reg(apc.ActDsort+hk.NameSuffix, g.mg.housekeep, hk.DayInterval)
	})

	return nil
}

func (*factory) Kind() string     { return apc.ActDsort }
func (p *factory) Get() core.Xact { return p.xctn }

func (*factory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

/////////////
// xaction //
/////////////

func (*xaction) Run(*sync.WaitGroup) { debug.Assert(false) }

// NOTE: two ways to abort:
// - Manager.abort(errs ...error) legacy, and
// - xaction.Abort, to implement the corresponding interface and uniformly support `api.AbortXaction`
func (r *xaction) Abort(err error) (ok bool) {
	m, exists := g.mg.Get(r.ID(), false /*incl. archived*/)
	if !exists {
		return
	}
	if aborted := m.aborted(); !aborted {
		m.abort(err)
		ok = m.aborted()
	}
	return
}

func (*xaction) CtlMsg() string { return "" }

func (r *xaction) Snap() (snap *core.Snap) {
	snap = r.Base.NewSnap(r)

	m, exists := g.mg.Get(r.ID(), true /*incl. archived*/)
	if !exists {
		return
	}
	m.Metrics.lock()
	m.Metrics.update()
	m.Metrics.unlock()

	snap.Ext = m.Metrics

	j := m.Metrics.ToJobInfo(r.ID(), m.Pars)
	snap.StartTime = j.StartedTime
	snap.StartTime = j.StartedTime
	snap.EndTime = j.FinishTime
	snap.SrcBck = j.SrcBck
	snap.DstBck = j.DstBck
	snap.AbortedX = j.Aborted

	return
}
