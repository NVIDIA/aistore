// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

//////////////
// xfactory //
//////////////

type (
	xaction struct {
		xact.Base
		args *xreg.DsortArgs
	}
	xfactory struct {
		xreg.RenewBase
		xctn *xaction
	}
)

func (*xfactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &xfactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *xfactory) Start() error {
	custom := p.Args.Custom
	args, ok := custom.(*xreg.DsortArgs)
	debug.Assert(ok)
	p.xctn = &xaction{args: args}
	p.xctn.InitBase(p.UUID(), apc.ActDsort, args.BckTo /*compare w/ tcb and tco*/)
	return nil
}

func (*xfactory) Kind() string        { return apc.ActDsort }
func (p *xfactory) Get() cluster.Xact { return p.xctn }

// TODO -- FIXME: compare w/ tcb/tco
func (*xfactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

/////////////
// xaction //
/////////////

func (*xaction) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *xaction) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	m, exists := Managers.Get(r.ID(), true /*allowPersisted*/)
	if exists {
		// TODO -- FIXME: new CLI table; consider JobInfo instead
		m.Metrics.update()
		snap.Ext = m.Metrics

		j := m.Metrics.ToJobInfo(r.ID())
		snap.StartTime = j.StartedTime
		snap.EndTime = j.FinishTime
		snap.SrcBck = r.args.BckFrom.Clone()
		snap.DstBck = r.args.BckTo.Clone()
		snap.AbortedX = j.Aborted

		// TODO -- FIXME: extended (creation, extraction) stats => snap.Stats.Objs et al.
	}
	return
}
