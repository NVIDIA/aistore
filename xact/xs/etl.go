// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	etlFactory struct {
		xreg.RenewBase
		xctn *xactETL
	}
	xactETL struct {
		xact.Base
	}
)

// interface guard
var (
	_ cluster.Xact   = (*xactETL)(nil)
	_ xreg.Renewable = (*etlFactory)(nil)
)

func (*etlFactory) New(args xreg.Args, _ *cluster.Bck) xreg.Renewable {
	return &etlFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *etlFactory) Start() error {
	uuid := p.Args.UUID
	if uuid == "" {
		uuid = cos.GenUUID()
	}
	p.xctn = newETL(uuid, p.Kind())
	return nil
}

func (*etlFactory) Kind() string        { return apc.ActETLInline }
func (p *etlFactory) Get() cluster.Xact { return p.xctn }

func (*etlFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

// (tests only)

func newETL(id, kind string) (xctn *xactETL) {
	xctn = &xactETL{}
	xctn.InitBase(id, kind, nil)
	return
}

func (*xactETL) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *xactETL) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
