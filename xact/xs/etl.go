// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	etlFactory struct {
		xreg.RenewBase
		xctn *XactETL
	}
	// represents `apc.ActETLInline` kind of xaction (`apc.ActETLBck`/`apc.ActETLObject` kinds are managed by tcb/tcobjs)
	// responsible for triggering global abort on error to ensure all related ETL resources are cleaned up across all targets.
	XactETL struct {
		ObjErrs cos.Errs
		msg     etl.InitMsg
		xact.Base
	}
)

const maxObjErr = 128

// interface guard
var (
	_ core.Xact      = (*XactETL)(nil)
	_ xreg.Renewable = (*etlFactory)(nil)
)

func (*etlFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &etlFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *etlFactory) Start() error {
	debug.Assert(cos.IsValidUUID(p.Args.UUID), p.Args.UUID)
	p.xctn = newETL(p)
	return nil
}

func (*etlFactory) Kind() string     { return apc.ActETLInline }
func (p *etlFactory) Get() core.Xact { return p.xctn }

func (*etlFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

// (tests only)

func newETL(p *etlFactory) *XactETL {
	msg, ok := p.Args.Custom.(etl.InitMsg)
	debug.Assert(ok)
	xctn := &XactETL{
		msg:     msg,
		ObjErrs: cos.NewErrs(maxObjErr),
	}
	xctn.InitBase(p.Args.UUID, p.Kind(), msg.String(), nil)
	return xctn
}

func (*XactETL) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *XactETL) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
