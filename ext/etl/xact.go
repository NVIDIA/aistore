// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	factory struct {
		xreg.RenewBase
		xctn *XactETL
	}
	// represents `apc.ActETLInline` kind of xaction (`apc.ActETLBck`/`apc.ActETLObject` kinds are managed by tcb/tcobjs)
	// responsible for triggering global abort on error to ensure all related ETL resources are cleaned up across all targets.
	XactETL struct {
		ObjErrs cos.Errs
		msg     InitMsg
		xact.Base
	}
)

const maxObjErr = 128

// interface guard
var (
	_ core.Xact      = (*XactETL)(nil)
	_ xreg.Renewable = (*factory)(nil)
)

func (*factory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &factory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *factory) Start() error {
	debug.Assert(cos.IsValidUUID(p.Args.UUID), p.Args.UUID)
	p.xctn = newETL(p)
	return nil
}

func (*factory) Kind() string     { return apc.ActETLInline }
func (p *factory) Get() core.Xact { return p.xctn }

func (*factory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	// always start a new ETL xaction for each ETL instance initialization
	return xreg.WprKeepAndStartNew, nil
}

// (tests only)

func newETL(p *factory) *XactETL {
	msg, ok := p.Args.Custom.(InitMsg)
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
