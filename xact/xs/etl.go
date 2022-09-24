// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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

////////////////
// etlFactory //
////////////////

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
	// TODO: check xprev and reinforce
	return xreg.WprKeepAndStartNew, nil
}

/////////////////
// ETL xaction //
/////////////////

func newETL(id, kind string) (xctn *xactETL) {
	xctn = &xactETL{}
	xctn.InitBase(id, kind, nil)
	return
}

func (*xactETL) Run(*sync.WaitGroup) { debug.Assert(false) }
