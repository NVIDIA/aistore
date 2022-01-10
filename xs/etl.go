// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
	p.xctn = newETL(p.Args.UUID, p.Kind())
	return nil
}

func (*etlFactory) Kind() string        { return cmn.ActETLInline }
func (p *etlFactory) Get() cluster.Xact { return p.xctn }

func (p *etlFactory) WhenPrevIsRunning(xprev xreg.Renewable) (action xreg.WPR, err error) {
	cos.Assert(p.UUID() != xprev.UUID())
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

func (*xactETL) Run(*sync.WaitGroup) { cos.Assert(false) }
