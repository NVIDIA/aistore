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
	eleFactory struct {
		xreg.RenewBase
		xctn *Election
	}
	Election struct {
		xact.Base
	}
)

// interface guard
var (
	_ cluster.Xact   = (*Election)(nil)
	_ xreg.Renewable = (*eleFactory)(nil)
)

func (*eleFactory) New(xreg.Args, *cluster.Bck) xreg.Renewable { return &eleFactory{} }

func (p *eleFactory) Start() error {
	p.xctn = &Election{}
	p.xctn.InitBase(cos.GenUUID(), apc.ActElection, nil)
	return nil
}

func (*eleFactory) Kind() string        { return apc.ActElection }
func (p *eleFactory) Get() cluster.Xact { return p.xctn }

func (*eleFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprUse, nil
}

func (*Election) Run(*sync.WaitGroup) { debug.Assert(false) }
