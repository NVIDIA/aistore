// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	eleFactory struct {
		xreg.BaseEntry
		xact *Election
	}
	Election struct {
		xaction.XactBase
	}
)

// interface guard
var (
	_ cluster.Xact = (*Election)(nil)
	_ xreg.Factory = (*eleFactory)(nil)
)

func (*eleFactory) New(_ xreg.Args) xreg.Renewable { return &eleFactory{} }

func (p *eleFactory) Start(_ cmn.Bck) error {
	p.xact = &Election{}
	p.xact.InitBase(cos.GenUUID(), cmn.ActElection, nil)
	return nil
}

func (*eleFactory) Kind() string        { return cmn.ActElection }
func (p *eleFactory) Get() cluster.Xact { return p.xact }

func (*Election) Run() { debug.Assert(false) }
