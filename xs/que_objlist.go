// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/objwalk/query"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	queFactory struct {
		xreg.RenewBase
		xctn  *query.ObjectsListingXact
		ctx   context.Context
		query *query.ObjectsQuery
		msg   *cmn.ListObjsMsg
	}
)

// interface guard
var (
	_ xreg.Renewable = (*queFactory)(nil)
)

////////////////
// queFactory //
////////////////

func (p *queFactory) Start() (err error) {
	xctn := query.NewObjectsListing(p.ctx, p.T, p.query, p.msg)
	p.xctn = xctn
	return
}

func (*queFactory) Kind() string        { return cmn.ActQueryObjects }
func (p *queFactory) Get() cluster.Xact { return p.xctn }

func (*queFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	queArgs := args.Custom.(*xreg.ObjectsQueryArgs)
	p := &queFactory{
		RenewBase: xreg.RenewBase{Args: args, Bck: bck},
		ctx:       queArgs.Ctx,
		query:     queArgs.Q,
		msg:       queArgs.Msg,
	}
	return p
}

func (p *queFactory) WhenPrevIsRunning(xreg.Renewable) (w xreg.WPR, e error) {
	query.Registry.Delete(p.msg.UUID)
	return xreg.WprAbort, nil
}
