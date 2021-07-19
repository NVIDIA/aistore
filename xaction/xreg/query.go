// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
)

type queEntry struct {
	BaseEntry
	xact  *query.ObjectsListingXact
	ctx   context.Context
	t     cluster.Target
	query *query.ObjectsQuery
	msg   *cmn.SelectMsg
}

// interface guard
var (
	_ xrunner = (*queEntry)(nil)
)

func RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) RenewRes {
	return defaultReg.RenewQuery(ctx, t, q, msg)
}

func (r *registry) RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) RenewRes {
	if xact := query.Registry.Get(msg.UUID); xact != nil {
		if !xact.Aborted() {
			return RenewRes{&DummyEntry{xact}, nil, msg.UUID}
		}
		query.Registry.Delete(msg.UUID)
	}
	r.entries.mtx.Lock()
	err := r.entries.del(msg.UUID)
	r.entries.mtx.Unlock()
	if err != nil {
		return RenewRes{&DummyEntry{nil}, err, msg.UUID}
	}
	e := &queEntry{ctx: ctx, t: t, query: q, msg: msg}
	return r.renew(e, q.BckSource.Bck)
}

//////////////
// queEntry //
//////////////

func (e *queEntry) Start(cmn.Bck) (err error) {
	xact := query.NewObjectsListing(e.ctx, e.t, e.query, e.msg)
	e.xact = xact
	if query.Registry.Get(e.msg.UUID) != nil {
		err = fmt.Errorf("result set with handle %s already exists", e.msg.UUID)
	}
	return
}

func (*queEntry) Kind() string        { return cmn.ActQueryObjects }
func (e *queEntry) Get() cluster.Xact { return e.xact }

func (e *queEntry) WhenPrevIsRunning(Renewable) (wpr WPR, err error) {
	wpr = WprKeepAndStartNew
	if query.Registry.Get(e.msg.UUID) != nil {
		wpr = WprUse
	}
	return
}
