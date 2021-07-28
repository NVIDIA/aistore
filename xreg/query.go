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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/query"
)

type (
	queFactory struct {
		xact  *query.ObjectsListingXact
		ctx   context.Context
		t     cluster.Target
		query *query.ObjectsQuery
		msg   *cmn.SelectMsg
	}
	// Serves to return the result of renewing
	DummyEntry struct {
		xact cluster.Xact
	}
)

// interface guard
var (
	_ Renewable = (*queFactory)(nil)
)

func RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) RenewRes {
	return defaultReg.RenewQuery(ctx, t, q, msg)
}

func (r *registry) RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) RenewRes {
	if xact := query.Registry.Get(msg.UUID); xact != nil {
		if !xact.Aborted() {
			return RenewRes{Entry: &DummyEntry{xact}, Err: nil, UUID: msg.UUID}
		}
		query.Registry.Delete(msg.UUID)
	}
	r.entries.mtx.Lock()
	err := r.entries.del(msg.UUID)
	r.entries.mtx.Unlock()
	if err != nil {
		return RenewRes{Entry: &DummyEntry{nil}, Err: err, UUID: msg.UUID}
	}
	e := &queFactory{ctx: ctx, t: t, query: q, msg: msg}
	if err = e.Start(); err != nil {
		return RenewRes{Entry: &DummyEntry{nil}, Err: err, UUID: msg.UUID}
	}
	r.add(e)
	return RenewRes{Entry: e, Err: err, UUID: ""}
}

//////////////
// queFactory //
//////////////

func (*queFactory) New(Args, *cluster.Bck) Renewable { debug.Assert(false); return nil }

func (e *queFactory) Start() (err error) {
	if query.Registry.Get(e.msg.UUID) != nil {
		err = fmt.Errorf("result set with handle %s already exists", e.msg.UUID)
	}
	xact := query.NewObjectsListing(e.ctx, e.t, e.query, e.msg)
	e.xact = xact
	return
}

func (*queFactory) Kind() string        { return cmn.ActQueryObjects }
func (e *queFactory) Get() cluster.Xact { return e.xact }

// never called
func (*queFactory) Bucket() *cluster.Bck                         { debug.Assert(false); return nil }
func (*queFactory) UUID() string                                 { debug.Assert(false); return "" }
func (*queFactory) WhenPrevIsRunning(Renewable) (w WPR, e error) { debug.Assert(false); return }

////////////////
// DummyEntry //
////////////////

// interface guard
var (
	_ Renewable = (*DummyEntry)(nil)
)

func (*DummyEntry) New(Args, *cluster.Bck) Renewable         { return nil }
func (*DummyEntry) Start() error                             { return nil }
func (*DummyEntry) Kind() string                             { return "" }
func (*DummyEntry) UUID() string                             { return "" }
func (*DummyEntry) Bucket() *cluster.Bck                     { return nil }
func (d *DummyEntry) Get() cluster.Xact                      { return d.xact }
func (*DummyEntry) WhenPrevIsRunning(Renewable) (WPR, error) { return WprUse, nil }
