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
	BaseBckEntry
	xact  *query.ObjectsListingXact
	ctx   context.Context
	t     cluster.Target
	query *query.ObjectsQuery
	msg   *cmn.SelectMsg
}

//////////////
// queEntry //
//////////////

// interface guard
var (
	_ BaseEntry = (*queEntry)(nil)
)

func (e *queEntry) Start(_ cmn.Bck) (err error) {
	xact := query.NewObjectsListing(e.ctx, e.t, e.query, e.msg)
	e.xact = xact
	if query.Registry.Get(e.msg.UUID) != nil {
		err = fmt.Errorf("result set with handle %s already exists", e.msg.UUID)
	}
	return
}

func (*queEntry) Kind() string        { return cmn.ActQueryObjects }
func (e *queEntry) Get() cluster.Xact { return e.xact }

func (e *queEntry) PreRenewHook(_ BucketEntry) (keep bool, err error) {
	return query.Registry.Get(e.msg.UUID) != nil, nil
}
