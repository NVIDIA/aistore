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

type queFactory struct {
	BaseBckEntry
	xact  *query.ObjectsListingXact
	ctx   context.Context
	t     cluster.Target
	query *query.ObjectsQuery
	msg   *cmn.SelectMsg
}

////////////////
// queFactory //
////////////////

func (e *queFactory) Start(_ cmn.Bck) (err error) {
	xact := query.NewObjectsListing(e.ctx, e.t, e.query, e.msg)
	e.xact = xact
	if query.Registry.Get(e.msg.UUID) != nil {
		err = fmt.Errorf("result set with handle %s already exists", e.msg.UUID)
	}
	return
}

func (*queFactory) Kind() string        { return cmn.ActQueryObjects }
func (e *queFactory) Get() cluster.Xact { return e.xact }

func (e *queFactory) PreRenewHook(_ BucketEntry) (keep bool, err error) {
	return query.Registry.Get(e.msg.UUID) != nil, nil
}
