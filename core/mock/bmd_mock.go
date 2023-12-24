// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

type BownerMock struct {
	meta.BMD
}

// interface guard
var _ meta.Bowner = (*BownerMock)(nil)

func (r BownerMock) Get() *meta.BMD { return &r.BMD }

func NewBaseBownerMock(bcks ...*meta.Bck) *BownerMock {
	var (
		providers  = make(meta.Providers)
		namespaces = make(meta.Namespaces)
		buckets    = make(meta.Buckets)
	)
	providers[apc.AIS] = namespaces
	debug.Assert(cmn.NsGlobalUname == cmn.NsGlobal.Uname())
	namespaces[cmn.NsGlobalUname] = buckets

	owner := &BownerMock{BMD: meta.BMD{Version: 1, Providers: providers}}
	for _, bck := range bcks {
		bck.Props.BID = bck.MaskBID(owner.BMD.Version)
		owner.Add(bck)
	}
	return owner
}
