// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

// interface guard
var _ Bowner = (*BownerMock)(nil)

type BownerMock struct {
	BMD
}

func (r BownerMock) Get() *BMD { return &r.BMD }

func NewBaseBownerMock(bcks ...*Bck) *BownerMock {
	var (
		providers  = make(Providers)
		namespaces = make(Namespaces)
		buckets    = make(Buckets)
	)
	providers[apc.ProviderAIS] = namespaces
	namespaces[cmn.NsGlobal.Uname()] = buckets

	owner := &BownerMock{BMD: BMD{Version: 1, Providers: providers}}
	for _, bck := range bcks {
		bck.Props.BID = bck.MaskBID(owner.BMD.Version)
		owner.Add(bck)
	}
	return owner
}
