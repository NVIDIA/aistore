// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type BownerMock struct {
	cluster.BMD
}

// interface guard
var _ cluster.Bowner = (*BownerMock)(nil)

func (r BownerMock) Get() *cluster.BMD { return &r.BMD }

func NewBaseBownerMock(bcks ...*cluster.Bck) *BownerMock {
	var (
		providers  = make(cluster.Providers)
		namespaces = make(cluster.Namespaces)
		buckets    = make(cluster.Buckets)
	)
	providers[apc.ProviderAIS] = namespaces
	namespaces[cmn.NsGlobal.Uname()] = buckets

	owner := &BownerMock{BMD: cluster.BMD{Version: 1, Providers: providers}}
	for _, bck := range bcks {
		bck.Props.BID = bck.MaskBID(owner.BMD.Version)
		owner.Add(bck)
	}
	return owner
}
