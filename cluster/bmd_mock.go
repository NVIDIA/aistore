// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import "github.com/NVIDIA/aistore/cmn"

var (
	_ Bowner = &BownerMock{}
)

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
	providers[cmn.ProviderAIS] = namespaces
	namespaces[cmn.NsGlobal.Uname()] = buckets

	owner := &BownerMock{BMD: BMD{Providers: providers}}
	for _, bck := range bcks {
		owner.Add(bck)
	}
	return owner
}
