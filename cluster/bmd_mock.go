/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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

func NewBaseBownerMock() *BownerMock {
	providers := make(Providers)
	namespaces := make(Namespaces)
	providers[cmn.ProviderAIS] = namespaces
	buckets := make(Buckets)
	namespaces[cmn.NsGlobal.Uname()] = buckets
	return &BownerMock{BMD: BMD{Providers: providers}}
}
