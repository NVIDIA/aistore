/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package cluster

var (
	_ Bowner = &BownerMock{}
)

type BownerMock struct {
	BMD
}

func (r BownerMock) Get() *BMD { return &r.BMD }
