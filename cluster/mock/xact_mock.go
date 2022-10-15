// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xact"
)

func init() {
	cos.InitShortID(0)
}

// interface guard
var _ cluster.Xact = (*XactMock)(nil)

// XactMock provides cluster.Xact interface with mocked return values.
type XactMock struct {
	xact.Base
}

func (*XactMock) Run(*sync.WaitGroup) {
	panic("unused")
}

func NewXact(kind string) *XactMock {
	xctn := &XactMock{}
	xctn.InitBase(cos.GenUUID(), kind, nil)
	return xctn
}
