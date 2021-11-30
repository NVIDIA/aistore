// Package mock provides mock implementation for cluster interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xaction"
)

func init() {
	cos.InitShortID(0)
}

// interface guard
var _ cluster.Xact = (*XactMock)(nil)

// XactMock provides cluster.Xact interface with mocked return values.
type XactMock struct {
	xaction.XactBase
}

func (*XactMock) Run(*sync.WaitGroup) {
	panic("unused")
}

func NewXact(kind string) *XactMock {
	xact := &XactMock{}
	xact.InitBase(cos.GenUUID(), kind, nil)
	return xact
}
