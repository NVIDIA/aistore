/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package cluster

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	_ Target = &TargetMock{}
)

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	BO Bowner
}

func NewTargetMock(bo Bowner) *TargetMock {
	return &TargetMock{
		BO: bo,
	}
}

func (t *TargetMock) AvgCapUsed(config *cmn.Config, used ...int32) (avgCapUsed int32, oos bool) {
	return 0, false
}
func (t *TargetMock) IsRebalancing() bool         { return false }
func (t *TargetMock) RunLRU()                     {}
func (t *TargetMock) PrefetchQueueLen() int       { return 0 }
func (t *TargetMock) Prefetch()                   {}
func (t *TargetMock) GetBowner() Bowner           { return t.BO }
func (t *TargetMock) FSHC(err error, path string) {}
func (t *TargetMock) GetMem2() *memsys.Mem2       { return memsys.Init() }
func (t *TargetMock) GetCold(ctx context.Context, lom *LOM, prefetch bool) (string, int) {
	return "", http.StatusOK
}
func (t *TargetMock) Receive(_ string, _ io.ReadCloser, _ *LOM, _ RecvType, _ cmn.Cksummer, _ time.Time) error {
	return nil
}
func (t *TargetMock) GetFSPRG() fs.PathRunGroup { return nil }
