// Package cluster provides local access to cluster-level metadata
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

func (*TargetMock) AvgCapUsed(config *cmn.Config, used ...int32) (avgCapUsed int32, oos bool) {
	return 0, false
}
func (*TargetMock) Snode() *Snode               { return nil }
func (*TargetMock) IsRebalancing() bool         { return false }
func (*TargetMock) RunLRU()                     {}
func (*TargetMock) PrefetchQueueLen() int       { return 0 }
func (*TargetMock) Prefetch()                   {}
func (t *TargetMock) GetBowner() Bowner         { return t.BO }
func (*TargetMock) FSHC(err error, path string) {}
func (*TargetMock) GetMem2() *memsys.Mem2       { return memsys.GMM() }

func (*TargetMock) GetCold(ctx context.Context, lom *LOM, prefetch bool) (error, int) {
	return nil, http.StatusOK
}
func (*TargetMock) PutObject(_ string, _ io.ReadCloser, _ *LOM, _ RecvType, _ cmn.Cksummer, _ time.Time) error {
	return nil
}
func (t *TargetMock) GetObject(_ io.Writer, _ *LOM, _ time.Time) error    { return nil }
func (t *TargetMock) CopyObject(_ *LOM, _ string, _ []byte, _ bool) error { return nil }

func (*TargetMock) GetFSPRG() fs.PathRunGroup { return nil }
func (*TargetMock) Cloud() CloudProvider      { return nil }
func (*TargetMock) GetSmap() *Smap            { return nil }
func (*TargetMock) StartTime() time.Time      { return time.Now() }
