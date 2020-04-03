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

func (*TargetMock) Snode() *Snode                                              { return nil }
func (*TargetMock) RunLRU(_ string)                                            {}
func (t *TargetMock) GetBowner() Bowner                                        { return t.BO }
func (*TargetMock) GetSowner() Sowner                                          { return nil }
func (*TargetMock) FSHC(_ error, _ string)                                     {}
func (*TargetMock) GetMMSA() *memsys.MMSA                                      { return memsys.DefaultPageMM() }
func (*TargetMock) GetSmallMMSA() *memsys.MMSA                                 { return memsys.DefaultSmallMM() }
func (*TargetMock) PutObject(_ PutObjectParams) error                          { return nil }
func (*TargetMock) GetObject(_ io.Writer, _ *LOM, _ time.Time) error           { return nil }
func (*TargetMock) GetCold(_ context.Context, _ *LOM, _ bool) (error, int)     { return nil, http.StatusOK }
func (*TargetMock) CopyObject(_ *LOM, _ *Bck, _ []byte, _ bool) (bool, error)  { return false, nil }
func (*TargetMock) PromoteFile(_ string, _ *Bck, _ string, _, _, _ bool) error { return nil }
func (*TargetMock) GetFSPRG() fs.PathRunGroup                                  { return nil }
func (*TargetMock) Cloud() CloudProvider                                       { return nil }
func (*TargetMock) StartTime() time.Time                                       { return time.Now() }
func (*TargetMock) GetGFN(_ GFNType) GFN                                       { return nil }
func (*TargetMock) LookupRemoteSingle(_ *LOM, _ *Snode) bool                   { return false }
func (*TargetMock) Health(_ *Snode, _ bool, _ time.Duration) ([]byte, error)   { return nil, nil }
func (*TargetMock) RebalanceNamespace(_ *Snode) ([]byte, int, error)           { return nil, 0, nil }

func (*TargetMock) AvgCapUsed(_ *cmn.Config, _ ...int32) (capInfo cmn.CapacityInfo) { return }
func (*TargetMock) RebalanceInfo() RebalanceInfo {
	return RebalanceInfo{IsRebalancing: false, RebID: 0}
}
func (*TargetMock) BMDVersionFixup(_ *http.Request, _ cmn.Bck, _ bool) {}
func (*TargetMock) CheckCloudVersion(_ context.Context, _ *LOM) (bool, error, int) {
	return false, nil, 0
}
