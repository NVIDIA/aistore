// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const mockID = "mock-id"

// TargetMock provides cluster.Target interface with mocked return values.
type TargetMock struct {
	BO       meta.Bowner
	SO       meta.Sowner
	Backends map[string]core.Backend
}

// interface guard
var _ core.Target = (*TargetMock)(nil)

func NewTarget(bo meta.Bowner) *TargetMock {
	t := &TargetMock{BO: bo}
	core.Tinit(t, NewStatsTracker(), nil /*config*/, false /*run HK*/)
	return t
}

func (t *TargetMock) Bowner() meta.Bowner { return t.BO }
func (t *TargetMock) Sowner() meta.Sowner { return t.SO }

func (*TargetMock) SID() string              { return mockID }
func (*TargetMock) String() string           { return "tmock" }
func (*TargetMock) Snode() *meta.Snode       { return &meta.Snode{DaeID: mockID} }
func (*TargetMock) ClusterStarted() bool     { return true }
func (*TargetMock) NodeStarted() bool        { return true }
func (*TargetMock) DataClient() *http.Client { return http.DefaultClient }
func (*TargetMock) PageMM() *memsys.MMSA     { return memsys.PageMM() }
func (*TargetMock) ByteMM() *memsys.MMSA     { return memsys.ByteMM() }

func (*TargetMock) MaxUtilLoad() (int64, float64) { return 0, 0 }

func (*TargetMock) GetAllRunning(*core.AllRunningInOut, bool)                      {}
func (*TargetMock) PutObject(*core.LOM, *core.PutParams) error                     { return nil }
func (*TargetMock) FinalizeObj(*core.LOM, string, core.Xact, cmn.OWT) (int, error) { return 0, nil }
func (*TargetMock) EvictObject(*core.LOM) (int, error)                             { return 0, nil }
func (*TargetMock) DeleteObject(*core.LOM, bool) (int, error)                      { return 0, nil }
func (*TargetMock) Promote(*core.PromoteParams) (int, error)                       { return 0, nil }
func (t *TargetMock) Backend(bck *meta.Bck) core.Backend                           { return t.Backends[bck.Provider] }
func (*TargetMock) HeadObjT2T(*core.LOM, *meta.Snode) bool                         { return false }
func (*TargetMock) BMDVersionFixup(*http.Request, ...cmn.Bck)                      {}

func (*TargetMock) SoftFSHC()                         {}
func (*TargetMock) FSHC(error, *fs.Mountpath, string) {}

func (*TargetMock) OOS(*fs.CapStatus, *cmn.Config, *fs.Tcdf) fs.CapStatus {
	return fs.CapStatus{}
}

func (*TargetMock) CopyObject(*core.LOM, core.DM, *core.CopyParams) (int64, error) {
	return 0, nil
}

func (*TargetMock) GetCold(context.Context, *core.LOM, cmn.OWT) (int, error) {
	return http.StatusOK, nil
}

func (*TargetMock) HeadCold(*core.LOM, *http.Request) (*cmn.ObjAttrs, int, error) {
	return nil, 0, nil
}

func (*TargetMock) GetColdBlob(*core.BlobParams, *cmn.ObjAttrs) (core.Xact, error) {
	return nil, nil
}

func (*TargetMock) Health(*meta.Snode, time.Duration, url.Values) ([]byte, int, error) {
	return nil, 0, nil
}

func (*TargetMock) ECRestoreReq(*core.CT, *meta.Snode, string) error {
	return nil
}
