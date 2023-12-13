// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// TargetMock provides cluster.Target interface with mocked return values.
type TargetMock struct {
	BO meta.Bowner
}

// interface guard
var _ cluster.Target = (*TargetMock)(nil)

func NewTarget(bo meta.Bowner) *TargetMock {
	t := &TargetMock{BO: bo}
	cluster.Tinit(t, NewStatsTracker(), false)
	return t
}

func (t *TargetMock) Bowner() meta.Bowner { return t.BO }

func (*TargetMock) Sname() string            { return "tmock" }
func (*TargetMock) SID() string              { return "mock-id" }
func (*TargetMock) String() string           { return "tmock" }
func (*TargetMock) Snode() *meta.Snode       { return nil }
func (*TargetMock) ClusterStarted() bool     { return true }
func (*TargetMock) NodeStarted() bool        { return true }
func (*TargetMock) DataClient() *http.Client { return http.DefaultClient }
func (*TargetMock) Sowner() meta.Sowner      { return nil }
func (*TargetMock) PageMM() *memsys.MMSA     { return memsys.PageMM() }
func (*TargetMock) ByteMM() *memsys.MMSA     { return memsys.ByteMM() }

func (*TargetMock) GetAllRunning(*cluster.AllRunningInOut, bool)                {}
func (*TargetMock) PutObject(*cluster.LOM, *cluster.PutObjectParams) error      { return nil }
func (*TargetMock) FinalizeObj(*cluster.LOM, string, cluster.Xact) (int, error) { return 0, nil }
func (*TargetMock) EvictObject(*cluster.LOM) (int, error)                       { return 0, nil }
func (*TargetMock) DeleteObject(*cluster.LOM, bool) (int, error)                { return 0, nil }
func (*TargetMock) Promote(*cluster.PromoteParams) (int, error)                 { return 0, nil }
func (*TargetMock) Backend(*meta.Bck) cluster.BackendProvider                   { return nil }
func (*TargetMock) HeadObjT2T(*cluster.LOM, *meta.Snode) bool                   { return false }
func (*TargetMock) BMDVersionFixup(*http.Request, ...cmn.Bck)                   {}
func (*TargetMock) FSHC(error, string)                                          {}
func (*TargetMock) OOS(*fs.CapStatus) fs.CapStatus                              { return fs.CapStatus{} }

func (*TargetMock) CopyObject(*cluster.LOM, cluster.DataMover, cluster.DP, cluster.Xact, *meta.Bck, string, []byte,
	bool) (int64, error) {
	return 0, nil
}

func (*TargetMock) CompareObjects(context.Context, *cluster.LOM) (bool, int, error) {
	return true, 0, nil
}

func (*TargetMock) GetCold(context.Context, *cluster.LOM, cmn.OWT) (int, error) {
	return http.StatusOK, nil
}

func (*TargetMock) Health(*meta.Snode, time.Duration, url.Values) ([]byte, int, error) {
	return nil, 0, nil
}
