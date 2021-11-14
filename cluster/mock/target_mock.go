// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// interface guard
var _ cluster.Target = (*TargetMock)(nil)

var (
	pmm  *memsys.MMSA
	smm  *memsys.MMSA
	once sync.Once
)

// unit tests only
func _init() {
	pmm = memsys.TestPageMM()
	smm = memsys.TestByteMM(pmm)
}

// TargetMock provides cluster.Target interface with mocked return values.
type TargetMock struct {
	BO  cluster.Bowner
	pmm *memsys.MMSA
	smm *memsys.MMSA
}

func NewTarget(bo cluster.Bowner, mm ...*memsys.MMSA) *TargetMock {
	t := &TargetMock{BO: bo}
	if len(mm) > 0 {
		t.pmm, t.smm = mm[0], mm[1]
	}
	cluster.Init(t)
	return t
}

func (t *TargetMock) Bowner() cluster.Bowner { return t.BO }

func (t *TargetMock) PageMM() *memsys.MMSA {
	if t.pmm != nil {
		return t.pmm
	}
	once.Do(_init)
	return pmm
}

func (t *TargetMock) ByteMM() *memsys.MMSA {
	if t.smm != nil {
		return t.smm
	}
	once.Do(_init)
	return smm
}

func (*TargetMock) Sname() string            { return "" }
func (*TargetMock) SID() string              { return "" }
func (*TargetMock) Snode() *cluster.Snode    { return nil }
func (*TargetMock) ClusterStarted() bool     { return true }
func (*TargetMock) NodeStarted() bool        { return true }
func (*TargetMock) DataClient() *http.Client { return http.DefaultClient }
func (*TargetMock) Sowner() cluster.Sowner   { return nil }

func (*TargetMock) PutObject(*cluster.LOM, cluster.PutObjectParams) error       { return nil }
func (*TargetMock) FinalizeObj(*cluster.LOM, string) (int, error)               { return 0, nil }
func (*TargetMock) EvictObject(*cluster.LOM) (int, error)                       { return 0, nil }
func (*TargetMock) DeleteObject(*cluster.LOM, bool) (int, error)                { return 0, nil }
func (*TargetMock) PromoteFile(cluster.PromoteFileParams) (*cluster.LOM, error) { return nil, nil }
func (*TargetMock) DB() dbdriver.Driver                                         { return nil }
func (*TargetMock) Backend(*cluster.Bck) cluster.BackendProvider                { return nil }
func (*TargetMock) LookupRemoteSingle(*cluster.LOM, *cluster.Snode) bool        { return false }
func (*TargetMock) RebalanceNamespace(*cluster.Snode) ([]byte, int, error)      { return nil, 0, nil }
func (*TargetMock) BMDVersionFixup(*http.Request, ...cmn.Bck)                   {}
func (*TargetMock) FSHC(error, string)                                          {}
func (*TargetMock) OOS(*fs.CapStatus) fs.CapStatus                              { return fs.CapStatus{} }

func (*TargetMock) CopyObject(*cluster.LOM, *cluster.CopyObjectParams, bool) (int64, error) {
	return 0, nil
}

func (*TargetMock) CompareObjects(context.Context, *cluster.LOM) (bool, int, error) {
	return true, 0, nil
}

func (*TargetMock) GetCold(context.Context, *cluster.LOM, cluster.GetColdType) (int, error) {
	return http.StatusOK, nil
}

func (*TargetMock) Health(*cluster.Snode, time.Duration, url.Values) ([]byte, int, error) {
	return nil, 0, nil
}
