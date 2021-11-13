// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// interface guard
var _ Target = (*TargetMock)(nil)

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

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	BO  Bowner
	pmm *memsys.MMSA
	smm *memsys.MMSA
}

func NewTargetMock(bo Bowner, mm ...*memsys.MMSA) *TargetMock {
	t := &TargetMock{BO: bo}
	T = t
	initLomLocker()
	if len(mm) > 0 {
		t.pmm, t.smm = mm[0], mm[1]
	}
	return t
}

func (t *TargetMock) Bowner() Bowner { return t.BO }

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
func (*TargetMock) Snode() *Snode            { return nil }
func (*TargetMock) ClusterStarted() bool     { return true }
func (*TargetMock) NodeStarted() bool        { return true }
func (*TargetMock) DataClient() *http.Client { return http.DefaultClient }
func (*TargetMock) Sowner() Sowner           { return nil }

func (*TargetMock) PutObject(*LOM, PutObjectParams) error                   { return nil }
func (*TargetMock) FinalizeObj(*LOM, string) (int, error)                   { return 0, nil }
func (*TargetMock) EvictObject(*LOM) (int, error)                           { return 0, nil }
func (*TargetMock) DeleteObject(*LOM, bool) (int, error)                    { return 0, nil }
func (*TargetMock) PromoteFile(PromoteFileParams) (*LOM, error)             { return nil, nil }
func (*TargetMock) DB() dbdriver.Driver                                     { return nil }
func (*TargetMock) Backend(*Bck) BackendProvider                            { return nil }
func (*TargetMock) LookupRemoteSingle(*LOM, *Snode) bool                    { return false }
func (*TargetMock) RebalanceNamespace(*Snode) ([]byte, int, error)          { return nil, 0, nil }
func (*TargetMock) BMDVersionFixup(*http.Request, ...cmn.Bck)               {}
func (*TargetMock) CopyObject(*LOM, *CopyObjectParams, bool) (int64, error) { return 0, nil }
func (*TargetMock) CompareObjects(context.Context, *LOM) (bool, int, error) { return true, 0, nil }

func (*TargetMock) GetCold(context.Context, *LOM, GetColdType) (int, error) {
	return http.StatusOK, nil
}

func (*TargetMock) Health(*Snode, time.Duration, url.Values) ([]byte, int, error) {
	return nil, 0, nil
}

func (*TargetMock) FSHC(error, string)             {}
func (*TargetMock) OOS(*fs.CapStatus) fs.CapStatus { return fs.CapStatus{} }
