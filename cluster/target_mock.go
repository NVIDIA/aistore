// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/memsys"
)

// interface guard
var _ Target = (*TargetMock)(nil)

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	BO Bowner
}

func NewTargetMock(bo Bowner) *TargetMock {
	t := &TargetMock{BO: bo}
	T = t
	initLomLocker()
	return t
}

func (t *TargetMock) Bowner() Bowner { return t.BO }

func (*TargetMock) Sname() string                                               { return "" }
func (*TargetMock) SID() string                                                 { return "" }
func (*TargetMock) Snode() *Snode                                               { return nil }
func (*TargetMock) ClusterStarted() bool                                        { return true }
func (*TargetMock) NodeStarted() bool                                           { return true }
func (*TargetMock) DataClient() *http.Client                                    { return http.DefaultClient }
func (*TargetMock) RunLRU(_ string, _ bool, _ ...cmn.Bck)                       {}
func (*TargetMock) Sowner() Sowner                                              { return nil }
func (*TargetMock) FSHC(_ error, _ string)                                      {}
func (*TargetMock) MMSA() *memsys.MMSA                                          { return memsys.DefaultPageMM() }
func (*TargetMock) SmallMMSA() *memsys.MMSA                                     { return memsys.DefaultSmallMM() }
func (*TargetMock) PutObject(_ *LOM, _ PutObjectParams) error                   { return nil }
func (*TargetMock) FinalizeObj(_ *LOM, _ string, _ int64) (int, error)          { return 0, nil }
func (*TargetMock) EvictObject(_ *LOM) (int, error)                             { return 0, nil }
func (*TargetMock) DeleteObject(_ context.Context, _ *LOM, _ bool) (int, error) { return 0, nil }
func (*TargetMock) PromoteFile(_ PromoteFileParams) (*LOM, error)               { return nil, nil }
func (*TargetMock) DB() dbdriver.Driver                                         { return nil }
func (*TargetMock) Backend(_ *Bck) BackendProvider                              { return nil }
func (*TargetMock) GFN(_ GFNType) GFN                                           { return nil }
func (*TargetMock) LookupRemoteSingle(_ *LOM, _ *Snode) bool                    { return false }
func (*TargetMock) RebalanceNamespace(_ *Snode) ([]byte, int, error)            { return nil, 0, nil }
func (*TargetMock) BMDVersionFixup(r *http.Request, bck ...cmn.Bck)             {}

func (*TargetMock) CopyObject(_ *LOM, _ *CopyObjectParams, _ bool) (int64, error) {
	return 0, nil
}

func (*TargetMock) GetCold(ctx context.Context, lom *LOM, _ GetColdType) (int, error) {
	return http.StatusOK, nil
}

func (*TargetMock) Health(si *Snode, timeout time.Duration, query url.Values) ([]byte, int, error) {
	return nil, 0, nil
}

func (*TargetMock) CheckRemoteVersion(ctx context.Context, lom *LOM) (bool, int, error) {
	return false, 0, nil
}
